from __future__ import annotations

import gzip
import io
import json
import math
from collections.abc import Iterable, Iterator
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pyarrow.parquet as pq

from foundinspace.octree.config import DEFAULT_CLASSIC_MAX_LEVEL, MORTON_BITS
from foundinspace.octree.duckdb_util import configure_connection
from foundinspace.octree.encoding.render import encode_render_records
from foundinspace.octree.reader.stats import (
    DEFAULT_SHELL_COALESCE_GAP_BYTES,
    coalesce_payload_ranges,
)
from foundinspace.octree.sources.routing import (
    PIPELINE_STATE_NAME,
    TREE_MANIFEST_NAME,
    _read_json,
)

DEFAULT_TARGET_VERTICAL_FOV_DEG = 40.0
DEFAULT_TARGET_ASPECT_RATIO = 16.0 / 9.0
DEFAULT_TARGET_NEAR_PC = 0.01
DEFAULT_TILE_PREFIX_DEPTH = 4
PROFILES = ("classic", "unbounded")
PACKING_ORDERS = ("dfs", "level-major", "tile-level-major")
SCENARIOS = ("observer-shell", "target-frustum")
CLASSIC_MAX_LEVEL = DEFAULT_CLASSIC_MAX_LEVEL
_RAW_RENDER_COLUMNS = {
    "x_icrs_pc",
    "y_icrs_pc",
    "z_icrs_pc",
    "mag_abs",
    "source",
    "source_id",
    "morton_code",
    "level",
}


@dataclass(frozen=True, slots=True)
class Point3:
    x: float
    y: float
    z: float


@dataclass(frozen=True, slots=True)
class PackingBenchmarkConfig:
    routed_dir: Path
    prepared_dir: Path
    profiles: tuple[str, ...] = PROFILES
    orders: tuple[str, ...] = PACKING_ORDERS
    scenarios: tuple[str, ...] = SCENARIOS
    center: Point3 = Point3(0.0, 0.0, 0.0)
    target: Point3 = Point3(1000.0, 0.0, 0.0)
    limiting_magnitude: float | None = None
    vertical_fov_deg: float = DEFAULT_TARGET_VERTICAL_FOV_DEG
    aspect_ratio: float = DEFAULT_TARGET_ASPECT_RATIO
    tile_prefix_depth: int = DEFAULT_TILE_PREFIX_DEPTH
    coalesce_gap_bytes: int = DEFAULT_SHELL_COALESCE_GAP_BYTES
    batch_rows: int = 100_000


@dataclass(frozen=True, slots=True)
class _World:
    center: Point3
    half_size_pc: float
    index_magnitude: float


@dataclass(frozen=True, slots=True)
class _FinalNode:
    level: int
    node_id: int
    star_count: int
    payload_length: int


@dataclass(frozen=True, slots=True)
class _Geometry:
    center: Point3
    half_size: float


def run_packing_benchmark(config: PackingBenchmarkConfig) -> dict[str, Any]:
    """Estimate range-read behavior for candidate packing orders."""
    _validate_config(config)
    manifest, state = _read_pipeline_state(config.routed_dir)
    world = _world_from_manifest(manifest)
    limiting_magnitude = (
        world.index_magnitude
        if config.limiting_magnitude is None
        else float(config.limiting_magnitude)
    )
    preparation_files = _preparation_files(config.prepared_dir, state)

    results: list[dict[str, Any]] = []
    profile_nodes: dict[str, list[_FinalNode]] = {}
    for profile in config.profiles:
        nodes = list(
            _iter_final_nodes(
                preparation_files,
                profile=profile,
                batch_rows=config.batch_rows,
            )
        )
        profile_nodes[profile] = nodes
        for order in config.orders:
            offsets = _payload_offsets(nodes, order, config.tile_prefix_depth)
            for scenario in config.scenarios:
                selected = _select_nodes(
                    nodes,
                    scenario=scenario,
                    world=world,
                    center=config.center,
                    target=config.target,
                    limiting_magnitude=limiting_magnitude,
                    vertical_fov_deg=config.vertical_fov_deg,
                    aspect_ratio=config.aspect_ratio,
                )
                results.append(
                    _scenario_result(
                        profile=profile,
                        order=order,
                        scenario=scenario,
                        final_node_count=len(nodes),
                        selected=selected,
                        offsets=offsets,
                        coalesce_gap_bytes=config.coalesce_gap_bytes,
                    )
                )

    return {
        "routed_dir": str(config.routed_dir),
        "prepared_dir": str(config.prepared_dir),
        "profiles": list(config.profiles),
        "orders": list(config.orders),
        "scenarios": list(config.scenarios),
        "center": _point_dict(config.center),
        "target": _point_dict(config.target),
        "limiting_magnitude": limiting_magnitude,
        "vertical_fov_deg": config.vertical_fov_deg,
        "aspect_ratio": config.aspect_ratio,
        "tile_prefix_depth": config.tile_prefix_depth,
        "coalesce_gap_bytes": config.coalesce_gap_bytes,
        "profile_node_counts": {
            profile: len(nodes) for profile, nodes in profile_nodes.items()
        },
        "results": results,
    }


def _validate_config(config: PackingBenchmarkConfig) -> None:
    if not config.routed_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {config.routed_dir}")
    if not config.prepared_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {config.prepared_dir}")
    _validate_choices(config.profiles, PROFILES, "profile")
    _validate_choices(config.orders, PACKING_ORDERS, "order")
    _validate_choices(config.scenarios, SCENARIOS, "scenario")
    if config.vertical_fov_deg <= 0 or config.vertical_fov_deg >= 180:
        raise ValueError("vertical_fov_deg must be > 0 and < 180")
    if config.aspect_ratio <= 0:
        raise ValueError("aspect_ratio must be > 0")
    if config.tile_prefix_depth < 0 or config.tile_prefix_depth > MORTON_BITS:
        raise ValueError(f"tile_prefix_depth must be in 0..{MORTON_BITS}")
    if config.coalesce_gap_bytes < 0:
        raise ValueError("coalesce_gap_bytes must be >= 0")
    if config.batch_rows <= 0:
        raise ValueError("batch_rows must be > 0")


def _validate_choices(
    values: Iterable[str], allowed: tuple[str, ...], label: str
) -> None:
    unknown = sorted(set(values) - set(allowed))
    if unknown:
        raise ValueError(
            f"Unsupported {label}(s): {', '.join(unknown)}. "
            f"Expected one of: {', '.join(allowed)}"
        )


def _read_pipeline_state(
    routed_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_path = routed_dir / TREE_MANIFEST_NAME
    state_path = routed_dir / PIPELINE_STATE_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing Routing tree manifest: {manifest_path}")
    if not state_path.is_file():
        raise FileNotFoundError(f"Missing Routing state: {state_path}")
    manifest = _read_json(manifest_path)
    state = _read_json(state_path)
    dirty = state["dirty"]["preparation"]
    if dirty["all"] or dirty["group_keys"] or dirty["deleted_routed_group_keys"]:
        raise ValueError("Packing benchmark requires Preparation to be current")
    if not state["products"]["prepared_groups"]:
        raise ValueError("Packing benchmark requires prepared groups")
    return manifest, state


def _world_from_manifest(manifest: dict[str, Any]) -> _World:
    identity = manifest.get("tree_identity")
    if not isinstance(identity, dict):
        raise ValueError("Routing tree manifest is missing tree_identity")
    center_raw = identity.get("world_center", [0.0, 0.0, 0.0])
    if not isinstance(center_raw, list | tuple) or len(center_raw) != 3:
        raise ValueError("tree_identity.world_center must contain three values")
    return _World(
        center=Point3(float(center_raw[0]), float(center_raw[1]), float(center_raw[2])),
        half_size_pc=float(identity["world_half_size_pc"]),
        index_magnitude=float(identity["limiting_magnitude"]),
    )


def _preparation_files(prepared_dir: Path, state: dict[str, Any]) -> list[Path]:
    files: list[Path] = []
    for group in state["products"]["prepared_groups"]:
        for rel_path in group.get("files", []):
            path = prepared_dir / str(rel_path)
            if not path.is_file():
                raise FileNotFoundError(f"Missing Preparation group file: {path}")
            files.append(path)
    if not files:
        raise ValueError("Preparation benchmark found no Preparation parquet files")
    return files


def _iter_final_nodes(
    files: list[Path],
    *,
    profile: str,
    batch_rows: int,
) -> Iterator[_FinalNode]:
    cap_level = CLASSIC_MAX_LEVEL if profile == "classic" else MORTON_BITS
    has_teff = _validate_raw_preparation_schema(files)
    source = _duckdb_read_parquet_source(files)
    final_level_expr = f"CASE WHEN level > {cap_level} THEN {cap_level} ELSE level END"
    teff_expr = "teff" if has_teff else "NULL::DOUBLE AS teff"
    query = f"""
        WITH staged AS (
            SELECT
                {final_level_expr} AS final_level,
                level AS source_level,
                morton_code,
                x_icrs_pc,
                y_icrs_pc,
                z_icrs_pc,
                mag_abs,
                {teff_expr},
                source,
                source_id
            FROM read_parquet({source}, union_by_name = true)
        )
        SELECT
            final_level,
            (
                morton_code
                >> CAST((3 * ({MORTON_BITS} - final_level)) AS INTEGER)
            ) AS final_node_id,
            source_level,
            morton_code,
            x_icrs_pc,
            y_icrs_pc,
            z_icrs_pc,
            mag_abs,
            teff,
            source,
            source_id
        FROM staged
        ORDER BY final_level, final_node_id, mag_abs, source, source_id
    """
    con = duckdb.connect()
    with redirect_stdout(io.StringIO()):
        configure_connection(con)
    try:
        con.execute(query)
        current_key: tuple[int, int] | None = None
        renders = bytearray()
        star_count = 0
        while True:
            batch = con.fetchmany(batch_rows)
            if not batch:
                break
            parsed_rows: list[tuple[int, int]] = []
            positions = np.empty((len(batch), 3), dtype=np.float64)
            magnitudes = np.empty(len(batch), dtype=np.float64)
            temperatures = np.empty(len(batch), dtype=np.float64)
            final_levels = np.empty(len(batch), dtype=np.int32)
            morton_codes = np.empty(len(batch), dtype=np.uint64)
            for index, (
                level_raw,
                node_raw,
                source_level_raw,
                morton_code_raw,
                x_raw,
                y_raw,
                z_raw,
                mag_raw,
                teff_raw,
                source_raw,
                source_id_raw,
            ) in enumerate(batch):
                required = {
                    "level": level_raw,
                    "final_node_id": node_raw,
                    "source_level": source_level_raw,
                    "morton_code": morton_code_raw,
                    "x_icrs_pc": x_raw,
                    "y_icrs_pc": y_raw,
                    "z_icrs_pc": z_raw,
                    "source": source_raw,
                    "source_id": source_id_raw,
                }
                missing = [name for name, value in required.items() if value is None]
                if missing:
                    raise ValueError(
                        "Sidecars benchmark input contains null required fields: "
                        f"{missing}"
                    )
                level = int(level_raw)
                if level < 0 or level > MORTON_BITS:
                    raise ValueError(f"Invalid final node level {level}")
                node_id = int(node_raw)
                source_level = int(source_level_raw)
                if source_level < level:
                    raise ValueError(
                        f"Final node level {level} exceeds source level {source_level}"
                    )
                parsed_rows.append((level, node_id))
                positions[index] = (float(x_raw), float(y_raw), float(z_raw))
                magnitudes[index] = np.nan if mag_raw is None else float(mag_raw)
                temperatures[index] = np.nan if teff_raw is None else float(teff_raw)
                final_levels[index] = level
                morton_codes[index] = int(morton_code_raw)

            encoded_renders = encode_render_records(
                morton_codes=morton_codes,
                positions=positions,
                mag_abs=magnitudes,
                teff=temperatures,
                levels=final_levels,
            )
            for (level, node_id), render in zip(
                parsed_rows,
                encoded_renders,
                strict=True,
            ):
                key = (level, node_id)
                if current_key is not None and key != current_key:
                    yield _compressed_node(current_key, renders, star_count)
                    renders = bytearray()
                    star_count = 0
                current_key = key
                renders.extend(render.tobytes())
                star_count += 1
        if current_key is not None:
            yield _compressed_node(current_key, renders, star_count)
    finally:
        con.close()


def _compressed_node(
    key: tuple[int, int],
    renders: bytearray,
    star_count: int,
) -> _FinalNode:
    return _FinalNode(
        level=key[0],
        node_id=key[1],
        star_count=star_count,
        payload_length=len(gzip.compress(bytes(renders), mtime=0)),
    )


def _duckdb_read_parquet_source(files: list[Path]) -> str:
    quoted = []
    for path in files:
        escaped = path.as_posix().replace("'", "''")
        quoted.append(f"'{escaped}'")
    if len(quoted) == 1:
        return quoted[0]
    return "[" + ", ".join(quoted) + "]"


def _validate_raw_preparation_schema(files: list[Path]) -> bool:
    has_teff = False
    for path in files:
        names = set(pq.read_schema(path).names)
        missing = sorted(_RAW_RENDER_COLUMNS - names)
        if missing:
            raise ValueError(
                "Sidecars benchmark requires raw Preparation fields; "
                f"{path} is missing {missing}. Rebuild Routing and Preparation."
            )
        has_teff = has_teff or "teff" in names
    return has_teff


def _payload_offsets(
    nodes: list[_FinalNode],
    order: str,
    tile_prefix_depth: int,
) -> dict[tuple[int, int], tuple[int, int]]:
    offset = 0
    out: dict[tuple[int, int], tuple[int, int]] = {}
    for node in _ordered_nodes(nodes, order, tile_prefix_depth):
        out[(node.level, node.node_id)] = (offset, node.payload_length)
        offset += node.payload_length
    return out


def _ordered_nodes(
    nodes: list[_FinalNode],
    order: str,
    tile_prefix_depth: int,
) -> list[_FinalNode]:
    if order == "level-major":
        return sorted(nodes, key=lambda node: (node.level, node.node_id))
    if order == "tile-level-major":
        return sorted(
            nodes,
            key=lambda node: (
                _tile_prefix(node.level, node.node_id, tile_prefix_depth),
                node.level,
                node.node_id,
            ),
        )
    if order == "dfs":
        return _dfs_ordered_nodes(nodes)
    raise ValueError(f"Unsupported packing order: {order}")


def _tile_prefix(level: int, node_id: int, prefix_depth: int) -> int:
    if prefix_depth == 0:
        return 0
    if level >= prefix_depth:
        return node_id >> (3 * (level - prefix_depth))
    return node_id << (3 * (prefix_depth - level))


def _dfs_ordered_nodes(nodes: list[_FinalNode]) -> list[_FinalNode]:
    by_key = {(node.level, node.node_id): node for node in nodes}
    child_octants: dict[tuple[int, int], set[int]] = {}
    for node in nodes:
        for ancestor_level in range(node.level):
            shift = 3 * (node.level - ancestor_level)
            ancestor_id = node.node_id >> shift
            child_shift = 3 * (node.level - ancestor_level - 1)
            child_id = node.node_id >> child_shift
            octant = child_id & 0b111
            child_octants.setdefault((ancestor_level, ancestor_id), set()).add(octant)

    ordered: list[_FinalNode] = []

    def walk(level: int, node_id: int) -> None:
        node = by_key.get((level, node_id))
        if node is not None:
            ordered.append(node)
        for octant in sorted(child_octants.get((level, node_id), ())):
            walk(level + 1, (node_id << 3) | octant)

    if (0, 0) in by_key or (0, 0) in child_octants:
        walk(0, 0)
    remaining = [node for node in nodes if node not in set(ordered)]
    if remaining:
        ordered.extend(sorted(remaining, key=lambda node: (node.level, node.node_id)))
    return ordered


def _select_nodes(
    nodes: list[_FinalNode],
    *,
    scenario: str,
    world: _World,
    center: Point3,
    target: Point3,
    limiting_magnitude: float,
    vertical_fov_deg: float,
    aspect_ratio: float,
) -> list[_FinalNode]:
    selected: list[_FinalNode] = []
    frustum = (
        _TargetFrustum(center, target, vertical_fov_deg, aspect_ratio)
        if scenario == "target-frustum"
        else None
    )
    for node in nodes:
        geometry = _node_geometry(node.level, node.node_id, world)
        load_radius = geometry.half_size * (
            10.0 ** ((limiting_magnitude - world.index_magnitude) / 5.0)
        )
        if _aabb_distance(center, geometry) > load_radius:
            continue
        if frustum is not None and not frustum.intersects(geometry, load_radius):
            continue
        selected.append(node)
    return selected


def _scenario_result(
    *,
    profile: str,
    order: str,
    scenario: str,
    final_node_count: int,
    selected: list[_FinalNode],
    offsets: dict[tuple[int, int], tuple[int, int]],
    coalesce_gap_bytes: int,
) -> dict[str, Any]:
    ranges = [offsets[(node.level, node.node_id)] for node in selected]
    coalesced = coalesce_payload_ranges(
        ranges,
        merge_gap_bytes=coalesce_gap_bytes,
    )
    gap_bytes = max(0, coalesced.total_span_bytes - coalesced.raw_payload_bytes)
    useful_ratio = (
        coalesced.raw_payload_bytes / coalesced.total_span_bytes
        if coalesced.total_span_bytes > 0
        else 1.0
    )
    return {
        "profile": profile,
        "order": order,
        "scenario": scenario,
        "final_node_count": final_node_count,
        "selected_node_count": len(selected),
        "selected_star_count": sum(node.star_count for node in selected),
        "payload_range_count": coalesced.input_ranges,
        "coalesced_batch_count": coalesced.output_batches,
        "raw_payload_bytes": coalesced.raw_payload_bytes,
        "span_bytes": coalesced.total_span_bytes,
        "gap_bytes": gap_bytes,
        "useful_ratio": useful_ratio,
        "largest_batch_bytes": coalesced.largest_batch_bytes,
    }


def _node_geometry(level: int, node_id: int, world: _World) -> _Geometry:
    grid_x, grid_y, grid_z = _decode_node_id(node_id, level)
    cells_per_axis = 1 << level
    cell_width = (2.0 * world.half_size_pc) / cells_per_axis
    half_size = cell_width / 2.0
    min_x = world.center.x - world.half_size_pc
    min_y = world.center.y - world.half_size_pc
    min_z = world.center.z - world.half_size_pc
    return _Geometry(
        center=Point3(
            min_x + (grid_x + 0.5) * cell_width,
            min_y + (grid_y + 0.5) * cell_width,
            min_z + (grid_z + 0.5) * cell_width,
        ),
        half_size=half_size,
    )


def _decode_node_id(node_id: int, level: int) -> tuple[int, int, int]:
    x = 0
    y = 0
    z = 0
    for bit in range(level):
        shift = bit * 3
        x |= ((node_id >> shift) & 1) << bit
        y |= ((node_id >> (shift + 1)) & 1) << bit
        z |= ((node_id >> (shift + 2)) & 1) << bit
    return x, y, z


def _aabb_distance(point: Point3, geometry: _Geometry) -> float:
    dx = max(abs(point.x - geometry.center.x) - geometry.half_size, 0.0)
    dy = max(abs(point.y - geometry.center.y) - geometry.half_size, 0.0)
    dz = max(abs(point.z - geometry.center.z) - geometry.half_size, 0.0)
    return math.hypot(dx, dy, dz)


class _TargetFrustum:
    def __init__(
        self,
        origin: Point3,
        target: Point3,
        vertical_fov_deg: float,
        aspect_ratio: float,
    ) -> None:
        forward = _normalize(
            (target.x - origin.x, target.y - origin.y, target.z - origin.z)
        )
        target_distance = _length(
            (target.x - origin.x, target.y - origin.y, target.z - origin.z)
        )
        if target_distance <= 0:
            raise ValueError("target must differ from center for target-frustum")
        up = (0.0, 0.0, 1.0)
        right = _cross(forward, up)
        if _length(right) < 1.0e-12:
            up = (0.0, 1.0, 0.0)
            right = _cross(forward, up)
        right = _normalize(right)
        true_up = _normalize(_cross(right, forward))
        self.origin = origin
        self.forward = forward
        self.right = right
        self.up = true_up
        self.target_distance = target_distance
        self.tan_y = math.tan(math.radians(vertical_fov_deg) / 2.0)
        self.tan_x = self.tan_y * aspect_ratio

    def intersects(self, geometry: _Geometry, load_radius: float) -> bool:
        if _aabb_distance(self.origin, geometry) == 0.0:
            return True
        far_pc = self.target_distance + load_radius
        if _ray_intersects_aabb(
            origin=self.origin,
            direction=self.forward,
            geometry=geometry,
            near_pc=DEFAULT_TARGET_NEAR_PC,
            far_pc=far_pc,
        ):
            return True
        for corner in _aabb_corners(geometry):
            x, y, z = self._camera_coordinates(corner)
            if z < DEFAULT_TARGET_NEAR_PC or z > far_pc:
                continue
            if abs(x) <= z * self.tan_x and abs(y) <= z * self.tan_y:
                return True
        return False

    def _camera_coordinates(self, point: Point3) -> tuple[float, float, float]:
        rel = (
            point.x - self.origin.x,
            point.y - self.origin.y,
            point.z - self.origin.z,
        )
        return (
            _dot(rel, self.right),
            _dot(rel, self.up),
            _dot(rel, self.forward),
        )


def _aabb_corners(geometry: _Geometry) -> Iterator[Point3]:
    for sx in (-1.0, 1.0):
        for sy in (-1.0, 1.0):
            for sz in (-1.0, 1.0):
                yield Point3(
                    geometry.center.x + sx * geometry.half_size,
                    geometry.center.y + sy * geometry.half_size,
                    geometry.center.z + sz * geometry.half_size,
                )


def _ray_intersects_aabb(
    *,
    origin: Point3,
    direction: tuple[float, float, float],
    geometry: _Geometry,
    near_pc: float,
    far_pc: float,
) -> bool:
    mins = (
        geometry.center.x - geometry.half_size,
        geometry.center.y - geometry.half_size,
        geometry.center.z - geometry.half_size,
    )
    maxs = (
        geometry.center.x + geometry.half_size,
        geometry.center.y + geometry.half_size,
        geometry.center.z + geometry.half_size,
    )
    origin_values = (origin.x, origin.y, origin.z)
    t_min = -math.inf
    t_max = math.inf
    for axis_origin, axis_dir, axis_min, axis_max in zip(
        origin_values,
        direction,
        mins,
        maxs,
        strict=True,
    ):
        if abs(axis_dir) < 1.0e-12:
            if axis_origin < axis_min or axis_origin > axis_max:
                return False
            continue
        t1 = (axis_min - axis_origin) / axis_dir
        t2 = (axis_max - axis_origin) / axis_dir
        lo = min(t1, t2)
        hi = max(t1, t2)
        t_min = max(t_min, lo)
        t_max = min(t_max, hi)
        if t_min > t_max:
            return False
    return max(t_min, near_pc) <= min(t_max, far_pc)


def _normalize(value: tuple[float, float, float]) -> tuple[float, float, float]:
    length = _length(value)
    if length <= 0:
        raise ValueError("Cannot normalize zero-length vector")
    return (value[0] / length, value[1] / length, value[2] / length)


def _length(value: tuple[float, float, float]) -> float:
    return math.sqrt(_dot(value, value))


def _dot(
    left: tuple[float, float, float],
    right: tuple[float, float, float],
) -> float:
    return left[0] * right[0] + left[1] * right[1] + left[2] * right[2]


def _cross(
    left: tuple[float, float, float],
    right: tuple[float, float, float],
) -> tuple[float, float, float]:
    return (
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    )


def _point_dict(point: Point3) -> dict[str, float]:
    return {"x": point.x, "y": point.y, "z": point.z}


def report_to_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"
