from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib

from .config import (
    DEFAULT_CLASSIC_MAX_LEVEL,
    DEFAULT_CLASSIC_PARTITION_FROM_LEVEL,
    DEFAULT_CLASSIC_PARTITION_PREFIX_BITS,
    DEFAULT_DEEP_SHARD_FROM_LEVEL,
    DEFAULT_MAG_VIS,
    DEFAULT_TERMINAL_WATERLINE,
    MORTON_BITS,
)

_DEFAULT_INPUT_SHARDS_DIR = "../data/processed/merged/healpix"
_DEFAULT_IDENTIFIERS_MAP_PATH = "../data/processed/identifiers_map.parquet"
_DEFAULT_ROUTED_DIR = "octree/routed"
_DEFAULT_PREPARED_DIR = "octree/prepared"
_DEFAULT_TOPOLOGY_DIR = "octree/topology"
_DEFAULT_MATERIALIZED_DIR = "octree/materialized"
_DEFAULT_BUILD_WORK_DIR = "octree/work"
_DEFAULT_RENDER_OUTPUT_PATH = "products/stars-v2.octree"
_DEFAULT_IDENTIFIERS_ORDER_OUTPUT_PATH = "products/identifiers-v2.order"
_DEFAULT_SIDECARS_OUTPUT_DIR = "products/sidecars"
_DEFAULT_SIDECARS_WORK_DIR = "octree/sidecars-work"

_ROUTING_INPUT_MODES = {"cartesian", "pre-routed"}
_PROFILE_NAMES = {"classic", "terminal-packed"}
_INDEX_EMISSION_STRATEGIES = {"temp-pwrite-batched", "forward"}


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    input_shards_dir: Path
    identifiers_map_path: Path
    routed_dir: Path
    prepared_dir: Path
    topology_dir: Path
    materialized_dir: Path
    build_work_dir: Path
    render_output_path: Path
    identifiers_order_output_path: Path
    sidecars_output_dir: Path
    sidecars_work_dir: Path


@dataclass(frozen=True, slots=True)
class DatasetProjectConfig:
    limiting_magnitude: float


@dataclass(frozen=True, slots=True)
class ExecutionProjectConfig:
    batch_rows: int
    max_open_files: int


@dataclass(frozen=True, slots=True)
class RoutingProjectConfig:
    input_mode: str
    scan_batch_rows: int
    bucket_rows: int
    fragment_target_rows: int
    max_open_writers: int
    compact_after_files: int


@dataclass(frozen=True, slots=True)
class MaterializationProjectConfig:
    partition_from_level: int
    partition_prefix_bits: int
    terminal_waterline: int


@dataclass(frozen=True, slots=True)
class ProfileProjectConfig:
    name: str
    max_level: int

    @property
    def star_format_version(self) -> int:
        return 2 if self.name == "terminal-packed" else 1


@dataclass(frozen=True, slots=True)
class PackingProjectConfig:
    index_emission_strategy: str


@dataclass(frozen=True, slots=True)
class SidecarFamilyConfig:
    name: str
    fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SidecarsProjectConfig:
    shard_from_level: int
    shard_prefix_bits: int
    families: tuple[SidecarFamilyConfig, ...]


@dataclass(frozen=True, slots=True)
class OctreeProject:
    project_path: Path
    paths: ProjectPaths
    dataset: DatasetProjectConfig
    execution: ExecutionProjectConfig
    routing: RoutingProjectConfig
    materialization: MaterializationProjectConfig
    profile: ProfileProjectConfig
    packing: PackingProjectConfig
    sidecars: SidecarsProjectConfig


def _reject_env_expansion(value: str, *, field_name: str) -> None:
    if "$" in value:
        raise ValueError(
            f"{field_name} must not contain environment-variable syntax: {value!r}"
        )


def _require_table(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Missing or invalid [{key}] table in project file")
    return value


def _require_int(raw: dict[str, Any], key: str) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value


def _require_float(raw: dict[str, Any], key: str) -> float:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric")
    return float(value)


def _require_str(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value.strip()


def _resolve_path(project_dir: Path, value: str, *, field_name: str) -> Path:
    _reject_env_expansion(value, field_name=field_name)
    raw_path = Path(value)
    return raw_path if raw_path.is_absolute() else project_dir / raw_path


def _project_path(
    project_dir: Path,
    paths_raw: dict[str, Any],
    key: str,
) -> Path:
    return _resolve_path(
        project_dir,
        _require_str(paths_raw, key),
        field_name=f"paths.{key}",
    )


def load_project(project_path: Path) -> OctreeProject:
    resolved_project_path = project_path.expanduser().resolve()
    with resolved_project_path.open("rb") as fp:
        raw = tomllib.load(fp)

    project_dir = resolved_project_path.parent
    paths_raw = _require_table(raw, "paths")
    dataset_raw = _require_table(raw, "dataset")
    execution_raw = _require_table(raw, "execution")
    routing_raw = _require_table(raw, "routing")
    materialization_raw = _require_table(raw, "materialization")
    profile_raw = _require_table(raw, "profile")
    packing_raw = _require_table(raw, "packing")
    sidecars_raw = _require_table(raw, "sidecars")

    paths = ProjectPaths(
        input_shards_dir=_project_path(project_dir, paths_raw, "input_shards_dir"),
        identifiers_map_path=_project_path(
            project_dir, paths_raw, "identifiers_map_path"
        ),
        routed_dir=_project_path(project_dir, paths_raw, "routed_dir"),
        prepared_dir=_project_path(project_dir, paths_raw, "prepared_dir"),
        topology_dir=_project_path(project_dir, paths_raw, "topology_dir"),
        materialized_dir=_project_path(project_dir, paths_raw, "materialized_dir"),
        build_work_dir=_project_path(project_dir, paths_raw, "build_work_dir"),
        render_output_path=_project_path(project_dir, paths_raw, "render_output_path"),
        identifiers_order_output_path=_project_path(
            project_dir, paths_raw, "identifiers_order_output_path"
        ),
        sidecars_output_dir=_project_path(
            project_dir, paths_raw, "sidecars_output_dir"
        ),
        sidecars_work_dir=_project_path(project_dir, paths_raw, "sidecars_work_dir"),
    )

    dataset = DatasetProjectConfig(
        limiting_magnitude=_require_float(dataset_raw, "limiting_magnitude")
    )
    execution = ExecutionProjectConfig(
        batch_rows=_require_int(execution_raw, "batch_rows"),
        max_open_files=_require_int(execution_raw, "max_open_files"),
    )
    if execution.batch_rows <= 0:
        raise ValueError("execution.batch_rows must be > 0")
    if execution.max_open_files <= 0:
        raise ValueError("execution.max_open_files must be > 0")

    routing = RoutingProjectConfig(
        input_mode=_require_str(routing_raw, "input_mode"),
        scan_batch_rows=_require_int(routing_raw, "scan_batch_rows"),
        bucket_rows=_require_int(routing_raw, "bucket_rows"),
        fragment_target_rows=_require_int(routing_raw, "fragment_target_rows"),
        max_open_writers=_require_int(routing_raw, "max_open_writers"),
        compact_after_files=_require_int(routing_raw, "compact_after_files"),
    )
    if routing.input_mode not in _ROUTING_INPUT_MODES:
        raise ValueError(
            "routing.input_mode must be one of "
            f"{sorted(_ROUTING_INPUT_MODES)}, got {routing.input_mode!r}"
        )
    for field_name in (
        "scan_batch_rows",
        "bucket_rows",
        "fragment_target_rows",
        "max_open_writers",
    ):
        if getattr(routing, field_name) <= 0:
            raise ValueError(f"routing.{field_name} must be > 0")
    if routing.compact_after_files < 0:
        raise ValueError("routing.compact_after_files must be >= 0")

    materialization = MaterializationProjectConfig(
        partition_from_level=_require_int(materialization_raw, "partition_from_level"),
        partition_prefix_bits=_require_int(
            materialization_raw, "partition_prefix_bits"
        ),
        terminal_waterline=_require_int(materialization_raw, "terminal_waterline"),
    )
    if materialization.partition_from_level < 0:
        raise ValueError("materialization.partition_from_level must be >= 0")
    if materialization.partition_prefix_bits < 0:
        raise ValueError("materialization.partition_prefix_bits must be >= 0")
    if materialization.terminal_waterline <= 0:
        raise ValueError("materialization.terminal_waterline must be > 0")

    profile_name = _require_str(profile_raw, "name")
    if profile_name not in _PROFILE_NAMES:
        raise ValueError(
            f"profile.name must be one of {sorted(_PROFILE_NAMES)}, got {profile_name!r}"
        )
    max_level = _require_int(profile_raw, "max_level")
    if max_level < 0 or max_level > MORTON_BITS:
        raise ValueError(f"profile.max_level must be in 0..{MORTON_BITS}")
    profile = ProfileProjectConfig(
        name=profile_name,
        max_level=max_level,
    )

    packing = PackingProjectConfig(
        index_emission_strategy=_require_str(packing_raw, "index_emission_strategy")
    )
    if packing.index_emission_strategy not in _INDEX_EMISSION_STRATEGIES:
        raise ValueError(
            "packing.index_emission_strategy must be one of "
            f"{sorted(_INDEX_EMISSION_STRATEGIES)}, "
            f"got {packing.index_emission_strategy!r}"
        )

    shard_from_level = _require_int(sidecars_raw, "shard_from_level")
    shard_prefix_bits = _require_int(sidecars_raw, "shard_prefix_bits")
    if shard_from_level < 0:
        raise ValueError("sidecars.shard_from_level must be >= 0")
    if shard_prefix_bits < 0:
        raise ValueError("sidecars.shard_prefix_bits must be >= 0")
    families_raw = sidecars_raw.get("families", [])
    if not isinstance(families_raw, list):
        raise ValueError("sidecars.families must be an array of tables")
    seen_names: set[str] = set()
    families: list[SidecarFamilyConfig] = []
    for idx, family_raw in enumerate(families_raw):
        if not isinstance(family_raw, dict):
            raise ValueError(f"sidecars.families[{idx}] must be a table")
        name = _require_str(family_raw, "name")
        if name in seen_names:
            raise ValueError(f"Duplicate sidecar family name: {name}")
        seen_names.add(name)
        fields_raw = family_raw.get("fields", [])
        if not isinstance(fields_raw, list) or not all(
            isinstance(value, str) and value.strip() for value in fields_raw
        ):
            raise ValueError(
                f"sidecars.families[{idx}].fields must be a list of non-empty strings"
            )
        families.append(
            SidecarFamilyConfig(
                name=name,
                fields=tuple(value.strip() for value in fields_raw),
            )
        )
    sidecars = SidecarsProjectConfig(
        shard_from_level=shard_from_level,
        shard_prefix_bits=shard_prefix_bits,
        families=tuple(families),
    )

    return OctreeProject(
        project_path=resolved_project_path,
        paths=paths,
        dataset=dataset,
        execution=execution,
        routing=routing,
        materialization=materialization,
        profile=profile,
        packing=packing,
        sidecars=sidecars,
    )


def render_project_template() -> str:
    return (
        "[paths]\n"
        f'input_shards_dir = "{_DEFAULT_INPUT_SHARDS_DIR}"\n'
        f'identifiers_map_path = "{_DEFAULT_IDENTIFIERS_MAP_PATH}"\n'
        f'routed_dir = "{_DEFAULT_ROUTED_DIR}"\n'
        f'prepared_dir = "{_DEFAULT_PREPARED_DIR}"\n'
        f'topology_dir = "{_DEFAULT_TOPOLOGY_DIR}"\n'
        f'materialized_dir = "{_DEFAULT_MATERIALIZED_DIR}"\n'
        f'build_work_dir = "{_DEFAULT_BUILD_WORK_DIR}"\n'
        f'render_output_path = "{_DEFAULT_RENDER_OUTPUT_PATH}"\n'
        f'identifiers_order_output_path = "{_DEFAULT_IDENTIFIERS_ORDER_OUTPUT_PATH}"\n'
        f'sidecars_output_dir = "{_DEFAULT_SIDECARS_OUTPUT_DIR}"\n'
        f'sidecars_work_dir = "{_DEFAULT_SIDECARS_WORK_DIR}"\n\n'
        "[dataset]\n"
        f"limiting_magnitude = {DEFAULT_MAG_VIS}\n\n"
        "[execution]\n"
        "batch_rows = 100000\n"
        "max_open_files = 32\n\n"
        "[routing]\n"
        'input_mode = "cartesian"\n'
        "scan_batch_rows = 1000000\n"
        "bucket_rows = 1000000\n"
        "fragment_target_rows = 100000\n"
        "max_open_writers = 128\n"
        "compact_after_files = 64\n\n"
        "[materialization]\n"
        f"partition_from_level = {DEFAULT_CLASSIC_PARTITION_FROM_LEVEL}\n"
        f"partition_prefix_bits = {DEFAULT_CLASSIC_PARTITION_PREFIX_BITS}\n"
        f"terminal_waterline = {DEFAULT_TERMINAL_WATERLINE}\n\n"
        "[profile]\n"
        'name = "terminal-packed"\n'
        f"max_level = {DEFAULT_CLASSIC_MAX_LEVEL}\n\n"
        "[packing]\n"
        'index_emission_strategy = "temp-pwrite-batched"\n\n'
        "[sidecars]\n"
        f"shard_from_level = {DEFAULT_DEEP_SHARD_FROM_LEVEL}\n"
        "shard_prefix_bits = 3\n\n"
        "[[sidecars.families]]\n"
        'name = "meta"\n'
        "fields = []\n"
    )
