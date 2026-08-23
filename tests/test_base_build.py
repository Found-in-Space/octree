from __future__ import annotations

import builtins
import json
import math
import struct
import threading
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import foundinspace.octree.base_build as base_build_module
import foundinspace.octree.materialization.pipeline as materialization
import foundinspace.octree.packing.streaming_index as streaming_index
from foundinspace.octree.assembly import BuildPlan, build_intermediates
from foundinspace.octree.base_build import (
    BaseBuildConfig,
    build_base_artifacts,
)
from foundinspace.octree.config import (
    MORTON_BITS,
    WORLD_CENTER,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.encoding.render import encode_render_records
from foundinspace.octree.identifiers_order import (
    IdentifiersOrderReader,
    pack_identifiers_order,
)
from foundinspace.octree.identifiers_order import (
    read_header as read_identifiers_header,
)
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.packing import (
    IndexEmissionStrategy,
    PackingPlan,
    pack_octree,
)
from foundinspace.octree.packing.records import (
    SHARD_HDR_FMT,
    SHARD_NODE_V2_FMT,
    PackedDescriptorFields,
)
from foundinspace.octree.reader import IndexNavigator, OctreeReader, Point, read_header
from foundinspace.octree.sources.preparation import (
    PreparationConfig,
    prepare_contributions,
)
from foundinspace.octree.sources.routing import (
    ROUTING_INPUT_MODE_CARTESIAN,
    RoutingConfig,
    route_contributions,
)
from magnitude_helpers import represented_magnitude_for_level


def test_final_pair_lock_preserves_live_temp_across_work_dirs(tmp_path: Path) -> None:
    output = tmp_path / "products" / "stars.octree"
    identifiers = tmp_path / "products" / "identifiers.order"
    base = BaseBuildConfig(
        routed_dir=tmp_path / "routing",
        prepared_dir=tmp_path / "preparation",
        output_path=output,
        identifiers_order_path=identifiers,
        limiting_magnitude=6.5,
        build_work_dir=tmp_path / "work-a",
    )
    competing = replace(base, build_work_dir=tmp_path / "work-b")
    started = threading.Event()
    finished = threading.Event()

    def cleanup_from_competing_build() -> None:
        started.set()
        with base_build_module._final_pair_locks(competing):
            base_build_module._clean_incomplete_final_products(output, identifiers)
        finished.set()

    with base_build_module._final_pair_locks(base):
        live = output.with_name(f".{output.name}.live.tmp")
        live.write_bytes(b"still in use")
        thread = threading.Thread(target=cleanup_from_competing_build)
        thread.start()
        assert started.wait(timeout=1)
        assert not finished.wait(timeout=0.1)
        assert live.is_file()

    thread.join(timeout=2)
    assert finished.is_set()
    assert not live.exists()


def test_emission_strategy_does_not_change_final_artifact_identity(
    tmp_path: Path,
) -> None:
    (tmp_path / "routing").mkdir()
    (tmp_path / "preparation").mkdir()
    base = BaseBuildConfig(
        routed_dir=tmp_path / "routing",
        prepared_dir=tmp_path / "preparation",
        output_path=tmp_path / "stars.octree",
        identifiers_order_path=tmp_path / "identifiers.order",
        limiting_magnitude=6.5,
    )
    forward = replace(
        base,
        index_emission_strategy=IndexEmissionStrategy.FORWARD,
    )

    assert base_build_module._final_base_identity(
        base, input_identity="sha256:input"
    ) == base_build_module._final_base_identity(forward, input_identity="sha256:input")
    with pytest.raises(ValueError, match="temp-pwrite-batched or forward"):
        replace(
            base,
            index_emission_strategy=IndexEmissionStrategy.TEMP_PWRITE_PER_CHILD,
        ).validate()


_RENDER = struct.Struct("<fffhBB")
_DATASET_UUID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_IDENTIFIERS_UUID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _morton_for_node(level: int, node_id: int) -> int:
    return int(node_id) << (3 * (MORTON_BITS - level))


def _render(
    x: float,
    y: float,
    z: float,
    *,
    magnitude: int = 700,
    teff: int = 100,
    pad: int = 0,
) -> bytes:
    return _RENDER.pack(x, y, z, magnitude, teff, pad)


def _write_compact_run(path: Path, rows: list[tuple[float | None, str, int]]) -> None:
    row_count = len(rows)
    pq.write_table(
        pa.table(
            {
                "final_level": pa.array([14] * row_count, type=pa.int16()),
                "final_node_id": pa.array([0] * row_count, type=pa.uint64()),
                "mag_abs": pa.array([row[0] for row in rows], type=pa.float64()),
                "source": pa.array(["gaia"] * row_count, type=pa.string()),
                "source_id": pa.array([row[1] for row in rows], type=pa.string()),
                "render": pa.array(
                    [bytes([row[2]]) * 16 for row in rows],
                    type=pa.binary(16),
                ),
            },
            schema=materialization._COMPACT_SCHEMA,
        ),
        path,
        compression="zstd",
    )


def _write_compact_cells(
    path: Path,
    rows: list[tuple[int, int, float, str]],
) -> None:
    pq.write_table(
        pa.table(
            {
                "final_level": pa.array([row[0] for row in rows], pa.int16()),
                "final_node_id": pa.array([row[1] for row in rows], pa.uint64()),
                "mag_abs": pa.array([row[2] for row in rows], pa.float64()),
                "source": pa.array(["gaia"] * len(rows), pa.string()),
                "source_id": pa.array([row[3] for row in rows], pa.string()),
                "render": pa.array(
                    [bytes([index % 256]) * 16 for index in range(len(rows))],
                    pa.binary(16),
                ),
            },
            schema=materialization._COMPACT_SCHEMA,
        ),
        path,
        compression="zstd",
    )


def _merged_compact_runs(
    paths: list[Path],
    *,
    spill_dir: Path,
    batch_size: int,
) -> pa.Table:
    return pa.concat_tables(
        [
            batch
            for _key, batch in materialization._iter_merged_batches(
                paths,
                batch_rows=batch_size,
                spill_dir=spill_dir,
            )
        ],
        promote_options="none",
    )


def _node_center(level: int, node_id: int) -> tuple[float, float, float]:
    grid = [0, 0, 0]
    for bit in range(level):
        for axis in range(3):
            grid[axis] |= ((node_id >> (3 * bit + axis)) & 1) << bit
    width = 2.0 * WORLD_HALF_SIZE_PC / (2**level)
    return tuple(
        float(WORLD_CENTER[axis] - WORLD_HALF_SIZE_PC + (value + 0.5) * width)
        for axis, value in enumerate(grid)
    )


def _write_input(
    root: Path,
    rows: list[dict],
    *,
    include_routing: bool = True,
    shard_id: str = "100",
) -> None:
    shard_dir = root / shard_id
    shard_dir.mkdir(parents=True, exist_ok=True)
    magnitudes = [row.get("mag_abs", 7.0) for row in rows]
    if include_routing:
        magnitudes = [
            represented_magnitude_for_level(row["level"], magnitude)
            for row, magnitude in zip(rows, magnitudes, strict=True)
        ]
    columns = {
        "source": pa.array(
            [row.get("source", "gaia") for row in rows],
            type=pa.string(),
        ),
        "source_id": pa.array(
            [row["source_id"] for row in rows],
            type=pa.string(),
        ),
        "x_icrs_pc": pa.array([row["x_icrs_pc"] for row in rows], pa.float64()),
        "y_icrs_pc": pa.array([row["y_icrs_pc"] for row in rows], pa.float64()),
        "z_icrs_pc": pa.array([row["z_icrs_pc"] for row in rows], pa.float64()),
        "mag_abs": pa.array(magnitudes, type=pa.float64()),
    }
    if any("teff" in row for row in rows):
        columns["teff"] = pa.array(
            [row.get("teff") for row in rows],
            pa.float64(),
        )
    if include_routing:
        columns["morton_code"] = pa.array(
            [row["morton_code"] for row in rows],
            type=pa.uint64(),
        )
        columns["level"] = pa.array(
            [row["level"] for row in rows],
            type=pa.int32(),
        )
    pq.write_table(
        pa.table(columns),
        shard_dir / "part.parquet",
        compression="zstd",
    )


def _write_legacy_input(root: Path, rows: list[dict], *, max_level: int) -> None:
    levels = np.array(
        [min(int(row["level"]), max_level) for row in rows],
        dtype=np.int32,
    )
    morton_codes = np.array([row["morton_code"] for row in rows], dtype=np.uint64)
    magnitudes = np.array(
        [
            represented_magnitude_for_level(row["level"], row["mag_abs"])
            for row in rows
        ],
        dtype=np.float64,
    )
    renders = encode_render_records(
        morton_codes=morton_codes,
        positions=np.array(
            [[row["x_icrs_pc"], row["y_icrs_pc"], row["z_icrs_pc"]] for row in rows],
            dtype=np.float64,
        ),
        mag_abs=magnitudes,
        teff=np.array([row.get("teff", 5800.0) for row in rows], dtype=np.float64),
        levels=levels,
    )
    legacy_rows = [
        row
        | {
            "level": int(level),
            "render": render.tobytes(),
            "mag_abs": float(magnitude),
        }
        for row, level, render, magnitude in zip(
            rows,
            levels,
            renders,
            magnitudes,
            strict=True,
        )
    ]
    shard_dir = root / "100"
    shard_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "source": pa.array(
                    [row.get("source", "gaia") for row in legacy_rows],
                    pa.string(),
                ),
                "source_id": pa.array(
                    [row["source_id"] for row in legacy_rows],
                    pa.string(),
                ),
                "morton_code": pa.array(
                    [row["morton_code"] for row in legacy_rows],
                    pa.uint64(),
                ),
                "render": pa.array(
                    [row["render"] for row in legacy_rows],
                    pa.binary(16),
                ),
                "level": pa.array(
                    [row["level"] for row in legacy_rows],
                    pa.int32(),
                ),
                "mag_abs": pa.array(
                    [row["mag_abs"] for row in legacy_rows],
                    pa.float64(),
                ),
            }
        ),
        shard_dir / "part.parquet",
        compression="zstd",
    )


def _build_products(
    tmp_path: Path,
    rows: list[dict],
    *,
    input_mode: str = "pre-routed",
) -> tuple[Path, Path, Path]:
    input_root = tmp_path / "input"
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    _write_input(
        input_root,
        rows,
        include_routing=input_mode == "pre-routed",
    )
    route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=routing_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=100,
            scan_batch_rows=10,
            fragment_target_rows=10,
            compact_after_files=0,
            input_mode=input_mode,
        )
    )
    prepare_contributions(
        PreparationConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            limiting_magnitude=6.5,
            bucket_rows=100,
            input_mode=input_mode,
            batch_rows=10,
            fragment_target_rows=10,
        )
    )
    return input_root, routing_dir, preparation_dir


def test_classic_build_matches_legacy_builder_when_rows_are_within_cap(
    tmp_path: Path,
) -> None:
    rows = [
        {
            "source_id": "root",
            "morton_code": _morton_for_node(0, 0),
            "level": 0,
            "mag_abs": 1.0,
            "x_icrs_pc": 20_000.0,
            "y_icrs_pc": 40_000.0,
            "z_icrs_pc": 60_000.0,
        },
        {
            "source_id": "a",
            "morton_code": _morton_for_node(1, 0),
            "level": 1,
            "mag_abs": 7.0,
            "x_icrs_pc": -150_000.0,
            "y_icrs_pc": -125_000.0,
            "z_icrs_pc": -100_000.0,
        },
        {
            "source_id": "b",
            "morton_code": _morton_for_node(1, 7),
            "level": 1,
            "mag_abs": 8.0,
            "x_icrs_pc": 150_000.0,
            "y_icrs_pc": 125_000.0,
            "z_icrs_pc": 100_000.0,
        },
        {
            "source_id": "deep-a",
            "morton_code": _morton_for_node(8, 0),
            "level": 8,
            "mag_abs": 9.0,
            "x_icrs_pc": _node_center(8, 0)[0],
            "y_icrs_pc": _node_center(8, 0)[1],
            "z_icrs_pc": _node_center(8, 0)[2],
        },
        {
            "source_id": "deep-b",
            "morton_code": _morton_for_node(8, 32 << 18),
            "level": 8,
            "mag_abs": 9.1,
            "x_icrs_pc": _node_center(8, 32 << 18)[0],
            "y_icrs_pc": _node_center(8, 32 << 18)[1],
            "z_icrs_pc": _node_center(8, 32 << 18)[2],
        },
    ]
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    classic_output = tmp_path / "base_build_module.octree"
    classic_identifiers = tmp_path / "base_build_module.identifiers.order"

    build_base_artifacts(
        BaseBuildConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            output_path=classic_output,
            identifiers_order_path=classic_identifiers,
            limiting_magnitude=6.5,
            max_level=8,
            batch_rows=10,
            max_open_files=4,
            star_format_version=1,
        ),
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    legacy_input_root = tmp_path / "legacy-input"
    _write_legacy_input(legacy_input_root, rows, max_level=8)
    legacy_intermediates = tmp_path / "legacy-intermediates"
    legacy_render_manifest = build_intermediates(
        (legacy_input_root / "**" / "*.parquet").as_posix(),
        legacy_intermediates,
        plan=BuildPlan(
            max_level=8,
            deep_shard_from_level=99,
            deep_prefix_bits=3,
            batch_size=10,
            mag_limit=6.5,
        ),
    )
    legacy_output = tmp_path / "legacy.octree"
    legacy_identifiers = tmp_path / "legacy.identifiers.order"
    pack_octree(
        legacy_render_manifest,
        legacy_output,
        plan=PackingPlan(max_open_files=4),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=_DATASET_UUID,
        ),
    )
    pack_identifiers_order(
        legacy_intermediates / "identifiers-manifest.json",
        legacy_identifiers,
        parent_dataset_uuid=_DATASET_UUID,
        artifact_uuid=_IDENTIFIERS_UUID,
    )

    assert classic_output.read_bytes() == legacy_output.read_bytes()
    assert classic_identifiers.read_bytes() == legacy_identifiers.read_bytes()


def test_classic_build_folds_deep_rows_into_capped_node(tmp_path: Path) -> None:
    source_level = 15
    node_zero_center = _node_center(source_level, 0)
    node_one_center = _node_center(source_level, 1)
    rows = [
        {
            "source_id": "b",
            "x_icrs_pc": node_one_center[0],
            "y_icrs_pc": node_one_center[1],
            "z_icrs_pc": node_one_center[2],
            "mag_abs": 8.5,
        },
        {
            "source_id": "a",
            "x_icrs_pc": node_zero_center[0],
            "y_icrs_pc": node_zero_center[1],
            "z_icrs_pc": node_zero_center[2],
            "mag_abs": 8.0,
        },
    ]
    _input_root, routing_dir, preparation_dir = _build_products(
        tmp_path,
        rows,
        input_mode=ROUTING_INPUT_MODE_CARTESIAN,
    )
    preparation_file = next(preparation_dir.rglob("*.parquet"))
    assert "render" not in pq.read_schema(preparation_file).names
    output_path = tmp_path / "stars.octree"
    identifiers_path = tmp_path / "identifiers.order"

    result = build_base_artifacts(
        BaseBuildConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            output_path=output_path,
            identifiers_order_path=identifiers_path,
            limiting_magnitude=6.5,
            max_level=14,
            batch_rows=10,
            max_open_files=4,
            star_format_version=1,
        ),
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    assert result.row_count == 2
    assert result.folded_row_count == 2
    assert result.cell_count == 1
    header = read_header(output_path)
    assert header.max_level == 14
    assert header.dataset_uuid == _DATASET_UUID
    identifiers_header = read_identifiers_header(identifiers_path)
    assert identifiers_header.parent_dataset_uuid == _DATASET_UUID
    with IdentifiersOrderReader(identifiers_path) as reader:
        cells = list(reader.iter_cells())
    assert len(cells) == 1
    record, identities = cells[0]
    assert (record.level, record.node_id, record.star_count) == (14, 0, 2)
    assert identities == [("gaia", "a"), ("gaia", "b")]
    with OctreeReader(output_path) as reader:
        stars = sorted(
            reader.stars_within_distance(Point(0.0, 0.0, 0.0), 1_000_000.0),
            key=lambda star: star.magnitude,
        )
    assert len(stars) == 2
    for star, expected in zip(stars, (node_zero_center, node_one_center), strict=True):
        assert (
            star.position.x,
            star.position.y,
            star.position.z,
        ) == pytest.approx(expected, abs=1e-5)


def test_classic_v2_packs_terminal_and_preserves_order_and_positions(
    tmp_path: Path,
) -> None:
    child_center = _node_center(2, 0)
    rows = [
        {
            "source_id": "later",
            "morton_code": _morton_for_node(0, 0),
            "level": 0,
            "mag_abs": 2.0,
            "x_icrs_pc": 20_000.0,
            "y_icrs_pc": 40_000.0,
            "z_icrs_pc": 60_000.0,
        },
        {
            "source_id": "first",
            "morton_code": _morton_for_node(2, 0),
            "level": 2,
            "mag_abs": 1.0,
            "x_icrs_pc": child_center[0],
            "y_icrs_pc": child_center[1],
            "z_icrs_pc": child_center[2],
        },
    ]
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    output_path = tmp_path / "stars.octree"
    identifiers_path = tmp_path / "identifiers.order"

    result = build_base_artifacts(
        BaseBuildConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            output_path=output_path,
            identifiers_order_path=identifiers_path,
            limiting_magnitude=6.5,
            max_level=2,
            batch_rows=1,
            max_open_files=2,
            star_format_version=2,
            terminal_waterline=2,
        ),
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    assert result.row_count == 2
    assert result.cell_count == 1
    header = read_header(output_path)
    assert header.version == 2
    assert header.max_level == 2
    with IndexNavigator(output_path, header) as navigator:
        [root] = list(navigator.root_entries())
    assert root.level == 0
    assert root.star_count == 2
    assert root.is_terminal is True
    assert root.is_leaf is True
    assert root.brightest_level == 0

    with IdentifiersOrderReader(identifiers_path) as reader:
        [(record, identities)] = list(reader.iter_cells())
    assert (record.level, record.node_id, record.star_count) == (0, 0, 2)
    assert identities == [("gaia", "later"), ("gaia", "first")]

    with OctreeReader(output_path) as reader:
        stars = sorted(
            reader.stars_within_distance(Point(0.0, 0.0, 0.0), 1_000_000.0),
            key=lambda star: star.magnitude,
        )
    assert len(stars) == 2
    assert (
        stars[0].position.x,
        stars[0].position.y,
        stars[0].position.z,
    ) == pytest.approx((20_000.0, 40_000.0, 60_000.0), abs=0.01)
    assert (
        stars[1].position.x,
        stars[1].position.y,
        stars[1].position.z,
    ) == pytest.approx(child_center, abs=1e-5)


def test_classic_v2_counts_index_only_and_nested_terminal_nodes(
    tmp_path: Path,
) -> None:
    rows = []
    for source_id, node_id in (("left", 0), ("right", 63)):
        center = _node_center(2, node_id)
        rows.append(
            {
                "source_id": source_id,
                "morton_code": _morton_for_node(2, node_id),
                "level": 2,
                "mag_abs": 7.0,
                "x_icrs_pc": center[0],
                "y_icrs_pc": center[1],
                "z_icrs_pc": center[2],
            }
        )
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    output_path = tmp_path / "stars.octree"

    build_base_artifacts(
        BaseBuildConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            output_path=output_path,
            identifiers_order_path=tmp_path / "identifiers.order",
            limiting_magnitude=6.5,
            max_level=2,
            batch_rows=1,
            max_open_files=2,
            star_format_version=2,
            terminal_waterline=1,
        )
    )

    header = read_header(output_path)
    with IndexNavigator(output_path, header) as navigator:
        [root] = list(navigator.root_entries())
        children = [
            navigator.get_child(root, octant)
            for octant in range(8)
            if root.child_mask & (1 << octant)
        ]
    assert root.star_count == 0
    assert root.is_terminal is False
    assert root.brightest_level == 2
    assert len(children) == 2
    assert all(child is not None for child in children)
    assert all(child.star_count == 1 for child in children if child is not None)
    assert all(child.is_terminal for child in children if child is not None)
    assert all(child.brightest_level == 2 for child in children if child is not None)


def test_classic_v2_preserves_exact_level_for_nonterminal_folded_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    natural_level = MORTON_BITS
    center = _node_center(natural_level, 0)
    rows = [
        {
            "source_id": source_id,
            "morton_code": _morton_for_node(natural_level, 0),
            "level": natural_level,
            "mag_abs": magnitude,
            "x_icrs_pc": center[0],
            "y_icrs_pc": center[1],
            "z_icrs_pc": center[2],
        }
        for source_id, magnitude in (("a", 7.0), ("b", 8.0))
    ]
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    output_path = tmp_path / "stars.octree"

    monkeypatch.setattr(
        streaming_index,
        "_iter_parent_masks",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("v2 packer recomputed authoritative topology")
        ),
    )

    build_base_artifacts(
        BaseBuildConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            output_path=output_path,
            identifiers_order_path=tmp_path / "identifiers.order",
            limiting_magnitude=6.5,
            max_level=2,
            batch_rows=1,
            max_open_files=2,
            star_format_version=2,
            terminal_waterline=1,
        )
    )

    header = read_header(output_path)
    with open(output_path, "rb") as fp:
        fp.seek(header.index_offset)
        shard = SHARD_HDR_FMT.unpack(fp.read(SHARD_HDR_FMT.size))
        fp.seek(shard[22])
        root_record = SHARD_NODE_V2_FMT.unpack(fp.read(SHARD_NODE_V2_FMT.size))
    assert root_record[4] & 0xF0 == 0
    assert root_record[5] == natural_level
    with IndexNavigator(output_path, header) as navigator:
        [root] = list(navigator.root_entries())
        level_one = navigator.get_child(root, 0)
        assert level_one is not None
        folded = navigator.get_child(level_one, 0)
    assert root.brightest_level == natural_level
    assert root.flags & 0xF0 == 0
    assert level_one.brightest_level == natural_level
    assert folded is not None
    assert folded.level == 2
    assert folded.star_count == 2
    assert folded.is_terminal is False
    assert folded.brightest_level == natural_level
    assert folded.flags & 0xF0 == 0


def test_classic_build_merges_sorted_preparation_groups_in_canonical_order(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    level14_center = _node_center(14, 0)
    level15_center = _node_center(15, 0)
    level16_center = _node_center(16, 0)
    _write_input(
        input_root,
        [
            {
                "source_id": "z",
                "morton_code": _morton_for_node(15, 0),
                "level": 15,
                "mag_abs": 8.0,
                "x_icrs_pc": level15_center[0],
                "y_icrs_pc": level15_center[1],
                "z_icrs_pc": level15_center[2],
            }
        ],
        shard_id="100",
    )
    _write_input(
        input_root,
        [
            {
                "source_id": "a",
                "morton_code": _morton_for_node(16, 0),
                "level": 16,
                "mag_abs": 7.0,
                "x_icrs_pc": level16_center[0],
                "y_icrs_pc": level16_center[1],
                "z_icrs_pc": level16_center[2],
            },
            {
                "source_id": "m",
                "morton_code": _morton_for_node(14, 0),
                "level": 14,
                "mag_abs": 7.5,
                "x_icrs_pc": level14_center[0],
                "y_icrs_pc": level14_center[1],
                "z_icrs_pc": level14_center[2],
            },
        ],
        shard_id="101",
    )
    _write_input(
        input_root,
        [
            {
                "source_id": "b",
                "morton_code": _morton_for_node(15, 0),
                "level": 15,
                "mag_abs": 7.25,
                "x_icrs_pc": level15_center[0],
                "y_icrs_pc": level15_center[1],
                "z_icrs_pc": level15_center[2],
            }
        ],
        shard_id="102",
    )
    route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=routing_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=100,
            scan_batch_rows=2,
            fragment_target_rows=1,
            compact_after_files=0,
        )
    )
    prepare_contributions(
        PreparationConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            limiting_magnitude=6.5,
            bucket_rows=100,
            batch_rows=2,
            fragment_target_rows=1,
        )
    )
    identifiers_path = tmp_path / "identifiers.order"

    result = build_base_artifacts(
        BaseBuildConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            output_path=tmp_path / "stars.octree",
            identifiers_order_path=identifiers_path,
            limiting_magnitude=6.5,
            max_level=14,
            batch_rows=2,
            max_open_files=2,
            star_format_version=1,
        ),
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    assert result.row_count == 4
    assert result.folded_row_count == 3
    with IdentifiersOrderReader(identifiers_path) as reader:
        cells = list(reader.iter_cells())
    assert len(cells) == 1
    assert cells[0][1] == [
        ("gaia", "m"),
        ("gaia", "b"),
        ("gaia", "z"),
        ("gaia", "a"),
    ]


def test_merged_run_coalesces_many_tiny_cells_into_bounded_row_groups(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    output = tmp_path / "merged.parquet"
    rows = [(14, node, float(node), f"id-{node:03d}") for node in range(37)]
    _write_compact_cells(first, rows[::2])
    _write_compact_cells(second, rows[1::2])
    # Exercise piece compaction without allowing the piece count to dictate
    # physical row groups.
    monkeypatch.setattr(
        materialization,
        "MATERIALIZATION_MERGE_WRITE_MAX_PIECES",
        3,
    )

    materialization._write_merged_run([first, second], output, batch_rows=10)

    parquet = pq.ParquetFile(output)
    assert parquet.metadata.num_row_groups == 4
    assert [
        parquet.metadata.row_group(index).num_rows
        for index in range(parquet.metadata.num_row_groups)
    ] == [10, 10, 10, 7]
    assert parquet.metadata.row_group(0).column(0).statistics is None
    assert parquet.read().column("source_id").to_pylist() == [row[3] for row in rows]


def test_merged_run_preserves_cell_across_physical_row_group_boundaries(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.parquet"
    output = tmp_path / "merged.parquet"
    rows = [(14, 0, float(index), f"zero-{index:02d}") for index in range(13)] + [
        (14, 1, float(index), f"one-{index:02d}") for index in range(4)
    ]
    _write_compact_cells(source, rows)

    materialization._write_merged_run([source], output, batch_rows=5)

    parquet = pq.ParquetFile(output)
    assert [
        parquet.metadata.row_group(index).num_rows
        for index in range(parquet.metadata.num_row_groups)
    ] == [5, 5, 5, 2]
    keyed = list(
        materialization._iter_merged_batches([output], batch_rows=4, spill_dir=tmp_path)
    )
    assert [key for key, _table in keyed] == [
        (14, 0),
        (14, 0),
        (14, 0),
        (14, 0),
        (14, 1),
        (14, 1),
    ]
    assert [
        sum(len(table) for key, table in keyed if key == wanted)
        for wanted in [(14, 0), (14, 1)]
    ] == [13, 4]


def test_merged_run_byte_cap_splits_oversized_variable_width_cell(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.parquet"
    output = tmp_path / "merged.parquet"
    rows = [(14, 0, float(index), f"id-{index}-" + "x" * 180) for index in range(12)]
    _write_compact_cells(source, rows)
    monkeypatch.setattr(
        materialization,
        "MATERIALIZATION_MERGE_WRITE_MAX_BYTES",
        700,
    )

    materialization._write_merged_run([source], output, batch_rows=12)

    parquet = pq.ParquetFile(output)
    assert parquet.metadata.num_row_groups > 1
    assert (
        max(
            parquet.metadata.row_group(index).num_rows
            for index in range(parquet.metadata.num_row_groups)
        )
        < 12
    )
    assert parquet.read().column("source_id").to_pylist() == [row[3] for row in rows]


def test_classic_overlap_spill_matches_in_memory_sort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write_compact_run(
        first,
        [
            (-math.inf, "negative", 1),
            (1.0, "same", 2),
            (1.0, "same", 3),
            (math.nan, "nan", 4),
            (None, "null", 5),
        ],
    )
    _write_compact_run(
        second,
        [
            (0.0, "zero", 11),
            (1.0, "same", 12),
            (math.nan, "nan", 13),
            (None, "null", 14),
        ],
    )
    spill_dir = tmp_path / "spill"
    spill_dir.mkdir()
    paths = [first, second]

    in_memory = _merged_compact_runs(
        paths,
        spill_dir=spill_dir,
        batch_size=100,
    )
    original_external_sort = materialization._iter_externally_sorted_overlap
    external_sort_calls = 0

    def track_external_sort(*args, **kwargs):
        nonlocal external_sort_calls
        external_sort_calls += 1
        yield from original_external_sort(*args, **kwargs)

    monkeypatch.setattr(
        materialization,
        "MATERIALIZATION_OVERLAP_IN_MEMORY_MAX_BYTES",
        1,
    )
    monkeypatch.setattr(
        materialization,
        "_iter_externally_sorted_overlap",
        track_external_sort,
    )
    spilled = _merged_compact_runs(
        paths,
        spill_dir=spill_dir,
        batch_size=100,
    )

    assert external_sort_calls == 1
    assert spilled.schema == in_memory.schema
    assert (
        spilled.column("render").to_pylist() == in_memory.column("render").to_pylist()
    )
    assert spilled.column("render").to_pylist() == [
        bytes([value]) * 16 for value in (1, 11, 2, 3, 12, 4, 13, 5, 14)
    ]
    assert [
        "null" if value is None else "nan" if math.isnan(value) else value
        for value in spilled.column("mag_abs").to_pylist()
    ] == [
        -math.inf,
        0.0,
        1.0,
        1.0,
        1.0,
        "nan",
        "nan",
        "null",
        "null",
    ]
    assert list(spill_dir.iterdir()) == []


def test_classic_build_externally_merges_folded_group_batches(
    tmp_path: Path,
) -> None:
    rows = []
    for level, magnitude, source_id in (
        (15, 9.0, "z"),
        (15, 8.0, "y"),
        (16, 7.0, "a"),
        (16, 6.0, "b"),
    ):
        center = _node_center(level, 0)
        rows.append(
            {
                "source_id": source_id,
                "morton_code": _morton_for_node(level, 0),
                "level": level,
                "mag_abs": magnitude,
                "x_icrs_pc": center[0],
                "y_icrs_pc": center[1],
                "z_icrs_pc": center[2],
            }
        )
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    identifiers_path = tmp_path / "identifiers.order"

    result = build_base_artifacts(
        BaseBuildConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            output_path=tmp_path / "stars.octree",
            identifiers_order_path=identifiers_path,
            limiting_magnitude=6.5,
            max_level=14,
            batch_rows=2,
            max_open_files=2,
            star_format_version=1,
        ),
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    assert result.folded_row_count == 4
    with IdentifiersOrderReader(identifiers_path) as reader:
        cells = list(reader.iter_cells())
    assert len(cells) == 1
    assert cells[0][1] == [
        ("gaia", "y"),
        ("gaia", "z"),
        ("gaia", "b"),
        ("gaia", "a"),
    ]


def test_classic_build_reuses_completed_sorted_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = [
        {
            "source_id": "a",
            "morton_code": _morton_for_node(1, 0),
            "level": 1,
            "mag_abs": 7.0,
            "x_icrs_pc": -100_000.0,
            "y_icrs_pc": -100_000.0,
            "z_icrs_pc": -100_000.0,
        }
    ]
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    config = BaseBuildConfig(
        routed_dir=routing_dir,
        prepared_dir=preparation_dir,
        output_path=tmp_path / "stars.octree",
        identifiers_order_path=tmp_path / "identifiers.order",
        limiting_magnitude=6.5,
        max_level=1,
        batch_rows=10,
        max_open_files=2,
        star_format_version=1,
    )
    build_base_artifacts(
        config,
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )
    intermediates_dir = (
        preparation_dir / base_build_module.DEFAULT_MATERIALIZED_DIR_NAME
    )
    mtimes = {
        path.name: path.stat().st_mtime_ns
        for path in intermediates_dir.iterdir()
        if path.is_file()
    }
    final_before = {
        path: (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in (config.output_path, config.identifiers_order_path)
    }
    stale_render = tmp_path / ".stars.octree.dead.tmp"
    stale_identifiers = tmp_path / ".identifiers.order.dead.tmp"
    stale_render.write_bytes(b"partial")
    stale_identifiers.write_bytes(b"partial")

    original_open = builtins.open

    def reject_intermediate_open(path, *args, **kwargs):
        name = str(path)
        if name.endswith((".index", ".payload", ".ident-index", ".ident-payload")):
            raise AssertionError(f"final no-op opened immutable intermediate: {path}")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", reject_intermediate_open)
    monkeypatch.setattr(
        base_build_module,
        "pack_octree",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("final no-op invoked render packing")
        ),
    )
    monkeypatch.setattr(
        base_build_module,
        "pack_identifiers_order",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("final no-op invoked identifiers packing")
        ),
    )

    reused = build_base_artifacts(config)

    assert {
        path.name: path.stat().st_mtime_ns
        for path in intermediates_dir.iterdir()
        if path.is_file()
    } == mtimes
    assert reused.dataset_uuid == _DATASET_UUID
    assert reused.identifiers_uuid == _IDENTIFIERS_UUID
    assert not stale_render.exists()
    assert not stale_identifiers.exists()
    assert {
        path: (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in (config.output_path, config.identifiers_order_path)
    } == final_before


def test_classic_build_supports_isolated_intermediates_and_work_dirs(
    tmp_path: Path,
) -> None:
    rows = [
        {
            "source_id": "a",
            "morton_code": _morton_for_node(1, 0),
            "level": 1,
            "mag_abs": 7.0,
            "x_icrs_pc": -100_000.0,
            "y_icrs_pc": -100_000.0,
            "z_icrs_pc": -100_000.0,
        }
    ]
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    v1_config = BaseBuildConfig(
        routed_dir=routing_dir,
        prepared_dir=preparation_dir,
        output_path=tmp_path / "stars-v1.octree",
        identifiers_order_path=tmp_path / "identifiers-v1.order",
        limiting_magnitude=6.5,
        max_level=1,
        batch_rows=10,
        max_open_files=2,
        star_format_version=1,
    )
    build_base_artifacts(v1_config)
    v1_intermediates = preparation_dir / base_build_module.DEFAULT_MATERIALIZED_DIR_NAME
    assert (v1_intermediates / "render-manifest.json").is_file()

    v2_intermediates = tmp_path / "v2-intermediates"
    v2_work = tmp_path / "v2-work"
    v2_config = replace(
        v1_config,
        output_path=tmp_path / "stars-v2.octree",
        identifiers_order_path=tmp_path / "identifiers-v2.order",
        star_format_version=2,
        terminal_waterline=1,
        materialized_dir=v2_intermediates,
        build_work_dir=v2_work,
    )
    build_base_artifacts(v2_config)

    assert (v1_intermediates / "render-manifest.json").is_file()
    assert (v2_intermediates / "render-manifest.json").is_file()
    assert v2_config.output_path.is_file()
    assert v2_config.identifiers_order_path.is_file()
    assert (v2_work / materialization.MATERIALIZATION_WORK_STATE_NAME).is_file()
    assert (v2_work / "runs").is_dir()
    assert (v2_work / "partition-cache").is_dir()


@pytest.mark.parametrize("damage", ("missing-reference", "missing-level-file"))
def test_classic_v2_rebuilds_damaged_published_terminal_map(
    tmp_path: Path,
    damage: str,
) -> None:
    center = _node_center(2, 0)
    rows = [
        {
            "source_id": "a",
            "morton_code": _morton_for_node(2, 0),
            "level": 2,
            "mag_abs": 7.0,
            "x_icrs_pc": center[0],
            "y_icrs_pc": center[1],
            "z_icrs_pc": center[2],
        }
    ]
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    config = BaseBuildConfig(
        routed_dir=routing_dir,
        prepared_dir=preparation_dir,
        output_path=tmp_path / "stars.octree",
        identifiers_order_path=tmp_path / "identifiers.order",
        limiting_magnitude=6.5,
        max_level=2,
        batch_rows=10,
        max_open_files=2,
        star_format_version=2,
        terminal_waterline=1,
    )
    build_base_artifacts(
        config,
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )
    intermediates_dir = (
        preparation_dir / base_build_module.DEFAULT_MATERIALIZED_DIR_NAME
    )
    render_manifest_path = intermediates_dir / "render-manifest.json"
    render_manifest = json.loads(render_manifest_path.read_text(encoding="utf-8"))
    terminal_map_path = intermediates_dir / render_manifest["terminal_map_path"]
    terminal_map = json.loads(terminal_map_path.read_text(encoding="utf-8"))
    terminal_level_path = intermediates_dir / terminal_map["levels"][0]["path"]

    if damage == "missing-reference":
        del render_manifest["terminal_map_path"]
        render_manifest_path.write_text(
            json.dumps(render_manifest, indent=2) + "\n",
            encoding="utf-8",
        )
    else:
        terminal_level_path.unlink()

    build_base_artifacts(
        config,
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    rebuilt_manifest = json.loads(render_manifest_path.read_text(encoding="utf-8"))
    rebuilt_map_path = intermediates_dir / rebuilt_manifest["terminal_map_path"]
    rebuilt_map = json.loads(rebuilt_map_path.read_text(encoding="utf-8"))
    rebuilt_level_path = intermediates_dir / rebuilt_map["levels"][0]["path"]
    assert rebuilt_level_path.is_file()
    header = read_header(config.output_path)
    with IndexNavigator(config.output_path, header) as navigator:
        [root] = list(navigator.root_entries())
    assert root.is_terminal is True
    assert root.star_count == 1


def test_classic_build_resumes_completed_spatial_partitions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_zero_center = _node_center(2, 0)
    node_thirty_two_center = _node_center(2, 32)
    rows = [
        {
            "source_id": "a",
            "morton_code": _morton_for_node(2, 0),
            "level": 2,
            "mag_abs": 7.0,
            "x_icrs_pc": node_zero_center[0],
            "y_icrs_pc": node_zero_center[1],
            "z_icrs_pc": node_zero_center[2],
        },
        {
            "source_id": "b",
            "morton_code": _morton_for_node(2, 32),
            "level": 2,
            "mag_abs": 7.1,
            "x_icrs_pc": node_thirty_two_center[0],
            "y_icrs_pc": node_thirty_two_center[1],
            "z_icrs_pc": node_thirty_two_center[2],
        },
    ]
    _input_root, routing_dir, preparation_dir = _build_products(tmp_path, rows)
    config = BaseBuildConfig(
        routed_dir=routing_dir,
        prepared_dir=preparation_dir,
        output_path=tmp_path / "stars.octree",
        identifiers_order_path=tmp_path / "identifiers.order",
        limiting_magnitude=6.5,
        max_level=2,
        batch_rows=1,
        max_open_files=2,
        partition_from_level=1,
        partition_prefix_bits=1,
        star_format_version=1,
    )
    original = materialization._materialize_partition
    calls = 0

    def fail_second_partition(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated partition failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        materialization,
        "_materialize_partition",
        fail_second_partition,
    )
    with pytest.raises(RuntimeError, match="simulated partition failure"):
        build_base_artifacts(config)

    work_dir = preparation_dir / base_build_module.DEFAULT_BUILD_WORK_DIR_NAME
    state = json.loads(
        (work_dir / materialization.MATERIALIZATION_WORK_STATE_NAME).read_text(
            encoding="utf-8"
        )
    )
    assert len(state["completed_partitions"]) == 1
    monkeypatch.setattr(
        materialization,
        "_materialize_partition",
        original,
    )
    monkeypatch.setattr(
        materialization,
        "_normalize_group",
        lambda *_args, **_kwargs: pytest.fail(
            "completed Preparation groups should be reused"
        ),
    )

    build_base_artifacts(
        config,
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    assert (work_dir / materialization.MATERIALIZATION_WORK_STATE_NAME).is_file()
    assert (work_dir / "partition-cache").is_dir()
    manifest = json.loads(
        (
            preparation_dir
            / base_build_module.DEFAULT_MATERIALIZED_DIR_NAME
            / "render-manifest.json"
        ).read_text(encoding="utf-8")
    )
    level_two = next(row for row in manifest["levels"] if row["level"] == 2)
    assert [shard["prefix"] for shard in level_two["shards"]] == [0, 1]


def test_classic_build_rejects_preparation_without_raw_fields(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    rows = [
        {
            "source_id": "legacy",
            "morton_code": _morton_for_node(1, 0),
            "render": _render(0.0, 0.0, 0.0),
            "level": 1,
            "mag_abs": 7.0,
        }
    ]
    shard_dir = input_root / "100"
    shard_dir.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "source": pa.array(["gaia"], pa.string()),
                "source_id": pa.array(["legacy"], pa.string()),
                "morton_code": pa.array([rows[0]["morton_code"]], pa.uint64()),
                "render": pa.array([rows[0]["render"]], pa.binary(16)),
                "level": pa.array([1], pa.int32()),
                "mag_abs": pa.array(
                    [represented_magnitude_for_level(1, 7.0)],
                    pa.float64(),
                ),
            }
        ),
        shard_dir / "part.parquet",
    )
    route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=routing_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=100,
            scan_batch_rows=10,
            fragment_target_rows=10,
            compact_after_files=0,
        )
    )
    prepare_contributions(
        PreparationConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            limiting_magnitude=6.5,
            bucket_rows=100,
            batch_rows=10,
            fragment_target_rows=10,
        )
    )

    with pytest.raises(ValueError, match="requires raw Preparation fields"):
        build_base_artifacts(
            BaseBuildConfig(
                routed_dir=routing_dir,
                prepared_dir=preparation_dir,
                output_path=tmp_path / "stars.octree",
                identifiers_order_path=tmp_path / "identifiers.order",
                limiting_magnitude=6.5,
                max_level=14,
                batch_rows=10,
                max_open_files=4,
                star_format_version=1,
            )
        )
