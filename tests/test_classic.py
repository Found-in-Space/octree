from __future__ import annotations

import json
import struct
from pathlib import Path
from uuid import UUID

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import foundinspace.octree.classic_materialization as classic_materialization
from foundinspace.octree.assembly import BuildPlan, build_intermediates
from foundinspace.octree.classic import (
    ClassicBuildConfig,
    build_classic_artifacts,
)
from foundinspace.octree.combine import CombinePlan, combine_octree
from foundinspace.octree.combine.records import PackedDescriptorFields
from foundinspace.octree.config import (
    MORTON_BITS,
    WORLD_CENTER,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.encoding.render import encode_render_records
from foundinspace.octree.identifiers_order import (
    IdentifiersOrderReader,
    combine_identifiers_order,
)
from foundinspace.octree.identifiers_order import (
    read_header as read_identifiers_header,
)
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.reader import OctreeReader, Point, read_header
from foundinspace.octree.sources.stage00 import (
    STAGE00_INPUT_FILTER_RAW_CARTESIAN,
    Stage00Config,
    run_stage00,
)
from foundinspace.octree.sources.stage01 import Stage01Config, run_stage01

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
        "mag_abs": pa.array(
            [row.get("mag_abs", 7.0) for row in rows],
            type=pa.float64(),
        ),
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
    renders = encode_render_records(
        morton_codes=morton_codes,
        positions=np.array(
            [[row["x_icrs_pc"], row["y_icrs_pc"], row["z_icrs_pc"]] for row in rows],
            dtype=np.float64,
        ),
        mag_abs=np.array([row["mag_abs"] for row in rows], dtype=np.float64),
        teff=np.array([row.get("teff", 5800.0) for row in rows], dtype=np.float64),
        levels=levels,
    )
    legacy_rows = [
        row
        | {
            "level": int(level),
            "render": render.tobytes(),
        }
        for row, level, render in zip(rows, levels, renders, strict=True)
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


def _build_stages(
    tmp_path: Path,
    rows: list[dict],
    *,
    input_filter: str = "none",
) -> tuple[Path, Path, Path]:
    input_root = tmp_path / "input"
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    _write_input(
        input_root,
        rows,
        include_routing=input_filter == "none",
    )
    run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=stage00_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_size=100,
            batch_size=10,
            fragment_target_rows=10,
            compact_after_files=0,
            input_filter=input_filter,
        )
    )
    run_stage01(
        Stage01Config(
            stage00_output_dir=stage00_dir,
            output_dir=stage01_dir,
            v_mag=6.5,
            bucket_size=100,
            input_filter=input_filter,
            batch_size=10,
            fragment_target_rows=10,
        )
    )
    return input_root, stage00_dir, stage01_dir


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
    _input_root, stage00_dir, stage01_dir = _build_stages(tmp_path, rows)
    classic_output = tmp_path / "classic.octree"
    classic_identifiers = tmp_path / "classic.identifiers.order"

    build_classic_artifacts(
        ClassicBuildConfig(
            stage00_output_dir=stage00_dir,
            stage01_output_dir=stage01_dir,
            output_path=classic_output,
            identifiers_order_path=classic_identifiers,
            mag_limit=6.5,
            max_level=8,
            batch_size=10,
            max_open_files=4,
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
    combine_octree(
        legacy_render_manifest,
        legacy_output,
        plan=CombinePlan(max_open_files=4),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=_DATASET_UUID,
        ),
    )
    combine_identifiers_order(
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
            "mag_abs": 8.0,
        },
        {
            "source_id": "a",
            "x_icrs_pc": node_zero_center[0],
            "y_icrs_pc": node_zero_center[1],
            "z_icrs_pc": node_zero_center[2],
            "mag_abs": 7.0,
        },
    ]
    _input_root, stage00_dir, stage01_dir = _build_stages(
        tmp_path,
        rows,
        input_filter=STAGE00_INPUT_FILTER_RAW_CARTESIAN,
    )
    stage01_file = next(stage01_dir.rglob("*.parquet"))
    assert "render" not in pq.read_schema(stage01_file).names
    output_path = tmp_path / "stars.octree"
    identifiers_path = tmp_path / "identifiers.order"

    result = build_classic_artifacts(
        ClassicBuildConfig(
            stage00_output_dir=stage00_dir,
            stage01_output_dir=stage01_dir,
            output_path=output_path,
            identifiers_order_path=identifiers_path,
            mag_limit=6.5,
            max_level=14,
            batch_size=10,
            max_open_files=4,
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


def test_classic_build_merges_sorted_stage01_groups_in_canonical_order(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
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
    run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=stage00_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_size=100,
            batch_size=2,
            fragment_target_rows=1,
            compact_after_files=0,
        )
    )
    run_stage01(
        Stage01Config(
            stage00_output_dir=stage00_dir,
            output_dir=stage01_dir,
            v_mag=6.5,
            bucket_size=100,
            batch_size=2,
            fragment_target_rows=1,
        )
    )
    identifiers_path = tmp_path / "identifiers.order"

    result = build_classic_artifacts(
        ClassicBuildConfig(
            stage00_output_dir=stage00_dir,
            stage01_output_dir=stage01_dir,
            output_path=tmp_path / "stars.octree",
            identifiers_order_path=identifiers_path,
            mag_limit=6.5,
            max_level=14,
            batch_size=2,
            max_open_files=2,
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
        ("gaia", "a"),
        ("gaia", "b"),
        ("gaia", "m"),
        ("gaia", "z"),
    ]


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
    _input_root, stage00_dir, stage01_dir = _build_stages(tmp_path, rows)
    identifiers_path = tmp_path / "identifiers.order"

    result = build_classic_artifacts(
        ClassicBuildConfig(
            stage00_output_dir=stage00_dir,
            stage01_output_dir=stage01_dir,
            output_path=tmp_path / "stars.octree",
            identifiers_order_path=identifiers_path,
            mag_limit=6.5,
            max_level=14,
            batch_size=2,
            max_open_files=2,
        ),
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    assert result.folded_row_count == 4
    with IdentifiersOrderReader(identifiers_path) as reader:
        cells = list(reader.iter_cells())
    assert len(cells) == 1
    assert cells[0][1] == [
        ("gaia", "b"),
        ("gaia", "a"),
        ("gaia", "y"),
        ("gaia", "z"),
    ]


def test_classic_build_reuses_completed_sorted_materialization(tmp_path: Path) -> None:
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
    _input_root, stage00_dir, stage01_dir = _build_stages(tmp_path, rows)
    config = ClassicBuildConfig(
        stage00_output_dir=stage00_dir,
        stage01_output_dir=stage01_dir,
        output_path=tmp_path / "stars.octree",
        identifiers_order_path=tmp_path / "identifiers.order",
        mag_limit=6.5,
        max_level=1,
        batch_size=10,
        max_open_files=2,
    )
    build_classic_artifacts(
        config,
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )
    intermediates_dir = stage01_dir / "classic-intermediates"
    mtimes = {
        path.name: path.stat().st_mtime_ns
        for path in intermediates_dir.iterdir()
        if path.is_file()
    }

    build_classic_artifacts(
        config,
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    assert {
        path.name: path.stat().st_mtime_ns
        for path in intermediates_dir.iterdir()
        if path.is_file()
    } == mtimes


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
    _input_root, stage00_dir, stage01_dir = _build_stages(tmp_path, rows)
    config = ClassicBuildConfig(
        stage00_output_dir=stage00_dir,
        stage01_output_dir=stage01_dir,
        output_path=tmp_path / "stars.octree",
        identifiers_order_path=tmp_path / "identifiers.order",
        mag_limit=6.5,
        max_level=2,
        batch_size=1,
        max_open_files=2,
        partition_from_level=1,
        partition_prefix_bits=1,
    )
    original = classic_materialization._materialize_partition
    calls = 0

    def fail_second_partition(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated partition failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        classic_materialization,
        "_materialize_partition",
        fail_second_partition,
    )
    with pytest.raises(RuntimeError, match="simulated partition failure"):
        build_classic_artifacts(config)

    work_dir = stage01_dir / ".classic-intermediates.work"
    state = json.loads(
        (work_dir / "classic-work-state.json").read_text(encoding="utf-8")
    )
    assert len(state["completed_partitions"]) == 1
    monkeypatch.setattr(
        classic_materialization,
        "_materialize_partition",
        original,
    )
    monkeypatch.setattr(
        classic_materialization,
        "_normalize_group",
        lambda *_args, **_kwargs: pytest.fail(
            "completed Stage 01 groups should be reused"
        ),
    )

    build_classic_artifacts(
        config,
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    assert not work_dir.exists()
    manifest = json.loads(
        (stage01_dir / "classic-intermediates" / "render-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    level_two = next(row for row in manifest["levels"] if row["level"] == 2)
    assert [shard["prefix"] for shard in level_two["shards"]] == [0, 1]


def test_classic_build_rejects_stage01_without_raw_fields(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
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
                "mag_abs": pa.array([7.0], pa.float64()),
            }
        ),
        shard_dir / "part.parquet",
    )
    run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=stage00_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_size=100,
            batch_size=10,
            fragment_target_rows=10,
            compact_after_files=0,
        )
    )
    run_stage01(
        Stage01Config(
            stage00_output_dir=stage00_dir,
            output_dir=stage01_dir,
            v_mag=6.5,
            bucket_size=100,
            batch_size=10,
            fragment_target_rows=10,
        )
    )

    with pytest.raises(ValueError, match="requires raw Stage 01 fields"):
        build_classic_artifacts(
            ClassicBuildConfig(
                stage00_output_dir=stage00_dir,
                stage01_output_dir=stage01_dir,
                output_path=tmp_path / "stars.octree",
                identifiers_order_path=tmp_path / "identifiers.order",
                mag_limit=6.5,
                max_level=14,
                batch_size=10,
                max_open_files=4,
            )
        )
