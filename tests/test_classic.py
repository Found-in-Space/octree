from __future__ import annotations

import struct
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from foundinspace.octree.assembly import BuildPlan, build_intermediates
from foundinspace.octree.classic import (
    ClassicBuildConfig,
    build_classic_artifacts,
    promote_render_to_ancestor,
)
from foundinspace.octree.combine import CombinePlan, combine_octree
from foundinspace.octree.combine.records import PackedDescriptorFields
from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.identifiers_order import (
    IdentifiersOrderReader,
    combine_identifiers_order,
)
from foundinspace.octree.identifiers_order import (
    read_header as read_identifiers_header,
)
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.reader import read_header
from foundinspace.octree.sources.stage00 import Stage00Config, run_stage00
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


def _write_input(root: Path, rows: list[dict]) -> None:
    shard_dir = root / "100"
    shard_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "source": pa.array(
                    [row.get("source", "gaia") for row in rows],
                    type=pa.string(),
                ),
                "source_id": pa.array(
                    [row["source_id"] for row in rows],
                    type=pa.string(),
                ),
                "morton_code": pa.array(
                    [row["morton_code"] for row in rows],
                    type=pa.uint64(),
                ),
                "render": pa.array(
                    [row["render"] for row in rows],
                    type=pa.binary(16),
                ),
                "level": pa.array(
                    [row["level"] for row in rows],
                    type=pa.int32(),
                ),
                "mag_abs": pa.array(
                    [row.get("mag_abs", 7.0) for row in rows],
                    type=pa.float64(),
                ),
            }
        ),
        shard_dir / "part.parquet",
        compression="zstd",
    )


def _build_stages(tmp_path: Path, rows: list[dict]) -> tuple[Path, Path, Path]:
    input_root = tmp_path / "input"
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    _write_input(input_root, rows)
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
    return input_root, stage00_dir, stage01_dir


def test_promote_render_to_direct_ancestor_preserves_absolute_position() -> None:
    source_level = 15
    target_level = 14
    source_node_id = 0b101
    raw = _render(0.0, 0.0, 0.0, magnitude=-123, teff=44, pad=9)

    promoted = promote_render_to_ancestor(
        raw,
        morton_code=_morton_for_node(source_level, source_node_id),
        source_level=source_level,
        target_level=target_level,
    )

    x, y, z, magnitude, teff, pad = _RENDER.unpack(promoted)
    assert (x, y, z) == pytest.approx((0.5, -0.5, 0.5))
    assert (magnitude, teff, pad) == (-123, 44, 9)


def test_classic_build_matches_legacy_builder_when_rows_are_within_cap(
    tmp_path: Path,
) -> None:
    rows = [
        {
            "source_id": "root",
            "morton_code": _morton_for_node(0, 0),
            "render": _render(0.1, 0.2, 0.3),
            "level": 0,
            "mag_abs": 1.0,
        },
        {
            "source_id": "a",
            "morton_code": _morton_for_node(1, 0),
            "render": _render(-0.5, -0.25, 0.0),
            "level": 1,
            "mag_abs": 7.0,
        },
        {
            "source_id": "b",
            "morton_code": _morton_for_node(1, 7),
            "render": _render(0.5, 0.25, 0.0),
            "level": 1,
            "mag_abs": 8.0,
        },
    ]
    input_root, stage00_dir, stage01_dir = _build_stages(tmp_path, rows)
    classic_output = tmp_path / "classic.octree"
    classic_identifiers = tmp_path / "classic.identifiers.order"

    build_classic_artifacts(
        ClassicBuildConfig(
            stage00_output_dir=stage00_dir,
            stage01_output_dir=stage01_dir,
            output_path=classic_output,
            identifiers_order_path=classic_identifiers,
            mag_limit=6.5,
            max_level=1,
            batch_size=10,
            max_open_files=4,
        ),
        dataset_uuid=_DATASET_UUID,
        identifiers_uuid=_IDENTIFIERS_UUID,
    )

    legacy_intermediates = tmp_path / "legacy-intermediates"
    legacy_render_manifest = build_intermediates(
        (input_root / "**" / "*.parquet").as_posix(),
        legacy_intermediates,
        plan=BuildPlan(
            max_level=1,
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
    rows = [
        {
            "source_id": "b",
            "morton_code": _morton_for_node(source_level, 1),
            "render": _render(0.0, 0.0, 0.0),
            "level": source_level,
            "mag_abs": 8.0,
        },
        {
            "source_id": "a",
            "morton_code": _morton_for_node(source_level, 0),
            "render": _render(0.0, 0.0, 0.0),
            "level": source_level,
            "mag_abs": 7.0,
        },
    ]
    _input_root, stage00_dir, stage01_dir = _build_stages(tmp_path, rows)
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
