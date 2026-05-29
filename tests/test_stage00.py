from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.sources.stage00 import Stage00Config, run_stage00


def _morton_for_node(level: int, node_id: int) -> int:
    return int(node_id) << (3 * (MORTON_BITS - level))


def _stage00_table(rows: list[dict], *, shard_id: str) -> pa.Table:
    return pa.table(
        {
            "source": pa.array([r["source"] for r in rows], type=pa.string()),
            "source_id": pa.array([r["source_id"] for r in rows], type=pa.string()),
            "morton_code": pa.array(
                [r["morton_code"] for r in rows],
                type=pa.uint64(),
            ),
            "render": pa.array([b"\x00" * 16 for _ in rows], type=pa.binary(16)),
            "level": pa.array([r["level"] for r in rows], type=pa.int32()),
            "mag_abs": pa.array([r["mag_abs"] for r in rows], type=pa.float64()),
            "healpix_id": pa.array([shard_id for _ in rows], type=pa.string()),
        }
    )


def _write_stage00_pixel(
    root: Path,
    pixel: str,
    rows: list[dict],
    *,
    part_name: str = "part.parquet",
) -> None:
    pixel_dir = root / pixel
    pixel_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _stage00_table(rows, shard_id=pixel),
        pixel_dir / part_name,
        compression="zstd",
    )


def _write_stage00_shard_file(root: Path, shard: str, rows: list[dict]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _stage00_table(rows, shard_id=shard),
        root / f"{shard}.parquet",
        compression="zstd",
    )


def _group_checksums(report: dict) -> dict[tuple[str, str, str], tuple[int, str]]:
    return {
        (row["node_path"], row["input_shard_id"], row["kind"]): (
            row["row_count"],
            row["content_checksum"],
        )
        for row in report["groups"]
    }


def test_stage00_rewrites_packed_files_when_node_becomes_lower_mag_limited(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
        input_root,
        "123",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(2, 0),
                "level": 2,
                "mag_abs": 7.0,
            },
            {
                "source": "gaia",
                "source_id": "b",
                "morton_code": _morton_for_node(2, 1),
                "level": 2,
                "mag_abs": 7.1,
            },
        ],
    )
    _write_stage00_pixel(
        input_root,
        "124",
        [
            {
                "source": "hip",
                "source_id": "root",
                "morton_code": _morton_for_node(0, 0),
                "level": 0,
                "mag_abs": 1.0,
            },
            {
                "source": "hip",
                "source_id": "c",
                "morton_code": _morton_for_node(2, 8),
                "level": 2,
                "mag_abs": 7.2,
            },
        ],
    )

    out_dir = tmp_path / "stage00"
    report_path = run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5, max_level=2),
            max_level=2,
            bucket_size=3,
            batch_size=10,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["rows_in"] == 4
    assert report["rows_current"] == 4
    assert report["staging_nodes"] == 3
    assert report["lower_mag_limited_nodes"] == 1
    assert report["fragment_files_deleted_on_split"] == 2

    tree = out_dir / "tree"
    assert (tree / "_LOWER_MAG_LIMITED").exists()
    assert not list(tree.glob("hp*-pack-*.parquet"))
    assert len(list(tree.glob("hp124-lim-*.parquet"))) == 1
    assert len(list((tree / "o=0").glob("hp123-pack-*.parquet"))) == 1
    assert len(list((tree / "o=1").glob("hp124-pack-*.parquet"))) == 1

    child_table = pq.read_table(next((tree / "o=0").glob("hp123-pack-*.parquet")))
    assert "healpix_id" not in child_table.schema.names
    assert report["group_checksum_algorithm"] == "arrow-ipc-sha256/v0"
    assert {
        (row["node_path"], row["input_shard_id"], row["kind"])
        for row in report["groups"]
    } == {
        ("", "124", "lim"),
        ("o=0", "123", "pack"),
        ("o=1", "124", "pack"),
    }
    for row in report["groups"]:
        assert row["content_checksum"].startswith("sha256:")
        assert row["row_count"] > 0
        assert row["file_count"] == len(row["files"])


def test_stage00_accepts_root_level_parquet_shards(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_stage00_shard_file(
        input_root,
        "batch-001",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            },
            {
                "source": "gaia",
                "source_id": "b",
                "morton_code": _morton_for_node(1, 1),
                "level": 1,
                "mag_abs": 7.1,
            },
        ],
    )

    out_dir = tmp_path / "stage00"
    report_path = run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5, max_level=1),
            max_level=1,
            bucket_size=100,
            batch_size=10,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["processed_healpix"] == ["batch-001"]
    assert report["input_files"] == 1
    assert report["rows_current"] == 2
    assert len(list((out_dir / "tree").glob("hpbatch-001-pack-*.parquet"))) == 1
    assert _group_checksums(report).keys() == {("", "batch-001", "pack")}


def test_stage00_group_checksums_do_not_depend_on_fragment_boundaries(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    rows = [
        {
            "source": "gaia",
            "source_id": str(idx),
            "morton_code": _morton_for_node(2, idx % 2),
            "level": 2,
            "mag_abs": 8.0 + idx,
        }
        for idx in range(5)
    ]
    _write_stage00_pixel(input_root, "200", rows)

    compact_report_path = run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=tmp_path / "compact",
            mag_config=MagLevelConfig(v_mag=6.5, max_level=2),
            max_level=2,
            bucket_size=100,
            batch_size=10,
            fragment_target_rows=10,
            compact_after_files=0,
        )
    )
    split_report_path = run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=tmp_path / "split",
            mag_config=MagLevelConfig(v_mag=6.5, max_level=2),
            max_level=2,
            bucket_size=100,
            batch_size=10,
            fragment_target_rows=2,
            compact_after_files=0,
        )
    )

    compact_report = json.loads(compact_report_path.read_text(encoding="utf-8"))
    split_report = json.loads(split_report_path.read_text(encoding="utf-8"))
    assert compact_report["current_fragment_files"] == 1
    assert split_report["current_fragment_files"] == 3
    assert _group_checksums(compact_report) == _group_checksums(split_report)


def test_stage00_rewrites_nested_octant_files_without_partition_columns(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
        input_root,
        "448",
        [
            {
                "source": "gaia",
                "source_id": str(idx),
                "morton_code": _morton_for_node(3, 0),
                "level": 3,
                "mag_abs": 8.0 + idx,
            }
            for idx in range(3)
        ],
    )

    out_dir = tmp_path / "stage00"
    report_path = run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5, max_level=3),
            max_level=3,
            bucket_size=2,
            batch_size=10,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    tree = out_dir / "tree"
    assert report["rows_current"] == 3
    assert report["lower_mag_limited_nodes"] == 4
    assert (tree / "o=0" / "_LOWER_MAG_LIMITED").exists()
    assert (tree / "o=0" / "o=0" / "o=0" / "_LOWER_MAG_LIMITED").exists()
    assert len(list((tree / "o=0" / "o=0" / "o=0").glob("hp448-lim-*.parquet"))) == 1


def test_stage00_rolls_fragments_by_target_rows(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
        input_root,
        "200",
        [
            {
                "source": "gaia",
                "source_id": str(idx),
                "morton_code": _morton_for_node(2, 0),
                "level": 2,
                "mag_abs": 8.0 + idx,
            }
            for idx in range(5)
        ],
    )

    out_dir = tmp_path / "stage00"
    report_path = run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5, max_level=2),
            max_level=2,
            bucket_size=100,
            batch_size=10,
            fragment_target_rows=2,
            compact_after_files=0,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    files = sorted((out_dir / "tree").glob("hp200-pack-*.parquet"))
    assert report["current_fragment_files"] == 3
    assert [pq.ParquetFile(path).metadata.num_rows for path in files] == [2, 2, 1]


def test_stage00_normalizes_schema_for_rolling_writers(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    pixel_dir = input_root / "202"
    pixel_dir.mkdir(parents=True)
    common = {
        "source": pa.array(["gaia"], type=pa.string()),
        "source_id": pa.array(["a"], type=pa.string()),
        "morton_code": pa.array([_morton_for_node(2, 0)], type=pa.uint64()),
        "render": pa.array([b"\x00" * 16], type=pa.binary(16)),
        "level": pa.array([2], type=pa.int32()),
        "mag_abs": pa.array([8.0], type=pa.float64()),
    }
    pq.write_table(
        pa.table(common | {"quality_flags": pa.array([1], type=pa.uint16())}),
        pixel_dir / "part-0.parquet",
    )
    pq.write_table(
        pa.table(common | {"quality_flags": pa.array([2], type=pa.int64())}),
        pixel_dir / "part-1.parquet",
    )

    out_dir = tmp_path / "stage00"
    report_path = run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5, max_level=2),
            max_level=2,
            bucket_size=100,
            batch_size=10,
            fragment_target_rows=10,
            compact_after_files=0,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    files = sorted((out_dir / "tree").glob("hp202-pack-*.parquet"))
    table = pq.read_table(files[0])
    assert report["current_fragment_files"] == 1
    assert table.schema.field("quality_flags").type == pa.int64()
    assert table.schema.field("source").type == pa.large_string()


def test_stage00_compacts_repeated_small_fragments_after_lru_churn(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    first_part = [
        {
            "source": "manual",
            "source_id": "root",
            "morton_code": _morton_for_node(0, 0),
            "level": 0,
            "mag_abs": 1.0,
        },
        {
            "source": "gaia",
            "source_id": "a0",
            "morton_code": _morton_for_node(2, 0),
            "level": 2,
            "mag_abs": 8.0,
        },
        {
            "source": "gaia",
            "source_id": "b0",
            "morton_code": _morton_for_node(2, 8),
            "level": 2,
            "mag_abs": 8.1,
        },
        {
            "source": "gaia",
            "source_id": "c0",
            "morton_code": _morton_for_node(2, 16),
            "level": 2,
            "mag_abs": 8.2,
        },
    ]
    _write_stage00_pixel(input_root, "201", first_part, part_name="part-0.parquet")
    for part_idx in range(1, 3):
        _write_stage00_pixel(
            input_root,
            "201",
            [
                {
                    "source": "gaia",
                    "source_id": f"a{part_idx}",
                    "morton_code": _morton_for_node(2, 0),
                    "level": 2,
                    "mag_abs": 8.0 + part_idx,
                },
                {
                    "source": "gaia",
                    "source_id": f"b{part_idx}",
                    "morton_code": _morton_for_node(2, 8),
                    "level": 2,
                    "mag_abs": 8.1 + part_idx,
                },
            ],
            part_name=f"part-{part_idx}.parquet",
        )

    out_dir = tmp_path / "stage00"
    report_path = run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5, max_level=2),
            max_level=2,
            bucket_size=4,
            batch_size=10,
            fragment_target_rows=10,
            max_open_writers=1,
            compact_after_files=2,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    tree = out_dir / "tree"
    assert report["fragment_files_deleted_on_compaction"] == 6
    assert report["compaction_rewrites"] == 2
    assert report["compaction_input_files"] == 6
    assert report["compaction_output_files"] == 2
    assert report["current_fragment_files"] == 4
    assert len(list((tree / "o=0").glob("hp201-pack-*.parquet"))) == 1
    assert len(list((tree / "o=1").glob("hp201-pack-*.parquet"))) == 1


def test_stage00_help_contains_packed_options() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-00", "--help"])
    assert result.exit_code == 0
    assert "--bucket-size" in result.output
    assert "--fragment-target-rows" in result.output
    assert "--max-open-writers" in result.output
    assert "--compact-after-files" in result.output
    assert "--healpix" in result.output
    assert "adaptive Stage 00 staging buckets" in result.output
