from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import foundinspace.octree.sources.routing as routing_module
from foundinspace.octree._cli import cli
from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.sources.routing import (
    ROUTING_INPUT_MODE_CARTESIAN,
    RoutingConfig,
    route_contributions,
)
from project_helpers import project_text


def _morton_for_node(level: int, node_id: int) -> int:
    return int(node_id) << (3 * (MORTON_BITS - level))


def _routing_table(rows: list[dict], *, shard_id: str) -> pa.Table:
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


def _write_routing_pixel(
    root: Path,
    pixel: str,
    rows: list[dict],
    *,
    part_name: str = "part.parquet",
) -> None:
    pixel_dir = root / pixel
    pixel_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _routing_table(rows, shard_id=pixel),
        pixel_dir / part_name,
        compression="zstd",
    )


def _write_routing_shard_file(root: Path, shard: str, rows: list[dict]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _routing_table(rows, shard_id=shard),
        root / f"{shard}.parquet",
        compression="zstd",
    )


def _write_raw_routing_pixel(root: Path, pixel: str, rows: list[dict]) -> None:
    pixel_dir = root / pixel
    pixel_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "source": pa.array([r["source"] for r in rows], type=pa.string()),
                "source_id": pa.array([r["source_id"] for r in rows], type=pa.string()),
                "x_icrs_pc": pa.array([r["x_icrs_pc"] for r in rows], pa.float64()),
                "y_icrs_pc": pa.array([r["y_icrs_pc"] for r in rows], pa.float64()),
                "z_icrs_pc": pa.array([r["z_icrs_pc"] for r in rows], pa.float64()),
                "mag_abs": pa.array([r["mag_abs"] for r in rows], pa.float64()),
                "teff": pa.array([r.get("teff", 5500.0) for r in rows], pa.float64()),
            }
        ),
        pixel_dir / "part.parquet",
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


def _clear_preparation_dirty(output_dir: Path) -> None:
    state_path = output_dir / "pipeline-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["dirty"]["preparation"]["group_keys"] = []
    state["dirty"]["preparation"]["deleted_routed_group_keys"] = []
    state["dirty"]["preparation"]["all"] = False
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def _routing_config(
    input_root: Path,
    output_dir: Path,
    *,
    bucket_size: int = 100,
    shard_ids: tuple[str, ...] = (),
    replace_shards: bool = False,
    input_mode: str = "pre-routed",
    force: bool = False,
) -> RoutingConfig:
    return RoutingConfig(
        input_shards_dir=input_root,
        routed_dir=output_dir,
        mag_config=MagLevelConfig(v_mag=6.5),
        bucket_rows=bucket_size,
        scan_batch_rows=10,
        fragment_target_rows=10,
        compact_after_files=0,
        shard_ids=shard_ids,
        replace_shards=replace_shards,
        input_mode=input_mode,
        force=force,
    )


def test_routing_writes_tree_manifest_and_state(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            }
        ],
    )

    out_dir = tmp_path / "routing"
    report_path = route_contributions(_routing_config(input_root, out_dir))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    manifest = json.loads((out_dir / "tree-manifest.json").read_text(encoding="utf-8"))
    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))

    assert manifest["tree_identity"] == state["tree_identity"]
    assert (
        manifest["tree_identity"]
        | {
            "limiting_magnitude": 6.5,
            "bucket_rows": 100,
            "morton_bits": MORTON_BITS,
            "row_schema_version": "routing-row-schema/v3",
        }
        == manifest["tree_identity"]
    )
    assert report["input_mode"] == "pre-routed"
    assert report["rows_after_filter"] == report["rows_in"]
    assert state["input_mode"] == "pre-routed"
    assert state["input_shards"][0]["shard_id"] == "100"
    assert state["input_shards"][0]["source_files"][0]["path"] == "100/part.parquet"
    assert [group["key"] for group in state["products"]["routed_groups"]] == [
        group["key"] for group in report["groups"]
    ]
    assert (
        state["products"]["routed_groups"][0]["checksum"]
        == report["groups"][0]["content_checksum"]
    )
    assert state["dirty"]["preparation"]["all"] is True
    assert state["dirty"]["preparation"]["group_keys"] == []
    assert state["dirty"]["preparation"]["deleted_routed_group_keys"] == []
    assert state["builds"]["routing"]["status"] == "complete"


def test_routing_fails_hard_without_required_routing_columns(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_raw_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "faint",
                "x_icrs_pc": 0.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 20.0,
            }
        ],
    )
    out_dir = tmp_path / "routing"
    config = _routing_config(input_root, out_dir)

    with pytest.raises(ValueError, match="missing required routing columns"):
        route_contributions(config)
    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
    assert state["builds"]["routing"]["status"] == "in_progress"
    assert state["builds"]["routing"]["completed_shards"] == []
    assert (out_dir / ".routing-transaction.json").is_file()


def test_routing_explicit_raw_filter_preserves_row_count_and_records_filter(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_raw_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "x_icrs_pc": 0.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 5.0,
                "teff": 5500.0,
            },
            {
                "source": "gaia",
                "source_id": "b",
                "x_icrs_pc": 1.0,
                "y_icrs_pc": 0.0,
                "z_icrs_pc": 0.0,
                "mag_abs": 6.0,
                "teff": 5000.0,
            },
        ],
    )
    out_dir = tmp_path / "routing"

    report_path = route_contributions(
        _routing_config(
            input_root,
            out_dir,
            input_mode=ROUTING_INPUT_MODE_CARTESIAN,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    manifest = json.loads((out_dir / "tree-manifest.json").read_text(encoding="utf-8"))
    fragment = next((out_dir / "tree").glob("*.parquet"))
    table = pq.read_table(fragment)
    assert report["input_mode"] == ROUTING_INPUT_MODE_CARTESIAN
    assert report["rows_in"] == 2
    assert report["rows_after_filter"] == 2
    assert report["rows_current"] == 2
    assert manifest["tree_identity"]["input_mode"] == ROUTING_INPUT_MODE_CARTESIAN
    assert {
        "x_icrs_pc",
        "y_icrs_pc",
        "z_icrs_pc",
        "teff",
        "morton_code",
        "level",
    }.issubset(table.schema.names)
    assert "render" not in table.schema.names
    assert table.column("level").to_pylist() == [13, 13]
    assert table.select(
        ["x_icrs_pc", "y_icrs_pc", "z_icrs_pc", "mag_abs", "teff"]
    ).to_pylist() == [
        {
            "x_icrs_pc": 0.0,
            "y_icrs_pc": 0.0,
            "z_icrs_pc": 0.0,
            "mag_abs": 5.0,
            "teff": 5500.0,
        },
        {
            "x_icrs_pc": 1.0,
            "y_icrs_pc": 0.0,
            "z_icrs_pc": 0.0,
            "mag_abs": 6.0,
            "teff": 5000.0,
        },
    ]


def test_routing_fails_if_input_mode_changes_row_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
        input_root,
        "100",
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
                "mag_abs": 8.0,
            },
        ],
    )

    def drop_one_row(table: pa.Table, _config: RoutingConfig) -> pa.Table:
        return table.slice(0, len(table) - 1)

    monkeypatch.setattr(routing_module, "_apply_input_mode", drop_one_row)

    with pytest.raises(ValueError, match="changed row count"):
        route_contributions(_routing_config(input_root, tmp_path / "routing"))


def test_routing_rewrites_packed_files_when_node_becomes_lower_mag_limited(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
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
    _write_routing_pixel(
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

    out_dir = tmp_path / "routing"
    report_path = route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=3,
            scan_batch_rows=10,
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
    assert not list(tree.glob("shard-*-pack-*.parquet"))
    assert len(list(tree.glob("shard-124-lim-*.parquet"))) == 1
    assert len(list((tree / "o=0").glob("shard-123-pack-*.parquet"))) == 1
    assert len(list((tree / "o=1").glob("shard-124-pack-*.parquet"))) == 1

    child_table = pq.read_table(next((tree / "o=0").glob("shard-123-pack-*.parquet")))
    assert "healpix_id" in child_table.schema.names
    assert report["group_checksum_algorithm"] == "arrow-ipc-sha256/fixed-batches-v1"
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


def test_routing_accepts_root_level_parquet_shards(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_routing_shard_file(
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

    out_dir = tmp_path / "routing"
    report_path = route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=100,
            scan_batch_rows=10,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["processed_input_shards"] == ["batch-001"]
    assert report["input_files"] == 1
    assert report["rows_current"] == 2
    assert len(list((out_dir / "tree").glob("shard-batch-001-pack-*.parquet"))) == 1
    assert _group_checksums(report).keys() == {("", "batch-001", "pack")}


def test_routing_cli_accepts_shard_option(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_routing_shard_file(
        input_root,
        "batch-001",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            }
        ],
    )
    project_path = tmp_path / "project.toml"
    out_dir = tmp_path / "routing"
    project_path.write_text(
        project_text(
            tmp_path,
            input_shards_dir=input_root,
            routed_dir=out_dir,
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli,
        ["route", "--project", str(project_path), "--shard", "batch-001"],
    )

    assert result.exit_code == 0
    assert "input_shards=1" in result.output
    report = json.loads((out_dir / "routing-report.json").read_text(encoding="utf-8"))
    assert report["processed_input_shards"] == ["batch-001"]


def test_routing_group_checksums_do_not_depend_on_fragment_boundaries(
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
    _write_routing_pixel(input_root, "200", rows)

    compact_report_path = route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=tmp_path / "compact",
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=100,
            scan_batch_rows=10,
            fragment_target_rows=10,
            compact_after_files=0,
        )
    )
    split_report_path = route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=tmp_path / "split",
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=100,
            scan_batch_rows=10,
            fragment_target_rows=2,
            compact_after_files=0,
        )
    )

    compact_report = json.loads(compact_report_path.read_text(encoding="utf-8"))
    split_report = json.loads(split_report_path.read_text(encoding="utf-8"))
    assert compact_report["current_fragment_files"] == 1
    assert split_report["current_fragment_files"] == 3
    assert _group_checksums(compact_report) == _group_checksums(split_report)


def test_routing_rewrites_nested_octant_files_without_partition_columns(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
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

    out_dir = tmp_path / "routing"
    report_path = route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=2,
            scan_batch_rows=10,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    tree = out_dir / "tree"
    assert report["rows_current"] == 3
    assert report["lower_mag_limited_nodes"] == 4
    assert (tree / "o=0" / "_LOWER_MAG_LIMITED").exists()
    assert (tree / "o=0" / "o=0" / "o=0" / "_LOWER_MAG_LIMITED").exists()
    assert (
        len(list((tree / "o=0" / "o=0" / "o=0").glob("shard-448-lim-*.parquet"))) == 1
    )


def test_routing_rolls_fragments_by_target_rows(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
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

    out_dir = tmp_path / "routing"
    report_path = route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=100,
            scan_batch_rows=10,
            fragment_target_rows=2,
            compact_after_files=0,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    files = sorted((out_dir / "tree").glob("shard-200-pack-*.parquet"))
    assert report["current_fragment_files"] == 3
    assert [pq.ParquetFile(path).metadata.num_rows for path in files] == [2, 2, 1]


def test_routing_normalizes_legacy_quality_flags_schema_drift(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    pixel_dir = input_root / "202"
    pixel_dir.mkdir(parents=True)
    common = {
        "source": pa.array(["gaia"], type=pa.string()),
        "source_id": pa.array(["a"], type=pa.string()),
        "x_icrs_pc": pa.array([1.0], type=pa.float64()),
        "y_icrs_pc": pa.array([0.0], type=pa.float64()),
        "z_icrs_pc": pa.array([0.0], type=pa.float64()),
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

    out_dir = tmp_path / "routing"
    report_path = route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=100,
            scan_batch_rows=10,
            fragment_target_rows=10,
            compact_after_files=0,
            input_mode=ROUTING_INPUT_MODE_CARTESIAN,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    fragment = next((out_dir / "tree").glob("shard-202-pack-*.parquet"))
    assert report["rows_current"] == 2
    assert pq.read_schema(fragment).field("quality_flags").type == pa.uint16()


def test_routing_rejects_out_of_range_legacy_quality_flags(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    pixel_dir = input_root / "202"
    pixel_dir.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "source": pa.array(["gaia"], type=pa.string()),
                "source_id": pa.array(["a"], type=pa.string()),
                "morton_code": pa.array([_morton_for_node(2, 0)], type=pa.uint64()),
                "level": pa.array([2], type=pa.int32()),
                "mag_abs": pa.array([8.0], type=pa.float64()),
                "quality_flags": pa.array([65_536], type=pa.int64()),
            }
        ),
        pixel_dir / "part.parquet",
    )

    with pytest.raises(ValueError, match="cannot safely normalize.*quality_flags"):
        route_contributions(
            RoutingConfig(
                input_shards_dir=input_root,
                routed_dir=tmp_path / "routing",
                mag_config=MagLevelConfig(v_mag=6.5),
                bucket_rows=100,
                scan_batch_rows=10,
            )
        )


def test_routing_still_rejects_unrelated_schema_drift(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    pixel_dir = input_root / "202"
    pixel_dir.mkdir(parents=True)
    common = {
        "source": pa.array(["gaia"], type=pa.string()),
        "source_id": pa.array(["a"], type=pa.string()),
        "morton_code": pa.array([_morton_for_node(2, 0)], type=pa.uint64()),
        "level": pa.array([2], type=pa.int32()),
        "mag_abs": pa.array([8.0], type=pa.float64()),
        "quality_flags": pa.array([1], type=pa.uint16()),
    }
    pq.write_table(
        pa.table(common | {"teff": pa.array([5_000], type=pa.float32())}),
        pixel_dir / "part-0.parquet",
    )
    pq.write_table(
        pa.table(common | {"teff": pa.array([5_000], type=pa.float64())}),
        pixel_dir / "part-1.parquet",
    )

    with pytest.raises(ValueError, match="schema changed"):
        route_contributions(
            RoutingConfig(
                input_shards_dir=input_root,
                routed_dir=tmp_path / "routing",
                mag_config=MagLevelConfig(v_mag=6.5),
                bucket_rows=100,
                scan_batch_rows=10,
            )
        )


def test_routing_compacts_repeated_small_fragments_after_lru_churn(
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
    _write_routing_pixel(input_root, "201", first_part, part_name="part-0.parquet")
    for part_idx in range(1, 3):
        _write_routing_pixel(
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

    out_dir = tmp_path / "routing"
    report_path = route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=out_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=4,
            scan_batch_rows=10,
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
    assert len(list((tree / "o=0").glob("shard-201-pack-*.parquet"))) == 1
    assert len(list((tree / "o=1").glob("shard-201-pack-*.parquet"))) == 1


def test_routing_replace_unchanged_shard_marks_no_dirty_groups(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            }
        ],
    )
    _write_routing_pixel(
        input_root,
        "101",
        [
            {
                "source": "hip",
                "source_id": "b",
                "morton_code": _morton_for_node(1, 1),
                "level": 1,
                "mag_abs": 7.1,
            }
        ],
    )
    out_dir = tmp_path / "routing"
    route_contributions(_routing_config(input_root, out_dir))
    _clear_preparation_dirty(out_dir)
    unrelated_files = sorted(
        path.relative_to(out_dir).as_posix()
        for path in (out_dir / "tree").glob("shard-101-pack-*.parquet")
    )

    report_path = route_contributions(
        _routing_config(
            input_root,
            out_dir,
            shard_ids=("100",),
            replace_shards=True,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
    assert report["replacement_mode"] is True
    assert report["processed_input_shards"] == ["100"]
    assert report["changed_group_count"] == 0
    assert report["unchanged_group_count"] == 1
    assert report["deleted_group_count"] == 0
    assert state["dirty"]["preparation"]["group_keys"] == []
    assert state["dirty"]["preparation"]["deleted_routed_group_keys"] == []
    assert (
        sorted(
            path.relative_to(out_dir).as_posix()
            for path in (out_dir / "tree").glob("shard-101-pack-*.parquet")
        )
        == unrelated_files
    )


def test_routing_replace_changed_shard_marks_changed_group(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            }
        ],
    )
    out_dir = tmp_path / "routing"
    route_contributions(_routing_config(input_root, out_dir))
    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "changed",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            }
        ],
    )

    report_path = route_contributions(
        _routing_config(
            input_root,
            out_dir,
            shard_ids=("100",),
            replace_shards=True,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
    assert report["changed_group_count"] == 1
    assert report["unchanged_group_count"] == 0
    assert report["deleted_group_count"] == 0
    assert state["dirty"]["preparation"]["group_keys"] == ["|100|pack"]


def test_routing_replace_one_row_reuses_unchanged_groups_from_same_shard(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    original_rows = [
        {
            "source": "gaia",
            "source_id": "changed-before",
            "morton_code": _morton_for_node(1, 0),
            "level": 1,
            "mag_abs": 7.0,
        },
        {
            "source": "gaia",
            "source_id": "unchanged",
            "morton_code": _morton_for_node(1, 1),
            "level": 1,
            "mag_abs": 7.1,
        },
    ]
    _write_routing_pixel(input_root, "100", original_rows)
    out_dir = tmp_path / "routing"
    route_contributions(_routing_config(input_root, out_dir, bucket_size=2))
    _clear_preparation_dirty(out_dir)

    before_state = json.loads(
        (out_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    before_groups = {
        group["key"]: group for group in before_state["products"]["routed_groups"]
    }
    unchanged_key = "o=1|100|pack"
    changed_key = "o=0|100|pack"
    assert set(before_groups) == {changed_key, unchanged_key}
    unchanged_paths = [
        out_dir / value for value in before_groups[unchanged_key]["files"]
    ]
    unchanged_stats = [
        (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in unchanged_paths
    ]
    old_changed_paths = [
        out_dir / value for value in before_groups[changed_key]["files"]
    ]

    replacement_rows = [dict(row) for row in original_rows]
    replacement_rows[0]["source_id"] = "changed-after"
    _write_routing_pixel(input_root, "100", replacement_rows)
    report_path = route_contributions(
        _routing_config(
            input_root,
            out_dir,
            bucket_size=2,
            shard_ids=("100",),
            replace_shards=True,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    after_state = json.loads(
        (out_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    after_groups = {
        group["key"]: group for group in after_state["products"]["routed_groups"]
    }
    assert report["changed_group_count"] == 1
    assert report["unchanged_group_count"] == 1
    assert report["deleted_group_count"] == 0
    assert after_state["dirty"]["preparation"]["group_keys"] == [changed_key]
    assert after_state["dirty"]["preparation"]["deleted_routed_group_keys"] == []

    assert after_groups[unchanged_key] == before_groups[unchanged_key]
    assert [
        (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in unchanged_paths
    ] == unchanged_stats
    assert after_groups[changed_key]["files"] != before_groups[changed_key]["files"]
    assert (
        after_groups[changed_key]["content_checksum"]
        != before_groups[changed_key]["content_checksum"]
    )
    assert all(not path.exists() for path in old_changed_paths)
    assert not (out_dir / ".routing-transaction.json").exists()


def test_routing_replace_recovers_uncommitted_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    initial_row = {
        "source": "gaia",
        "source_id": "before",
        "morton_code": _morton_for_node(1, 0),
        "level": 1,
        "mag_abs": 7.0,
    }
    _write_routing_pixel(input_root, "100", [initial_row])
    out_dir = tmp_path / "routing"
    route_contributions(_routing_config(input_root, out_dir))
    before_state = json.loads(
        (out_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    published_paths = [
        out_dir / value
        for value in before_state["products"]["routed_groups"][0]["files"]
    ]

    replacement_row = dict(initial_row)
    replacement_row["source_id"] = "after"
    _write_routing_pixel(input_root, "100", [replacement_row])
    original = routing_module._replacement_group_reports

    def interrupt_candidate_comparison(*args, **kwargs):
        raise RuntimeError("candidate comparison interrupted")

    monkeypatch.setattr(
        routing_module,
        "_replacement_group_reports",
        interrupt_candidate_comparison,
    )
    config = _routing_config(
        input_root,
        out_dir,
        shard_ids=("100",),
        replace_shards=True,
    )
    with pytest.raises(RuntimeError, match="candidate comparison interrupted"):
        route_contributions(config)

    assert (
        json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
        == before_state
    )
    assert all(path.is_file() for path in published_paths)
    assert (out_dir / ".routing-transaction.json").is_file()

    monkeypatch.setattr(routing_module, "_replacement_group_reports", original)
    route_contributions(config)

    after_state = json.loads(
        (out_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    assert (
        after_state["products"]["routed_groups"][0]["content_checksum"]
        != before_state["products"]["routed_groups"][0]["content_checksum"]
    )
    assert all(not path.exists() for path in published_paths)
    assert not (out_dir / ".routing-transaction.json").exists()


def test_routing_replace_records_deleted_group(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    root_row = {
        "source": "manual",
        "source_id": "root",
        "morton_code": _morton_for_node(0, 0),
        "level": 0,
        "mag_abs": 1.0,
    }
    _write_routing_pixel(
        input_root,
        "200",
        [
            root_row,
            {
                "source": "gaia",
                "source_id": "child",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 8.0,
            },
        ],
    )
    out_dir = tmp_path / "routing"
    route_contributions(_routing_config(input_root, out_dir, bucket_size=1))
    _clear_preparation_dirty(out_dir)
    _write_routing_pixel(input_root, "200", [root_row])

    report_path = route_contributions(
        _routing_config(
            input_root,
            out_dir,
            bucket_size=1,
            shard_ids=("200",),
            replace_shards=True,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
    assert report["changed_group_count"] == 0
    assert report["unchanged_group_count"] == 1
    assert report["deleted_group_count"] == 1
    assert state["dirty"]["preparation"]["group_keys"] == []
    assert state["dirty"]["preparation"]["deleted_routed_group_keys"] == ["o=0|200|lim"]


def test_routing_replace_rejects_invalid_modes_and_identity_mismatch(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            }
        ],
    )
    out_dir = tmp_path / "routing"

    with pytest.raises(ValueError, match="requires one or more --shard"):
        route_contributions(_routing_config(input_root, out_dir, replace_shards=True))
    with pytest.raises(ValueError, match="cannot be used with --force"):
        route_contributions(
            _routing_config(
                input_root,
                out_dir,
                shard_ids=("100",),
                replace_shards=True,
                force=True,
            )
        )
    with pytest.raises(FileNotFoundError, match="tree manifest"):
        route_contributions(
            _routing_config(
                input_root,
                out_dir,
                shard_ids=("100",),
                replace_shards=True,
            )
        )

    route_contributions(_routing_config(input_root, out_dir, bucket_size=100))
    with pytest.raises(ValueError, match="tree identity"):
        route_contributions(
            _routing_config(
                input_root,
                out_dir,
                bucket_size=99,
                shard_ids=("100",),
                replace_shards=True,
            )
        )


def test_routing_resumes_after_uncommitted_shard_split(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            }
        ],
    )
    _write_routing_pixel(
        input_root,
        "101",
        [
            {
                "source": "gaia",
                "source_id": "b",
                "morton_code": _morton_for_node(1, 1),
                "level": 1,
                "mag_abs": 7.1,
            }
        ],
    )
    out_dir = tmp_path / "routing"
    config = _routing_config(input_root, out_dir, bucket_size=2)
    original = routing_module._process_input_shards
    failed = False

    def fail_after_second_shard(*args, **kwargs):
        nonlocal failed
        result = original(*args, **kwargs)
        input_shards = args[1]
        if input_shards[0].shard_id == "101" and not failed:
            failed = True
            raise RuntimeError("simulated interruption")
        return result

    monkeypatch.setattr(
        routing_module,
        "_process_input_shards",
        fail_after_second_shard,
    )
    with pytest.raises(RuntimeError, match="simulated interruption"):
        route_contributions(config)

    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
    assert state["builds"]["routing"]["completed_shards"] == ["100"]
    committed_path = out_dir / state["products"]["routed_groups"][0]["files"][0]
    assert committed_path.is_file()
    assert (out_dir / ".routing-transaction.json").is_file()

    monkeypatch.setattr(routing_module, "_process_input_shards", original)
    report_path = route_contributions(config)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
    assert report["rows_current"] == 2
    assert report["lower_mag_limited_nodes"] == 1
    assert state["builds"]["routing"]["status"] == "complete"
    assert state["builds"]["routing"]["completed_shards"] == ["100", "101"]
    assert not (out_dir / ".routing-transaction.json").exists()
    assert not committed_path.exists()


def test_routing_resumes_group_checksums(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    for shard_id, node_id in (("100", 0), ("101", 1)):
        _write_routing_pixel(
            input_root,
            shard_id,
            [
                {
                    "source": "gaia",
                    "source_id": shard_id,
                    "morton_code": _morton_for_node(1, node_id),
                    "level": 1,
                    "mag_abs": 7.0,
                }
            ],
        )
    out_dir = tmp_path / "routing"
    config = _routing_config(input_root, out_dir)
    original = routing_module._routing_group_checksum
    calls = 0

    def fail_on_second_group(paths):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("checksum interruption")
        return original(paths)

    monkeypatch.setattr(
        routing_module,
        "_routing_group_checksum",
        fail_on_second_group,
    )
    with pytest.raises(RuntimeError, match="checksum interruption"):
        route_contributions(config)

    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
    assert state["builds"]["routing"]["status"] == "checksumming"
    checkpoints = list((out_dir / ".routing-checksums").glob("*.json"))
    assert len(checkpoints) == 1

    resumed_calls = 0

    def count_resumed_group(paths):
        nonlocal resumed_calls
        resumed_calls += 1
        return original(paths)

    monkeypatch.setattr(
        routing_module,
        "_routing_group_checksum",
        count_resumed_group,
    )
    route_contributions(config)

    state = json.loads((out_dir / "pipeline-state.json").read_text(encoding="utf-8"))
    assert resumed_calls == 1
    assert state["builds"]["routing"]["status"] == "complete"
    assert all(
        "content_checksum" in group for group in state["products"]["routed_groups"]
    )


def test_route_help_contains_selection_and_replacement_options() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["route", "--help"])
    assert result.exit_code == 0
    assert "--project" in result.output
    assert "--shard" in result.output
    assert "--replace-shards" in result.output
    assert "--max-shards" in result.output
    assert "adaptive contribution buckets" in result.output
