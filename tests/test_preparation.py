from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import foundinspace.octree.sources.preparation as preparation_module
from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.sources.preparation import (
    PreparationConfig,
    prepare_contributions,
)
from foundinspace.octree.sources.routing import RoutingConfig, route_contributions


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
            "render": pa.array(
                [bytes([idx]) * 16 for idx, _row in enumerate(rows)],
                type=pa.binary(16),
            ),
            "level": pa.array([r["level"] for r in rows], type=pa.int32()),
            "mag_abs": pa.array([r["mag_abs"] for r in rows], type=pa.float64()),
            "healpix_id": pa.array([shard_id for _ in rows], type=pa.string()),
        }
    )


def _write_routing_pixel(
    root: Path,
    pixel: str,
    rows: list[dict],
) -> None:
    pixel_dir = root / pixel
    pixel_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _routing_table(rows, shard_id=pixel),
        pixel_dir / "part.parquet",
        compression="zstd",
    )


def _routing_config(
    input_root: Path,
    output_dir: Path,
    *,
    bucket_size: int = 100,
    fragment_target_rows: int = 10,
    shard_ids: tuple[str, ...] = (),
    replace_shards: bool = False,
) -> RoutingConfig:
    return RoutingConfig(
        input_shards_dir=input_root,
        routed_dir=output_dir,
        mag_config=MagLevelConfig(v_mag=6.5),
        bucket_rows=bucket_size,
        scan_batch_rows=10,
        fragment_target_rows=fragment_target_rows,
        compact_after_files=0,
        shard_ids=shard_ids,
        replace_shards=replace_shards,
    )


def _preparation_config(
    routed_dir: Path,
    output_dir: Path,
    *,
    bucket_size: int = 100,
    fragment_target_rows: int = 10,
    force: bool = False,
) -> PreparationConfig:
    return PreparationConfig(
        routed_dir=routed_dir,
        prepared_dir=output_dir,
        limiting_magnitude=6.5,
        bucket_rows=bucket_size,
        batch_rows=10,
        fragment_target_rows=fragment_target_rows,
        force=force,
    )


def test_preparation_first_run_writes_sorted_groups_and_state(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "z",
                "morton_code": _morton_for_node(1, 1),
                "level": 1,
                "mag_abs": 6.0,
            },
            {
                "source": "gaia",
                "source_id": "b",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 8.0,
            },
            {
                "source": "gaia",
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            },
            {
                "source": "hip",
                "source_id": "root",
                "morton_code": _morton_for_node(0, 0),
                "level": 0,
                "mag_abs": 1.0,
            },
        ],
    )
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))

    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir, fragment_target_rows=2)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    files = sorted((preparation_dir / "tree").glob("shard-100-pack-sorted-*.parquet"))
    sorted_table = pa.concat_tables([pq.read_table(path) for path in files])
    assert sorted_table.column("source_id").to_pylist() == ["root", "a", "b", "z"]
    assert "_preparation_final_node_id" not in sorted_table.schema.names
    assert "healpix_id" in sorted_table.schema.names
    assert len(files) == 2
    assert report["processed_group_count"] == 1
    assert report["changed_group_count"] == 1
    assert report["in_memory_sort_group_count"] == 1
    assert report["external_sort_group_count"] == 0
    assert state["dirty"]["preparation"]["group_keys"] == []
    assert state["dirty"]["preparation"]["deleted_routed_group_keys"] == []
    assert state["products"]["prepared_groups"][0]["files"] == [
        path.relative_to(preparation_dir).as_posix() for path in files
    ]
    assert state["products"]["prepared_groups"][0]["natural_max_level"] == 1
    assert "final_nodes" not in state["products"]["prepared_groups"][0]


def test_preparation_all_rebuilds_existing_groups(tmp_path: Path) -> None:
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
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    prepare_contributions(_preparation_config(routing_dir, preparation_dir))

    state_path = routing_dir / "pipeline-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["dirty"]["preparation"]["all"] = True
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")

    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 1
    assert report["unchanged_group_count"] == 1


def test_preparation_only_rebuilds_changed_group(
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
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    prepare_contributions(_preparation_config(routing_dir, preparation_dir))
    unchanged_file = next(
        (preparation_dir / "tree").glob("shard-101-pack-sorted-*.parquet")
    )
    unchanged_mtime = unchanged_file.stat().st_mtime_ns

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
    route_contributions(
        _routing_config(
            input_root,
            routing_dir,
            shard_ids=("100",),
            replace_shards=True,
        )
    )

    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 1
    assert report["changed_group_count"] == 1
    assert report["unchanged_group_count"] == 0
    assert unchanged_file.stat().st_mtime_ns == unchanged_mtime


def test_one_star_replacement_preserves_unrelated_sorted_group_from_same_shard(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    rows = [
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
    _write_routing_pixel(input_root, "100", rows)
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    routing_config = _routing_config(input_root, routing_dir, bucket_size=2)
    preparation_config = _preparation_config(
        routing_dir, preparation_dir, bucket_size=2
    )
    route_contributions(routing_config)
    prepare_contributions(preparation_config)

    before_state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    before_groups = {
        group["key"]: group for group in before_state["products"]["prepared_groups"]
    }
    unchanged_key = "o=1|100|pack"
    changed_key = "o=0|100|pack"
    unchanged_files = [
        preparation_dir / value for value in before_groups[unchanged_key]["files"]
    ]
    unchanged_stats = [
        (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in unchanged_files
    ]

    replacement = [dict(row) for row in rows]
    replacement[0]["source_id"] = "changed-after"
    _write_routing_pixel(input_root, "100", replacement)
    route_contributions(
        _routing_config(
            input_root,
            routing_dir,
            bucket_size=2,
            shard_ids=("100",),
            replace_shards=True,
        )
    )

    report_path = prepare_contributions(preparation_config)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    after_state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    after_groups = {
        group["key"]: group for group in after_state["products"]["prepared_groups"]
    }
    assert report["processed_group_count"] == 1
    assert report["changed_group_count"] == 1
    assert report["unchanged_group_count"] == 0
    assert after_groups[unchanged_key] == before_groups[unchanged_key]
    assert (
        after_groups[changed_key]["checksum"] != before_groups[changed_key]["checksum"]
    )
    assert [
        (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in unchanged_files
    ] == unchanged_stats


def test_preparation_processes_dirty_groups_accumulated_across_replacements(
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
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    prepare_contributions(_preparation_config(routing_dir, preparation_dir))

    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "a-changed",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
                "mag_abs": 7.0,
            }
        ],
    )
    route_contributions(
        _routing_config(
            input_root,
            routing_dir,
            shard_ids=("100",),
            replace_shards=True,
        )
    )

    _write_routing_pixel(
        input_root,
        "101",
        [
            {
                "source": "hip",
                "source_id": "b-changed",
                "morton_code": _morton_for_node(1, 1),
                "level": 1,
                "mag_abs": 7.1,
            }
        ],
    )
    route_contributions(
        _routing_config(
            input_root,
            routing_dir,
            shard_ids=("101",),
            replace_shards=True,
        )
    )

    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    assert state["dirty"]["preparation"]["group_keys"] == ["|100|pack", "|101|pack"]

    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    assert report["processed_group_count"] == 2
    assert report["changed_group_count"] == 2
    assert state["dirty"]["preparation"]["group_keys"] == []
    assert pq.read_table(
        next((preparation_dir / "tree").glob("shard-100-pack-sorted-*.parquet"))
    ).column("source_id").to_pylist() == ["a-changed"]
    assert pq.read_table(
        next((preparation_dir / "tree").glob("shard-101-pack-sorted-*.parquet"))
    ).column("source_id").to_pylist() == ["b-changed"]


def test_preparation_unchanged_replacement_processes_no_groups(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    rows = [
        {
            "source": "gaia",
            "source_id": "a",
            "morton_code": _morton_for_node(1, 0),
            "level": 1,
            "mag_abs": 7.0,
        }
    ]
    _write_routing_pixel(input_root, "100", rows)
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    prepare_contributions(_preparation_config(routing_dir, preparation_dir))

    _write_routing_pixel(input_root, "100", rows)
    route_contributions(
        _routing_config(
            input_root,
            routing_dir,
            shard_ids=("100",),
            replace_shards=True,
        )
    )
    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 0
    assert report["changed_group_count"] == 0


def test_preparation_deleted_group_removes_sorted_files_and_dirties_old_nodes(
    tmp_path: Path,
) -> None:
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
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir, bucket_size=1))
    prepare_contributions(
        _preparation_config(routing_dir, preparation_dir, bucket_size=1)
    )
    deleted_files = sorted((preparation_dir / "tree" / "o=0").glob("*.parquet"))
    assert deleted_files
    _write_routing_pixel(input_root, "200", [root_row])
    route_contributions(
        _routing_config(
            input_root,
            routing_dir,
            bucket_size=1,
            shard_ids=("200",),
            replace_shards=True,
        )
    )

    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir, bucket_size=1)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    assert report["deleted_group_count"] == 1
    assert not any(path.exists() for path in deleted_files)
    assert {group["key"] for group in state["products"]["prepared_groups"]} == {
        "|200|lim"
    }


def test_preparation_rejects_missing_manifest_and_identity_mismatch(
    tmp_path: Path,
) -> None:
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    routing_dir.mkdir()
    with pytest.raises(FileNotFoundError, match="tree manifest"):
        prepare_contributions(_preparation_config(routing_dir, preparation_dir))

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
    route_contributions(_routing_config(input_root, routing_dir))
    with pytest.raises(ValueError, match="tree identity"):
        prepare_contributions(
            _preparation_config(
                routing_dir,
                preparation_dir,
                bucket_size=99,
            )
        )


def test_preparation_rejects_routing_group_schema_drift(tmp_path: Path) -> None:
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
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    state_path = routing_dir / "pipeline-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    original_rel = state["products"]["routed_groups"][0]["files"][0]
    original_table = pq.read_table(routing_dir / original_rel)
    drift_rel = "tree/shard-100-pack-999999.parquet"
    pq.write_table(
        original_table.append_column(
            "unexpected",
            pa.array(["x"], type=pa.string()),
        ),
        routing_dir / drift_rel,
        compression="zstd",
    )
    state["products"]["routed_groups"][0]["files"].append(drift_rel)
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="identical schemas"):
        prepare_contributions(_preparation_config(routing_dir, preparation_dir))


def test_preparation_force_rebuilds_all_groups(tmp_path: Path) -> None:
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
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    prepare_contributions(_preparation_config(routing_dir, preparation_dir))

    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir, force=True)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 1
    assert report["changed_group_count"] == 1
    assert report["force"] is True


def test_preparation_uses_external_sort_for_oversized_group(
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
                "source_id": source_id,
                "morton_code": _morton_for_node(level, node_id),
                "level": level,
                "mag_abs": magnitude,
            }
            for source_id, level, node_id, magnitude in (
                ("z", 2, 1, 9.0),
                ("b", 1, 0, 8.0),
                ("a", 1, 0, 7.0),
                ("y", 2, 0, 6.0),
                ("x", 2, 1, 5.0),
            )
        ],
    )
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    routing_state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    expected_sorted = preparation_module._sorted_routing_group(
        routing_dir,
        routing_state["products"]["routed_groups"][0],
    )
    expected_checksum = preparation_module._preparation_group_checksum(expected_sorted)
    monkeypatch.setattr(
        preparation_module,
        "PREPARATION_IN_MEMORY_MAX_UNCOMPRESSED_BYTES",
        1,
    )
    monkeypatch.setattr(
        preparation_module,
        "_sorted_routing_group_files",
        lambda *_args, **_kwargs: pytest.fail("in-memory sort path was used"),
    )

    report_path = prepare_contributions(
        _preparation_config(
            routing_dir,
            preparation_dir,
            fragment_target_rows=2,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    files = [
        preparation_dir / path
        for path in state["products"]["prepared_groups"][0]["files"]
    ]
    sorted_table = pa.concat_tables([pq.read_table(path) for path in files])
    assert report["external_sort_group_count"] == 1
    assert report["in_memory_sort_group_count"] == 0
    assert [pq.read_metadata(path).num_rows for path in files] == [2, 2, 1]
    assert sorted_table.column("source_id").to_pylist() == ["a", "b", "y", "x", "z"]
    assert state["products"]["prepared_groups"][0]["checksum"] == expected_checksum


def test_preparation_external_sort_matches_in_memory_for_primary_key_ties(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    base_morton = _morton_for_node(2, 1)
    _write_routing_pixel(
        input_root,
        "100",
        [
            {
                "source": "gaia",
                "source_id": "same",
                "morton_code": base_morton + suffix,
                "level": 2,
                "mag_abs": 7.0,
            }
            for suffix in (9, 1, 7, 3, 5)
        ],
    )
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    expected = preparation_module._sorted_routing_group(
        routing_dir,
        state["products"]["routed_groups"][0],
    )
    expected_checksum = preparation_module._preparation_group_checksum(expected)
    monkeypatch.setattr(
        preparation_module,
        "PREPARATION_IN_MEMORY_MAX_UNCOMPRESSED_BYTES",
        1,
    )

    prepare_contributions(
        _preparation_config(routing_dir, preparation_dir, fragment_target_rows=2)
    )

    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    files = [
        preparation_dir / path
        for path in state["products"]["prepared_groups"][0]["files"]
    ]
    actual = pa.concat_tables([pq.read_table(path) for path in files])
    assert state["products"]["prepared_groups"][0]["checksum"] == expected_checksum
    assert actual.column("morton_code").to_pylist() == [
        base_morton + suffix for suffix in (1, 3, 5, 7, 9)
    ]


def test_preparation_external_sort_matches_arrow_null_and_nan_ordering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    rows = [
        {
            "source": "gaia",
            "source_id": source_id,
            "morton_code": _morton_for_node(1, 0),
            "level": 1,
            "mag_abs": magnitude,
        }
        for source_id, magnitude in (
            (None, 1.0),
            ("null-mag", None),
            ("nan", float("nan")),
            ("z", 1.0),
        )
    ]
    _write_routing_pixel(input_root, "100", rows)
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    expected = preparation_module._sorted_routing_group(
        routing_dir,
        state["products"]["routed_groups"][0],
    )
    expected_checksum = preparation_module._preparation_group_checksum(expected)
    monkeypatch.setattr(
        preparation_module,
        "PREPARATION_IN_MEMORY_MAX_UNCOMPRESSED_BYTES",
        1,
    )

    prepare_contributions(_preparation_config(routing_dir, preparation_dir))

    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    files = [
        preparation_dir / path
        for path in state["products"]["prepared_groups"][0]["files"]
    ]
    actual = pa.concat_tables([pq.read_table(path) for path in files])
    assert state["products"]["prepared_groups"][0]["checksum"] == expected_checksum
    assert actual.column("source_id").to_pylist() == [
        "z",
        None,
        "nan",
        "null-mag",
    ]


def test_preparation_reprocessing_keeps_published_group_files_immutable(
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
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    prepare_contributions(_preparation_config(routing_dir, preparation_dir))
    state_path = routing_dir / "pipeline-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    group = state["products"]["prepared_groups"][0]
    output_path = preparation_dir / group["files"][0]
    inode = output_path.stat().st_ino
    state["dirty"]["preparation"]["group_keys"] = [group["key"]]
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")

    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert report["unchanged_group_count"] == 1
    assert state["products"]["prepared_groups"][0]["files"] == group["files"]
    assert output_path.stat().st_ino == inode


def test_preparation_external_sort_ignores_hive_node_directories(
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
                "source_id": str(node_id),
                "morton_code": _morton_for_node(2, node_id),
                "level": 2,
                "mag_abs": 7.0 + node_id,
            }
            for node_id in range(3)
        ],
    )
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir, bucket_size=1))
    monkeypatch.setattr(
        preparation_module,
        "PREPARATION_IN_MEMORY_MAX_UNCOMPRESSED_BYTES",
        1,
    )

    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir, bucket_size=1)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    assert report["external_sort_group_count"] == 3
    for group in state["products"]["prepared_groups"]:
        for rel_path in group["files"]:
            schema = pq.read_schema(preparation_dir / rel_path)
            assert "o" not in schema.names


def test_preparation_resumes_after_completed_group(
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
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(_routing_config(input_root, routing_dir))
    original = preparation_module._prepare_sorted_group
    calls = 0

    def fail_on_second_group(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated Preparation interruption")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        preparation_module,
        "_prepare_sorted_group",
        fail_on_second_group,
    )
    with pytest.raises(RuntimeError, match="Preparation interruption"):
        prepare_contributions(_preparation_config(routing_dir, preparation_dir))

    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    assert state["builds"]["preparation"]["status"] == "in_progress"
    assert state["products"]["prepared_groups"] == []
    checkpoints = list((preparation_dir / ".preparation-checkpoints").glob("*.json"))
    assert len(checkpoints) == 1
    completed_file = next(
        (preparation_dir / "tree").glob("shard-100-pack-sorted-*.parquet")
    )
    completed_mtime = completed_file.stat().st_mtime_ns

    monkeypatch.setattr(preparation_module, "_prepare_sorted_group", original)
    report_path = prepare_contributions(
        _preparation_config(routing_dir, preparation_dir)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads(
        (routing_dir / "pipeline-state.json").read_text(encoding="utf-8")
    )
    assert report["processed_group_count"] == 1
    assert report["resumed"] is True
    assert state["builds"]["preparation"]["status"] == "complete"
    assert len(state["products"]["prepared_groups"]) == 2
    assert completed_file.stat().st_mtime_ns == completed_mtime


def test_preparation_state_does_not_enumerate_final_nodes(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    rows = [
        {
            "source": "gaia",
            "source_id": str(node_id),
            "morton_code": _morton_for_node(8, node_id),
            "level": 8,
            "mag_abs": 10.0,
        }
        for node_id in range(256)
    ]
    _write_routing_pixel(input_root, "100", rows)
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    route_contributions(
        _routing_config(
            input_root,
            routing_dir,
            bucket_size=10_000,
            fragment_target_rows=1_000,
        )
    )
    prepare_contributions(
        _preparation_config(
            routing_dir,
            preparation_dir,
            bucket_size=10_000,
            fragment_target_rows=1_000,
        )
    )

    state_path = routing_dir / "pipeline-state.json"
    state_text = state_path.read_text(encoding="utf-8")
    state = json.loads(state_text)
    assert len(state_text) < 20_000
    assert "final_nodes" not in state_text
    assert state["products"]["prepared_groups"][0]["natural_max_level"] == 8
