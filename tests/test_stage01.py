from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import foundinspace.octree.sources.stage01 as stage01_module
from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.sources.stage00 import Stage00Config, run_stage00
from foundinspace.octree.sources.stage01 import Stage01Config, run_stage01


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
            "render": pa.array(
                [bytes([idx]) * 16 for idx, _row in enumerate(rows)],
                type=pa.binary(16),
            ),
            "level": pa.array([r["level"] for r in rows], type=pa.int32()),
            "mag_abs": pa.array([r["mag_abs"] for r in rows], type=pa.float64()),
            "healpix_id": pa.array([shard_id for _ in rows], type=pa.string()),
        }
    )


def _write_stage00_pixel(
    root: Path,
    pixel: str,
    rows: list[dict],
) -> None:
    pixel_dir = root / pixel
    pixel_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _stage00_table(rows, shard_id=pixel),
        pixel_dir / "part.parquet",
        compression="zstd",
    )


def _stage00_config(
    input_root: Path,
    output_dir: Path,
    *,
    bucket_size: int = 100,
    fragment_target_rows: int = 10,
    shard_ids: tuple[str, ...] = (),
    replace_shards: bool = False,
) -> Stage00Config:
    return Stage00Config(
        input_root=input_root,
        output_dir=output_dir,
        mag_config=MagLevelConfig(v_mag=6.5),
        bucket_size=bucket_size,
        batch_size=10,
        fragment_target_rows=fragment_target_rows,
        compact_after_files=0,
        shard_ids=shard_ids,
        replace_shards=replace_shards,
    )


def _stage01_config(
    stage00_output_dir: Path,
    output_dir: Path,
    *,
    bucket_size: int = 100,
    fragment_target_rows: int = 10,
    force: bool = False,
) -> Stage01Config:
    return Stage01Config(
        stage00_output_dir=stage00_output_dir,
        output_dir=output_dir,
        v_mag=6.5,
        bucket_size=bucket_size,
        batch_size=10,
        fragment_target_rows=fragment_target_rows,
        force=force,
    )


def _clear_stage03_dirty(stage00_dir: Path) -> None:
    state_path = stage00_dir / "stage-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["dirty"].pop("stage03_nodes", None)
    state["dirty"]["stage03"] = {"mode": "clean"}
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def test_stage01_first_run_writes_sorted_groups_and_state(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))

    report_path = run_stage01(
        _stage01_config(stage00_dir, stage01_dir, fragment_target_rows=2)
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    files = sorted((stage01_dir / "tree").glob("shard-100-pack-sorted-*.parquet"))
    sorted_table = pa.concat_tables([pq.read_table(path) for path in files])
    assert sorted_table.column("source_id").to_pylist() == ["root", "a", "b", "z"]
    assert "_stage01_final_node_id" not in sorted_table.schema.names
    assert "healpix_id" in sorted_table.schema.names
    assert len(files) == 2
    assert report["processed_group_count"] == 1
    assert report["changed_group_count"] == 1
    assert report["dirty_stage03_mode"] == "all"
    assert report["dirty_stage03_node_count"] == 0
    assert report["in_memory_sort_group_count"] == 1
    assert report["external_sort_group_count"] == 0
    assert state["dirty"]["stage01_groups"] == []
    assert state["dirty"]["deleted_stage00_groups"] == []
    assert state["dirty"]["stage03"]["mode"] == "all"
    assert state["stage01_groups"][0]["files"] == [
        path.relative_to(stage01_dir).as_posix() for path in files
    ]
    assert state["stage01_groups"][0]["natural_max_level"] == 1
    assert "final_nodes" not in state["stage01_groups"][0]


def test_stage01_all_rebuilds_existing_groups(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))
    run_stage01(_stage01_config(stage00_dir, stage01_dir))

    state_path = stage00_dir / "stage-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["dirty"]["stage01_all"] = True
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")

    report_path = run_stage01(_stage01_config(stage00_dir, stage01_dir))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 1
    assert report["unchanged_group_count"] == 1


def test_stage01_dirty_only_changed_group_marks_stage03_all(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))
    run_stage01(_stage01_config(stage00_dir, stage01_dir))
    _clear_stage03_dirty(stage00_dir)
    unchanged_file = next(
        (stage01_dir / "tree").glob("shard-101-pack-sorted-*.parquet")
    )
    unchanged_mtime = unchanged_file.stat().st_mtime_ns

    _write_stage00_pixel(
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
    run_stage00(
        _stage00_config(
            input_root,
            stage00_dir,
            shard_ids=("100",),
            replace_shards=True,
        )
    )

    report_path = run_stage01(_stage01_config(stage00_dir, stage01_dir))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 1
    assert report["changed_group_count"] == 1
    assert report["unchanged_group_count"] == 0
    assert state["dirty"]["stage03"]["mode"] == "all"
    assert unchanged_file.stat().st_mtime_ns == unchanged_mtime


def test_stage01_processes_dirty_groups_accumulated_across_replacements(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))
    run_stage01(_stage01_config(stage00_dir, stage01_dir))
    _clear_stage03_dirty(stage00_dir)

    _write_stage00_pixel(
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
    run_stage00(
        _stage00_config(
            input_root,
            stage00_dir,
            shard_ids=("100",),
            replace_shards=True,
        )
    )

    _write_stage00_pixel(
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
    run_stage00(
        _stage00_config(
            input_root,
            stage00_dir,
            shard_ids=("101",),
            replace_shards=True,
        )
    )

    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    assert state["dirty"]["stage01_groups"] == ["|100|pack", "|101|pack"]

    report_path = run_stage01(_stage01_config(stage00_dir, stage01_dir))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 2
    assert report["changed_group_count"] == 2
    assert state["dirty"]["stage01_groups"] == []
    assert state["dirty"]["stage03"]["mode"] == "all"
    assert pq.read_table(
        next((stage01_dir / "tree").glob("shard-100-pack-sorted-*.parquet"))
    ).column("source_id").to_pylist() == ["a-changed"]
    assert pq.read_table(
        next((stage01_dir / "tree").glob("shard-101-pack-sorted-*.parquet"))
    ).column("source_id").to_pylist() == ["b-changed"]


def test_stage01_unchanged_replacement_processes_no_groups(tmp_path: Path) -> None:
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
    _write_stage00_pixel(input_root, "100", rows)
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))
    run_stage01(_stage01_config(stage00_dir, stage01_dir))
    _clear_stage03_dirty(stage00_dir)

    _write_stage00_pixel(input_root, "100", rows)
    run_stage00(
        _stage00_config(
            input_root,
            stage00_dir,
            shard_ids=("100",),
            replace_shards=True,
        )
    )
    report_path = run_stage01(_stage01_config(stage00_dir, stage01_dir))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 0
    assert report["changed_group_count"] == 0
    assert state["dirty"]["stage03"] == {"mode": "clean"}


def test_stage01_deleted_group_removes_sorted_files_and_dirties_old_nodes(
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
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir, bucket_size=1))
    run_stage01(_stage01_config(stage00_dir, stage01_dir, bucket_size=1))
    _clear_stage03_dirty(stage00_dir)
    deleted_files = sorted((stage01_dir / "tree" / "o=0").glob("*.parquet"))
    assert deleted_files
    _write_stage00_pixel(input_root, "200", [root_row])
    run_stage00(
        _stage00_config(
            input_root,
            stage00_dir,
            bucket_size=1,
            shard_ids=("200",),
            replace_shards=True,
        )
    )

    report_path = run_stage01(_stage01_config(stage00_dir, stage01_dir, bucket_size=1))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    assert report["deleted_group_count"] == 1
    assert not any(path.exists() for path in deleted_files)
    assert state["dirty"]["stage03"]["mode"] == "all"
    assert {group["key"] for group in state["stage01_groups"]} == {"|200|lim"}


def test_stage01_rejects_missing_manifest_and_identity_mismatch(
    tmp_path: Path,
) -> None:
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    stage00_dir.mkdir()
    with pytest.raises(FileNotFoundError, match="tree manifest"):
        run_stage01(_stage01_config(stage00_dir, stage01_dir))

    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    run_stage00(_stage00_config(input_root, stage00_dir))
    with pytest.raises(ValueError, match="tree identity"):
        run_stage01(
            _stage01_config(
                stage00_dir,
                stage01_dir,
                bucket_size=99,
            )
        )


def test_stage01_rejects_stage00_group_schema_drift(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))
    state_path = stage00_dir / "stage-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    original_rel = state["stage00_groups"][0]["files"][0]
    original_table = pq.read_table(stage00_dir / original_rel)
    drift_rel = "tree/shard-100-pack-999999.parquet"
    pq.write_table(
        original_table.append_column(
            "unexpected",
            pa.array(["x"], type=pa.string()),
        ),
        stage00_dir / drift_rel,
        compression="zstd",
    )
    state["stage00_groups"][0]["files"].append(drift_rel)
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="identical schemas"):
        run_stage01(_stage01_config(stage00_dir, stage01_dir))


def test_stage01_force_rebuilds_all_groups(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))
    run_stage01(_stage01_config(stage00_dir, stage01_dir))
    _clear_stage03_dirty(stage00_dir)

    report_path = run_stage01(_stage01_config(stage00_dir, stage01_dir, force=True))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 1
    assert report["changed_group_count"] == 1
    assert report["force"] is True


def test_stage01_uses_external_sort_for_oversized_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))
    stage00_state = json.loads(
        (stage00_dir / "stage-state.json").read_text(encoding="utf-8")
    )
    expected_sorted = stage01_module._sorted_stage00_group(
        stage00_dir,
        stage00_state["stage00_groups"][0],
    )
    expected_checksum = stage01_module._stage01_group_checksum(expected_sorted)
    monkeypatch.setattr(
        stage01_module,
        "STAGE01_IN_MEMORY_MAX_UNCOMPRESSED_BYTES",
        1,
    )
    monkeypatch.setattr(
        stage01_module,
        "_sorted_stage00_group_files",
        lambda *_args, **_kwargs: pytest.fail("in-memory sort path was used"),
    )

    report_path = run_stage01(
        _stage01_config(
            stage00_dir,
            stage01_dir,
            fragment_target_rows=2,
        )
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    files = [stage01_dir / path for path in state["stage01_groups"][0]["files"]]
    sorted_table = pa.concat_tables([pq.read_table(path) for path in files])
    assert report["external_sort_group_count"] == 1
    assert report["in_memory_sort_group_count"] == 0
    assert [pq.read_metadata(path).num_rows for path in files] == [2, 2, 1]
    assert sorted_table.column("source_id").to_pylist() == ["a", "b", "y", "x", "z"]
    assert state["stage01_groups"][0]["checksum"] == expected_checksum


def test_stage01_external_sort_ignores_hive_node_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir, bucket_size=1))
    monkeypatch.setattr(
        stage01_module,
        "STAGE01_IN_MEMORY_MAX_UNCOMPRESSED_BYTES",
        1,
    )

    report_path = run_stage01(_stage01_config(stage00_dir, stage01_dir, bucket_size=1))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    assert report["external_sort_group_count"] == 3
    for group in state["stage01_groups"]:
        for rel_path in group["files"]:
            schema = pq.read_schema(stage01_dir / rel_path)
            assert "o" not in schema.names


def test_stage01_resumes_after_completed_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    for shard_id, node_id in (("100", 0), ("101", 1)):
        _write_stage00_pixel(
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
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(_stage00_config(input_root, stage00_dir))
    original = stage01_module._prepare_sorted_group
    calls = 0

    def fail_on_second_group(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated Stage 01 interruption")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        stage01_module,
        "_prepare_sorted_group",
        fail_on_second_group,
    )
    with pytest.raises(RuntimeError, match="Stage 01 interruption"):
        run_stage01(_stage01_config(stage00_dir, stage01_dir))

    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    assert state["stage01_build"]["status"] == "in_progress"
    assert state.get("stage01_groups", []) == []
    checkpoints = list((stage01_dir / ".stage01-checkpoints").glob("*.json"))
    assert len(checkpoints) == 1
    completed_file = next(
        (stage01_dir / "tree").glob("shard-100-pack-sorted-*.parquet")
    )
    completed_mtime = completed_file.stat().st_mtime_ns

    monkeypatch.setattr(stage01_module, "_prepare_sorted_group", original)
    report_path = run_stage01(_stage01_config(stage00_dir, stage01_dir))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    state = json.loads((stage00_dir / "stage-state.json").read_text(encoding="utf-8"))
    assert report["processed_group_count"] == 1
    assert report["resumed"] is True
    assert state["stage01_build"]["status"] == "complete"
    assert len(state["stage01_groups"]) == 2
    assert completed_file.stat().st_mtime_ns == completed_mtime


def test_stage01_state_does_not_enumerate_final_nodes(tmp_path: Path) -> None:
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
    _write_stage00_pixel(input_root, "100", rows)
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    run_stage00(
        _stage00_config(
            input_root,
            stage00_dir,
            bucket_size=10_000,
            fragment_target_rows=1_000,
        )
    )
    run_stage01(
        _stage01_config(
            stage00_dir,
            stage01_dir,
            bucket_size=10_000,
            fragment_target_rows=1_000,
        )
    )

    state_path = stage00_dir / "stage-state.json"
    state_text = state_path.read_text(encoding="utf-8")
    state = json.loads(state_text)
    assert len(state_text) < 20_000
    assert "final_nodes" not in state_text
    assert "stage03_nodes" not in state_text
    assert state["stage01_groups"][0]["natural_max_level"] == 8
    assert state["dirty"]["stage03"]["mode"] == "all"
