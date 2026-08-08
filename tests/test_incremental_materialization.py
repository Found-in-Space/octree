from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import foundinspace.octree.classic_materialization as materialization
from foundinspace.octree.classic_materialization import (
    ClassicMaterializationPlan,
    Stage01GroupInput,
    materialize_classic_groups,
)
from foundinspace.octree.config import MORTON_BITS, WORLD_CENTER, WORLD_HALF_SIZE_PC


def _morton_for_node(level: int, node_id: int) -> int:
    return int(node_id) << (3 * (MORTON_BITS - level))


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


def _write_group(
    root: Path,
    *,
    key: str,
    checksum: str,
    node_id: int,
    source_ids: tuple[str, ...],
) -> Stage01GroupInput:
    level = 2
    center = _node_center(level, node_id)
    path = root / f"{key}.parquet"
    pq.write_table(
        pa.table(
            {
                "x_icrs_pc": pa.array([center[0]] * len(source_ids), pa.float64()),
                "y_icrs_pc": pa.array([center[1]] * len(source_ids), pa.float64()),
                "z_icrs_pc": pa.array([center[2]] * len(source_ids), pa.float64()),
                "mag_abs": pa.array(
                    [7.0 + index / 10 for index in range(len(source_ids))],
                    pa.float64(),
                ),
                "source": pa.array(["gaia"] * len(source_ids), pa.string()),
                "source_id": pa.array(source_ids, pa.string()),
                "morton_code": pa.array(
                    [_morton_for_node(level, node_id)] * len(source_ids),
                    pa.uint64(),
                ),
                "level": pa.array([level] * len(source_ids), pa.int32()),
            }
        ),
        path,
        compression="zstd",
    )
    return Stage01GroupInput(
        key=key,
        checksum=checksum,
        row_count=len(source_ids),
        files=(path,),
        natural_max_level=level,
    )


def _plan(*, star_format_version: int) -> ClassicMaterializationPlan:
    return ClassicMaterializationPlan(
        max_level=2,
        mag_limit=6.5,
        batch_size=1,
        max_open_files=2,
        partition_from_level=1,
        partition_prefix_bits=1,
        star_format_version=star_format_version,
        terminal_waterline=10,
    )


def _state(work_dir: Path) -> dict:
    return json.loads(
        (work_dir / materialization.CLASSIC_WORK_STATE_NAME).read_text(encoding="utf-8")
    )


def _partition_files(work_dir: Path, state: dict, key: str) -> tuple[Path, ...]:
    partition = state["completed_partitions"][key]
    cache_dir = work_dir / partition["cache_dir"]
    return tuple(
        cache_dir / entry[path_key]
        for entry_name in ("render_entry", "identifiers_entry")
        for entry in (partition[entry_name],)
        for path_key in ("index_path", "payload_path")
    )


def _file_identity(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_ino, stat.st_mtime_ns


def test_one_changed_group_reuses_unrelated_runs_and_partition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "groups"
    source_dir.mkdir()
    group_a = _write_group(
        source_dir,
        key="a",
        checksum="sha256:a1",
        node_id=0,
        source_ids=("a1",),
    )
    group_b = _write_group(
        source_dir,
        key="b",
        checksum="sha256:b1",
        node_id=32,
        source_ids=("b1",),
    )
    work_dir = tmp_path / "work"
    plan = _plan(star_format_version=1)
    materialize_classic_groups(groups=(group_a, group_b), work_dir=work_dir, plan=plan)

    first = _state(work_dir)
    stable_group = first["completed_groups"]["b"]
    stable_run = work_dir / stable_group["runs"][0]["path"]
    stable_run_identity = _file_identity(stable_run)
    stable_partition_key = "2:1:1"
    stable_partition = first["completed_partitions"][stable_partition_key]
    stable_partition_files = _partition_files(work_dir, first, stable_partition_key)
    stable_partition_identities = {
        path: _file_identity(path) for path in stable_partition_files
    }

    changed_a = _write_group(
        source_dir,
        key="a",
        checksum="sha256:a2",
        node_id=0,
        source_ids=("a2",),
    )
    original_normalize = materialization._normalize_group
    original_partition = materialization._materialize_partition
    normalized: list[str] = []
    materialized_prefixes: list[int] = []

    def track_normalize(group, **kwargs):
        normalized.append(group.key)
        return original_normalize(group, **kwargs)

    def track_partition(runs, *, shard, **kwargs):
        materialized_prefixes.append(shard.prefix)
        return original_partition(runs, shard=shard, **kwargs)

    monkeypatch.setattr(materialization, "_normalize_group", track_normalize)
    monkeypatch.setattr(materialization, "_materialize_partition", track_partition)
    result = materialize_classic_groups(
        groups=(changed_a, group_b),
        work_dir=work_dir,
        plan=plan,
    )

    assert normalized == ["a"]
    assert materialized_prefixes == [0]
    second = _state(work_dir)
    assert second["completed_groups"]["b"] == stable_group
    assert _file_identity(stable_run) == stable_run_identity
    assert second["completed_partitions"][stable_partition_key] == stable_partition
    assert {
        path: _file_identity(path) for path in stable_partition_files
    } == stable_partition_identities
    for path in stable_partition_files:
        published = result.render_manifest_path.parent / path.name
        assert published.stat().st_ino == path.stat().st_ino

    monkeypatch.setattr(
        materialization,
        "_normalize_group",
        lambda *_args, **_kwargs: pytest.fail("unchanged group was normalized"),
    )
    monkeypatch.setattr(
        materialization,
        "_materialize_partition",
        lambda *_args, **_kwargs: pytest.fail("unchanged partition was materialized"),
    )
    materialize_classic_groups(groups=(group_b,), work_dir=work_dir, plan=plan)
    deleted = _state(work_dir)
    assert set(deleted["completed_groups"]) == {"b"}
    assert set(deleted["completed_partitions"]) == {stable_partition_key}
    assert _file_identity(stable_run) == stable_run_identity
    assert {
        path: _file_identity(path) for path in stable_partition_files
    } == stable_partition_identities


def test_v2_count_change_with_same_terminal_map_reuses_unrelated_group_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "groups"
    source_dir.mkdir()
    group_a = _write_group(
        source_dir,
        key="a",
        checksum="sha256:a1",
        node_id=0,
        source_ids=("a1",),
    )
    group_b = _write_group(
        source_dir,
        key="b",
        checksum="sha256:b1",
        node_id=1,
        source_ids=("b1",),
    )
    work_dir = tmp_path / "work"
    plan = _plan(star_format_version=2)
    materialize_classic_groups(groups=(group_a, group_b), work_dir=work_dir, plan=plan)
    first = _state(work_dir)
    first_topology = first["topology_identity"]
    stable_group = first["completed_groups"]["b"]
    stable_run = work_dir / stable_group["runs"][0]["path"]
    stable_run_identity = _file_identity(stable_run)

    changed_a = _write_group(
        source_dir,
        key="a",
        checksum="sha256:a2",
        node_id=0,
        source_ids=("a1", "a2"),
    )
    original_normalize = materialization._normalize_group
    normalized: list[str] = []

    def track_normalize(group, **kwargs):
        normalized.append(group.key)
        return original_normalize(group, **kwargs)

    monkeypatch.setattr(materialization, "_normalize_group", track_normalize)
    materialize_classic_groups(
        groups=(changed_a, group_b),
        work_dir=work_dir,
        plan=plan,
    )

    second = _state(work_dir)
    assert second["topology_identity"] == first_topology
    assert normalized == ["a"]
    assert second["completed_groups"]["b"] == stable_group
    assert _file_identity(stable_run) == stable_run_identity
