from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import foundinspace.octree.terminal_packing as terminal_packing
from foundinspace.octree.classic_materialization import Stage01GroupInput
from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.terminal_packing import TerminalMap, build_terminal_map


def _morton_for_node(level: int, node_id: int) -> int:
    return int(node_id) << (3 * (MORTON_BITS - level))


def _build_map(
    tmp_path: Path,
    cells: list[tuple[int, int, int]],
    *,
    waterline: int,
    max_level: int = 2,
) -> TerminalMap:
    tmp_path.mkdir(parents=True, exist_ok=True)
    levels: list[int] = []
    morton_codes: list[int] = []
    for level, node_id, count in cells:
        levels.extend([level] * count)
        morton_codes.extend([_morton_for_node(level, node_id)] * count)
    source = tmp_path / "stage01.parquet"
    pq.write_table(
        pa.table(
            {
                "level": pa.array(levels, type=pa.int32()),
                "morton_code": pa.array(morton_codes, type=pa.uint64()),
            }
        ),
        source,
    )
    group = Stage01GroupInput(
        key="group",
        checksum="sha256:test",
        row_count=len(levels),
        files=(source,),
        natural_max_level=max(levels),
    )
    work_dir = tmp_path / "work"
    artifacts_dir = work_dir / "artifacts"
    artifacts_dir.mkdir(parents=True)
    manifest = build_terminal_map(
        groups=(group,),
        work_dir=work_dir,
        artifacts_dir=artifacts_dir,
        max_level=max_level,
        waterline=waterline,
        batch_size=127,
    )
    return TerminalMap(manifest)


def _write_group(
    directory: Path,
    *,
    key: str,
    checksum: str,
    cells: list[tuple[int, int, int]],
) -> Stage01GroupInput:
    directory.mkdir(parents=True, exist_ok=True)
    levels: list[int] = []
    morton_codes: list[int] = []
    for level, node_id, count in cells:
        levels.extend([level] * count)
        morton_codes.extend([_morton_for_node(level, node_id)] * count)
    source = directory / f"{key}.parquet"
    pq.write_table(
        pa.table(
            {
                "level": pa.array(levels, type=pa.int32()),
                "morton_code": pa.array(morton_codes, type=pa.uint64()),
            }
        ),
        source,
    )
    return Stage01GroupInput(
        key=key,
        checksum=checksum,
        row_count=len(levels),
        files=(source,),
        natural_max_level=max(levels, default=None),
    )


def _build_groups(
    tmp_path: Path,
    groups: tuple[Stage01GroupInput, ...],
    *,
    max_level: int = 2,
    waterline: int = 2,
    batch_size: int = 127,
    merge_fan_in: int = 32,
) -> TerminalMap:
    work_dir = tmp_path / "work"
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return TerminalMap(
        build_terminal_map(
            groups=groups,
            work_dir=work_dir,
            artifacts_dir=artifacts_dir,
            max_level=max_level,
            waterline=waterline,
            batch_size=batch_size,
            merge_fan_in=merge_fan_in,
        )
    )


def test_terminal_map_selects_shallowest_complete_subtrees(tmp_path: Path) -> None:
    terminal_map = _build_map(
        tmp_path,
        [
            (2, 0, 2),
            (2, 63, 1),
        ],
        waterline=2,
    )

    assert terminal_map.contains(0, 0) is False
    assert terminal_map.contains(1, 0) is True
    assert terminal_map.contains(1, 7) is True
    assert terminal_map.terminal_count == 2

    levels, nodes = terminal_map.remap(
        np.asarray([2, 2, 2], dtype=np.int16),
        np.asarray([0, 0, 63], dtype=np.uint64),
    )
    assert levels.tolist() == [1, 1, 1]
    assert nodes.tolist() == [0, 0, 7]


def test_terminal_map_includes_existing_root_payload(tmp_path: Path) -> None:
    terminal_map = _build_map(
        tmp_path,
        [
            (0, 0, 1),
            (1, 0, 1),
        ],
        waterline=2,
    )

    assert terminal_map.contains(0, 0) is True


def test_terminal_map_does_not_mark_natural_leaf(tmp_path: Path) -> None:
    terminal_map = _build_map(
        tmp_path,
        [(0, 0, 1)],
        waterline=1,
        max_level=0,
    )

    assert terminal_map.terminal_count == 0
    assert terminal_map.contains(0, 0) is False


def test_terminal_map_waterline_boundary(tmp_path: Path) -> None:
    exact = _build_map(
        tmp_path / "exact",
        [(2, 0, 1_000)],
        waterline=1_000,
    )
    above = _build_map(
        tmp_path / "above",
        [(2, 0, 1_001)],
        waterline=1_000,
    )

    assert exact.contains(0, 0) is True
    assert above.contains(0, 0) is False
    assert above.terminal_count == 0


def test_terminal_map_supports_maximum_depth_node_ids(tmp_path: Path) -> None:
    terminal_map = _build_map(
        tmp_path,
        [(MORTON_BITS, (1 << (3 * MORTON_BITS)) - 1, 1)],
        waterline=1,
        max_level=MORTON_BITS,
    )

    assert terminal_map.contains(0, 0) is True


def test_terminal_map_rejects_non_ascending_level_file(tmp_path: Path) -> None:
    terminal_map = _build_map(
        tmp_path,
        [
            (2, 0, 1),
            (2, 63, 1),
        ],
        waterline=1,
    )
    manifest_path = terminal_map.manifest_path
    manifest = json.loads(terminal_map.manifest_path.read_text(encoding="utf-8"))
    level_path = terminal_map.manifest_path.parent / manifest["levels"][0]["path"]
    del terminal_map
    level_path.write_bytes(np.asarray([7, 0], dtype="<u8").tobytes())

    with pytest.raises(ValueError, match="Non-ascending terminal node IDs"):
        TerminalMap(manifest_path)


def test_terminal_map_matches_reference_selection(tmp_path: Path) -> None:
    cells = [
        (0, 0, 3),
        (1, 1, 2),
        (2, 8, 4),
        (3, 73, 1),
        (3, 511, 2),
    ]
    max_level = 2
    waterline = 6
    terminal_map = _build_map(
        tmp_path,
        cells,
        waterline=waterline,
        max_level=max_level,
    )

    own: dict[tuple[int, int], int] = {}
    for source_level, source_node, count in cells:
        level = min(source_level, max_level)
        node = source_node >> (3 * (source_level - level))
        own[level, node] = own.get((level, node), 0) + count
    nodes = {
        key: {"own": count, "subtree": count, "descendants": False}
        for key, count in own.items()
    }
    for child_level in range(max_level, 0, -1):
        child_totals: dict[int, int] = {}
        for (level, node), value in list(nodes.items()):
            if level == child_level:
                child_totals[node >> 3] = child_totals.get(node >> 3, 0) + int(
                    value["subtree"]
                )
        for parent, descendant_count in child_totals.items():
            value = nodes.setdefault(
                (child_level - 1, parent),
                {"own": 0, "subtree": 0, "descendants": False},
            )
            value["subtree"] = int(value["own"]) + descendant_count
            value["descendants"] = True
    expected: set[tuple[int, int]] = set()
    for level in range(max_level + 1):
        for (candidate_level, node), value in sorted(nodes.items()):
            if candidate_level != level:
                continue
            covered = any(
                (ancestor_level, node >> (3 * (level - ancestor_level))) in expected
                for ancestor_level in range(level)
            )
            if (
                bool(value["descendants"])
                and 1 <= int(value["subtree"]) <= waterline
                and not covered
            ):
                expected.add((level, node))

    actual = {
        (level, node)
        for level in range(max_level + 1)
        for node in range(1 << (3 * level))
        if terminal_map.contains(level, node)
    }
    assert actual == expected


def test_terminal_map_restart_reuses_immutable_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    group = _write_group(
        tmp_path / "sources",
        key="group",
        checksum="sha256:one",
        cells=[(2, 0, 2), (2, 63, 1)],
    )
    first = _build_groups(tmp_path, (group,))
    first.manifest_path.unlink()

    def fail_if_reopened(_path: Path):
        raise AssertionError("cached Parquet group was reopened")

    monkeypatch.setattr(terminal_packing.pq, "ParquetFile", fail_if_reopened)
    rebuilt = _build_groups(tmp_path, (group,))

    assert rebuilt.terminal_count == first.terminal_count
    assert rebuilt.levels == first.levels


def test_changed_group_does_not_reopen_unchanged_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sources = tmp_path / "sources"
    group_a = _write_group(
        sources,
        key="a",
        checksum="sha256:a1",
        cells=[(2, 0, 1)],
    )
    group_b = _write_group(
        sources,
        key="b",
        checksum="sha256:b1",
        cells=[(2, 63, 1)],
    )
    _build_groups(tmp_path, (group_a, group_b), waterline=3)
    changed_a = _write_group(
        sources,
        key="a",
        checksum="sha256:a2",
        cells=[(2, 0, 2)],
    )

    original = terminal_packing.pq.ParquetFile
    opened: list[Path] = []

    def track_open(path: Path):
        opened.append(Path(path))
        return original(path)

    monkeypatch.setattr(terminal_packing.pq, "ParquetFile", track_open)
    rebuilt = _build_groups(tmp_path, (changed_a, group_b), waterline=3)

    assert opened == [changed_a.files[0]]
    assert rebuilt.contains(0, 0) is True


def test_count_merge_fan_in_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    group = _write_group(
        tmp_path / "sources",
        key="many-runs",
        checksum="sha256:many",
        cells=[(2, node, 1) for node in range(32)],
    )
    original = terminal_packing._merge_count_run_batch
    merge_widths: list[int] = []

    def track_merge(inputs, output):
        merge_widths.append(len(inputs))
        return original(inputs, output)

    monkeypatch.setattr(terminal_packing, "_merge_count_run_batch", track_merge)
    terminal_map = _build_groups(
        tmp_path,
        (group,),
        waterline=32,
        batch_size=1,
        merge_fan_in=2,
    )

    assert merge_widths
    assert max(merge_widths) <= 2
    assert terminal_map.contains(0, 0) is True


def test_count_merge_combines_duplicate_at_chunk_watermark(tmp_path: Path) -> None:
    left_nodes = np.arange(1_024, dtype=np.uint64)
    left = terminal_packing._write_count_arrays(
        tmp_path / "left.counts",
        left_nodes,
        np.ones(len(left_nodes), dtype=np.uint64),
    )
    right = terminal_packing._write_count_arrays(
        tmp_path / "right.counts",
        np.asarray([1_023, 2_048], dtype=np.uint64),
        np.asarray([2, 1], dtype=np.uint64),
    )

    merged = terminal_packing._merge_count_run_batch(
        (left, right), tmp_path / "merged.counts"
    )
    records = np.fromfile(merged.path, dtype=terminal_packing._COUNT_DTYPE)

    assert merged.record_count == 1_025
    assert records[1_023].tolist() == (1_023, 3)
    assert records[-1].tolist() == (2_048, 1)


def test_parent_count_combines_children_split_across_chunks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(terminal_packing, "_IO_RECORDS", 3)
    records = np.empty(9, dtype=terminal_packing._NODE_DTYPE)
    records["node_id"] = np.arange(9, dtype=np.uint64)
    records["subtree_count"] = np.ones(9, dtype=np.uint64)
    records["has_descendants"] = 0
    path = tmp_path / "nodes.bin"
    path.write_bytes(records.tobytes())

    chunks = list(terminal_packing._iter_parent_subtree_chunks(path))
    nodes = np.concatenate([nodes for nodes, _counts in chunks])
    counts = np.concatenate([counts for _nodes, counts in chunks])

    assert nodes.tolist() == [0, 1]
    assert counts.tolist() == [8, 1]


def test_count_merge_rejects_uint64_total_overflow(tmp_path: Path) -> None:
    left = terminal_packing._write_count_arrays(
        tmp_path / "left.counts",
        np.asarray([0], dtype=np.uint64),
        np.asarray([terminal_packing._MAX_U64], dtype=np.uint64),
    )
    right = terminal_packing._write_count_arrays(
        tmp_path / "right.counts",
        np.asarray([0], dtype=np.uint64),
        np.asarray([1], dtype=np.uint64),
    )

    with pytest.raises(OverflowError, match="exceeds uint64"):
        terminal_packing._merge_count_run_batch(
            (left, right), tmp_path / "merged.counts"
        )


def test_immutable_publication_preserves_existing_target(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "value").write_text("winner", encoding="utf-8")
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    (temporary / "value").write_text("loser", encoding="utf-8")

    terminal_packing._publish_immutable_directory(temporary, target)

    assert (target / "value").read_text(encoding="utf-8") == "winner"
    assert not temporary.exists()
