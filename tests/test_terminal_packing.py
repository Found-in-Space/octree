from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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
