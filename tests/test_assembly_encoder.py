from __future__ import annotations

import gzip

from foundinspace.octree.assembly.encoder import iter_encoded_cells
from foundinspace.octree.assembly.types import CellKey


def _row(node_id: int, render: bytes) -> tuple[int, bytes, str, str]:
    return (node_id, render, "gaia", str(node_id))


class TestIterEncodedCells:
    def test_single_cell(self):
        rows = [_row(42, b"\x00" * 16), _row(42, b"\x01" * 16)]
        cells = list(iter_encoded_cells(iter(rows), level=5))
        assert len(cells) == 1
        assert cells[0].key == CellKey(level=5, node_id=42)
        assert cells[0].star_count == 2
        raw = gzip.decompress(cells[0].payload)
        assert raw == b"\x00" * 16 + b"\x01" * 16

    def test_multiple_cells(self):
        rows = [
            _row(1, b"\xaa" * 16),
            _row(2, b"\xbb" * 16),
            _row(2, b"\xcc" * 16),
        ]
        cells = list(iter_encoded_cells(iter(rows), level=3))
        assert len(cells) == 2
        assert cells[0].key.node_id == 1
        assert cells[0].star_count == 1
        assert cells[1].key.node_id == 2
        assert cells[1].star_count == 2

    def test_empty_stream(self):
        cells = list(iter_encoded_cells(iter([]), level=0))
        assert cells == []

    def test_deterministic_payload(self):
        rows = [_row(10, b"\xff" * 16)] * 5
        c1 = list(iter_encoded_cells(iter(rows), level=1))
        c2 = list(iter_encoded_cells(iter(rows), level=1))
        assert c1[0].payload == c2[0].payload

    def test_preserves_row_order(self):
        r1 = bytes(range(16))
        r2 = bytes(range(16, 32))
        r3 = bytes(range(32, 48))
        rows = [_row(7, r1), _row(7, r2), _row(7, r3)]
        cells = list(iter_encoded_cells(iter(rows), level=2))
        raw = gzip.decompress(cells[0].payload)
        assert raw == r1 + r2 + r3

    def test_many_cells_sequential(self):
        rows = [_row(i, b"\x00" * 16) for i in range(100)]
        cells = list(iter_encoded_cells(iter(rows), level=4))
        assert len(cells) == 100
        assert all(c.star_count == 1 for c in cells)
        assert [c.key.node_id for c in cells] == list(range(100))
