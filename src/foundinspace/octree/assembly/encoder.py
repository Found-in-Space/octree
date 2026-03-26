from __future__ import annotations

import gzip
from collections.abc import Iterator

from .types import CellKey, EncodedCell


def _flush_cell(level: int, node_id: int, renders: list[bytes]) -> EncodedCell:
    raw = b"".join(renders)
    payload = gzip.compress(raw)
    return EncodedCell(
        key=CellKey(level=level, node_id=node_id),
        payload=payload,
        star_count=len(renders),
    )


def iter_encoded_cells(
    rows: Iterator[tuple[int, bytes, str, str]],
    level: int,
) -> Iterator[EncodedCell]:
    """Group contiguous rows by node_id and yield one EncodedCell per cell.

    Rows must arrive ordered by (node_id, mag_abs, source_id).  Only
    ``node_id`` and ``render`` are used; ``source`` / ``source_id`` are ignored
    here (used by the metadata sidecar encoder).

    Flush occurs on node_id change or end-of-stream.  Only the current cell is
    held in memory.
    """
    current_node_id: int | None = None
    current_renders: list[bytes] = []

    for node_id, render, _source, _source_id in rows:
        if current_node_id is not None and node_id != current_node_id:
            yield _flush_cell(level, current_node_id, current_renders)
            current_renders = []
        current_node_id = node_id
        current_renders.append(render)

    if current_node_id is not None and current_renders:
        yield _flush_cell(level, current_node_id, current_renders)
