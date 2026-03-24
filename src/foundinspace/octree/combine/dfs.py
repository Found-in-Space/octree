from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from ..assembly.types import ShardKey
from .lookup import IntermediateLookup
from .manifest import read_combine_manifest


@dataclass(frozen=True, slots=True)
class CellPayloadRef:
    shard: ShardKey
    level: int
    node_id: int
    payload_offset: int
    payload_length: int
    star_count: int


def iter_cells_dfs(
    manifest_path: Path, *, max_open_files: int = 32
) -> Iterator[CellPayloadRef]:
    manifest = read_combine_manifest(manifest_path)
    lookup = IntermediateLookup(manifest, max_open_files=max_open_files)
    try:
        max_level = manifest.max_level
        if max_level < 0:
            return
        if not lookup.descendant_exists(0, 0, max_level):
            return
        yield from _walk_node(lookup, max_level=max_level, level=0, node_id=0)
    finally:
        lookup.close()


def _walk_node(
    lookup: IntermediateLookup, *, max_level: int, level: int, node_id: int
) -> Iterator[CellPayloadRef]:
    found = lookup.find_payload(level, node_id)
    if found is not None:
        shard, rec = found
        yield CellPayloadRef(
            shard=shard,
            level=level,
            node_id=node_id,
            payload_offset=int(rec[1]),
            payload_length=int(rec[2]),
            star_count=int(rec[3]),
        )

    if level >= max_level:
        return

    next_level = level + 1
    base = node_id << 3
    for octant in range(8):
        child_node = base | octant
        if lookup.descendant_exists(next_level, child_node, max_level):
            yield from _walk_node(
                lookup,
                max_level=max_level,
                level=next_level,
                node_id=child_node,
            )
