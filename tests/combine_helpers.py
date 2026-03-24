from __future__ import annotations

import gzip
from dataclasses import dataclass
from pathlib import Path

from foundinspace.octree.assembly.manifest import write_manifest
from foundinspace.octree.assembly.types import CellKey, EncodedCell, ShardKey
from foundinspace.octree.assembly.writer import IntermediateShardWriter


@dataclass(frozen=True, slots=True)
class PayloadNode:
    level: int
    node_id: int
    star_count: int
    raw_payload: bytes


def build_intermediates(
    tmp_path: Path,
    nodes: list[PayloadNode],
    max_level: int,
    *,
    mag_limit: float = 6.5,
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    nodes_by_level: dict[int, list[PayloadNode]] = {}
    for node in nodes:
        nodes_by_level.setdefault(node.level, []).append(node)

    entries: list[dict] = []
    for level, level_nodes in sorted(nodes_by_level.items()):
        shard = ShardKey(level=level, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        for node in sorted(level_nodes, key=lambda n: n.node_id):
            cell = EncodedCell(
                key=CellKey(level=node.level, node_id=node.node_id),
                payload=gzip.compress(node.raw_payload),
                star_count=node.star_count,
            )
            writer.write_cell(cell)
        result = writer.close()
        if result is not None:
            entries.append(result)

    return write_manifest(
        tmp_path,
        max_level=max_level,
        shard_entries=entries,
        mag_limit=mag_limit,
    )
