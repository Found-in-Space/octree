from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from pathlib import Path

from foundinspace.octree.assembly.formats import META_INDEX_MAGIC
from foundinspace.octree.assembly.manifest import write_manifest
from foundinspace.octree.assembly.types import CellKey, EncodedCell, ShardKey
from foundinspace.octree.assembly.writer import (
    IntermediateShardWriter,
    meta_shard_filenames,
)


@dataclass(frozen=True, slots=True)
class PayloadNode:
    level: int
    node_id: int
    star_count: int
    raw_payload: bytes
    meta_entries: list[dict] | None = None


def build_intermediates(
    tmp_path: Path,
    nodes: list[PayloadNode],
    max_level: int,
    *,
    mag_limit: float = 6.5,
    with_meta: bool = False,
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    nodes_by_level: dict[int, list[PayloadNode]] = {}
    for node in nodes:
        nodes_by_level.setdefault(node.level, []).append(node)

    entries: list[dict] = []
    for level, level_nodes in sorted(nodes_by_level.items()):
        shard = ShardKey(level=level, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        meta_writer: IntermediateShardWriter | None = None
        if with_meta:
            meta_writer = IntermediateShardWriter(
                shard,
                tmp_path,
                index_magic=META_INDEX_MAGIC,
                filename_fn=meta_shard_filenames,
                manifest_index_key="meta_index_path",
                manifest_payload_key="meta_payload_path",
            )
        for node in sorted(level_nodes, key=lambda n: n.node_id):
            cell = EncodedCell(
                key=CellKey(level=node.level, node_id=node.node_id),
                payload=gzip.compress(node.raw_payload),
                star_count=node.star_count,
            )
            writer.write_cell(cell)
            if meta_writer is not None:
                meta_entries = node.meta_entries
                if meta_entries is None:
                    meta_entries = [{}] * node.star_count
                meta_payload = gzip.compress(
                    json.dumps(meta_entries, separators=(",", ":")).encode()
                )
                meta_writer.write_cell(
                    EncodedCell(
                        key=CellKey(level=node.level, node_id=node.node_id),
                        payload=meta_payload,
                        star_count=node.star_count,
                    )
                )
        result = writer.close()
        meta_result = meta_writer.close() if meta_writer is not None else None
        if result is not None:
            if meta_result is not None:
                result = {
                    **result,
                    "meta_index_path": meta_result["meta_index_path"],
                    "meta_payload_path": meta_result["meta_payload_path"],
                }
            entries.append(result)

    return write_manifest(
        tmp_path,
        max_level=max_level,
        shard_entries=entries,
        mag_limit=mag_limit,
    )
