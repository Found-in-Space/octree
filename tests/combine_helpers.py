from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from pathlib import Path

from foundinspace.octree.assembly.formats import (
    IDENTIFIERS_ARTIFACT_KIND,
    IDENTIFIERS_INDEX_MAGIC,
    INDEX_MAGIC,
    RENDER_ARTIFACT_KIND,
    SIDECAR_ARTIFACT_KIND,
    SIDECAR_INDEX_MAGIC,
)
from foundinspace.octree.assembly.identity_encoder import encode_identity_rows
from foundinspace.octree.assembly.manifest import write_manifest
from foundinspace.octree.assembly.types import CellKey, EncodedCell, ShardKey
from foundinspace.octree.assembly.writer import (
    IntermediateShardWriter,
    identifiers_shard_filenames,
    sidecar_shard_filenames,
)


@dataclass(frozen=True, slots=True)
class PayloadNode:
    level: int
    node_id: int
    star_count: int
    raw_payload: bytes
    meta_entries: list[dict] | None = None
    identities: list[tuple[str, str]] | None = None


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
            writer.write_cell(
                EncodedCell(
                    key=CellKey(level=node.level, node_id=node.node_id),
                    payload=gzip.compress(node.raw_payload),
                    star_count=node.star_count,
                )
            )
        result = writer.close()
        if result is not None:
            entries.append(result)

    return write_manifest(
        tmp_path,
        max_level=max_level,
        shard_entries=entries,
        artifact_kind=RENDER_ARTIFACT_KIND,
        index_magic=INDEX_MAGIC,
        mag_limit=mag_limit,
    )


def build_sidecar_intermediates(
    tmp_path: Path,
    nodes: list[PayloadNode],
    max_level: int,
    *,
    mag_limit: float = 6.5,
    sidecar_kind: str = "meta",
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    nodes_by_level: dict[int, list[PayloadNode]] = {}
    for node in nodes:
        nodes_by_level.setdefault(node.level, []).append(node)

    entries: list[dict] = []
    for level, level_nodes in sorted(nodes_by_level.items()):
        shard = ShardKey(level=level, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(
            shard,
            tmp_path,
            index_magic=SIDECAR_INDEX_MAGIC,
            filename_fn=sidecar_shard_filenames(sidecar_kind),
        )
        for node in sorted(level_nodes, key=lambda n: n.node_id):
            meta_entries = (
                node.meta_entries
                if node.meta_entries is not None
                else [{}] * node.star_count
            )
            writer.write_cell(
                EncodedCell(
                    key=CellKey(level=node.level, node_id=node.node_id),
                    payload=gzip.compress(
                        json.dumps(meta_entries, separators=(",", ":")).encode()
                    ),
                    star_count=node.star_count,
                )
            )
        result = writer.close()
        if result is not None:
            entries.append(result)

    return write_manifest(
        tmp_path,
        max_level=max_level,
        shard_entries=entries,
        artifact_kind=SIDECAR_ARTIFACT_KIND,
        index_magic=SIDECAR_INDEX_MAGIC,
        mag_limit=mag_limit,
    )


def build_identifiers_intermediates(
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
        writer = IntermediateShardWriter(
            shard,
            tmp_path,
            index_magic=IDENTIFIERS_INDEX_MAGIC,
            filename_fn=identifiers_shard_filenames,
        )
        for node in sorted(level_nodes, key=lambda n: n.node_id):
            identities = (
                node.identities
                if node.identities is not None
                else [("gaia", str(i)) for i in range(node.star_count)]
            )
            writer.write_cell(
                EncodedCell(
                    key=CellKey(level=node.level, node_id=node.node_id),
                    payload=encode_identity_rows(identities),
                    star_count=node.star_count,
                )
            )
        result = writer.close()
        if result is not None:
            entries.append(result)

    return write_manifest(
        tmp_path,
        max_level=max_level,
        shard_entries=entries,
        artifact_kind=IDENTIFIERS_ARTIFACT_KIND,
        index_magic=IDENTIFIERS_INDEX_MAGIC,
        mag_limit=mag_limit,
    )
