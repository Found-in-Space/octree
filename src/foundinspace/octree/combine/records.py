from __future__ import annotations

import struct
from dataclasses import dataclass

from ..assembly.formats import INDEX_FILE_HDR, INDEX_HEADER_SIZE, INDEX_RECORD

HEADER_FMT = struct.Struct("<4sHHQQ3ffHHf16s")
HEADER_SIZE = 64
HEADER_MAGIC = b"STAR"
HEADER_VERSION = 1
HEADER_FLAGS = 0
PAYLOAD_RECORD_SIZE = 16
HEADER_RESERVED = b"\x00" * 16

SHARD_HDR_FMT = struct.Struct("<4sHBBIIHHHhIII8HHQQQ2x")
SHARD_HDR_SIZE = 80
SHARD_MAGIC = b"OSHR"
SHARD_VERSION = 1
LEVELS_PER_SHARD = 5
SHARD_FLAGS = 0

SHARD_NODE_FMT = struct.Struct("<HHBBBBQI")
SHARD_NODE_SIZE = 20

FRONTIER_REF_FMT = struct.Struct("<Q")
FRONTIER_REF_SIZE = 8

HAS_PAYLOAD = 0x01
HAS_CHILDREN = 0x02
IS_FRONTIER = 0x04

RELOC_MAGIC = b"ORLX"
RELOC_VERSION = 1
RELOC_HEADER_FMT = INDEX_FILE_HDR
RELOC_HEADER_SIZE = INDEX_HEADER_SIZE
RELOC_RECORD_FMT = INDEX_RECORD
RELOC_RECORD_SIZE = INDEX_RECORD.size

assert HEADER_FMT.size == HEADER_SIZE
assert SHARD_HDR_FMT.size == SHARD_HDR_SIZE
assert SHARD_NODE_FMT.size == SHARD_NODE_SIZE
assert FRONTIER_REF_FMT.size == FRONTIER_REF_SIZE


@dataclass(frozen=True, slots=True)
class PackedHeaderFields:
    world_center: tuple[float, float, float]
    world_half_size_pc: float
    max_level: int
    mag_limit: float
    index_offset: int = 0
    index_length: int = 0


def pack_top_level_header(fields: PackedHeaderFields) -> bytes:
    cx, cy, cz = fields.world_center
    return HEADER_FMT.pack(
        HEADER_MAGIC,
        HEADER_VERSION,
        HEADER_FLAGS,
        int(fields.index_offset),
        int(fields.index_length),
        float(cx),
        float(cy),
        float(cz),
        float(fields.world_half_size_pc),
        PAYLOAD_RECORD_SIZE,
        int(fields.max_level),
        float(fields.mag_limit),
        HEADER_RESERVED,
    )


def unpack_top_level_header(buf: bytes) -> tuple:
    if len(buf) != HEADER_SIZE:
        raise ValueError(f"Expected {HEADER_SIZE} bytes, got {len(buf)}")
    return HEADER_FMT.unpack(buf)


def pack_shard_header(
    *,
    shard_id: int,
    parent_shard_id: int,
    parent_node_index: int,
    node_count: int,
    parent_global_depth: int,
    parent_grid_x: int,
    parent_grid_y: int,
    parent_grid_z: int,
    entry_nodes: tuple[int, int, int, int, int, int, int, int],
    first_frontier_index: int,
    node_table_offset: int,
    frontier_table_offset: int,
    payload_base_offset: int,
) -> bytes:
    if node_count > 0xFFFF:
        raise ValueError(f"node_count exceeds u16: {node_count}")
    return SHARD_HDR_FMT.pack(
        SHARD_MAGIC,
        SHARD_VERSION,
        LEVELS_PER_SHARD,
        SHARD_FLAGS,
        int(shard_id),
        int(parent_shard_id),
        int(parent_node_index),
        int(node_count),
        0,  # reserved0
        int(parent_global_depth),
        int(parent_grid_x),
        int(parent_grid_y),
        int(parent_grid_z),
        *entry_nodes,
        int(first_frontier_index),
        int(node_table_offset),
        int(frontier_table_offset),
        int(payload_base_offset),
    )
