from __future__ import annotations

import struct
from dataclasses import dataclass
from uuid import UUID

from ..assembly.formats import INDEX_FILE_HDR, INDEX_HEADER_SIZE, INDEX_RECORD

HEADER_FMT = struct.Struct("<4sHHQQ3ffHHf16s")
HEADER_SIZE = 64
HEADER_MAGIC = b"STAR"
HEADER_VERSION = 1
HEADER_FLAGS = 0
PAYLOAD_RECORD_SIZE = 16
HEADER_RESERVED = b"\x00" * 16
DESCRIPTOR_FMT = struct.Struct("<4sHH16s16s16s32s40x")
DESCRIPTOR_SIZE = 128
DESCRIPTOR_MAGIC = b"ODSC"
DESCRIPTOR_VERSION = 1
RENDER_DESCRIPTOR_KIND = 1
SIDECAR_DESCRIPTOR_KIND = 2

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
assert DESCRIPTOR_FMT.size == DESCRIPTOR_SIZE
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


@dataclass(frozen=True, slots=True)
class PackedDescriptorFields:
    artifact_kind: str
    dataset_uuid: UUID | None = None
    parent_dataset_uuid: UUID | None = None
    sidecar_uuid: UUID | None = None
    sidecar_kind: str | None = None


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


def _descriptor_kind_code(kind: str) -> int:
    if kind == "render":
        return RENDER_DESCRIPTOR_KIND
    if kind == "sidecar":
        return SIDECAR_DESCRIPTOR_KIND
    raise ValueError(f"Unsupported descriptor artifact kind: {kind!r}")


def _descriptor_kind_name(code: int) -> str:
    if code == RENDER_DESCRIPTOR_KIND:
        return "render"
    if code == SIDECAR_DESCRIPTOR_KIND:
        return "sidecar"
    raise ValueError(f"Unsupported descriptor artifact code: {code}")


def _uuid_bytes(value: UUID | None) -> bytes:
    return (value or UUID(int=0)).bytes


def pack_descriptor(fields: PackedDescriptorFields) -> bytes:
    sidecar_kind = (fields.sidecar_kind or "").encode("utf-8")
    if len(sidecar_kind) > 32:
        raise ValueError("sidecar_kind exceeds 32 UTF-8 bytes")
    return DESCRIPTOR_FMT.pack(
        DESCRIPTOR_MAGIC,
        DESCRIPTOR_VERSION,
        _descriptor_kind_code(fields.artifact_kind),
        _uuid_bytes(fields.dataset_uuid),
        _uuid_bytes(fields.parent_dataset_uuid),
        _uuid_bytes(fields.sidecar_uuid),
        sidecar_kind.ljust(32, b"\x00"),
    )


def unpack_descriptor(buf: bytes) -> PackedDescriptorFields:
    if len(buf) != DESCRIPTOR_SIZE:
        raise ValueError(f"Expected {DESCRIPTOR_SIZE} bytes, got {len(buf)}")
    (
        magic,
        version,
        kind_code,
        dataset_uuid_raw,
        parent_dataset_uuid_raw,
        sidecar_uuid_raw,
        sidecar_kind_raw,
    ) = DESCRIPTOR_FMT.unpack(buf)
    if magic != DESCRIPTOR_MAGIC:
        raise ValueError(f"Invalid descriptor magic: {magic!r}")
    if version != DESCRIPTOR_VERSION:
        raise ValueError(f"Unsupported descriptor version: {version}")
    dataset_uuid = UUID(bytes=dataset_uuid_raw)
    parent_dataset_uuid = UUID(bytes=parent_dataset_uuid_raw)
    sidecar_uuid = UUID(bytes=sidecar_uuid_raw)
    sidecar_kind = sidecar_kind_raw.rstrip(b"\x00").decode("utf-8") or None
    return PackedDescriptorFields(
        artifact_kind=_descriptor_kind_name(kind_code),
        dataset_uuid=None if dataset_uuid.int == 0 else dataset_uuid,
        parent_dataset_uuid=None
        if parent_dataset_uuid.int == 0
        else parent_dataset_uuid,
        sidecar_uuid=None if sidecar_uuid.int == 0 else sidecar_uuid,
        sidecar_kind=sidecar_kind,
    )


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
