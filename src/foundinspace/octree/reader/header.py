from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..combine.records import (
    HEADER_FMT,
    HEADER_MAGIC,
    HEADER_SIZE,
    HEADER_VERSION,
    SHARD_MAGIC,
)


@dataclass(frozen=True, slots=True)
class OctreeHeader:
    version: int
    index_offset: int
    index_length: int
    world_center: tuple[float, float, float]
    world_half_size: float
    payload_record_size: int
    max_level: int
    mag_limit: float


def read_header(path: Path) -> OctreeHeader:
    with open(path, "rb") as fp:
        header_bytes = fp.read(HEADER_SIZE)
        if len(header_bytes) != HEADER_SIZE:
            raise ValueError(
                f"Invalid STAR header size: expected {HEADER_SIZE}, got {len(header_bytes)}"
            )
        (
            magic,
            version,
            _flags,
            index_offset,
            index_length,
            center_x,
            center_y,
            center_z,
            world_half_size,
            payload_record_size,
            max_level,
            mag_limit,
            _reserved,
        ) = HEADER_FMT.unpack(header_bytes)
        if magic != HEADER_MAGIC:
            raise ValueError(f"Invalid STAR magic: expected {HEADER_MAGIC!r}, got {magic!r}")
        if version != HEADER_VERSION:
            raise ValueError(
                f"Unsupported STAR version: expected {HEADER_VERSION}, got {version}"
            )
        fp.seek(int(index_offset))
        shard_magic = fp.read(4)
        if shard_magic != SHARD_MAGIC:
            raise ValueError(
                "Invalid shard probe at index_offset: "
                f"expected {SHARD_MAGIC!r}, got {shard_magic!r}"
            )
    return OctreeHeader(
        version=int(version),
        index_offset=int(index_offset),
        index_length=int(index_length),
        world_center=(float(center_x), float(center_y), float(center_z)),
        world_half_size=float(world_half_size),
        payload_record_size=int(payload_record_size),
        max_level=int(max_level),
        mag_limit=float(mag_limit),
    )
