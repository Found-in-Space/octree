from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CellKey:
    level: int
    node_id: int


@dataclass(frozen=True, slots=True)
class ShardKey:
    level: int
    prefix_bits: int
    prefix: int


@dataclass(frozen=True, slots=True)
class EncodedCell:
    key: CellKey
    payload: bytes
    star_count: int
