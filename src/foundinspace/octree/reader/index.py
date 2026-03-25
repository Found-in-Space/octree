from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from ..combine.records import (
    FRONTIER_REF_FMT,
    FRONTIER_REF_SIZE,
    HAS_PAYLOAD,
    IS_FRONTIER,
    LEVELS_PER_SHARD,
    SHARD_HDR_FMT,
    SHARD_HDR_SIZE,
    SHARD_MAGIC,
    SHARD_NODE_FMT,
    SHARD_NODE_SIZE,
    SHARD_VERSION,
)
from .header import OctreeHeader


@dataclass(frozen=True, slots=True)
class Point:
    x: float
    y: float
    z: float

    def distance_to(self, other: Point) -> float:
        dx = self.x - other.x
        dy = self.y - other.y
        dz = self.z - other.z
        return (dx * dx + dy * dy + dz * dz) ** 0.5


@dataclass(frozen=True, slots=True)
class GridCoord:
    x: int
    y: int
    z: int


@dataclass(frozen=True, slots=True)
class NodeEntry:
    level: int
    grid: GridCoord
    center: Point
    half_size: float
    flags: int
    child_mask: int
    payload_offset: int
    payload_length: int
    _shard_offset: int
    _node_index: int
    _first_child: int
    _local_depth: int
    _local_path: int

    @property
    def is_leaf(self) -> bool:
        return self.child_mask == 0

    @property
    def has_payload(self) -> bool:
        return bool(self.flags & HAS_PAYLOAD) and self.payload_length > 0

    def aabb_distance(self, point: Point) -> float:
        dx = max(abs(point.x - self.center.x) - self.half_size, 0.0)
        dy = max(abs(point.y - self.center.y) - self.half_size, 0.0)
        dz = max(abs(point.z - self.center.z) - self.half_size, 0.0)
        return (dx * dx + dy * dy + dz * dz) ** 0.5


@dataclass(frozen=True, slots=True)
class _ShardHeader:
    offset: int
    node_count: int
    parent_global_depth: int
    parent_grid: GridCoord
    entry_nodes: tuple[int, int, int, int, int, int, int, int]
    first_frontier_index: int
    node_table_offset: int
    frontier_table_offset: int


class IndexNavigator:
    def __init__(self, path: Path, header: OctreeHeader) -> None:
        self._path = path
        self._header = header
        self._fp: BinaryIO = open(path, "rb")

    def close(self) -> None:
        self._fp.close()

    def __enter__(self) -> IndexNavigator:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def root_entries(self) -> Iterator[NodeEntry]:
        root_shard = self._read_shard_header(self._header.index_offset)
        for octant in range(8):
            node_index = root_shard.entry_nodes[octant]
            if node_index > 0:
                yield self._read_node(root_shard, node_index)

    def get_child(self, parent: NodeEntry, octant: int) -> NodeEntry | None:
        if octant < 0 or octant > 7:
            raise ValueError(f"octant must be in [0, 7], got {octant}")
        if (parent.child_mask & (1 << octant)) == 0:
            return None

        shard = self._read_shard_header(parent._shard_offset)
        if parent.flags & IS_FRONTIER:
            return self._get_frontier_child(shard, parent, octant)

        if parent._first_child <= 0:
            return None
        rank = (parent.child_mask & ((1 << octant) - 1)).bit_count()
        child_index = parent._first_child + rank
        if child_index > shard.node_count:
            raise ValueError(
                f"Child index out of bounds: {child_index} > {shard.node_count}"
            )
        return self._read_node(shard, child_index)

    def find_node_at(self, point: Point, level: int) -> NodeEntry | None:
        if level < 0:
            return None
        node: NodeEntry | None = None
        for root in self.root_entries():
            if root.aabb_distance(point) == 0.0:
                node = root
                break
        if node is None:
            return None
        while node.level < level:
            octant = (
                (1 if point.x >= node.center.x else 0)
                | (2 if point.y >= node.center.y else 0)
                | (4 if point.z >= node.center.z else 0)
            )
            child = self.get_child(node, octant)
            if child is None:
                return None
            node = child
        return node if node.level == level else None

    def _get_frontier_child(
        self, shard: _ShardHeader, parent: NodeEntry, octant: int
    ) -> NodeEntry | None:
        if parent._local_depth != LEVELS_PER_SHARD:
            raise ValueError(
                f"Frontier flag on non-frontier depth node: {parent._local_depth}"
            )
        if shard.first_frontier_index <= 0:
            return None
        frontier_slot = parent._node_index - shard.first_frontier_index
        if frontier_slot < 0:
            raise ValueError(
                f"Frontier slot underflow for node index {parent._node_index}"
            )
        ref_offset = shard.frontier_table_offset + frontier_slot * FRONTIER_REF_SIZE
        self._fp.seek(ref_offset)
        raw = self._fp.read(FRONTIER_REF_SIZE)
        if len(raw) != FRONTIER_REF_SIZE:
            raise ValueError("Truncated frontier reference")
        (child_shard_offset,) = FRONTIER_REF_FMT.unpack(raw)
        if child_shard_offset == 0:
            return None
        child_shard = self._read_shard_header(int(child_shard_offset))
        child_index = child_shard.entry_nodes[octant]
        if child_index <= 0:
            return None
        return self._read_node(child_shard, child_index)

    def _read_shard_header(self, offset: int) -> _ShardHeader:
        self._fp.seek(offset)
        raw = self._fp.read(SHARD_HDR_SIZE)
        if len(raw) != SHARD_HDR_SIZE:
            raise ValueError(f"Truncated shard header at offset {offset}")
        fields = SHARD_HDR_FMT.unpack(raw)
        magic = fields[0]
        version = fields[1]
        if magic != SHARD_MAGIC:
            raise ValueError(
                f"Invalid shard magic at offset {offset}: expected {SHARD_MAGIC!r}, got {magic!r}"
            )
        if version != SHARD_VERSION:
            raise ValueError(
                f"Unsupported shard version at offset {offset}: expected {SHARD_VERSION}, got {version}"
            )
        entry_nodes = tuple(int(v) for v in fields[13:21])
        return _ShardHeader(
            offset=int(offset),
            node_count=int(fields[7]),
            parent_global_depth=int(fields[9]),
            parent_grid=GridCoord(int(fields[10]), int(fields[11]), int(fields[12])),
            entry_nodes=(
                entry_nodes[0],
                entry_nodes[1],
                entry_nodes[2],
                entry_nodes[3],
                entry_nodes[4],
                entry_nodes[5],
                entry_nodes[6],
                entry_nodes[7],
            ),
            first_frontier_index=int(fields[21]),
            node_table_offset=int(fields[22]),
            frontier_table_offset=int(fields[23]),
        )

    def _read_node(self, shard: _ShardHeader, node_index: int) -> NodeEntry:
        if node_index <= 0 or node_index > shard.node_count:
            raise ValueError(
                f"Node index out of range for shard at {shard.offset}: {node_index}"
            )
        node_offset = shard.node_table_offset + (node_index - 1) * SHARD_NODE_SIZE
        self._fp.seek(node_offset)
        raw = self._fp.read(SHARD_NODE_SIZE)
        if len(raw) != SHARD_NODE_SIZE:
            raise ValueError(
                f"Truncated node record at shard {shard.offset}, node {node_index}"
            )
        (
            first_child,
            local_path,
            child_mask,
            local_depth,
            flags,
            _reserved,
            payload_offset,
            payload_length,
        ) = SHARD_NODE_FMT.unpack(raw)

        global_level = shard.parent_global_depth + int(local_depth)
        grid = self._decode_local_grid(
            shard.parent_grid,
            local_depth=int(local_depth),
            local_path=int(local_path),
        )
        center, half_size = self._node_geometry(grid, global_level)
        return NodeEntry(
            level=int(global_level),
            grid=grid,
            center=center,
            half_size=half_size,
            flags=int(flags),
            child_mask=int(child_mask),
            payload_offset=int(payload_offset),
            payload_length=int(payload_length),
            _shard_offset=shard.offset,
            _node_index=int(node_index),
            _first_child=int(first_child),
            _local_depth=int(local_depth),
            _local_path=int(local_path),
        )

    def _node_geometry(self, grid: GridCoord, level: int) -> tuple[Point, float]:
        n = 1 << level
        half_size = self._header.world_half_size / n
        world_cx, world_cy, world_cz = self._header.world_center
        center = Point(
            x=world_cx + (2.0 * (grid.x + 0.5) - n) * half_size,
            y=world_cy + (2.0 * (grid.y + 0.5) - n) * half_size,
            z=world_cz + (2.0 * (grid.z + 0.5) - n) * half_size,
        )
        return center, float(half_size)

    @staticmethod
    def _decode_local_grid(
        parent_grid: GridCoord, *, local_depth: int, local_path: int
    ) -> GridCoord:
        gx = parent_grid.x
        gy = parent_grid.y
        gz = parent_grid.z
        for i in range(local_depth):
            shift = 3 * (local_depth - 1 - i)
            octant = (local_path >> shift) & 0x7
            gx = (gx << 1) | (octant & 0x1)
            gy = (gy << 1) | ((octant >> 1) & 0x1)
            gz = (gz << 1) | ((octant >> 2) & 0x1)
        return GridCoord(gx, gy, gz)
