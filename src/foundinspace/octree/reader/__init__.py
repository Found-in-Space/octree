from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from .header import OctreeHeader, read_header
from .index import IndexNavigator, NodeEntry, Point
from .payload import Star, decode_payload


class OctreeReader:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._header = read_header(path)
        self._navigator = IndexNavigator(path, self._header)
        self._payload_fp: BinaryIO = open(path, "rb")

    def close(self) -> None:
        self._navigator.close()
        self._payload_fp.close()

    def __enter__(self) -> OctreeReader:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @property
    def header(self) -> OctreeHeader:
        return self._header

    def stars_brighter_than(
        self,
        point: Point,
        limiting_magnitude: float,
    ) -> Iterator[Star]:
        stack = list(self._navigator.root_entries())
        while stack:
            node = stack.pop()
            if self._should_prune_shell(
                node=node,
                point=point,
                limiting_magnitude=limiting_magnitude,
            ):
                continue
            if node.has_payload:
                for star in decode_payload(
                    self._payload_fp, node, self._header.payload_record_size
                ):
                    if star.apparent_magnitude_at(point) <= limiting_magnitude:
                        yield star
            self._push_children(stack, node)

    def stars_within_distance(
        self,
        point: Point,
        distance_pc: float,
    ) -> Iterator[Star]:
        if distance_pc < 0:
            raise ValueError(f"distance_pc must be >= 0, got {distance_pc}")
        stack = list(self._navigator.root_entries())
        while stack:
            node = stack.pop()
            if node.aabb_distance(point) > distance_pc:
                continue
            if node.has_payload:
                for star in decode_payload(
                    self._payload_fp, node, self._header.payload_record_size
                ):
                    if star.position.distance_to(point) <= distance_pc:
                        yield star
            self._push_children(stack, node)

    def _push_children(self, stack: list[NodeEntry], node: NodeEntry) -> None:
        for octant in range(7, -1, -1):
            if (node.child_mask & (1 << octant)) == 0:
                continue
            child = self._navigator.get_child(node, octant)
            if child is not None:
                stack.append(child)

    def _should_prune_shell(
        self,
        *,
        node: NodeEntry,
        point: Point,
        limiting_magnitude: float,
    ) -> bool:
        radius = node.half_size * (
            10.0 ** ((limiting_magnitude - self._header.mag_limit) / 5.0)
        )
        return node.aabb_distance(point) > radius


__all__ = [
    "IndexNavigator",
    "NodeEntry",
    "OctreeHeader",
    "OctreeReader",
    "Point",
    "Star",
    "decode_payload",
    "read_header",
]
