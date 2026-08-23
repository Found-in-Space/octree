from __future__ import annotations

from foundinspace.octree.visibility import (
    DEFAULT_LOAD_FACTOR,
    half_size_at_level,
    load_radius_for_magnitude_shell,
    validate_load_factor,
)

from .header import OctreeHeader
from .index import NodeEntry, Point


def brightest_level_for_node(node: NodeEntry) -> int:
    """Use STAR v2 B metadata, falling back to emitted level for STAR v1."""
    return node.level if node.brightest_level is None else node.brightest_level


def should_prune_magnitude_node(
    *,
    header: OctreeHeader,
    node: NodeEntry,
    point: Point,
    limiting_magnitude: float,
    load_factor: float = DEFAULT_LOAD_FACTOR,
) -> bool:
    """Apply the octree-spec magnitude-shell node predicate."""
    factor = validate_load_factor(load_factor)
    brightest_half_size = half_size_at_level(
        header.world_half_size,
        brightest_level_for_node(node),
    )
    radius = load_radius_for_magnitude_shell(
        brightest_half_size,
        limiting_magnitude,
        header.mag_limit,
        load_factor=factor,
    )
    return node.aabb_distance(point) >= radius
