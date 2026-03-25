from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .header import OctreeHeader, read_header
from .index import IndexNavigator, NodeEntry, Point
from .payload import decode_payload


@dataclass(frozen=True, slots=True)
class LevelStats:
    level: int
    nodes: int
    stars_loaded: int
    stars_rendered: int
    payload_bytes: int


@dataclass(frozen=True, slots=True)
class CoalesceStats:
    input_ranges: int
    output_batches: int
    raw_payload_bytes: int
    total_span_bytes: int
    largest_batch_bytes: int


@dataclass(frozen=True, slots=True)
class NearestStar:
    star_id: int
    distance_pc: float
    magnitude: float
    apparent_magnitude: float
    teff: float


@dataclass(frozen=True, slots=True)
class StatsReport:
    header: OctreeHeader
    by_level: tuple[LevelStats, ...]
    totals: LevelStats
    coalesced: CoalesceStats
    nearest: tuple[NearestStar, ...]


@dataclass(slots=True)
class _MutableLevelStats:
    nodes: int = 0
    stars_loaded: int = 0
    stars_rendered: int = 0
    payload_bytes: int = 0


def collect_stats(
    path: Path,
    *,
    point: Point,
    limiting_magnitude: float,
    radius_pc: float,
    nearest_n: int = 10,
    coalesce_gap_bytes: int = 64 * 1024,
) -> StatsReport:
    header = read_header(path)
    with IndexNavigator(path, header) as nav, open(path, "rb") as payload_fp:
        by_level, touched_ranges = _collect_shell_level_stats(
            nav,
            payload_fp,
            header,
            point=point,
            limiting_magnitude=limiting_magnitude,
        )
        nearest = _collect_nearest(
            nav,
            payload_fp,
            header,
            point=point,
            radius_pc=radius_pc,
            nearest_n=nearest_n,
        )

    level_rows = tuple(
        LevelStats(
            level=level,
            nodes=stats.nodes,
            stars_loaded=stats.stars_loaded,
            stars_rendered=stats.stars_rendered,
            payload_bytes=stats.payload_bytes,
        )
        for level, stats in sorted(by_level.items())
    )
    totals = LevelStats(
        level=-1,
        nodes=sum(row.nodes for row in level_rows),
        stars_loaded=sum(row.stars_loaded for row in level_rows),
        stars_rendered=sum(row.stars_rendered for row in level_rows),
        payload_bytes=sum(row.payload_bytes for row in level_rows),
    )
    coalesced = coalesce_payload_ranges(
        touched_ranges,
        merge_gap_bytes=coalesce_gap_bytes,
    )
    return StatsReport(
        header=header,
        by_level=level_rows,
        totals=totals,
        coalesced=coalesced,
        nearest=tuple(nearest),
    )


def coalesce_payload_ranges(
    ranges: list[tuple[int, int]],
    *,
    merge_gap_bytes: int,
) -> CoalesceStats:
    filtered = [(start, length) for start, length in ranges if length > 0]
    if not filtered:
        return CoalesceStats(
            input_ranges=0,
            output_batches=0,
            raw_payload_bytes=0,
            total_span_bytes=0,
            largest_batch_bytes=0,
        )
    sorted_ranges = sorted(filtered, key=lambda r: r[0])
    merged: list[tuple[int, int]] = []
    cur_start = sorted_ranges[0][0]
    cur_end = sorted_ranges[0][0] + sorted_ranges[0][1]
    for start, length in sorted_ranges[1:]:
        end = start + length
        if start <= cur_end + merge_gap_bytes:
            cur_end = max(cur_end, end)
            continue
        merged.append((cur_start, cur_end))
        cur_start = start
        cur_end = end
    merged.append((cur_start, cur_end))

    spans = [end - start for start, end in merged]
    return CoalesceStats(
        input_ranges=len(sorted_ranges),
        output_batches=len(merged),
        raw_payload_bytes=sum(length for _, length in sorted_ranges),
        total_span_bytes=sum(spans),
        largest_batch_bytes=max(spans) if spans else 0,
    )


def _collect_shell_level_stats(
    nav: IndexNavigator,
    payload_fp: BinaryIO,
    header: OctreeHeader,
    *,
    point: Point,
    limiting_magnitude: float,
) -> tuple[dict[int, _MutableLevelStats], list[tuple[int, int]]]:
    by_level: dict[int, _MutableLevelStats] = {}
    payload_ranges: list[tuple[int, int]] = []
    stack = list(nav.root_entries())
    while stack:
        node = stack.pop()
        load_radius = node.half_size * (
            10.0 ** ((limiting_magnitude - header.mag_limit) / 5.0)
        )
        if node.aabb_distance(point) > load_radius:
            continue
        level_stats = by_level.setdefault(node.level, _MutableLevelStats())
        level_stats.nodes += 1
        if node.has_payload:
            stars = decode_payload(payload_fp, node, header.payload_record_size)
            level_stats.stars_loaded += len(stars)
            level_stats.stars_rendered += sum(
                1
                for star in stars
                if star.apparent_magnitude_at(point) <= limiting_magnitude
            )
            level_stats.payload_bytes += node.payload_length
            payload_ranges.append((node.payload_offset, node.payload_length))
        _push_children(nav, stack, node)
    return by_level, payload_ranges


def _collect_nearest(
    nav: IndexNavigator,
    payload_fp: BinaryIO,
    header: OctreeHeader,
    *,
    point: Point,
    radius_pc: float,
    nearest_n: int,
) -> list[NearestStar]:
    if radius_pc < 0:
        raise ValueError(f"radius_pc must be >= 0, got {radius_pc}")
    if nearest_n <= 0:
        return []

    stack = list(nav.root_entries())
    nearest: list[NearestStar] = []
    star_id = 0
    while stack:
        node = stack.pop()
        if node.aabb_distance(point) > radius_pc:
            continue
        if node.has_payload:
            stars = decode_payload(payload_fp, node, header.payload_record_size)
            for star in stars:
                distance_pc = star.position.distance_to(point)
                this_id = star_id
                star_id += 1
                if distance_pc > radius_pc:
                    continue
                nearest.append(
                    NearestStar(
                        star_id=this_id,
                        distance_pc=distance_pc,
                        magnitude=star.magnitude,
                        apparent_magnitude=star.apparent_magnitude_at(point),
                        teff=star.teff,
                    )
                )
        _push_children(nav, stack, node)
    nearest.sort(key=lambda s: s.distance_pc)
    return nearest[:nearest_n]


def _push_children(nav: IndexNavigator, stack: list[NodeEntry], node: NodeEntry) -> None:
    for octant in range(7, -1, -1):
        if (node.child_mask & (1 << octant)) == 0:
            continue
        child = nav.get_child(node, octant)
        if child is not None:
            stack.append(child)
