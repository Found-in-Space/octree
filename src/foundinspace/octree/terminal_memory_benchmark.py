from __future__ import annotations

import bisect
import gzip
import hashlib
import json
import math
import struct
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from foundinspace.octree.packing.records import SHARD_NODE_SIZE
from foundinspace.octree.reader.header import OctreeHeader, read_header
from foundinspace.octree.reader.index import IndexNavigator, NodeEntry, Point
from foundinspace.octree.reader.payload import STAR_RECORD_FMT
from foundinspace.octree.reader.source import OctreeSource, is_url_source

BENCHMARK_FORMAT = "foundinspace.octree.terminal-memory-benchmark/v0"
SAMPLE_CACHE_FORMAT = "foundinspace.octree.terminal-memory-sample/v0"

V2_INDEX_RECORD_BYTES = 24
TERMINAL_DIRECTORY_RECORD_BYTES = 24
RAW_PAYLOAD_BYTES_PER_STAR = STAR_RECORD_FMT.size
DECODED_BYTES_PER_STAR = 17
TRANSFORMED_POSITION_BYTES_PER_STAR = 12
RENDERER_CPU_BYTES_PER_STAR = 17
RENDERER_GPU_BYTES_PER_STAR = 17
LIVE_BYTES_PER_STAR = (
    RAW_PAYLOAD_BYTES_PER_STAR
    + DECODED_BYTES_PER_STAR
    + TRANSFORMED_POSITION_BYTES_PER_STAR
    + RENDERER_CPU_BYTES_PER_STAR
    + RENDERER_GPU_BYTES_PER_STAR
)
TRANSITION_OVERLAP_BYTES_PER_STAR = (
    TRANSFORMED_POSITION_BYTES_PER_STAR
    + RENDERER_CPU_BYTES_PER_STAR
    + RENDERER_GPU_BYTES_PER_STAR
)

DEFAULT_WATERLINES = (512, 1000, 2000, 4000, 8000)
DEFAULT_CHUNK_STAR_COUNTS = (128, 256, 512, 1000)
DEFAULT_DECODED_CACHE_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_INFLIGHT_PAYLOADS = 8
DEFAULT_WORKERS = 16

_MAG_CENTI = struct.Struct("<h")


@dataclass(frozen=True, slots=True)
class SampleSpec:
    name: str
    point: Point
    level: int


@dataclass(frozen=True, slots=True)
class TraceView:
    name: str
    observer: Point
    limiting_magnitude: float


@dataclass(frozen=True, slots=True)
class TerminalMemoryBenchmarkConfig:
    source: OctreeSource
    samples: tuple[SampleSpec, ...]
    views: tuple[TraceView, ...] = ()
    waterlines: tuple[int, ...] = DEFAULT_WATERLINES
    chunk_star_counts: tuple[int, ...] = DEFAULT_CHUNK_STAR_COUNTS
    decoded_cache_bytes: int = DEFAULT_DECODED_CACHE_BYTES
    terminal_directory_record_bytes: int = TERMINAL_DIRECTORY_RECORD_BYTES
    max_inflight_payloads: int = DEFAULT_MAX_INFLIGHT_PAYLOADS
    workers: int = DEFAULT_WORKERS
    cache_dir: Path | None = None


NodeKey = tuple[int, int, int, int]


@dataclass(frozen=True, slots=True)
class LogicalNode:
    key: NodeKey
    center: Point
    half_size: float
    payload_offset: int
    payload_length: int
    magnitudes_centi: tuple[int, ...] = ()
    children: tuple[LogicalNode, ...] = ()
    subtree_star_count: int = 0
    subtree_node_count: int = 1
    subtree_payload_count: int = 0

    @property
    def level(self) -> int:
        return self.key[0]

    @property
    def natural_star_count(self) -> int:
        return len(self.magnitudes_centi)

    @property
    def has_payload(self) -> bool:
        return self.natural_star_count > 0

    def aabb_distance(self, point: Point) -> float:
        dx = max(abs(point.x - self.center.x) - self.half_size, 0.0)
        dy = max(abs(point.y - self.center.y) - self.half_size, 0.0)
        dz = max(abs(point.z - self.center.z) - self.half_size, 0.0)
        return math.sqrt(dx * dx + dy * dy + dz * dz)


@dataclass(frozen=True, slots=True)
class ExtractedSample:
    spec: SampleSpec
    source: str
    index_magnitude: float
    root: LogicalNode


@dataclass(frozen=True, slots=True)
class TerminalPlanNode:
    logical: LogicalNode
    terminal: bool
    children: tuple[TerminalPlanNode, ...] = ()


@dataclass(frozen=True, slots=True)
class StorageEntry:
    key: str
    rows: int
    compressed_bytes: int


@dataclass(slots=True)
class _Selection:
    inspected_keys: set[NodeKey] = field(default_factory=set)
    payload_nodes: list[LogicalNode] = field(default_factory=list)
    loaded_terminals: set[NodeKey] = field(default_factory=set)


@dataclass(slots=True)
class _ReplayState:
    raw_cache: dict[str, StorageEntry] = field(default_factory=dict)
    decoded_cache: OrderedDict[str, StorageEntry] = field(default_factory=OrderedDict)
    decoded_cache_bytes: int = 0
    inspected_keys: set[NodeKey] = field(default_factory=set)
    loaded_terminal_keys: set[NodeKey] = field(default_factory=set)
    previous_active_entries: dict[str, StorageEntry] = field(default_factory=dict)
    previous_active_rows: int = 0
    total_fetch_units: int = 0
    total_compressed_bytes: int = 0
    minimum_geometry_rebuild_bytes: int = 0


def parse_sample_spec(value: str) -> SampleSpec:
    """Parse ``NAME:X,Y,Z@LEVEL`` into a sample specification."""
    try:
        name, raw_location = value.split(":", 1)
        raw_point, raw_level = raw_location.rsplit("@", 1)
        parts = [part.strip() for part in raw_point.split(",")]
        if len(parts) != 3:
            raise ValueError("point must contain exactly three values")
        point = Point(*(float(part) for part in parts))
        level = int(raw_level)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid sample '{value}'; expected NAME:X,Y,Z@LEVEL"
        ) from exc
    if not name.strip():
        raise ValueError("Sample name must not be empty")
    if level < 0:
        raise ValueError("Sample level must be >= 0")
    return SampleSpec(name=name.strip(), point=point, level=level)


def load_trace(path: Path) -> tuple[TraceView, ...]:
    """Load a trace JSON document containing an ordered ``views`` array."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not isinstance(raw.get("views"), list):
        raise ValueError("Trace JSON must contain a views array")
    views: list[TraceView] = []
    for index, item in enumerate(raw["views"]):
        if not isinstance(item, dict):
            raise ValueError(f"Trace view {index} must be an object")
        point_raw = item.get("observer_pc")
        if not isinstance(point_raw, list | tuple) or len(point_raw) != 3:
            raise ValueError(f"Trace view {index}.observer_pc must contain 3 values")
        try:
            observer = Point(*(float(value) for value in point_raw))
            limiting_magnitude = float(item["limiting_magnitude"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"Trace view {index} requires a finite limiting_magnitude"
            ) from exc
        if not math.isfinite(limiting_magnitude):
            raise ValueError(f"Trace view {index}.limiting_magnitude must be finite")
        name = str(item.get("name", f"view-{index + 1}")).strip()
        views.append(
            TraceView(
                name=name or f"view-{index + 1}",
                observer=observer,
                limiting_magnitude=limiting_magnitude,
            )
        )
    if not views:
        raise ValueError("Trace JSON must contain at least one view")
    return tuple(views)


def run_terminal_memory_benchmark(
    config: TerminalMemoryBenchmarkConfig,
) -> dict[str, Any]:
    """Run the sample-scoped virtual STAR v2 memory benchmark."""
    _validate_config(config)
    header = read_header(config.source)
    if header.artifact_kind != "render":
        raise ValueError(
            "Terminal memory benchmark requires a render octree, "
            f"got {header.artifact_kind!r}"
        )
    if header.payload_record_size != STAR_RECORD_FMT.size:
        raise ValueError(
            "Terminal memory benchmark requires "
            f"{STAR_RECORD_FMT.size}-byte star records, "
            f"got {header.payload_record_size}"
        )

    sample_reports: list[dict[str, Any]] = []
    for spec in config.samples:
        sample = extract_sample(
            config.source,
            header,
            spec,
            workers=config.workers,
            cache_dir=config.cache_dir,
        )
        views = config.views or (
            TraceView(
                name=spec.name,
                observer=spec.point,
                limiting_magnitude=header.mag_limit,
            ),
        )
        sample_reports.append(_benchmark_sample(sample, views, config))

    return {
        "format": BENCHMARK_FORMAT,
        "scope": "sample-subtrees",
        "source": str(config.source),
        "header": {
            "version": header.version,
            "index_magnitude": header.mag_limit,
            "max_level": header.max_level,
            "world_half_size_pc": header.world_half_size,
        },
        "assumptions": {
            "v1_index_record_bytes": SHARD_NODE_SIZE,
            "v2_index_record_bytes": V2_INDEX_RECORD_BYTES,
            "terminal_directory_record_bytes": (config.terminal_directory_record_bytes),
            "raw_payload_bytes_per_star": RAW_PAYLOAD_BYTES_PER_STAR,
            "decoded_bytes_per_star": DECODED_BYTES_PER_STAR,
            "transformed_position_bytes_per_star": (
                TRANSFORMED_POSITION_BYTES_PER_STAR
            ),
            "renderer_cpu_bytes_per_star": RENDERER_CPU_BYTES_PER_STAR,
            "renderer_gpu_bytes_per_star": RENDERER_GPU_BYTES_PER_STAR,
            "live_bytes_per_star": LIVE_BYTES_PER_STAR,
            "decoded_cache_bytes": config.decoded_cache_bytes,
            "raw_cache_policy": "unbounded",
            "index_cache_policy": "unbounded-record-model",
            "compressed_terminal_bytes": "sum-of-v1-members",
            "chunk_compressed_bytes": "proportional-v1-estimate",
            "transition_peak": (
                "resident + prior live cell/CPU/GPU overlap + "
                "largest inflight compressed/inflate scratch"
            ),
        },
        "waterlines": list(config.waterlines),
        "chunk_star_counts": list(config.chunk_star_counts),
        "samples": sample_reports,
    }


def extract_sample(
    source: OctreeSource,
    header: OctreeHeader,
    spec: SampleSpec,
    *,
    workers: int = DEFAULT_WORKERS,
    cache_dir: Path | None = None,
) -> ExtractedSample:
    """Extract one complete published-octree subtree and its magnitude ordering."""
    cache_path = (
        _sample_cache_path(cache_dir, source, header, spec)
        if cache_dir is not None
        else None
    )
    if cache_path is not None and cache_path.is_file():
        return _read_sample_cache(cache_path, source, spec)

    payload_nodes: list[NodeEntry] = []
    with IndexNavigator(source, header) as navigator:
        start = navigator.find_node_at(spec.point, spec.level)
        if start is None:
            raise ValueError(
                f"No node at level {spec.level} contains sample point "
                f"{spec.point.x},{spec.point.y},{spec.point.z}"
            )
        topology = _crawl_topology(navigator, start, payload_nodes)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        payload_data = dict(
            executor.map(
                lambda node: _read_payload_magnitudes(source, node),
                payload_nodes,
            )
        )
    root = _hydrate_topology(topology, payload_data)
    sample = ExtractedSample(
        spec=spec,
        source=str(source),
        index_magnitude=header.mag_limit,
        root=root,
    )
    if cache_path is not None:
        _write_sample_cache(cache_path, sample)
    return sample


def build_terminal_plan(root: LogicalNode, waterline: int) -> TerminalPlanNode:
    """Collapse every positive subtree at or below ``waterline`` into a terminal."""
    if waterline <= 0:
        raise ValueError("waterline must be > 0")
    if root.children and 0 < root.subtree_star_count <= waterline:
        return TerminalPlanNode(logical=root, terminal=True)
    return TerminalPlanNode(
        logical=root,
        terminal=False,
        children=tuple(
            build_terminal_plan(child, waterline) for child in root.children
        ),
    )


def safe_magnitude_prefix_count(node: LogicalNode, view: TraceView) -> int:
    """Count a conservative absolute-magnitude prefix for a logical cell."""
    if not node.magnitudes_centi:
        return 0
    threshold_centi = _safe_magnitude_threshold_centi(node, view)
    if threshold_centi is None:
        return node.natural_star_count
    return bisect.bisect_right(node.magnitudes_centi, threshold_centi)


def _safe_magnitude_threshold_centi(
    node: LogicalNode,
    view: TraceView,
) -> int | None:
    minimum_distance_pc = node.aabb_distance(view.observer)
    if minimum_distance_pc <= 0.0:
        return None
    max_absolute_magnitude = view.limiting_magnitude - 5.0 * (
        math.log10(max(minimum_distance_pc, 1e-6)) - 1.0
    )
    return math.floor(max_absolute_magnitude * 100.0 + 1e-9)


def report_to_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


def _validate_config(config: TerminalMemoryBenchmarkConfig) -> None:
    if not config.samples:
        raise ValueError("At least one sample is required")
    if len({sample.name for sample in config.samples}) != len(config.samples):
        raise ValueError("Sample names must be unique")
    for label, values in (
        ("waterline", config.waterlines),
        ("chunk star count", config.chunk_star_counts),
    ):
        if not values or any(value <= 0 for value in values):
            raise ValueError(f"Every {label} must be > 0")
    if config.decoded_cache_bytes < 0:
        raise ValueError("decoded_cache_bytes must be >= 0")
    if config.terminal_directory_record_bytes < 0:
        raise ValueError("terminal_directory_record_bytes must be >= 0")
    if config.max_inflight_payloads <= 0:
        raise ValueError("max_inflight_payloads must be > 0")
    if config.workers <= 0:
        raise ValueError("workers must be > 0")


@dataclass(frozen=True, slots=True)
class _TopologyNode:
    entry: NodeEntry
    children: tuple[_TopologyNode, ...]


def _crawl_topology(
    navigator: IndexNavigator,
    node: NodeEntry,
    payload_nodes: list[NodeEntry],
) -> _TopologyNode:
    if node.has_payload:
        payload_nodes.append(node)
    children: list[_TopologyNode] = []
    for octant in range(8):
        if not (node.child_mask & (1 << octant)):
            continue
        child = navigator.get_child(node, octant)
        if child is not None:
            children.append(_crawl_topology(navigator, child, payload_nodes))
    return _TopologyNode(entry=node, children=tuple(children))


def _read_payload_magnitudes(
    source: OctreeSource,
    node: NodeEntry,
) -> tuple[NodeKey, tuple[int, ...]]:
    compressed = _read_exact_range(
        source,
        offset=node.payload_offset,
        length=node.payload_length,
    )
    try:
        raw = gzip.decompress(compressed)
    except Exception as exc:
        raise ValueError(
            f"Failed to decompress payload at {node.payload_offset}"
        ) from exc
    if len(raw) % STAR_RECORD_FMT.size:
        raise ValueError(
            f"Payload at {node.payload_offset} has {len(raw)} raw bytes, "
            f"not divisible by {STAR_RECORD_FMT.size}"
        )
    magnitudes = tuple(
        sorted(
            _MAG_CENTI.unpack_from(raw, offset + 12)[0]
            for offset in range(0, len(raw), STAR_RECORD_FMT.size)
        )
    )
    return _node_entry_key(node), magnitudes


def _read_exact_range(source: OctreeSource, *, offset: int, length: int) -> bytes:
    if length <= 0:
        return b""
    if isinstance(source, str) and is_url_source(source):
        end = offset + length - 1
        request = Request(source, headers={"Range": f"bytes={offset}-{end}"})
        with urlopen(request, timeout=60) as response:
            if response.status != 206:
                raise ValueError(
                    f"Expected HTTP 206 for payload range, got {response.status}"
                )
            raw = response.read()
    else:
        path = Path(source)
        with path.open("rb") as stream:
            stream.seek(offset)
            raw = stream.read(length)
    if len(raw) != length:
        raise ValueError(
            f"Truncated payload range at {offset}: expected {length}, got {len(raw)}"
        )
    return raw


def _hydrate_topology(
    topology: _TopologyNode,
    payload_data: dict[NodeKey, tuple[int, ...]],
) -> LogicalNode:
    children = tuple(
        _hydrate_topology(child, payload_data) for child in topology.children
    )
    entry = topology.entry
    key = _node_entry_key(entry)
    magnitudes = payload_data.get(key, ())
    return LogicalNode(
        key=key,
        center=entry.center,
        half_size=entry.half_size,
        payload_offset=entry.payload_offset,
        payload_length=entry.payload_length,
        magnitudes_centi=magnitudes,
        children=children,
        subtree_star_count=len(magnitudes)
        + sum(child.subtree_star_count for child in children),
        subtree_node_count=1 + sum(child.subtree_node_count for child in children),
        subtree_payload_count=(1 if magnitudes else 0)
        + sum(child.subtree_payload_count for child in children),
    )


def _node_entry_key(node: NodeEntry) -> NodeKey:
    return (node.level, node.grid.x, node.grid.y, node.grid.z)


def _benchmark_sample(
    sample: ExtractedSample,
    views: tuple[TraceView, ...],
    config: TerminalMemoryBenchmarkConfig,
) -> dict[str, Any]:
    aggregated_magnitude_cache: dict[NodeKey, tuple[int, ...]] = {}
    scenarios = [
        _replay_policy(
            sample,
            views,
            policy="v1",
            plan=None,
            waterline=None,
            chunk_star_count=None,
            aggregated_magnitude_cache=aggregated_magnitude_cache,
            config=config,
        )
    ]
    plan_summaries: list[dict[str, Any]] = []
    for waterline in config.waterlines:
        plan = build_terminal_plan(sample.root, waterline)
        plan_summary = _plan_summary(plan, waterline)
        plan_summaries.append(plan_summary)
        scenarios.append(
            _replay_policy(
                sample,
                views,
                policy="terminal-monolithic",
                plan=plan,
                waterline=waterline,
                chunk_star_count=None,
                aggregated_magnitude_cache=aggregated_magnitude_cache,
                config=config,
            )
        )
        for chunk_star_count in config.chunk_star_counts:
            scenarios.append(
                _replay_policy(
                    sample,
                    views,
                    policy="terminal-magnitude-chunked",
                    plan=plan,
                    waterline=waterline,
                    chunk_star_count=chunk_star_count,
                    aggregated_magnitude_cache=aggregated_magnitude_cache,
                    config=config,
                )
            )
            scenarios.append(
                _replay_policy(
                    sample,
                    views,
                    policy="terminal-logical-chunked",
                    plan=plan,
                    waterline=waterline,
                    chunk_star_count=chunk_star_count,
                    aggregated_magnitude_cache=aggregated_magnitude_cache,
                    config=config,
                )
            )
    return {
        "name": sample.spec.name,
        "point": [
            sample.spec.point.x,
            sample.spec.point.y,
            sample.spec.point.z,
        ],
        "sample_level": sample.spec.level,
        "classic": {
            "node_count": sample.root.subtree_node_count,
            "payload_node_count": sample.root.subtree_payload_count,
            "star_count": sample.root.subtree_star_count,
            "index_record_bytes": sample.root.subtree_node_count * SHARD_NODE_SIZE,
        },
        "views": [
            {
                "name": view.name,
                "observer_pc": [
                    view.observer.x,
                    view.observer.y,
                    view.observer.z,
                ],
                "limiting_magnitude": view.limiting_magnitude,
            }
            for view in views
        ],
        "plans": plan_summaries,
        "scenarios": scenarios,
    }


def _plan_summary(plan: TerminalPlanNode, waterline: int) -> dict[str, Any]:
    terminals = list(_iter_terminals(plan))
    external_node_count = _count_plan_nodes(plan)
    terminal_rows = [terminal.logical.subtree_star_count for terminal in terminals]
    directory_entries = sum(
        terminal.logical.subtree_payload_count for terminal in terminals
    )
    return {
        "waterline": waterline,
        "external_node_count": external_node_count,
        "eliminated_external_nodes": (
            plan.logical.subtree_node_count - external_node_count
        ),
        "terminal_count": len(terminals),
        "packed_terminal_count": sum(
            1 for terminal in terminals if terminal.logical.subtree_node_count > 1
        ),
        "terminal_directory_entries": directory_entries,
        "terminal_rows_max": max(terminal_rows, default=0),
        "terminal_rows_p50": _quantile(terminal_rows, 0.50),
        "terminal_rows_p95": _quantile(terminal_rows, 0.95),
        "largest_terminals": [
            {
                "key": list(terminal.logical.key),
                "center_pc": [
                    terminal.logical.center.x,
                    terminal.logical.center.y,
                    terminal.logical.center.z,
                ],
                "half_size_pc": terminal.logical.half_size,
                "stars": terminal.logical.subtree_star_count,
                "nodes": terminal.logical.subtree_node_count,
                "payloads": terminal.logical.subtree_payload_count,
                "depth_span": (
                    _deepest_level(terminal.logical) - terminal.logical.level
                ),
            }
            for terminal in sorted(
                terminals,
                key=lambda item: item.logical.subtree_star_count,
                reverse=True,
            )[:32]
        ],
    }


def _iter_terminals(plan: TerminalPlanNode):
    if plan.terminal:
        yield plan
        return
    for child in plan.children:
        yield from _iter_terminals(child)


def _count_plan_nodes(plan: TerminalPlanNode) -> int:
    return 1 + sum(_count_plan_nodes(child) for child in plan.children)


def _deepest_level(node: LogicalNode) -> int:
    return max([node.level, *(_deepest_level(child) for child in node.children)])


def _replay_policy(
    sample: ExtractedSample,
    views: tuple[TraceView, ...],
    *,
    policy: str,
    plan: TerminalPlanNode | None,
    waterline: int | None,
    chunk_star_count: int | None,
    aggregated_magnitude_cache: dict[NodeKey, tuple[int, ...]],
    config: TerminalMemoryBenchmarkConfig,
) -> dict[str, Any]:
    if policy != "v1" and plan is None:
        raise ValueError(f"{policy} requires a terminal plan")
    state = _ReplayState()
    frame_reports: list[dict[str, Any]] = []
    terminal_by_payload = _terminal_mapping(plan) if plan is not None else {}
    plan_by_key = _plan_mapping(plan) if plan is not None else {}

    for view in views:
        if policy == "v1":
            selection = _select_logical(sample.root, view, sample.index_magnitude)
            entries = {
                entry.key: entry
                for entry in (
                    _v1_storage_entry(node) for node in selection.payload_nodes
                )
            }
            directory_entry_count = 0
        else:
            selection = _select_terminal_plan(
                plan,
                view,
                sample.index_magnitude,
            )
            if policy == "terminal-monolithic":
                entries = _monolithic_entries(
                    selection.payload_nodes,
                    terminal_by_payload,
                )
            elif policy == "terminal-magnitude-chunked":
                if chunk_star_count is None:
                    raise ValueError(
                        "terminal-magnitude-chunked requires chunk_star_count"
                    )
                entries = _magnitude_chunked_entries(
                    selection.payload_nodes,
                    terminal_by_payload,
                    view,
                    chunk_star_count,
                    aggregated_magnitude_cache,
                )
            elif policy == "terminal-logical-chunked":
                if chunk_star_count is None:
                    raise ValueError(
                        "terminal-logical-chunked requires chunk_star_count"
                    )
                entries = _chunked_entries(
                    selection.payload_nodes,
                    terminal_by_payload,
                    view,
                    chunk_star_count,
                )
            else:
                raise ValueError(f"Unsupported policy: {policy}")
            directory_entry_count = sum(
                plan_by_key[key].logical.subtree_payload_count
                for key in selection.loaded_terminals
                if key not in state.loaded_terminal_keys
            )

        admissible_rows = sum(
            safe_magnitude_prefix_count(node, view) for node in selection.payload_nodes
        )
        new_entries = [
            entry for key, entry in entries.items() if key not in state.raw_cache
        ]
        for entry in new_entries:
            state.raw_cache[entry.key] = entry
        state.total_fetch_units += len(new_entries)
        state.total_compressed_bytes += sum(
            entry.compressed_bytes for entry in new_entries
        )
        state.inspected_keys.update(selection.inspected_keys)
        state.loaded_terminal_keys.update(selection.loaded_terminals)

        for entry in entries.values():
            _touch_decoded_cache(state, entry, config.decoded_cache_bytes)

        active_keys = set(entries)
        active_rows = sum(entry.rows for entry in entries.values())
        raw_historical_rows = sum(
            entry.rows
            for key, entry in state.raw_cache.items()
            if key not in active_keys
        )
        decoded_historical_rows = sum(
            entry.rows
            for key, entry in state.decoded_cache.items()
            if key not in active_keys
        )
        index_record_bytes = len(state.inspected_keys) * (
            SHARD_NODE_SIZE if policy == "v1" else V2_INDEX_RECORD_BYTES
        )
        terminal_directory_bytes = (
            sum(
                plan_by_key[key].logical.subtree_payload_count
                for key in state.loaded_terminal_keys
            )
            * config.terminal_directory_record_bytes
            if plan is not None
            else 0
        )
        resident_bytes = (
            active_rows * LIVE_BYTES_PER_STAR
            + raw_historical_rows * RAW_PAYLOAD_BYTES_PER_STAR
            + decoded_historical_rows * DECODED_BYTES_PER_STAR
            + index_record_bytes
            + terminal_directory_bytes
        )
        inflight_entries = sorted(
            new_entries,
            key=lambda entry: (
                entry.rows * RAW_PAYLOAD_BYTES_PER_STAR + entry.compressed_bytes
            ),
            reverse=True,
        )[: config.max_inflight_payloads]
        inflate_scratch_bytes = sum(
            entry.rows * RAW_PAYLOAD_BYTES_PER_STAR + entry.compressed_bytes
            for entry in inflight_entries
        )
        active_changed = entries != state.previous_active_entries
        transition_overlap_bytes = (
            state.previous_active_rows * TRANSITION_OVERLAP_BYTES_PER_STAR
            if active_changed
            else 0
        )
        peak_bytes = resident_bytes + inflate_scratch_bytes + transition_overlap_bytes
        minimum_geometry_rebuild_bytes = (
            active_rows * RENDERER_CPU_BYTES_PER_STAR if active_changed else 0
        )
        state.minimum_geometry_rebuild_bytes += minimum_geometry_rebuild_bytes
        state.previous_active_entries = dict(entries)
        state.previous_active_rows = active_rows

        frame_reports.append(
            {
                "view": view.name,
                "logical_payload_nodes": len(selection.payload_nodes),
                "loaded_terminals": len(selection.loaded_terminals),
                "storage_entries": len(entries),
                "largest_storage_entry_rows": max(
                    (entry.rows for entry in entries.values()),
                    default=0,
                ),
                "new_fetch_units": len(new_entries),
                "new_compressed_bytes": sum(
                    entry.compressed_bytes for entry in new_entries
                ),
                "admissible_rows": admissible_rows,
                "active_rows": active_rows,
                "active_set_changed": active_changed,
                "overfetch_ratio": (
                    active_rows / admissible_rows
                    if admissible_rows > 0
                    else (1.0 if active_rows == 0 else None)
                ),
                "raw_cache_rows": sum(entry.rows for entry in state.raw_cache.values()),
                "decoded_cache_rows": sum(
                    entry.rows for entry in state.decoded_cache.values()
                ),
                "index_record_bytes": index_record_bytes,
                "new_terminal_directory_entries": directory_entry_count,
                "terminal_directory_bytes": terminal_directory_bytes,
                "resident_bytes": resident_bytes,
                "inflate_scratch_bytes": inflate_scratch_bytes,
                "transition_overlap_bytes": transition_overlap_bytes,
                "peak_bytes": peak_bytes,
                "minimum_geometry_rebuild_bytes": (minimum_geometry_rebuild_bytes),
            }
        )

    summary = _plan_summary(plan, waterline=0) if plan is not None else None
    ratios = [
        float(frame["overfetch_ratio"])
        for frame in frame_reports
        if frame["overfetch_ratio"] is not None
    ]
    return {
        "policy": policy,
        "waterline": waterline,
        "chunk_star_count": chunk_star_count,
        "external_node_count": (
            sample.root.subtree_node_count if plan is None else _count_plan_nodes(plan)
        ),
        "terminal_count": 0 if summary is None else summary["terminal_count"],
        "max_active_rows": max(
            (int(frame["active_rows"]) for frame in frame_reports),
            default=0,
        ),
        "max_resident_bytes": max(
            (int(frame["resident_bytes"]) for frame in frame_reports),
            default=0,
        ),
        "max_peak_bytes": max(
            (int(frame["peak_bytes"]) for frame in frame_reports),
            default=0,
        ),
        "max_overfetch_ratio": max(ratios, default=1.0),
        "max_storage_entry_rows": max(
            (int(frame["largest_storage_entry_rows"]) for frame in frame_reports),
            default=0,
        ),
        "max_atomic_live_bytes": max(
            (
                int(frame["largest_storage_entry_rows"]) * LIVE_BYTES_PER_STAR
                for frame in frame_reports
            ),
            default=0,
        ),
        "max_index_record_bytes": max(
            (int(frame["index_record_bytes"]) for frame in frame_reports),
            default=0,
        ),
        "max_terminal_directory_bytes": max(
            (int(frame["terminal_directory_bytes"]) for frame in frame_reports),
            default=0,
        ),
        "final_raw_cache_rows": sum(entry.rows for entry in state.raw_cache.values()),
        "final_decoded_cache_rows": sum(
            entry.rows for entry in state.decoded_cache.values()
        ),
        "payload_fetch_units": state.total_fetch_units,
        "compressed_bytes": state.total_compressed_bytes,
        "minimum_geometry_rebuild_bytes": state.minimum_geometry_rebuild_bytes,
        "frames": frame_reports,
    }


def _select_logical(
    root: LogicalNode,
    view: TraceView,
    index_magnitude: float,
) -> _Selection:
    selection = _Selection()

    def visit(node: LogicalNode) -> None:
        selection.inspected_keys.add(node.key)
        if not _node_relevant(node, view, index_magnitude):
            return
        if node.has_payload:
            selection.payload_nodes.append(node)
        for child in node.children:
            visit(child)

    visit(root)
    return selection


def _select_terminal_plan(
    plan: TerminalPlanNode,
    view: TraceView,
    index_magnitude: float,
) -> _Selection:
    selection = _Selection()

    def visit(node_plan: TerminalPlanNode) -> None:
        node = node_plan.logical
        selection.inspected_keys.add(node.key)
        if not _node_relevant(node, view, index_magnitude):
            return
        if node_plan.terminal:
            selection.loaded_terminals.add(node.key)
            internal = _select_logical(node, view, index_magnitude)
            selection.payload_nodes.extend(internal.payload_nodes)
            return
        if node.has_payload:
            selection.payload_nodes.append(node)
        for child in node_plan.children:
            visit(child)

    visit(plan)
    return selection


def _node_relevant(
    node: LogicalNode,
    view: TraceView,
    index_magnitude: float,
) -> bool:
    load_radius = node.half_size * (
        10.0 ** ((view.limiting_magnitude - index_magnitude) / 5.0)
    )
    return node.aabb_distance(view.observer) <= load_radius


def _terminal_mapping(plan: TerminalPlanNode) -> dict[NodeKey, TerminalPlanNode]:
    mapping: dict[NodeKey, TerminalPlanNode] = {}

    def map_payloads(logical: LogicalNode, terminal: TerminalPlanNode) -> None:
        if logical.has_payload:
            mapping[logical.key] = terminal
        for child in logical.children:
            map_payloads(child, terminal)

    def visit(node_plan: TerminalPlanNode) -> None:
        if node_plan.terminal:
            map_payloads(node_plan.logical, node_plan)
            return
        for child in node_plan.children:
            visit(child)

    visit(plan)
    return mapping


def _plan_mapping(plan: TerminalPlanNode) -> dict[NodeKey, TerminalPlanNode]:
    mapping: dict[NodeKey, TerminalPlanNode] = {}

    def visit(node_plan: TerminalPlanNode) -> None:
        mapping[node_plan.logical.key] = node_plan
        for child in node_plan.children:
            visit(child)

    visit(plan)
    return mapping


def _v1_storage_entry(node: LogicalNode) -> StorageEntry:
    return StorageEntry(
        key=f"v1:{_key_text(node.key)}",
        rows=node.natural_star_count,
        compressed_bytes=node.payload_length,
    )


def _monolithic_entries(
    payload_nodes: list[LogicalNode],
    terminal_by_payload: dict[NodeKey, TerminalPlanNode],
) -> dict[str, StorageEntry]:
    entries: dict[str, StorageEntry] = {}
    for node in payload_nodes:
        terminal = terminal_by_payload.get(node.key)
        if terminal is None:
            entry = StorageEntry(
                key=f"v2-normal:{_key_text(node.key)}",
                rows=node.natural_star_count,
                compressed_bytes=node.payload_length,
            )
        else:
            logical = terminal.logical
            entry = StorageEntry(
                key=f"v2-terminal:{_key_text(logical.key)}",
                rows=logical.subtree_star_count,
                compressed_bytes=_subtree_compressed_bytes(logical),
            )
        entries[entry.key] = entry
    return entries


def _magnitude_chunked_entries(
    payload_nodes: list[LogicalNode],
    terminal_by_payload: dict[NodeKey, TerminalPlanNode],
    view: TraceView,
    chunk_star_count: int,
    aggregated_magnitude_cache: dict[NodeKey, tuple[int, ...]],
) -> dict[str, StorageEntry]:
    entries: dict[str, StorageEntry] = {}
    selected_terminals: dict[NodeKey, TerminalPlanNode] = {}
    for node in payload_nodes:
        terminal = terminal_by_payload.get(node.key)
        if terminal is None:
            entry = StorageEntry(
                key=f"v2-normal:{_key_text(node.key)}",
                rows=node.natural_star_count,
                compressed_bytes=node.payload_length,
            )
            entries[entry.key] = entry
        else:
            selected_terminals[terminal.logical.key] = terminal

    for terminal in selected_terminals.values():
        logical = terminal.logical
        magnitudes = _subtree_magnitudes(logical, aggregated_magnitude_cache)
        threshold_centi = _safe_magnitude_threshold_centi(logical, view)
        prefix_count = (
            len(magnitudes)
            if threshold_centi is None
            else bisect.bisect_right(magnitudes, threshold_centi)
        )
        compressed_total = _subtree_compressed_bytes(logical)
        for chunk_start in range(0, prefix_count, chunk_star_count):
            rows = min(chunk_star_count, len(magnitudes) - chunk_start)
            chunk_index = chunk_start // chunk_star_count
            compressed_bytes = max(
                1,
                math.ceil(compressed_total * rows / len(magnitudes)),
            )
            key = f"v2-mag-chunk:{_key_text(logical.key)}:{chunk_index}"
            entries[key] = StorageEntry(
                key=key,
                rows=rows,
                compressed_bytes=compressed_bytes,
            )
    return entries


def _chunked_entries(
    payload_nodes: list[LogicalNode],
    terminal_by_payload: dict[NodeKey, TerminalPlanNode],
    view: TraceView,
    chunk_star_count: int,
) -> dict[str, StorageEntry]:
    entries: dict[str, StorageEntry] = {}
    for node in payload_nodes:
        terminal = terminal_by_payload.get(node.key)
        if terminal is None:
            entry = StorageEntry(
                key=f"v2-normal:{_key_text(node.key)}",
                rows=node.natural_star_count,
                compressed_bytes=node.payload_length,
            )
            entries[entry.key] = entry
            continue

        prefix_count = safe_magnitude_prefix_count(node, view)
        for chunk_start in range(0, prefix_count, chunk_star_count):
            rows = min(chunk_star_count, node.natural_star_count - chunk_start)
            chunk_index = chunk_start // chunk_star_count
            compressed_bytes = max(
                1,
                math.ceil(node.payload_length * rows / node.natural_star_count),
            )
            key = (
                f"v2-chunk:{_key_text(terminal.logical.key)}:"
                f"{_key_text(node.key)}:{chunk_index}"
            )
            entries[key] = StorageEntry(
                key=key,
                rows=rows,
                compressed_bytes=compressed_bytes,
            )
    return entries


def _subtree_magnitudes(
    node: LogicalNode,
    cache: dict[NodeKey, tuple[int, ...]],
) -> tuple[int, ...]:
    cached = cache.get(node.key)
    if cached is not None:
        return cached
    magnitudes = tuple(
        sorted(
            (
                *node.magnitudes_centi,
                *(
                    magnitude
                    for child in node.children
                    for magnitude in _subtree_magnitudes(child, cache)
                ),
            )
        )
    )
    cache[node.key] = magnitudes
    return magnitudes


def _subtree_compressed_bytes(node: LogicalNode) -> int:
    return node.payload_length + sum(
        _subtree_compressed_bytes(child) for child in node.children
    )


def _touch_decoded_cache(
    state: _ReplayState,
    entry: StorageEntry,
    budget_bytes: int,
) -> None:
    current = state.decoded_cache.pop(entry.key, None)
    if current is not None:
        state.decoded_cache_bytes -= current.rows * DECODED_BYTES_PER_STAR
    state.decoded_cache[entry.key] = entry
    state.decoded_cache_bytes += entry.rows * DECODED_BYTES_PER_STAR
    while state.decoded_cache and state.decoded_cache_bytes > budget_bytes:
        _, evicted = state.decoded_cache.popitem(last=False)
        state.decoded_cache_bytes -= evicted.rows * DECODED_BYTES_PER_STAR


def _key_text(key: NodeKey) -> str:
    return ":".join(str(value) for value in key)


def _quantile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = round((len(ordered) - 1) * fraction)
    return ordered[index]


def _sample_cache_path(
    cache_dir: Path,
    source: OctreeSource,
    header: OctreeHeader,
    spec: SampleSpec,
) -> Path:
    source_details: dict[str, Any] = {"value": str(source)}
    if isinstance(source, Path):
        stat = source.stat()
        source_details.update(
            {
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    identity = json.dumps(
        {
            "source": source_details,
            "version": header.version,
            "index_offset": header.index_offset,
            "index_length": header.index_length,
            "dataset_uuid": (
                str(header.dataset_uuid) if header.dataset_uuid is not None else None
            ),
            "sample": {
                "name": spec.name,
                "point": [spec.point.x, spec.point.y, spec.point.z],
                "level": spec.level,
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(identity.encode()).hexdigest()[:20]
    return cache_dir / f"{spec.name}-{digest}.json.gz"


def _write_sample_cache(path: Path, sample: ExtractedSample) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    payload = {
        "format": SAMPLE_CACHE_FORMAT,
        "source": sample.source,
        "index_magnitude": sample.index_magnitude,
        "spec": {
            "name": sample.spec.name,
            "point": [
                sample.spec.point.x,
                sample.spec.point.y,
                sample.spec.point.z,
            ],
            "level": sample.spec.level,
        },
        "root": _node_to_json(sample.root),
    }
    with gzip.open(temporary, "wt", encoding="utf-8") as stream:
        json.dump(payload, stream, separators=(",", ":"))
    temporary.replace(path)


def _read_sample_cache(
    path: Path,
    source: OctreeSource,
    expected_spec: SampleSpec,
) -> ExtractedSample:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    if payload.get("format") != SAMPLE_CACHE_FORMAT:
        raise ValueError(f"Unsupported terminal sample cache: {path}")
    raw_spec = payload["spec"]
    point = Point(*(float(value) for value in raw_spec["point"]))
    spec = SampleSpec(
        name=str(raw_spec["name"]),
        point=point,
        level=int(raw_spec["level"]),
    )
    if spec != expected_spec:
        raise ValueError(f"Terminal sample cache does not match {expected_spec.name}")
    return ExtractedSample(
        spec=spec,
        source=str(source),
        index_magnitude=float(payload["index_magnitude"]),
        root=_node_from_json(payload["root"]),
    )


def _node_to_json(node: LogicalNode) -> dict[str, Any]:
    return {
        "key": list(node.key),
        "center": [node.center.x, node.center.y, node.center.z],
        "half_size": node.half_size,
        "payload_offset": node.payload_offset,
        "payload_length": node.payload_length,
        "magnitudes_centi": list(node.magnitudes_centi),
        "children": [_node_to_json(child) for child in node.children],
    }


def _node_from_json(raw: dict[str, Any]) -> LogicalNode:
    children = tuple(_node_from_json(child) for child in raw["children"])
    magnitudes = tuple(int(value) for value in raw["magnitudes_centi"])
    return LogicalNode(
        key=tuple(int(value) for value in raw["key"]),
        center=Point(*(float(value) for value in raw["center"])),
        half_size=float(raw["half_size"]),
        payload_offset=int(raw["payload_offset"]),
        payload_length=int(raw["payload_length"]),
        magnitudes_centi=magnitudes,
        children=children,
        subtree_star_count=len(magnitudes)
        + sum(child.subtree_star_count for child in children),
        subtree_node_count=1 + sum(child.subtree_node_count for child in children),
        subtree_payload_count=(1 if magnitudes else 0)
        + sum(child.subtree_payload_count for child in children),
    )
