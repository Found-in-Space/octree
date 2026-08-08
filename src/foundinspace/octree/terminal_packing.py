from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from .config import MORTON_BITS

TERMINAL_MAP_FORMAT = "foundinspace.octree.terminal-map/v1"
TERMINAL_MAP_NAME = "terminal-map.json"
TERMINAL_MAP_DIR_NAME = "terminal-map"
TERMINAL_COUNTS_DIR_NAME = "terminal-counts"

_GROUP_COUNTS_FORMAT = "foundinspace.octree.terminal-group-counts/v1"
_OWN_COUNTS_FORMAT = "foundinspace.octree.terminal-own-counts/v1"
_NODE_COUNTS_FORMAT = "foundinspace.octree.terminal-node-counts/v1"
_TERMINAL_PLAN_FORMAT = "foundinspace.octree.terminal-plan/v1"
_COUNT_ALGORITHM = "capped-cell-count-runs/v1"
_TOPOLOGY_ALGORITHM = "streamed-bottom-up-topology/v1"
_DEFAULT_MERGE_FAN_IN = 32
_IO_RECORDS = 65_536
_COUNT_DTYPE = np.dtype([("node_id", "<u8"), ("star_count", "<u8")])
_NODE_DTYPE = np.dtype(
    [
        ("node_id", "<u8"),
        ("subtree_count", "<u8"),
        ("has_descendants", "u1"),
    ]
)
_MAX_U64 = int(np.iinfo(np.uint64).max)


class TerminalMap:
    def __init__(self, manifest_path: Path):
        self.manifest_path = Path(manifest_path)
        raw = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        if raw.get("format") != TERMINAL_MAP_FORMAT:
            raise ValueError(f"Unsupported terminal map format: {raw.get('format')!r}")
        self.max_level = int(raw["max_level"])
        self.waterline = int(raw["waterline"])
        self.terminal_count = int(raw["terminal_count"])
        if self.max_level < 0 or self.max_level > MORTON_BITS:
            raise ValueError(f"Invalid terminal map max_level: {self.max_level}")
        if self.waterline <= 0:
            raise ValueError(f"Invalid terminal map waterline: {self.waterline}")
        if self.terminal_count < 0:
            raise ValueError(
                f"Invalid terminal map terminal_count: {self.terminal_count}"
            )
        level_entries = raw.get("levels")
        if not isinstance(level_entries, list):
            raise ValueError("Terminal map levels must be a list")
        self._by_level: dict[int, np.ndarray] = {}
        counted_terminals = 0
        for entry in level_entries:
            level = int(entry["level"])
            count = int(entry["count"])
            if level < 0 or level > self.max_level:
                raise ValueError(f"Invalid terminal map level: {level}")
            if level in self._by_level:
                raise ValueError(f"Duplicate terminal map level: {level}")
            if count <= 0:
                raise ValueError(
                    f"Invalid terminal map count at level {level}: {count}"
                )
            path = self.manifest_path.parent / str(entry["path"])
            if path.stat().st_size != count * np.dtype("<u8").itemsize:
                raise ValueError(f"Invalid terminal map byte length: {path}")
            nodes = np.memmap(path, dtype="<u8", mode="r", shape=(count,))
            _validate_terminal_level(nodes, level=level, path=path)
            self._by_level[level] = nodes
            counted_terminals += count
        if counted_terminals != self.terminal_count:
            raise ValueError(
                "Terminal map count mismatch: "
                f"manifest={self.terminal_count}, levels={counted_terminals}"
            )

    @property
    def levels(self) -> tuple[int, ...]:
        return tuple(sorted(self._by_level))

    def contains(self, level: int, node_id: int) -> bool:
        nodes = self._by_level.get(int(level))
        if nodes is None or len(nodes) == 0:
            return False
        index = int(np.searchsorted(nodes, np.uint64(node_id)))
        return index < len(nodes) and int(nodes[index]) == int(node_id)

    def iter_level(self, level: int) -> Iterator[int]:
        """Iterate a terminal level in ascending order without random lookup."""
        nodes = self._by_level.get(int(level))
        if nodes is None:
            return
        for node_id in nodes:
            yield int(node_id)

    def remap(
        self,
        levels: np.ndarray,
        node_ids: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        mapped_levels = np.asarray(levels, dtype=np.int16).copy()
        mapped_nodes = np.asarray(node_ids, dtype=np.uint64).copy()
        unresolved = np.ones(len(mapped_levels), dtype=np.bool_)
        for terminal_level in self.levels:
            candidates = np.flatnonzero(unresolved & (mapped_levels >= terminal_level))
            if len(candidates) == 0:
                continue
            shifts = (
                3 * (mapped_levels[candidates].astype(np.int64) - terminal_level)
            ).astype(np.uint64)
            ancestors = np.right_shift(mapped_nodes[candidates], shifts)
            terminals = self._by_level[terminal_level]
            positions = np.searchsorted(terminals, ancestors)
            in_bounds = positions < len(terminals)
            matches = np.zeros(len(candidates), dtype=np.bool_)
            if np.any(in_bounds):
                matches[in_bounds] = (
                    terminals[positions[in_bounds]] == ancestors[in_bounds]
                )
            selected = candidates[matches]
            mapped_levels[selected] = terminal_level
            mapped_nodes[selected] = ancestors[matches]
            unresolved[selected] = False
        return mapped_levels, mapped_nodes


def _validate_terminal_level(
    nodes: np.ndarray,
    *,
    level: int,
    path: Path,
) -> None:
    previous: int | None = None
    for start in range(0, len(nodes), 1_000_000):
        chunk = nodes[start : start + 1_000_000]
        first = int(chunk[0])
        if previous is not None and first <= previous:
            raise ValueError(f"Non-ascending terminal node IDs: {path}")
        if len(chunk) > 1 and np.any(chunk[1:] <= chunk[:-1]):
            raise ValueError(f"Non-ascending terminal node IDs: {path}")
        previous = int(chunk[-1])
    if previous is not None and previous >= 1 << (3 * level):
        raise ValueError(f"Terminal node ID exceeds level {level}: {path}")


@dataclass(frozen=True, slots=True)
class _CountRun:
    path: Path
    record_count: int
    star_count: int
    checksum: str

    def content_record(self) -> dict[str, Any]:
        return {
            "record_count": self.record_count,
            "star_count": self.star_count,
            "checksum": self.checksum,
        }


@dataclass(frozen=True, slots=True)
class _NodeRun:
    path: Path
    record_count: int
    checksum: str

    def content_record(self) -> dict[str, Any]:
        return {"record_count": self.record_count, "checksum": self.checksum}


@dataclass(frozen=True, slots=True)
class _GroupCounts:
    source_identity: str
    row_count: int
    levels: dict[int, _CountRun]


def build_terminal_map(
    *,
    groups: Sequence[Any],
    work_dir: Path,
    artifacts_dir: Path,
    max_level: int,
    waterline: int,
    batch_size: int,
    merge_fan_in: int = _DEFAULT_MERGE_FAN_IN,
) -> Path:
    """Build a terminal map from immutable, bounded-memory sorted count runs."""
    if max_level < 0 or max_level > MORTON_BITS:
        raise ValueError(f"max_level must be between 0 and {MORTON_BITS}")
    if waterline <= 0:
        raise ValueError("waterline must be > 0")
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if merge_fan_in < 2:
        raise ValueError("merge_fan_in must be >= 2")

    artifacts_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = artifacts_dir / TERMINAL_MAP_NAME
    source_identity = _source_input_identity(groups, max_level=max_level)
    if _terminal_map_is_valid(
        manifest_path,
        max_level=max_level,
        waterline=waterline,
        source_input_identity=source_identity,
    ):
        return manifest_path

    counts_root = work_dir / TERMINAL_COUNTS_DIR_NAME
    counts_root.mkdir(parents=True, exist_ok=True)
    group_counts = [
        _prepare_group_counts(
            group,
            counts_root=counts_root,
            max_level=max_level,
            batch_size=batch_size,
            merge_fan_in=merge_fan_in,
        )
        for group in groups
    ]
    expected_rows = sum(int(group.row_count) for group in groups)
    counted_rows = sum(counts.row_count for counts in group_counts)
    if counted_rows != expected_rows:
        raise ValueError(
            "Terminal count map row mismatch: "
            f"expected={expected_rows}, actual={counted_rows}"
        )

    own_runs: dict[int, _CountRun] = {}
    for level in range(max_level + 1):
        inputs = [
            run
            for counts in group_counts
            if (run := counts.levels.get(level)) is not None
        ]
        if inputs:
            own_runs[level] = _aggregate_own_counts(
                inputs,
                counts_root=counts_root,
                level=level,
                merge_fan_in=merge_fan_in,
            )
    aggregated_rows = sum(run.star_count for run in own_runs.values())
    if aggregated_rows != expected_rows:
        raise ValueError(
            "Terminal aggregated count row mismatch: "
            f"expected={expected_rows}, actual={aggregated_rows}"
        )
    counts_identity = _counts_identity(own_runs, max_level=max_level)
    if _terminal_map_is_valid(
        manifest_path,
        max_level=max_level,
        waterline=waterline,
        counts_identity=counts_identity,
    ):
        _refresh_source_identity(manifest_path, source_identity=source_identity)
        return manifest_path

    node_runs = _build_node_levels(
        own_runs,
        counts_root=counts_root,
        max_level=max_level,
    )
    plan = _select_terminal_plan(
        node_runs,
        artifacts_dir=artifacts_dir,
        max_level=max_level,
        waterline=waterline,
        counts_identity=counts_identity,
    )
    return _publish_terminal_map(
        plan,
        artifacts_dir=artifacts_dir,
        max_level=max_level,
        waterline=waterline,
        source_identity=source_identity,
        counts_identity=counts_identity,
    )


def _source_input_identity(groups: Sequence[Any], *, max_level: int) -> str:
    group_identities = sorted(
        _group_source_identity(group, max_level=max_level) for group in groups
    )
    return _identity(
        {
            "algorithm": _COUNT_ALGORITHM,
            "max_level": max_level,
            "groups": group_identities,
        }
    )


def _group_source_identity(group: Any, *, max_level: int) -> str:
    return _identity(
        {
            "algorithm": _COUNT_ALGORITHM,
            "checksum": str(group.checksum),
            "row_count": int(group.row_count),
            "max_level": max_level,
        }
    )


def _prepare_group_counts(
    group: Any,
    *,
    counts_root: Path,
    max_level: int,
    batch_size: int,
    merge_fan_in: int,
) -> _GroupCounts:
    source_identity = _group_source_identity(group, max_level=max_level)
    group_dir = counts_root / "groups" / source_identity.removeprefix("sha256:")
    cached = _load_group_counts(
        group_dir,
        source_identity=source_identity,
        expected_rows=int(group.row_count),
        max_level=max_level,
    )
    if cached is not None:
        return cached

    temporary = _temporary_sibling(group_dir)
    scratch = temporary / "scratch"
    scratch.mkdir(parents=True)
    accumulators: dict[int, _CountRunAccumulator] = {}
    counted_rows = 0
    try:
        for path in group.files:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(
                batch_size=batch_size,
                columns=["level", "morton_code"],
            ):
                source_levels = np.asarray(batch.column(0), dtype=np.int32)
                morton_codes = np.asarray(batch.column(1), dtype=np.uint64)
                if len(source_levels) == 0:
                    continue
                if np.any(source_levels < 0) or np.any(source_levels > MORTON_BITS):
                    raise ValueError(
                        f"Invalid row level in terminal count group {group.key}"
                    )
                final_levels = np.minimum(source_levels, max_level).astype(
                    np.uint16, copy=False
                )
                final_nodes = np.empty(len(final_levels), dtype=np.uint64)
                for level_raw in np.unique(final_levels):
                    level = int(level_raw)
                    selected = final_levels == level_raw
                    final_nodes[selected] = morton_codes[selected] >> np.uint64(
                        3 * (MORTON_BITS - level)
                    )
                keys = np.empty(
                    len(final_nodes),
                    dtype=np.dtype([("level", "<u2"), ("node_id", "<u8")]),
                )
                keys["level"] = final_levels
                keys["node_id"] = final_nodes
                unique_keys, counts = np.unique(keys, return_counts=True)
                for level_raw in np.unique(unique_keys["level"]):
                    level = int(level_raw)
                    selected = unique_keys["level"] == level_raw
                    accumulator = accumulators.setdefault(
                        level,
                        _CountRunAccumulator(scratch, merge_fan_in=merge_fan_in),
                    )
                    accumulator.add_arrays(
                        unique_keys["node_id"][selected], counts[selected]
                    )
                counted_rows += len(final_levels)
        if counted_rows != int(group.row_count):
            raise ValueError(
                f"Terminal count row mismatch for sorted group {group.key}: "
                f"expected={group.row_count}, actual={counted_rows}"
            )

        levels: dict[int, _CountRun] = {}
        for level, accumulator in sorted(accumulators.items()):
            path = temporary / f"level-{level:02d}.counts"
            levels[level] = accumulator.finish(path)
        shutil.rmtree(scratch)
        _atomic_write_json(
            temporary / "manifest.json",
            {
                "format": _GROUP_COUNTS_FORMAT,
                "source_identity": source_identity,
                "max_level": max_level,
                "row_count": counted_rows,
                "levels": [
                    _count_manifest_entry(level, run, relative_to=temporary)
                    for level, run in sorted(levels.items())
                ],
            },
        )
        _publish_immutable_directory(temporary, group_dir)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    result = _load_group_counts(
        group_dir,
        source_identity=source_identity,
        expected_rows=int(group.row_count),
        max_level=max_level,
    )
    if result is None:
        raise ValueError(f"Published terminal group counts are invalid: {group.key}")
    return result


class _CountRunAccumulator:
    def __init__(self, scratch: Path, *, merge_fan_in: int) -> None:
        self.scratch = scratch
        self.merge_fan_in = merge_fan_in
        self.tiers: list[list[_CountRun]] = []
        self.sequence = 0

    def add_arrays(self, nodes: np.ndarray, counts: np.ndarray) -> None:
        path = self._next_path("raw")
        run = _write_count_arrays(path, nodes, counts)
        self._add(run, tier=0)

    def _add(self, run: _CountRun, *, tier: int) -> None:
        while len(self.tiers) <= tier:
            self.tiers.append([])
        self.tiers[tier].append(run)
        if len(self.tiers[tier]) < self.merge_fan_in:
            return
        inputs = self.tiers[tier]
        self.tiers[tier] = []
        merged = _merge_count_run_batch(inputs, self._next_path(f"tier-{tier}"))
        for input_run in inputs:
            input_run.path.unlink()
        self._add(merged, tier=tier + 1)

    def finish(self, output: Path) -> _CountRun:
        runs = [run for tier in self.tiers for run in tier]
        if not runs:
            raise ValueError("Cannot finish an empty count-run accumulator")
        return _reduce_count_runs(
            runs,
            output,
            scratch=self.scratch,
            merge_fan_in=self.merge_fan_in,
        )

    def _next_path(self, kind: str) -> Path:
        self.sequence += 1
        return self.scratch / f"{kind}-{id(self):x}-{self.sequence:08d}.counts"


def _aggregate_own_counts(
    inputs: Sequence[_CountRun],
    *,
    counts_root: Path,
    level: int,
    merge_fan_in: int,
) -> _CountRun:
    input_identity = _identity(
        {
            "format": _OWN_COUNTS_FORMAT,
            "level": level,
            "inputs": sorted(
                (run.content_record() for run in inputs),
                key=lambda row: (
                    row["checksum"],
                    row["record_count"],
                    row["star_count"],
                ),
            ),
        }
    )
    output_dir = counts_root / "own" / input_identity.removeprefix("sha256:")
    cached = _load_single_count_run(
        output_dir,
        expected_format=_OWN_COUNTS_FORMAT,
        expected_identity=input_identity,
        expected_level=level,
    )
    if cached is not None:
        return cached

    temporary = _temporary_sibling(output_dir)
    scratch = temporary / "scratch"
    scratch.mkdir(parents=True)
    try:
        run = _reduce_count_runs(
            inputs,
            temporary / "counts.bin",
            scratch=scratch,
            merge_fan_in=merge_fan_in,
        )
        shutil.rmtree(scratch)
        _atomic_write_json(
            temporary / "manifest.json",
            {
                "format": _OWN_COUNTS_FORMAT,
                "identity": input_identity,
                "level": level,
                "run": _count_manifest_entry(level, run, relative_to=temporary),
            },
        )
        _publish_immutable_directory(temporary, output_dir)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    result = _load_single_count_run(
        output_dir,
        expected_format=_OWN_COUNTS_FORMAT,
        expected_identity=input_identity,
        expected_level=level,
    )
    if result is None:
        raise ValueError(f"Published own-count run is invalid at level {level}")
    return result


def _build_node_levels(
    own_runs: dict[int, _CountRun],
    *,
    counts_root: Path,
    max_level: int,
) -> dict[int, _NodeRun]:
    child: _NodeRun | None = None
    result: dict[int, _NodeRun] = {}
    for level in range(max_level, -1, -1):
        own = own_runs.get(level)
        if own is None and child is None:
            continue
        identity = _identity(
            {
                "format": _NODE_COUNTS_FORMAT,
                "algorithm": _TOPOLOGY_ALGORITHM,
                "level": level,
                "own": None if own is None else own.content_record(),
                "children": None if child is None else child.content_record(),
            }
        )
        output_dir = counts_root / "nodes" / identity.removeprefix("sha256:")
        node_run = _load_node_run(
            output_dir, expected_identity=identity, expected_level=level
        )
        if node_run is None:
            temporary = _temporary_sibling(output_dir)
            temporary.mkdir(parents=True)
            try:
                node_run = _write_node_level(
                    own,
                    child,
                    output=temporary / "nodes.bin",
                )
                _atomic_write_json(
                    temporary / "manifest.json",
                    {
                        "format": _NODE_COUNTS_FORMAT,
                        "identity": identity,
                        "level": level,
                        "run": {
                            "path": "nodes.bin",
                            **node_run.content_record(),
                        },
                    },
                )
                _publish_immutable_directory(temporary, output_dir)
            except Exception:
                shutil.rmtree(temporary, ignore_errors=True)
                raise
            node_run = _load_node_run(
                output_dir, expected_identity=identity, expected_level=level
            )
            if node_run is None:
                raise ValueError(
                    f"Published node-count run is invalid at level {level}"
                )
        result[level] = node_run
        child = node_run
    return result


def _write_node_level(
    own: _CountRun | None,
    child: _NodeRun | None,
    *,
    output: Path,
) -> _NodeRun:
    sources: list[_ArrayCursor] = []
    if own is not None:
        sources.append(
            _ArrayCursor(
                (
                    records["node_id"],
                    records["star_count"],
                    np.zeros(len(records), dtype=np.uint8),
                )
                for records in _iter_count_chunks(own.path)
            )
        )
    if child is not None:
        sources.append(
            _ArrayCursor(
                (
                    (nodes, counts, np.ones(len(nodes), dtype=np.uint8))
                    for nodes, counts in _iter_parent_subtree_chunks(child.path)
                )
            )
        )
    digest = hashlib.sha256()
    record_count = 0
    with open(output, "wb") as fp:
        while active := [source for source in sources if source.ensure_buffer()]:
            watermark = min(source.last_node for source in active)
            pieces = [source.take_through(watermark) for source in active]
            pieces = [piece for piece in pieces if len(piece[0])]
            nodes = np.concatenate([piece[0] for piece in pieces])
            counts = np.concatenate([piece[1] for piece in pieces])
            descendant_flags = np.concatenate([piece[2] for piece in pieces])
            order = np.argsort(nodes, kind="stable")
            nodes = nodes[order]
            counts = counts[order]
            descendant_flags = descendant_flags[order]
            starts = np.r_[0, np.flatnonzero(nodes[1:] != nodes[:-1]) + 1]
            unique_nodes = nodes[starts]
            subtree_counts = np.add.reduceat(counts, starts, dtype=np.uint64)
            has_descendants = np.maximum.reduceat(descendant_flags, starts)
            records = np.empty(len(starts), dtype=_NODE_DTYPE)
            records["node_id"] = unique_nodes
            records["subtree_count"] = subtree_counts
            records["has_descendants"] = has_descendants
            data = records.tobytes()
            fp.write(data)
            digest.update(data)
            record_count += len(records)
        fp.flush()
        os.fsync(fp.fileno())
    return _NodeRun(output, record_count, f"sha256:{digest.hexdigest()}")


def _iter_parent_subtree_chunks(path: Path) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    pending_parent: int | None = None
    pending_count = 0
    for records in _iter_node_chunks(path):
        parents = np.right_shift(records["node_id"], np.uint64(3))
        starts = np.r_[0, np.flatnonzero(parents[1:] != parents[:-1]) + 1]
        unique_parents = parents[starts]
        counts = np.add.reduceat(records["subtree_count"], starts, dtype=np.uint64)
        if pending_parent is not None:
            if int(unique_parents[0]) == pending_parent:
                counts[0] = np.uint64(_checked_add(pending_count, int(counts[0])))
            else:
                yield (
                    np.asarray([pending_parent], dtype=np.uint64),
                    np.asarray([pending_count], dtype=np.uint64),
                )
        if len(unique_parents) > 1:
            yield unique_parents[:-1].copy(), counts[:-1].copy()
        pending_parent = int(unique_parents[-1])
        pending_count = int(counts[-1])
    if pending_parent is not None:
        yield (
            np.asarray([pending_parent], dtype=np.uint64),
            np.asarray([pending_count], dtype=np.uint64),
        )


def _select_terminal_plan(
    node_runs: dict[int, _NodeRun],
    *,
    artifacts_dir: Path,
    max_level: int,
    waterline: int,
    counts_identity: str,
) -> dict[str, Any]:
    plan_identity = _identity(
        {
            "format": _TERMINAL_PLAN_FORMAT,
            "algorithm": _TOPOLOGY_ALGORITHM,
            "counts_identity": counts_identity,
            "max_level": max_level,
            "waterline": waterline,
        }
    )
    plan_dir = (
        artifacts_dir / TERMINAL_MAP_DIR_NAME / plan_identity.removeprefix("sha256:")
    )
    cached = _load_terminal_plan(plan_dir, expected_identity=plan_identity)
    if cached is not None:
        return cached

    temporary = _temporary_sibling(plan_dir)
    temporary.mkdir(parents=True)
    levels: list[dict[str, Any]] = []
    ancestors: list[tuple[int, np.ndarray]] = []
    terminal_count = 0
    try:
        for level in range(max_level + 1):
            node_run = node_runs.get(level)
            if node_run is None:
                continue
            filename = f"level-{level:02d}.u64"
            path = temporary / filename
            digest = hashlib.sha256()
            count = 0
            with open(path, "wb") as fp:
                for records in _iter_node_chunks(node_run.path):
                    selected = records[
                        (records["has_descendants"] != 0)
                        & (records["subtree_count"] >= 1)
                        & (records["subtree_count"] <= waterline)
                    ]["node_id"]
                    for ancestor_level, terminal_nodes in ancestors:
                        if len(selected) == 0:
                            break
                        candidate_ancestors = np.right_shift(
                            selected,
                            np.uint64(3 * (level - ancestor_level)),
                        )
                        positions = np.searchsorted(terminal_nodes, candidate_ancestors)
                        in_bounds = positions < len(terminal_nodes)
                        covered = np.zeros(len(selected), dtype=np.bool_)
                        if np.any(in_bounds):
                            covered[in_bounds] = (
                                terminal_nodes[positions[in_bounds]]
                                == candidate_ancestors[in_bounds]
                            )
                        selected = selected[~covered]
                    if len(selected):
                        data = np.asarray(selected, dtype="<u8").tobytes()
                        fp.write(data)
                        digest.update(data)
                        count += len(selected)
                fp.flush()
                os.fsync(fp.fileno())
            if count == 0:
                path.unlink()
                continue
            levels.append(
                {
                    "level": level,
                    "path": filename,
                    "count": count,
                    "checksum": f"sha256:{digest.hexdigest()}",
                }
            )
            terminal_count += count
            ancestors.append(
                (level, np.memmap(path, dtype="<u8", mode="r", shape=(count,)))
            )
        del ancestors
        plan = {
            "format": _TERMINAL_PLAN_FORMAT,
            "identity": plan_identity,
            "max_level": max_level,
            "waterline": waterline,
            "terminal_count": terminal_count,
            "levels": levels,
        }
        _atomic_write_json(temporary / "manifest.json", plan)
        _publish_immutable_directory(temporary, plan_dir)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    result = _load_terminal_plan(plan_dir, expected_identity=plan_identity)
    if result is None:
        raise ValueError("Published terminal plan is invalid")
    return result


def _publish_terminal_map(
    plan: dict[str, Any],
    *,
    artifacts_dir: Path,
    max_level: int,
    waterline: int,
    source_identity: str,
    counts_identity: str,
) -> Path:
    plan_dir_name = str(plan["identity"]).removeprefix("sha256:")
    levels = [
        {
            "level": int(entry["level"]),
            "path": (f"{TERMINAL_MAP_DIR_NAME}/{plan_dir_name}/{entry['path']}"),
            "count": int(entry["count"]),
        }
        for entry in plan["levels"]
    ]
    manifest_path = artifacts_dir / TERMINAL_MAP_NAME
    _atomic_write_json(
        manifest_path,
        {
            "format": TERMINAL_MAP_FORMAT,
            "max_level": max_level,
            "waterline": waterline,
            "terminal_count": int(plan["terminal_count"]),
            "source_input_identity": source_identity,
            "counts_identity": counts_identity,
            "plan_identity": plan["identity"],
            "levels": levels,
        },
    )
    return manifest_path


def _refresh_source_identity(manifest_path: Path, *, source_identity: str) -> None:
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    if raw.get("source_input_identity") == source_identity:
        return
    raw["source_input_identity"] = source_identity
    _atomic_write_json(manifest_path, raw)


def _counts_identity(own_runs: dict[int, _CountRun], *, max_level: int) -> str:
    return _identity(
        {
            "algorithm": _COUNT_ALGORITHM,
            "max_level": max_level,
            "levels": [
                {"level": level, **run.content_record()}
                for level, run in sorted(own_runs.items())
            ],
        }
    )


def _reduce_count_runs(
    inputs: Sequence[_CountRun],
    output: Path,
    *,
    scratch: Path,
    merge_fan_in: int,
) -> _CountRun:
    current = list(inputs)
    temporary_paths: set[Path] = set()
    sequence = 0
    while len(current) > merge_fan_in:
        reduced: list[_CountRun] = []
        for start in range(0, len(current), merge_fan_in):
            chunk = current[start : start + merge_fan_in]
            if len(chunk) == 1:
                reduced.append(chunk[0])
                continue
            sequence += 1
            merged = _merge_count_run_batch(
                chunk, scratch / f"reduce-{sequence:08d}.counts"
            )
            reduced.append(merged)
            temporary_paths.add(merged.path)
            for input_run in chunk:
                if input_run.path in temporary_paths:
                    input_run.path.unlink()
                    temporary_paths.remove(input_run.path)
        current = reduced
    if len(current) == 1:
        source = current[0]
        if source.path != output:
            shutil.copyfile(source.path, output)
        if source.path in temporary_paths:
            source.path.unlink()
        return _CountRun(
            output, source.record_count, source.star_count, source.checksum
        )
    return _merge_count_run_batch(current, output)


def _merge_count_run_batch(inputs: Sequence[_CountRun], output: Path) -> _CountRun:
    if not inputs:
        raise ValueError("Cannot merge an empty count-run set")
    star_count = sum(run.star_count for run in inputs)
    if star_count > _MAX_U64:
        raise OverflowError("Terminal star count exceeds uint64")
    chunk_records = max(1_024, _IO_RECORDS // len(inputs))
    cursors = [_CountChunkCursor(run, chunk_records=chunk_records) for run in inputs]
    digest = hashlib.sha256()
    record_count = 0
    try:
        with open(output, "wb") as fp:
            while active := [cursor for cursor in cursors if cursor.ensure_buffer()]:
                watermark = min(cursor.last_node for cursor in active)
                pieces = [cursor.take_through(watermark) for cursor in active]
                pieces = [piece for piece in pieces if len(piece)]
                records = np.concatenate(pieces)
                order = np.argsort(records["node_id"], kind="stable")
                records = records[order]
                nodes = records["node_id"]
                starts = np.r_[0, np.flatnonzero(nodes[1:] != nodes[:-1]) + 1]
                merged = np.empty(len(starts), dtype=_COUNT_DTYPE)
                merged["node_id"] = nodes[starts]
                merged["star_count"] = np.add.reduceat(
                    records["star_count"], starts, dtype=np.uint64
                )
                data = merged.tobytes()
                fp.write(data)
                digest.update(data)
                record_count += len(merged)
            fp.flush()
            os.fsync(fp.fileno())
    finally:
        for cursor in cursors:
            cursor.close()
    return _CountRun(
        output,
        record_count,
        star_count,
        f"sha256:{digest.hexdigest()}",
    )


class _CountChunkCursor:
    def __init__(self, run: _CountRun, *, chunk_records: int) -> None:
        self.fp = open(run.path, "rb")  # noqa: SIM115 - closed after the merge
        self.remaining = run.record_count
        self.chunk_records = chunk_records
        self.buffer = np.empty(0, dtype=_COUNT_DTYPE)
        self.offset = 0
        self.previous: int | None = None

    @property
    def last_node(self) -> int:
        return int(self.buffer[-1]["node_id"])

    def ensure_buffer(self) -> bool:
        if self.offset < len(self.buffer):
            return True
        if self.remaining == 0:
            return False
        count = min(self.chunk_records, self.remaining)
        self.buffer = np.fromfile(self.fp, dtype=_COUNT_DTYPE, count=count)
        self.offset = 0
        if len(self.buffer) != count:
            raise ValueError(f"Truncated count run: {self.fp.name}")
        nodes = self.buffer["node_id"]
        counts = self.buffer["star_count"]
        if (self.previous is not None and int(nodes[0]) <= self.previous) or (
            len(nodes) > 1 and np.any(nodes[1:] <= nodes[:-1])
        ):
            raise ValueError(f"Non-ascending count run: {self.fp.name}")
        if np.any(counts == 0):
            raise ValueError(f"Non-positive count run record: {self.fp.name}")
        self.previous = int(nodes[-1])
        self.remaining -= count
        return True

    def take_through(self, node_id: int) -> np.ndarray:
        end = int(
            np.searchsorted(self.buffer["node_id"], np.uint64(node_id), side="right")
        )
        start = self.offset
        self.offset = max(self.offset, end)
        return self.buffer[start:end]

    def close(self) -> None:
        self.fp.close()


def _iter_count_chunks(path: Path) -> Iterator[np.ndarray]:
    previous: int | None = None
    with open(path, "rb") as fp:
        while len(records := np.fromfile(fp, dtype=_COUNT_DTYPE, count=_IO_RECORDS)):
            nodes = records["node_id"]
            if (previous is not None and int(nodes[0]) <= previous) or (
                len(nodes) > 1 and np.any(nodes[1:] <= nodes[:-1])
            ):
                raise ValueError(f"Non-ascending count run: {path}")
            if np.any(records["star_count"] == 0):
                raise ValueError(f"Non-positive count run record: {path}")
            previous = int(nodes[-1])
            yield records


class _ArrayCursor:
    def __init__(
        self, chunks: Iterator[tuple[np.ndarray, np.ndarray, np.ndarray]]
    ) -> None:
        self.chunks = iter(chunks)
        self.nodes = np.empty(0, dtype=np.uint64)
        self.counts = np.empty(0, dtype=np.uint64)
        self.flags = np.empty(0, dtype=np.uint8)
        self.offset = 0

    @property
    def last_node(self) -> int:
        return int(self.nodes[-1])

    def ensure_buffer(self) -> bool:
        if self.offset < len(self.nodes):
            return True
        try:
            self.nodes, self.counts, self.flags = next(self.chunks)
        except StopIteration:
            return False
        self.offset = 0
        return len(self.nodes) > 0

    def take_through(self, node_id: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        end = int(np.searchsorted(self.nodes, np.uint64(node_id), side="right"))
        start = self.offset
        self.offset = max(self.offset, end)
        return (
            self.nodes[start:end],
            self.counts[start:end],
            self.flags[start:end],
        )


def _iter_node_records(path: Path) -> Iterator[tuple[int, int, bool]]:
    previous: int | None = None
    for records in _iter_node_chunks(path):
        for record in records:
            node_id = int(record["node_id"])
            if previous is not None and node_id <= previous:
                raise ValueError(f"Non-ascending node run: {path}")
            previous = node_id
            yield (
                node_id,
                int(record["subtree_count"]),
                bool(record["has_descendants"]),
            )


def _iter_node_chunks(path: Path) -> Iterator[np.ndarray]:
    with open(path, "rb") as fp:
        while len(records := np.fromfile(fp, dtype=_NODE_DTYPE, count=_IO_RECORDS)):
            yield records


def _write_count_arrays(path: Path, nodes: np.ndarray, counts: np.ndarray) -> _CountRun:
    records = np.empty(len(nodes), dtype=_COUNT_DTYPE)
    records["node_id"] = nodes
    records["star_count"] = counts
    data = records.tobytes()
    with open(path, "wb") as fp:
        fp.write(data)
        fp.flush()
        os.fsync(fp.fileno())
    return _CountRun(
        path,
        len(records),
        int(np.asarray(counts, dtype=np.uint64).sum(dtype=np.uint64)),
        f"sha256:{hashlib.sha256(data).hexdigest()}",
    )


def _checked_add(left: int, right: int) -> int:
    total = int(left) + int(right)
    if total > _MAX_U64:
        raise OverflowError("Terminal star count exceeds uint64")
    return total


def _load_group_counts(
    directory: Path,
    *,
    source_identity: str,
    expected_rows: int,
    max_level: int,
) -> _GroupCounts | None:
    raw = _read_json(directory / "manifest.json")
    if raw is None:
        return None
    try:
        if raw.get("format") != _GROUP_COUNTS_FORMAT:
            return None
        if raw.get("source_identity") != source_identity:
            return None
        if int(raw["max_level"]) != max_level:
            return None
        if int(raw["row_count"]) != expected_rows:
            return None
        levels: dict[int, _CountRun] = {}
        for entry in raw["levels"]:
            level = int(entry["level"])
            if level in levels or level < 0 or level > max_level:
                return None
            run = _count_run_from_entry(directory, entry)
            if run is None:
                return None
            levels[level] = run
        if sum(run.star_count for run in levels.values()) != expected_rows:
            return None
        return _GroupCounts(source_identity, expected_rows, levels)
    except (KeyError, OSError, TypeError, ValueError):
        return None


def _load_single_count_run(
    directory: Path,
    *,
    expected_format: str,
    expected_identity: str,
    expected_level: int,
) -> _CountRun | None:
    raw = _read_json(directory / "manifest.json")
    if raw is None:
        return None
    try:
        if raw.get("format") != expected_format:
            return None
        if raw.get("identity") != expected_identity:
            return None
        if int(raw["level"]) != expected_level:
            return None
        return _count_run_from_entry(directory, raw["run"])
    except (KeyError, OSError, TypeError, ValueError):
        return None


def _count_run_from_entry(directory: Path, entry: dict[str, Any]) -> _CountRun | None:
    path = directory / str(entry["path"])
    record_count = int(entry["record_count"])
    star_count = int(entry["star_count"])
    checksum = str(entry["checksum"])
    if record_count <= 0 or star_count <= 0:
        return None
    if not checksum.startswith("sha256:"):
        return None
    if (
        not path.is_file()
        or path.stat().st_size != record_count * _COUNT_DTYPE.itemsize
    ):
        return None
    return _CountRun(path, record_count, star_count, checksum)


def _load_node_run(
    directory: Path, *, expected_identity: str, expected_level: int
) -> _NodeRun | None:
    raw = _read_json(directory / "manifest.json")
    if raw is None:
        return None
    try:
        if raw.get("format") != _NODE_COUNTS_FORMAT:
            return None
        if raw.get("identity") != expected_identity:
            return None
        if int(raw["level"]) != expected_level:
            return None
        entry = raw["run"]
        path = directory / str(entry["path"])
        record_count = int(entry["record_count"])
        checksum = str(entry["checksum"])
        if record_count <= 0 or not checksum.startswith("sha256:"):
            return None
        if (
            not path.is_file()
            or path.stat().st_size != record_count * _NODE_DTYPE.itemsize
        ):
            return None
        return _NodeRun(path, record_count, checksum)
    except (KeyError, OSError, TypeError, ValueError):
        return None


def _load_terminal_plan(
    directory: Path, *, expected_identity: str
) -> dict[str, Any] | None:
    raw = _read_json(directory / "manifest.json")
    if raw is None:
        return None
    try:
        if raw.get("format") != _TERMINAL_PLAN_FORMAT:
            return None
        if raw.get("identity") != expected_identity:
            return None
        counted = 0
        seen: set[int] = set()
        for entry in raw["levels"]:
            level = int(entry["level"])
            count = int(entry["count"])
            path = directory / str(entry["path"])
            if level in seen or count <= 0:
                return None
            seen.add(level)
            if not path.is_file() or path.stat().st_size != count * 8:
                return None
            counted += count
        if counted != int(raw["terminal_count"]):
            return None
        return raw
    except (KeyError, OSError, TypeError, ValueError):
        return None


def _count_manifest_entry(
    level: int, run: _CountRun, *, relative_to: Path
) -> dict[str, Any]:
    return {
        "level": level,
        "path": run.path.relative_to(relative_to).as_posix(),
        **run.content_record(),
    }


def _terminal_map_is_valid(
    manifest_path: Path,
    *,
    max_level: int,
    waterline: int,
    source_input_identity: str | None = None,
    counts_identity: str | None = None,
) -> bool:
    if not manifest_path.is_file():
        return False
    try:
        terminal_map = TerminalMap(manifest_path)
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        if terminal_map.max_level != max_level or terminal_map.waterline != waterline:
            return False
        if source_input_identity is not None:
            return raw.get("source_input_identity") == source_input_identity
        if counts_identity is not None:
            return raw.get("counts_identity") == counts_identity
        return True
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _identity(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return raw if isinstance(raw, dict) else None


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(path)
    with open(temporary, "w", encoding="utf-8") as fp:
        json.dump(value, fp, indent=2)
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(temporary, path)


def _temporary_sibling(path: Path) -> Path:
    return path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")


def _publish_immutable_directory(temporary: Path, target: Path) -> None:
    """Publish once; a concurrent writer's existing target wins unchanged."""
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(temporary, target)
    except OSError as error:
        if error.errno not in (errno.EEXIST, errno.ENOTEMPTY) or not target.is_dir():
            raise
        shutil.rmtree(temporary)
