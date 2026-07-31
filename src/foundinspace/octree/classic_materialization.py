from __future__ import annotations

import gzip
import hashlib
import heapq
import io
import json
import os
import shutil
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import redirect_stdout
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .assembly.formats import (
    IDENTIFIERS_ARTIFACT_KIND,
    IDENTIFIERS_INDEX_MAGIC,
    IDENTIFIERS_MANIFEST_NAME,
    INDEX_MAGIC,
    RENDER_ARTIFACT_KIND,
    RENDER_MANIFEST_NAME,
)
from .assembly.identity_encoder import write_identity_arrays
from .assembly.manifest import (
    manifest_entries,
    read_manifest,
    validate_shard,
    write_manifest,
)
from .assembly.types import CellKey, ShardKey
from .assembly.writer import IntermediateShardWriter, identifiers_shard_filenames
from .config import (
    DEFAULT_STAR_FORMAT_VERSION,
    DEFAULT_TERMINAL_WATERLINE,
    MORTON_BITS,
)
from .duckdb_util import (
    MEMORY_LIMIT,
    PRESERVE_INSERTION_ORDER,
    TEMP_DIR,
    configure_connection,
)
from .encoding.render import RENDER_RECORD_SIZE, encode_render_records
from .sources.stage00 import _atomic_write_json
from .terminal_packing import TerminalMap, build_terminal_map

CLASSIC_BUILD_STATE_NAME = "classic-build-state.json"
CLASSIC_BUILD_STATE_FORMAT = "foundinspace.octree.classic-build/v1"
CLASSIC_WORK_STATE_NAME = "classic-work-state.json"
CLASSIC_WORK_STATE_FORMAT = "foundinspace.octree.classic-work/v1"
CLASSIC_ALGORITHM_VERSION = "sorted-cell-merge-terminal-packing/v3"
CLASSIC_OVERLAP_IN_MEMORY_MAX_BYTES = 256 * 1024 * 1024
CLASSIC_OVERLAP_EXTERNAL_SORT_MEMORY_LIMIT = "512MB"

_RAW_COLUMNS = (
    "x_icrs_pc",
    "y_icrs_pc",
    "z_icrs_pc",
    "mag_abs",
    "source",
    "source_id",
    "morton_code",
    "level",
)
_OPTIONAL_COLUMNS = ("teff",)
_COMPACT_SCHEMA = pa.schema(
    [
        pa.field("final_level", pa.int16(), nullable=False),
        pa.field("final_node_id", pa.uint64(), nullable=False),
        pa.field("mag_abs", pa.float64()),
        pa.field("source", pa.string(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("render", pa.binary(RENDER_RECORD_SIZE), nullable=False),
    ]
)
_COMPACT_COLUMNS = tuple(_COMPACT_SCHEMA.names)
_CANONICAL_SORT_KEYS = [
    ("final_level", "ascending"),
    ("final_node_id", "ascending"),
    ("mag_abs", "ascending"),
    ("source", "ascending"),
    ("source_id", "ascending"),
]
_CONTRIBUTOR_COLUMN = "_classic_contributor_index"
_CONTRIBUTOR_ROW_COLUMN = "_classic_contributor_row"
_OVERLAP_SCHEMA = _COMPACT_SCHEMA.append(
    pa.field(_CONTRIBUTOR_COLUMN, pa.int32(), nullable=False)
).append(
    pa.field(_CONTRIBUTOR_ROW_COLUMN, pa.int64(), nullable=False),
)
_OVERLAP_SORT_KEYS = [
    ("mag_abs", "ascending"),
    ("source", "ascending"),
    ("source_id", "ascending"),
    (_CONTRIBUTOR_COLUMN, "ascending"),
    (_CONTRIBUTOR_ROW_COLUMN, "ascending"),
]


@dataclass(frozen=True, slots=True)
class Stage01GroupInput:
    key: str
    checksum: str
    row_count: int
    files: tuple[Path, ...]
    natural_max_level: int | None = None


@dataclass(frozen=True, slots=True)
class ClassicMaterializationPlan:
    max_level: int
    mag_limit: float
    batch_size: int
    max_open_files: int
    partition_from_level: int
    partition_prefix_bits: int
    star_format_version: int = DEFAULT_STAR_FORMAT_VERSION
    terminal_waterline: int = DEFAULT_TERMINAL_WATERLINE

    @property
    def merge_fan_in(self) -> int:
        return max(2, self.max_open_files)


@dataclass(frozen=True, slots=True)
class ClassicMaterializationResult:
    render_manifest_path: Path
    identifiers_manifest_path: Path
    row_count: int
    folded_row_count: int
    cell_count: int
    input_identity: str


@dataclass(frozen=True, slots=True)
class _RunInfo:
    path: Path
    shard: ShardKey
    row_count: int


class _RunCursor:
    def __init__(self, path: Path, *, batch_size: int) -> None:
        parquet = pq.ParquetFile(path)
        self._batches = iter(
            parquet.iter_batches(
                batch_size=batch_size,
                columns=list(_COMPACT_COLUMNS),
            )
        )
        self._table: pa.Table | None = None
        self._cell_ends = np.empty(0, dtype=np.int64)
        self._offset = 0
        self._load_batch()

    @property
    def exhausted(self) -> bool:
        return self._table is None

    @property
    def cell_key(self) -> tuple[int, int]:
        table = self._require_table()
        return (
            int(table.column("final_level")[self._offset].as_py()),
            int(table.column("final_node_id")[self._offset].as_py()),
        )

    def take_cell_chunk(self, key: tuple[int, int]) -> pa.Table:
        table = self._require_table()
        start = self._offset
        boundary_index = int(np.searchsorted(self._cell_ends, start, side="right"))
        end = int(self._cell_ends[boundary_index])
        chunk = table.slice(start, end - start)
        self._offset = end
        self._load_batch_if_consumed()
        return chunk

    def _load_batch_if_consumed(self) -> None:
        if self._table is not None and self._offset >= len(self._table):
            self._load_batch()

    def _load_batch(self) -> None:
        for batch in self._batches:
            if len(batch) == 0:
                continue
            self._table = pa.Table.from_batches([batch], schema=_COMPACT_SCHEMA)
            levels = np.asarray(self._table.column("final_level"), dtype=np.int16)
            nodes = np.asarray(self._table.column("final_node_id"), dtype=np.uint64)
            self._cell_ends = np.append(
                np.flatnonzero((levels[1:] != levels[:-1]) | (nodes[1:] != nodes[:-1]))
                + 1,
                len(self._table),
            )
            self._offset = 0
            return
        self._table = None
        self._cell_ends = np.empty(0, dtype=np.int64)
        self._offset = 0

    def _require_table(self) -> pa.Table:
        if self._table is None:
            raise RuntimeError("Sorted run cursor is exhausted")
        return self._table


def classic_input_identity(
    groups: Sequence[Stage01GroupInput],
    plan: ClassicMaterializationPlan,
) -> str:
    canonical = {
        "format": CLASSIC_BUILD_STATE_FORMAT,
        "algorithm": CLASSIC_ALGORITHM_VERSION,
        "groups": [
            {
                "key": group.key,
                "checksum": group.checksum,
                "row_count": group.row_count,
                "files": [path.as_posix() for path in group.files],
            }
            for group in groups
        ],
        "max_level": plan.max_level,
        "mag_limit": plan.mag_limit,
        "partition_from_level": plan.partition_from_level,
        "partition_prefix_bits": plan.partition_prefix_bits,
        "star_format_version": plan.star_format_version,
        "terminal_waterline": (
            plan.terminal_waterline if plan.star_format_version == 2 else None
        ),
        "row_schema": list(_RAW_COLUMNS),
        "render_record_size": RENDER_RECORD_SIZE,
        "sort_keys": [name for name, _order in _CANONICAL_SORT_KEYS],
    }
    payload = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def load_published_materialization(
    out_dir: Path,
    *,
    input_identity: str,
    plan: ClassicMaterializationPlan,
) -> ClassicMaterializationResult | None:
    state_path = out_dir / CLASSIC_BUILD_STATE_NAME
    if not state_path.is_file():
        return None
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("format") != CLASSIC_BUILD_STATE_FORMAT:
            return None
        if state.get("input_identity") != input_identity:
            return None
        render_manifest = read_manifest(out_dir, name=RENDER_MANIFEST_NAME)
        identifiers_manifest = read_manifest(out_dir, name=IDENTIFIERS_MANIFEST_NAME)
        if render_manifest is None or identifiers_manifest is None:
            return None
        _validate_published_terminal_map(
            out_dir,
            render_manifest,
            plan=plan,
        )
        for entry in manifest_entries(render_manifest):
            validate_shard(out_dir, entry, expected_magic=INDEX_MAGIC)
        for entry in manifest_entries(identifiers_manifest):
            validate_shard(
                out_dir,
                entry,
                expected_magic=IDENTIFIERS_INDEX_MAGIC,
            )
        return ClassicMaterializationResult(
            render_manifest_path=out_dir / RENDER_MANIFEST_NAME,
            identifiers_manifest_path=out_dir / IDENTIFIERS_MANIFEST_NAME,
            row_count=int(state["row_count"]),
            folded_row_count=int(state["folded_row_count"]),
            cell_count=int(state["cell_count"]),
            input_identity=input_identity,
        )
    except (KeyError, OSError, TypeError, ValueError):
        return None


def _validate_published_terminal_map(
    out_dir: Path,
    render_manifest: dict[str, Any],
    *,
    plan: ClassicMaterializationPlan,
) -> None:
    terminal_map_path_raw = render_manifest.get("terminal_map_path")
    if plan.star_format_version == 1:
        if terminal_map_path_raw is not None:
            raise ValueError("STAR v1 materialization unexpectedly has a terminal map")
        return
    if not isinstance(terminal_map_path_raw, str) or not terminal_map_path_raw:
        raise ValueError("STAR v2 materialization is missing its terminal map")
    terminal_map = TerminalMap(out_dir / terminal_map_path_raw)
    if terminal_map.max_level != plan.max_level:
        raise ValueError(
            "Published terminal map max_level does not match the materialization plan"
        )
    if terminal_map.waterline != plan.terminal_waterline:
        raise ValueError(
            "Published terminal map waterline does not match the materialization plan"
        )


def materialize_classic_groups(
    *,
    groups: Sequence[Stage01GroupInput],
    work_dir: Path,
    plan: ClassicMaterializationPlan,
) -> ClassicMaterializationResult:
    if not groups:
        raise ValueError("Classic materialization requires Stage 01 groups")
    input_identity = classic_input_identity(groups, plan)
    state = _prepare_work_state(work_dir, input_identity=input_identity)
    runs_dir = work_dir / "runs"
    merge_dir = work_dir / "merge"
    artifacts_dir = work_dir / "artifacts"
    runs_dir.mkdir(parents=True, exist_ok=True)
    merge_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    terminal_map: TerminalMap | None = None
    terminal_map_manifest_path: Path | None = None
    if plan.star_format_version == 2:
        terminal_map_manifest_path = build_terminal_map(
            groups=groups,
            work_dir=work_dir,
            artifacts_dir=artifacts_dir,
            max_level=plan.max_level,
            waterline=plan.terminal_waterline,
            batch_size=plan.batch_size,
        )
        terminal_map = TerminalMap(terminal_map_manifest_path)

    completed_groups = state.setdefault("completed_groups", {})
    for group in groups:
        existing = completed_groups.get(group.key)
        if _completed_group_is_valid(work_dir, existing):
            continue
        group_result = _normalize_group(
            group,
            runs_dir=runs_dir,
            plan=plan,
            terminal_map=terminal_map,
        )
        completed_groups[group.key] = group_result
        _atomic_write_json(work_dir / CLASSIC_WORK_STATE_NAME, state)

    runs_by_partition: dict[str, list[_RunInfo]] = {}
    folded_row_count = 0
    row_count = 0
    for group in groups:
        result = completed_groups[group.key]
        row_count += int(result["row_count"])
        folded_row_count += int(result["folded_row_count"])
        for raw_run in result.get("runs", []):
            run = _run_from_state(work_dir, raw_run)
            runs_by_partition.setdefault(_shard_state_key(run.shard), []).append(run)

    expected_rows = sum(group.row_count for group in groups)
    if row_count != expected_rows:
        raise ValueError(
            "Classic normalized run row count mismatch: "
            f"expected={expected_rows}, actual={row_count}"
        )

    completed_partitions = state.setdefault("completed_partitions", {})
    for partition_key in sorted(runs_by_partition, key=_shard_state_sort_key):
        runs = runs_by_partition[partition_key]
        shard = runs[0].shard
        existing = completed_partitions.get(partition_key)
        if _completed_partition_is_valid(artifacts_dir, existing):
            continue
        result = _materialize_partition(
            runs,
            shard=shard,
            merge_root=merge_dir,
            artifacts_dir=artifacts_dir,
            plan=plan,
        )
        completed_partitions[partition_key] = result
        _atomic_write_json(work_dir / CLASSIC_WORK_STATE_NAME, state)

    render_entries: list[dict[str, Any]] = []
    identifiers_entries: list[dict[str, Any]] = []
    cell_count = 0
    partition_rows = 0
    for key in sorted(completed_partitions, key=_shard_state_sort_key):
        result = completed_partitions[key]
        render_entries.append(dict(result["render_entry"]))
        identifiers_entries.append(dict(result["identifiers_entry"]))
        cell_count += int(result["cell_count"])
        partition_rows += int(result["row_count"])
    if partition_rows != row_count:
        raise ValueError(
            "Classic partition row count mismatch: "
            f"normalized={row_count}, materialized={partition_rows}"
        )

    render_manifest_path = write_manifest(
        artifacts_dir,
        plan.max_level,
        render_entries,
        artifact_kind=RENDER_ARTIFACT_KIND,
        index_magic=INDEX_MAGIC,
        mag_limit=plan.mag_limit,
        name=RENDER_MANIFEST_NAME,
        terminal_map_path=terminal_map_manifest_path,
    )
    identifiers_manifest_path = write_manifest(
        artifacts_dir,
        plan.max_level,
        identifiers_entries,
        artifact_kind=IDENTIFIERS_ARTIFACT_KIND,
        index_magic=IDENTIFIERS_INDEX_MAGIC,
        mag_limit=plan.mag_limit,
        name=IDENTIFIERS_MANIFEST_NAME,
    )
    _atomic_write_json(
        artifacts_dir / CLASSIC_BUILD_STATE_NAME,
        {
            "format": CLASSIC_BUILD_STATE_FORMAT,
            "input_identity": input_identity,
            "row_count": row_count,
            "folded_row_count": folded_row_count,
            "cell_count": cell_count,
        },
    )
    return ClassicMaterializationResult(
        render_manifest_path=render_manifest_path,
        identifiers_manifest_path=identifiers_manifest_path,
        row_count=row_count,
        folded_row_count=folded_row_count,
        cell_count=cell_count,
        input_identity=input_identity,
    )


def _prepare_work_state(work_dir: Path, *, input_identity: str) -> dict[str, Any]:
    state_path = work_dir / CLASSIC_WORK_STATE_NAME
    if state_path.is_file():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            state = {}
        if (
            state.get("format") == CLASSIC_WORK_STATE_FORMAT
            and state.get("input_identity") == input_identity
        ):
            return state
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True)
    state = {
        "format": CLASSIC_WORK_STATE_FORMAT,
        "input_identity": input_identity,
        "completed_groups": {},
        "completed_partitions": {},
    }
    _atomic_write_json(state_path, state)
    return state


def _normalize_group(
    group: Stage01GroupInput,
    *,
    runs_dir: Path,
    plan: ClassicMaterializationPlan,
    terminal_map: TerminalMap | None,
) -> dict[str, Any]:
    group_dir = runs_dir / _safe_group_dir_name(group.key)
    if group_dir.exists():
        shutil.rmtree(group_dir)
    group_dir.mkdir(parents=True)
    if group.row_count == 0:
        return {
            "row_count": 0,
            "folded_row_count": 0,
            "runs": [],
        }
    requires_folding = _group_requires_folding(group, max_level=plan.max_level)
    requires_terminal_reordering = (
        terminal_map is not None and terminal_map.terminal_count > 0
    )
    if not requires_folding and not requires_terminal_reordering:
        return _stream_ordered_group(
            group,
            group_dir=group_dir,
            plan=plan,
            terminal_map=terminal_map,
        )
    return _externally_sort_folded_group(
        group,
        group_dir=group_dir,
        plan=plan,
        terminal_map=terminal_map,
    )


def _stream_ordered_group(
    group: Stage01GroupInput,
    *,
    group_dir: Path,
    plan: ClassicMaterializationPlan,
    terminal_map: TerminalMap | None,
) -> dict[str, Any]:
    runs: list[dict[str, Any]] = []
    current_shard: ShardKey | None = None
    current_path: Path | None = None
    current_writer: pq.ParquetWriter | None = None
    current_rows = 0
    row_count = 0

    def close_run() -> None:
        nonlocal current_writer, current_rows
        if current_writer is None or current_shard is None or current_path is None:
            return
        current_writer.close()
        runs.append(
            _run_state(
                path=current_path,
                work_dir=group_dir.parent.parent,
                shard=current_shard,
                row_count=current_rows,
            )
        )
        current_writer = None
        current_rows = 0

    try:
        for raw_batch in _iter_raw_group_batches(group, batch_size=plan.batch_size):
            compact, folded_rows = _normalize_batch(
                raw_batch,
                plan=plan,
                terminal_map=terminal_map,
            )
            if folded_rows:
                raise ValueError(
                    f"Stage 01 group {group.key} exceeded its tracked natural max level"
                )
            for shard, segment in _partition_segments(compact, plan=plan):
                if current_shard != shard:
                    if current_shard is not None and _shard_sort_key(
                        shard
                    ) <= _shard_sort_key(current_shard):
                        raise ValueError(
                            f"Stage 01 group {group.key} is not in canonical order"
                        )
                    close_run()
                    current_shard = shard
                    current_path = group_dir / f"{_shard_file_stem(shard)}.parquet"
                    current_writer = pq.ParquetWriter(
                        current_path,
                        _COMPACT_SCHEMA,
                        compression="zstd",
                    )
                assert current_writer is not None
                current_writer.write_table(
                    segment,
                    row_group_size=plan.batch_size,
                )
                current_rows += len(segment)
                row_count += len(segment)
    except Exception:
        if current_writer is not None:
            current_writer.close()
        raise
    close_run()
    _ensure_group_row_count(group, row_count)
    return {
        "row_count": row_count,
        "folded_row_count": 0,
        "runs": runs,
    }


def _externally_sort_folded_group(
    group: Stage01GroupInput,
    *,
    group_dir: Path,
    plan: ClassicMaterializationPlan,
    terminal_map: TerminalMap | None,
) -> dict[str, Any]:
    batches_dir = group_dir / "batches"
    merge_dir = group_dir / "merge"
    batches_dir.mkdir()
    merge_dir.mkdir()
    paths_by_shard: dict[ShardKey, list[Path]] = {}
    rows_by_shard: dict[ShardKey, int] = {}
    row_count = 0
    folded_row_count = 0
    for batch_index, raw_batch in enumerate(
        _iter_raw_group_batches(group, batch_size=plan.batch_size)
    ):
        compact, batch_folded_rows = _normalize_batch(
            raw_batch,
            plan=plan,
            terminal_map=terminal_map,
        )
        folded_row_count += batch_folded_rows
        compact = compact.take(
            pc.sort_indices(
                compact,
                sort_keys=_CANONICAL_SORT_KEYS,
                null_placement="at_end",
            )
        )
        for shard, segment in _partition_segments(compact, plan=plan):
            path = batches_dir / (
                f"batch-{batch_index:08d}-{_shard_file_stem(shard)}.parquet"
            )
            pq.write_table(
                segment,
                path,
                compression="zstd",
                row_group_size=plan.batch_size,
            )
            paths_by_shard.setdefault(shard, []).append(path)
            rows_by_shard[shard] = rows_by_shard.get(shard, 0) + len(segment)
            row_count += len(segment)

    _ensure_group_row_count(group, row_count)
    if folded_row_count == 0 and (
        terminal_map is None or terminal_map.terminal_count == 0
    ):
        raise ValueError(
            f"Stage 01 group {group.key} did not reach its tracked natural max level"
        )
    runs: list[dict[str, Any]] = []
    for shard in sorted(paths_by_shard, key=_shard_sort_key):
        paths = paths_by_shard[shard]
        shard_merge_dir = merge_dir / _shard_file_stem(shard)
        shard_merge_dir.mkdir()
        reduced = _reduce_runs(
            paths,
            partition_dir=shard_merge_dir,
            batch_size=plan.batch_size,
            fan_in=plan.merge_fan_in,
        )
        output = group_dir / f"{_shard_file_stem(shard)}.parquet"
        if len(reduced) == 1:
            os.replace(reduced[0], output)
        else:
            _write_merged_run(reduced, output, batch_size=plan.batch_size)
        runs.append(
            _run_state(
                path=output,
                work_dir=group_dir.parent.parent,
                shard=shard,
                row_count=rows_by_shard[shard],
            )
        )
    shutil.rmtree(batches_dir)
    shutil.rmtree(merge_dir)
    return {
        "row_count": row_count,
        "folded_row_count": folded_row_count,
        "runs": runs,
    }


def _normalize_batch(
    table: pa.Table,
    *,
    plan: ClassicMaterializationPlan,
    terminal_map: TerminalMap | None = None,
) -> tuple[pa.Table, int]:
    if len(table) == 0:
        return pa.Table.from_batches([], schema=_COMPACT_SCHEMA), 0

    source_levels = _numpy_column(table, "level", np.int32, required=True)
    morton_codes = _numpy_column(table, "morton_code", np.uint64, required=True)
    final_levels = np.minimum(source_levels, plan.max_level).astype(
        np.int16,
        copy=False,
    )
    final_node_ids = np.empty(len(table), dtype=np.uint64)
    for level_raw in np.unique(final_levels):
        level = int(level_raw)
        indices = np.flatnonzero(final_levels == level)
        final_node_ids[indices] = morton_codes[indices] >> np.uint64(
            3 * (MORTON_BITS - level)
        )
    if terminal_map is not None:
        final_levels, final_node_ids = terminal_map.remap(
            final_levels,
            final_node_ids,
        )

    positions = np.column_stack(
        [
            _numpy_column(table, name, np.float64, required=True)
            for name in ("x_icrs_pc", "y_icrs_pc", "z_icrs_pc")
        ]
    )
    magnitudes = _numpy_column(table, "mag_abs", np.float64, required=False)
    temperatures = _numpy_column(table, "teff", np.float64, required=False)
    render = encode_render_records(
        morton_codes=morton_codes,
        positions=positions,
        mag_abs=magnitudes,
        teff=temperatures,
        levels=final_levels,
        node_ids=final_node_ids,
    )
    render_column = pa.FixedSizeBinaryArray.from_buffers(
        pa.binary(RENDER_RECORD_SIZE),
        len(render),
        [None, pa.py_buffer(render)],
    )
    for name in ("source", "source_id"):
        if table.column(name).null_count:
            raise ValueError(f"Classic materialization input has null {name} values")
    compact = pa.table(
        {
            "final_level": pa.array(final_levels, type=pa.int16()),
            "final_node_id": pa.array(final_node_ids, type=pa.uint64()),
            "mag_abs": table.column("mag_abs").combine_chunks(),
            "source": table.column("source").combine_chunks(),
            "source_id": table.column("source_id").combine_chunks(),
            "render": render_column,
        },
        schema=_COMPACT_SCHEMA,
    )
    folded_row_count = int(np.count_nonzero(source_levels > plan.max_level))
    return compact, folded_row_count


def _partition_segments(
    compact: pa.Table,
    *,
    plan: ClassicMaterializationPlan,
) -> Iterator[tuple[ShardKey, pa.Table]]:
    levels = np.asarray(compact.column("final_level"), dtype=np.int16)
    nodes = np.asarray(compact.column("final_node_id"), dtype=np.uint64)
    start = 0
    while start < len(compact):
        shard = _shard_for_cell(int(levels[start]), int(nodes[start]), plan)
        end = start + 1
        while end < len(compact):
            candidate = _shard_for_cell(int(levels[end]), int(nodes[end]), plan)
            if candidate != shard:
                break
            end += 1
        yield shard, compact.slice(start, end - start)
        start = end


def _iter_raw_group_batches(
    group: Stage01GroupInput,
    *,
    batch_size: int,
) -> Iterator[pa.Table]:
    for path in group.files:
        schema = pq.read_schema(path)
        names = set(schema.names)
        missing = sorted(set(_RAW_COLUMNS) - names)
        if missing:
            raise ValueError(
                "Classic materialization requires raw Stage 01 fields; "
                f"{path} is missing {missing}. Rebuild Stage 00 and Stage 01."
            )
        columns = [*_RAW_COLUMNS]
        columns.extend(name for name in _OPTIONAL_COLUMNS if name in names)
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=batch_size, columns=columns):
            table = pa.Table.from_batches([batch])
            if "teff" not in table.schema.names:
                table = table.append_column(
                    "teff",
                    pa.nulls(len(table), type=pa.float64()),
                )
            yield table


def _group_requires_folding(
    group: Stage01GroupInput,
    *,
    max_level: int,
) -> bool:
    if group.natural_max_level is not None:
        return group.natural_max_level > max_level
    for path in group.files:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(
            batch_size=64 * 1024,
            columns=["level"],
        ):
            levels = np.asarray(batch.column(0), dtype=np.int32)
            if len(levels) and int(levels.max()) > max_level:
                return True
    return False


def _ensure_group_row_count(group: Stage01GroupInput, actual: int) -> None:
    if actual != group.row_count:
        raise ValueError(
            f"Stage 01 group {group.key} row count mismatch: "
            f"state={group.row_count}, files={actual}"
        )


def _run_state(
    *,
    path: Path,
    work_dir: Path,
    shard: ShardKey,
    row_count: int,
) -> dict[str, Any]:
    return {
        "path": path.relative_to(work_dir).as_posix(),
        "level": shard.level,
        "prefix_bits": shard.prefix_bits,
        "prefix": shard.prefix,
        "row_count": row_count,
    }


def _numpy_column(
    table: pa.Table,
    name: str,
    dtype: np.dtype[Any] | type[Any],
    *,
    required: bool,
) -> np.ndarray:
    if name not in table.schema.names:
        if required:
            raise ValueError(f"Classic materialization input is missing {name}")
        return np.full(len(table), np.nan, dtype=dtype)
    column = table.column(name).combine_chunks()
    if required and column.null_count:
        raise ValueError(f"Classic materialization input has null {name} values")
    values = column.to_numpy(zero_copy_only=False)
    if not required and column.null_count:
        values = np.asarray(values, dtype=np.float64)
        values[column.is_null().to_numpy(zero_copy_only=False)] = np.nan
    return np.asarray(values, dtype=dtype)


def _shard_for_cell(
    level: int,
    node_id: int,
    plan: ClassicMaterializationPlan,
) -> ShardKey:
    if (
        level == 0
        or level < plan.partition_from_level
        or plan.partition_prefix_bits == 0
    ):
        return ShardKey(level=level, prefix_bits=0, prefix=0)
    prefix_bits = min(plan.partition_prefix_bits, 3 * level)
    prefix = node_id >> (3 * level - prefix_bits)
    return ShardKey(level=level, prefix_bits=prefix_bits, prefix=prefix)


def _materialize_partition(
    runs: Sequence[_RunInfo],
    *,
    shard: ShardKey,
    merge_root: Path,
    artifacts_dir: Path,
    plan: ClassicMaterializationPlan,
) -> dict[str, Any]:
    partition_dir = merge_root / _shard_file_stem(shard)
    if partition_dir.exists():
        shutil.rmtree(partition_dir)
    partition_dir.mkdir(parents=True)
    final_runs = _reduce_runs(
        [run.path for run in runs],
        partition_dir=partition_dir,
        batch_size=plan.batch_size,
        fan_in=plan.merge_fan_in,
    )
    render_writer = IntermediateShardWriter(shard, artifacts_dir)
    identifiers_writer = IntermediateShardWriter(
        shard,
        artifacts_dir,
        index_magic=IDENTIFIERS_INDEX_MAGIC,
        filename_fn=identifiers_shard_filenames,
    )
    row_count = 0
    cell_count = 0
    render_cell_path = partition_dir / "render-cell.gz"
    identifiers_cell_path = partition_dir / "identifiers-cell.gz"
    try:
        merged_batches = _iter_merged_batches(
            final_runs,
            batch_size=plan.batch_size,
            spill_dir=partition_dir,
        )
        for key, keyed_batches in groupby(merged_batches, key=lambda item: item[0]):
            star_count = 0
            with (
                open(render_cell_path, "wb") as render_fp,
                gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=render_fp,
                    mtime=0,
                ) as render_gzip,
                open(identifiers_cell_path, "wb") as identifiers_fp,
                gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=identifiers_fp,
                    mtime=0,
                ) as identifiers_gzip,
            ):
                for _batch_key, batch in keyed_batches:
                    render_gzip.write(_fixed_binary_bytes(batch.column("render")))
                    sources = batch.column("source").combine_chunks()
                    source_ids = batch.column("source_id").combine_chunks()
                    write_identity_arrays(
                        identifiers_gzip,
                        sources,
                        source_ids,
                    )
                    star_count += len(batch)
            cell_key = CellKey(level=key[0], node_id=key[1])
            render_writer.write_cell_payload_file(
                key=cell_key,
                payload_path=render_cell_path,
                star_count=star_count,
            )
            identifiers_writer.write_cell_payload_file(
                key=cell_key,
                payload_path=identifiers_cell_path,
                star_count=star_count,
            )
            row_count += star_count
            cell_count += 1
        render_entry = render_writer.close()
        identifiers_entry = identifiers_writer.close()
        if render_entry is None or identifiers_entry is None:
            raise ValueError(f"Classic partition unexpectedly empty: {shard}")
        if render_entry["record_count"] != identifiers_entry["record_count"]:
            raise ValueError("Classic render and identifiers record counts differ")
    except Exception:
        render_writer.abort()
        identifiers_writer.abort()
        raise
    finally:
        render_cell_path.unlink(missing_ok=True)
        identifiers_cell_path.unlink(missing_ok=True)
        shutil.rmtree(partition_dir, ignore_errors=True)
    return {
        "render_entry": render_entry,
        "identifiers_entry": identifiers_entry,
        "row_count": row_count,
        "cell_count": cell_count,
    }


def _reduce_runs(
    paths: list[Path],
    *,
    partition_dir: Path,
    batch_size: int,
    fan_in: int,
) -> list[Path]:
    current = list(paths)
    generated: set[Path] = set()
    round_index = 0
    while len(current) > fan_in:
        next_round: list[Path] = []
        for chunk_index, offset in enumerate(range(0, len(current), fan_in)):
            chunk = current[offset : offset + fan_in]
            if len(chunk) == 1:
                next_round.append(chunk[0])
                continue
            output = partition_dir / (
                f"merge-{round_index:03d}-{chunk_index:06d}.parquet"
            )
            _write_merged_run(
                chunk,
                output,
                batch_size=batch_size,
            )
            next_round.append(output)
            generated.add(output)
        for path in current:
            if path in generated and path not in next_round:
                path.unlink(missing_ok=True)
        current = next_round
        round_index += 1
    return current


def _write_merged_run(
    paths: Sequence[Path],
    output: Path,
    *,
    batch_size: int,
) -> None:
    tmp_path = output.with_name(f".{output.name}.tmp")
    writer = pq.ParquetWriter(tmp_path, _COMPACT_SCHEMA, compression="zstd")
    try:
        for _key, batch in _iter_merged_batches(
            paths,
            batch_size=batch_size,
            spill_dir=output.parent,
        ):
            writer.write_table(batch, row_group_size=batch_size)
    except Exception:
        writer.close()
        tmp_path.unlink(missing_ok=True)
        raise
    writer.close()
    os.replace(tmp_path, output)


def _iter_merged_batches(
    paths: Sequence[Path],
    *,
    batch_size: int,
    spill_dir: Path,
) -> Iterator[tuple[tuple[int, int], pa.Table]]:
    if batch_size <= 0:
        raise ValueError("Classic merge batch_size must be > 0")
    if not paths:
        return
    cursor_batch_size = max(1, batch_size // len(paths))
    cursors = [_RunCursor(path, batch_size=cursor_batch_size) for path in paths]
    cell_heap: list[tuple[int, int, int]] = []
    for index, cursor in enumerate(cursors):
        if not cursor.exhausted:
            level, node_id = cursor.cell_key
            heapq.heappush(cell_heap, (level, node_id, index))

    while cell_heap:
        level, node_id, index = heapq.heappop(cell_heap)
        key = (level, node_id)
        contributors = [index]
        while cell_heap and (cell_heap[0][0], cell_heap[0][1]) == key:
            _level, _node, other_index = heapq.heappop(cell_heap)
            contributors.append(other_index)

        if len(contributors) == 1:
            cursor = cursors[index]
            while not cursor.exhausted and cursor.cell_key == key:
                yield key, cursor.take_cell_chunk(key)
        else:
            yield from _merge_overlapping_cell(
                cursors,
                contributors,
                key=key,
                batch_size=batch_size,
                spill_dir=spill_dir,
            )

        for contributor in contributors:
            cursor = cursors[contributor]
            if not cursor.exhausted:
                next_level, next_node_id = cursor.cell_key
                heapq.heappush(
                    cell_heap,
                    (next_level, next_node_id, contributor),
                )


def _merge_overlapping_cell(
    cursors: Sequence[_RunCursor],
    contributors: Sequence[int],
    *,
    key: tuple[int, int],
    batch_size: int,
    spill_dir: Path,
) -> Iterator[tuple[tuple[int, int], pa.Table]]:
    buffered: list[pa.Table] = []
    buffered_rows = 0
    buffered_bytes = 0
    temporary_dir: tempfile.TemporaryDirectory | None = None
    spill_path: Path | None = None
    spill_writer: pq.ParquetWriter | None = None

    def start_spilling() -> None:
        nonlocal temporary_dir, spill_path, spill_writer
        temporary_dir = tempfile.TemporaryDirectory(
            prefix=".classic-overlap-sort-",
            dir=spill_dir,
        )
        spill_path = Path(temporary_dir.name) / "cell.parquet"
        spill_writer = pq.ParquetWriter(
            spill_path,
            _OVERLAP_SCHEMA,
            compression="zstd",
        )
        for table in buffered:
            spill_writer.write_table(table, row_group_size=batch_size)
        buffered.clear()

    try:
        for index in contributors:
            contributor_row = 0
            cursor = cursors[index]
            while not cursor.exhausted and cursor.cell_key == key:
                chunk = cursor.take_cell_chunk(key)
                tagged = _tag_overlap_chunk(
                    chunk,
                    contributor_index=index,
                    contributor_row=contributor_row,
                )
                contributor_row += len(tagged)
                if spill_writer is None and (
                    buffered_rows + len(tagged) > batch_size
                    or buffered_bytes + tagged.nbytes
                    > CLASSIC_OVERLAP_IN_MEMORY_MAX_BYTES
                ):
                    start_spilling()
                if spill_writer is None:
                    buffered.append(tagged)
                    buffered_rows += len(tagged)
                    buffered_bytes += tagged.nbytes
                else:
                    spill_writer.write_table(tagged, row_group_size=batch_size)

        if spill_writer is None:
            yield from _iter_in_memory_sorted_overlap(
                buffered,
                key=key,
                batch_size=batch_size,
            )
            return

        spill_writer.close()
        spill_writer = None
        assert spill_path is not None
        yield from _iter_externally_sorted_overlap(
            spill_path,
            key=key,
            batch_size=batch_size,
        )
    finally:
        if spill_writer is not None:
            spill_writer.close()
        if temporary_dir is not None:
            temporary_dir.cleanup()


def _tag_overlap_chunk(
    chunk: pa.Table,
    *,
    contributor_index: int,
    contributor_row: int,
) -> pa.Table:
    row_count = len(chunk)
    return chunk.append_column(
        pa.field(_CONTRIBUTOR_COLUMN, pa.int32(), nullable=False),
        pa.array(
            np.full(row_count, contributor_index, dtype=np.int32),
            type=pa.int32(),
        ),
    ).append_column(
        pa.field(_CONTRIBUTOR_ROW_COLUMN, pa.int64(), nullable=False),
        pa.array(
            np.arange(
                contributor_row,
                contributor_row + row_count,
                dtype=np.int64,
            ),
            type=pa.int64(),
        ),
    )


def _iter_in_memory_sorted_overlap(
    chunks: Sequence[pa.Table],
    *,
    key: tuple[int, int],
    batch_size: int,
) -> Iterator[tuple[tuple[int, int], pa.Table]]:
    combined = pa.concat_tables(chunks, promote_options="none")
    order = pc.sort_indices(
        combined,
        sort_keys=_OVERLAP_SORT_KEYS,
        null_placement="at_end",
    )
    sorted_table = combined.take(order).select(_COMPACT_COLUMNS)
    for offset in range(0, len(sorted_table), batch_size):
        yield key, sorted_table.slice(offset, batch_size)


def _iter_externally_sorted_overlap(
    spill_path: Path,
    *,
    key: tuple[int, int],
    batch_size: int,
) -> Iterator[tuple[tuple[int, int], pa.Table]]:
    local_spill_dir: Path | None = None
    if TEMP_DIR is None:
        local_spill_dir = spill_path.parent / "duckdb-spill"
        local_spill_dir.mkdir()

    con = duckdb.connect()
    try:
        with redirect_stdout(io.StringIO()):
            configure_connection(con)
        if local_spill_dir is not None:
            con.execute("SET temp_directory = ?", [str(local_spill_dir)])
        if MEMORY_LIMIT is None:
            con.execute(
                "SET memory_limit = ?",
                [CLASSIC_OVERLAP_EXTERNAL_SORT_MEMORY_LIMIT],
            )
        if PRESERVE_INSERTION_ORDER is None:
            con.execute("SET preserve_insertion_order = false")
        con.execute(_overlap_sort_query(spill_path))
        for batch in con.to_arrow_reader(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            if not table.schema.equals(_COMPACT_SCHEMA, check_metadata=False):
                table = table.cast(_COMPACT_SCHEMA)
            yield key, table.replace_schema_metadata(None)
    finally:
        con.close()
        if local_spill_dir is not None:
            shutil.rmtree(local_spill_dir, ignore_errors=True)


def _overlap_sort_query(path: Path) -> str:
    escaped = path.as_posix().replace("'", "''")
    return f"""
        SELECT
            final_level,
            final_node_id,
            mag_abs,
            source,
            source_id,
            render
        FROM read_parquet(
            '{escaped}',
            hive_partitioning = false,
            union_by_name = false
        )
        ORDER BY
            final_level ASC,
            final_node_id ASC,
            mag_abs ASC NULLS LAST,
            source ASC,
            source_id ASC,
            {_CONTRIBUTOR_COLUMN} ASC,
            {_CONTRIBUTOR_ROW_COLUMN} ASC
    """


def _fixed_binary_bytes(column: pa.ChunkedArray) -> bytes:
    values = column.combine_chunks()
    if values.null_count:
        raise ValueError("Classic compact render column contains nulls")
    data = values.buffers()[1]
    if data is None:
        return b""
    start = values.offset * RENDER_RECORD_SIZE
    end = start + len(values) * RENDER_RECORD_SIZE
    return bytes(memoryview(data)[start:end])


def _completed_group_is_valid(work_dir: Path, raw: Any) -> bool:
    if not isinstance(raw, dict):
        return False
    try:
        expected_rows = int(raw["row_count"])
        state_rows = sum(int(run["row_count"]) for run in raw["runs"])
        if state_rows != expected_rows:
            return False
        actual_rows = 0
        for run in raw["runs"]:
            path = work_dir / str(run["path"])
            if not path.is_file() or not pq.read_schema(path).equals(_COMPACT_SCHEMA):
                return False
            actual_rows += pq.read_metadata(path).num_rows
        return actual_rows == expected_rows
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _completed_partition_is_valid(artifacts_dir: Path, raw: Any) -> bool:
    if not isinstance(raw, dict):
        return False
    try:
        validate_shard(
            artifacts_dir,
            raw["render_entry"],
            expected_magic=INDEX_MAGIC,
        )
        validate_shard(
            artifacts_dir,
            raw["identifiers_entry"],
            expected_magic=IDENTIFIERS_INDEX_MAGIC,
        )
        return True
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _run_from_state(work_dir: Path, raw: dict[str, Any]) -> _RunInfo:
    return _RunInfo(
        path=work_dir / str(raw["path"]),
        shard=ShardKey(
            level=int(raw["level"]),
            prefix_bits=int(raw["prefix_bits"]),
            prefix=int(raw["prefix"]),
        ),
        row_count=int(raw["row_count"]),
    )


def _safe_group_dir_name(key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:20]
    return f"group-{digest}"


def _shard_state_key(shard: ShardKey) -> str:
    return f"{shard.level}:{shard.prefix_bits}:{shard.prefix}"


def _shard_state_sort_key(value: str) -> tuple[int, int, int]:
    level, prefix_bits, prefix = value.split(":", 2)
    return int(level), int(prefix_bits), int(prefix)


def _shard_sort_key(shard: ShardKey) -> tuple[int, int, int]:
    return shard.level, shard.prefix_bits, shard.prefix


def _shard_file_stem(shard: ShardKey) -> str:
    return (
        f"level-{shard.level:02d}-"
        f"bits-{shard.prefix_bits:02d}-prefix-{shard.prefix:08x}"
    )
