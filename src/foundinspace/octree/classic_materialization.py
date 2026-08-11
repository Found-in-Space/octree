from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any

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
from .encoding.render import RENDER_RECORD_SIZE, encode_render_records
from .materialization import runs as shared_runs
from .sources.stage00 import _atomic_write_json
from .terminal_packing import TerminalMap, build_terminal_map

CLASSIC_BUILD_STATE_NAME = "classic-build-state.json"
CLASSIC_BUILD_STATE_FORMAT = "foundinspace.octree.classic-build/v1"
CLASSIC_WORK_STATE_NAME = "classic-work-state.json"
CLASSIC_WORK_STATE_FORMAT = "foundinspace.octree.classic-work/v2"
CLASSIC_ALGORITHM_VERSION = "sorted-cell-merge-terminal-packing/v4"
CLASSIC_PARTITION_CACHE_DIR = "partition-cache"
CLASSIC_TOPOLOGY_CACHE_DIR = "topology-cache"
CLASSIC_OVERLAP_IN_MEMORY_MAX_BYTES = 256 * 1024 * 1024
CLASSIC_OVERLAP_EXTERNAL_SORT_MEMORY_LIMIT = "512MB"
# Merge runs are sequential-scan intermediates.  Keep their physical writes
# bounded independently of the number (and size distribution) of cells.
CLASSIC_MERGE_WRITE_MAX_BYTES = 256 * 1024 * 1024
CLASSIC_MERGE_WRITE_MAX_PIECES = 1024

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
_RUN_LAYOUT = shared_runs.SortedRunLayout(
    schema=_COMPACT_SCHEMA,
    cell_level_column="final_level",
    cell_node_column="final_node_id",
    overlap_sort_keys=tuple(_OVERLAP_SORT_KEYS[:-2]),
    contributor_column=_CONTRIBUTOR_COLUMN,
    contributor_row_column=_CONTRIBUTOR_ROW_COLUMN,
)


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
            }
            for group in sorted(groups, key=lambda item: item.key)
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
    return _materialization_identity(canonical)


def _topology_identity(
    plan: ClassicMaterializationPlan,
    terminal_map_path: Path | None,
) -> str:
    if plan.star_format_version == 1:
        return _materialization_identity(
            {
                "kind": "level-cap/v1",
                "max_level": plan.max_level,
            }
        )
    if terminal_map_path is None:
        raise ValueError("STAR v2 materialization requires a terminal map")
    raw = json.loads(terminal_map_path.read_text(encoding="utf-8"))
    level_content: list[dict[str, Any]] = []
    for entry in raw.get("levels", []):
        relative = Path(str(entry["path"]))
        level_content.append(
            {
                "level": int(entry["level"]),
                "count": int(entry["count"]),
                "checksum": _file_identity(terminal_map_path.parent / relative),
            }
        )
    return _materialization_identity(
        {
            "kind": "terminal-map/v1",
            "max_level": plan.max_level,
            "waterline": plan.terminal_waterline,
            # Deliberately exclude counts/source identities and physical paths.
            # Equal terminal decisions must have equal materialization identity,
            # even when different source counts produced those decisions.
            "levels": level_content,
        }
    )


def _group_materialization_identity(
    group: Stage01GroupInput,
    *,
    plan: ClassicMaterializationPlan,
    topology_identity: str,
) -> str:
    return _materialization_identity(
        {
            "format": "foundinspace.octree.normalized-group/v1",
            "algorithm": CLASSIC_ALGORITHM_VERSION,
            "group_key": group.key,
            "checksum": group.checksum,
            "row_count": group.row_count,
            "natural_max_level": group.natural_max_level,
            "max_level": plan.max_level,
            "partition_from_level": plan.partition_from_level,
            "partition_prefix_bits": plan.partition_prefix_bits,
            "star_format_version": plan.star_format_version,
            "topology_identity": topology_identity,
            "compact_schema": str(_COMPACT_SCHEMA),
            "sort_keys": _CANONICAL_SORT_KEYS,
        }
    )


def _partition_materialization_identity(
    partition_key: str,
    *,
    contributors: list[dict[str, Any]],
    topology_identity: str,
    plan: ClassicMaterializationPlan,
) -> str:
    return _materialization_identity(
        {
            "format": "foundinspace.octree.materialized-partition/v1",
            "algorithm": CLASSIC_ALGORITHM_VERSION,
            "partition_key": partition_key,
            "contributors": contributors,
            "topology_identity": topology_identity,
            "star_format_version": plan.star_format_version,
            "max_level": plan.max_level,
        }
    )


def _materialization_identity(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _file_identity(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fp:
        while chunk := fp.read(1024 * 1024):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


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
    groups = tuple(sorted(groups, key=lambda group: group.key))
    if len({group.key for group in groups}) != len(groups):
        raise ValueError("Classic materialization group keys must be unique")
    input_identity = classic_input_identity(groups, plan)
    state = _prepare_work_state(work_dir, input_identity=input_identity)
    runs_dir = work_dir / "runs"
    merge_dir = work_dir / "merge"
    artifacts_dir = work_dir / "artifacts"
    partition_cache_dir = work_dir / CLASSIC_PARTITION_CACHE_DIR
    topology_cache_dir = work_dir / CLASSIC_TOPOLOGY_CACHE_DIR
    runs_dir.mkdir(parents=True, exist_ok=True)
    merge_dir.mkdir(parents=True, exist_ok=True)
    partition_cache_dir.mkdir(parents=True, exist_ok=True)
    topology_cache_dir.mkdir(parents=True, exist_ok=True)
    terminal_map: TerminalMap | None = None
    cached_terminal_map_path: Path | None = None
    if plan.star_format_version == 2:
        cached_terminal_map_path = build_terminal_map(
            groups=groups,
            work_dir=work_dir,
            artifacts_dir=topology_cache_dir,
            max_level=plan.max_level,
            waterline=plan.terminal_waterline,
            batch_size=plan.batch_size,
        )
        terminal_map = TerminalMap(cached_terminal_map_path)
    topology_identity = _topology_identity(plan, cached_terminal_map_path)

    active_group_keys = {group.key for group in groups}
    completed_groups = {
        str(key): value
        for key, value in state.get("completed_groups", {}).items()
        if str(key) in active_group_keys
    }
    state["completed_groups"] = completed_groups
    for group in groups:
        group_identity = _group_materialization_identity(
            group,
            plan=plan,
            topology_identity=topology_identity,
        )
        existing = completed_groups.get(group.key)
        if _completed_group_is_valid(
            work_dir,
            existing,
            expected_identity=group_identity,
        ):
            continue
        group_result = _normalize_group(
            group,
            runs_dir=runs_dir,
            plan=plan,
            terminal_map=terminal_map,
            group_identity=group_identity,
        )
        group_result["identity"] = group_identity
        completed_groups[group.key] = group_result
        _atomic_write_json(work_dir / CLASSIC_WORK_STATE_NAME, state)

    runs_by_partition: dict[str, list[_RunInfo]] = {}
    contributors_by_partition: dict[str, list[dict[str, Any]]] = {}
    folded_row_count = 0
    row_count = 0
    for group in groups:
        result = completed_groups[group.key]
        row_count += int(result["row_count"])
        folded_row_count += int(result["folded_row_count"])
        for raw_run in result.get("runs", []):
            run = _run_from_state(work_dir, raw_run)
            partition_key = _shard_state_key(run.shard)
            runs_by_partition.setdefault(partition_key, []).append(run)
            contributors_by_partition.setdefault(partition_key, []).append(
                {
                    "group_key": group.key,
                    "group_identity": result["identity"],
                    "row_count": run.row_count,
                }
            )

    expected_rows = sum(group.row_count for group in groups)
    if row_count != expected_rows:
        raise ValueError(
            "Classic normalized run row count mismatch: "
            f"expected={expected_rows}, actual={row_count}"
        )

    completed_partitions = {
        str(key): value
        for key, value in state.get("completed_partitions", {}).items()
        if str(key) in runs_by_partition
    }
    state["completed_partitions"] = completed_partitions
    for partition_key in sorted(runs_by_partition, key=_shard_state_sort_key):
        runs = runs_by_partition[partition_key]
        shard = runs[0].shard
        partition_identity = _partition_materialization_identity(
            partition_key,
            contributors=contributors_by_partition[partition_key],
            topology_identity=topology_identity,
            plan=plan,
        )
        existing = completed_partitions.get(partition_key)
        if _completed_partition_is_valid(
            work_dir,
            existing,
            expected_identity=partition_identity,
        ):
            continue
        cache_dir = (
            partition_cache_dir
            / _safe_partition_dir_name(partition_key)
            / partition_identity.removeprefix("sha256:")
        )
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        cache_dir.mkdir(parents=True)
        result = _materialize_partition(
            runs,
            shard=shard,
            merge_root=merge_dir,
            artifacts_dir=cache_dir,
            plan=plan,
        )
        result["identity"] = partition_identity
        result["cache_dir"] = cache_dir.relative_to(work_dir).as_posix()
        result["contributors"] = contributors_by_partition[partition_key]
        completed_partitions[partition_key] = result
        _atomic_write_json(work_dir / CLASSIC_WORK_STATE_NAME, state)

    published_terminal_map_path = _assemble_publication_artifacts(
        work_dir=work_dir,
        artifacts_dir=artifacts_dir,
        completed_partitions=completed_partitions,
        cached_terminal_map_path=cached_terminal_map_path,
    )

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
        terminal_map_path=published_terminal_map_path,
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
    state["input_identity"] = input_identity
    state["topology_identity"] = topology_identity
    _atomic_write_json(work_dir / CLASSIC_WORK_STATE_NAME, state)
    _prune_inactive_materialization_cache(
        work_dir,
        completed_groups=completed_groups,
        completed_partitions=completed_partitions,
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
        if state.get("format") == CLASSIC_WORK_STATE_FORMAT:
            state["input_identity"] = input_identity
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
    group_identity: str,
) -> dict[str, Any]:
    group_dir = (
        runs_dir
        / _safe_group_dir_name(group.key)
        / group_identity.removeprefix("sha256:")
    )
    if group_dir.exists():
        shutil.rmtree(group_dir)
    group_dir.mkdir(parents=True)
    if group.row_count == 0:
        return {
            "row_count": 0,
            "folded_row_count": 0,
            "runs": [],
            "cache_dir": group_dir.relative_to(runs_dir.parent).as_posix(),
        }
    requires_folding = _group_requires_folding(group, max_level=plan.max_level)
    requires_terminal_reordering = (
        terminal_map is not None and terminal_map.terminal_count > 0
    )
    if not requires_folding and not requires_terminal_reordering:
        result = _stream_ordered_group(
            group,
            group_dir=group_dir,
            plan=plan,
            terminal_map=terminal_map,
        )
    else:
        result = _externally_sort_folded_group(
            group,
            group_dir=group_dir,
            plan=plan,
            terminal_map=terminal_map,
        )
    result["cache_dir"] = group_dir.relative_to(runs_dir.parent).as_posix()
    return result


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
                work_dir=group_dir.parents[2],
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
                work_dir=group_dir.parents[2],
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
    return shared_runs.reduce_sorted_runs(
        paths,
        partition_dir=partition_dir,
        batch_size=batch_size,
        fan_in=fan_in,
        layout=_RUN_LAYOUT,
        bounds=_classic_merge_bounds(),
        write_run=lambda source, output, rows: _write_merged_run(
            source,
            output,
            batch_size=rows,
        ),
    )


def _write_merged_run(
    paths: Sequence[Path],
    output: Path,
    *,
    batch_size: int,
) -> None:
    def merge_batches(
        source: Sequence[Path], rows: int, spill_dir: Path
    ) -> Iterator[tuple[tuple[int, int], pa.Table]]:
        yield from _iter_merged_batches(
            source,
            batch_size=rows,
            spill_dir=spill_dir,
        )

    shared_runs.write_merged_run(
        paths,
        output,
        batch_size=batch_size,
        layout=_RUN_LAYOUT,
        bounds=_classic_merge_bounds(),
        merge_batches=merge_batches,
    )


def _iter_merged_batches(
    paths: Sequence[Path],
    *,
    batch_size: int,
    spill_dir: Path,
) -> Iterator[tuple[tuple[int, int], pa.Table]]:
    def external_sort(
        spill_path: Path,
        *,
        key: tuple[int, int],
        batch_size: int,
        **_ignored: Any,
    ) -> Iterator[tuple[tuple[int, int], pa.Table]]:
        yield from _iter_externally_sorted_overlap(
            spill_path,
            key=key,
            batch_size=batch_size,
        )

    yield from shared_runs.iter_merged_batches(
        paths,
        batch_size=batch_size,
        spill_dir=spill_dir,
        layout=_RUN_LAYOUT,
        bounds=_classic_merge_bounds(),
        external_overlap_sort=external_sort,
    )


def _tag_overlap_chunk(
    chunk: pa.Table,
    *,
    contributor_index: int,
    contributor_row: int,
) -> pa.Table:
    return shared_runs._tag_overlap_chunk(
        chunk,
        contributor_index=contributor_index,
        contributor_row=contributor_row,
        layout=_RUN_LAYOUT,
    )


def _iter_in_memory_sorted_overlap(
    chunks: Sequence[pa.Table],
    *,
    key: tuple[int, int],
    batch_size: int,
) -> Iterator[tuple[tuple[int, int], pa.Table]]:
    yield from shared_runs._iter_in_memory_sorted_overlap(
        chunks,
        key=key,
        batch_size=batch_size,
        layout=_RUN_LAYOUT,
    )


def _iter_externally_sorted_overlap(
    spill_path: Path,
    *,
    key: tuple[int, int],
    batch_size: int,
) -> Iterator[tuple[tuple[int, int], pa.Table]]:
    yield from shared_runs._iter_externally_sorted_overlap(
        spill_path,
        key=key,
        batch_size=batch_size,
        layout=_RUN_LAYOUT,
        bounds=_classic_merge_bounds(),
    )


def _overlap_sort_query(path: Path) -> str:
    return shared_runs._overlap_sort_query(path, layout=_RUN_LAYOUT)


def _classic_merge_bounds() -> shared_runs.RunMergeBounds:
    return shared_runs.RunMergeBounds(
        overlap_in_memory_max_bytes=CLASSIC_OVERLAP_IN_MEMORY_MAX_BYTES,
        external_sort_memory_limit=CLASSIC_OVERLAP_EXTERNAL_SORT_MEMORY_LIMIT,
        write_max_bytes=CLASSIC_MERGE_WRITE_MAX_BYTES,
        write_max_pieces=CLASSIC_MERGE_WRITE_MAX_PIECES,
    )


def _assemble_publication_artifacts(
    *,
    work_dir: Path,
    artifacts_dir: Path,
    completed_partitions: dict[str, Any],
    cached_terminal_map_path: Path | None,
) -> Path | None:
    """Assemble a disposable publication tree from durable cached products."""
    if artifacts_dir.exists():
        shutil.rmtree(artifacts_dir)
    artifacts_dir.mkdir(parents=True)
    for partition_key in sorted(completed_partitions, key=_shard_state_sort_key):
        result = completed_partitions[partition_key]
        cache_dir = work_dir / str(result["cache_dir"])
        for entry_name in ("render_entry", "identifiers_entry"):
            entry = result[entry_name]
            for path_name in ("index_path", "payload_path"):
                relative = Path(str(entry[path_name]))
                _link_or_copy_cached_file(
                    cache_dir / relative,
                    artifacts_dir / relative,
                )

    if cached_terminal_map_path is None:
        return None
    raw = json.loads(cached_terminal_map_path.read_text(encoding="utf-8"))
    for entry in raw.get("levels", []):
        relative = Path(str(entry["path"]))
        _link_or_copy_cached_file(
            cached_terminal_map_path.parent / relative,
            artifacts_dir / relative,
        )
    topology_entries = raw.get("topology_levels")
    if not isinstance(topology_entries, list) or not topology_entries:
        raise ValueError("Cached terminal map is missing logical topology")
    for entry in topology_entries:
        relative = Path(str(entry["path"]))
        _link_or_copy_cached_file(
            cached_terminal_map_path.parent / relative,
            artifacts_dir / relative,
        )
    published_manifest = artifacts_dir / cached_terminal_map_path.name
    _link_or_copy_cached_file(cached_terminal_map_path, published_manifest)
    return published_manifest


def _link_or_copy_cached_file(source: Path, target: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(f"Missing cached materialization file: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source, target)
    except OSError:
        # Hardlinks preserve inode/mtime on the common same-device path. A
        # configured cross-device cache must still publish correctly.
        shutil.copy2(source, target)


def _prune_inactive_materialization_cache(
    work_dir: Path,
    *,
    completed_groups: dict[str, Any],
    completed_partitions: dict[str, Any],
) -> None:
    active_group_dirs = {
        (work_dir / str(result["cache_dir"])).resolve()
        for result in completed_groups.values()
    }
    runs_dir = work_dir / "runs"
    if runs_dir.is_dir():
        for group_root in runs_dir.iterdir():
            if not group_root.is_dir():
                continue
            for identity_dir in group_root.iterdir():
                if (
                    identity_dir.is_dir()
                    and identity_dir.resolve() not in active_group_dirs
                ):
                    shutil.rmtree(identity_dir)
            if not any(group_root.iterdir()):
                group_root.rmdir()

    active_partition_dirs = {
        (work_dir / str(result["cache_dir"])).resolve()
        for result in completed_partitions.values()
    }
    partition_root = work_dir / CLASSIC_PARTITION_CACHE_DIR
    if partition_root.is_dir():
        for shard_root in partition_root.iterdir():
            if not shard_root.is_dir():
                continue
            for identity_dir in shard_root.iterdir():
                if (
                    identity_dir.is_dir()
                    and identity_dir.resolve() not in active_partition_dirs
                ):
                    shutil.rmtree(identity_dir)
            if not any(shard_root.iterdir()):
                shard_root.rmdir()


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


def _completed_group_is_valid(
    work_dir: Path,
    raw: Any,
    *,
    expected_identity: str | None = None,
) -> bool:
    if not isinstance(raw, dict):
        return False
    try:
        if expected_identity is not None and raw.get("identity") != expected_identity:
            return False
        cache_dir = _cached_state_dir(work_dir, raw)
        if not cache_dir.is_dir():
            return False
        expected_rows = int(raw["row_count"])
        state_rows = sum(int(run["row_count"]) for run in raw["runs"])
        if state_rows != expected_rows:
            return False
        actual_rows = 0
        for run in raw["runs"]:
            path = work_dir / str(run["path"])
            if not path.resolve().is_relative_to(cache_dir.resolve()):
                return False
            if not path.is_file() or not pq.read_schema(path).equals(_COMPACT_SCHEMA):
                return False
            actual_rows += pq.read_metadata(path).num_rows
        return actual_rows == expected_rows
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _completed_partition_is_valid(
    work_dir: Path,
    raw: Any,
    *,
    expected_identity: str | None = None,
) -> bool:
    if not isinstance(raw, dict):
        return False
    try:
        if expected_identity is not None and raw.get("identity") != expected_identity:
            return False
        cache_dir = _cached_state_dir(work_dir, raw)
        if not cache_dir.is_dir():
            return False
        validate_shard(
            cache_dir,
            raw["render_entry"],
            expected_magic=INDEX_MAGIC,
        )
        validate_shard(
            cache_dir,
            raw["identifiers_entry"],
            expected_magic=IDENTIFIERS_INDEX_MAGIC,
        )
        return True
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _cached_state_dir(work_dir: Path, raw: dict[str, Any]) -> Path:
    relative = Path(str(raw["cache_dir"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Invalid materialization cache path: {relative}")
    path = work_dir / relative
    if not path.resolve().is_relative_to(work_dir.resolve()):
        raise ValueError(f"Materialization cache escapes work directory: {relative}")
    return path


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


def _safe_partition_dir_name(key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:20]
    return f"partition-{digest}"


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
