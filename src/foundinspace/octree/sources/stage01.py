from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
from collections.abc import Iterable
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.duckdb_util import (
    MEMORY_LIMIT,
    PRESERVE_INSERTION_ORDER,
    TEMP_DIR,
    configure_connection,
)

from .semantic_checksum import (
    SEMANTIC_CHECKSUM_ALGORITHM,
    ArrowIpcChecksum,
    ParquetGroupStats,
    checksum_table,
    parquet_group_stats,
)
from .stage00 import (
    STAGE_STATE_FORMAT,
    STAGE_STATE_NAME,
    TREE_MANIFEST_FORMAT,
    TREE_MANIFEST_NAME,
    _align_tables_to_union_schema,
    _atomic_write_json,
    _read_json,
    _safe_input_shard_id,
    _tree_identity_values,
)

STAGE01_FORMAT = "foundinspace.octree.stage01/v0"
STAGE01_GROUP_CHECKSUM_ALGORITHM = SEMANTIC_CHECKSUM_ALGORITHM
STAGE01_SORT_KEY = "level,final_node_id,mag_abs,source,source_id"
TREE_DIR_NAME = "tree"
REPORT_NAME = "stage01-report.json"
STAGE01_IN_MEMORY_MAX_ROWS = 1_000_000
STAGE01_IN_MEMORY_MAX_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
STAGE01_EXTERNAL_SORT_MEMORY_LIMIT = "512MB"
STAGE01_BUILD_FORMAT = "foundinspace.octree.stage01-build/v1"
STAGE01_CHECKPOINT_FORMAT = "foundinspace.octree.stage01-checkpoint/v1"
STAGE01_CHECKPOINT_DIR = ".stage01-checkpoints"


@dataclass(frozen=True, slots=True)
class Stage01Config:
    stage00_output_dir: Path
    output_dir: Path
    v_mag: float
    bucket_size: int
    input_filter: str = "none"
    batch_size: int = 100_000
    fragment_target_rows: int = 100_000
    force: bool = False

    def validate(self) -> None:
        if not self.stage00_output_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.stage00_output_dir}")
        if self.bucket_size <= 0:
            raise ValueError("bucket_size must be > 0")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if self.fragment_target_rows <= 0:
            raise ValueError("fragment_target_rows must be > 0")


@dataclass(frozen=True, slots=True)
class _PreparedSortedGroup:
    files: tuple[Path, ...]
    row_count: int
    checksum: str
    natural_max_level: int | None
    external_sort: bool


def run_stage01(config: Stage01Config) -> Path:
    """Sort and compact Stage 00 groups into deterministic Stage 01 groups."""
    config.validate()
    manifest_path = config.stage00_output_dir / TREE_MANIFEST_NAME
    state_path = config.stage00_output_dir / STAGE_STATE_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing Stage 00 tree manifest: {manifest_path}")
    if not state_path.is_file():
        raise FileNotFoundError(f"Missing Stage 00 state: {state_path}")

    manifest = _read_json(manifest_path)
    state = _read_json(state_path)
    _validate_stage00_identity(config, manifest, state)

    if config.force and config.output_dir.exists():
        shutil.rmtree(config.output_dir)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    _delete_stale_stage01_temp_files(config.output_dir)

    stage00_groups = {group["key"]: group for group in state.get("stage00_groups", [])}
    baseline_stage01_groups = {
        group["key"]: _bounded_stage01_group_state(group)
        for group in state.get("stage01_groups", [])
    }
    if config.force:
        baseline_stage01_groups = {}
        state["stage01_groups"] = []
    (
        checkpoint_groups,
        checkpoint_deleted_keys,
        checkpoint_stage03_dirty,
    ) = _load_stage01_checkpoints(config.output_dir, stage00_groups)
    existing_stage01_groups = dict(baseline_stage01_groups)
    existing_stage01_groups.update(checkpoint_groups)
    for key in checkpoint_deleted_keys:
        existing_stage01_groups.pop(key, None)
    dirty = _normalized_dirty_state(state)
    if checkpoint_stage03_dirty:
        _mark_stage03_all(dirty, reason="resumed_stage01_checkpoint")
    if config.force or not existing_stage01_groups:
        dirty["stage01_all"] = True
    deleted_keys = sorted(
        str(key)
        for key in dirty["deleted_stage00_groups"]
        if str(key) not in checkpoint_deleted_keys
    )
    explicit_dirty_keys = {
        str(key)
        for key in dirty["stage01_groups"]
        if key in stage00_groups and str(key) not in checkpoint_groups
    }
    if dirty["stage01_all"]:
        explicit_dirty_keys.update(set(stage00_groups) - set(checkpoint_groups))
    missing_keys = set(stage00_groups) - set(existing_stage01_groups)
    target_keys = sorted(explicit_dirty_keys | missing_keys)

    deleted_count = 0
    for key in deleted_keys:
        old_group = existing_stage01_groups.pop(key, None)
        if old_group is None:
            continue
        _delete_stage01_group_files(config.output_dir, old_group)
        _mark_stage03_all(dirty, reason="stage01_group_deleted")
        _write_stage01_checkpoint(
            config.output_dir,
            key=key,
            input_checksum=None,
            group=None,
            changed=True,
        )
        checkpoint_deleted_keys.add(key)
        deleted_count += 1

    processed = 0
    changed = 0
    unchanged = 0
    rows_written = 0
    files_written = 0
    in_memory_sort_groups = 0
    external_sort_groups = 0
    resumed = bool(
        state.get("stage01_build", {}).get("status") == "in_progress"
        or checkpoint_groups
        or checkpoint_deleted_keys
    )
    _checkpoint_stage01_state(state_path, state, status="in_progress")

    for key in target_keys:
        stage00_group = stage00_groups[key]
        old_group = baseline_stage01_groups.get(key)
        prepared = _prepare_sorted_group(config, stage00_group)
        checksum = prepared.checksum
        new_files = list(prepared.files)
        if old_group is not None:
            _delete_stage01_group_files(config.output_dir, old_group)
        _publish_group_files(new_files)
        if prepared.external_sort:
            external_sort_groups += 1
        else:
            in_memory_sort_groups += 1

        group_changed = old_group is None or old_group.get("checksum") != checksum
        if group_changed:
            changed += 1
            _mark_stage03_all(dirty, reason="stage01_group_changed")
        else:
            unchanged += 1

        existing_stage01_groups[key] = _stage01_group_state(
            stage00_group,
            files=[
                path.relative_to(config.output_dir).as_posix() for path in new_files
            ],
            row_count=prepared.row_count,
            checksum=checksum,
            natural_max_level=prepared.natural_max_level,
        )
        _write_stage01_checkpoint(
            config.output_dir,
            key=key,
            input_checksum=str(stage00_group["content_checksum"]),
            group=existing_stage01_groups[key],
            changed=group_changed,
        )
        _delete_untracked_stage01_group_files(
            config,
            stage00_group,
            keep=set(new_files),
        )
        processed += 1
        rows_written += prepared.row_count
        files_written += len(new_files)

    remaining_keys = set(stage00_groups) - set(existing_stage01_groups)
    if remaining_keys:
        raise ValueError(
            "Stage 01 checkpoint is incomplete after processing: "
            f"{len(remaining_keys)} group(s) remain"
        )
    dirty["stage01_all"] = False
    dirty["stage01_groups"] = []
    dirty["deleted_stage00_groups"] = []
    state["stage01_groups"] = _sorted_group_state(existing_stage01_groups)
    _checkpoint_stage01_state(state_path, state, status="complete")
    _delete_untracked_stage01_files(config.output_dir, state["stage01_groups"])
    shutil.rmtree(
        config.output_dir / STAGE01_CHECKPOINT_DIR,
        ignore_errors=True,
    )
    dirty_stage03_mode = str(dirty["stage03"]["mode"])

    report = {
        "format": STAGE01_FORMAT,
        "stage00_output_dir": str(config.stage00_output_dir),
        "output_dir": str(config.output_dir),
        "tree_dir": str(config.output_dir / TREE_DIR_NAME),
        "batch_size": config.batch_size,
        "fragment_target_rows": config.fragment_target_rows,
        "sort_key": STAGE01_SORT_KEY,
        "group_checksum_algorithm": STAGE01_GROUP_CHECKSUM_ALGORITHM,
        "force": config.force,
        "resumed": resumed,
        "processed_group_count": processed,
        "changed_group_count": changed,
        "unchanged_group_count": unchanged,
        "deleted_group_count": deleted_count,
        "current_group_count": len(state["stage01_groups"]),
        "output_files_written": files_written,
        "rows_written": rows_written,
        "in_memory_sort_group_count": in_memory_sort_groups,
        "external_sort_group_count": external_sort_groups,
        "dirty_stage03_mode": dirty_stage03_mode,
        "dirty_stage03_node_count": 0,
    }
    report_path = config.output_dir / REPORT_NAME
    _atomic_write_json(report_path, report)
    return report_path


def _validate_stage00_identity(
    config: Stage01Config,
    manifest: dict[str, Any],
    state: dict[str, Any],
) -> None:
    if manifest.get("format") != TREE_MANIFEST_FORMAT:
        raise ValueError(
            f"Unsupported Stage 00 tree manifest format: {manifest.get('format')!r}"
        )
    if state.get("format") != STAGE_STATE_FORMAT:
        raise ValueError(f"Unsupported Stage 00 state format: {state.get('format')!r}")
    expected = _tree_identity_values(
        v_mag=config.v_mag,
        bucket_size=config.bucket_size,
        input_filter=config.input_filter,
    )
    existing = manifest.get("tree_identity")
    if existing != expected:
        raise ValueError(
            "Existing Stage 00 tree identity does not match current project config"
        )
    if state.get("tree_identity") != existing:
        raise ValueError("Stage 00 state identity does not match tree manifest")
    stage00_build = state.get("stage00_build")
    if isinstance(stage00_build, dict) and stage00_build.get("status") != "complete":
        raise ValueError("Stage 01 requires a complete Stage 00 checkpoint")


def _prepare_sorted_group(
    config: Stage01Config,
    group: dict[str, Any],
) -> _PreparedSortedGroup:
    files = _stage00_group_files(config.stage00_output_dir, group)
    stats = parquet_group_stats(files)
    _ensure_stage01_group_row_count(group, stats.row_count)
    if _should_sort_in_memory(config, stats):
        sorted_table = _sorted_stage00_group_files(files)
        checksum = _stage01_group_checksum(sorted_table)
        output_files = _write_sorted_group(config, group, sorted_table)
        return _PreparedSortedGroup(
            files=tuple(output_files),
            row_count=len(sorted_table),
            checksum=checksum,
            natural_max_level=_natural_max_level(sorted_table),
            external_sort=False,
        )
    return _externally_sort_stage00_group(
        config,
        group,
        files=files,
        stats=stats,
    )


def _stage00_group_files(
    stage00_output_dir: Path,
    group: dict[str, Any],
) -> list[Path]:
    files = [stage00_output_dir / rel_path for rel_path in group.get("files", [])]
    if not files:
        return []
    for path in files:
        if not path.is_file():
            raise FileNotFoundError(f"Missing Stage 00 group fragment: {path}")
    return files


def _should_sort_in_memory(
    config: Stage01Config,
    stats: ParquetGroupStats,
) -> bool:
    row_limit = max(
        config.batch_size,
        min(config.bucket_size, STAGE01_IN_MEMORY_MAX_ROWS),
    )
    return (
        stats.row_count <= row_limit
        and stats.uncompressed_bytes <= STAGE01_IN_MEMORY_MAX_UNCOMPRESSED_BYTES
    )


def _sorted_stage00_group(
    stage00_output_dir: Path,
    group: dict[str, Any],
) -> pa.Table:
    files = _stage00_group_files(stage00_output_dir, group)
    return _sorted_stage00_group_files(files)


def _sorted_stage00_group_files(files: list[Path]) -> pa.Table:
    if not files:
        return pa.table({})
    tables = [pq.read_table(path) for path in files]
    table = _align_tables_to_union_schema(tables)
    if len(table) == 0:
        return table
    final_node_ids = _final_node_id_array(table)
    sort_table = table.append_column(
        "_stage01_final_node_id",
        pa.array(final_node_ids, type=pa.uint64()),
    )
    sorted_with_helper = sort_table.take(
        pc.sort_indices(
            sort_table,
            sort_keys=[
                ("level", "ascending"),
                ("_stage01_final_node_id", "ascending"),
                ("mag_abs", "ascending"),
                ("source", "ascending"),
                ("source_id", "ascending"),
            ],
            null_placement="at_end",
        )
    )
    return sorted_with_helper.drop(["_stage01_final_node_id"])


def _ensure_stage01_group_row_count(
    group: dict[str, Any],
    table_or_row_count: pa.Table | int,
) -> None:
    expected = int(group.get("row_count", 0))
    actual = (
        len(table_or_row_count)
        if isinstance(table_or_row_count, pa.Table)
        else table_or_row_count
    )
    if actual != expected:
        raise ValueError(
            "Stage 01 sorting changed row count for "
            f"{group.get('key', '<unknown>')}: before={expected}, after={actual}"
        )


def _final_node_id_array(table: pa.Table) -> np.ndarray:
    levels = np.asarray(table.column("level"), dtype=np.int32)
    morton_codes = np.asarray(table.column("morton_code"), dtype=np.uint64)
    out = np.zeros(len(table), dtype=np.uint64)
    for level in sorted(int(v) for v in np.unique(levels)):
        if level < 0 or level > MORTON_BITS:
            raise ValueError(f"Invalid row level {level}; expected 0..{MORTON_BITS}")
        indices = np.flatnonzero(levels == level)
        shift = 3 * (MORTON_BITS - level)
        out[indices] = morton_codes[indices] >> np.uint64(shift)
    return out


def _natural_max_level(table: pa.Table) -> int | None:
    if len(table) == 0:
        return None
    levels = np.asarray(table.column("level"), dtype=np.int32)
    return int(levels.max())


def _stage01_group_checksum(table: pa.Table) -> str:
    checksum, _row_count = checksum_table(table)
    return checksum


def _externally_sort_stage00_group(
    config: Stage01Config,
    group: dict[str, Any],
    *,
    files: list[Path],
    stats: ParquetGroupStats,
) -> _PreparedSortedGroup:
    local_spill_dir: Path | None = None
    if TEMP_DIR is None:
        group_digest = hashlib.sha256(str(group["key"]).encode("utf-8")).hexdigest()[
            :16
        ]
        local_spill_dir = (
            config.output_dir / f".stage01-sort-{group_digest}-{os.getpid()}.tmp"
        )
        if local_spill_dir.exists():
            shutil.rmtree(local_spill_dir)
        local_spill_dir.mkdir(parents=True)

    con = duckdb.connect()
    try:
        with redirect_stdout(io.StringIO()):
            configure_connection(con)
        if local_spill_dir is not None:
            con.execute("SET temp_directory = ?", [str(local_spill_dir)])
        if MEMORY_LIMIT is None:
            con.execute("SET memory_limit = ?", [STAGE01_EXTERNAL_SORT_MEMORY_LIMIT])
        if PRESERVE_INSERTION_ORDER is None:
            con.execute("SET preserve_insertion_order = false")
        con.execute(_external_sort_query(files))
        reader = con.to_arrow_reader(batch_size=config.batch_size)
        return _write_externally_sorted_batches(
            config,
            group,
            batches=reader,
            expected_schema=stats.schema,
        )
    finally:
        con.close()
        if local_spill_dir is not None:
            shutil.rmtree(local_spill_dir, ignore_errors=True)


def _external_sort_query(files: list[Path]) -> str:
    source = _duckdb_read_parquet_source(files)
    return f"""
        WITH staged AS (
            SELECT
                *,
                (
                    morton_code
                    >> CAST((3 * ({MORTON_BITS} - level)) AS INTEGER)
                ) AS _stage01_final_node_id
            FROM read_parquet(
                {source},
                hive_partitioning = false,
                union_by_name = false
            )
        )
        SELECT * EXCLUDE (_stage01_final_node_id)
        FROM staged
        ORDER BY
            level ASC NULLS LAST,
            _stage01_final_node_id ASC NULLS LAST,
            mag_abs ASC NULLS LAST,
            source ASC NULLS LAST,
            source_id ASC NULLS LAST
    """


def _duckdb_read_parquet_source(files: list[Path]) -> str:
    quoted = []
    for path in files:
        escaped = path.as_posix().replace("'", "''")
        quoted.append(f"'{escaped}'")
    if len(quoted) == 1:
        return quoted[0]
    return "[" + ", ".join(quoted) + "]"


def _write_externally_sorted_batches(
    config: Stage01Config,
    group: dict[str, Any],
    *,
    batches: Iterable[pa.RecordBatch],
    expected_schema: pa.Schema,
) -> _PreparedSortedGroup:
    output_files: list[Path] = []
    natural_max_level: int | None = None
    checksum = ArrowIpcChecksum(expected_schema)
    writer: pq.ParquetWriter | None = None
    writer_rows = 0
    row_count = 0
    sequence = 1

    def close_writer() -> None:
        nonlocal writer, writer_rows
        if writer is not None:
            writer.close()
            writer = None
            writer_rows = 0

    try:
        for batch in batches:
            table = pa.Table.from_batches([batch])
            if not table.schema.equals(expected_schema, check_metadata=False):
                table = table.cast(expected_schema)
            table = table.replace_schema_metadata(None)
            checksum.update(table)
            batch_max_level = _natural_max_level(table)
            if batch_max_level is not None:
                natural_max_level = (
                    batch_max_level
                    if natural_max_level is None
                    else max(natural_max_level, batch_max_level)
                )
            row_count += len(table)
            offset = 0
            while offset < len(table):
                if writer is None:
                    final_path = _sorted_fragment_path(config, group, sequence)
                    tmp_path = _tmp_fragment_path(final_path)
                    tmp_path.unlink(missing_ok=True)
                    writer = pq.ParquetWriter(
                        tmp_path,
                        expected_schema,
                        compression="zstd",
                    )
                    output_files.append(final_path)
                    sequence += 1
                take = min(
                    config.fragment_target_rows - writer_rows,
                    len(table) - offset,
                )
                writer.write_table(table.slice(offset, take))
                writer_rows += take
                offset += take
                if writer_rows == config.fragment_target_rows:
                    close_writer()
        close_writer()
        digest, checksummed_rows = checksum.finish()
        if checksummed_rows != row_count:
            raise ValueError(
                "Stage 01 external-sort checksum row mismatch: "
                f"sorted={row_count}, checksummed={checksummed_rows}"
            )
        _ensure_stage01_group_row_count(group, row_count)
        return _PreparedSortedGroup(
            files=tuple(output_files),
            row_count=row_count,
            checksum=digest,
            natural_max_level=natural_max_level,
            external_sort=True,
        )
    except Exception:
        close_writer()
        for final_path in output_files:
            _tmp_fragment_path(final_path).unlink(missing_ok=True)
        raise


def _write_sorted_group(
    config: Stage01Config,
    group: dict[str, Any],
    table: pa.Table,
) -> list[Path]:
    paths: list[Path] = []
    offset = 0
    sequence = 1
    while offset < len(table) or (len(table) == 0 and sequence == 1):
        rows = min(config.fragment_target_rows, len(table) - offset)
        if rows <= 0 and len(table) > 0:
            break
        final_path = _sorted_fragment_path(config, group, sequence)
        tmp_path = _tmp_fragment_path(final_path)
        pq.write_table(table.slice(offset, rows), tmp_path, compression="zstd")
        paths.append(final_path)
        offset += rows
        sequence += 1
        if len(table) == 0:
            break
    return paths


def _sorted_fragment_path(
    config: Stage01Config,
    group: dict[str, Any],
    sequence: int,
) -> Path:
    path_octants = tuple(int(v) for v in group["path_octants"])
    directory = config.output_dir / TREE_DIR_NAME
    for octant in path_octants:
        directory = directory / f"o={octant}"
    directory.mkdir(parents=True, exist_ok=True)

    shard_id = _safe_input_shard_id(str(group["input_shard_id"]))
    kind = str(group["kind"])
    if kind not in {"pack", "lim"}:
        raise ValueError(f"Unsupported Stage 00 group kind: {kind!r}")
    return directory / f"shard-{shard_id}-{kind}-sorted-{sequence:06d}.parquet"


def _tmp_fragment_path(final_path: Path) -> Path:
    return final_path.with_name(f".{final_path.name}.{os.getpid()}.tmp")


def _publish_group_files(final_paths: list[Path]) -> None:
    for final_path in final_paths:
        tmp_path = _tmp_fragment_path(final_path)
        os.replace(tmp_path, final_path)


def _delete_stage01_group_files(output_dir: Path, group: dict[str, Any]) -> None:
    for rel_file in group.get("files", []):
        path = output_dir / rel_file
        if path.exists():
            path.unlink()


def _stage01_group_state(
    stage00_group: dict[str, Any],
    *,
    files: list[str],
    row_count: int,
    checksum: str,
    natural_max_level: int | None,
) -> dict[str, Any]:
    return {
        "key": stage00_group["key"],
        "node_path": stage00_group["node_path"],
        "path_octants": list(stage00_group["path_octants"]),
        "depth": stage00_group["depth"],
        "shard_id": stage00_group["input_shard_id"],
        "input_shard_id": stage00_group["input_shard_id"],
        "kind": stage00_group["kind"],
        "files": files,
        "file_count": len(files),
        "row_count": row_count,
        "checksum": checksum,
        "sorted_checksum": checksum,
        "natural_max_level": natural_max_level,
    }


def _bounded_stage01_group_state(group: dict[str, Any]) -> dict[str, Any]:
    bounded = dict(group)
    legacy_nodes = bounded.pop("final_nodes", [])
    if bounded.get("natural_max_level") is None and legacy_nodes:
        bounded["natural_max_level"] = max(
            int(str(node).split(":", 1)[0]) for node in legacy_nodes
        )
    return bounded


def _normalized_dirty_state(state: dict[str, Any]) -> dict[str, Any]:
    raw = state.setdefault("dirty", {})
    legacy_nodes = raw.pop("stage03_nodes", [])
    stage03 = raw.get("stage03")
    if not isinstance(stage03, dict) or stage03.get("mode") not in {"clean", "all"}:
        stage03 = {"mode": "all" if legacy_nodes else "clean"}
    elif legacy_nodes:
        stage03 = {"mode": "all", "reason": "legacy_stage03_nodes"}
    raw["stage03"] = stage03
    raw["stage01_groups"] = list(raw.get("stage01_groups", []))
    raw["deleted_stage00_groups"] = list(raw.get("deleted_stage00_groups", []))
    raw["stage01_all"] = bool(raw.get("stage01_all", False))
    return raw


def _mark_stage03_all(dirty: dict[str, Any], *, reason: str) -> None:
    dirty["stage03"] = {"mode": "all", "reason": reason}


def _sorted_group_state(groups: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [groups[key] for key in sorted(groups)]


def _checkpoint_stage01_state(
    state_path: Path,
    state: dict[str, Any],
    *,
    status: str,
) -> None:
    state["stage01_build"] = {
        "format": STAGE01_BUILD_FORMAT,
        "status": status,
    }
    _atomic_write_json(state_path, state)


def _stage01_checkpoint_path(output_dir: Path, key: str) -> Path:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return output_dir / STAGE01_CHECKPOINT_DIR / f"{digest}.json"


def _write_stage01_checkpoint(
    output_dir: Path,
    *,
    key: str,
    input_checksum: str | None,
    group: dict[str, Any] | None,
    changed: bool,
) -> None:
    _atomic_write_json(
        _stage01_checkpoint_path(output_dir, key),
        {
            "format": STAGE01_CHECKPOINT_FORMAT,
            "key": key,
            "input_checksum": input_checksum,
            "deleted": group is None,
            "changed": changed,
            "group": group,
        },
    )


def _load_stage01_checkpoints(
    output_dir: Path,
    stage00_groups: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], set[str], bool]:
    checkpoint_dir = output_dir / STAGE01_CHECKPOINT_DIR
    groups: dict[str, dict[str, Any]] = {}
    deleted_keys: set[str] = set()
    stage03_dirty = False
    if not checkpoint_dir.is_dir():
        return groups, deleted_keys, stage03_dirty
    for path in sorted(checkpoint_dir.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if raw.get("format") != STAGE01_CHECKPOINT_FORMAT:
                raise ValueError("unsupported checkpoint format")
            key = str(raw["key"])
            if bool(raw.get("deleted", False)):
                if key in stage00_groups:
                    raise ValueError("deleted checkpoint has a current Stage 00 group")
                deleted_keys.add(key)
            else:
                stage00_group = stage00_groups.get(key)
                if stage00_group is None:
                    raise ValueError("checkpoint input group no longer exists")
                if raw.get("input_checksum") != stage00_group.get("content_checksum"):
                    raise ValueError("checkpoint input checksum changed")
                group = raw.get("group")
                if not isinstance(group, dict):
                    raise ValueError("checkpoint group is missing")
                for rel_path in group.get("files", []):
                    if not (output_dir / str(rel_path)).is_file():
                        raise ValueError("checkpoint output file is missing")
                groups[key] = group
            stage03_dirty = stage03_dirty or bool(raw.get("changed", False))
        except (KeyError, OSError, TypeError, ValueError):
            path.unlink(missing_ok=True)
    return groups, deleted_keys, stage03_dirty


def _delete_stale_stage01_temp_files(output_dir: Path) -> None:
    for path in output_dir.rglob(".*.tmp"):
        if path.is_file():
            path.unlink()


def _delete_untracked_stage01_group_files(
    config: Stage01Config,
    group: dict[str, Any],
    *,
    keep: set[Path],
) -> None:
    first = _sorted_fragment_path(config, group, 1)
    prefix = first.name.rsplit("000001.parquet", 1)[0]
    for path in first.parent.glob(f"{prefix}*.parquet"):
        if path not in keep:
            path.unlink()


def _delete_untracked_stage01_files(
    output_dir: Path,
    groups: list[dict[str, Any]],
) -> None:
    referenced = {
        output_dir / str(rel_path)
        for group in groups
        for rel_path in group.get("files", [])
    }
    tree_dir = output_dir / TREE_DIR_NAME
    if not tree_dir.exists():
        return
    for path in tree_dir.rglob("*-sorted-*.parquet"):
        if path not in referenced:
            path.unlink()
