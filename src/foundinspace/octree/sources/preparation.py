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

from .routing import (
    PIPELINE_STATE_NAME,
    TREE_MANIFEST_NAME,
    _align_tables_to_union_schema,
    _atomic_write_json,
    _read_json,
    _safe_input_shard_id,
    _tree_identity_values,
)
from .semantic_checksum import (
    SEMANTIC_CHECKSUM_ALGORITHM,
    ArrowIpcChecksum,
    ParquetGroupStats,
    checksum_table,
    parquet_group_stats,
)

PREPARATION_GROUP_CHECKSUM_ALGORITHM = SEMANTIC_CHECKSUM_ALGORITHM
PREPARATION_SORT_KEY = "level,final_node_id,mag_abs,source,source_id"
PREPARATION_SORT_TIE_BREAKER = "remaining_columns_in_schema_order"
TREE_DIR_NAME = "tree"
REPORT_NAME = "preparation-report.json"
PREPARATION_IN_MEMORY_MAX_ROWS = 1_000_000
PREPARATION_IN_MEMORY_MAX_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
PREPARATION_EXTERNAL_SORT_MEMORY_LIMIT = "512MB"
PREPARATION_CHECKPOINT_DIR = ".preparation-checkpoints"


@dataclass(frozen=True, slots=True)
class PreparationConfig:
    routed_dir: Path
    prepared_dir: Path
    limiting_magnitude: float
    bucket_rows: int
    input_mode: str = "pre-routed"
    batch_rows: int = 100_000
    fragment_target_rows: int = 100_000
    force: bool = False

    def validate(self) -> None:
        if not self.routed_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.routed_dir}")
        if self.bucket_rows <= 0:
            raise ValueError("bucket_rows must be > 0")
        if self.batch_rows <= 0:
            raise ValueError("batch_rows must be > 0")
        if self.fragment_target_rows <= 0:
            raise ValueError("fragment_target_rows must be > 0")


@dataclass(frozen=True, slots=True)
class _PreparedSortedGroup:
    files: tuple[Path, ...]
    row_count: int
    checksum: str
    natural_max_level: int | None
    external_sort: bool


def prepare_contributions(config: PreparationConfig) -> Path:
    """Sort and compact Routing groups into deterministic Preparation groups."""
    config.validate()
    manifest_path = config.routed_dir / TREE_MANIFEST_NAME
    state_path = config.routed_dir / PIPELINE_STATE_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing Routing tree manifest: {manifest_path}")
    if not state_path.is_file():
        raise FileNotFoundError(f"Missing Routing state: {state_path}")

    manifest = _read_json(manifest_path)
    state = _read_json(state_path)
    _validate_routing_identity(config, manifest, state)

    if not config.force and _published_preparation_is_current(config, state):
        # Preparation is a shared immutable product. A second profile may
        # validate and reuse it, but must not checkpoint, clean, or rewrite it.
        return config.prepared_dir / REPORT_NAME

    if config.force and config.prepared_dir.exists():
        shutil.rmtree(config.prepared_dir)
    config.prepared_dir.mkdir(parents=True, exist_ok=True)
    _delete_stale_preparation_temp_files(config.prepared_dir)

    products = state["products"]
    routing_groups = {
        group["key"]: group for group in products.get("routed_groups", [])
    }
    baseline_preparation_groups = {
        group["key"]: dict(group) for group in products.get("prepared_groups", [])
    }
    if config.force:
        baseline_preparation_groups = {}
        products["prepared_groups"] = []
    checkpoint_groups, checkpoint_deleted_keys = _load_preparation_checkpoints(
        config.prepared_dir, routing_groups
    )
    existing_preparation_groups = dict(baseline_preparation_groups)
    existing_preparation_groups.update(checkpoint_groups)
    for key in checkpoint_deleted_keys:
        existing_preparation_groups.pop(key, None)
    dirty = state["dirty"]["preparation"]
    if config.force or not existing_preparation_groups:
        dirty["all"] = True
    deleted_keys = sorted(
        str(key)
        for key in dirty["deleted_routed_group_keys"]
        if str(key) not in checkpoint_deleted_keys
    )
    explicit_dirty_keys = {
        str(key)
        for key in dirty["group_keys"]
        if key in routing_groups and str(key) not in checkpoint_groups
    }
    if dirty["all"]:
        explicit_dirty_keys.update(set(routing_groups) - set(checkpoint_groups))
    missing_keys = set(routing_groups) - set(existing_preparation_groups)
    target_keys = sorted(explicit_dirty_keys | missing_keys)

    deleted_count = 0
    for key in deleted_keys:
        old_group = existing_preparation_groups.pop(key, None)
        if old_group is None:
            continue
        _delete_preparation_group_files(config.prepared_dir, old_group)
        _write_preparation_checkpoint(
            config.prepared_dir,
            key=key,
            input_checksum=None,
            group=None,
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
        state.get("builds", {}).get("preparation", {}).get("status") == "in_progress"
        or checkpoint_groups
        or checkpoint_deleted_keys
    )
    _checkpoint_preparation_state(state_path, state, status="in_progress")

    for key in target_keys:
        routing_group = routing_groups[key]
        old_group = baseline_preparation_groups.get(key)
        prepared = _prepare_sorted_group(config, routing_group)
        checksum = prepared.checksum
        new_files = list(prepared.files)
        _publish_group_files(new_files)
        if prepared.external_sort:
            external_sort_groups += 1
        else:
            in_memory_sort_groups += 1

        group_changed = old_group is None or old_group.get("checksum") != checksum
        if group_changed:
            changed += 1
        else:
            unchanged += 1

        existing_preparation_groups[key] = _preparation_group_state(
            routing_group,
            files=[
                path.relative_to(config.prepared_dir).as_posix() for path in new_files
            ],
            row_count=prepared.row_count,
            checksum=checksum,
            natural_max_level=prepared.natural_max_level,
        )
        _write_preparation_checkpoint(
            config.prepared_dir,
            key=key,
            input_checksum=str(routing_group["content_checksum"]),
            group=existing_preparation_groups[key],
        )
        if old_group is not None:
            _delete_preparation_group_files(
                config.prepared_dir,
                old_group,
                keep=set(new_files),
            )
        _delete_untracked_preparation_group_files(
            config,
            routing_group,
            keep=set(new_files),
        )
        processed += 1
        rows_written += prepared.row_count
        files_written += len(new_files)

    remaining_keys = set(routing_groups) - set(existing_preparation_groups)
    if remaining_keys:
        raise ValueError(
            "Preparation checkpoint is incomplete after processing: "
            f"{len(remaining_keys)} group(s) remain"
        )
    dirty["all"] = False
    dirty["group_keys"] = []
    dirty["deleted_routed_group_keys"] = []
    products["prepared_groups"] = _sorted_group_state(existing_preparation_groups)
    _checkpoint_preparation_state(state_path, state, status="complete")
    _delete_untracked_preparation_files(
        config.prepared_dir, products["prepared_groups"]
    )
    shutil.rmtree(
        config.prepared_dir / PREPARATION_CHECKPOINT_DIR,
        ignore_errors=True,
    )
    report = {
        "routed_dir": str(config.routed_dir),
        "prepared_dir": str(config.prepared_dir),
        "tree_dir": str(config.prepared_dir / TREE_DIR_NAME),
        "batch_rows": config.batch_rows,
        "fragment_target_rows": config.fragment_target_rows,
        "sort_key": PREPARATION_SORT_KEY,
        "sort_tie_breaker": PREPARATION_SORT_TIE_BREAKER,
        "group_checksum_algorithm": PREPARATION_GROUP_CHECKSUM_ALGORITHM,
        "force": config.force,
        "resumed": resumed,
        "processed_group_count": processed,
        "changed_group_count": changed,
        "unchanged_group_count": unchanged,
        "deleted_group_count": deleted_count,
        "current_group_count": len(products["prepared_groups"]),
        "output_files_written": files_written,
        "rows_written": rows_written,
        "in_memory_sort_group_count": in_memory_sort_groups,
        "external_sort_group_count": external_sort_groups,
    }
    report_path = config.prepared_dir / REPORT_NAME
    _atomic_write_json(report_path, report)
    return report_path


def _published_preparation_is_current(
    config: PreparationConfig,
    state: dict[str, Any],
) -> bool:
    if not config.prepared_dir.is_dir():
        return False
    report_path = config.prepared_dir / REPORT_NAME
    if not report_path.is_file():
        return False
    build = state.get("builds", {}).get("preparation")
    if not isinstance(build, dict) or build.get("status") != "complete":
        return False
    dirty = state.get("dirty", {}).get("preparation")
    if not isinstance(dirty, dict):
        return False
    if (
        dirty.get("all")
        or dirty.get("group_keys")
        or dirty.get("deleted_routed_group_keys")
    ):
        return False
    groups = state.get("products", {}).get("prepared_groups")
    if not isinstance(groups, list) or not groups:
        return False
    seen: set[Path] = set()
    for group in groups:
        files = group.get("files")
        if not isinstance(files, list) or not files:
            return False
        for raw_path in files:
            relative = Path(str(raw_path))
            if relative.is_absolute() or ".." in relative.parts:
                return False
            path = config.prepared_dir / relative
            if path in seen or not path.is_file():
                return False
            seen.add(path)
    return True


def _validate_routing_identity(
    config: PreparationConfig,
    manifest: dict[str, Any],
    state: dict[str, Any],
) -> None:
    expected = _tree_identity_values(
        limiting_magnitude=config.limiting_magnitude,
        bucket_rows=config.bucket_rows,
        input_mode=config.input_mode,
    )
    existing = manifest.get("tree_identity")
    if existing != expected:
        raise ValueError(
            "Existing Routing tree identity does not match current project config; "
            "rerun route with --force."
        )
    if state.get("tree_identity") != existing:
        raise ValueError("Routing state identity does not match tree manifest")
    routing_build = state.get("builds", {}).get("routing")
    if isinstance(routing_build, dict) and routing_build.get("status") != "complete":
        raise ValueError("Preparation requires a complete Routing checkpoint")


def _prepare_sorted_group(
    config: PreparationConfig,
    group: dict[str, Any],
) -> _PreparedSortedGroup:
    files = _routing_group_files(config.routed_dir, group)
    stats = parquet_group_stats(files)
    _ensure_preparation_group_row_count(group, stats.row_count)
    if _should_sort_in_memory(config, stats):
        sorted_table = _sorted_routing_group_files(files)
        checksum = _preparation_group_checksum(sorted_table)
        output_files = _write_sorted_group(config, group, sorted_table)
        return _PreparedSortedGroup(
            files=tuple(output_files),
            row_count=len(sorted_table),
            checksum=checksum,
            natural_max_level=_natural_max_level(sorted_table),
            external_sort=False,
        )
    return _externally_sort_routing_group(
        config,
        group,
        files=files,
        stats=stats,
    )


def _routing_group_files(
    routed_dir: Path,
    group: dict[str, Any],
) -> list[Path]:
    files = [routed_dir / rel_path for rel_path in sorted(group.get("files", []))]
    if not files:
        return []
    for path in files:
        if not path.is_file():
            raise FileNotFoundError(f"Missing Routing group fragment: {path}")
    return files


def _should_sort_in_memory(
    config: PreparationConfig,
    stats: ParquetGroupStats,
) -> bool:
    row_limit = max(
        config.batch_rows,
        min(config.bucket_rows, PREPARATION_IN_MEMORY_MAX_ROWS),
    )
    return (
        stats.row_count <= row_limit
        and stats.uncompressed_bytes <= PREPARATION_IN_MEMORY_MAX_UNCOMPRESSED_BYTES
    )


def _sorted_routing_group(
    routed_dir: Path,
    group: dict[str, Any],
) -> pa.Table:
    files = _routing_group_files(routed_dir, group)
    return _sorted_routing_group_files(files)


def _sorted_routing_group_files(files: list[Path]) -> pa.Table:
    if not files:
        return pa.table({})
    # ParquetFile reads the physical file schema without inferring Hive partition
    # columns from the octree directory names.
    tables = [pq.ParquetFile(path).read() for path in files]
    table = _align_tables_to_union_schema(tables)
    return _sort_preparation_table(table)


def _sort_preparation_table(table: pa.Table) -> pa.Table:
    if len(table) == 0:
        return table
    final_node_ids = _final_node_id_array(table)
    sort_table = table.append_column(
        "_preparation_final_node_id",
        pa.array(final_node_ids, type=pa.uint64()),
    )
    primary_columns = {
        "level",
        "mag_abs",
        "source",
        "source_id",
    }
    sort_keys = [
        ("level", "ascending"),
        ("_preparation_final_node_id", "ascending"),
        ("mag_abs", "ascending"),
        ("source", "ascending"),
        ("source_id", "ascending"),
    ]
    sort_keys.extend(
        (name, "ascending")
        for name in table.column_names
        if name not in primary_columns
    )
    sorted_with_helper = sort_table.take(
        pc.sort_indices(
            sort_table,
            sort_keys=sort_keys,
            null_placement="at_end",
        )
    )
    return sorted_with_helper.drop(["_preparation_final_node_id"])


def _canonicalize_preparation_arrow_buffers(table: pa.Table) -> pa.Table:
    """Normalize DuckDB validity padding and hidden null values for hashing.

    Arrow IPC includes bytes outside the logical value (validity padding and
    values beneath null bits). An identity take uses Arrow's kernels to rebuild
    those buffers exactly as the in-memory sort does, keeping checksums
    engine-independent while retaining bounded batch memory.
    """
    indices = pa.array(np.arange(len(table), dtype=np.uint64))
    return table.take(indices)


def _ensure_preparation_group_row_count(
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
            "Preparation sorting changed row count for "
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


def _preparation_group_checksum(table: pa.Table) -> str:
    checksum, _row_count = checksum_table(table)
    return checksum


def _externally_sort_routing_group(
    config: PreparationConfig,
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
            config.prepared_dir / f".preparation-sort-{group_digest}-{os.getpid()}.tmp"
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
            con.execute(
                "SET memory_limit = ?", [PREPARATION_EXTERNAL_SORT_MEMORY_LIMIT]
            )
        if PRESERVE_INSERTION_ORDER is None:
            con.execute("SET preserve_insertion_order = false")
        con.execute(_external_sort_query(files, schema=stats.schema))
        reader = con.to_arrow_reader(batch_size=config.batch_rows)
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


def _external_sort_query(files: list[Path], *, schema: pa.Schema) -> str:
    source = _duckdb_read_parquet_source(files)
    primary_columns = {"level", "mag_abs", "source", "source_id"}
    tie_breakers = "".join(
        f",\n            {_quoted_identifier(name)} ASC NULLS LAST"
        for name in schema.names
        if name not in primary_columns
    )
    return f"""
        WITH staged AS (
            SELECT
                *,
                (
                    morton_code
                    >> CAST((3 * ({MORTON_BITS} - level)) AS INTEGER)
                ) AS _preparation_final_node_id
            FROM read_parquet(
                {source},
                hive_partitioning = false,
                union_by_name = false
            )
        )
        SELECT * EXCLUDE (_preparation_final_node_id)
        FROM staged
        ORDER BY
            level ASC NULLS LAST,
            _preparation_final_node_id ASC NULLS LAST,
            mag_abs ASC NULLS LAST,
            source ASC NULLS LAST,
            source_id ASC NULLS LAST{tie_breakers}
    """


def _quoted_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _duckdb_read_parquet_source(files: list[Path]) -> str:
    quoted = []
    for path in files:
        escaped = path.as_posix().replace("'", "''")
        quoted.append(f"'{escaped}'")
    if len(quoted) == 1:
        return quoted[0]
    return "[" + ", ".join(quoted) + "]"


def _write_externally_sorted_batches(
    config: PreparationConfig,
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
            table = _canonicalize_preparation_arrow_buffers(table)
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
                "Preparation external-sort checksum row mismatch: "
                f"sorted={row_count}, checksummed={checksummed_rows}"
            )
        _ensure_preparation_group_row_count(group, row_count)
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
    config: PreparationConfig,
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
    config: PreparationConfig,
    group: dict[str, Any],
    sequence: int,
) -> Path:
    path_octants = tuple(int(v) for v in group["path_octants"])
    directory = config.prepared_dir / TREE_DIR_NAME
    for octant in path_octants:
        directory = directory / f"o={octant}"
    directory.mkdir(parents=True, exist_ok=True)

    shard_id = _safe_input_shard_id(str(group["input_shard_id"]))
    kind = str(group["kind"])
    if kind not in {"pack", "lim"}:
        raise ValueError(f"Unsupported Routing group kind: {kind!r}")
    version = _preparation_group_version(config, group)
    return directory / (
        f"shard-{shard_id}-{kind}-sorted-{version}-{sequence:06d}.parquet"
    )


def _preparation_group_version(config: PreparationConfig, group: dict[str, Any]) -> str:
    identity = json.dumps(
        {
            "fragment_target_rows": config.fragment_target_rows,
            "input_checksum": group.get("content_checksum"),
            "sort_key": PREPARATION_SORT_KEY,
            "sort_tie_breaker": PREPARATION_SORT_TIE_BREAKER,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def _tmp_fragment_path(final_path: Path) -> Path:
    return final_path.with_name(f".{final_path.name}.{os.getpid()}.tmp")


def _publish_group_files(final_paths: list[Path]) -> None:
    for final_path in final_paths:
        tmp_path = _tmp_fragment_path(final_path)
        if final_path.exists():
            tmp_path.unlink(missing_ok=True)
        else:
            os.replace(tmp_path, final_path)


def _delete_preparation_group_files(
    prepared_dir: Path,
    group: dict[str, Any],
    *,
    keep: set[Path] | None = None,
) -> None:
    keep = keep or set()
    for rel_file in group.get("files", []):
        path = prepared_dir / rel_file
        if path not in keep and path.exists():
            path.unlink()


def _preparation_group_state(
    routing_group: dict[str, Any],
    *,
    files: list[str],
    row_count: int,
    checksum: str,
    natural_max_level: int | None,
) -> dict[str, Any]:
    return {
        "key": routing_group["key"],
        "node_path": routing_group["node_path"],
        "path_octants": list(routing_group["path_octants"]),
        "depth": routing_group["depth"],
        "shard_id": routing_group["input_shard_id"],
        "input_shard_id": routing_group["input_shard_id"],
        "kind": routing_group["kind"],
        "files": files,
        "file_count": len(files),
        "row_count": row_count,
        "checksum": checksum,
        "sorted_checksum": checksum,
        "natural_max_level": natural_max_level,
    }


def _sorted_group_state(groups: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [groups[key] for key in sorted(groups)]


def _checkpoint_preparation_state(
    state_path: Path,
    state: dict[str, Any],
    *,
    status: str,
) -> None:
    state["builds"]["preparation"] = {"status": status}
    _atomic_write_json(state_path, state)


def _preparation_checkpoint_path(prepared_dir: Path, key: str) -> Path:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return prepared_dir / PREPARATION_CHECKPOINT_DIR / f"{digest}.json"


def _write_preparation_checkpoint(
    prepared_dir: Path,
    *,
    key: str,
    input_checksum: str | None,
    group: dict[str, Any] | None,
) -> None:
    _atomic_write_json(
        _preparation_checkpoint_path(prepared_dir, key),
        {
            "key": key,
            "input_checksum": input_checksum,
            "deleted": group is None,
            "group": group,
        },
    )


def _load_preparation_checkpoints(
    prepared_dir: Path,
    routing_groups: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    checkpoint_dir = prepared_dir / PREPARATION_CHECKPOINT_DIR
    groups: dict[str, dict[str, Any]] = {}
    deleted_keys: set[str] = set()
    if not checkpoint_dir.is_dir():
        return groups, deleted_keys
    for path in sorted(checkpoint_dir.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            key = str(raw["key"])
            if bool(raw.get("deleted", False)):
                if key in routing_groups:
                    raise ValueError("deleted checkpoint has a current Routing group")
                deleted_keys.add(key)
            else:
                routing_group = routing_groups.get(key)
                if routing_group is None:
                    raise ValueError("checkpoint input group no longer exists")
                if raw.get("input_checksum") != routing_group.get("content_checksum"):
                    raise ValueError("checkpoint input checksum changed")
                group = raw.get("group")
                if not isinstance(group, dict):
                    raise ValueError("checkpoint group is missing")
                for rel_path in group.get("files", []):
                    if not (prepared_dir / str(rel_path)).is_file():
                        raise ValueError("checkpoint output file is missing")
                groups[key] = group
        except (KeyError, OSError, TypeError, ValueError):
            path.unlink(missing_ok=True)
    return groups, deleted_keys


def _delete_stale_preparation_temp_files(prepared_dir: Path) -> None:
    for path in prepared_dir.rglob(".*.tmp"):
        if path.is_file():
            path.unlink()
    for path in prepared_dir.glob(".preparation-sort-*.tmp"):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)


def _delete_untracked_preparation_group_files(
    config: PreparationConfig,
    group: dict[str, Any],
    *,
    keep: set[Path],
) -> None:
    first = _sorted_fragment_path(config, group, 1)
    prefix = first.name.rsplit("000001.parquet", 1)[0]
    for path in first.parent.glob(f"{prefix}*.parquet"):
        if path not in keep:
            path.unlink()


def _delete_untracked_preparation_files(
    prepared_dir: Path,
    groups: list[dict[str, Any]],
) -> None:
    referenced = {
        prepared_dir / str(rel_path)
        for group in groups
        for rel_path in group.get("files", [])
    }
    tree_dir = prepared_dir / TREE_DIR_NAME
    if not tree_dir.exists():
        return
    for path in tree_dir.rglob("*-sorted-*.parquet"):
        if path not in referenced:
            path.unlink()
