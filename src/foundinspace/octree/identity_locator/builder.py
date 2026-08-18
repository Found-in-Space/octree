"""Bounded, restartable builder for exact Gaia/HIP identity locators."""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import shutil
import struct
import time
from collections.abc import Callable, Sequence
from contextlib import redirect_stdout
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from uuid import UUID

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..assembly.identity_encoder import decode_identity_rows
from ..duckdb_util import (
    configure_connection,
)
from ..identifiers_order import (
    DIRECTORY_RECORD_SIZE,
    IDENTITY_UNCOMPRESSED_CELL_LIMIT_BYTES,
    IdentifiersOrderReader,
    IdentifiersOrderRecord,
)
from ..identifiers_order import (
    HEADER_SIZE as IDENTIFIERS_HEADER_SIZE,
)
from ..reader import read_header as read_octree_header
from .format import (
    CHILD_RECORD_FMT,
    CODEC_CODES,
    CODEC_NONE,
    FOOTER_SIZE,
    GZIP_COMPRESSLEVEL,
    HEADER_SIZE,
    KEY_CODEC_U64_DECIMAL,
    LEAF_RECORD_FMT,
    NAMESPACE_SIZE,
    PAGE_HEADER_SIZE,
    PAGE_KIND_INTERNAL,
    PAGE_KIND_LEAF,
    VALUE_CODEC_CELL_U32_ORDINAL_U32,
    IdentityLocatorHeader,
    NamespaceDescriptor,
    pack_footer,
    pack_header,
    pack_namespace,
    pack_page,
)

NAMESPACES = ("gaia", "hip")
DEFAULT_PAGE_SIZE = 32 * 1024
DEFAULT_LEAF_CODEC = "none"
DEFAULT_SCAN_BATCH_BYTES = 32 * 1024 * 1024
DEFAULT_MERGE_FAN_IN = 32
DEFAULT_MERGE_BATCH_ROWS = 262_144
DEFAULT_EXTERNAL_SORT_MEMORY_LIMIT = "2GB"
DEFAULT_PROGRESS_INTERVAL_CELLS = 100_000
REPORT_FORMAT = "foundinspace.octree.identity-locator-report/v1"
WORK_STATE_FORMAT = "foundinspace.octree.identity-locator-work/v1"
BUILD_ALGORITHM = "parquet-runs-duckdb-fanin-btree/v1"
RUN_SCHEMA = pa.schema(
    [
        pa.field("source_id", pa.uint64(), nullable=False),
        pa.field("cell_record", pa.uint32(), nullable=False),
        pa.field("ordinal", pa.uint32(), nullable=False),
    ]
)
RUN_DTYPE = np.dtype([("source_id", "<u8"), ("cell_record", "<u4"), ("ordinal", "<u4")])
LEAF_CATALOG_FMT = struct.Struct("<QQQQI4x")
_SOURCE_PREFIXES = {
    "gaia": b"\x04\x00gaia",
    "hip": b"\x03\x00hip",
    "manual": b"\x06\x00manual",
}
_MAX_U64_DECIMAL = b"18446744073709551615"


@dataclass(frozen=True, slots=True)
class IdentityLocatorBuildProgress:
    phase: str
    completed: int
    total: int
    detail: str = ""


@dataclass(frozen=True, slots=True)
class IdentityLocatorBuildConfig:
    render_octree_path: Path
    identifiers_order_path: Path
    output_path: Path
    report_path: Path
    work_dir: Path
    decoded_page_size: int = DEFAULT_PAGE_SIZE
    leaf_codec: str = DEFAULT_LEAF_CODEC
    scan_batch_bytes: int = DEFAULT_SCAN_BATCH_BYTES
    max_cell_payload_bytes: int = IDENTITY_UNCOMPRESSED_CELL_LIMIT_BYTES
    merge_fan_in: int = DEFAULT_MERGE_FAN_IN
    merge_batch_rows: int = DEFAULT_MERGE_BATCH_ROWS
    external_sort_memory_limit: str = DEFAULT_EXTERNAL_SORT_MEMORY_LIMIT
    progress_interval_cells: int = DEFAULT_PROGRESS_INTERVAL_CELLS
    force: bool = False
    retain_work: bool = False
    progress: Callable[[IdentityLocatorBuildProgress], None] | None = None

    def validate(self) -> None:
        if self.decoded_page_size < 128 or self.decoded_page_size > 64 * 1024:
            raise ValueError("decoded_page_size must be in 128..65536")
        if self.leaf_codec not in CODEC_CODES:
            raise ValueError("leaf_codec must be 'none' or 'gzip'")
        if self.scan_batch_bytes <= 0:
            raise ValueError("scan_batch_bytes must be > 0")
        if self.max_cell_payload_bytes <= 0:
            raise ValueError("max_cell_payload_bytes must be > 0")
        if self.merge_fan_in < 2:
            raise ValueError("merge_fan_in must be >= 2")
        if self.merge_batch_rows <= 0:
            raise ValueError("merge_batch_rows must be > 0")
        if not self.external_sort_memory_limit:
            raise ValueError("external_sort_memory_limit must not be empty")
        if self.progress_interval_cells <= 0:
            raise ValueError("progress_interval_cells must be > 0")


@dataclass(frozen=True, slots=True)
class IdentityLocatorBuildResult:
    output_path: Path
    report_path: Path
    locator_uuid: UUID
    parent_dataset_uuid: UUID
    identifiers_order_uuid: UUID
    source_sha256: str
    output_sha256: str
    namespace_counts: dict[str, int]
    decoded_page_size: int
    leaf_codec: str


@dataclass(frozen=True, slots=True)
class _BufferedCell:
    record_index: int
    record: IdentifiersOrderRecord
    payload: bytes


@dataclass(frozen=True, slots=True)
class _Run:
    path: Path
    record_count: int
    minimum_key: int
    maximum_key: int
    checksum: str

    def to_json(self, work_dir: Path) -> dict[str, object]:
        return {
            "path": self.path.relative_to(work_dir).as_posix(),
            "record_count": self.record_count,
            "minimum_key": self.minimum_key,
            "maximum_key": self.maximum_key,
            "checksum": self.checksum,
        }

    @classmethod
    def from_json(cls, raw: dict[str, object], work_dir: Path) -> _Run:
        return cls(
            path=work_dir / str(raw["path"]),
            record_count=int(raw["record_count"]),
            minimum_key=int(raw["minimum_key"]),
            maximum_key=int(raw["maximum_key"]),
            checksum=str(raw["checksum"]),
        )


@dataclass(frozen=True, slots=True)
class _LeafProduct:
    namespace: str
    spool_path: Path
    catalog_path: Path
    record_count: int
    minimum_key: int
    maximum_key: int
    page_count: int
    encoded_bytes: int
    decoded_bytes: int
    content_checksum: bytes


@dataclass(slots=True)
class _NavPage:
    child_level: int
    child_start: int
    child_count: int
    maximum_key: int
    absolute_offset: int = 0
    encoded_length: int = 0


@dataclass(slots=True)
class _NamespaceTree:
    leaf: _LeafProduct
    levels: list[list[_NavPage]]
    leaf_base: int = 0

    @property
    def navigation_page_count(self) -> int:
        return sum(len(level) for level in self.levels)


def _notify(
    config: IdentityLocatorBuildConfig,
    phase: str,
    completed: int,
    total: int,
    detail: str = "",
) -> None:
    if config.progress is not None:
        config.progress(
            IdentityLocatorBuildProgress(
                phase=phase,
                completed=completed,
                total=total,
                detail=detail,
            )
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fp:
        while chunk := fp.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temporary, "w", encoding="utf-8") as fp:
        json.dump(value, fp, indent=2, sort_keys=True)
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(temporary, path)


def _accumulate_timing(
    state_path: Path,
    state: dict[str, Any],
    phase: str,
    elapsed: float,
) -> None:
    timings = state.setdefault("cumulative_timings_seconds", {})
    timings[phase] = float(timings.get(phase, 0.0)) + elapsed
    _atomic_write_json(state_path, state)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _strict_u64_decimal(raw: bytes, *, source: str) -> np.uint64:
    if not raw or any(value < 48 or value > 57 for value in raw):
        raise ValueError(f"{source} source IDs must be canonical decimal integers")
    if len(raw) > 1 and raw[0] == 48:
        raise ValueError(f"{source} source IDs must not contain leading zeroes")
    if len(raw) > 20 or (len(raw) == 20 and raw > _MAX_U64_DECIMAL):
        raise ValueError(f"{source} source ID exceeds uint64")
    return np.uint64(int(raw))


def _find_prefix_offsets(values: np.ndarray, prefix: bytes) -> np.ndarray:
    limit = len(values) - len(prefix) + 1
    if limit <= 0:
        return np.empty(0, dtype=np.int64)
    mask = values[:limit] == prefix[0]
    for offset, expected in enumerate(prefix[1:], start=1):
        mask &= values[offset : offset + limit] == expected
    return np.flatnonzero(mask)


def _candidate_row_ends(
    values: np.ndarray,
    *,
    starts: np.ndarray,
    prefix_length: int,
    cell_ends: np.ndarray,
) -> np.ndarray:
    """Find candidate row ends without trusting raw prefix matches as boundaries."""
    ends = np.full(len(starts), -1, dtype=np.int64)
    if not len(starts):
        return ends
    length_positions = starts + prefix_length
    safe = length_positions + 1 < len(values)
    safe_indexes = np.flatnonzero(safe)
    if not len(safe_indexes):
        return ends
    selected_positions = length_positions[safe_indexes]
    lengths = values[selected_positions].astype(np.uint16)
    lengths |= values[selected_positions + 1].astype(np.uint16) << 8
    candidate_ends = selected_positions + 2 + lengths
    cells = np.searchsorted(cell_ends, starts[safe_indexes], side="right")
    in_cell = cells < len(cell_ends)
    bounded_indexes = safe_indexes[in_cell]
    bounded_cells = cells[in_cell]
    bounded_ends = candidate_ends[in_cell]
    within = bounded_ends <= cell_ends[bounded_cells]
    ends[bounded_indexes[within]] = bounded_ends[within]
    return ends


def _decode_numeric_ids(
    values: np.ndarray,
    *,
    starts: np.ndarray,
    prefix_length: int,
    cell_ends: np.ndarray,
    source: str,
) -> tuple[np.ndarray, np.ndarray]:
    if not len(starts):
        return np.empty(0, dtype=np.uint64), np.empty(0, dtype=np.int64)
    length_positions = starts + prefix_length
    if int(length_positions[-1]) + 1 >= len(values):
        raise ValueError("Identifiers/order payload is truncated reading source_id")
    lengths = values[length_positions].astype(np.uint16)
    lengths |= values[length_positions + 1].astype(np.uint16) << 8
    id_starts = length_positions + 2
    cells = np.searchsorted(cell_ends, starts, side="right")
    if np.any(cells >= len(cell_ends)):
        raise ValueError("Identifiers/order row falls outside its cell payload")
    if np.any(id_starts + lengths > cell_ends[cells]):
        raise ValueError("Identifiers/order source_id crosses a cell boundary")
    if np.any((lengths == 0) | (lengths > 20)):
        raise ValueError(f"{source} source ID is outside the uint64 decimal codec")

    decoded = np.zeros(len(starts), dtype=np.uint64)
    for digit_count in np.unique(lengths):
        indexes = np.flatnonzero(lengths == digit_count)
        selected = id_starts[indexes]
        digits = np.empty((len(indexes), int(digit_count)), dtype=np.uint8)
        for digit_offset in range(int(digit_count)):
            digits[:, digit_offset] = values[selected + digit_offset]
        if np.any((digits < 48) | (digits > 57)):
            raise ValueError(f"{source} source IDs must be canonical decimal integers")
        if digit_count > 1 and np.any(digits[:, 0] == 48):
            raise ValueError(f"{source} source IDs must not contain leading zeroes")
        if digit_count == 20:
            maximum = np.frombuffer(_MAX_U64_DECIMAL, dtype=np.uint8)
            undecided = np.ones(len(indexes), dtype=bool)
            greater = np.zeros(len(indexes), dtype=bool)
            for digit_offset in range(20):
                greater |= undecided & (digits[:, digit_offset] > maximum[digit_offset])
                undecided &= digits[:, digit_offset] == maximum[digit_offset]
            if np.any(greater):
                raise ValueError(f"{source} source ID exceeds uint64")
        values_u64 = np.zeros(len(indexes), dtype=np.uint64)
        for digit_offset in range(int(digit_count)):
            values_u64 *= np.uint64(10)
            values_u64 += digits[:, digit_offset] - 48
        decoded[indexes] = values_u64
    return decoded, cells


def _records_from_cells(
    cells: Sequence[_BufferedCell],
) -> tuple[dict[str, np.ndarray], int]:
    if not cells:
        return {namespace: np.empty(0, dtype=RUN_DTYPE) for namespace in NAMESPACES}, 0
    cell_starts_list: list[int] = []
    cell_ends_list: list[int] = []
    payload_offset = 0
    for cell in cells:
        cell_starts_list.append(payload_offset)
        payload_offset += len(cell.payload)
        cell_ends_list.append(payload_offset)
    payload = b"".join(cell.payload for cell in cells)
    values = np.frombuffer(payload, dtype=np.uint8)
    cell_starts = np.asarray(cell_starts_list, dtype=np.int64)
    cell_ends = np.asarray(cell_ends_list, dtype=np.int64)
    starts_by_source = {
        source: _find_prefix_offsets(values, prefix)
        for source, prefix in _SOURCE_PREFIXES.items()
    }
    ends_by_source = {
        source: _candidate_row_ends(
            values,
            starts=starts,
            prefix_length=len(_SOURCE_PREFIXES[source]),
            cell_ends=cell_ends,
        )
        for source, starts in starts_by_source.items()
    }
    all_starts_unsorted = np.concatenate(tuple(starts_by_source.values()))
    all_ends_unsorted = np.concatenate(tuple(ends_by_source.values()))
    sort_order = np.argsort(all_starts_unsorted, kind="stable")
    all_starts = all_starts_unsorted[sort_order]
    all_ends = all_ends_unsorted[sort_order]
    candidate_cells = np.searchsorted(cell_ends, all_starts, side="right")
    observed = np.bincount(candidate_cells, minlength=len(cells))
    expected = np.fromiter(
        (cell.record.star_count for cell in cells),
        dtype=np.int64,
        count=len(cells),
    )
    is_fast = observed == expected
    nonempty_cells = np.flatnonzero(observed)
    cumulative = np.cumsum(observed)
    first_indexes = cumulative[nonempty_cells] - observed[nonempty_cells]
    last_indexes = cumulative[nonempty_cells] - 1
    is_fast[nonempty_cells] &= (
        all_starts[first_indexes] == cell_starts[nonempty_cells]
    ) & (all_ends[last_indexes] == cell_ends[nonempty_cells])
    invalid_ends = np.flatnonzero(all_ends < 0)
    if len(invalid_ends):
        is_fast[candidate_cells[invalid_ends]] = False
    if len(all_starts) > 1:
        same_cell = candidate_cells[1:] == candidate_cells[:-1]
        bad_links = same_cell & (all_starts[1:] != all_ends[:-1])
        if np.any(bad_links):
            is_fast[candidate_cells[1:][bad_links]] = False
    slow_cells = np.flatnonzero(~is_fast)
    record_indexes = np.fromiter(
        (cell.record_index for cell in cells), dtype=np.uint32, count=len(cells)
    )
    out: dict[str, list[np.ndarray]] = {namespace: [] for namespace in NAMESPACES}

    for source in NAMESPACES:
        starts = starts_by_source[source]
        selected_cells = np.searchsorted(cell_ends, starts, side="right")
        starts = starts[is_fast[selected_cells]]
        decoded, selected_cells = _decode_numeric_ids(
            values,
            starts=starts,
            prefix_length=len(_SOURCE_PREFIXES[source]),
            cell_ends=cell_ends,
            source=source,
        )
        ordinals = np.searchsorted(all_starts, starts, side="left")
        ordinals -= np.searchsorted(
            all_starts,
            cell_starts[selected_cells],
            side="left",
        )
        records = np.empty(len(starts), dtype=RUN_DTYPE)
        records["source_id"] = decoded
        records["cell_record"] = record_indexes[selected_cells]
        records["ordinal"] = ordinals.astype(np.uint32)
        out[source].append(records)

    manual_starts = starts_by_source["manual"]
    manual_cells = np.searchsorted(cell_ends, manual_starts, side="right")
    unknown_rows = int(np.count_nonzero(is_fast[manual_cells]))
    for cell_index in slow_cells:
        cell = cells[int(cell_index)]
        rows = decode_identity_rows(cell.payload, star_count=cell.record.star_count)
        for ordinal, (source, source_id) in enumerate(rows):
            if source not in NAMESPACES:
                unknown_rows += 1
                continue
            record = np.empty(1, dtype=RUN_DTYPE)
            record["source_id"][0] = _strict_u64_decimal(
                source_id.encode("utf-8"), source=source
            )
            record["cell_record"][0] = cell.record_index
            record["ordinal"][0] = ordinal
            out[source].append(record)

    combined = {
        namespace: (np.concatenate(parts) if parts else np.empty(0, dtype=RUN_DTYPE))
        for namespace, parts in out.items()
    }
    return combined, unknown_rows


def _table_from_records(records: np.ndarray) -> pa.Table:
    return pa.table(
        {
            "source_id": pa.array(records["source_id"], type=pa.uint64()),
            "cell_record": pa.array(records["cell_record"], type=pa.uint32()),
            "ordinal": pa.array(records["ordinal"], type=pa.uint32()),
        },
        schema=RUN_SCHEMA,
    )


def _records_from_table(table: pa.Table) -> np.ndarray:
    if not table.schema.equals(RUN_SCHEMA, check_metadata=False):
        table = table.cast(RUN_SCHEMA)
    table = table.combine_chunks()
    records = np.empty(len(table), dtype=RUN_DTYPE)
    records["source_id"] = table.column("source_id").to_numpy(zero_copy_only=False)
    records["cell_record"] = table.column("cell_record").to_numpy(zero_copy_only=False)
    records["ordinal"] = table.column("ordinal").to_numpy(zero_copy_only=False)
    return records


def _validate_sorted_records(
    records: np.ndarray,
    *,
    namespace: str,
    previous_key: int | None = None,
) -> int | None:
    if not len(records):
        return previous_key
    keys = records["source_id"]
    if previous_key is not None and int(keys[0]) <= previous_key:
        if int(keys[0]) == previous_key:
            raise ValueError(f"Duplicate rendered identity: {namespace}:{previous_key}")
        raise ValueError(f"Identity run is not sorted for namespace {namespace}")
    descending = np.flatnonzero(keys[1:] <= keys[:-1])
    if len(descending):
        index = int(descending[0])
        if keys[index + 1] == keys[index]:
            raise ValueError(
                f"Duplicate rendered identity: {namespace}:{int(keys[index])}"
            )
        raise ValueError(f"Identity run is not sorted for namespace {namespace}")
    return int(keys[-1])


def _write_sorted_run(
    records: np.ndarray,
    path: Path,
    *,
    namespace: str,
    row_group_size: int,
) -> _Run:
    if not len(records):
        raise ValueError("Cannot write an empty identity locator run")
    records.sort(order=("source_id", "cell_record", "ordinal"))
    _validate_sorted_records(records, namespace=namespace)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    pq.write_table(
        _table_from_records(records),
        temporary,
        compression="zstd",
        write_statistics=False,
        row_group_size=row_group_size,
    )
    os.replace(temporary, path)
    return _Run(
        path=path,
        record_count=len(records),
        minimum_key=int(records["source_id"][0]),
        maximum_key=int(records["source_id"][-1]),
        checksum=hashlib.sha256(records.tobytes()).hexdigest(),
    )


def _run_from_state(raw: dict[str, object], work_dir: Path) -> _Run:
    run = _Run.from_json(raw, work_dir)
    if not run.path.is_file():
        raise FileNotFoundError(f"Identity locator run is missing: {run.path}")
    parquet = pq.ParquetFile(run.path)
    if parquet.metadata.num_rows != run.record_count:
        raise ValueError(f"Identity locator run row count changed: {run.path}")
    if not parquet.schema_arrow.equals(RUN_SCHEMA, check_metadata=False):
        raise ValueError(f"Identity locator run schema changed: {run.path}")
    return run


def _merge_run_group(
    runs: Sequence[_Run],
    output_path: Path,
    *,
    namespace: str,
    config: IdentityLocatorBuildConfig,
) -> _Run:
    if not runs:
        raise ValueError("Merging identity locator runs requires input")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path = output_path.with_suffix(output_path.suffix + ".json")
    input_identity = [run.checksum for run in runs]
    if output_path.is_file() and metadata_path.is_file():
        raw = _read_json(metadata_path)
        if raw.get("input_checksums") == input_identity:
            return _run_from_state(raw["run"], config.work_dir)

    temporary = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    local_spill = output_path.parent / f".{output_path.stem}.duckdb-spill"
    shutil.rmtree(local_spill, ignore_errors=True)
    local_spill.mkdir(parents=True)
    con = duckdb.connect()
    writer: pq.ParquetWriter | None = None
    digest = hashlib.sha256()
    record_count = 0
    minimum_key: int | None = None
    maximum_key: int | None = None
    previous_key: int | None = None
    try:
        with redirect_stdout(io.StringIO()):
            configure_connection(con)
        con.execute("SET temp_directory = ?", [str(local_spill)])
        con.execute("SET memory_limit = ?", [config.external_sort_memory_limit])
        con.execute("SET preserve_insertion_order = false")
        paths = ",".join(
            f"'{run.path.as_posix().replace(chr(39), chr(39) * 2)}'" for run in runs
        )
        query = f"""
            SELECT
                source_id::UBIGINT AS source_id,
                cell_record::UINTEGER AS cell_record,
                ordinal::UINTEGER AS ordinal
            FROM read_parquet(
                [{paths}],
                hive_partitioning = false,
                union_by_name = false
            )
            ORDER BY source_id, cell_record, ordinal
        """
        con.execute(query)
        writer = pq.ParquetWriter(
            temporary,
            RUN_SCHEMA,
            compression="zstd",
            write_statistics=False,
        )
        for batch in con.to_arrow_reader(batch_size=config.merge_batch_rows):
            table = pa.Table.from_batches([batch]).cast(RUN_SCHEMA)
            records = _records_from_table(table)
            previous_key = _validate_sorted_records(
                records,
                namespace=namespace,
                previous_key=previous_key,
            )
            if len(records):
                minimum_key = (
                    int(records["source_id"][0]) if minimum_key is None else minimum_key
                )
                maximum_key = int(records["source_id"][-1])
                digest.update(records.tobytes())
                record_count += len(records)
                writer.write_table(table, row_group_size=config.merge_batch_rows)
        writer.close()
        writer = None
        os.replace(temporary, output_path)
    except BaseException:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
        raise
    finally:
        con.close()
        shutil.rmtree(local_spill, ignore_errors=True)

    if minimum_key is None or maximum_key is None:
        raise ValueError("Merged identity locator run is empty")
    result = _Run(
        path=output_path,
        record_count=record_count,
        minimum_key=minimum_key,
        maximum_key=maximum_key,
        checksum=digest.hexdigest(),
    )
    _atomic_write_json(
        metadata_path,
        {
            "input_checksums": input_identity,
            "run": result.to_json(config.work_dir),
        },
    )
    return result


def _work_identity(
    *,
    source_sha256: str,
    parent_dataset_uuid: UUID,
    identifiers_order_uuid: UUID,
) -> str:
    encoded = json.dumps(
        {
            "algorithm": BUILD_ALGORITHM,
            "source_sha256": source_sha256,
            "parent_dataset_uuid": str(parent_dataset_uuid),
            "identifiers_order_uuid": str(identifiers_order_uuid),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _initial_state(
    *,
    work_identity: str,
    source_sha256: str,
    source_size: int,
    source_file_stat: dict[str, int],
    parent_dataset_uuid: UUID,
    identifiers_order_uuid: UUID,
) -> dict[str, Any]:
    return {
        "format": WORK_STATE_FORMAT,
        "work_identity": work_identity,
        "source_sha256": source_sha256,
        "source_size": source_size,
        "source_file_stat": source_file_stat,
        "parent_dataset_uuid": str(parent_dataset_uuid),
        "identifiers_order_uuid": str(identifiers_order_uuid),
        "scan_complete": False,
        "next_record": 0,
        "scanned_stars": 0,
        "skipped_namespace_rows": 0,
        "runs": {namespace: [] for namespace in NAMESPACES},
        "current_runs": None,
        "merge_rounds": dict.fromkeys(NAMESPACES, 0),
        "cumulative_timings_seconds": {},
    }


def _load_or_create_state(
    config: IdentityLocatorBuildConfig,
    *,
    work_identity: str,
    source_sha256: str,
    source_size: int,
    source_file_stat: dict[str, int],
    parent_dataset_uuid: UUID,
    identifiers_order_uuid: UUID,
) -> tuple[Path, dict[str, Any]]:
    state_path = config.work_dir / "state.json"
    if state_path.is_file():
        state = _read_json(state_path)
        if state.get("format") != WORK_STATE_FORMAT:
            raise ValueError("Unsupported identity locator work state")
        if state.get("work_identity") != work_identity:
            raise FileExistsError(
                f"Identity locator work belongs to another input: {config.work_dir}"
            )
        return state_path, state
    if config.work_dir.exists() and any(config.work_dir.iterdir()):
        raise FileExistsError(
            f"Identity locator work directory is not empty: {config.work_dir}"
        )
    config.work_dir.mkdir(parents=True, exist_ok=True)
    state = _initial_state(
        work_identity=work_identity,
        source_sha256=source_sha256,
        source_size=source_size,
        source_file_stat=source_file_stat,
        parent_dataset_uuid=parent_dataset_uuid,
        identifiers_order_uuid=identifiers_order_uuid,
    )
    _atomic_write_json(state_path, state)
    return state_path, state


def _scan_identity_runs(
    config: IdentityLocatorBuildConfig,
    *,
    state_path: Path,
    state: dict[str, Any],
) -> dict[str, list[_Run]]:
    if state.get("scan_complete"):
        raw_current = state.get("current_runs") or state["runs"]
        return {
            namespace: [
                _run_from_state(raw, config.work_dir)
                for raw in raw_current.get(namespace, [])
            ]
            for namespace in NAMESPACES
        }

    next_record = int(state.get("next_record", 0))
    run_values: dict[str, list[dict[str, object]]] = {
        namespace: list(state["runs"].get(namespace, [])) for namespace in NAMESPACES
    }
    buffered: list[_BufferedCell] = []
    buffered_bytes = 0
    scanned_stars = int(state.get("scanned_stars", 0))
    skipped_rows = int(state.get("skipped_namespace_rows", 0))
    next_progress = next_record + config.progress_interval_cells

    def flush() -> None:
        nonlocal buffered, buffered_bytes, next_record, skipped_rows
        if not buffered:
            return
        arrays, batch_skipped = _records_from_cells(buffered)
        skipped_rows += batch_skipped
        for namespace in NAMESPACES:
            records = arrays[namespace]
            if not len(records):
                continue
            run_index = len(run_values[namespace])
            run_path = (
                config.work_dir / "scan" / namespace / f"run-{run_index:06d}.parquet"
            )
            run = _write_sorted_run(
                records,
                run_path,
                namespace=namespace,
                row_group_size=config.merge_batch_rows,
            )
            run_values[namespace].append(run.to_json(config.work_dir))
        next_record = buffered[-1].record_index + 1
        state.update(
            {
                "next_record": next_record,
                "scanned_stars": scanned_stars,
                "skipped_namespace_rows": skipped_rows,
                "runs": run_values,
            }
        )
        _atomic_write_json(state_path, state)
        buffered = []
        buffered_bytes = 0

    with IdentifiersOrderReader(config.identifiers_order_path) as reader:
        total_records = reader.header.record_count
        for record_index, record, payload in reader.iter_indexed_cell_identity_payloads(
            max_uncompressed_bytes=config.max_cell_payload_bytes,
            start_record=next_record,
        ):
            if buffered and buffered_bytes + len(payload) > config.scan_batch_bytes:
                flush()
            buffered.append(
                _BufferedCell(
                    record_index=record_index,
                    record=record,
                    payload=payload,
                )
            )
            buffered_bytes += len(payload)
            scanned_stars += record.star_count
            if record_index + 1 >= next_progress:
                flush()
                _notify(
                    config,
                    "scan",
                    next_record,
                    total_records,
                    f"stars={scanned_stars:,}",
                )
                next_progress = next_record + config.progress_interval_cells
        flush()
        state["scan_complete"] = True
        state["current_runs"] = run_values
        _atomic_write_json(state_path, state)
        _notify(
            config,
            "scan",
            total_records,
            total_records,
            f"stars={scanned_stars:,}",
        )
    return {
        namespace: [
            _run_from_state(raw, config.work_dir) for raw in run_values[namespace]
        ]
        for namespace in NAMESPACES
    }


def _reduce_runs(
    config: IdentityLocatorBuildConfig,
    *,
    state_path: Path,
    state: dict[str, Any],
    namespace: str,
    runs: list[_Run],
) -> _Run | None:
    if not runs:
        return None
    merge_rounds = state.setdefault("merge_rounds", dict.fromkeys(NAMESPACES, 0))
    round_index = int(merge_rounds.get(namespace, 0))
    while len(runs) > 1:
        next_runs: list[_Run] = []
        chunks = math.ceil(len(runs) / config.merge_fan_in)
        for chunk_index, offset in enumerate(range(0, len(runs), config.merge_fan_in)):
            chunk = runs[offset : offset + config.merge_fan_in]
            if len(chunk) == 1:
                next_runs.append(chunk[0])
            else:
                output = (
                    config.work_dir
                    / "merge"
                    / namespace
                    / f"round-{round_index:03d}"
                    / f"run-{chunk_index:06d}.parquet"
                )
                next_runs.append(
                    _merge_run_group(
                        chunk,
                        output,
                        namespace=namespace,
                        config=config,
                    )
                )
            _notify(
                config,
                f"merge-{namespace}",
                chunk_index + 1,
                chunks,
                f"round={round_index}",
            )
        current = state.get("current_runs") or {value: [] for value in NAMESPACES}
        current[namespace] = [run.to_json(config.work_dir) for run in next_runs]
        state["current_runs"] = current
        merge_rounds[namespace] = round_index + 1
        state["merge_rounds"] = merge_rounds
        _atomic_write_json(state_path, state)
        keep = {run.path for run in next_runs}
        for old in runs:
            if old.path not in keep:
                old.path.unlink(missing_ok=True)
                old.path.with_suffix(old.path.suffix + ".json").unlink(missing_ok=True)
        runs = next_runs
        round_index += 1
    return runs[0]


def _encode_leaf_product(
    config: IdentityLocatorBuildConfig,
    *,
    namespace: str,
    run: _Run | None,
) -> _LeafProduct:
    product_dir = (
        config.work_dir
        / "pages"
        / f"{config.decoded_page_size}-{config.leaf_codec}"
        / namespace
    )
    product_dir.mkdir(parents=True, exist_ok=True)
    spool_path = product_dir / "leaves.bin"
    catalog_path = product_dir / "leaves.catalog"
    spool_tmp = spool_path.with_name(f".{spool_path.name}.{os.getpid()}.tmp")
    catalog_tmp = catalog_path.with_name(f".{catalog_path.name}.{os.getpid()}.tmp")
    codec = CODEC_CODES[config.leaf_codec]
    page_capacity = config.decoded_page_size // LEAF_RECORD_FMT.size
    if page_capacity <= 0:
        raise ValueError("Identity locator page cannot hold one leaf record")

    if run is None:
        spool_tmp.write_bytes(b"")
        catalog_tmp.write_bytes(b"")
        os.replace(spool_tmp, spool_path)
        os.replace(catalog_tmp, catalog_path)
        return _LeafProduct(
            namespace=namespace,
            spool_path=spool_path,
            catalog_path=catalog_path,
            record_count=0,
            minimum_key=0,
            maximum_key=0,
            page_count=0,
            encoded_bytes=0,
            decoded_bytes=0,
            content_checksum=hashlib.sha256(b"").digest(),
        )

    digest = hashlib.sha256()
    page_buffer = bytearray()
    page_count = 0
    record_count = 0
    minimum_key: int | None = None
    maximum_key: int | None = None
    previous_key: int | None = None

    with open(spool_tmp, "wb") as spool, open(catalog_tmp, "wb") as catalog:

        def flush_page() -> None:
            nonlocal page_count, page_buffer
            if not page_buffer:
                return
            entry_count = len(page_buffer) // LEAF_RECORD_FMT.size
            first_key = LEAF_RECORD_FMT.unpack_from(page_buffer, 0)[0]
            last_key = LEAF_RECORD_FMT.unpack_from(
                page_buffer, len(page_buffer) - LEAF_RECORD_FMT.size
            )[0]
            encoded = pack_page(
                kind=PAGE_KIND_LEAF,
                codec=codec,
                entry_count=entry_count,
                decoded=bytes(page_buffer),
            )
            spool_offset = spool.tell()
            spool.write(encoded)
            catalog.write(
                LEAF_CATALOG_FMT.pack(
                    first_key,
                    last_key,
                    spool_offset,
                    len(encoded),
                    entry_count,
                )
            )
            page_count += 1
            page_buffer = bytearray()

        parquet = pq.ParquetFile(run.path)
        for batch in parquet.iter_batches(
            batch_size=config.merge_batch_rows,
            columns=list(RUN_SCHEMA.names),
            use_threads=False,
        ):
            records = _records_from_table(pa.Table.from_batches([batch]))
            previous_key = _validate_sorted_records(
                records,
                namespace=namespace,
                previous_key=previous_key,
            )
            if not len(records):
                continue
            if minimum_key is None:
                minimum_key = int(records["source_id"][0])
            maximum_key = int(records["source_id"][-1])
            raw = records.tobytes()
            digest.update(raw)
            record_count += len(records)
            offset = 0
            while offset < len(raw):
                page_room = page_capacity * LEAF_RECORD_FMT.size - len(page_buffer)
                take = min(page_room, len(raw) - offset)
                page_buffer.extend(raw[offset : offset + take])
                offset += take
                if len(page_buffer) == page_capacity * LEAF_RECORD_FMT.size:
                    flush_page()
        flush_page()
        spool.flush()
        os.fsync(spool.fileno())
        catalog.flush()
        os.fsync(catalog.fileno())

    if record_count != run.record_count:
        raise ValueError(
            f"Identity locator run row count changed for {namespace}: "
            f"{record_count} != {run.record_count}"
        )
    if digest.hexdigest() != run.checksum:
        raise ValueError(f"Identity locator run checksum changed for {namespace}")
    if minimum_key is None or maximum_key is None:
        raise ValueError(f"Identity locator run is unexpectedly empty: {namespace}")
    os.replace(spool_tmp, spool_path)
    os.replace(catalog_tmp, catalog_path)
    return _LeafProduct(
        namespace=namespace,
        spool_path=spool_path,
        catalog_path=catalog_path,
        record_count=record_count,
        minimum_key=minimum_key,
        maximum_key=maximum_key,
        page_count=page_count,
        encoded_bytes=spool_path.stat().st_size,
        decoded_bytes=record_count * LEAF_RECORD_FMT.size,
        content_checksum=digest.digest(),
    )


def _leaf_catalog_record(product: _LeafProduct, index: int) -> tuple[int, ...]:
    if index < 0 or index >= product.page_count:
        raise IndexError(index)
    with open(product.catalog_path, "rb") as fp:
        fp.seek(index * LEAF_CATALOG_FMT.size)
        raw = fp.read(LEAF_CATALOG_FMT.size)
    if len(raw) != LEAF_CATALOG_FMT.size:
        raise ValueError("Identity locator leaf catalog is truncated")
    return LEAF_CATALOG_FMT.unpack(raw)


def _build_tree(product: _LeafProduct, *, decoded_page_size: int) -> _NamespaceTree:
    if product.page_count <= 1:
        return _NamespaceTree(leaf=product, levels=[])
    fanout = decoded_page_size // CHILD_RECORD_FMT.size
    if fanout < 2:
        raise ValueError("Identity locator page cannot hold two child records")
    bottom: list[_NavPage] = []
    for start in range(0, product.page_count, fanout):
        count = min(fanout, product.page_count - start)
        maximum_key = int(_leaf_catalog_record(product, start + count - 1)[1])
        bottom.append(
            _NavPage(
                child_level=-1,
                child_start=start,
                child_count=count,
                maximum_key=maximum_key,
                encoded_length=PAGE_HEADER_SIZE + count * CHILD_RECORD_FMT.size,
            )
        )
    levels = [bottom]
    while len(levels[-1]) > 1:
        children = levels[-1]
        parent: list[_NavPage] = []
        for start in range(0, len(children), fanout):
            count = min(fanout, len(children) - start)
            parent.append(
                _NavPage(
                    child_level=len(levels) - 1,
                    child_start=start,
                    child_count=count,
                    maximum_key=children[start + count - 1].maximum_key,
                    encoded_length=(PAGE_HEADER_SIZE + count * CHILD_RECORD_FMT.size),
                )
            )
        levels.append(parent)
    return _NamespaceTree(leaf=product, levels=levels)


def _assign_tree_offsets(
    trees: Sequence[_NamespaceTree],
    *,
    navigation_start: int,
) -> int:
    cursor = navigation_start
    for tree in trees:
        for level in reversed(tree.levels):
            for page in level:
                page.absolute_offset = cursor
                cursor += page.encoded_length
    for tree in trees:
        tree.leaf_base = cursor
        cursor += tree.leaf.encoded_bytes
    return cursor


def _encode_navigation_page(tree: _NamespaceTree, page: _NavPage) -> bytes:
    decoded = bytearray()
    if page.child_level < 0:
        with open(tree.leaf.catalog_path, "rb") as catalog:
            catalog.seek(page.child_start * LEAF_CATALOG_FMT.size)
            for _index in range(page.child_count):
                raw = catalog.read(LEAF_CATALOG_FMT.size)
                if len(raw) != LEAF_CATALOG_FMT.size:
                    raise ValueError("Identity locator leaf catalog is truncated")
                _minimum, maximum, spool_offset, length, _entries = (
                    LEAF_CATALOG_FMT.unpack(raw)
                )
                decoded.extend(
                    CHILD_RECORD_FMT.pack(
                        maximum,
                        tree.leaf_base + spool_offset,
                        length,
                    )
                )
    else:
        children = tree.levels[page.child_level]
        for child in children[page.child_start : page.child_start + page.child_count]:
            decoded.extend(
                CHILD_RECORD_FMT.pack(
                    child.maximum_key,
                    child.absolute_offset,
                    child.encoded_length,
                )
            )
    encoded = pack_page(
        kind=PAGE_KIND_INTERNAL,
        codec=CODEC_NONE,
        entry_count=page.child_count,
        decoded=bytes(decoded),
    )
    if len(encoded) != page.encoded_length:
        raise AssertionError("Identity locator navigation page length changed")
    return encoded


def _build_identity_digest(
    *,
    source_sha256: str,
    parent_dataset_uuid: UUID,
    identifiers_order_uuid: UUID,
    decoded_page_size: int,
    leaf_codec: str,
) -> bytes:
    value = {
        "format": 1,
        "algorithm": BUILD_ALGORITHM,
        "source_sha256": source_sha256,
        "parent_dataset_uuid": str(parent_dataset_uuid),
        "identifiers_order_uuid": str(identifiers_order_uuid),
        "decoded_page_size": decoded_page_size,
        "leaf_codec": leaf_codec,
        "gzip_mtime": 0 if leaf_codec == "gzip" else None,
    }
    if leaf_codec == "gzip":
        value["gzip_compresslevel"] = GZIP_COMPRESSLEVEL
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).digest()


def _uuid_from_digest(digest: bytes) -> UUID:
    raw = bytearray(digest[:16])
    raw[6] = (raw[6] & 0x0F) | 0x50
    raw[8] = (raw[8] & 0x3F) | 0x80
    return UUID(bytes=bytes(raw))


def _root_range(tree: _NamespaceTree) -> tuple[int, int]:
    if tree.levels:
        root = tree.levels[-1][0]
        return root.absolute_offset, root.encoded_length
    if tree.leaf.page_count == 1:
        _minimum, _maximum, spool_offset, length, _entries = _leaf_catalog_record(
            tree.leaf, 0
        )
        return tree.leaf_base + spool_offset, length
    return 0, 0


def _validation_samples(products: dict[str, _LeafProduct]) -> dict[str, list[int]]:
    samples: dict[str, list[int]] = {}
    for namespace, product in products.items():
        if not product.page_count:
            samples[namespace] = []
            continue
        page_indexes = {
            0,
            product.page_count // 4,
            product.page_count // 2,
            (3 * product.page_count) // 4,
            product.page_count - 1,
        }
        keys: list[int] = []
        for page_index in sorted(page_indexes):
            minimum, maximum, _offset, _length, _entries = _leaf_catalog_record(
                product, page_index
            )
            keys.extend((int(minimum), int(maximum)))
        samples[namespace] = list(dict.fromkeys(keys))
    return samples


def _assemble_locator(
    config: IdentityLocatorBuildConfig,
    *,
    products: dict[str, _LeafProduct],
    source_sha256: str,
    parent_dataset_uuid: UUID,
    identifiers_order_uuid: UUID,
) -> tuple[UUID, list[NamespaceDescriptor]]:
    trees = [
        _build_tree(products[namespace], decoded_page_size=config.decoded_page_size)
        for namespace in NAMESPACES
    ]
    namespace_directory_offset = HEADER_SIZE
    namespace_directory_length = len(trees) * NAMESPACE_SIZE
    content_end = _assign_tree_offsets(
        trees,
        navigation_start=HEADER_SIZE + namespace_directory_length,
    )
    integrity_offset = content_end
    total_length = integrity_offset + FOOTER_SIZE
    build_identity = _build_identity_digest(
        source_sha256=source_sha256,
        parent_dataset_uuid=parent_dataset_uuid,
        identifiers_order_uuid=identifiers_order_uuid,
        decoded_page_size=config.decoded_page_size,
        leaf_codec=config.leaf_codec,
    )
    locator_uuid = _uuid_from_digest(build_identity)
    codec = CODEC_CODES[config.leaf_codec]
    descriptors: list[NamespaceDescriptor] = []
    for tree in trees:
        root_offset, root_length = _root_range(tree)
        descriptors.append(
            NamespaceDescriptor(
                name=tree.leaf.namespace,
                key_codec=KEY_CODEC_U64_DECIMAL,
                value_codec=VALUE_CODEC_CELL_U32_ORDINAL_U32,
                leaf_codec=codec,
                flags=0,
                record_count=tree.leaf.record_count,
                root_offset=root_offset,
                root_length=root_length,
                minimum_key=tree.leaf.minimum_key,
                maximum_key=tree.leaf.maximum_key,
                leaf_page_count=tree.leaf.page_count,
                navigation_page_count=tree.navigation_page_count,
                decoded_record_size=LEAF_RECORD_FMT.size,
                content_checksum=tree.leaf.content_checksum,
            )
        )
    header = IdentityLocatorHeader(
        total_length=total_length,
        locator_uuid=locator_uuid,
        parent_dataset_uuid=parent_dataset_uuid,
        identifiers_order_uuid=identifiers_order_uuid,
        namespace_directory_offset=namespace_directory_offset,
        namespace_directory_length=namespace_directory_length,
        namespace_count=len(descriptors),
        decoded_page_size=config.decoded_page_size,
        navigation_codec_mask=1 << CODEC_NONE,
        leaf_codec_mask=1 << codec,
        build_identity=build_identity,
        integrity_offset=integrity_offset,
        integrity_length=FOOTER_SIZE,
    )

    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = config.output_path.with_name(
        f".{config.output_path.name}.{os.getpid()}.tmp"
    )
    temporary.unlink(missing_ok=True)
    prefix_digest = hashlib.sha256()
    try:
        with open(temporary, "wb") as output:

            def write_prefix(raw: bytes) -> None:
                output.write(raw)
                prefix_digest.update(raw)

            write_prefix(pack_header(header))
            for descriptor in descriptors:
                write_prefix(pack_namespace(descriptor))
            for tree in trees:
                for level in reversed(tree.levels):
                    for page in level:
                        if output.tell() != page.absolute_offset:
                            raise AssertionError(
                                "Identity locator navigation offset changed"
                            )
                        write_prefix(_encode_navigation_page(tree, page))
            for tree in trees:
                if output.tell() != tree.leaf_base:
                    raise AssertionError("Identity locator leaf offset changed")
                with open(tree.leaf.spool_path, "rb") as leaves:
                    while chunk := leaves.read(8 * 1024 * 1024):
                        write_prefix(chunk)
            if output.tell() != integrity_offset:
                raise AssertionError("Identity locator integrity offset changed")
            output.write(
                pack_footer(
                    hashed_length=integrity_offset,
                    prefix_checksum=prefix_digest.digest(),
                )
            )
            output.flush()
            os.fsync(output.fileno())
        if temporary.stat().st_size != total_length:
            raise ValueError("Identity locator assembled length mismatch")
        os.replace(temporary, config.output_path)
    finally:
        temporary.unlink(missing_ok=True)
    return locator_uuid, descriptors


def _cached_result(
    config: IdentityLocatorBuildConfig,
    *,
    source_sha256: str,
) -> IdentityLocatorBuildResult | None:
    if not config.output_path.exists() and not config.report_path.exists():
        return None
    if not config.output_path.is_file() or not config.report_path.is_file():
        raise FileExistsError(
            "Identity locator output and report must either both exist or both be absent"
        )
    report = _read_json(config.report_path)
    if (
        report.get("format") != REPORT_FORMAT
        or report.get("source_sha256") != source_sha256
        or report.get("decoded_page_size") != config.decoded_page_size
        or report.get("leaf_codec") != config.leaf_codec
    ):
        raise FileExistsError(
            f"Existing identity locator does not match this build: {config.output_path}"
        )
    output_sha256 = _sha256_file(config.output_path)
    if output_sha256 != report.get("output_sha256"):
        raise ValueError("Existing identity locator output checksum changed")
    return IdentityLocatorBuildResult(
        output_path=config.output_path,
        report_path=config.report_path,
        locator_uuid=UUID(str(report["locator_uuid"])),
        parent_dataset_uuid=UUID(str(report["parent_dataset_uuid"])),
        identifiers_order_uuid=UUID(str(report["identifiers_order_uuid"])),
        source_sha256=source_sha256,
        output_sha256=output_sha256,
        namespace_counts={
            name: int(value) for name, value in report["namespace_counts"].items()
        },
        decoded_page_size=config.decoded_page_size,
        leaf_codec=config.leaf_codec,
    )


def build_identity_locator(
    config: IdentityLocatorBuildConfig,
) -> IdentityLocatorBuildResult:
    """Build or resume a deterministic exact Gaia/HIP identity locator."""
    config.validate()
    config = replace(
        config,
        render_octree_path=config.render_octree_path.expanduser().resolve(),
        identifiers_order_path=config.identifiers_order_path.expanduser().resolve(),
        output_path=config.output_path.expanduser().resolve(),
        report_path=config.report_path.expanduser().resolve(),
        work_dir=config.work_dir.expanduser().resolve(),
    )
    started = time.perf_counter()
    for path, label in (
        (config.render_octree_path, "Render octree"),
        (config.identifiers_order_path, "Identifiers/order artifact"),
    ):
        if not path.is_file():
            raise FileNotFoundError(f"{label} not found: {path}")
    if len({config.output_path, config.report_path, config.work_dir}) != 3:
        raise ValueError("Identity locator output, report, and work paths must differ")
    if config.force and config.work_dir.exists():
        shutil.rmtree(config.work_dir)

    render_header = read_octree_header(config.render_octree_path)
    if render_header.artifact_kind != "render" or render_header.dataset_uuid is None:
        raise ValueError(
            "Identity locator requires a render artifact with dataset_uuid"
        )
    with IdentifiersOrderReader(config.identifiers_order_path) as order_reader:
        order_header = order_reader.header
    if order_header.parent_dataset_uuid != render_header.dataset_uuid:
        raise ValueError(
            "Identifiers/order artifact does not match render octree dataset_uuid"
        )
    source_size = config.identifiers_order_path.stat().st_size
    source_stat = config.identifiers_order_path.stat()
    source_file_stat = {
        "device": source_stat.st_dev,
        "inode": source_stat.st_ino,
        "size": source_stat.st_size,
        "mtime_ns": source_stat.st_mtime_ns,
    }
    if order_header.record_count >= 2**32:
        raise ValueError("Identity locator v1 supports fewer than 2^32 cells")
    if order_header.directory_offset < IDENTIFIERS_HEADER_SIZE:
        raise ValueError("Identifiers/order directory overlaps its header")
    if order_header.directory_length != (
        order_header.record_count * DIRECTORY_RECORD_SIZE
    ):
        raise ValueError("Identifiers/order directory length is inconsistent")
    if order_header.directory_offset + order_header.directory_length > source_size:
        raise ValueError("Identifiers/order directory exceeds the source file")
    if order_header.payload_offset < (
        order_header.directory_offset + order_header.directory_length
    ):
        raise ValueError("Identifiers/order payload overlaps its directory")
    if order_header.payload_offset + order_header.payload_length > source_size:
        raise ValueError("Identifiers/order payload exceeds the source file")

    source_sha256: str | None = None
    existing_state_path = config.work_dir / "state.json"
    if existing_state_path.is_file():
        existing_state = _read_json(existing_state_path)
        if (
            existing_state.get("format") == WORK_STATE_FORMAT
            and existing_state.get("source_file_stat") == source_file_stat
        ):
            cached_source_hash = existing_state.get("source_sha256")
            if isinstance(cached_source_hash, str) and len(cached_source_hash) == 64:
                source_sha256 = cached_source_hash
    source_hash_started = time.perf_counter()
    if source_sha256 is None:
        _notify(config, "source-sha256", 0, source_size)
        source_sha256 = _sha256_file(config.identifiers_order_path)
        _notify(config, "source-sha256", source_size, source_size)
    source_hash_seconds = time.perf_counter() - source_hash_started
    cached = (
        None if config.force else _cached_result(config, source_sha256=source_sha256)
    )
    if cached is not None:
        return cached

    work_identity = _work_identity(
        source_sha256=source_sha256,
        parent_dataset_uuid=render_header.dataset_uuid,
        identifiers_order_uuid=order_header.artifact_uuid,
    )
    state_path, state = _load_or_create_state(
        config,
        work_identity=work_identity,
        source_sha256=source_sha256,
        source_size=source_size,
        source_file_stat=source_file_stat,
        parent_dataset_uuid=render_header.dataset_uuid,
        identifiers_order_uuid=order_header.artifact_uuid,
    )
    _accumulate_timing(
        state_path,
        state,
        "source_sha256",
        source_hash_seconds,
    )
    scan_started = time.perf_counter()
    try:
        runs = _scan_identity_runs(config, state_path=state_path, state=state)
    finally:
        scan_seconds = time.perf_counter() - scan_started
        _accumulate_timing(state_path, state, "scan", scan_seconds)
    merge_started = time.perf_counter()
    try:
        final_runs = {
            namespace: _reduce_runs(
                config,
                state_path=state_path,
                state=state,
                namespace=namespace,
                runs=runs[namespace],
            )
            for namespace in NAMESPACES
        }
    finally:
        merge_seconds = time.perf_counter() - merge_started
        _accumulate_timing(state_path, state, "merge", merge_seconds)
    encode_started = time.perf_counter()
    products = {
        namespace: _encode_leaf_product(
            config,
            namespace=namespace,
            run=final_runs[namespace],
        )
        for namespace in NAMESPACES
    }
    locator_uuid, descriptors = _assemble_locator(
        config,
        products=products,
        source_sha256=source_sha256,
        parent_dataset_uuid=render_header.dataset_uuid,
        identifiers_order_uuid=order_header.artifact_uuid,
    )
    encode_seconds = time.perf_counter() - encode_started
    output_sha256 = _sha256_file(config.output_path)
    validation_samples = _validation_samples(products)
    from .reader import validate_identity_locator

    validation = validate_identity_locator(
        config.output_path,
        config.identifiers_order_path,
        samples=validation_samples,
        full_checksum=True,
    )
    namespace_counts = {
        descriptor.name: descriptor.record_count for descriptor in descriptors
    }
    report: dict[str, object] = {
        "format": REPORT_FORMAT,
        "algorithm": BUILD_ALGORITHM,
        "render_octree_path": str(config.render_octree_path),
        "identifiers_order_path": str(config.identifiers_order_path),
        "output_path": str(config.output_path),
        "source_sha256": source_sha256,
        "source_size": source_size,
        "output_sha256": output_sha256,
        "output_size": config.output_path.stat().st_size,
        "locator_uuid": str(locator_uuid),
        "parent_dataset_uuid": str(render_header.dataset_uuid),
        "identifiers_order_uuid": str(order_header.artifact_uuid),
        "decoded_page_size": config.decoded_page_size,
        "leaf_codec": config.leaf_codec,
        "gzip_compresslevel": (
            GZIP_COMPRESSLEVEL if config.leaf_codec == "gzip" else None
        ),
        "benchmark_choice": {
            "decoded_page_size": config.decoded_page_size,
            "leaf_codec": config.leaf_codec,
        },
        "namespace_counts": namespace_counts,
        "namespace_minimum_keys": {
            descriptor.name: descriptor.minimum_key for descriptor in descriptors
        },
        "namespace_maximum_keys": {
            descriptor.name: descriptor.maximum_key for descriptor in descriptors
        },
        "namespace_leaf_pages": {
            descriptor.name: descriptor.leaf_page_count for descriptor in descriptors
        },
        "namespace_navigation_pages": {
            descriptor.name: descriptor.navigation_page_count
            for descriptor in descriptors
        },
        "namespace_content_sha256": {
            descriptor.name: descriptor.content_checksum.hex()
            for descriptor in descriptors
        },
        "decoded_record_bytes": sum(
            product.decoded_bytes for product in products.values()
        ),
        "encoded_leaf_bytes": sum(
            product.encoded_bytes for product in products.values()
        ),
        "skipped_namespace_rows": int(state.get("skipped_namespace_rows", 0)),
        "duplicate_identity_count": 0,
        "rejected_identity_count": 0,
        "validation_samples": validation_samples,
        "validation": validation,
        "bounds": {
            "scan_batch_bytes": config.scan_batch_bytes,
            "max_cell_payload_bytes": config.max_cell_payload_bytes,
            "merge_fan_in": config.merge_fan_in,
            "merge_batch_rows": config.merge_batch_rows,
            "external_sort_memory_limit": config.external_sort_memory_limit,
        },
        "timings_seconds": {
            "scan": scan_seconds,
            "merge": merge_seconds,
            "encode_and_publish": encode_seconds,
            "total": time.perf_counter() - started,
        },
        "cumulative_checkpointed_timings_seconds": dict(
            state.get("cumulative_timings_seconds", {})
        ),
    }
    _atomic_write_json(config.report_path, report)
    page_work_dir = (
        config.work_dir / "pages" / f"{config.decoded_page_size}-{config.leaf_codec}"
    )
    shutil.rmtree(page_work_dir, ignore_errors=True)
    if not config.retain_work:
        shutil.rmtree(config.work_dir)
    return IdentityLocatorBuildResult(
        output_path=config.output_path,
        report_path=config.report_path,
        locator_uuid=locator_uuid,
        parent_dataset_uuid=render_header.dataset_uuid,
        identifiers_order_uuid=order_header.artifact_uuid,
        source_sha256=source_sha256,
        output_sha256=output_sha256,
        namespace_counts=namespace_counts,
        decoded_page_size=config.decoded_page_size,
        leaf_codec=config.leaf_codec,
    )
