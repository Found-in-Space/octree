from __future__ import annotations

import heapq
import io
import os
import shutil
import tempfile
from collections.abc import Callable, Iterator, Sequence
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..duckdb_util import (
    MEMORY_LIMIT,
    PRESERVE_INSERTION_ORDER,
    TEMP_DIR,
    configure_connection,
)

CellKey = tuple[int, int]
KeyedBatch = tuple[CellKey, pa.Table]
BatchIterator = Callable[
    [Sequence[Path], int, Path],
    Iterator[KeyedBatch],
]
RunWriter = Callable[[Sequence[Path], Path, int], None]


@dataclass(frozen=True, slots=True)
class SortedRunLayout:
    """Logical contract for a profile's canonical materialization runs."""

    schema: pa.Schema
    cell_level_column: str
    cell_node_column: str
    overlap_sort_keys: tuple[tuple[str, str], ...]
    contributor_column: str = "_materialization_contributor_index"
    contributor_row_column: str = "_materialization_contributor_row"

    def __post_init__(self) -> None:
        names = set(self.schema.names)
        required = {
            self.cell_level_column,
            self.cell_node_column,
            *(name for name, _order in self.overlap_sort_keys),
        }
        missing = sorted(required - names)
        if missing:
            raise ValueError(f"Sorted run layout schema is missing columns: {missing}")
        if self.contributor_column in names or self.contributor_row_column in names:
            raise ValueError("Sorted run layout reserves its contributor columns")
        for _name, order in self.overlap_sort_keys:
            if order not in {"ascending", "descending"}:
                raise ValueError(f"Unsupported sorted run order: {order!r}")

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self.schema.names)

    @property
    def overlap_schema(self) -> pa.Schema:
        return self.schema.append(
            pa.field(self.contributor_column, pa.int32(), nullable=False)
        ).append(pa.field(self.contributor_row_column, pa.int64(), nullable=False))

    @property
    def tagged_overlap_sort_keys(self) -> tuple[tuple[str, str], ...]:
        return (
            *self.overlap_sort_keys,
            (self.contributor_column, "ascending"),
            (self.contributor_row_column, "ascending"),
        )


@dataclass(frozen=True, slots=True)
class RunMergeBounds:
    """Operational bounds that do not affect canonical row order."""

    overlap_in_memory_max_bytes: int = 256 * 1024 * 1024
    external_sort_memory_limit: str = "512MB"
    write_max_bytes: int = 256 * 1024 * 1024
    write_max_pieces: int = 1024

    def __post_init__(self) -> None:
        if self.overlap_in_memory_max_bytes <= 0:
            raise ValueError("overlap_in_memory_max_bytes must be > 0")
        if not self.external_sort_memory_limit:
            raise ValueError("external_sort_memory_limit must not be empty")
        if self.write_max_bytes <= 0:
            raise ValueError("write_max_bytes must be > 0")
        if self.write_max_pieces <= 0:
            raise ValueError("write_max_pieces must be > 0")


class _RunCursor:
    def __init__(
        self,
        path: Path,
        *,
        batch_size: int,
        layout: SortedRunLayout,
    ) -> None:
        self._layout = layout
        parquet = pq.ParquetFile(path)
        self._batches = iter(
            parquet.iter_batches(
                batch_size=batch_size,
                columns=list(layout.columns),
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
    def cell_key(self) -> CellKey:
        table = self._require_table()
        return (
            int(table.column(self._layout.cell_level_column)[self._offset].as_py()),
            int(table.column(self._layout.cell_node_column)[self._offset].as_py()),
        )

    def take_cell_chunk(self, key: CellKey) -> pa.Table:
        if self.cell_key != key:
            raise ValueError(
                f"Sorted run cursor is at cell {self.cell_key}, requested {key}"
            )
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
            table = pa.Table.from_batches([batch])
            if not table.schema.equals(self._layout.schema, check_metadata=False):
                table = table.cast(self._layout.schema)
            self._table = table.replace_schema_metadata(None)
            levels = np.asarray(
                self._table.column(self._layout.cell_level_column),
                dtype=np.int64,
            )
            nodes = np.asarray(
                self._table.column(self._layout.cell_node_column),
                dtype=np.uint64,
            )
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


def reduce_sorted_runs(
    paths: Sequence[Path],
    *,
    partition_dir: Path,
    batch_size: int,
    fan_in: int,
    layout: SortedRunLayout,
    bounds: RunMergeBounds | None = None,
    write_run: RunWriter | None = None,
) -> list[Path]:
    """Reduce sorted runs until a final merge can open them with bounded fan-in."""
    _validate_batch_size(batch_size)
    if fan_in < 2:
        raise ValueError("Sorted run merge fan_in must be >= 2")
    partition_dir.mkdir(parents=True, exist_ok=True)
    merge_bounds = bounds or RunMergeBounds()

    if write_run is None:

        def write_run(source: Sequence[Path], output: Path, rows: int) -> None:
            write_merged_run(
                source,
                output,
                batch_size=rows,
                layout=layout,
                bounds=merge_bounds,
            )

    current = list(paths)
    generated: set[Path] = set()
    round_index = 0
    try:
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
                write_run(chunk, output, batch_size)
                next_round.append(output)
                generated.add(output)
            for path in current:
                if path in generated and path not in next_round:
                    path.unlink(missing_ok=True)
            current = next_round
            round_index += 1
    except Exception:
        for path in generated:
            path.unlink(missing_ok=True)
        raise
    return current


def write_merged_run(
    paths: Sequence[Path],
    output: Path,
    *,
    batch_size: int,
    layout: SortedRunLayout,
    bounds: RunMergeBounds | None = None,
    merge_batches: BatchIterator | None = None,
) -> None:
    """Atomically write canonical merged runs with row and byte buffer bounds."""
    _validate_batch_size(batch_size)
    if not paths:
        raise ValueError("Writing a merged run requires at least one input")
    merge_bounds = bounds or RunMergeBounds()
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output.with_name(f".{output.name}.tmp")
    writer = pq.ParquetWriter(
        tmp_path,
        layout.schema,
        compression="zstd",
        write_statistics=False,
    )
    buffered: list[pa.Table] = []
    buffered_rows = 0
    buffered_bytes = 0

    def flush() -> None:
        nonlocal buffered, buffered_rows, buffered_bytes
        if not buffered:
            return
        table = pa.concat_tables(buffered, promote_options="none")
        writer.write_table(table, row_group_size=batch_size)
        buffered = []
        buffered_rows = 0
        buffered_bytes = 0

    def compact_pieces() -> None:
        nonlocal buffered, buffered_bytes
        table = pa.concat_tables(buffered, promote_options="none").combine_chunks()
        buffered = [table]
        buffered_bytes = table.nbytes

    if merge_batches is None:

        def merge_batches(
            source: Sequence[Path], rows: int, spill: Path
        ) -> Iterator[KeyedBatch]:
            return iter_merged_batches(
                source,
                batch_size=rows,
                spill_dir=spill,
                layout=layout,
                bounds=merge_bounds,
            )

    try:
        for _key, batch in merge_batches(paths, batch_size, output.parent):
            offset = 0
            while offset < len(batch):
                if buffered_rows >= batch_size:
                    flush()
                row_room = batch_size - buffered_rows
                piece_rows = min(row_room, len(batch) - offset)
                piece = batch.slice(offset, piece_rows)
                while len(piece) > 1 and piece.nbytes > merge_bounds.write_max_bytes:
                    piece_rows = max(1, piece_rows // 2)
                    piece = batch.slice(offset, piece_rows)
                if buffered and (
                    buffered_bytes + piece.nbytes > merge_bounds.write_max_bytes
                ):
                    flush()
                    continue
                buffered.append(piece)
                buffered_rows += len(piece)
                buffered_bytes += piece.nbytes
                offset += len(piece)
                if len(buffered) >= merge_bounds.write_max_pieces:
                    compact_pieces()
                if (
                    buffered_rows >= batch_size
                    or buffered_bytes >= merge_bounds.write_max_bytes
                ):
                    flush()
        flush()
    except Exception:
        writer.close()
        tmp_path.unlink(missing_ok=True)
        raise
    writer.close()
    os.replace(tmp_path, output)


def iter_merged_batches(
    paths: Sequence[Path],
    *,
    batch_size: int,
    spill_dir: Path,
    layout: SortedRunLayout,
    bounds: RunMergeBounds | None = None,
    external_overlap_sort: Callable[..., Iterator[KeyedBatch]] | None = None,
) -> Iterator[KeyedBatch]:
    """Merge canonical runs by cell, sorting only cells with multiple contributors."""
    _validate_batch_size(batch_size)
    if not paths:
        return
    merge_bounds = bounds or RunMergeBounds()
    spill_dir.mkdir(parents=True, exist_ok=True)
    cursor_batch_size = max(1, batch_size // len(paths))
    cursors = [
        _RunCursor(path, batch_size=cursor_batch_size, layout=layout) for path in paths
    ]
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
                layout=layout,
                bounds=merge_bounds,
                external_overlap_sort=external_overlap_sort,
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
    key: CellKey,
    batch_size: int,
    spill_dir: Path,
    layout: SortedRunLayout,
    bounds: RunMergeBounds,
    external_overlap_sort: Callable[..., Iterator[KeyedBatch]] | None,
) -> Iterator[KeyedBatch]:
    buffered: list[pa.Table] = []
    buffered_rows = 0
    buffered_bytes = 0
    temporary_dir: tempfile.TemporaryDirectory[str] | None = None
    spill_path: Path | None = None
    spill_writer: pq.ParquetWriter | None = None

    def start_spilling() -> None:
        nonlocal temporary_dir, spill_path, spill_writer
        temporary_dir = tempfile.TemporaryDirectory(
            prefix=".materialization-overlap-sort-",
            dir=spill_dir,
        )
        spill_path = Path(temporary_dir.name) / "cell.parquet"
        spill_writer = pq.ParquetWriter(
            spill_path,
            layout.overlap_schema,
            compression="zstd",
            write_statistics=False,
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
                    layout=layout,
                )
                contributor_row += len(tagged)
                if spill_writer is None and (
                    buffered_rows + len(tagged) > batch_size
                    or buffered_bytes + tagged.nbytes
                    > bounds.overlap_in_memory_max_bytes
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
                layout=layout,
            )
            return

        spill_writer.close()
        spill_writer = None
        assert spill_path is not None
        sorter = external_overlap_sort or _iter_externally_sorted_overlap
        yield from sorter(
            spill_path,
            key=key,
            batch_size=batch_size,
            layout=layout,
            bounds=bounds,
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
    layout: SortedRunLayout,
) -> pa.Table:
    row_count = len(chunk)
    return chunk.append_column(
        pa.field(layout.contributor_column, pa.int32(), nullable=False),
        pa.array(
            np.full(row_count, contributor_index, dtype=np.int32),
            type=pa.int32(),
        ),
    ).append_column(
        pa.field(layout.contributor_row_column, pa.int64(), nullable=False),
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
    key: CellKey,
    batch_size: int,
    layout: SortedRunLayout,
) -> Iterator[KeyedBatch]:
    combined = pa.concat_tables(chunks, promote_options="none")
    order = pc.sort_indices(
        combined,
        sort_keys=layout.tagged_overlap_sort_keys,
        null_placement="at_end",
    )
    sorted_table = combined.take(order).select(layout.columns)
    for offset in range(0, len(sorted_table), batch_size):
        yield key, sorted_table.slice(offset, batch_size)


def _iter_externally_sorted_overlap(
    spill_path: Path,
    *,
    key: CellKey,
    batch_size: int,
    layout: SortedRunLayout,
    bounds: RunMergeBounds,
) -> Iterator[KeyedBatch]:
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
            con.execute("SET memory_limit = ?", [bounds.external_sort_memory_limit])
        if PRESERVE_INSERTION_ORDER is None:
            con.execute("SET preserve_insertion_order = false")
        con.execute(_overlap_sort_query(spill_path, layout=layout))
        for batch in con.to_arrow_reader(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            if not table.schema.equals(layout.schema, check_metadata=False):
                table = table.cast(layout.schema)
            yield key, table.replace_schema_metadata(None)
    finally:
        con.close()
        if local_spill_dir is not None:
            shutil.rmtree(local_spill_dir, ignore_errors=True)


def _overlap_sort_query(path: Path, *, layout: SortedRunLayout) -> str:
    escaped = path.as_posix().replace("'", "''")
    selected = ",\n            ".join(
        _quote_identifier(name) for name in layout.columns
    )
    ordered = ",\n            ".join(
        f"{_quote_identifier(name)} {'ASC' if order == 'ascending' else 'DESC'} "
        "NULLS LAST"
        for name, order in layout.tagged_overlap_sort_keys
    )
    return f"""
        SELECT
            {selected}
        FROM read_parquet(
            '{escaped}',
            hive_partitioning = false,
            union_by_name = false
        )
        ORDER BY
            {ordered}
    """


def _quote_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _validate_batch_size(batch_size: int) -> None:
    if batch_size <= 0:
        raise ValueError("Sorted run merge batch_size must be > 0")
