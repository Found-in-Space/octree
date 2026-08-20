from __future__ import annotations

import hashlib
import io
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

SEMANTIC_CHECKSUM_ALGORITHM = "arrow-ipc-sha256/fixed-batches-v1"
SEMANTIC_CHECKSUM_BATCH_ROWS = 100_000


def _canonical_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema(
        [field.with_metadata(None) for field in schema],
        metadata=None,
    )


@dataclass(frozen=True, slots=True)
class ParquetGroupStats:
    row_count: int
    uncompressed_bytes: int
    schema: pa.Schema


class _HashSink(io.RawIOBase):
    def __init__(self) -> None:
        super().__init__()
        self.digest = hashlib.sha256()
        self.position = 0

    def writable(self) -> bool:
        return True

    def write(self, data: bytes | bytearray | memoryview) -> int:
        size = len(data)
        self.digest.update(data)
        self.position += size
        return size

    def tell(self) -> int:
        return self.position


class ArrowIpcChecksum:
    """Incrementally hash a logical Arrow table in fixed row batches.

    Fixed logical batches make the digest independent of parquet file, row-group,
    and input batch boundaries while keeping resident data bounded.
    """

    def __init__(
        self,
        schema: pa.Schema,
        *,
        batch_rows: int = SEMANTIC_CHECKSUM_BATCH_ROWS,
    ) -> None:
        if batch_rows <= 0:
            raise ValueError("checksum batch_rows must be > 0")
        self.schema = _canonical_schema(schema)
        self.batch_rows = batch_rows
        self.row_count = 0
        self._buffered_rows = 0
        self._pieces: list[pa.Table] = []
        self._sink = _HashSink()
        self._writer = pa_ipc.new_stream(self._sink, self.schema)
        self._finished = False

    def update(self, table: pa.Table) -> None:
        if self._finished:
            raise RuntimeError("Cannot update a finished checksum")
        if not table.schema.equals(self.schema, check_metadata=False):
            raise ValueError(
                "Semantic checksum inputs must have identical schemas; "
                "schema drift would require padding or dropping columns"
            )
        if not table.schema.equals(self.schema):
            table = table.cast(self.schema)
        offset = 0
        while offset < len(table):
            take = min(
                self.batch_rows - self._buffered_rows,
                len(table) - offset,
            )
            self._pieces.append(table.slice(offset, take))
            self._buffered_rows += take
            self.row_count += take
            offset += take
            if self._buffered_rows == self.batch_rows:
                self._flush()

    def finish(self) -> tuple[str, int]:
        if not self._finished:
            self._flush()
            self._writer.close()
            self._finished = True
        return f"sha256:{self._sink.digest.hexdigest()}", self.row_count

    def _flush(self) -> None:
        if not self._pieces:
            return
        if len(self._pieces) == 1:
            canonical = self._pieces[0].combine_chunks()
        else:
            canonical = pa.concat_tables(
                self._pieces,
                promote_options="none",
            ).combine_chunks()
        self._writer.write_table(canonical, max_chunksize=self.batch_rows)
        self._pieces = []
        self._buffered_rows = 0


def parquet_group_stats(paths: Sequence[Path]) -> ParquetGroupStats:
    if not paths:
        return ParquetGroupStats(
            row_count=0,
            uncompressed_bytes=0,
            schema=pa.schema([]),
        )

    schema: pa.Schema | None = None
    row_count = 0
    uncompressed_bytes = 0
    for path in paths:
        parquet = pq.ParquetFile(path)
        candidate = parquet.schema_arrow
        if schema is None:
            schema = _canonical_schema(candidate)
        elif not candidate.equals(schema, check_metadata=False):
            raise ValueError(
                "Parquet group fragments must have identical schemas; "
                "schema drift would require padding or dropping columns"
            )
        metadata = parquet.metadata
        row_count += metadata.num_rows
        uncompressed_bytes += sum(
            metadata.row_group(row_group_index)
            .column(column_index)
            .total_uncompressed_size
            for row_group_index in range(metadata.num_row_groups)
            for column_index in range(metadata.num_columns)
        )
    assert schema is not None
    return ParquetGroupStats(
        row_count=row_count,
        uncompressed_bytes=uncompressed_bytes,
        schema=schema,
    )


def checksum_tables(
    tables: Iterable[pa.Table],
    *,
    schema: pa.Schema,
    batch_rows: int = SEMANTIC_CHECKSUM_BATCH_ROWS,
) -> tuple[str, int]:
    checksum = ArrowIpcChecksum(schema, batch_rows=batch_rows)
    for table in tables:
        checksum.update(table)
    return checksum.finish()


def checksum_table(
    table: pa.Table,
    *,
    batch_rows: int = SEMANTIC_CHECKSUM_BATCH_ROWS,
) -> tuple[str, int]:
    return checksum_tables(
        (table,),
        schema=table.schema,
        batch_rows=batch_rows,
    )


def checksum_parquet_files(
    paths: Sequence[Path],
    *,
    batch_rows: int = SEMANTIC_CHECKSUM_BATCH_ROWS,
) -> tuple[str, int]:
    checksum: ArrowIpcChecksum | None = None
    metadata_rows = 0
    for path in paths:
        parquet = pq.ParquetFile(path)
        candidate = parquet.schema_arrow
        if checksum is None:
            checksum = ArrowIpcChecksum(candidate, batch_rows=batch_rows)
        elif not candidate.equals(checksum.schema, check_metadata=False):
            raise ValueError(
                "Parquet group fragments must have identical schemas; "
                "schema drift would require padding or dropping columns"
            )
        metadata_rows += parquet.metadata.num_rows
        for batch in parquet.iter_batches(batch_size=batch_rows):
            checksum.update(pa.Table.from_batches([batch]))

    if checksum is None:
        checksum = ArrowIpcChecksum(pa.schema([]), batch_rows=batch_rows)
    digest, row_count = checksum.finish()
    if row_count != metadata_rows:
        raise ValueError(
            "Parquet group row count changed while checksumming: "
            f"metadata={metadata_rows}, streamed={row_count}"
        )
    return digest, row_count
