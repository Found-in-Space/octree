"""Metadata sidecar encoding (gzip JSON per cell). See docs/sidecars.md."""

from __future__ import annotations

import gzip
import io
import json
import math
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any, BinaryIO

import pyarrow.parquet as pq

from .encoder import _flush_cell
from .types import EncodedCell

ALL_IDENTIFIER_FIELDS = [
    "gaia_source_id",
    "hip_id",
    "hd",
    "bayer",
    "flamsteed",
    "constellation",
    "proper_name",
]
INTEGER_FIELDS = frozenset({"gaia_source_id", "hip_id", "hd", "flamsteed"})
STRING_FIELDS = frozenset({"bayer", "constellation", "proper_name"})
_PARQUET_BATCH_SIZE = 16_384
_JSON_SEPARATORS = (",", ":")
IDENTIFIERS_MEMORY_LIMIT_BYTES = 64 * 1024 * 1024
_IDENTIFIER_ENTRY_OVERHEAD_BYTES = 256


def _is_empty_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return isinstance(value, str) and value.strip() == ""


def _ordered_identifier_entry(
    row: Mapping[str, object],
    use_set: frozenset[str],
) -> dict[str, Any]:
    """Fields appear in ALL_IDENTIFIER_FIELDS order (deterministic JSON keys)."""
    out: dict[str, Any] = {}
    for name in ALL_IDENTIFIER_FIELDS:
        if name not in use_set or name not in row:
            continue
        val = row[name]
        if _is_empty_value(val):
            continue
        if name in INTEGER_FIELDS:
            try:
                out[name] = int(val)
            except (TypeError, ValueError):
                continue
        elif name in STRING_FIELDS:
            out[name] = str(val).strip()
        else:
            out[name] = val
    return out


class IdentifiersMap:
    """Bounded lookup keyed by ``(source, source_id)``.

    Small enrichment maps stay in memory so the much larger identity stream can
    be joined with direct hash lookups. If the conservative memory estimate
    reaches ``memory_limit_bytes``, existing entries are promoted to a private
    disk-backed SQLite index and subsequent input remains streaming.
    """

    def __init__(
        self,
        parquet_path: Path,
        *,
        fields: list[str] | None = None,
        memory_limit_bytes: int = IDENTIFIERS_MEMORY_LIMIT_BYTES,
    ) -> None:
        path = Path(parquet_path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"Identifiers map not found: {path}")
        if memory_limit_bytes < 0:
            raise ValueError("memory_limit_bytes must be >= 0")

        use_list = list(ALL_IDENTIFIER_FIELDS if fields is None else fields)
        unknown = set(use_list) - set(ALL_IDENTIFIER_FIELDS)
        if unknown:
            raise ValueError(f"Unknown sidecar identifier field(s): {sorted(unknown)}")
        use_set = frozenset(use_list)

        parquet = pq.ParquetFile(path)
        for col in ("source", "source_id"):
            if col not in parquet.schema_arrow.names:
                raise ValueError(f"Identifiers map missing required column: {col}")

        available_fields = [
            field
            for field in ALL_IDENTIFIER_FIELDS
            if field in use_set and field in parquet.schema_arrow.names
        ]
        read_columns = ["source", "source_id", *available_fields]

        self._connection: sqlite3.Connection | None = None
        self._entries: dict[tuple[str, str], str] | None = {}
        self._estimated_memory_bytes = 0
        self._closed = False
        try:
            for batch in parquet.iter_batches(
                batch_size=_PARQUET_BATCH_SIZE,
                columns=read_columns,
                use_threads=False,
            ):
                encoded_rows = [
                    (
                        str(row["source"]),
                        str(row["source_id"]),
                        json.dumps(
                            _ordered_identifier_entry(row, use_set),
                            separators=_JSON_SEPARATORS,
                        ),
                    )
                    for row in batch.to_pylist()
                ]
                self._add_encoded_rows(
                    encoded_rows,
                    memory_limit_bytes=memory_limit_bytes,
                )
            if self._entries is not None:
                self._length = len(self._entries)
                self._backend = "memory"
            else:
                connection = self._require_connection()
                self._length = connection.execute(
                    "SELECT COUNT(*) FROM identifiers"
                ).fetchone()[0]
                self._backend = "sqlite"
        except BaseException:
            self.close()
            raise

    def __len__(self) -> int:
        return self._length

    @property
    def backend(self) -> str:
        """Backend selected after bounded ingestion: ``memory`` or ``sqlite``."""
        return self._backend

    @property
    def estimated_memory_bytes(self) -> int:
        return self._estimated_memory_bytes

    def lookup(self, source: str, source_id: str) -> dict[str, Any]:
        self._require_open()
        normalized_source = str(source)
        normalized_source_id = str(source_id)
        if self._entries is not None:
            fields_json = self._entries.get(
                (normalized_source, normalized_source_id),
                "{}",
            )
            return json.loads(fields_json)
        row = (
            self._require_connection()
            .execute(
                """
            SELECT fields_json
            FROM identifiers
            WHERE source = ? AND source_id = ?
            """,
                (normalized_source, normalized_source_id),
            )
            .fetchone()
        )
        if row is None:
            return {}
        return json.loads(row[0])

    def iter_json_entries(
        self,
        identities: Iterable[tuple[str, str]],
    ) -> Iterator[tuple[str, str, str]]:
        """Yield normalized identities and enrichment JSON in input order."""
        self._require_open()
        if self._entries is not None:
            for source, source_id in identities:
                normalized_source = str(source)
                normalized_source_id = str(source_id)
                yield (
                    normalized_source,
                    normalized_source_id,
                    self._entries.get(
                        (normalized_source, normalized_source_id),
                        "{}",
                    ),
                )
            return

        connection = self._require_connection()
        connection.execute("DELETE FROM requested_identifiers")
        connection.executemany(
            """
            INSERT INTO requested_identifiers (ordinal, source, source_id)
            VALUES (?, ?, ?)
            """,
            (
                (ordinal, str(source), str(source_id))
                for ordinal, (source, source_id) in enumerate(identities)
            ),
        )
        cursor = connection.execute(
            """
            SELECT requested.source,
                   requested.source_id,
                   COALESCE(identifiers.fields_json, '{}')
            FROM requested_identifiers AS requested
            LEFT JOIN identifiers
              ON identifiers.source = requested.source
             AND identifiers.source_id = requested.source_id
            ORDER BY requested.ordinal
            """
        )
        try:
            yield from cursor
        finally:
            cursor.close()

    def close(self) -> None:
        self._closed = True
        self._entries = None
        connection = getattr(self, "_connection", None)
        if connection is not None:
            self._connection = None
            connection.close()

    def __enter__(self) -> IdentifiersMap:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        # Destructors may run during interpreter shutdown.
        with suppress(Exception):
            self.close()

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("IdentifiersMap is closed")
        return self._connection

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("IdentifiersMap is closed")

    def _add_encoded_rows(
        self,
        rows: list[tuple[str, str, str]],
        *,
        memory_limit_bytes: int,
    ) -> None:
        insert_from = 0
        if self._entries is not None:
            for index, (source, source_id, fields_json) in enumerate(rows):
                key = (source, source_id)
                old_json = self._entries.get(key)
                old_bytes = (
                    0
                    if old_json is None
                    else _estimated_identifier_entry_bytes(source, source_id, old_json)
                )
                new_bytes = _estimated_identifier_entry_bytes(
                    source,
                    source_id,
                    fields_json,
                )
                projected = self._estimated_memory_bytes - old_bytes + new_bytes
                if projected > memory_limit_bytes:
                    self._promote_to_sqlite()
                    insert_from = index
                    break
                self._entries[key] = fields_json
                self._estimated_memory_bytes = projected
            else:
                return

        connection = self._require_connection()
        connection.executemany(
            """
            INSERT OR REPLACE INTO identifiers (source, source_id, fields_json)
            VALUES (?, ?, ?)
            """,
            rows[insert_from:],
        )
        connection.commit()

    def _promote_to_sqlite(self) -> None:
        entries = self._entries
        if entries is None:
            return
        # An empty database filename asks SQLite for a private, disk-backed
        # temporary database that is deleted automatically when closed.
        connection = sqlite3.connect("")
        self._connection = connection
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute("PRAGMA temp_store = FILE")
        connection.execute("PRAGMA cache_size = -8192")
        connection.execute(
            """
            CREATE TABLE identifiers (
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                fields_json TEXT NOT NULL,
                PRIMARY KEY (source, source_id)
            ) WITHOUT ROWID
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE requested_identifiers (
                ordinal INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                source_id TEXT NOT NULL
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO identifiers (source, source_id, fields_json)
            VALUES (?, ?, ?)
            """,
            (
                (source, source_id, fields_json)
                for (source, source_id), fields_json in entries.items()
            ),
        )
        connection.commit()
        self._entries = None
        self._estimated_memory_bytes = 0


def _estimated_identifier_entry_bytes(
    source: str,
    source_id: str,
    fields_json: str,
) -> int:
    return (
        _IDENTIFIER_ENTRY_OVERHEAD_BYTES
        + len(source.encode("utf-8"))
        + len(source_id.encode("utf-8"))
        + len(fields_json.encode("utf-8"))
    )


def write_meta_payload(
    identities: Iterable[tuple[str, str]],
    ident_map: IdentifiersMap,
    target: BinaryIO,
) -> None:
    """Stream one gzip JSON cell payload to ``target``."""
    with gzip.GzipFile(fileobj=target, mode="wb", mtime=0) as compressed:
        compressed.write(b"[")
        for ordinal, (source, source_id, fields_json) in enumerate(
            ident_map.iter_json_entries(identities)
        ):
            if ordinal:
                compressed.write(b",")
            identity_json = json.dumps(
                {"source": source, "source_id": source_id},
                separators=_JSON_SEPARATORS,
            )
            if fields_json == "{}":
                compressed.write(identity_json.encode("utf-8"))
            else:
                compressed.write(identity_json[:-1].encode("utf-8"))
                compressed.write(b",")
                compressed.write(fields_json[1:].encode("utf-8"))
        compressed.write(b"]")


def build_meta_payload(
    identities: list[tuple[str, str]],
    ident_map: IdentifiersMap,
) -> bytes:
    """gzip(JSON array of per-star objects) in cell order."""
    target = io.BytesIO()
    write_meta_payload(identities, ident_map, target)
    return target.getvalue()


def iter_encoded_cells_with_meta(
    rows: Iterator[tuple[int, bytes, str, str]],
    level: int,
    ident_map: IdentifiersMap,
) -> Iterator[tuple[EncodedCell, bytes]]:
    """Like ``iter_encoded_cells`` but also emit gzip JSON meta blob per cell."""
    current_node_id: int | None = None
    current_renders: list[bytes] = []
    current_identities: list[tuple[str, str]] = []

    for node_id, render, source, source_id in rows:
        if current_node_id is not None and node_id != current_node_id:
            render_cell = _flush_cell(level, current_node_id, current_renders)
            meta_blob = build_meta_payload(current_identities, ident_map)
            yield render_cell, meta_blob
            current_renders = []
            current_identities = []
        current_node_id = node_id
        current_renders.append(render)
        current_identities.append((source, source_id))

    if current_node_id is not None and current_renders:
        render_cell = _flush_cell(level, current_node_id, current_renders)
        meta_blob = build_meta_payload(current_identities, ident_map)
        yield render_cell, meta_blob
