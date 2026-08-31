from __future__ import annotations

import os
import re
import struct
import tempfile
import time
import urllib.request
from collections.abc import Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from .identifiers_order import IdentifiersOrderReader

_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")

HEADER_STRUCT = struct.Struct("<4sHHIQQQQQQQ")
TOP_RECORD_STRUCT = struct.Struct("<c3xII")
SHARD_RECORD_STRUCT = struct.Struct("<2sHII")
BLOCK_RECORD_STRUCT = struct.Struct("<QQII16s16s")

MAGIC = b"OIBF"
VERSION = 1
TOP_BUCKETS = "_0123456789abcdefghijklmnopqrstuvwxyz"

TYPE_FLAGS = {
    "gaia": 1,
    "gaia_source_id": 1,
    "hip": 2,
    "hip_id": 2,
    "hd": 3,
    "proper_name": 4,
    "name": 4,
}


@dataclass(frozen=True, slots=True)
class IdentifierRecord:
    term: str
    flag: int
    level: int
    node_id: int
    ordinal: int


@dataclass(frozen=True, slots=True)
class BigfileHeader:
    magic: bytes
    version: int
    header_size: int
    target_block_bytes: int
    top_count: int
    shard_count: int
    block_count: int
    top_offset: int
    shard_offset: int
    block_index_offset: int
    blocks_offset: int


@dataclass(frozen=True, slots=True)
class QueryMatch:
    term: str
    flag: int
    level: int
    node_id: int
    ordinal: int


@dataclass(frozen=True, slots=True)
class QueryStats:
    requests: int
    bytes_fetched: int
    elapsed_ms: float


def normalize_identifier(value: object) -> str:
    text = str(value).strip().lower()
    if not text:
        return ""
    return _NORMALIZE_RE.sub("", text)


def _shard_key(term: str) -> bytes:
    padded = (term + "__")[:2]
    safe = "".join(ch if ch in TOP_BUCKETS else "_" for ch in padded)
    return safe.encode("ascii")


def _encode_block(records: list[IdentifierRecord]) -> bytes:
    offsets: list[int] = []
    payload = bytearray()
    for record in records:
        offsets.append(len(payload))
        term_raw = record.term.encode("utf-8")
        payload.extend(struct.pack("<H", len(term_raw)))
        payload.extend(term_raw)
        payload.extend(
            struct.pack(
                "<BHQI",
                int(record.flag),
                int(record.level),
                int(record.node_id),
                int(record.ordinal),
            )
        )
    out = bytearray(struct.pack("<I", len(records)))
    out.extend(struct.pack(f"<{len(offsets)}I", *offsets))
    out.extend(payload)
    return bytes(out)


def _build_resolved_lookup(identifiers_map_path: Path) -> dict[tuple[str, str], list[tuple[int, str]]]:
    table = pq.read_table(identifiers_map_path)
    lookup: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for row in table.to_pylist():
        source = str(row.get("source", ""))
        source_id = str(row.get("source_id", ""))
        key = (source, source_id)
        values: list[tuple[int, str]] = []
        for field in ("gaia_source_id", "hip_id", "hd", "proper_name"):
            raw = row.get(field)
            if raw is None:
                continue
            normalized = normalize_identifier(raw)
            if not normalized:
                continue
            values.append((TYPE_FLAGS.get(field, 0), normalized))
        if values:
            lookup[key] = values
    return lookup


def _iter_identifier_records(
    order_path: Path,
    resolved_lookup: dict[tuple[str, str], list[tuple[int, str]]],
) -> Iterator[IdentifierRecord]:
    with IdentifiersOrderReader(order_path) as reader:
        for record, identities in reader.iter_cells():
            for ordinal, (source, source_id) in enumerate(identities):
                normalized_source_id = normalize_identifier(source_id)
                if normalized_source_id:
                    yield IdentifierRecord(
                        term=normalized_source_id,
                        flag=TYPE_FLAGS.get(source, 0),
                        level=record.level,
                        node_id=record.node_id,
                        ordinal=ordinal,
                    )
                for flag, resolved in resolved_lookup.get((source, source_id), []):
                    yield IdentifierRecord(
                        term=resolved,
                        flag=flag,
                        level=record.level,
                        node_id=record.node_id,
                        ordinal=ordinal,
                    )


def _record_storage_size(record: IdentifierRecord) -> int:
    return 2 + len(record.term.encode("utf-8")) + 1 + 2 + 8 + 4


class _RangeReader:
    def __init__(self) -> None:
        self.requests = 0
        self.bytes_fetched = 0

    def read(self, offset: int, length: int) -> bytes:
        raise NotImplementedError


class _LocalRangeReader(_RangeReader):
    def __init__(self, path: Path):
        super().__init__()
        self._fp = open(path, "rb")  # noqa: SIM115

    def close(self) -> None:
        self._fp.close()

    def read(self, offset: int, length: int) -> bytes:
        self.requests += 1
        self._fp.seek(offset)
        data = self._fp.read(length)
        self.bytes_fetched += len(data)
        return data


class _HttpRangeReader(_RangeReader):
    def __init__(self, url: str):
        super().__init__()
        self._url = url

    def read(self, offset: int, length: int) -> bytes:
        self.requests += 1
        req = urllib.request.Request(
            self._url,
            headers={"Range": f"bytes={offset}-{offset + length - 1}"},
        )
        with urllib.request.urlopen(req) as response:  # noqa: S310
            data = response.read()
        self.bytes_fetched += len(data)
        return data


def _parse_header(raw: bytes) -> BigfileHeader:
    unpacked = HEADER_STRUCT.unpack(raw)
    header = BigfileHeader(
        magic=unpacked[0],
        version=unpacked[1],
        header_size=unpacked[2],
        target_block_bytes=unpacked[3],
        top_count=unpacked[4],
        shard_count=unpacked[5],
        block_count=unpacked[6],
        top_offset=unpacked[7],
        shard_offset=unpacked[8],
        block_index_offset=unpacked[9],
        blocks_offset=unpacked[10],
    )
    if header.magic != MAGIC:
        raise ValueError(f"Invalid identifier bigfile magic: {header.magic!r}")
    if header.version != VERSION:
        raise ValueError(f"Unsupported identifier bigfile version: {header.version}")
    return header


def _decode_prefix(raw: bytes) -> str:
    return raw.rstrip(b"\0").decode("utf-8")


def _decode_block(data: bytes) -> list[QueryMatch]:
    count = struct.unpack_from("<I", data, 0)[0]
    offsets = struct.unpack_from(f"<{count}I", data, 4)
    base = 4 + count * 4
    out: list[QueryMatch] = []
    for rel in offsets:
        pos = base + rel
        term_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        term = data[pos : pos + term_len].decode("utf-8")
        pos += term_len
        flag, level, node_id, ordinal = struct.unpack_from("<BHQI", data, pos)
        out.append(
            QueryMatch(
                term=term,
                flag=flag,
                level=level,
                node_id=node_id,
                ordinal=ordinal,
            )
        )
    return out


def query_identifier_bigfile(
    *,
    query: str,
    path: Path | None = None,
    url: str | None = None,
    limit: int = 50,
    exact: bool = False,
) -> tuple[list[QueryMatch], QueryStats]:
    if (path is None and url is None) or (path is not None and url is not None):
        raise ValueError("Provide exactly one of path or url")
    needle = normalize_identifier(query)
    if not needle:
        return [], QueryStats(requests=0, bytes_fetched=0, elapsed_ms=0.0)

    reader: _RangeReader
    local_reader: _LocalRangeReader | None = None
    if path is not None:
        local_reader = _LocalRangeReader(path)
        reader = local_reader
    else:
        reader = _HttpRangeReader(str(url))

    started = time.perf_counter()
    try:
        header = _parse_header(reader.read(0, HEADER_STRUCT.size))
        top_raw = reader.read(header.top_offset, header.top_count * TOP_RECORD_STRUCT.size)
        top_map: dict[str, tuple[int, int]] = {}
        for idx in range(header.top_count):
            ch, shard_start, shard_count = TOP_RECORD_STRUCT.unpack_from(
                top_raw, idx * TOP_RECORD_STRUCT.size
            )
            top_map[ch.decode("ascii")] = (shard_start, shard_count)

        bucket = needle[0] if needle and needle[0] in TOP_BUCKETS else "_"
        shard_start, shard_count = top_map.get(bucket, (0, 0))
        if shard_count == 0:
            elapsed = (time.perf_counter() - started) * 1000.0
            return [], QueryStats(reader.requests, reader.bytes_fetched, elapsed)

        shard_raw = reader.read(
            header.shard_offset + shard_start * SHARD_RECORD_STRUCT.size,
            shard_count * SHARD_RECORD_STRUCT.size,
        )
        shard_records: list[tuple[str, int, int]] = []
        for idx in range(shard_count):
            prefix_raw, _reserved, block_start, block_count = SHARD_RECORD_STRUCT.unpack_from(
                shard_raw, idx * SHARD_RECORD_STRUCT.size
            )
            shard_records.append((prefix_raw.decode("ascii"), block_start, block_count))

        query_prefix = (needle + "__")[:2]
        target_shards = [row for row in shard_records if row[0] == query_prefix]
        if len(needle) < 2:
            target_shards = shard_records

        matches: list[QueryMatch] = []
        for _prefix, block_start, block_count in target_shards:
            if block_count == 0:
                continue
            block_index_raw = reader.read(
                header.block_index_offset + block_start * BLOCK_RECORD_STRUCT.size,
                block_count * BLOCK_RECORD_STRUCT.size,
            )
            for idx in range(block_count):
                (
                    block_offset,
                    block_len,
                    _entry_count,
                    _reserved,
                    first_key,
                    last_key,
                ) = BLOCK_RECORD_STRUCT.unpack_from(
                    block_index_raw, idx * BLOCK_RECORD_STRUCT.size
                )
                first_term = _decode_prefix(first_key)
                last_term = _decode_prefix(last_key)
                if needle < first_term and not first_term.startswith(needle):
                    continue
                if needle > last_term and not last_term.startswith(needle):
                    continue
                block_raw = reader.read(header.blocks_offset + block_offset, block_len)
                for row in _decode_block(block_raw):
                    if exact:
                        if row.term == needle:
                            matches.append(row)
                    elif row.term.startswith(needle):
                        matches.append(row)
                    if len(matches) >= limit:
                        elapsed = (time.perf_counter() - started) * 1000.0
                        return matches, QueryStats(
                            reader.requests,
                            reader.bytes_fetched,
                            elapsed,
                        )
        elapsed = (time.perf_counter() - started) * 1000.0
        return matches, QueryStats(reader.requests, reader.bytes_fetched, elapsed)
    finally:
        if local_reader is not None:
            local_reader.close()


def build_identifier_bigfile(
    *,
    identifiers_order_path: Path,
    identifiers_map_path: Path,
    output_path: Path,
    target_block_bytes: int = 256 * 1024,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_lookup = _build_resolved_lookup(identifiers_map_path)

    with tempfile.TemporaryDirectory(prefix="identifier-bigfile-") as temp_dir:
        temp_root = Path(temp_dir)
        shard_paths: dict[bytes, Path] = {}
        with ExitStack() as stack:
            shard_writers: dict[bytes, object] = {}
            for item in _iter_identifier_records(identifiers_order_path, resolved_lookup):
                shard = _shard_key(item.term)
                if shard not in shard_paths:
                    shard_paths[shard] = temp_root / f"{shard.decode('ascii')}.txt"
                if shard not in shard_writers:
                    shard_writers[shard] = stack.enter_context(
                        open(shard_paths[shard], "a", encoding="utf-8")
                    )
                shard_writers[shard].write(
                    f"{item.term}\t{item.flag}\t{item.level}\t{item.node_id}\t{item.ordinal}\n"
                )

        top_ranges = {bucket: [0, 0] for bucket in TOP_BUCKETS}
        shard_records: list[tuple[bytes, int, int]] = []
        block_records: list[tuple[int, int, int, bytes, bytes]] = []
        blocks = bytearray()

        for shard_key in sorted(shard_paths):
            lines = shard_paths[shard_key].read_text(encoding="utf-8").splitlines()
            parsed: list[IdentifierRecord] = []
            for line in lines:
                term, flag, level, node_id, ordinal = line.split("\t")
                parsed.append(
                    IdentifierRecord(
                        term=term,
                        flag=int(flag),
                        level=int(level),
                        node_id=int(node_id),
                        ordinal=int(ordinal),
                    )
                )
            parsed.sort(key=lambda r: (r.term, r.level, r.node_id, r.ordinal, r.flag))

            shard_block_start = len(block_records)
            block_entries: list[IdentifierRecord] = []
            block_storage_size = 0
            current_term = ""
            for entry in parsed:
                record_size = _record_storage_size(entry)
                projected = 4 + (len(block_entries) + 1) * 4 + block_storage_size + record_size
                if (
                    block_entries
                    and projected >= target_block_bytes
                    and entry.term != current_term
                ):
                    block_offset = len(blocks)
                    block_blob = _encode_block(block_entries)
                    blocks.extend(block_blob)
                    block_records.append(
                        (
                            block_offset,
                            len(block_blob),
                            len(block_entries),
                            block_entries[0].term.encode("utf-8")[:16].ljust(16, b"\0"),
                            block_entries[-1].term.encode("utf-8")[:16].ljust(16, b"\0"),
                        )
                    )
                    block_entries = []
                    block_storage_size = 0
                block_entries.append(entry)
                block_storage_size += record_size
                current_term = entry.term

            if block_entries:
                block_offset = len(blocks)
                block_blob = _encode_block(block_entries)
                blocks.extend(block_blob)
                block_records.append(
                    (
                        block_offset,
                        len(block_blob),
                        len(block_entries),
                        block_entries[0].term.encode("utf-8")[:16].ljust(16, b"\0"),
                        block_entries[-1].term.encode("utf-8")[:16].ljust(16, b"\0"),
                    )
                )

            shard_block_count = len(block_records) - shard_block_start
            shard_records.append((shard_key, 0, shard_block_count))

        for bucket, span in top_ranges.items():
            first = None
            count = 0
            for shard_idx, (shard, _block_start, _block_count) in enumerate(shard_records):
                if shard.decode("ascii")[0] != bucket:
                    continue
                if first is None:
                    first = shard_idx
                count += 1
            span[0] = 0 if first is None else first
            span[1] = count

        block_cursor = 0
        concrete_shards: list[tuple[bytes, int, int]] = []
        for shard_key, _unused, block_count in shard_records:
            concrete_shards.append((shard_key, block_cursor, block_count))
            block_cursor += block_count

        header_size = HEADER_STRUCT.size
        top_offset = header_size
        top_size = len(TOP_BUCKETS) * TOP_RECORD_STRUCT.size
        shard_offset = top_offset + top_size
        shard_size = len(concrete_shards) * SHARD_RECORD_STRUCT.size
        block_index_offset = shard_offset + shard_size
        block_index_size = len(block_records) * BLOCK_RECORD_STRUCT.size
        blocks_offset = block_index_offset + block_index_size

        tmp_path = output_path.with_name(f".{output_path.name}.tmp")
        with open(tmp_path, "wb") as fp:
            fp.write(
                HEADER_STRUCT.pack(
                    MAGIC,
                    VERSION,
                    header_size,
                    target_block_bytes,
                    len(TOP_BUCKETS),
                    len(concrete_shards),
                    len(block_records),
                    top_offset,
                    shard_offset,
                    block_index_offset,
                    blocks_offset,
                )
            )
            for bucket in TOP_BUCKETS:
                shard_start, shard_count = top_ranges[bucket]
                fp.write(
                    TOP_RECORD_STRUCT.pack(
                        bucket.encode("ascii"),
                        int(shard_start),
                        int(shard_count),
                    )
                )
            for shard_key, block_start, block_count in concrete_shards:
                fp.write(
                    SHARD_RECORD_STRUCT.pack(
                        shard_key,
                        0,
                        int(block_start),
                        int(block_count),
                    )
                )
            for block_offset, block_length, entry_count, first_key, last_key in block_records:
                fp.write(
                    BLOCK_RECORD_STRUCT.pack(
                        int(block_offset),
                        int(block_length),
                        int(entry_count),
                        0,
                        first_key,
                        last_key,
                    )
                )
            fp.write(blocks)
        os.replace(tmp_path, output_path)

    return output_path
