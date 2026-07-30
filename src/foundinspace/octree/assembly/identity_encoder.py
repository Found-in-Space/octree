from __future__ import annotations

import gzip
import struct
from collections.abc import Iterable, Iterator
from typing import BinaryIO

import pyarrow as pa

from .encoder import _flush_cell
from .types import EncodedCell

_LEN_FMT = struct.Struct("<H")


def encode_identity_rows(identities: list[tuple[str, str]]) -> bytes:
    return encode_identity_columns(
        (source for source, _source_id in identities),
        (source_id for _source, source_id in identities),
    )


def encode_identity_columns(
    sources: Iterable[str],
    source_ids: Iterable[str],
) -> bytes:
    raw = bytearray()
    for chunk in _identity_chunks(sources, source_ids):
        raw.extend(chunk)
    return gzip.compress(bytes(raw), mtime=0)


def write_identity_columns(
    output: BinaryIO,
    sources: Iterable[str],
    source_ids: Iterable[str],
) -> None:
    for chunk in _identity_chunks(sources, source_ids):
        output.write(chunk)


def write_identity_arrays(
    output: BinaryIO,
    sources: pa.Array,
    source_ids: pa.Array,
) -> None:
    """Write Arrow UTF-8 columns without creating one Python string per row."""
    if len(sources) != len(source_ids):
        raise ValueError("Identity source columns must have the same length")
    if sources.null_count or source_ids.null_count:
        raise ValueError("Identity source columns must not contain nulls")
    source_offsets, source_data = _utf8_buffers(sources)
    id_offsets, id_data = _utf8_buffers(source_ids)
    source_base = sources.offset
    id_base = source_ids.offset
    row_count = len(sources)
    raw_size = (
        2 * _LEN_FMT.size * row_count
        + source_offsets[source_base + row_count]
        - source_offsets[source_base]
        + id_offsets[id_base + row_count]
        - id_offsets[id_base]
    )
    raw = bytearray(raw_size)
    raw_view = memoryview(raw)
    offset = 0
    for index in range(row_count):
        source_start = source_offsets[source_base + index]
        source_end = source_offsets[source_base + index + 1]
        source_len = source_end - source_start
        id_start = id_offsets[id_base + index]
        id_end = id_offsets[id_base + index + 1]
        id_len = id_end - id_start
        if source_len > 0xFFFF or id_len > 0xFFFF:
            raise ValueError("Identity value exceeds the uint16 encoded length")
        _LEN_FMT.pack_into(raw, offset, source_len)
        offset += _LEN_FMT.size
        raw_view[offset : offset + source_len] = source_data[source_start:source_end]
        offset += source_len
        _LEN_FMT.pack_into(raw, offset, id_len)
        offset += _LEN_FMT.size
        raw_view[offset : offset + id_len] = id_data[id_start:id_end]
        offset += id_len
    output.write(raw)


def _utf8_buffers(values: pa.Array) -> tuple[memoryview, memoryview]:
    if pa.types.is_string(values.type):
        offset_format = "i"
    elif pa.types.is_large_string(values.type):
        offset_format = "q"
    else:
        raise TypeError(f"Identity column must be UTF-8, got {values.type}")
    _validity, offsets, data = values.buffers()
    if offsets is None:
        raise ValueError("Identity UTF-8 column has no offsets buffer")
    return (
        memoryview(offsets).cast(offset_format),
        memoryview(data).cast("B") if data is not None else memoryview(b""),
    )


def _identity_chunks(
    sources: Iterable[str],
    source_ids: Iterable[str],
) -> Iterator[bytes]:
    for source, source_id in zip(sources, source_ids, strict=True):
        source_bytes = str(source).encode("utf-8")
        source_id_bytes = str(source_id).encode("utf-8")
        yield _LEN_FMT.pack(len(source_bytes))
        yield source_bytes
        yield _LEN_FMT.pack(len(source_id_bytes))
        yield source_id_bytes


def decode_identity_rows(raw: bytes, *, star_count: int) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    offset = 0
    for _ in range(star_count):
        if offset + _LEN_FMT.size > len(raw):
            raise ValueError("Identity payload truncated reading source length")
        (source_len,) = _LEN_FMT.unpack_from(raw, offset)
        offset += _LEN_FMT.size
        end = offset + source_len
        if end > len(raw):
            raise ValueError("Identity payload truncated reading source bytes")
        source = raw[offset:end].decode("utf-8")
        offset = end

        if offset + _LEN_FMT.size > len(raw):
            raise ValueError("Identity payload truncated reading source_id length")
        (source_id_len,) = _LEN_FMT.unpack_from(raw, offset)
        offset += _LEN_FMT.size
        end = offset + source_id_len
        if end > len(raw):
            raise ValueError("Identity payload truncated reading source_id bytes")
        source_id = raw[offset:end].decode("utf-8")
        offset = end
        out.append((source, source_id))
    if offset != len(raw):
        raise ValueError("Identity payload has trailing bytes")
    return out


def decode_identity_blob(blob: bytes, *, star_count: int) -> list[tuple[str, str]]:
    return decode_identity_rows(gzip.decompress(blob), star_count=star_count)


def iter_encoded_cells_with_identities(
    rows: Iterator[tuple[int, bytes, str, str]],
    level: int,
) -> Iterator[tuple[EncodedCell, EncodedCell]]:
    current_node_id: int | None = None
    current_renders: list[bytes] = []
    current_identities: list[tuple[str, str]] = []

    for node_id, render, source, source_id in rows:
        if current_node_id is not None and node_id != current_node_id:
            render_cell = _flush_cell(level, current_node_id, current_renders)
            yield (
                render_cell,
                EncodedCell(
                    key=render_cell.key,
                    payload=encode_identity_rows(current_identities),
                    star_count=len(current_identities),
                ),
            )
            current_renders = []
            current_identities = []
        current_node_id = node_id
        current_renders.append(render)
        current_identities.append((source, source_id))

    if current_node_id is not None and current_renders:
        render_cell = _flush_cell(level, current_node_id, current_renders)
        yield (
            render_cell,
            EncodedCell(
                key=render_cell.key,
                payload=encode_identity_rows(current_identities),
                star_count=len(current_identities),
            ),
        )
