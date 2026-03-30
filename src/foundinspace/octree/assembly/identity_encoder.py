from __future__ import annotations

import gzip
import struct
from collections.abc import Iterator

from .encoder import _flush_cell
from .types import EncodedCell

_LEN_FMT = struct.Struct("<H")


def encode_identity_rows(identities: list[tuple[str, str]]) -> bytes:
    raw = bytearray()
    for source, source_id in identities:
        source_bytes = str(source).encode("utf-8")
        source_id_bytes = str(source_id).encode("utf-8")
        raw.extend(_LEN_FMT.pack(len(source_bytes)))
        raw.extend(source_bytes)
        raw.extend(_LEN_FMT.pack(len(source_id_bytes)))
        raw.extend(source_id_bytes)
    return gzip.compress(bytes(raw))


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
