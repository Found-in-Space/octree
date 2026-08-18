"""Compact locator-v1 numeric leaf encoding."""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass

import numpy as np

LEAF_MAGIC = b"OILD"
LEAF_VERSION = 1
KEY_BLOCK_SIZE = 32
CHECKPOINT_OFFSET_BYTES = 2
LEAF_HEADER_FMT = struct.Struct("<4sBBBBBBBHHI1x")
LEAF_HEADER_SIZE = LEAF_HEADER_FMT.size
LEAF_DTYPE = np.dtype(
    [("source_id", "<u8"), ("cell_record", "<u4"), ("ordinal", "<u4")]
)

assert LEAF_HEADER_SIZE == 20


@dataclass(frozen=True, slots=True)
class CompactLeafLayout:
    entry_count: int
    key_shift: int
    key_width: int
    cell_width: int
    cell_index_width: int
    ordinal_width: int
    dictionary_count: int
    block_count: int
    delta_length: int
    table_start: int
    dictionary_start: int
    cell_indexes_start: int
    ordinals_start: int
    deltas_start: int


def _byte_width(value: int) -> int:
    return max(1, math.ceil(int(value).bit_length() / 8))


def _pack_fixed(values: np.ndarray, width: int) -> bytes:
    values = values.astype(np.uint64, copy=False)
    if width < 1 or width > 8:
        raise ValueError("Compact identity leaf fixed width must be in 1..8")
    if len(values) and int(values.max()) >= 1 << (width * 8):
        raise ValueError("Compact identity leaf value exceeds its fixed width")
    out = np.empty((len(values), width), dtype=np.uint8)
    for byte in range(width):
        out[:, byte] = ((values >> np.uint64(byte * 8)) & np.uint64(0xFF)).astype(
            np.uint8
        )
    return out.tobytes()


def _unpack_fixed(body: bytes, start: int, count: int, width: int) -> np.ndarray:
    raw = np.frombuffer(body, dtype=np.uint8, count=count * width, offset=start)
    raw = raw.reshape(count, width)
    values = np.zeros(count, dtype=np.uint64)
    for byte in range(width):
        values |= raw[:, byte].astype(np.uint64) << np.uint64(byte * 8)
    return values


def _read_fixed(body: bytes, offset: int, width: int) -> int:
    return int.from_bytes(body[offset : offset + width], "little")


def _pack_bits(values: np.ndarray, width: int) -> bytes:
    values = values.astype(np.uint64, copy=False)
    if width < 1 or width > 64:
        raise ValueError("Compact identity leaf bit width must be in 1..64")
    if len(values) and width < 64 and int(values.max()) >= 1 << width:
        raise ValueError("Compact identity leaf value exceeds its bit width")
    shifts = np.arange(width, dtype=np.uint64)
    bits = ((values[:, None] >> shifts) & np.uint64(1)).astype(np.uint8)
    return np.packbits(bits.reshape(-1), bitorder="little").tobytes()


def _unpack_bits(body: bytes, start: int, count: int, width: int) -> np.ndarray:
    byte_count = math.ceil(count * width / 8)
    packed = np.frombuffer(body, dtype=np.uint8, count=byte_count, offset=start)
    bits = np.unpackbits(packed, bitorder="little", count=count * width)
    bits = bits.reshape(count, width).astype(np.uint64)
    weights = np.left_shift(np.uint64(1), np.arange(width, dtype=np.uint64))
    return bits @ weights


def _read_bits(body: bytes, start: int, width: int, index: int) -> int:
    bit_offset = index * width
    byte_offset, intra = divmod(bit_offset, 8)
    byte_count = math.ceil((intra + width) / 8)
    word = int.from_bytes(
        body[start + byte_offset : start + byte_offset + byte_count], "little"
    )
    return (word >> intra) & ((1 << width) - 1)


def _append_uvarint(out: bytearray, value: int) -> None:
    if value <= 0:
        raise ValueError("Compact identity leaf deltas must be positive")
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)


def _read_uvarint(body: bytes, cursor: int, end: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while cursor < end:
        byte = body[cursor]
        cursor += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            if value <= 0:
                raise ValueError("Compact identity leaf delta is not positive")
            return value, cursor
        shift += 7
        if shift >= 70:
            raise ValueError("Compact identity leaf varint exceeds uint64")
    raise ValueError("Compact identity leaf varint is truncated")


def parse_compact_leaf(body: bytes, *, entry_count: int) -> CompactLeafLayout:
    if entry_count <= 0:
        raise ValueError("Compact identity leaf pages must not be empty")
    if len(body) < LEAF_HEADER_SIZE:
        raise ValueError("Compact identity leaf header is truncated")
    (
        magic,
        version,
        key_shift,
        key_width,
        cell_width,
        cell_index_width,
        ordinal_width,
        block_size,
        dictionary_count,
        block_count,
        delta_length,
    ) = LEAF_HEADER_FMT.unpack_from(body)
    if magic != LEAF_MAGIC or version != LEAF_VERSION:
        raise ValueError("Unsupported compact identity leaf")
    if block_size != KEY_BLOCK_SIZE:
        raise ValueError("Unsupported compact identity leaf key block size")
    if key_shift > 63:
        raise ValueError("Compact identity leaf key shift exceeds 63")
    if key_width < 1 or key_width > 8:
        raise ValueError("Compact identity leaf key width must be in 1..8")
    if cell_width < 1 or cell_width > 4:
        raise ValueError("Compact identity leaf cell width must be in 1..4")
    if cell_index_width < 1 or cell_index_width > 16:
        raise ValueError("Compact identity leaf cell index width is invalid")
    if ordinal_width < 1 or ordinal_width > 32:
        raise ValueError("Compact identity leaf ordinal width is invalid")
    if dictionary_count < 1 or dictionary_count > entry_count:
        raise ValueError("Compact identity leaf dictionary count is invalid")
    expected_blocks = math.ceil(entry_count / KEY_BLOCK_SIZE)
    if block_count != expected_blocks:
        raise ValueError("Compact identity leaf block count differs")
    if dictionary_count > 1 << cell_index_width:
        raise ValueError("Compact identity leaf cell index width is too small")

    table_start = LEAF_HEADER_SIZE
    table_end = table_start + block_count * (key_width + CHECKPOINT_OFFSET_BYTES)
    dictionary_start = table_end
    cell_indexes_start = dictionary_start + dictionary_count * cell_width
    ordinals_start = cell_indexes_start + math.ceil(entry_count * cell_index_width / 8)
    deltas_start = ordinals_start + math.ceil(entry_count * ordinal_width / 8)
    expected_length = deltas_start + delta_length
    if expected_length != len(body):
        raise ValueError("Compact identity leaf body length differs")
    if delta_length >= 1 << (CHECKPOINT_OFFSET_BYTES * 8):
        raise ValueError("Compact identity leaf delta stream is too large")
    return CompactLeafLayout(
        entry_count=entry_count,
        key_shift=key_shift,
        key_width=key_width,
        cell_width=cell_width,
        cell_index_width=cell_index_width,
        ordinal_width=ordinal_width,
        dictionary_count=dictionary_count,
        block_count=block_count,
        delta_length=delta_length,
        table_start=table_start,
        dictionary_start=dictionary_start,
        cell_indexes_start=cell_indexes_start,
        ordinals_start=ordinals_start,
        deltas_start=deltas_start,
    )


def encode_compact_leaf(records: np.ndarray, *, key_shift: int) -> bytes:
    records = np.asarray(records, dtype=LEAF_DTYPE)
    if not len(records):
        raise ValueError("Cannot encode an empty compact identity leaf")
    keys = records["source_id"]
    if len(keys) > 1 and np.any(keys[1:] <= keys[:-1]):
        raise ValueError("Compact identity leaf keys are not increasing")
    if key_shift < 0 or key_shift > 63:
        raise ValueError("Compact identity leaf key shift must be in 0..63")
    if key_shift:
        low_mask = np.uint64((1 << key_shift) - 1)
        if np.any((keys & low_mask) != 0):
            raise ValueError("Identity source IDs violate their compact key shift")
    shifted_keys = keys >> np.uint64(key_shift)
    key_width = _byte_width(int(shifted_keys[-1]))

    dictionary, inverse = np.unique(records["cell_record"], return_inverse=True)
    dictionary = dictionary.astype(np.uint64, copy=False)
    cell_width = _byte_width(int(dictionary[-1]))
    cell_index_width = max(1, (len(dictionary) - 1).bit_length())
    ordinal_width = max(1, int(records["ordinal"].max()).bit_length())

    table = bytearray()
    deltas = bytearray()
    for start in range(0, len(records), KEY_BLOCK_SIZE):
        table.extend(int(shifted_keys[start]).to_bytes(key_width, "little"))
        if len(deltas) >= 1 << (CHECKPOINT_OFFSET_BYTES * 8):
            raise ValueError("Compact identity leaf delta stream exceeds uint16")
        table.extend(len(deltas).to_bytes(CHECKPOINT_OFFSET_BYTES, "little"))
        previous = int(shifted_keys[start])
        for raw_value in shifted_keys[start + 1 : start + KEY_BLOCK_SIZE]:
            value = int(raw_value)
            _append_uvarint(deltas, value - previous)
            previous = value
    if len(deltas) >= 1 << (CHECKPOINT_OFFSET_BYTES * 8):
        raise ValueError("Compact identity leaf delta stream exceeds uint16")

    header = LEAF_HEADER_FMT.pack(
        LEAF_MAGIC,
        LEAF_VERSION,
        key_shift,
        key_width,
        cell_width,
        cell_index_width,
        ordinal_width,
        KEY_BLOCK_SIZE,
        len(dictionary),
        math.ceil(len(records) / KEY_BLOCK_SIZE),
        len(deltas),
    )
    return b"".join(
        (
            header,
            bytes(table),
            _pack_fixed(dictionary, cell_width),
            _pack_bits(inverse, cell_index_width),
            _pack_bits(records["ordinal"], ordinal_width),
            bytes(deltas),
        )
    )


def _checkpoint_offset(body: bytes, layout: CompactLeafLayout, block: int) -> int:
    table_entry_size = layout.key_width + CHECKPOINT_OFFSET_BYTES
    return _read_fixed(
        body,
        layout.table_start + block * table_entry_size + layout.key_width,
        CHECKPOINT_OFFSET_BYTES,
    )


def _decode_keys(body: bytes, layout: CompactLeafLayout) -> np.ndarray:
    keys = np.empty(layout.entry_count, dtype=np.uint64)
    table_entry_size = layout.key_width + CHECKPOINT_OFFSET_BYTES
    previous_checkpoint: int | None = None
    previous_delta_offset = -1
    shifted_limit = (2**64 - 1) >> layout.key_shift
    for block in range(layout.block_count):
        start = block * KEY_BLOCK_SIZE
        stop = min(layout.entry_count, start + KEY_BLOCK_SIZE)
        checkpoint = _read_fixed(
            body,
            layout.table_start + block * table_entry_size,
            layout.key_width,
        )
        if checkpoint > shifted_limit:
            raise ValueError("Compact identity leaf key exceeds uint64")
        if previous_checkpoint is not None and checkpoint <= previous_checkpoint:
            raise ValueError("Compact identity leaf checkpoints are not increasing")
        previous_checkpoint = checkpoint
        delta_offset = _checkpoint_offset(body, layout, block)
        if delta_offset < previous_delta_offset or delta_offset > layout.delta_length:
            raise ValueError("Compact identity leaf checkpoint offset is invalid")
        previous_delta_offset = delta_offset
        next_delta_offset = (
            _checkpoint_offset(body, layout, block + 1)
            if block + 1 < layout.block_count
            else layout.delta_length
        )
        if next_delta_offset < delta_offset or next_delta_offset > layout.delta_length:
            raise ValueError("Compact identity leaf checkpoint range is invalid")
        cursor = layout.deltas_start + delta_offset
        end = layout.deltas_start + next_delta_offset
        keys[start] = np.uint64(checkpoint << layout.key_shift)
        value = checkpoint
        for index in range(start + 1, stop):
            delta, cursor = _read_uvarint(body, cursor, end)
            value += delta
            if value > shifted_limit:
                raise ValueError("Compact identity leaf key exceeds uint64")
            keys[index] = np.uint64(value << layout.key_shift)
        if cursor != end:
            raise ValueError("Compact identity leaf block has trailing deltas")
    if len(keys) > 1 and np.any(keys[1:] <= keys[:-1]):
        raise ValueError("Compact identity leaf keys are not increasing")
    return keys


def decode_compact_leaf(body: bytes, *, entry_count: int) -> np.ndarray:
    layout = parse_compact_leaf(body, entry_count=entry_count)
    dictionary = _unpack_fixed(
        body,
        layout.dictionary_start,
        layout.dictionary_count,
        layout.cell_width,
    )
    if len(dictionary) > 1 and np.any(dictionary[1:] <= dictionary[:-1]):
        raise ValueError("Compact identity leaf dictionary is not increasing")
    cell_indexes = _unpack_bits(
        body,
        layout.cell_indexes_start,
        layout.entry_count,
        layout.cell_index_width,
    )
    if np.any(cell_indexes >= layout.dictionary_count):
        raise ValueError("Compact identity leaf cell index exceeds its dictionary")
    ordinals = _unpack_bits(
        body,
        layout.ordinals_start,
        layout.entry_count,
        layout.ordinal_width,
    )
    if np.any(ordinals > np.iinfo(np.uint32).max):
        raise ValueError("Compact identity leaf ordinal exceeds uint32")
    records = np.empty(layout.entry_count, dtype=LEAF_DTYPE)
    records["source_id"] = _decode_keys(body, layout)
    records["cell_record"] = dictionary[cell_indexes].astype(np.uint32)
    records["ordinal"] = ordinals.astype(np.uint32)
    return records


def lookup_compact_leaf(
    body: bytes,
    *,
    entry_count: int,
    source_id: int,
) -> tuple[int, int] | None:
    layout = parse_compact_leaf(body, entry_count=entry_count)
    if source_id < 0 or source_id > 2**64 - 1:
        raise ValueError("Identity source ID is outside uint64")
    if layout.key_shift and source_id & ((1 << layout.key_shift) - 1):
        return None
    shifted_target = source_id >> layout.key_shift
    table_entry_size = layout.key_width + CHECKPOINT_OFFSET_BYTES
    lo, hi = 0, layout.block_count
    while lo < hi:
        middle = (lo + hi) // 2
        checkpoint = _read_fixed(
            body,
            layout.table_start + middle * table_entry_size,
            layout.key_width,
        )
        if checkpoint <= shifted_target:
            lo = middle + 1
        else:
            hi = middle
    block = lo - 1
    if block < 0:
        return None
    checkpoint = _read_fixed(
        body,
        layout.table_start + block * table_entry_size,
        layout.key_width,
    )
    rank = block * KEY_BLOCK_SIZE
    if checkpoint != shifted_target:
        cursor = layout.deltas_start + _checkpoint_offset(body, layout, block)
        end = layout.deltas_start + (
            _checkpoint_offset(body, layout, block + 1)
            if block + 1 < layout.block_count
            else layout.delta_length
        )
        value = checkpoint
        stop = min(layout.entry_count, rank + KEY_BLOCK_SIZE)
        rank += 1
        while cursor < end and rank < stop:
            delta, cursor = _read_uvarint(body, cursor, end)
            value += delta
            if value >= shifted_target:
                if value != shifted_target:
                    return None
                break
            rank += 1
        else:
            return None

    cell_index = _read_bits(
        body,
        layout.cell_indexes_start,
        layout.cell_index_width,
        rank,
    )
    if cell_index >= layout.dictionary_count:
        raise ValueError("Compact identity leaf cell index exceeds its dictionary")
    cell_record = _read_fixed(
        body,
        layout.dictionary_start + cell_index * layout.cell_width,
        layout.cell_width,
    )
    ordinal = _read_bits(body, layout.ordinals_start, layout.ordinal_width, rank)
    return cell_record, ordinal
