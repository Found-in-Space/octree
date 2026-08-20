from __future__ import annotations

import numpy as np
import pytest

from foundinspace.octree.identity_locator.leaf import (
    KEY_BLOCK_SIZE,
    LEAF_DTYPE,
    decode_compact_leaf,
    encode_compact_leaf,
    lookup_compact_leaf,
    parse_compact_leaf,
)


def _records(count: int) -> np.ndarray:
    records = np.empty(count, dtype=LEAF_DTYPE)
    records["source_id"] = (np.arange(count, dtype=np.uint64) * 3 + 1) * 128
    records["cell_record"] = np.asarray(
        [(index * 7) % 11 for index in range(count)],
        dtype=np.uint32,
    )
    records["ordinal"] = np.asarray(
        [(index * 13) % 37 for index in range(count)],
        dtype=np.uint32,
    )
    return records


@pytest.mark.parametrize("count", [1, 31, 32, 33, 65])
def test_compact_leaf_round_trip_across_block_boundaries(count: int) -> None:
    records = _records(count)
    encoded = encode_compact_leaf(records, key_shift=7)

    layout = parse_compact_leaf(encoded, entry_count=count)
    decoded = decode_compact_leaf(encoded, entry_count=count)

    assert layout.block_count == (count + KEY_BLOCK_SIZE - 1) // KEY_BLOCK_SIZE
    assert decoded.tobytes() == records.tobytes()
    for record in records:
        assert lookup_compact_leaf(
            encoded,
            entry_count=count,
            source_id=int(record["source_id"]),
        ) == (int(record["cell_record"]), int(record["ordinal"]))


def test_compact_leaf_absent_keys_and_noncanonical_low_bits() -> None:
    records = _records(40)
    encoded = encode_compact_leaf(records, key_shift=7)

    assert lookup_compact_leaf(encoded, entry_count=40, source_id=0) is None
    assert lookup_compact_leaf(encoded, entry_count=40, source_id=256) is None
    assert lookup_compact_leaf(encoded, entry_count=40, source_id=129) is None
    assert lookup_compact_leaf(encoded, entry_count=40, source_id=2**64 - 1) is None


def test_compact_leaf_preserves_uint_boundaries() -> None:
    records = np.array(
        [
            (0, 0, 0),
            (2**64 - 128, 2**32 - 1, 2**32 - 1),
        ],
        dtype=LEAF_DTYPE,
    )
    encoded = encode_compact_leaf(records, key_shift=7)

    assert decode_compact_leaf(encoded, entry_count=2).tobytes() == records.tobytes()
    assert lookup_compact_leaf(
        encoded,
        entry_count=2,
        source_id=2**64 - 128,
    ) == (2**32 - 1, 2**32 - 1)


def test_compact_leaf_is_byte_deterministic() -> None:
    records = _records(65)

    assert encode_compact_leaf(records, key_shift=7) == encode_compact_leaf(
        records,
        key_shift=7,
    )


def test_compact_leaf_rejects_invalid_input() -> None:
    records = _records(2)
    descending = records[::-1].copy()
    unshifted = records.copy()
    unshifted["source_id"][0] += 1

    with pytest.raises(ValueError, match="not increasing"):
        encode_compact_leaf(descending, key_shift=7)
    with pytest.raises(ValueError, match="compact key shift"):
        encode_compact_leaf(unshifted, key_shift=7)


def test_compact_leaf_rejects_corrupt_body() -> None:
    encoded = bytearray(encode_compact_leaf(_records(2), key_shift=7))
    layout = parse_compact_leaf(encoded, entry_count=2)

    bad_magic = bytearray(encoded)
    bad_magic[0] ^= 0xFF
    with pytest.raises(ValueError, match="Unsupported compact"):
        decode_compact_leaf(bytes(bad_magic), entry_count=2)

    zero_delta = bytearray(encoded)
    zero_delta[layout.deltas_start] = 0
    with pytest.raises(ValueError, match="not positive"):
        decode_compact_leaf(bytes(zero_delta), entry_count=2)

    with pytest.raises(ValueError, match="length differs"):
        decode_compact_leaf(bytes(encoded[:-1]), entry_count=2)
