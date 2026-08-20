from __future__ import annotations

from uuid import UUID

import pytest

from foundinspace.octree.packing.records import (
    DESCRIPTOR_SIZE,
    FRONTIER_REF_SIZE,
    HAS_CHILDREN,
    HAS_PAYLOAD,
    HEADER_SIZE,
    IS_FRONTIER,
    IS_TERMINAL,
    SHARD_HDR_SIZE,
    SHARD_NODE_SIZE,
    SHARD_NODE_V2_SIZE,
    PackedDescriptorFields,
    PackedHeaderFields,
    pack_brightest_level,
    pack_descriptor,
    pack_top_level_header,
    unpack_descriptor,
    unpack_top_level_header,
)


def test_record_sizes_are_pinned() -> None:
    assert HEADER_SIZE == 64
    assert DESCRIPTOR_SIZE == 128
    assert SHARD_HDR_SIZE == 80
    assert SHARD_NODE_SIZE == 20
    assert SHARD_NODE_V2_SIZE == 24
    assert FRONTIER_REF_SIZE == 8


def test_node_flag_bits_are_distinct() -> None:
    assert HAS_PAYLOAD == 0x01
    assert HAS_CHILDREN == 0x02
    assert IS_FRONTIER == 0x04
    assert IS_TERMINAL == 0x08
    assert len({HAS_PAYLOAD, HAS_CHILDREN, IS_FRONTIER, IS_TERMINAL}) == 4


def test_pack_unpack_top_level_header_round_trip() -> None:
    packed = pack_top_level_header(
        PackedHeaderFields(
            world_center=(1.0, 2.0, 3.0),
            world_half_size_pc=200_000.0,
            max_level=13,
            mag_limit=6.5,
            index_offset=1234,
            index_length=5678,
        )
    )
    assert len(packed) == HEADER_SIZE
    fields = unpack_top_level_header(packed)
    assert fields[0] == b"STAR"
    assert fields[1] == 1
    assert fields[3] == 1234
    assert fields[4] == 5678
    assert fields[9] == 16
    assert fields[10] == 13


def test_pack_v2_top_level_header() -> None:
    packed = pack_top_level_header(
        PackedHeaderFields(
            world_center=(0.0, 0.0, 0.0),
            world_half_size_pc=200_000.0,
            max_level=14,
            mag_limit=6.5,
        ),
        version=2,
    )

    assert unpack_top_level_header(packed)[1] == 2
    assert unpack_top_level_header(packed)[2] == 0


def test_brightest_level_uses_reserved_byte_exactly() -> None:
    packed = pack_brightest_level(
        level=8,
        brightest_level=21,
    )

    assert packed == 21


def test_brightest_level_rejects_a_shallower_level() -> None:
    with pytest.raises(ValueError, match="cannot be shallower"):
        pack_brightest_level(level=8, brightest_level=7)

    with pytest.raises(ValueError, match="outside the Morton address space"):
        pack_brightest_level(level=0, brightest_level=22)


def test_pack_unpack_descriptor_round_trip() -> None:
    packed = pack_descriptor(
        PackedDescriptorFields(
            artifact_kind="sidecar",
            dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            parent_dataset_uuid=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            sidecar_uuid=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
            sidecar_kind="meta",
        )
    )

    assert len(packed) == DESCRIPTOR_SIZE
    fields = unpack_descriptor(packed)
    assert fields.artifact_kind == "sidecar"
    assert fields.dataset_uuid == UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    assert fields.parent_dataset_uuid == UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    assert fields.sidecar_uuid == UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    assert fields.sidecar_kind == "meta"
