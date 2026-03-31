from __future__ import annotations

from uuid import UUID

from foundinspace.octree.combine.records import (
    DESCRIPTOR_SIZE,
    FRONTIER_REF_SIZE,
    HAS_CHILDREN,
    HAS_PAYLOAD,
    HEADER_SIZE,
    IS_FRONTIER,
    SHARD_HDR_SIZE,
    SHARD_NODE_SIZE,
    PackedDescriptorFields,
    PackedHeaderFields,
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
    assert FRONTIER_REF_SIZE == 8


def test_node_flag_bits_are_distinct() -> None:
    assert HAS_PAYLOAD == 0x01
    assert HAS_CHILDREN == 0x02
    assert IS_FRONTIER == 0x04
    assert len({HAS_PAYLOAD, HAS_CHILDREN, IS_FRONTIER}) == 3


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
