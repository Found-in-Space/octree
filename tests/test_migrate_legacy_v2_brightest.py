from __future__ import annotations

import os
from pathlib import Path

import pytest

from foundinspace.octree.migrate_legacy_v2_brightest import (
    LEGACY_HEADER_FLAG,
    MigrationError,
    SaturatedLegacyDeltaError,
    migrate_legacy_v2,
    preflight_legacy_v2,
)
from foundinspace.octree.packing import PackingPlan, pack_octree
from foundinspace.octree.packing.records import (
    FRONTIER_REF_SIZE,
    HEADER_FMT,
    HEADER_SIZE,
    SHARD_HDR_FMT,
    SHARD_NODE_V2_FMT,
    SHARD_NODE_V2_SIZE,
)
from foundinspace.octree.reader import IndexNavigator, read_header
from packing_helpers import PayloadNode, build_intermediates


def _build_legacy_artifact(tmp_path: Path, *, level: int = 6) -> Path:
    manifest = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=level, node_id=0, star_count=3, raw_payload=b"payload")],
        max_level=level,
    )
    artifact = tmp_path / "legacy-v2.octree"
    pack_octree(
        manifest,
        artifact,
        plan=PackingPlan(max_open_files=2, star_format_version=2),
    )
    _rewrite_current_v2_as_legacy(artifact)
    return artifact


def _rewrite_current_v2_as_legacy(path: Path) -> None:
    with open(path, "r+b") as fp:
        header = list(HEADER_FMT.unpack(fp.read(HEADER_SIZE)))
        index_offset = int(header[3])
        index_end = index_offset + int(header[4])
        header[2] = LEGACY_HEADER_FLAG
        fp.seek(0)
        fp.write(HEADER_FMT.pack(*header))

        shard_offset = index_offset
        while shard_offset < index_end:
            fp.seek(shard_offset)
            shard = SHARD_HDR_FMT.unpack(fp.read(SHARD_HDR_FMT.size))
            node_count = int(shard[7])
            parent_depth = int(shard[9])
            first_frontier = int(shard[21])
            node_table_offset = int(shard[22])
            frontier_table_offset = int(shard[23])
            fp.seek(node_table_offset)
            nodes = bytearray(fp.read(node_count * SHARD_NODE_V2_SIZE))
            for node_index in range(node_count):
                record_offset = node_index * SHARD_NODE_V2_SIZE
                record = SHARD_NODE_V2_FMT.unpack_from(nodes, record_offset)
                level = parent_depth + int(record[3])
                flags = int(record[4])
                brightest_level = int(record[5])
                delta = brightest_level - level
                assert 0 <= delta < 15
                nodes[record_offset + 6] = flags | (delta << 4)
                nodes[record_offset + 7] = 0
            fp.seek(node_table_offset)
            fp.write(nodes)
            frontier_count = node_count - first_frontier + 1 if first_frontier else 0
            shard_offset = frontier_table_offset + frontier_count * FRONTIER_REF_SIZE
        assert shard_offset == index_end
        fp.flush()
        os.fsync(fp.fileno())


def test_preflight_does_not_modify_lossless_legacy_artifact(tmp_path: Path) -> None:
    artifact = _build_legacy_artifact(tmp_path)
    before = artifact.read_bytes()

    report = preflight_legacy_v2(artifact)

    assert artifact.read_bytes() == before
    assert report.node_count == 7
    assert report.shard_count == 2
    assert report.max_delta == 6
    assert report.new_index_offset is None


def test_migration_appends_rebased_index_and_preserves_payload(tmp_path: Path) -> None:
    artifact = _build_legacy_artifact(tmp_path)
    before = artifact.read_bytes()
    old_header = HEADER_FMT.unpack(before[:HEADER_SIZE])
    old_index_offset = int(old_header[3])
    old_index_length = int(old_header[4])

    report = migrate_legacy_v2(artifact)

    after = artifact.read_bytes()
    new_header = HEADER_FMT.unpack(after[:HEADER_SIZE])
    assert report.new_index_offset == len(before)
    assert len(after) == len(before) + old_index_length
    assert new_header[2] == 0
    assert new_header[3] == len(before)
    assert new_header[4] == old_index_length
    assert after[HEADER_SIZE:old_index_offset] == before[HEADER_SIZE:old_index_offset]
    assert (
        after[old_index_offset : old_index_offset + old_index_length]
        == before[old_index_offset:]
    )

    header = read_header(artifact)
    with IndexNavigator(artifact, header) as navigator:
        [node] = list(navigator.root_entries())
        assert node.brightest_level == 6
        assert node.flags & 0xF0 == 0
        for _level in range(1, 7):
            child = navigator.get_child(node, 0)
            assert child is not None
            node = child
            assert node.brightest_level == 6
            assert node.flags & 0xF0 == 0
    assert node.level == 6
    assert node.star_count == 3


def test_saturated_delta_aborts_before_writing(tmp_path: Path) -> None:
    artifact = _build_legacy_artifact(tmp_path)
    with open(artifact, "r+b") as fp:
        header = HEADER_FMT.unpack(fp.read(HEADER_SIZE))
        fp.seek(int(header[3]))
        shard = SHARD_HDR_FMT.unpack(fp.read(SHARD_HDR_FMT.size))
        root_flags_offset = int(shard[22]) + 6
        fp.seek(root_flags_offset)
        old_flags = fp.read(1)[0]
        fp.seek(root_flags_offset)
        fp.write(bytes([(old_flags & 0x0F) | 0xF0]))
    before = artifact.read_bytes()

    with pytest.raises(SaturatedLegacyDeltaError, match="No bytes were changed"):
        migrate_legacy_v2(artifact)

    assert artifact.read_bytes() == before


def test_migration_rolls_back_if_post_commit_validation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _build_legacy_artifact(tmp_path)
    before = artifact.read_bytes()

    def fail_validation(*_args: object, **_kwargs: object) -> None:
        raise MigrationError("forced validation failure")

    monkeypatch.setattr(
        "foundinspace.octree.migrate_legacy_v2_brightest._validate_migrated",
        fail_validation,
    )

    with pytest.raises(MigrationError, match="forced validation failure"):
        migrate_legacy_v2(artifact)

    assert artifact.read_bytes() == before


def test_migration_rejects_current_v2_artifact(tmp_path: Path) -> None:
    artifact = _build_legacy_artifact(tmp_path)
    migrate_legacy_v2(artifact)

    with pytest.raises(MigrationError, match="already zero"):
        preflight_legacy_v2(artifact)
