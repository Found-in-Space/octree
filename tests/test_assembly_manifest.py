from __future__ import annotations

import gzip
import json

import pytest

from foundinspace.octree.assembly.formats import (
    IDENTIFIERS_ARTIFACT_KIND,
    IDENTIFIERS_INDEX_MAGIC,
    INDEX_MAGIC,
    RENDER_ARTIFACT_KIND,
)
from foundinspace.octree.assembly.manifest import (
    manifest_entries,
    validate_shard,
    write_manifest,
)
from foundinspace.octree.assembly.types import CellKey, EncodedCell, ShardKey
from foundinspace.octree.assembly.writer import (
    IntermediateShardWriter,
    identifiers_shard_filenames,
)
from foundinspace.octree.config import DEFAULT_MAG_VIS


def _write_test_shard(
    tmp_path, level: int = 5, node_ids: tuple[int, ...] = (10, 20)
) -> dict:
    shard = ShardKey(level=level, prefix_bits=0, prefix=0)
    writer = IntermediateShardWriter(shard, tmp_path)
    for nid in node_ids:
        cell = EncodedCell(
            key=CellKey(level=level, node_id=nid),
            payload=gzip.compress(b"\x00" * 16),
            star_count=1,
        )
        writer.write_cell(cell)
    result = writer.close()
    assert result is not None
    return result


def _write_identifiers_shard(
    tmp_path, level: int = 5, node_ids: tuple[int, ...] = (10, 20)
) -> dict:
    shard = ShardKey(level=level, prefix_bits=0, prefix=0)
    writer = IntermediateShardWriter(
        shard,
        tmp_path,
        index_magic=IDENTIFIERS_INDEX_MAGIC,
        filename_fn=identifiers_shard_filenames,
    )
    for nid in node_ids:
        writer.write_cell(
            EncodedCell(
                key=CellKey(level=level, node_id=nid),
                payload=gzip.compress(b"ident"),
                star_count=1,
            )
        )
    result = writer.close()
    assert result is not None
    return result


class TestValidateShard:
    def test_valid_render_shard(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        validate_shard(tmp_path, entry, expected_magic=INDEX_MAGIC)

    def test_valid_identifiers_shard(self, tmp_path):
        entry = _write_identifiers_shard(tmp_path)
        validate_shard(tmp_path, entry, expected_magic=IDENTIFIERS_INDEX_MAGIC)

    def test_missing_index(self, tmp_path):
        entry = {"index_path": "missing.index", "payload_path": "missing.payload"}
        with pytest.raises(ValueError, match="Index file missing"):
            validate_shard(tmp_path, entry, expected_magic=INDEX_MAGIC)


class TestWriteManifest:
    def test_basic_render_manifest(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        manifest_path = write_manifest(
            tmp_path,
            max_level=13,
            shard_entries=[entry],
            artifact_kind=RENDER_ARTIFACT_KIND,
            index_magic=INDEX_MAGIC,
            mag_limit=DEFAULT_MAG_VIS,
        )

        assert manifest_path.name == "manifest.json"
        data = json.loads(manifest_path.read_text())
        assert data["format"] == "three_dee.octree.intermediates/v1"
        assert data["artifact_kind"] == RENDER_ARTIFACT_KIND
        assert data["index_magic"] == "OIDX"
        assert data["max_level"] == 13
        assert data["mag_limit"] == DEFAULT_MAG_VIS

    def test_basic_identifiers_manifest(self, tmp_path):
        entry = _write_identifiers_shard(tmp_path)
        manifest_path = write_manifest(
            tmp_path,
            max_level=13,
            shard_entries=[entry],
            artifact_kind=IDENTIFIERS_ARTIFACT_KIND,
            index_magic=IDENTIFIERS_INDEX_MAGIC,
            mag_limit=DEFAULT_MAG_VIS,
            name="identifiers-manifest.json",
        )
        data = json.loads(manifest_path.read_text())
        assert data["artifact_kind"] == IDENTIFIERS_ARTIFACT_KIND
        assert data["index_magic"] == IDENTIFIERS_INDEX_MAGIC.decode("ascii")

    def test_manifest_roundtrip(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        write_manifest(
            tmp_path,
            max_level=13,
            shard_entries=[entry],
            artifact_kind=RENDER_ARTIFACT_KIND,
            index_magic=INDEX_MAGIC,
            mag_limit=DEFAULT_MAG_VIS,
        )
        data = json.loads((tmp_path / "manifest.json").read_text())
        entries = manifest_entries(data)
        assert entries[0]["index_path"] == data["levels"][0]["shards"][0]["index_path"]
