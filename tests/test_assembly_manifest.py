from __future__ import annotations

import gzip
import json

import pytest

from foundinspace.octree.assembly.manifest import validate_shard, write_manifest
from foundinspace.octree.config import DEFAULT_MAG_VIS
from foundinspace.octree.assembly.types import CellKey, EncodedCell, ShardKey
from foundinspace.octree.assembly.writer import IntermediateShardWriter


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


class TestValidateShard:
    def test_valid_shard(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        validate_shard(tmp_path, entry)

    def test_missing_index(self, tmp_path):
        entry = {"index_path": "missing.index", "payload_path": "missing.payload"}
        with pytest.raises(ValueError, match="Index file missing"):
            validate_shard(tmp_path, entry)

    def test_missing_payload(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        (tmp_path / entry["payload_path"]).unlink()
        with pytest.raises(ValueError, match="Payload file missing"):
            validate_shard(tmp_path, entry)

    def test_corrupt_magic(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        idx_path = tmp_path / entry["index_path"]
        data = bytearray(idx_path.read_bytes())
        data[0:4] = b"XXXX"
        idx_path.write_bytes(data)
        with pytest.raises(ValueError, match="Bad magic"):
            validate_shard(tmp_path, entry)

    def test_truncated_index(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        idx_path = tmp_path / entry["index_path"]
        idx_path.write_bytes(b"\x00" * 10)
        with pytest.raises(ValueError, match="too small"):
            validate_shard(tmp_path, entry)


class TestWriteManifest:
    def test_basic_manifest(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        manifest_path = write_manifest(
            tmp_path, max_level=13, shard_entries=[entry], mag_limit=DEFAULT_MAG_VIS
        )

        assert manifest_path.name == "manifest.json"
        data = json.loads(manifest_path.read_text())
        assert data["format"] == "three_dee.octree.intermediates/v1"
        assert data["max_level"] == 13
        assert data["mag_limit"] == DEFAULT_MAG_VIS
        assert data["payload_codec"] == "gzip"
        assert len(data["levels"]) == 1
        assert data["levels"][0]["level"] == 5
        assert len(data["levels"][0]["shards"]) == 1
        assert data["levels"][0]["shards"][0]["record_count"] == 2

    def test_multiple_levels(self, tmp_path):
        e1 = _write_test_shard(tmp_path, level=0, node_ids=(0,))
        e2 = _write_test_shard(tmp_path, level=3, node_ids=(100, 200))
        manifest_path = write_manifest(
            tmp_path, max_level=5, shard_entries=[e1, e2], mag_limit=DEFAULT_MAG_VIS
        )
        data = json.loads(manifest_path.read_text())
        assert len(data["levels"]) == 2
        assert data["levels"][0]["level"] == 0
        assert data["levels"][1]["level"] == 3

    def test_empty_entries_produces_empty_levels(self, tmp_path):
        manifest_path = write_manifest(
            tmp_path, max_level=13, shard_entries=[], mag_limit=DEFAULT_MAG_VIS
        )
        data = json.loads(manifest_path.read_text())
        assert data["levels"] == []

    def test_world_geometry_in_manifest(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        manifest_path = write_manifest(
            tmp_path, max_level=13, shard_entries=[entry], mag_limit=DEFAULT_MAG_VIS
        )
        data = json.loads(manifest_path.read_text())
        assert data["world_center"] == [0.0, 0.0, 0.0]
        assert data["world_half_size_pc"] == 200_000.0

    def test_struct_formats_in_manifest(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        manifest_path = write_manifest(
            tmp_path, max_level=13, shard_entries=[entry], mag_limit=DEFAULT_MAG_VIS
        )
        data = json.loads(manifest_path.read_text())
        assert data["index_header_struct"] == "<4sHHBBHIQQ"
        assert data["index_record_struct"] == "<QQII"

    def test_atomic_publish(self, tmp_path):
        entry = _write_test_shard(tmp_path)
        write_manifest(
            tmp_path, max_level=13, shard_entries=[entry], mag_limit=DEFAULT_MAG_VIS
        )
        assert (tmp_path / "manifest.json").exists()
        assert not (tmp_path / ".manifest.json.tmp").exists()
