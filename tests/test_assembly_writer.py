from __future__ import annotations

import gzip

import pytest

from foundinspace.octree.assembly.formats import (
    INDEX_FILE_HDR,
    INDEX_HEADER_SIZE,
    INDEX_MAGIC,
    INDEX_RECORD,
    INDEX_VERSION,
)
from foundinspace.octree.assembly.types import CellKey, EncodedCell, ShardKey
from foundinspace.octree.assembly.writer import (
    IntermediateShardWriter,
    belongs_to_shard,
    shard_filenames,
)


def _make_cell(
    level: int, node_id: int, star_count: int = 1
) -> EncodedCell:
    raw = b"\x00" * (16 * star_count)
    payload = gzip.compress(raw)
    return EncodedCell(
        key=CellKey(level=level, node_id=node_id),
        payload=payload,
        star_count=star_count,
    )


class TestShardFilenames:
    def test_unsharded(self):
        shard = ShardKey(level=0, prefix_bits=0, prefix=0)
        assert shard_filenames(shard) == ("level-00.index", "level-00.payload")

    def test_sharded(self):
        shard = ShardKey(level=10, prefix_bits=3, prefix=5)
        assert shard_filenames(shard) == (
            "level-10-p3-5.index",
            "level-10-p3-5.payload",
        )

    def test_two_digit_level(self):
        shard = ShardKey(level=3, prefix_bits=0, prefix=0)
        assert shard_filenames(shard) == ("level-03.index", "level-03.payload")


class TestBelongsToShard:
    def test_unsharded_always_belongs(self):
        shard = ShardKey(level=5, prefix_bits=0, prefix=0)
        assert belongs_to_shard(0, shard)
        assert belongs_to_shard(12345, shard)

    def test_prefix_match(self):
        # level=2, prefix_bits=3 => node_id >> (6-3) = node_id >> 3
        shard = ShardKey(level=2, prefix_bits=3, prefix=5)
        assert belongs_to_shard(40, shard)   # 40 >> 3 = 5
        assert belongs_to_shard(47, shard)   # 47 >> 3 = 5

    def test_prefix_mismatch(self):
        shard = ShardKey(level=2, prefix_bits=3, prefix=5)
        assert not belongs_to_shard(48, shard)  # 48 >> 3 = 6


class TestIntermediateShardWriter:
    def test_write_and_close(self, tmp_path):
        shard = ShardKey(level=5, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        writer.write_cell(_make_cell(5, 10))
        writer.write_cell(_make_cell(5, 20))
        result = writer.close()

        assert result is not None
        assert result["record_count"] == 2
        assert result["level"] == 5

        index_path = tmp_path / result["index_path"]
        with open(index_path, "rb") as f:
            hdr = INDEX_FILE_HDR.unpack(f.read(INDEX_FILE_HDR.size))
            assert hdr[0] == INDEX_MAGIC
            assert hdr[1] == INDEX_VERSION
            assert hdr[2] == INDEX_HEADER_SIZE
            assert hdr[8] == 2  # record_count

            rec1 = INDEX_RECORD.unpack(f.read(INDEX_RECORD.size))
            assert rec1[0] == 10
            rec2 = INDEX_RECORD.unpack(f.read(INDEX_RECORD.size))
            assert rec2[0] == 20

    def test_payload_offsets_are_valid(self, tmp_path):
        shard = ShardKey(level=5, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        c1 = _make_cell(5, 10, star_count=3)
        c2 = _make_cell(5, 20, star_count=1)
        writer.write_cell(c1)
        writer.write_cell(c2)
        result = writer.close()

        payload_path = tmp_path / result["payload_path"]
        payload_size = payload_path.stat().st_size

        index_path = tmp_path / result["index_path"]
        with open(index_path, "rb") as f:
            f.read(INDEX_FILE_HDR.size)
            for _ in range(2):
                rec = INDEX_RECORD.unpack(f.read(INDEX_RECORD.size))
                offset, length = rec[1], rec[2]
                assert offset + length <= payload_size

    def test_empty_shard_cleanup(self, tmp_path):
        shard = ShardKey(level=3, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        result = writer.close()

        assert result is None
        assert not (tmp_path / "level-03.index").exists()
        assert not (tmp_path / "level-03.payload").exists()

    def test_level_mismatch_raises(self, tmp_path):
        shard = ShardKey(level=5, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        with pytest.raises(ValueError, match="Level mismatch"):
            writer.write_cell(_make_cell(3, 10))
        writer.abort()

    def test_non_monotonic_node_id_raises(self, tmp_path):
        shard = ShardKey(level=5, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        writer.write_cell(_make_cell(5, 20))
        with pytest.raises(ValueError, match="Non-monotonic"):
            writer.write_cell(_make_cell(5, 10))
        writer.abort()

    def test_duplicate_node_id_raises(self, tmp_path):
        shard = ShardKey(level=5, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        writer.write_cell(_make_cell(5, 10))
        with pytest.raises(ValueError, match="Non-monotonic"):
            writer.write_cell(_make_cell(5, 10))
        writer.abort()

    def test_shard_membership_raises(self, tmp_path):
        shard = ShardKey(level=2, prefix_bits=3, prefix=5)
        writer = IntermediateShardWriter(shard, tmp_path)
        with pytest.raises(ValueError, match="does not belong"):
            writer.write_cell(_make_cell(2, 0))  # 0 >> 3 = 0, not 5
        writer.abort()

    def test_abort_removes_files(self, tmp_path):
        shard = ShardKey(level=7, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        writer.write_cell(_make_cell(7, 1))
        writer.abort()

        assert not (tmp_path / "level-07.index").exists()
        assert not (tmp_path / "level-07.payload").exists()

    def test_close_idempotent(self, tmp_path):
        shard = ShardKey(level=5, prefix_bits=0, prefix=0)
        writer = IntermediateShardWriter(shard, tmp_path)
        writer.write_cell(_make_cell(5, 1))
        r1 = writer.close()
        r2 = writer.close()
        assert r1 is not None
        assert r2 is None
