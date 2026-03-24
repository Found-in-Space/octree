from __future__ import annotations

import gzip
import json

import pytest

from foundinspace.octree.assembly.build import build_intermediates
from foundinspace.octree.assembly.manifest import write_manifest
from foundinspace.octree.assembly.plan import BuildPlan
from foundinspace.octree.assembly.types import CellKey, EncodedCell, ShardKey
from foundinspace.octree.assembly.writer import IntermediateShardWriter


def _make_entry(tmp_path, *, level: int, prefix_bits: int, prefix: int) -> dict:
    shard = ShardKey(level=level, prefix_bits=prefix_bits, prefix=prefix)
    writer = IntermediateShardWriter(shard, tmp_path)
    writer.write_cell(
        EncodedCell(
            key=CellKey(level=level, node_id=0),
            payload=gzip.compress(b"\x00" * 16),
            star_count=1,
        )
    )
    entry = writer.close()
    assert entry is not None
    return entry


def test_non_empty_out_dir_without_manifest_fails(tmp_path, monkeypatch):
    (tmp_path / "junk.txt").write_text("junk")
    monkeypatch.setattr(
        "foundinspace.octree.assembly.build._check_input_columns",
        lambda _glob: None,
    )
    plan = BuildPlan(
        max_level=0,
        deep_shard_from_level=1,
        deep_prefix_bits=3,
        batch_size=100,
    )
    with pytest.raises(FileExistsError, match="has no manifest"):
        build_intermediates("unused/*.parquet", tmp_path, plan=plan)


def test_resume_skips_completed_shards(tmp_path, monkeypatch):
    entry = _make_entry(tmp_path, level=0, prefix_bits=0, prefix=0)
    write_manifest(tmp_path, max_level=0, shard_entries=[entry], mag_limit=6.5)

    monkeypatch.setattr(
        "foundinspace.octree.assembly.build._check_input_columns",
        lambda _glob: None,
    )

    def fail_iter_rows(*_args, **_kwargs):
        raise AssertionError("iter_sorted_rows should not be called for skipped shard")

    monkeypatch.setattr(
        "foundinspace.octree.assembly.build.iter_sorted_rows",
        fail_iter_rows,
    )

    plan = BuildPlan(
        max_level=0,
        deep_shard_from_level=1,
        deep_prefix_bits=3,
        batch_size=100,
    )
    manifest_path = build_intermediates("unused/*.parquet", tmp_path, plan=plan)

    data = json.loads(manifest_path.read_text())
    assert len(data["levels"]) == 1
    assert data["levels"][0]["level"] == 0
    assert len(data["levels"][0]["shards"]) == 1


def test_resume_manifest_max_level_mismatch_fails(tmp_path, monkeypatch):
    entry = _make_entry(tmp_path, level=0, prefix_bits=0, prefix=0)
    write_manifest(tmp_path, max_level=2, shard_entries=[entry], mag_limit=6.5)
    monkeypatch.setattr(
        "foundinspace.octree.assembly.build._check_input_columns",
        lambda _glob: None,
    )

    plan = BuildPlan(
        max_level=0,
        deep_shard_from_level=1,
        deep_prefix_bits=3,
        batch_size=100,
    )
    with pytest.raises(ValueError, match="max_level"):
        build_intermediates("unused/*.parquet", tmp_path, plan=plan)
