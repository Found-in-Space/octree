from __future__ import annotations

import pytest

from combine_helpers import PayloadNode, build_intermediates
from foundinspace.octree.combine.pipeline import (
    CombinePlan,
    relocate_payloads_dfs,
    write_final_shard_index,
)
from foundinspace.octree.combine.records import (
    SHARD_HDR_FMT,
    SHARD_MAGIC,
    SHARD_NODE_FMT,
)


def test_write_final_shard_index_writes_shard_block(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=1, node_id=1, star_count=1, raw_payload=b"b"),
            PayloadNode(level=2, node_id=1, star_count=1, raw_payload=b"ba"),
        ],
        max_level=2,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        phase_a = relocate_payloads_dfs(
            manifest_path, fp, plan=CombinePlan(max_open_files=2)
        )
        phase_b = write_final_shard_index(
            manifest_path,
            phase_a.relocation_files,
            fp,
            plan=CombinePlan(max_open_files=2),
        )
    data = out.read_bytes()
    assert phase_b.index_offset >= phase_a.payload_end_offset
    assert phase_b.index_length > 0

    shard_hdr = SHARD_HDR_FMT.unpack_from(data, phase_b.index_offset)
    assert shard_hdr[0] == SHARD_MAGIC
    node_count = shard_hdr[7]
    assert node_count >= 1
    node_table_offset = shard_hdr[22]
    node = SHARD_NODE_FMT.unpack_from(data, node_table_offset)
    assert node[3] >= 1  # local_depth


def test_write_final_shard_index_node_count_limit(monkeypatch, tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"x")],
        max_level=0,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=CombinePlan())

        import foundinspace.octree.combine.pipeline as p

        fake_node = p._ShardNode(
            global_level=0,
            node_id=0,
            local_depth=1,
            local_path=0,
            child_mask=0,
            payload=None,
            children=(),
        )
        monkeypatch.setattr(
            p, "_build_shard_nodes", lambda *args, **kwargs: [fake_node] * 65536
        )
        with pytest.raises(ValueError, match="node_count exceeds u16"):
            p.write_final_shard_index(
                manifest_path,
                phase_a.relocation_files,
                fp,
                plan=CombinePlan(),
            )


def test_missing_relocation_for_payload_node_raises(monkeypatch, tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"child"),
        ],
        max_level=1,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=CombinePlan())

        import foundinspace.octree.combine.pipeline as p

        orig = p.RelocationLookup.get_payload

        def _missing(self, level, node_id):
            if level == 1 and node_id == 0:
                return None
            return orig(self, level, node_id)

        monkeypatch.setattr(p.RelocationLookup, "get_payload", _missing)

        with pytest.raises(ValueError, match="Missing relocation entry"):
            p.write_final_shard_index(
                manifest_path,
                phase_a.relocation_files,
                fp,
                plan=CombinePlan(),
            )


def test_shard_node_ordering_is_deterministic_and_path_sorted(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=1, node_id=7, star_count=1, raw_payload=b"h"),
            PayloadNode(level=2, node_id=56, star_count=1, raw_payload=b"hh"),
        ],
        max_level=2,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        phase_a = relocate_payloads_dfs(
            manifest_path, fp, plan=CombinePlan(max_open_files=2)
        )
        phase_b = write_final_shard_index(
            manifest_path,
            phase_a.relocation_files,
            fp,
            plan=CombinePlan(max_open_files=2),
        )
    data = out.read_bytes()
    shard_hdr = SHARD_HDR_FMT.unpack_from(data, phase_b.index_offset)
    node_count = shard_hdr[7]
    node_table_offset = shard_hdr[22]
    keys: list[tuple[int, int]] = []
    for i in range(node_count):
        rec = SHARD_NODE_FMT.unpack_from(
            data, node_table_offset + i * SHARD_NODE_FMT.size
        )
        local_path = rec[1]
        local_depth = rec[3]
        keys.append((local_depth, local_path))
    assert keys == sorted(keys)
