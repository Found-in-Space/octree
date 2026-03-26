from __future__ import annotations

import gzip
import json

from combine_helpers import PayloadNode, build_intermediates
from foundinspace.octree.combine.dfs import iter_cells_dfs
from foundinspace.octree.combine.lookup import FixedRecordFile
from foundinspace.octree.combine.pipeline import CombinePlan, relocate_payloads_dfs
from foundinspace.octree.combine.records import (
    RELOC_HEADER_FMT,
    RELOC_MAGIC,
    RELOC_RECORD_FMT,
)


def test_iter_cells_dfs_order(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"r"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=2, node_id=0, star_count=1, raw_payload=b"aa"),
            PayloadNode(level=1, node_id=1, star_count=1, raw_payload=b"b"),
        ],
        max_level=2,
    )
    got = [(c.level, c.node_id) for c in iter_cells_dfs(manifest_path)]
    assert got == [(0, 0), (1, 0), (2, 0), (1, 1)]


def test_relocate_payloads_writes_payloads_and_relocation(tmp_path) -> None:
    nodes = [
        PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
        PayloadNode(level=1, node_id=0, star_count=2, raw_payload=b"left"),
        PayloadNode(level=1, node_id=1, star_count=3, raw_payload=b"right"),
    ]
    manifest_path = build_intermediates(tmp_path, nodes, max_level=1)
    out = tmp_path / "final.octree"

    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        result = relocate_payloads_dfs(manifest_path, fp, plan=CombinePlan(max_open_files=2))

    payload_bytes = out.read_bytes()[64: result.payload_end_offset]
    expected = b"".join(gzip.compress(n.raw_payload) for n in [nodes[0], nodes[1], nodes[2]])
    assert payload_bytes == expected
    assert len(result.relocation_files) == 2

    for reloc_path in result.relocation_files:
        fr = FixedRecordFile(
            reloc_path,
            header_struct=RELOC_HEADER_FMT,
            record_struct=RELOC_RECORD_FMT,
            magic=RELOC_MAGIC,
        )
        try:
            assert fr.header.record_count >= 1
        finally:
            fr.close()


def test_relocate_meta_payloads_writes_correct_bytes(tmp_path) -> None:
    nodes = [
        PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
        PayloadNode(level=1, node_id=0, star_count=2, raw_payload=b"left"),
        PayloadNode(level=1, node_id=1, star_count=3, raw_payload=b"right"),
    ]
    manifest_path = build_intermediates(
        tmp_path, nodes, max_level=1, with_meta=True
    )
    out = tmp_path / "final.octree"

    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        result = relocate_payloads_dfs(
            manifest_path,
            fp,
            plan=CombinePlan(max_open_files=2),
            payload_kind="meta",
        )

    payload_bytes = out.read_bytes()[64 : result.payload_end_offset]
    expected = b"".join(
        gzip.compress(json.dumps([{}] * n.star_count).encode())
        for n in [nodes[0], nodes[1], nodes[2]]
    )
    assert payload_bytes == expected
