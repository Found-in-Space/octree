from __future__ import annotations

from combine_helpers import PayloadNode, build_intermediates
from foundinspace.octree.combine.lookup import (
    FileHandleCache,
    FixedRecordFile,
    IntermediateLookup,
    RelocationLookup,
)
from foundinspace.octree.combine.pipeline import relocate_payloads_dfs
from foundinspace.octree.combine.records import (
    RELOC_HEADER_FMT,
    RELOC_MAGIC,
    RELOC_RECORD_FMT,
)


def test_fixed_record_find_and_range(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=1, node_id=3, star_count=1, raw_payload=b"b"),
        ],
        max_level=1,
    )
    index_path = tmp_path / "level-01.index"
    fr = FixedRecordFile(
        index_path,
        header_struct=RELOC_HEADER_FMT,
        record_struct=RELOC_RECORD_FMT,
        magic=b"OIDX",
    )
    try:
        assert fr.find_u64_key(0) is not None
        assert fr.find_u64_key(2) is None
        assert fr.any_key_in_range(1, 2) is False
        assert fr.any_key_in_range(2, 3) is True
    finally:
        fr.close()

    with open(tmp_path / "out.bin", "wb") as out_fp:
        out_fp.write(b"\x00" * 64)
        result = relocate_payloads_dfs(manifest_path, out_fp, plan=_Plan())
    reloc = FixedRecordFile(
        result.relocation_files[0],
        header_struct=RELOC_HEADER_FMT,
        record_struct=RELOC_RECORD_FMT,
        magic=RELOC_MAGIC,
    )
    try:
        assert reloc.header.record_count == 2
    finally:
        reloc.close()


def test_intermediate_and_relocation_lookup(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=1, star_count=2, raw_payload=b"child"),
        ],
        max_level=1,
    )
    with open(tmp_path / "out.bin", "wb") as out_fp:
        out_fp.write(b"\x00" * 64)
        phase_a = relocate_payloads_dfs(manifest_path, out_fp, plan=_Plan())

    from foundinspace.octree.combine.manifest import read_combine_manifest

    manifest = read_combine_manifest(manifest_path)
    il = IntermediateLookup(manifest, max_open_files=2)
    rl = RelocationLookup(phase_a.relocation_files, max_open_files=2)
    try:
        assert il.has_payload_node(0, 0)
        assert il.has_payload_node(1, 1)
        assert not il.has_payload_node(1, 7)
        assert rl.get_payload(0, 0) is not None
        assert rl.get_payload(1, 1) is not None
    finally:
        il.close()
        rl.close()


def test_lookup_open_files_are_bounded(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"b"),
            PayloadNode(level=2, node_id=0, star_count=1, raw_payload=b"c"),
        ],
        max_level=2,
    )
    from foundinspace.octree.combine.manifest import read_combine_manifest

    manifest = read_combine_manifest(manifest_path)
    il = IntermediateLookup(manifest, max_open_files=1)
    try:
        assert il.has_payload_node(0, 0)
        assert il.open_file_count <= 1
        assert il.has_payload_node(1, 0)
        assert il.open_file_count <= 1
        assert il.has_payload_node(2, 0)
        assert il.open_file_count <= 1
    finally:
        il.close()


def test_file_handle_cache_bounded(tmp_path) -> None:
    p1 = tmp_path / "a.bin"
    p2 = tmp_path / "b.bin"
    p1.write_bytes(b"a")
    p2.write_bytes(b"b")
    cache = FileHandleCache(1)
    f1 = cache.open(p1, "rb")
    assert not f1.closed
    _ = cache.open(p2, "rb")
    assert f1.closed
    cache.close_all()


class _Plan:
    max_open_files = 4
