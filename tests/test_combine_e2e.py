from __future__ import annotations

import json

import pytest

from combine_helpers import PayloadNode, build_intermediates
from foundinspace.octree.combine import CombinePlan, combine_octree
from foundinspace.octree.combine.records import HEADER_FMT, HEADER_SIZE


def test_combine_octree_is_deterministic(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=1, node_id=7, star_count=1, raw_payload=b"h"),
            PayloadNode(level=2, node_id=56, star_count=1, raw_payload=b"hh"),
        ],
        max_level=2,
    )
    out1 = tmp_path / "run1.octree"
    out2 = tmp_path / "run2.octree"

    combine_octree(manifest_path, out1, plan=CombinePlan(max_open_files=2))
    combine_octree(manifest_path, out2, plan=CombinePlan(max_open_files=2))

    assert out1.read_bytes() == out2.read_bytes()


def test_combine_header_mag_limit_matches_manifest(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root")],
        max_level=0,
        mag_limit=4.25,
    )
    out = tmp_path / "out.octree"
    combine_octree(manifest_path, out, plan=CombinePlan(max_open_files=2))

    hdr = HEADER_FMT.unpack(out.read_bytes()[:HEADER_SIZE])
    assert hdr[11] == pytest.approx(4.25)


def test_manifest_identifier_mismatch_fails_fast(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root")],
        max_level=0,
    )
    manifest = json.loads(manifest_path.read_text())
    manifest["format"] = "wrong/format"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    with pytest.raises(ValueError, match="Unsupported manifest format"):
        combine_octree(manifest_path, tmp_path / "out.octree", plan=CombinePlan())


def test_manifest_missing_mag_limit_fails(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root")],
        max_level=0,
    )
    manifest = json.loads(manifest_path.read_text())
    manifest.pop("mag_limit", None)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    with pytest.raises(ValueError, match="mag_limit"):
        combine_octree(manifest_path, tmp_path / "out.octree", plan=CombinePlan())


def test_combine_meta_octree_is_deterministic(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=1, node_id=7, star_count=1, raw_payload=b"h"),
            PayloadNode(level=2, node_id=56, star_count=1, raw_payload=b"hh"),
        ],
        max_level=2,
        with_meta=True,
    )
    out1 = tmp_path / "run1.meta.octree"
    out2 = tmp_path / "run2.meta.octree"

    combine_octree(
        manifest_path,
        out1,
        plan=CombinePlan(max_open_files=2),
        payload_kind="meta",
    )
    combine_octree(
        manifest_path,
        out2,
        plan=CombinePlan(max_open_files=2),
        payload_kind="meta",
    )

    assert out1.read_bytes() == out2.read_bytes()


def test_combine_meta_header_matches_manifest(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root")],
        max_level=0,
        mag_limit=4.25,
        with_meta=True,
    )
    out = tmp_path / "out.meta.octree"
    combine_octree(
        manifest_path,
        out,
        plan=CombinePlan(max_open_files=2),
        payload_kind="meta",
    )

    hdr = HEADER_FMT.unpack(out.read_bytes()[:HEADER_SIZE])
    assert hdr[11] == pytest.approx(4.25)


def test_combine_meta_fails_without_meta_in_manifest(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root")],
        max_level=0,
    )
    with pytest.raises(ValueError, match="missing meta paths"):
        combine_octree(
            manifest_path,
            tmp_path / "out.meta.octree",
            plan=CombinePlan(),
            payload_kind="meta",
        )
