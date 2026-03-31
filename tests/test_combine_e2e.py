from __future__ import annotations

import json
from uuid import UUID

import pytest

from combine_helpers import (
    PayloadNode,
    build_intermediates,
    build_sidecar_intermediates,
)
from foundinspace.octree.combine import CombinePlan, combine_octree
from foundinspace.octree.combine.records import (
    HEADER_FMT,
    HEADER_SIZE,
    PackedDescriptorFields,
)
from foundinspace.octree.reader import read_header


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
    descriptor = PackedDescriptorFields(
        artifact_kind="render",
        dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
    )

    combine_octree(
        manifest_path, out1, plan=CombinePlan(max_open_files=2), descriptor=descriptor
    )
    combine_octree(
        manifest_path, out2, plan=CombinePlan(max_open_files=2), descriptor=descriptor
    )

    assert out1.read_bytes() == out2.read_bytes()


def test_combine_header_mag_limit_matches_manifest(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root")],
        max_level=0,
        mag_limit=4.25,
    )
    out = tmp_path / "out.octree"
    combine_octree(
        manifest_path,
        out,
        plan=CombinePlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        ),
    )

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


def test_combine_sidecar_writes_descriptor_metadata(tmp_path) -> None:
    manifest_path = build_sidecar_intermediates(
        tmp_path / "sidecar-intermediates",
        [
            PayloadNode(
                level=0,
                node_id=0,
                star_count=1,
                raw_payload=b"",
                meta_entries=[{"source": "gaia", "source_id": "1"}],
            )
        ],
        max_level=0,
    )
    out = tmp_path / "meta.octree"
    combine_octree(
        manifest_path,
        out,
        plan=CombinePlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="sidecar",
            parent_dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            sidecar_uuid=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            sidecar_kind="meta",
        ),
    )
    header = read_header(out)
    assert header.artifact_kind == "sidecar"
    assert header.parent_dataset_uuid == UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    assert header.sidecar_uuid == UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    assert header.sidecar_kind == "meta"
