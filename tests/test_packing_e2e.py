from __future__ import annotations

import json
from uuid import UUID

import pytest

from foundinspace.octree.packing import (
    IndexEmissionStrategy,
    PackingPlan,
    pack_octree,
)
from foundinspace.octree.packing.records import (
    HEADER_FMT,
    HEADER_SIZE,
    SHARD_HDR_FMT,
    PackedDescriptorFields,
)
from foundinspace.octree.reader import IndexNavigator, read_header
from packing_helpers import (
    PayloadNode,
    build_intermediates,
    build_sidecar_intermediates,
)


def test_pack_octree_is_deterministic(tmp_path) -> None:
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

    pack_octree(
        manifest_path, out1, plan=PackingPlan(max_open_files=2), descriptor=descriptor
    )
    pack_octree(
        manifest_path, out2, plan=PackingPlan(max_open_files=2), descriptor=descriptor
    )

    assert out1.read_bytes() == out2.read_bytes()


def test_packing_header_mag_limit_matches_manifest(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root")],
        max_level=0,
        mag_limit=4.25,
    )
    out = tmp_path / "out.octree"
    pack_octree(
        manifest_path,
        out,
        plan=PackingPlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        ),
    )

    hdr = HEADER_FMT.unpack(out.read_bytes()[:HEADER_SIZE])
    assert hdr[11] == pytest.approx(4.25)


@pytest.mark.parametrize(
    "strategy",
    [
        IndexEmissionStrategy.FORWARD,
        IndexEmissionStrategy.TEMP_PWRITE_BATCHED,
    ],
)
def test_packing_v2_writes_v2_shard_and_node_star_count(
    tmp_path, strategy: IndexEmissionStrategy
) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=3, raw_payload=b"payload")],
        max_level=0,
    )
    out = tmp_path / "out-v2.octree"
    pack_octree(
        manifest_path,
        out,
        plan=PackingPlan(
            max_open_files=2,
            star_format_version=2,
            index_emission_strategy=strategy,
        ),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        ),
    )

    header = read_header(out)
    assert header.version == 2
    assert HEADER_FMT.unpack(out.read_bytes()[:HEADER_SIZE])[2] == 0
    with open(out, "rb") as fp:
        fp.seek(header.index_offset)
        shard = SHARD_HDR_FMT.unpack(fp.read(SHARD_HDR_FMT.size))
    assert shard[1] == 2
    with IndexNavigator(out, header) as navigator:
        [root] = list(navigator.root_entries())
    assert root.star_count == 3
    assert root.is_terminal is False
    assert root.brightest_level == 0


def test_v1_reader_reports_unavailable_node_star_count(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"payload")],
        max_level=0,
    )
    out = tmp_path / "out-v1.octree"
    pack_octree(manifest_path, out, plan=PackingPlan(max_open_files=2))

    header = read_header(out)
    assert header.version == 1
    with IndexNavigator(out, header) as navigator:
        [root] = list(navigator.root_entries())
    assert root.star_count is None
    assert root.is_terminal is False
    assert root.brightest_level is None


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
        pack_octree(manifest_path, tmp_path / "out.octree", plan=PackingPlan())


def test_packing_sidecar_writes_descriptor_metadata(tmp_path) -> None:
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
    pack_octree(
        manifest_path,
        out,
        plan=PackingPlan(max_open_files=2),
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
