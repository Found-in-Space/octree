from __future__ import annotations

import gzip
import json
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from foundinspace.octree.identifiers_order import pack_identifiers_order
from foundinspace.octree.packing import PackingPlan, pack_octree
from foundinspace.octree.packing.dfs import iter_cells_dfs
from foundinspace.octree.packing.manifest import read_packing_manifest
from foundinspace.octree.packing.records import PackedDescriptorFields
from foundinspace.octree.reader import read_header
from foundinspace.octree.sidecars.visual_duplicates import (
    PAYLOAD_ENCODING,
    SIDECAR_KIND,
    VisualDuplicatesBuildConfig,
    build_visual_duplicates_sidecar,
)
from packing_helpers import (
    PayloadNode,
    build_identifiers_intermediates,
    build_intermediates,
)

DATASET_UUID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
ORDER_UUID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _write_render_and_order(tmp_path: Path) -> tuple[Path, Path]:
    nodes = [
        PayloadNode(
            level=1,
            node_id=0,
            star_count=3,
            raw_payload=b"\0" * 48,
            identities=[("manual", "sun"), ("gaia", "100"), ("gaia", "101")],
        ),
        PayloadNode(
            level=1,
            node_id=3,
            star_count=1,
            raw_payload=b"\0" * 16,
            identities=[("hip", "200")],
        ),
        PayloadNode(
            level=1,
            node_id=7,
            star_count=1,
            raw_payload=b"\0" * 16,
            identities=[("gaia", "999")],
        ),
    ]
    render_manifest = build_intermediates(
        tmp_path / "render-intermediates",
        nodes,
        max_level=1,
    )
    identifiers_manifest = build_identifiers_intermediates(
        tmp_path / "identifier-intermediates",
        nodes,
        max_level=1,
    )
    render_path = tmp_path / "stars.octree"
    pack_octree(
        render_manifest,
        render_path,
        plan=PackingPlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )
    order_path = tmp_path / "identifiers.order"
    pack_identifiers_order(
        identifiers_manifest,
        order_path,
        parent_dataset_uuid=DATASET_UUID,
        artifact_uuid=ORDER_UUID,
    )
    return render_path, order_path


def _write_evidence(path: Path, rows: list[dict[str, object]]) -> None:
    schema = pa.schema(
        [
            pa.field("gaia_source_id", pa.uint64(), nullable=False),
            pa.field("hip_source_id", pa.uint64(), nullable=False),
            pa.field("mapping_source", pa.string(), nullable=False),
            pa.field("number_of_neighbours", pa.int16(), nullable=False),
            pa.field("angular_distance", pa.float32(), nullable=False),
        ]
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _config(
    tmp_path: Path,
    *,
    render_path: Path,
    order_path: Path,
    evidence_path: Path,
) -> VisualDuplicatesBuildConfig:
    return VisualDuplicatesBuildConfig(
        render_octree_path=render_path,
        identifiers_order_path=order_path,
        evidence_path=evidence_path,
        output_path=tmp_path / "stars.visual-duplicates.octree",
        work_dir=tmp_path / "visual-duplicates-work",
        report_path=tmp_path / "stars.visual-duplicates.report.json",
        deep_shard_from_level=99,
        deep_prefix_bits=3,
        max_open_files=2,
        scan_batch_bytes=20,
    )


def _payloads_by_cell(manifest_path: Path) -> dict[tuple[int, int], list[dict]]:
    manifest = read_packing_manifest(manifest_path)
    shards = {shard.key: shard for shard in manifest.shards}
    result: dict[tuple[int, int], list[dict]] = {}
    for ref in iter_cells_dfs(manifest_path):
        shard = shards[ref.shard]
        with open(shard.payload_path, "rb") as fp:
            fp.seek(ref.payload_offset)
            compressed = fp.read(ref.payload_length)
        result[(ref.level, ref.node_id)] = json.loads(gzip.decompress(compressed))
    return result


def test_build_visual_duplicates_sidecar_is_sparse_and_doubly_linked(
    tmp_path: Path,
) -> None:
    render_path, order_path = _write_render_and_order(tmp_path)
    evidence_path = tmp_path / "display-map.parquet"
    _write_evidence(
        evidence_path,
        [
            {
                "gaia_source_id": 100,
                "hip_source_id": 200,
                "mapping_source": "test-map",
                "number_of_neighbours": 1,
                "angular_distance": 0.125,
            }
        ],
    )

    result = build_visual_duplicates_sidecar(
        _config(
            tmp_path,
            render_path=render_path,
            order_path=order_path,
            evidence_path=evidence_path,
        )
    )

    header = read_header(result.output_path)
    assert header.artifact_kind == "sidecar"
    assert header.sidecar_kind == SIDECAR_KIND
    assert header.parent_dataset_uuid == DATASET_UUID
    assert header.sidecar_uuid == result.sidecar_uuid

    payloads = _payloads_by_cell(result.work_manifest_path)
    assert set(payloads) == {(1, 0), (1, 3)}
    gaia = payloads[(1, 0)][0]
    hip = payloads[(1, 3)][0]
    assert gaia["ordinal"] == 1
    assert gaia["identity"] == {"source": "gaia", "source_id": "100"}
    assert gaia["counterpart_ref"] == {
        "level": 1,
        "mortonCode": "3",
        "ordinal": 0,
    }
    assert hip["identity"] == {"source": "hip", "source_id": "200"}
    assert hip["counterpart_ref"] == {
        "level": 1,
        "mortonCode": "0",
        "ordinal": 1,
    }
    assert gaia["pair_id"] == hip["pair_id"] == "gaia:100|hip:200"

    report = json.loads(result.report_path.read_text())
    assert report["payload_encoding"] == PAYLOAD_ENCODING
    assert report["payload_cell_count"] == 2
    assert report["coverage"] == {
        "pairs_with_both_endpoints_rendered": 1,
        "pairs_with_neither_endpoint_rendered": 0,
        "pairs_with_only_gaia_rendered": 0,
        "pairs_with_only_hip_rendered": 0,
        "rendered_candidate_endpoints": 2,
    }


def test_visual_duplicate_evidence_must_be_one_to_one(tmp_path: Path) -> None:
    render_path, order_path = _write_render_and_order(tmp_path)
    evidence_path = tmp_path / "display-map.parquet"
    _write_evidence(
        evidence_path,
        [
            {
                "gaia_source_id": 100,
                "hip_source_id": 200,
                "mapping_source": "test-map",
                "number_of_neighbours": 1,
                "angular_distance": 0.125,
            },
            {
                "gaia_source_id": 100,
                "hip_source_id": 201,
                "mapping_source": "test-map",
                "number_of_neighbours": 1,
                "angular_distance": 0.25,
            },
        ],
    )

    with pytest.raises(ValueError, match="not one-to-one"):
        build_visual_duplicates_sidecar(
            _config(
                tmp_path,
                render_path=render_path,
                order_path=order_path,
                evidence_path=evidence_path,
            )
        )


def test_visual_duplicates_sidecar_retains_identity_when_counterpart_is_absent(
    tmp_path: Path,
) -> None:
    render_path, order_path = _write_render_and_order(tmp_path)
    evidence_path = tmp_path / "display-map.parquet"
    _write_evidence(
        evidence_path,
        [
            {
                "gaia_source_id": 100,
                "hip_source_id": 404,
                "mapping_source": "test-map",
                "number_of_neighbours": 1,
                "angular_distance": 0.5,
            }
        ],
    )

    result = build_visual_duplicates_sidecar(
        _config(
            tmp_path,
            render_path=render_path,
            order_path=order_path,
            evidence_path=evidence_path,
        )
    )

    payloads = _payloads_by_cell(result.work_manifest_path)
    assert set(payloads) == {(1, 0)}
    [entry] = payloads[(1, 0)]
    assert entry["counterpart_identity"] == {"source": "hip", "source_id": "404"}
    assert entry["counterpart_ref"] is None
    report = json.loads(result.report_path.read_text())
    assert report["coverage"]["pairs_with_only_gaia_rendered"] == 1
