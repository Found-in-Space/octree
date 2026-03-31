from __future__ import annotations

import json
import struct
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from combine_helpers import (
    PayloadNode,
    build_identifiers_intermediates,
    build_intermediates,
)
from foundinspace.octree.combine import CombinePlan, combine_octree
from foundinspace.octree.combine.records import PackedDescriptorFields
from foundinspace.octree.identifiers_order import combine_identifiers_order
from foundinspace.octree.project import (
    OctreeProject,
    ProjectPaths,
    SidecarProjectConfig,
    Stage00ProjectConfig,
    Stage01ProjectConfig,
    Stage02ProjectConfig,
    Stage03ProjectConfig,
)
from foundinspace.octree.reader import Point, read_header
from foundinspace.octree.reader.stats import collect_stats
from foundinspace.octree.stage3 import build_stage03_sidecars

STAR_RECORD_FMT = struct.Struct("<fffhBB")
DATASET_UUID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
ORDER_UUID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_NULL_IDENT = dict.fromkeys(
    (
        "gaia_source_id",
        "hip_id",
        "hd",
        "bayer",
        "flamsteed",
        "constellation",
        "proper_name",
    ),
    None,
)


def _encode_star(
    *,
    x_rel: float,
    y_rel: float,
    z_rel: float,
    abs_mag: float,
    teff_log8: int,
) -> bytes:
    return STAR_RECORD_FMT.pack(
        float(x_rel),
        float(y_rel),
        float(z_rel),
        int(round(abs_mag * 100.0)),
        int(teff_log8),
        0,
    )


def _write_ident_map(path: Path, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def _make_project(tmp_path: Path) -> OctreeProject:
    payload = b"".join(
        [
            _encode_star(x_rel=0.0, y_rel=0.0, z_rel=0.0, abs_mag=4.8, teff_log8=128),
            _encode_star(x_rel=1.0e-5, y_rel=0.0, z_rel=0.0, abs_mag=5.0, teff_log8=80),
        ]
    )
    node = PayloadNode(
        level=0,
        node_id=0,
        star_count=2,
        raw_payload=payload,
        identities=[("manual", "sun"), ("hip", "71683")],
    )

    stage01_dir = tmp_path / "stage01"
    render_manifest_path = build_intermediates(
        stage01_dir / "render",
        [node],
        max_level=0,
        mag_limit=6.5,
    )
    identifiers_manifest_path = build_identifiers_intermediates(
        stage01_dir / "identifiers",
        [node],
        max_level=0,
        mag_limit=6.5,
    )

    render_path = tmp_path / "stars.octree"
    combine_octree(
        render_manifest_path,
        render_path,
        plan=CombinePlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )

    identifiers_order_path = tmp_path / "identifiers.order"
    combine_identifiers_order(
        identifiers_manifest_path,
        identifiers_order_path,
        parent_dataset_uuid=DATASET_UUID,
        artifact_uuid=ORDER_UUID,
    )

    identifiers_map_path = tmp_path / "identifiers_map.parquet"
    _write_ident_map(
        identifiers_map_path,
        [
            _NULL_IDENT
            | {
                "source": "manual",
                "source_id": "sun",
                "proper_name": "Sun",
            },
            _NULL_IDENT
            | {
                "source": "hip",
                "source_id": "71683",
                "hip_id": 71683,
                "proper_name": "Rigil Kentaurus",
            },
        ],
    )

    return OctreeProject(
        project_path=tmp_path / "project.toml",
        paths=ProjectPaths(
            merged_healpix_dir=tmp_path / "merged",
            identifiers_map_path=identifiers_map_path,
            stage00_output_dir=tmp_path / "stage00",
            stage01_output_dir=stage01_dir,
            stage02_output_path=render_path,
            identifiers_order_output_path=identifiers_order_path,
            stage03_output_dir=tmp_path / "stage03",
        ),
        stage00=Stage00ProjectConfig(batch_size=1000, v_mag=6.5, max_level=0),
        stage01=Stage01ProjectConfig(
            input_glob="unused/*.parquet",
            batch_size=1000,
            deep_shard_from_level=99,
            deep_prefix_bits=3,
        ),
        stage02=Stage02ProjectConfig(max_open_files=2),
        stage03=Stage03ProjectConfig(
            sidecars=(
                SidecarProjectConfig(
                    name="meta",
                    fields=("proper_name", "hip_id"),
                ),
            )
        ),
    )


def test_build_stage03_sidecars_writes_meta_sidecar_and_manifest(
    tmp_path: Path,
) -> None:
    project = _make_project(tmp_path)

    manifest_path = build_stage03_sidecars(project)

    manifest = json.loads(manifest_path.read_text())
    assert manifest["render_octree_path"] == str(project.paths.stage02_output_path)
    assert manifest["identifiers_order_path"] == str(
        project.paths.identifiers_order_output_path
    )
    assert manifest["parent_dataset_uuid"] == str(DATASET_UUID)
    assert [item["name"] for item in manifest["sidecars"]] == ["meta"]

    meta_path = project.paths.stage03_output_dir / "meta.octree"
    header = read_header(meta_path)
    assert header.artifact_kind == "sidecar"
    assert header.sidecar_kind == "meta"
    assert header.parent_dataset_uuid == DATASET_UUID
    assert header.sidecar_uuid is not None

    report = collect_stats(
        project.paths.stage02_output_path,
        point=Point(0.0, 0.0, 0.0),
        limiting_magnitude=6.5,
        radius_pc=3.0,
        metadata_path=meta_path,
        nearest_n=2,
    )
    first = dict(report.nearest[0].identifiers)
    second = dict(report.nearest[1].identifiers)
    assert first.get("proper_name") == "Sun"
    assert second.get("proper_name") == "Rigil Kentaurus"
    assert second.get("hip_id") == 71683


def test_build_stage03_sidecars_rebuilds_with_fresh_sidecar_uuid(
    tmp_path: Path,
) -> None:
    project = _make_project(tmp_path)

    build_stage03_sidecars(project)
    first_uuid = read_header(
        project.paths.stage03_output_dir / "meta.octree"
    ).sidecar_uuid

    build_stage03_sidecars(project, family_name="meta")
    second_uuid = read_header(
        project.paths.stage03_output_dir / "meta.octree"
    ).sidecar_uuid

    assert first_uuid is not None
    assert second_uuid is not None
    assert second_uuid != first_uuid
