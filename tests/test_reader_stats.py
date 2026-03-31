from __future__ import annotations

import math
import struct
from pathlib import Path
from uuid import UUID

import pytest

import foundinspace.octree.reader.source as reader_source
from combine_helpers import (
    PayloadNode,
    build_intermediates,
    build_sidecar_intermediates,
)
from foundinspace.octree.combine import CombinePlan, combine_octree
from foundinspace.octree.combine.records import (
    DESCRIPTOR_SIZE,
    HEADER_SIZE,
    SHARD_MAGIC,
    PackedDescriptorFields,
    PackedHeaderFields,
    pack_descriptor,
    pack_top_level_header,
)
from foundinspace.octree.reader import NodeEntry, OctreeReader, Point, read_header
from foundinspace.octree.reader.index import GridCoord
from foundinspace.octree.reader.stats import collect_stats

STAR_RECORD_FMT = struct.Struct("<fffhBB")
DATASET_UUID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SIDECAR_UUID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


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


def _build_small_octree(tmp_path: Path) -> Path:
    payload = b"".join(
        [
            _encode_star(x_rel=0.0, y_rel=0.0, z_rel=0.0, abs_mag=4.8, teff_log8=128),
            _encode_star(
                x_rel=1.0e-5, y_rel=0.0, z_rel=0.0, abs_mag=12.0, teff_log8=80
            ),
            _encode_star(
                x_rel=5.0e-5, y_rel=0.0, z_rel=0.0, abs_mag=5.0, teff_log8=255
            ),
        ]
    )
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=3, raw_payload=payload)],
        max_level=0,
        mag_limit=6.5,
    )
    output = tmp_path / "stars.octree"
    combine_octree(
        manifest_path,
        output,
        plan=CombinePlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )
    return output


def _build_small_octree_with_meta(tmp_path: Path) -> tuple[Path, Path]:
    payload = b"".join(
        [
            _encode_star(x_rel=0.0, y_rel=0.0, z_rel=0.0, abs_mag=4.8, teff_log8=128),
            _encode_star(
                x_rel=1.0e-5, y_rel=0.0, z_rel=0.0, abs_mag=12.0, teff_log8=80
            ),
            _encode_star(
                x_rel=5.0e-5, y_rel=0.0, z_rel=0.0, abs_mag=5.0, teff_log8=255
            ),
        ]
    )
    render_manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=3, raw_payload=payload)],
        max_level=0,
        mag_limit=6.5,
    )
    sidecar_manifest_path = build_sidecar_intermediates(
        tmp_path / "intermediates_meta",
        [
            PayloadNode(
                level=0,
                node_id=0,
                star_count=3,
                raw_payload=payload,
                meta_entries=[
                    {"proper_name": "Sun"},
                    {"hip_id": 71683, "proper_name": "Rigil Kentaurus"},
                    {},
                ],
            )
        ],
        max_level=0,
        mag_limit=6.5,
    )
    render_output = tmp_path / "stars.octree"
    meta_output = tmp_path / "stars.meta.octree"
    combine_octree(
        render_manifest_path,
        render_output,
        plan=CombinePlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )
    combine_octree(
        sidecar_manifest_path,
        meta_output,
        plan=CombinePlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="sidecar",
            parent_dataset_uuid=DATASET_UUID,
            sidecar_uuid=SIDECAR_UUID,
            sidecar_kind="meta",
        ),
    )
    return render_output, meta_output


class _FakeHttpResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeHttpResponse:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _install_fake_range_urlopen(monkeypatch: pytest.MonkeyPatch, path: Path) -> str:
    octree_bytes = path.read_bytes()
    octree_url = "https://example.test/stars.octree"

    def fake_urlopen(request: object) -> _FakeHttpResponse:
        range_header = getattr(request, "headers", {}).get("Range")
        if not range_header:
            return _FakeHttpResponse(octree_bytes)
        raw_range = range_header.removeprefix("bytes=")
        start_s, end_s = raw_range.split("-", 1)
        start = int(start_s)
        end = int(end_s)
        return _FakeHttpResponse(octree_bytes[start : end + 1])

    monkeypatch.setattr(reader_source, "urlopen", fake_urlopen)
    return octree_url


def test_read_header_roundtrip_with_shard_probe(tmp_path: Path) -> None:
    header = pack_top_level_header(
        PackedHeaderFields(
            world_center=(1.0, 2.0, 3.0),
            world_half_size_pc=10.0,
            max_level=7,
            mag_limit=5.5,
            index_offset=HEADER_SIZE + DESCRIPTOR_SIZE,
            index_length=123,
        )
    )
    descriptor = pack_descriptor(
        PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        )
    )
    path = tmp_path / "header-only.octree"
    path.write_bytes(header + descriptor + SHARD_MAGIC + b"\x00" * 16)

    parsed = read_header(path)
    assert parsed.index_offset == HEADER_SIZE + DESCRIPTOR_SIZE
    assert parsed.index_length == 123
    assert parsed.world_center == pytest.approx((1.0, 2.0, 3.0))
    assert parsed.world_half_size == pytest.approx(10.0)
    assert parsed.max_level == 7
    assert parsed.mag_limit == pytest.approx(5.5)
    assert parsed.artifact_kind == "render"
    assert parsed.dataset_uuid == DATASET_UUID


def test_node_aabb_distance_cases() -> None:
    node = NodeEntry(
        level=1,
        grid=GridCoord(0, 0, 0),
        center=Point(0.0, 0.0, 0.0),
        half_size=2.0,
        flags=0,
        child_mask=0,
        payload_offset=0,
        payload_length=0,
        _shard_offset=0,
        _node_index=1,
        _first_child=0,
        _local_depth=1,
        _local_path=0,
    )
    assert node.aabb_distance(Point(0.0, 0.0, 0.0)) == pytest.approx(0.0)
    assert node.aabb_distance(Point(2.0, 0.0, 0.0)) == pytest.approx(0.0)
    assert node.aabb_distance(Point(3.0, 0.0, 0.0)) == pytest.approx(1.0)


def test_octree_reader_queries_and_teff_sentinel(tmp_path: Path) -> None:
    octree_path = _build_small_octree(tmp_path)
    with OctreeReader(octree_path) as reader:
        bright = list(reader.stars_brighter_than(Point(0.0, 0.0, 0.0), 6.5))
        near = list(reader.stars_within_distance(Point(0.0, 0.0, 0.0), 3.0))

    assert len(bright) == 2
    assert len(near) == 2
    assert any(
        star.position.distance_to(Point(0.0, 0.0, 0.0)) == pytest.approx(0.0)
        for star in near
    )
    assert any(math.isnan(star.teff) for star in bright)


def test_collect_stats_level_totals_and_nearest(tmp_path: Path) -> None:
    octree_path = _build_small_octree(tmp_path)
    report = collect_stats(
        octree_path,
        point=Point(0.0, 0.0, 0.0),
        limiting_magnitude=6.5,
        radius_pc=3.0,
        nearest_n=2,
        coalesce_gap_bytes=0,
    )

    assert len(report.by_level) == 1
    row = report.by_level[0]
    assert row.level == 0
    assert row.nodes == 1
    assert row.stars_loaded == 3
    assert row.stars_rendered == 2
    assert row.payload_bytes > 0

    assert report.totals.nodes == row.nodes
    assert report.totals.stars_loaded == row.stars_loaded
    assert report.totals.stars_rendered == row.stars_rendered

    assert report.coalesced.input_ranges == 1
    assert report.coalesced.output_batches == 1
    assert report.coalesced.total_span_bytes == report.coalesced.raw_payload_bytes

    assert len(report.nearest) == 2
    assert report.nearest[0].distance_pc <= report.nearest[1].distance_pc


def test_collect_stats_includes_identifiers_from_meta_octree(tmp_path: Path) -> None:
    octree_path, meta_path = _build_small_octree_with_meta(tmp_path)
    report = collect_stats(
        octree_path,
        point=Point(0.0, 0.0, 0.0),
        limiting_magnitude=6.5,
        radius_pc=3.0,
        metadata_path=meta_path,
        nearest_n=2,
    )

    assert len(report.nearest) == 2
    first = dict(report.nearest[0].identifiers)
    second = dict(report.nearest[1].identifiers)
    assert first.get("proper_name") == "Sun"
    assert second.get("proper_name") == "Rigil Kentaurus"
    assert second.get("hip_id") == 71683


def test_octree_reader_accepts_http_range_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    octree_path = _build_small_octree(tmp_path)
    octree_url = _install_fake_range_urlopen(monkeypatch, octree_path)

    with OctreeReader(octree_url) as reader:
        bright = list(reader.stars_brighter_than(Point(0.0, 0.0, 0.0), 6.5))

    assert len(bright) == 2
