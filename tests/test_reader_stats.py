from __future__ import annotations

import math
import struct
from pathlib import Path
from uuid import UUID

import pytest

import foundinspace.octree.reader.source as reader_source
from foundinspace.octree.packing import PackingPlan, pack_octree
from foundinspace.octree.packing.records import (
    DESCRIPTOR_SIZE,
    HEADER_SIZE,
    IS_TERMINAL,
    SHARD_MAGIC,
    PackedDescriptorFields,
    PackedHeaderFields,
    pack_descriptor,
    pack_top_level_header,
)
from foundinspace.octree.reader import (
    NodeEntry,
    OctreeHeader,
    OctreeReader,
    Point,
    read_header,
)
from foundinspace.octree.reader.index import GridCoord
from foundinspace.octree.reader.stats import collect_stats
from foundinspace.octree.reader.visibility import should_prune_magnitude_node
from packing_helpers import (
    PayloadNode,
    build_intermediates,
    build_sidecar_intermediates,
)

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
    pack_octree(
        manifest_path,
        output,
        plan=PackingPlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )
    return output


def _build_full_width_shell_octree(tmp_path: Path) -> Path:
    payload = b"".join(
        [
            _encode_star(
                x_rel=1.0,
                y_rel=0.0,
                z_rel=0.0,
                abs_mag=-14.72,
                teff_log8=128,
            ),
            _encode_star(
                x_rel=1.0,
                y_rel=0.0,
                z_rel=0.0,
                abs_mag=-14.0,
                teff_log8=80,
            ),
        ]
    )
    manifest_path = build_intermediates(
        tmp_path / "shell-intermediates",
        [PayloadNode(level=1, node_id=0, star_count=2, raw_payload=payload)],
        max_level=1,
        mag_limit=6.5,
    )
    output = tmp_path / "shell-stars.octree"
    pack_octree(
        manifest_path,
        output,
        plan=PackingPlan(max_open_files=2, star_format_version=1),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )
    return output


def _synthetic_header(*, version: int) -> OctreeHeader:
    return OctreeHeader(
        version=version,
        artifact_kind="render",
        index_offset=0,
        index_length=0,
        world_center=(0.0, 0.0, 0.0),
        world_half_size=200_000.0,
        payload_record_size=STAR_RECORD_FMT.size,
        max_level=2,
        mag_limit=6.5,
        dataset_uuid=None,
        parent_dataset_uuid=None,
        sidecar_uuid=None,
        sidecar_kind=None,
    )


def _synthetic_node(*, flags: int, brightest_level: int | None) -> NodeEntry:
    return NodeEntry(
        level=1,
        grid=GridCoord(0, 0, 0),
        center=Point(0.0, 0.0, 0.0),
        half_size=100_000.0,
        flags=flags,
        child_mask=0,
        payload_offset=0,
        payload_length=0,
        _shard_offset=0,
        _node_index=1,
        _first_child=0,
        _local_depth=1,
        _local_path=0,
        brightest_level=brightest_level,
    )


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
    pack_octree(
        render_manifest_path,
        render_output,
        plan=PackingPlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )
    pack_octree(
        sidecar_manifest_path,
        meta_output,
        plan=PackingPlan(max_open_files=2),
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


def test_load_factor_two_is_exhaustive_and_one_is_approximate(tmp_path: Path) -> None:
    octree_path = _build_full_width_shell_octree(tmp_path)
    observer = Point(150_000.0, -100_000.0, -100_000.0)

    with OctreeReader(octree_path) as reader:
        exhaustive = list(
            reader.stars_brighter_than(observer, 6.5, load_factor=2.0)
        )
        approximate = list(
            reader.stars_brighter_than(observer, 6.5, load_factor=1.0)
        )

    assert [star.magnitude for star in exhaustive] == pytest.approx([-14.72])
    assert approximate == []


@pytest.mark.parametrize("flags", [0, IS_TERMINAL])
def test_v2_uses_brightest_level_while_v1_falls_back_to_emitted_level(
    flags: int,
) -> None:
    observer = Point(225_000.0, 0.0, 0.0)
    v2_node = _synthetic_node(flags=flags, brightest_level=2)
    v1_node = _synthetic_node(flags=flags, brightest_level=None)

    assert should_prune_magnitude_node(
        header=_synthetic_header(version=2),
        node=v2_node,
        point=observer,
        limiting_magnitude=6.5,
    )
    assert not should_prune_magnitude_node(
        header=_synthetic_header(version=1),
        node=v1_node,
        point=observer,
        limiting_magnitude=6.5,
    )


@pytest.mark.parametrize("load_factor", [0.999, 2.001, float("inf"), float("nan")])
def test_octree_reader_rejects_invalid_load_factor(
    tmp_path: Path,
    load_factor: float,
) -> None:
    octree_path = _build_small_octree(tmp_path)
    with (
        OctreeReader(octree_path) as reader,
        pytest.raises(ValueError, match="load_factor"),
    ):
        list(
            reader.stars_brighter_than(
                Point(0.0, 0.0, 0.0),
                6.5,
                load_factor=load_factor,
            )
        )


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
    assert report.load_factor == 2.0
    assert report.m_complete == pytest.approx(6.5)

    assert report.coalesced.input_ranges == 1
    assert report.coalesced.output_batches == 1
    assert report.coalesced.total_span_bytes == report.coalesced.raw_payload_bytes

    assert len(report.nearest) == 2
    assert report.nearest[0].distance_pc <= report.nearest[1].distance_pc


def test_collect_stats_uses_and_reports_load_factor(tmp_path: Path) -> None:
    octree_path = _build_full_width_shell_octree(tmp_path)
    observer = Point(150_000.0, -100_000.0, -100_000.0)

    exhaustive = collect_stats(
        octree_path,
        point=observer,
        limiting_magnitude=6.5,
        load_factor=2.0,
        radius_pc=0.0,
    )
    approximate = collect_stats(
        octree_path,
        point=observer,
        limiting_magnitude=6.5,
        load_factor=1.0,
        radius_pc=0.0,
    )

    assert exhaustive.load_factor == 2.0
    assert exhaustive.m_complete == pytest.approx(6.5)
    assert exhaustive.totals.stars_loaded == 2
    assert exhaustive.totals.stars_rendered == 1
    assert approximate.load_factor == 1.0
    assert approximate.m_complete == pytest.approx(
        6.5 + 5.0 * math.log10(0.5)
    )
    assert approximate.totals.stars_loaded == 0
    assert approximate.totals.stars_rendered == 0


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
