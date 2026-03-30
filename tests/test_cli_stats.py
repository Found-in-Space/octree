from __future__ import annotations

import struct
from pathlib import Path

from click.testing import CliRunner
import pytest
import foundinspace.octree.reader.source as reader_source

from combine_helpers import PayloadNode, build_intermediates
from foundinspace.octree._cli import _format_identifiers, cli
from foundinspace.octree.combine import CombinePlan, combine_octree

STAR_RECORD_FMT = struct.Struct("<fffhBB")


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
            _encode_star(x_rel=5.0e-5, y_rel=0.0, z_rel=0.0, abs_mag=5.0, teff_log8=255),
        ]
    )
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=3, raw_payload=payload)],
        max_level=0,
        mag_limit=6.5,
    )
    output = tmp_path / "stars.octree"
    combine_octree(manifest_path, output, plan=CombinePlan(max_open_files=2))
    return output


def _build_small_octree_with_meta(tmp_path: Path) -> Path:
    payload = b"".join(
        [
            _encode_star(x_rel=0.0, y_rel=0.0, z_rel=0.0, abs_mag=4.8, teff_log8=128),
            _encode_star(
                x_rel=1.0e-5, y_rel=0.0, z_rel=0.0, abs_mag=12.0, teff_log8=80
            ),
            _encode_star(x_rel=5.0e-5, y_rel=0.0, z_rel=0.0, abs_mag=5.0, teff_log8=255),
        ]
    )
    manifest_path = build_intermediates(
        tmp_path / "intermediates_meta",
        [
            PayloadNode(
                level=0,
                node_id=0,
                star_count=3,
                raw_payload=payload,
                meta_entries=[
                    {"proper_name": "Sun"},
                    {"hip_id": 71683},
                    {},
                ],
            )
        ],
        max_level=0,
        mag_limit=6.5,
        with_meta=True,
    )
    output = tmp_path / "stars.octree"
    meta_output = tmp_path / "stars.meta.octree"
    combine_octree(manifest_path, output, plan=CombinePlan(max_open_files=2))
    combine_octree(
        manifest_path,
        meta_output,
        plan=CombinePlan(max_open_files=2),
        payload_kind="meta",
    )
    return output


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


def test_cli_stats_output_sections(tmp_path: Path) -> None:
    octree_path = _build_small_octree(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stats",
            str(octree_path),
            "--point",
            "0,0,0",
            "--magnitude",
            "6.5",
            "--radius",
            "3.0",
            "--nearest",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "By level (shell set at Sun)" in result.output
    assert "Coalesced" in result.output
    assert "Total span bytes" in result.output
    assert "Nearest 2 stars" in result.output


def test_cli_stats_uses_inferred_meta_and_stars_alias(tmp_path: Path) -> None:
    octree_path = _build_small_octree_with_meta(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stats",
            str(octree_path),
            "--point",
            "0,0,0",
            "--magnitude",
            "6.5",
            "--radius",
            "3.0",
            "--stars",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Identifiers" in result.output
    assert "Sun" in result.output
    assert "HIP 71683" in result.output


def test_cli_stats_accepts_http_range_urls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    octree_path = _build_small_octree(tmp_path)
    octree_url = _install_fake_range_urlopen(monkeypatch, octree_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stats",
            octree_url,
            "--point",
            "0,0,0",
            "--magnitude",
            "6.5",
            "--radius",
            "3.0",
            "--nearest",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    assert octree_url in result.output
    assert "By level (shell set at Sun)" in result.output


def test_format_identifiers_falls_back_to_primary_key() -> None:
    assert _format_identifiers((("source", "gaia"), ("source_id", "123"))) == "Gaia 123"
    assert _format_identifiers((("source", "hip"), ("source_id", "42"))) == "HIP 42"
