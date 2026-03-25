from __future__ import annotations

import struct
from pathlib import Path

from click.testing import CliRunner

from combine_helpers import PayloadNode, build_intermediates
from foundinspace.octree._cli import cli
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
