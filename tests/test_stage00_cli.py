from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.sources.add_shard_columns import run_enrich_healpix


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), str(path), compression="zstd")


def _build_input_tree(root: Path) -> None:
    cols = [
        "source",
        "source_id",
        "x_icrs_pc",
        "y_icrs_pc",
        "z_icrs_pc",
        "mag_abs",
        "quality_flags",
        "astrometry_quality",
        "photometry_quality",
    ]
    p0 = pd.DataFrame(
        [
            ["gaia", "1001", 0.0, 1.0, 0.0, 5.0, 1, 0.1, 0.1],
            ["gaia", "1002", 1.0, 0.0, 0.0, 3.0, 1, 0.1, 0.1],
            ["gaia", "1003", 0.0, 0.0, 1.0, 1.0, 1, 0.1, 0.1],
        ],
        columns=cols,
    )
    p1 = pd.DataFrame(
        [
            ["hip", "2001", -1.0, 0.0, 0.0, 6.0, 2, 0.2, 0.2],
            ["hip", "2002", 0.0, -1.0, 0.0, 2.0, 2, 0.2, 0.2],
        ],
        columns=cols,
    )
    _write_parquet(p0, root / "0" / "part-a.parquet")
    _write_parquet(p1, root / "1" / "part-b.parquet")


def test_stage00_help_contains_batch_size():
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-00", "--help"])
    assert result.exit_code == 0
    assert "Rows per streaming enrichment batch" in result.output
    assert "--batch-size" in result.output
    assert "--no-clear-output" not in result.output


def test_run_enrich_healpix_per_pixel_non_destructive_and_resumable(tmp_path: Path):
    input_root = tmp_path / "merged" / "healpix"
    output_root = tmp_path / "stage00"
    _build_input_tree(input_root)

    processed, skipped = run_enrich_healpix(
        src_root=input_root,
        output_root=output_root,
        batch_size=2,
        force=False,
        verbose=False,
    )
    assert processed == 2
    assert skipped == 0

    # Source files are unchanged (no in-place enrichment).
    for src in sorted(input_root.glob("*/*.parquet")):
        src_df = pd.read_parquet(src)
        assert "morton_code" not in src_df.columns
        assert "render" not in src_df.columns
        assert "level" not in src_df.columns

    # Each processed pixel has output parquet and completion marker.
    for pixel in ("0", "1"):
        pixel_dir = output_root / pixel
        assert pixel_dir.is_dir()
        assert (pixel_dir / ".complete").exists()
        parts = sorted(pixel_dir.glob("*.parquet"))
        assert parts
        out_df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
        assert {"morton_code", "render", "level", "mag_abs"}.issubset(out_df.columns)
        assert out_df["render"].map(len).eq(16).all()

        sorted_index = out_df[["morton_code", "mag_abs"]].sort_values(
            ["morton_code", "mag_abs"],
            kind="mergesort",
        ).index.to_numpy()
        assert np.array_equal(sorted_index, np.arange(len(out_df)))

    # Rerun should skip completed pixels.
    processed2, skipped2 = run_enrich_healpix(
        src_root=input_root,
        output_root=output_root,
        batch_size=2,
        force=False,
        verbose=False,
    )
    assert processed2 == 0
    assert skipped2 == 2
