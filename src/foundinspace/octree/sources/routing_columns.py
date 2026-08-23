#!/usr/bin/env python3
"""Add routing columns to HEALPix parquet in a streaming, non-destructive flow.

Routing processes one HEALPix pixel directory at a time:
1. Stream source rows in batches.
2. Compute morton_code and natural level columns while preserving raw fields.
3. Sort each batch by morton_code/mag_abs and write temporary run files.
4. DuckDB merge-sort runs into size-limited parquet outputs for that pixel.

Input files are never modified in place.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from foundinspace.octree.config import (
    DEFAULT_MAG_VIS,
    MORTON_BITS,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.duckdb_util import configure_connection
from foundinspace.octree.encoding.morton import morton3d_u64_from_xyz_arrays
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.magnitudes import quantize_render_magnitudes


def _compute_level(
    mag_abs: np.ndarray,
    mag_config: MagLevelConfig,
) -> np.ndarray:
    """Compute the natural magnitude-assigned level for each row."""
    represented = quantize_render_magnitudes(mag_abs)
    return mag_config.assign_level_array(represented)


def _compression_from_metadata(file_meta) -> str:
    """Extract compression codec from already-read parquet metadata."""
    if file_meta.num_row_groups == 0:
        return "snappy"
    col0 = file_meta.row_group(0).column(0)
    codec = getattr(col0, "compression", None)
    if codec is None:
        return "snappy"
    return str(codec).lower()


def _resolve_mag_config(
    mag_config: MagLevelConfig | None,
    *,
    v_mag: float | None,
) -> MagLevelConfig:
    if mag_config is not None:
        return mag_config
    vm = DEFAULT_MAG_VIS if v_mag is None else v_mag
    return MagLevelConfig(
        v_mag=vm,
        world_half_size=WORLD_HALF_SIZE_PC,
        morton_bits=MORTON_BITS,
    )


def _is_pixel_complete(pixel_output_dir: Path) -> bool:
    return (pixel_output_dir / ".complete").exists() or any(
        pixel_output_dir.glob("*.parquet")
    )


def _pixel_dirs(src_root: Path) -> list[Path]:
    return sorted(
        p for p in src_root.iterdir() if p.is_dir() and any(p.glob("*.parquet"))
    )


def _sort_and_write_pixel_runs(
    pixel_tmp_dir: Path,
    pixel_output_dir: Path,
    *,
    verbose: bool,
) -> int:
    run_glob = (pixel_tmp_dir / "*.parquet").as_posix().replace("'", "''")
    tmp_output_dir = pixel_output_dir.parent / f".tmp-merge-{pixel_output_dir.name}"
    if tmp_output_dir.exists():
        shutil.rmtree(tmp_output_dir)
    if pixel_output_dir.exists():
        shutil.rmtree(pixel_output_dir)

    con = duckdb.connect()
    configure_connection(con)
    try:
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{run_glob}')
                ORDER BY morton_code, mag_abs
            )
            TO '{tmp_output_dir.as_posix()}'
            (
                FORMAT parquet,
                CODEC zstd,
                ROW_GROUP_SIZE 122880,
                PER_THREAD_OUTPUT false,
                FILE_SIZE_BYTES '1GB'
            );
            """
        )
    finally:
        con.close()

    tmp_output_dir.rename(pixel_output_dir)
    out_files = len(list(pixel_output_dir.glob("*.parquet")))
    if verbose:
        print(
            f"  merged {len(list(pixel_tmp_dir.glob('*.parquet')))} runs -> {out_files} shard(s)"
        )
    return out_files


def _add_routing_columns(
    table: pa.Table,
    *,
    mag_config: MagLevelConfig,
) -> pa.Table:
    names = set(table.schema.names)
    x = np.asarray(table.column("x_icrs_pc"), dtype=np.float64)
    y = np.asarray(table.column("y_icrs_pc"), dtype=np.float64)
    z = np.asarray(table.column("z_icrs_pc"), dtype=np.float64)
    morton_code = morton3d_u64_from_xyz_arrays(x, y, z)
    mag_abs = np.asarray(table.column("mag_abs"), dtype=np.float64)
    level = _compute_level(mag_abs, mag_config)

    for col in ("morton_code", "level", "render"):
        if col in names:
            table = table.drop([col])
    table = table.append_column(
        "morton_code", pa.array(morton_code, type=pa.uint64())
    ).append_column("level", pa.array(level, type=pa.int32()))
    sort_idx = pc.sort_indices(
        table,
        sort_keys=[("morton_code", "ascending"), ("mag_abs", "ascending")],
    )
    return table.take(sort_idx)


def run_enrich_healpix(
    src_root: Path,
    output_root: Path,
    *,
    mag_config: MagLevelConfig | None = None,
    force: bool = False,
    batch_size: int = 1_000_000,
    v_mag: float | None = None,
    verbose: bool = True,
) -> tuple[int, int]:
    """
    Add routing columns to HEALPix-sharded parquet.

    Each HEALPix pixel directory is processed independently in bounded-memory batches.
    Returns ``(processed_pixels, skipped_pixels)``.
    """
    if not src_root.is_dir():
        raise NotADirectoryError(f"Not a directory: {src_root}")
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    mag_config = _resolve_mag_config(
        mag_config,
        v_mag=v_mag,
    )
    output_root.mkdir(parents=True, exist_ok=True)

    pixels = _pixel_dirs(src_root)
    if not pixels:
        if verbose:
            print(f"No HEALPix pixel directories with parquet files under {src_root}")
        return (0, 0)

    processed = 0
    skipped = 0
    for pixel_dir in pixels:
        pixel_name = pixel_dir.name
        out_pixel_dir = output_root / pixel_name
        pixel_tmp_dir = output_root / f".tmp-pixel-{pixel_name}"
        if force and out_pixel_dir.exists():
            shutil.rmtree(out_pixel_dir)
        if not force and _is_pixel_complete(out_pixel_dir):
            skipped += 1
            if verbose:
                print(f"Skipping pixel {pixel_name} (already complete)")
            continue

        if pixel_tmp_dir.exists():
            shutil.rmtree(pixel_tmp_dir)
        pixel_tmp_dir.mkdir(parents=True, exist_ok=True)

        run_count = 0
        row_count = 0
        if verbose:
            print(f"Processing pixel {pixel_name}...")

        try:
            for src_file in sorted(pixel_dir.glob("*.parquet")):
                file_meta = pq.read_metadata(src_file)
                schema = file_meta.schema.to_arrow_schema()
                names = set(schema.names)
                required = {"x_icrs_pc", "y_icrs_pc", "z_icrs_pc", "mag_abs"}
                missing = required - names
                if missing:
                    raise ValueError(f"{src_file}: missing columns {sorted(missing)}")

                compression = _compression_from_metadata(file_meta)
                parquet_file = pq.ParquetFile(src_file)
                for batch in parquet_file.iter_batches(batch_size=batch_size):
                    table = pa.Table.from_batches([batch])
                    if len(table) == 0:
                        continue
                    routed = _add_routing_columns(
                        table,
                        mag_config=mag_config,
                    )
                    run_path = pixel_tmp_dir / f"{run_count:08d}.parquet"
                    pq.write_table(routed, run_path, compression=compression)
                    run_count += 1
                    row_count += len(routed)

            if run_count == 0:
                out_pixel_dir.mkdir(parents=True, exist_ok=True)
                (out_pixel_dir / ".complete").write_text("empty\n", encoding="utf-8")
                if verbose:
                    print(f"  no rows for pixel {pixel_name}; wrote completion marker")
            else:
                out_count = _sort_and_write_pixel_runs(
                    pixel_tmp_dir,
                    out_pixel_dir,
                    verbose=verbose,
                )
                (out_pixel_dir / ".complete").write_text("ok\n", encoding="utf-8")
                if verbose:
                    print(
                        f"  wrote pixel {pixel_name}: {row_count:,} rows across {out_count} file(s)"
                    )
            processed += 1
        finally:
            if pixel_tmp_dir.exists():
                shutil.rmtree(pixel_tmp_dir)

    if verbose:
        print(f"Done: {processed} processed, {skipped} skipped.")
    return (processed, skipped)
