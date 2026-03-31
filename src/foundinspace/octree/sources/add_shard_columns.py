#!/usr/bin/env python3
"""Enrich HEALPix parquet with morton_code/render/level in a streaming, non-destructive flow.

Stage 00 processes one HEALPix pixel directory at a time:
1. Stream source rows in batches.
2. Compute morton_code, render, and level columns.
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
    DEFAULT_MAX_LEVEL,
    LEVEL_CONFIG,
    MORTON_BITS,
    WORLD_CENTER,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.duckdb_util import configure_connection
from foundinspace.octree.encoding.morton import morton3d_u64_from_xyz_arrays
from foundinspace.octree.encoding.teff import encode_teff
from foundinspace.octree.mag_levels import MagLevelConfig

# Numpy dtypes for vectorized pack/unpack (16-byte render, 10-byte meta)
_RENDER_DT = np.dtype(
    [
        ("x", "<f4"),
        ("y", "<f4"),
        ("z", "<f4"),
        ("mag", "<i2"),
        ("teff", "u1"),
        ("pad", "u1"),
    ]
)
assert _RENDER_DT.itemsize == 16


def _compute_render_and_level(
    morton_code: np.ndarray,
    positions: np.ndarray,
    mag_abs: np.ndarray,
    teff: np.ndarray,
    center: np.ndarray,
    half_size: float,
    mag_config: MagLevelConfig,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute render (N, 16) uint8 and level (N,) int32 from parquet columns. Fully vectorized per level band."""
    n = positions.shape[0]
    mag_abs = np.where(np.isfinite(mag_abs), mag_abs.astype(np.float64), 99.0)
    teff = np.where(np.isfinite(teff), teff.astype(np.float64), 5800.0)
    level_arr = mag_config.assign_level_array(mag_abs)

    teff_log8 = encode_teff(teff)
    render_out = np.zeros(n, dtype=_RENDER_DT)

    for level in np.unique(level_arr):
        level = int(level)
        indices_level = np.where(level_arr == level)[0]
        shift = 3 * (MORTON_BITS - level)
        node_ids = np.asarray(morton_code[indices_level], dtype=np.uint64) >> shift

        # De-interleave only unique node_ids (num_cells << N)
        unq_nodes, inv = np.unique(node_ids, return_inverse=True)
        m = len(unq_nodes)
        gx_u = np.zeros(m, dtype=np.uint32)
        gy_u = np.zeros(m, dtype=np.uint32)
        gz_u = np.zeros(m, dtype=np.uint32)
        for b in range(level):
            gx_u |= ((unq_nodes >> (3 * b)) & 1).astype(np.uint32) << b
            gy_u |= ((unq_nodes >> (3 * b + 1)) & 1).astype(np.uint32) << b
            gz_u |= ((unq_nodes >> (3 * b + 2)) & 1).astype(np.uint32) << b

        # Vectorized cell centers for all unique cells, then expand per-star via inv
        hs = max(half_size / (2**level), 1e-20)
        cell_size = 2.0 * hs
        cx_u = center[0] + (gx_u.astype(np.float64) + 0.5) * cell_size - half_size
        cy_u = center[1] + (gy_u.astype(np.float64) + 0.5) * cell_size - half_size
        cz_u = center[2] + (gz_u.astype(np.float64) + 0.5) * cell_size - half_size

        cx_s = cx_u[inv]
        cy_s = cy_u[inv]
        cz_s = cz_u[inv]

        pos_band = positions[indices_level]
        rec = render_out[indices_level]
        rec["x"] = np.clip((pos_band[:, 0] - cx_s) / hs, -1.0, 1.0)
        rec["y"] = np.clip((pos_band[:, 1] - cy_s) / hs, -1.0, 1.0)
        rec["z"] = np.clip((pos_band[:, 2] - cz_s) / hs, -1.0, 1.0)
        rec["mag"] = np.clip(np.round(mag_abs[indices_level] * 100.0), -32768, 32767)
        rec["teff"] = teff_log8[indices_level]
        render_out[indices_level] = rec

    render_bytes = np.ascontiguousarray(render_out.view(np.uint8).reshape(n, 16))
    assert render_bytes.flags["C_CONTIGUOUS"], (
        "render buffer must be C-contiguous for pa.py_buffer"
    )
    return render_bytes, level_arr


def _make_fixed_size_binary_column(render: np.ndarray) -> pa.Array:
    """Build a Parquet column of 16-byte values from (n, 16) uint8 array.
    pa.binary(16) is the fixed-size binary type in PyArrow (>= 0.17).
    """
    n = len(render)
    ty = pa.binary(16)
    return pa.FixedSizeBinaryArray.from_buffers(ty, n, [None, pa.py_buffer(render)])


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
    max_level: int | None,
) -> MagLevelConfig:
    if mag_config is not None:
        return mag_config
    vm = DEFAULT_MAG_VIS if v_mag is None else v_mag
    ml = DEFAULT_MAX_LEVEL if max_level is None else max_level
    if vm == DEFAULT_MAG_VIS and ml == DEFAULT_MAX_LEVEL:
        return LEVEL_CONFIG
    return MagLevelConfig(
        v_mag=vm,
        world_half_size=WORLD_HALF_SIZE_PC,
        max_level=ml,
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


def _enrich_table(
    table: pa.Table,
    *,
    mag_config: MagLevelConfig,
    center: np.ndarray,
    half_size: float,
) -> pa.Table:
    names = set(table.schema.names)
    if "teff" not in names:
        table = table.append_column(
            "teff",
            pa.array(np.full(len(table), 5800.0, dtype=np.float64)),
        )
        names = set(table.schema.names)

    x = np.asarray(table.column("x_icrs_pc"), dtype=np.float64)
    y = np.asarray(table.column("y_icrs_pc"), dtype=np.float64)
    z = np.asarray(table.column("z_icrs_pc"), dtype=np.float64)
    morton_code = morton3d_u64_from_xyz_arrays(x, y, z)
    positions = np.column_stack([x, y, z])
    mag_abs = np.asarray(table.column("mag_abs"), dtype=np.float64)
    teff = np.asarray(table.column("teff"), dtype=np.float64)
    render, level = _compute_render_and_level(
        morton_code, positions, mag_abs, teff, center, half_size, mag_config
    )

    for col in ("morton_code", "render", "level"):
        if col in names:
            table = table.drop([col])
    table = (
        table.append_column("morton_code", pa.array(morton_code, type=pa.uint64()))
        .append_column("render", _make_fixed_size_binary_column(render))
        .append_column("level", pa.array(level, type=pa.int32()))
    )
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
    max_level: int | None = None,
    verbose: bool = True,
) -> tuple[int, int]:
    """
    Enrich HEALPix-sharded parquet from ``src_root`` to ``output_root``.

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
        max_level=max_level,
    )
    center = WORLD_CENTER.copy()
    half_size = WORLD_HALF_SIZE_PC
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
                    enriched = _enrich_table(
                        table,
                        mag_config=mag_config,
                        center=center,
                        half_size=half_size,
                    )
                    run_path = pixel_tmp_dir / f"{run_count:08d}.parquet"
                    pq.write_table(enriched, run_path, compression=compression)
                    run_count += 1
                    row_count += len(enriched)

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
