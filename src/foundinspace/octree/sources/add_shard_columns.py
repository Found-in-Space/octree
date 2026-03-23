#!/usr/bin/env python3
"""Add morton_code, render (16-byte fixed), and level (int32) to parquet files in-place.

Computes morton_code from x_icrs_pc, y_icrs_pc, z_icrs_pc using world geometry in
foundinspace.octree.config and foundinspace.octree.encoding.morton (no upstream morton).

Derives level from mag_abs via MagLevelConfig; gets cell (gx, gy, gz) from
morton_code >> (3 * (MORTON_BITS - level)), then cell-relative quantized render bytes.

Writes to a temp file and renames to replace the original. Skip files that already have
morton_code, render, and level unless ``force`` is True.

Run before ``run_sort_shards`` (stage 00) when input lacks morton_code; see docs/pipeline-stage-00.md.

Invoked from ``foundinspace.octree._cli``; use ``uv run fis-octree stage-00 ...``.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.config import (
    DEFAULT_MAX_LEVEL,
    DEFAULT_MAG_VIS,
    LEVEL_CONFIG,
    MORTON_BITS,
    WORLD_CENTER,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.encoding.morton import morton3d_u64_from_xyz_arrays
from foundinspace.octree.encoding.teff import encode_teff

# Numpy dtypes for vectorized pack/unpack (16-byte render, 10-byte meta)
_RENDER_DT = np.dtype([
    ("x", "<f4"),
    ("y", "<f4"),
    ("z", "<f4"),
    ("mag", "<i2"),
    ("teff", "u1"),
    ("pad", "u1"),
])
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

    max_L = int(level_arr.max()) if n > 0 else 0
    if max_L > 13:
        raise NotImplementedError(
            f"Max level {max_L} exceeds 13. This script is currently limited to level ≤ 13. "
            f"Higher levels require uint64 for node_id handling — extend and re-enable if needed."
        )

    teff_log8 = encode_teff(teff)
    render_out = np.zeros(n, dtype=_RENDER_DT)

    for L in np.unique(level_arr):
        L = int(L)
        indices_L = np.where(level_arr == L)[0]
        shift = 3 * (MORTON_BITS - L)
        node_ids = np.asarray(morton_code[indices_L], dtype=np.uint64) >> shift

        # De-interleave only unique node_ids (num_cells << N)
        unq_nodes, inv = np.unique(node_ids, return_inverse=True)
        m = len(unq_nodes)
        gx_u = np.zeros(m, dtype=np.uint32)
        gy_u = np.zeros(m, dtype=np.uint32)
        gz_u = np.zeros(m, dtype=np.uint32)
        for b in range(L):
            gx_u |= ((unq_nodes >> (3 * b)) & 1).astype(np.uint32) << b
            gy_u |= ((unq_nodes >> (3 * b + 1)) & 1).astype(np.uint32) << b
            gz_u |= ((unq_nodes >> (3 * b + 2)) & 1).astype(np.uint32) << b

        # Vectorized cell centers for all unique cells, then expand per-star via inv
        hs = max(half_size / (2**L), 1e-20)
        cell_size = 2.0 * hs
        cx_u = center[0] + (gx_u.astype(np.float64) + 0.5) * cell_size - half_size
        cy_u = center[1] + (gy_u.astype(np.float64) + 0.5) * cell_size - half_size
        cz_u = center[2] + (gz_u.astype(np.float64) + 0.5) * cell_size - half_size

        cx_s = cx_u[inv]
        cy_s = cy_u[inv]
        cz_s = cz_u[inv]

        pos_band = positions[indices_L]
        rec = render_out[indices_L]
        rec["x"] = np.clip((pos_band[:, 0] - cx_s) / hs, -1.0, 1.0)
        rec["y"] = np.clip((pos_band[:, 1] - cy_s) / hs, -1.0, 1.0)
        rec["z"] = np.clip((pos_band[:, 2] - cz_s) / hs, -1.0, 1.0)
        rec["mag"] = np.clip(np.round(mag_abs[indices_L] * 100.0), -32768, 32767)
        rec["teff"] = teff_log8[indices_L]
        render_out[indices_L] = rec

    render_bytes = np.ascontiguousarray(render_out.view(np.uint8).reshape(n, 16))
    assert render_bytes.flags["C_CONTIGUOUS"], "render buffer must be C-contiguous for pa.py_buffer"
    return render_bytes, level_arr


def _make_fixed_size_binary_column(render: np.ndarray) -> pa.Array:
    """Build a Parquet column of 16-byte values from (n, 16) uint8 array.
    pa.binary(16) is the fixed-size binary type in PyArrow (>= 0.17).
    """
    n = len(render)
    ty = pa.binary(16)
    return pa.FixedSizeBinaryArray.from_buffers(
        ty, n, [None, pa.py_buffer(render)]
    )


def _compression_from_metadata(file_meta) -> str:
    """Extract compression codec from already-read parquet metadata."""
    if file_meta.num_row_groups == 0:
        return "snappy"
    col0 = file_meta.row_group(0).column(0)
    codec = getattr(col0, "compression", None)
    if codec is None:
        return "snappy"
    return str(codec).lower()


def process_file(
    path: Path,
    mag_config: MagLevelConfig,
    force: bool,
    center: np.ndarray,
    half_size: float,
) -> tuple[bool, int, float]:
    """Process one parquet file: add morton_code, render, and level; write in-place.
    Returns (written, n_stars, elapsed_sec); if skipped, (False, 0, 0.0)."""
    file_meta = pq.read_metadata(path)
    schema = file_meta.schema.to_arrow_schema()
    names = set(schema.names)
    if (
        "render" in names
        and "level" in names
        and "morton_code" in names
        and not force
    ):
        return (False, 0, 0.0)
    required = {"x_icrs_pc", "y_icrs_pc", "z_icrs_pc", "mag_abs"}
    missing = required - names
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")

    compression = _compression_from_metadata(file_meta)
    t0 = time.perf_counter()
    table = pq.read_table(path)

    if "teff" not in names:
        table = table.append_column(
            "teff",
            pa.array(np.full(len(table), 5800.0, dtype=np.float64)),
        )

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
    n_stars = len(render)
    for col in ("morton_code", "render", "level"):
        if col in names:
            table = table.drop([col])
    morton_col = pa.array(morton_code, type=pa.uint64())
    render_col = _make_fixed_size_binary_column(render)
    level_col = pa.array(level, type=pa.int32())
    table = (
        table.append_column("morton_code", morton_col)
        .append_column("render", render_col)
        .append_column("level", level_col)
    )

    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression=compression)
    os.replace(tmp, path)
    elapsed = time.perf_counter() - t0
    return (True, n_stars, elapsed)


def run_add_shard_columns(
    data_dir: Path,
    *,
    mag_config: MagLevelConfig | None = None,
    force: bool = False,
    v_mag: float | None = None,
    max_level: int | None = None,
    verbose: bool = True,
) -> tuple[int, int]:
    """
    Walk ``data_dir`` recursively for ``*.parquet`` and add morton_code, render, level in-place.

    If ``mag_config`` is given, it is used for level assignment. Otherwise ``v_mag`` / ``max_level``
    (defaulting to config defaults) determine a ``MagLevelConfig``.

    Returns ``(written_count, skipped_count)``.
    """
    if not data_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {data_dir}")

    if mag_config is None:
        vm = DEFAULT_MAG_VIS if v_mag is None else v_mag
        ml = DEFAULT_MAX_LEVEL if max_level is None else max_level
        if vm == DEFAULT_MAG_VIS and ml == DEFAULT_MAX_LEVEL:
            mag_config = LEVEL_CONFIG
        else:
            mag_config = MagLevelConfig(
                v_mag=vm,
                world_half_size=WORLD_HALF_SIZE_PC,
                max_level=ml,
            )
    center = WORLD_CENTER.copy()
    half_size = WORLD_HALF_SIZE_PC

    paths = sorted(data_dir.rglob("*.parquet"))
    if not paths:
        if verbose:
            print(f"No .parquet files under {data_dir}")
        return (0, 0)

    written = 0
    skipped = 0
    for p in paths:
        if verbose:
            label = p.relative_to(data_dir)
            print(f"Processing {label}....", end="", flush=True)
        ok, n_stars, elapsed = process_file(p, mag_config, force, center, half_size)
        if ok:
            if verbose:
                print(f" Wrote {n_stars:,} stars in {elapsed:.1f}s")
            written += 1
        else:
            if verbose:
                print(" Skipped (already has morton_code+render+level).")
            skipped += 1
    if verbose:
        print(f"Done: {written} written, {skipped} skipped.")
    return (written, skipped)
