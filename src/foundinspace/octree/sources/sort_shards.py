"""Sort parquet by morton_code, mag_abs and split into three magnitude bands.

Uses MagLevelConfig (same as octree build) so bands match level 11, 12, 13+:
  bright: mag_abs <= lev_11.m_max (brighter than level 12)
  medium: lev_12 band only (mag_abs > lev_11.m_max AND mag_abs <= lev_12.m_max)
  faint:  mag_abs > lev_12.m_max (level 13 or fainter)

Lower bound exclusive, upper inclusive, consistent with mag_levels.py.

Invoked from foundinspace.octree._cli (stage 00); not a standalone script.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from ..config import LEVEL_CONFIG
from ..duckdb_util import configure_connection
from ..mag_levels import MagLevelConfig


def run_sort_shards(
    src_root: Path,
    dst_root: Path,
    *,
    mag_config: MagLevelConfig | None = None,
    clear_dst: bool = True,
    verbose: bool = True,
) -> None:
    """
    For each ``*.parquet`` file directly under ``src_root``, write three band directories
    under ``dst_root`` named ``{stem}-bright``, ``{stem}-medium``, ``{stem}-faint``,
    each sorted by ``morton_code, mag_abs``.

    Requires ``morton_code`` on input rows (e.g. after ``run_add_shard_columns``).
    """
    if not src_root.is_dir():
        raise NotADirectoryError(f"Not a directory: {src_root}")

    mag_config = mag_config or LEVEL_CONFIG
    lev_11 = mag_config.get_level(11)
    lev_12 = mag_config.get_level(12)
    if lev_11 is None or lev_12 is None:
        raise RuntimeError("MagLevelConfig must have levels 11 and 12 (check max_level)")

    m11_max = lev_11.m_max
    m12_max = lev_12.m_max

    if clear_dst and dst_root.exists():
        shutil.rmtree(dst_root)
    dst_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    configure_connection(con)
    try:
        for src_file in sorted(src_root.glob("*.parquet")):
            run_name = src_file.stem
            src_path = src_file.as_posix()

            con.execute(
                f"CREATE OR REPLACE VIEW input_stars AS SELECT * FROM read_parquet('{src_path}')"
            )
            row_count = con.execute("SELECT count(*) FROM input_stars").fetchone()[0]

            for band, where_sql, band_name, partition_opt in [
                ("bright", f"mag_abs <= {m11_max}", "BRIGHT", "FILE_SIZE_BYTES '1GB'"),
                ("medium", f"mag_abs > {m11_max} AND mag_abs <= {m12_max}", "MEDIUM", "FILE_SIZE_BYTES '1GB'"),
                ("faint", f"mag_abs > {m12_max}", "FAINT", "FILE_SIZE_BYTES '1GB'"),
            ]:
                tmp_dir = dst_root / f".tmp-{run_name}-{band}"
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir)
                out_dir = dst_root / f"{run_name}-{band}"
                if out_dir.exists():
                    shutil.rmtree(out_dir)

                if verbose:
                    print(f"sorting {src_file.name} ({row_count:,} rows) - {band_name}")
                con.execute(f"""
                    COPY (
                        SELECT
                            *
                        FROM input_stars
                        WHERE {where_sql}
                        ORDER BY morton_code, mag_abs
                    )
                    TO '{tmp_dir.as_posix()}'
                    (
                        FORMAT parquet,
                        CODEC zstd,
                        ROW_GROUP_SIZE 122880,
                        PER_THREAD_OUTPUT false,
                        {partition_opt}
                    );
                """)
                tmp_dir.rename(out_dir)
    finally:
        con.close()
