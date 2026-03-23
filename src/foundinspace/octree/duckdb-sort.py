"""Sort parquet by morton_code, mag_abs and split into three magnitude bands.

Uses MagLevelConfig (same as octree build) so bands match level 11, 12, 13+:
  bright: mag_abs <= lev_11.m_max (brighter than level 12)
  medium: lev_12 band only (mag_abs > lev_11.m_max AND mag_abs <= lev_12.m_max)
  faint:  mag_abs > lev_12.m_max (level 13 or fainter)

Lower bound exclusive, upper inclusive, consistent with mag_levels.py.
"""
from pathlib import Path
import shutil

import duckdb

from three_dee.duckdb_util import configure_connection
from three_dee.config import DEFAULT_MAX_LEVEL, WORLD_HALF_SIZE_PC, LEVEL_CONFIG

PARTITION_LEVEL = 13
PARTITION_SHIFT = 3 * (21 - PARTITION_LEVEL) 


con = duckdb.connect()
configure_connection(con)

# Level bounds from same config as octree build (lower exclusive, upper inclusive)
mag_config = LEVEL_CONFIG
lev_11 = mag_config.get_level(11)
lev_12 = mag_config.get_level(12)
if lev_11 is None or lev_12 is None:
    raise RuntimeError("MagLevelConfig must have levels 11 and 12 (check max_level)")

m11_max = lev_11.m_max  # bright: mag_abs <= m11_max
m12_max = lev_12.m_max  # medium: mag_abs > m11_max AND mag_abs <= m12_max; faint: mag_abs > m12_max

src_root = Path('/data/astro/gaia-processed')
dst_root = Path('/data/astro/gaia-processed-sorted')
if dst_root.exists():
    shutil.rmtree(dst_root)
dst_root.mkdir(parents=True, exist_ok=True)

for src_file in sorted(src_root.glob('*.parquet')):
    run_name = src_file.stem
    dst_dir = dst_root / run_name
    src_path = src_file.as_posix()

    # One view per file so we don't repeat read_parquet in each query
    con.execute(f"CREATE OR REPLACE VIEW input_stars AS SELECT * FROM read_parquet('{src_path}')")
    row_count = con.execute("SELECT count(*) FROM input_stars").fetchone()[0]

    for band, where_sql, band_name, PARTITION in [
        ("bright", f"mag_abs <= {m11_max}", "BRIGHT", "FILE_SIZE_BYTES '1GB'"),
        ("medium", f"mag_abs > {m11_max} AND mag_abs <= {m12_max}", "MEDIUM", "FILE_SIZE_BYTES '1GB'"),
        ("faint", f"mag_abs > {m12_max}", "FAINT", "FILE_SIZE_BYTES '1GB'"),
    ]:
        tmp_dir = dst_root / f'.tmp-{run_name}-{band}'
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        out_dir = dst_root / f"{run_name}-{band}"
        if out_dir.exists():
            shutil.rmtree(out_dir)

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
                {PARTITION}
            );
        """)
        tmp_dir.rename(out_dir)