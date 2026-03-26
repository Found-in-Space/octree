"""Filesystem path defaults for octree pipeline artifacts.

Environment variables:
- FIS_PROCESSED_DIR (default: data/processed)
- FIS_OCTREE_DIR (default: data/octree)
"""

from __future__ import annotations

from pathlib import Path

from decouple import config

PROCESSED_DIR = Path(config("FIS_PROCESSED_DIR", default="data/processed")).expanduser()
OCTREE_DIR = Path(config("FIS_OCTREE_DIR", default="data/octree")).expanduser()

MERGED_HEALPIX_DIR = PROCESSED_DIR / "merged" / "healpix"
IDENTIFIERS_MAP_PATH = PROCESSED_DIR / "identifiers_map.parquet"
STAGE00_OUTPUT_DIR = OCTREE_DIR / "stage00"
STAGE01_DIR = OCTREE_DIR / "stage01"
STAGE01_MANIFEST_PATH = STAGE01_DIR / "manifest.json"
STAGE02_OUTPUT = OCTREE_DIR / "stars.octree"
# Glob for DuckDB read_parquet (all Stage 00 shards under pixel dirs).
STAGE00_PARQUET_GLOB = (STAGE00_OUTPUT_DIR / "**" / "*.parquet").as_posix()
