from __future__ import annotations

import time
from pathlib import Path

import duckdb

from ..duckdb_util import configure_connection
from .encoder import iter_encoded_cells
from .manifest import write_manifest
from .plan import BuildPlan
from .row_source import iter_sorted_rows
from .writer import IntermediateShardWriter


def _check_input_columns(parquet_glob: str) -> None:
    """Fail fast if required columns are absent from the input parquet."""
    escaped = parquet_glob.replace("'", "''")
    con = duckdb.connect()
    configure_connection(con)
    try:
        con.execute(
            f"SELECT render, level, morton_code, mag_abs "
            f"FROM read_parquet('{escaped}') LIMIT 0"
        )
    finally:
        con.close()


def build_intermediates(
    parquet_glob: str,
    out_dir: Path,
    *,
    plan: BuildPlan,
) -> Path:
    """Build intermediate shard files and return the path to manifest.json."""
    start_t = time.perf_counter()
    plan.validate()

    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"Output directory is not empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Stage 01: validating input columns...", flush=True)
    _check_input_columns(parquet_glob)
    print("Stage 01: input columns OK.", flush=True)

    manifest_entries: list[dict] = []
    shard_total = 0
    shard_non_empty = 0
    total_cells = 0

    for level in range(plan.max_level + 1):
        shard_keys = plan.shard_keys_for_level(level)
        print(
            f"Stage 01: level {level}/{plan.max_level} "
            f"({len(shard_keys)} shard(s))...",
            flush=True,
        )

        for shard_i, shard in enumerate(shard_keys, start=1):
            shard_total += 1
            print(
                f"Stage 01: shard {shard_i}/{len(shard_keys)} at level {level} "
                f"(prefix_bits={shard.prefix_bits}, prefix={shard.prefix})",
                flush=True,
            )
            writer = IntermediateShardWriter(shard, out_dir)
            shard_cells = 0
            try:
                rows = iter_sorted_rows(
                    parquet_glob,
                    level=level,
                    shard=shard,
                    batch_size=plan.batch_size,
                )
                for cell in iter_encoded_cells(rows, level=level):
                    writer.write_cell(cell)
                    shard_cells += 1

                shard_manifest = writer.close()
                total_cells += shard_cells
                if shard_manifest is not None:
                    manifest_entries.append(shard_manifest)
                    shard_non_empty += 1
                    print(
                        f"Stage 01: shard complete ({shard_cells} cell(s), "
                        f"{shard_manifest['record_count']} record(s)).",
                        flush=True,
                    )
                else:
                    print("Stage 01: shard complete (empty).", flush=True)
            except Exception:
                writer.abort()
                raise

    print(
        f"Stage 01: writing manifest ({len(manifest_entries)} non-empty shard(s))...",
        flush=True,
    )
    manifest_path = write_manifest(out_dir, plan.max_level, manifest_entries)
    elapsed = time.perf_counter() - start_t
    print(
        f"Stage 01: done in {elapsed:.1f}s "
        f"(levels={plan.max_level + 1}, shards={shard_total}, "
        f"non_empty_shards={shard_non_empty}, cells={total_cells}).",
        flush=True,
    )
    return manifest_path
