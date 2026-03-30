from __future__ import annotations

import math
import time
from pathlib import Path

import duckdb

from ..duckdb_util import configure_connection
from .formats import (
    IDENTIFIERS_ARTIFACT_KIND,
    IDENTIFIERS_INDEX_MAGIC,
    IDENTIFIERS_MANIFEST_NAME,
    RENDER_ARTIFACT_KIND,
    RENDER_MANIFEST_NAME,
    INDEX_MAGIC,
)
from .identity_encoder import iter_encoded_cells_with_identities
from .manifest import manifest_entries, read_manifest, validate_shard, write_manifest
from .plan import BuildPlan
from .row_source import iter_sorted_rows
from .writer import IntermediateShardWriter, identifiers_shard_filenames


def _shard_id(level: int, prefix_bits: int, prefix: int) -> tuple[int, int, int]:
    return (level, prefix_bits, prefix)


def _check_input_columns(parquet_glob: str) -> None:
    escaped = parquet_glob.replace("'", "''")
    con = duckdb.connect()
    configure_connection(con)
    try:
        con.execute(
            f"SELECT render, level, morton_code, mag_abs, source, source_id "
            f"FROM read_parquet('{escaped}') LIMIT 0"
        )
    finally:
        con.close()


def _load_existing_manifest_entries(
    out_dir: Path,
    *,
    max_level: int,
    mag_limit: float,
) -> tuple[list[dict], list[dict], set[tuple[int, int, int]]]:
    render_manifest = read_manifest(out_dir, name=RENDER_MANIFEST_NAME)
    identifiers_manifest = read_manifest(out_dir, name=IDENTIFIERS_MANIFEST_NAME)

    if render_manifest is None and identifiers_manifest is None:
        if any(out_dir.iterdir()):
            raise FileExistsError(f"Output directory is not empty and has no manifest: {out_dir}")
        return [], [], set()
    if render_manifest is None or identifiers_manifest is None:
        raise ValueError("Stage 01 resume requires both render and identifiers manifests")

    render_entries_list = manifest_entries(render_manifest)
    identifiers_entries_list = manifest_entries(identifiers_manifest)

    for manifest in (render_manifest, identifiers_manifest):
        existing_max_level = int(manifest.get("max_level", -1))
        if existing_max_level != max_level:
            raise ValueError(
                "Existing manifest max_level does not match build plan: "
                f"{existing_max_level} != {max_level}"
            )
        if "mag_limit" in manifest:
            existing_mag = float(manifest["mag_limit"])
            if not math.isclose(existing_mag, mag_limit, rel_tol=0.0, abs_tol=1e-12):
                raise ValueError(
                    "Existing manifest mag_limit does not match build plan: "
                    f"{existing_mag} != {mag_limit}"
                )

    render_by_shard = {
        _shard_id(entry["level"], entry["prefix_bits"], entry["prefix"]): entry
        for entry in render_entries_list
    }
    identifiers_by_shard = {
        _shard_id(entry["level"], entry["prefix_bits"], entry["prefix"]): entry
        for entry in identifiers_entries_list
    }
    if set(render_by_shard) != set(identifiers_by_shard):
        raise ValueError("Render and identifiers manifests contain different completed shards")

    completed_shards: set[tuple[int, int, int]] = set()
    for shard_key in sorted(render_by_shard):
        render_entry = render_by_shard[shard_key]
        identifiers_entry = identifiers_by_shard[shard_key]
        validate_shard(out_dir, render_entry, expected_magic=INDEX_MAGIC)
        validate_shard(
            out_dir,
            identifiers_entry,
            expected_magic=IDENTIFIERS_INDEX_MAGIC,
        )
        if render_entry["record_count"] != identifiers_entry["record_count"]:
            raise ValueError(
                "Render / identifiers shard record_count mismatch: "
                f"{render_entry['record_count']} != {identifiers_entry['record_count']}"
            )
        completed_shards.add(shard_key)

    return render_entries_list, identifiers_entries_list, completed_shards


def build_intermediates(
    parquet_glob: str,
    out_dir: Path,
    *,
    plan: BuildPlan,
) -> Path:
    start_t = time.perf_counter()
    plan.validate()

    out_dir.mkdir(parents=True, exist_ok=True)

    print("Stage 01: validating input columns...", flush=True)
    _check_input_columns(parquet_glob)
    print("Stage 01: input columns OK.", flush=True)

    render_entries_list, identifiers_entries_list, completed_shards = (
        _load_existing_manifest_entries(
            out_dir,
            max_level=plan.max_level,
            mag_limit=plan.mag_limit,
        )
    )
    if completed_shards:
        print(
            f"Stage 01: resuming from existing manifests with "
            f"{len(completed_shards)} completed shard(s).",
            flush=True,
        )

    shard_total = 0
    shard_non_empty = len(render_entries_list)
    skipped_shards = 0
    total_cells = 0

    for level in range(plan.max_level + 1):
        shard_keys = plan.shard_keys_for_level(level)
        print(
            f"Stage 01: level {level}/{plan.max_level} ({len(shard_keys)} shard(s))...",
            flush=True,
        )

        for shard_i, shard in enumerate(shard_keys, start=1):
            shard_total += 1
            shard_key_id = _shard_id(
                shard.level,
                shard.prefix_bits,
                shard.prefix,
            )
            if shard_key_id in completed_shards:
                skipped_shards += 1
                print(
                    "Stage 01: shard already complete in manifests, skipping.",
                    flush=True,
                )
                continue

            print(
                f"Stage 01: shard {shard_i}/{len(shard_keys)} at level {level} "
                f"(prefix_bits={shard.prefix_bits}, prefix={shard.prefix})",
                flush=True,
            )
            render_writer = IntermediateShardWriter(shard, out_dir)
            identifiers_writer = IntermediateShardWriter(
                shard,
                out_dir,
                index_magic=IDENTIFIERS_INDEX_MAGIC,
                filename_fn=identifiers_shard_filenames,
            )
            shard_cells = 0
            try:
                rows = iter_sorted_rows(
                    parquet_glob,
                    level=level,
                    shard=shard,
                    batch_size=plan.batch_size,
                )
                for render_cell, identifiers_cell in iter_encoded_cells_with_identities(
                    rows, level
                ):
                    render_writer.write_cell(render_cell)
                    identifiers_writer.write_cell(identifiers_cell)
                    shard_cells += 1

                render_manifest = render_writer.close()
                identifiers_manifest = identifiers_writer.close()
                total_cells += shard_cells
                if render_manifest is not None or identifiers_manifest is not None:
                    if render_manifest is None or identifiers_manifest is None:
                        raise ValueError(
                            "Render / identifiers shard presence mismatch during Stage 01"
                        )
                    if render_manifest["record_count"] != identifiers_manifest["record_count"]:
                        raise ValueError(
                            "Render / identifiers shard record_count mismatch: "
                            f"{render_manifest['record_count']} != "
                            f"{identifiers_manifest['record_count']}"
                        )
                    render_entries_list.append(render_manifest)
                    identifiers_entries_list.append(identifiers_manifest)
                    completed_shards.add(shard_key_id)
                    shard_non_empty += 1
                    write_manifest(
                        out_dir,
                        plan.max_level,
                        render_entries_list,
                        artifact_kind=RENDER_ARTIFACT_KIND,
                        index_magic=INDEX_MAGIC,
                        mag_limit=plan.mag_limit,
                        name=RENDER_MANIFEST_NAME,
                    )
                    write_manifest(
                        out_dir,
                        plan.max_level,
                        identifiers_entries_list,
                        artifact_kind=IDENTIFIERS_ARTIFACT_KIND,
                        index_magic=IDENTIFIERS_INDEX_MAGIC,
                        mag_limit=plan.mag_limit,
                        name=IDENTIFIERS_MANIFEST_NAME,
                    )
                    print(
                        f"Stage 01: shard complete ({shard_cells} cell(s), "
                        f"{render_manifest['record_count']} record(s)).",
                        flush=True,
                    )
                else:
                    print("Stage 01: shard complete (empty).", flush=True)
            except Exception:
                render_writer.abort()
                identifiers_writer.abort()
                raise

    print(
        "Stage 01: writing manifests "
        f"({len(render_entries_list)} non-empty shard(s))...",
        flush=True,
    )
    render_manifest_path = write_manifest(
        out_dir,
        plan.max_level,
        render_entries_list,
        artifact_kind=RENDER_ARTIFACT_KIND,
        index_magic=INDEX_MAGIC,
        mag_limit=plan.mag_limit,
        name=RENDER_MANIFEST_NAME,
    )
    write_manifest(
        out_dir,
        plan.max_level,
        identifiers_entries_list,
        artifact_kind=IDENTIFIERS_ARTIFACT_KIND,
        index_magic=IDENTIFIERS_INDEX_MAGIC,
        mag_limit=plan.mag_limit,
        name=IDENTIFIERS_MANIFEST_NAME,
    )
    elapsed = time.perf_counter() - start_t
    print(
        f"Stage 01: done in {elapsed:.1f}s "
        f"(levels={plan.max_level + 1}, shards={shard_total}, "
        f"non_empty_shards={shard_non_empty}, skipped_shards={skipped_shards}, "
        f"new_cells={total_cells}).",
        flush=True,
    )
    return render_manifest_path
