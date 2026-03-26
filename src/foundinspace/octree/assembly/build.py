from __future__ import annotations

import math
import time
from pathlib import Path

import duckdb

from ..duckdb_util import configure_connection
from .encoder import iter_encoded_cells
from .formats import META_INDEX_MAGIC
from .manifest import manifest_entries, read_manifest, validate_shard, write_manifest
from .meta_encoder import IdentifiersMap, iter_encoded_cells_with_meta
from .plan import BuildPlan
from .row_source import iter_sorted_rows
from .types import EncodedCell
from .writer import IntermediateShardWriter, meta_shard_filenames


def _shard_id(level: int, prefix_bits: int, prefix: int) -> tuple[int, int, int]:
    return (level, prefix_bits, prefix)


def _check_input_columns(parquet_glob: str) -> None:
    """Fail fast if required columns are absent from the input parquet."""
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


def build_intermediates(
    parquet_glob: str,
    out_dir: Path,
    *,
    plan: BuildPlan,
    identifiers_map_path: Path | None = None,
    sidecar_fields: list[str] | None = None,
) -> Path:
    """Build intermediate shard files and return the path to manifest.json.

    When ``identifiers_map_path`` is set, writes ``.meta-index`` / ``.meta-payload``
    alongside each render shard and adds ``meta_*`` paths to the manifest.
    """
    start_t = time.perf_counter()
    plan.validate()

    out_dir.mkdir(parents=True, exist_ok=True)

    print("Stage 01: validating input columns...", flush=True)
    _check_input_columns(parquet_glob)
    print("Stage 01: input columns OK.", flush=True)

    ident_map: IdentifiersMap | None = None
    if identifiers_map_path is not None:
        ident_map = IdentifiersMap(
            identifiers_map_path,
            fields=sidecar_fields,
        )
        print(
            f"Stage 01: identifiers map loaded ({len(ident_map)} row(s)).",
            flush=True,
        )

    existing_manifest = read_manifest(out_dir)
    if existing_manifest is None and any(out_dir.iterdir()):
        raise FileExistsError(
            f"Output directory is not empty and has no manifest: {out_dir}"
        )

    manifest_entries_list: list[dict] = []
    completed_shards: set[tuple[int, int, int]] = set()
    if existing_manifest is not None:
        existing_max_level = int(existing_manifest.get("max_level", -1))
        if existing_max_level != plan.max_level:
            raise ValueError(
                "Existing manifest max_level does not match build plan: "
                f"{existing_max_level} != {plan.max_level}"
            )
        if "mag_limit" in existing_manifest:
            existing_mag = float(existing_manifest["mag_limit"])
            if not math.isclose(
                existing_mag, plan.mag_limit, rel_tol=0.0, abs_tol=1e-12
            ):
                raise ValueError(
                    "Existing manifest mag_limit does not match build plan: "
                    f"{existing_mag} != {plan.mag_limit}"
                )
        manifest_entries_list = manifest_entries(existing_manifest)
        for entry in manifest_entries_list:
            validate_shard(out_dir, entry)
            completed_shards.add(
                _shard_id(entry["level"], entry["prefix_bits"], entry["prefix"])
            )
        print(
            f"Stage 01: resuming from existing manifest with "
            f"{len(manifest_entries_list)} completed shard(s).",
            flush=True,
        )

    shard_total = 0
    shard_non_empty = len(manifest_entries_list)
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
                    "Stage 01: shard already complete in manifest, skipping.",
                    flush=True,
                )
                continue

            print(
                f"Stage 01: shard {shard_i}/{len(shard_keys)} at level {level} "
                f"(prefix_bits={shard.prefix_bits}, prefix={shard.prefix})",
                flush=True,
            )
            render_writer = IntermediateShardWriter(shard, out_dir)
            meta_writer: IntermediateShardWriter | None = None
            if ident_map is not None:
                meta_writer = IntermediateShardWriter(
                    shard,
                    out_dir,
                    index_magic=META_INDEX_MAGIC,
                    filename_fn=meta_shard_filenames,
                    manifest_index_key="meta_index_path",
                    manifest_payload_key="meta_payload_path",
                )
            shard_cells = 0
            try:
                rows = iter_sorted_rows(
                    parquet_glob,
                    level=level,
                    shard=shard,
                    batch_size=plan.batch_size,
                )
                if ident_map is None:
                    for cell in iter_encoded_cells(rows, level=level):
                        render_writer.write_cell(cell)
                        shard_cells += 1
                else:
                    assert meta_writer is not None
                    for render_cell, meta_blob in iter_encoded_cells_with_meta(
                        rows, level, ident_map
                    ):
                        render_writer.write_cell(render_cell)
                        meta_writer.write_cell(
                            EncodedCell(
                                key=render_cell.key,
                                payload=meta_blob,
                                star_count=render_cell.star_count,
                            )
                        )
                        shard_cells += 1

                render_manifest = render_writer.close()
                meta_manifest = meta_writer.close() if meta_writer is not None else None
                total_cells += shard_cells
                if render_manifest is not None:
                    if meta_manifest is not None:
                        if (
                            render_manifest["record_count"]
                            != meta_manifest["record_count"]
                        ):
                            raise ValueError(
                                "Render / meta shard record_count mismatch: "
                                f"{render_manifest['record_count']} != "
                                f"{meta_manifest['record_count']}"
                            )
                        shard_manifest = {
                            **render_manifest,
                            "meta_index_path": meta_manifest["meta_index_path"],
                            "meta_payload_path": meta_manifest["meta_payload_path"],
                        }
                    else:
                        shard_manifest = render_manifest
                    manifest_entries_list.append(shard_manifest)
                    completed_shards.add(shard_key_id)
                    shard_non_empty += 1
                    write_manifest(
                        out_dir,
                        plan.max_level,
                        manifest_entries_list,
                        mag_limit=plan.mag_limit,
                    )
                    print(
                        f"Stage 01: shard complete ({shard_cells} cell(s), "
                        f"{shard_manifest['record_count']} record(s)).",
                        flush=True,
                    )
                else:
                    print("Stage 01: shard complete (empty).", flush=True)
            except Exception:
                render_writer.abort()
                if meta_writer is not None:
                    meta_writer.abort()
                raise

    print(
        f"Stage 01: writing manifest ({len(manifest_entries_list)} non-empty shard(s))...",
        flush=True,
    )
    manifest_path = write_manifest(
        out_dir, plan.max_level, manifest_entries_list, mag_limit=plan.mag_limit
    )
    elapsed = time.perf_counter() - start_t
    print(
        f"Stage 01: done in {elapsed:.1f}s "
        f"(levels={plan.max_level + 1}, shards={shard_total}, "
        f"non_empty_shards={shard_non_empty}, skipped_shards={skipped_shards}, "
        f"new_cells={total_cells}).",
        flush=True,
    )
    return manifest_path
