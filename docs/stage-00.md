# Stage 00: Per-Pixel Streaming Enrichment

## Purpose

Stage 00 prepares merge output for Stage 01 by adding:

- `morton_code` (`uint64`)
- `render` (fixed 16-byte payload)
- `level` (`int32`)

The input is expected to be HEALPix-sharded parquet under:

- `.../merged/healpix/{pixel}/*.parquet`

Stage 00 processes one pixel directory at a time and writes enriched parquet to:

- `.../octree/stage00/{pixel}/*.parquet`

Each output parquet part is sorted by `morton_code, mag_abs`.

## Stage boundary

Stage 00 starts from already merged, HEALPix-sharded parquet input.

It does not perform catalog reconciliation tasks such as duplicate resolution, crossmatch decisions, or override policy. Its scope starts at per-row octree enrichment (`morton_code`, `render`, `level`) and file-local ordering for Stage 01.

## Why this design

- **Non-destructive**: source merge files are never modified in-place.
- **Bounded memory**: rows are processed in batches (`--batch-size`, default `1_000_000`).
- **Disk-efficient**: no full-dataset intermediate copy; only per-pixel temporary run files.
- **Stage 01-compatible**: output includes `render`, `level`, `morton_code`, and `mag_abs`.

## Execution model

For each pixel directory:

1. Read source parquet in streaming batches.
2. Compute `morton_code` from `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`.
3. Compute `render` and `level` from Morton/position/magnitude/temperature.
4. Sort each batch by `morton_code, mag_abs`; write temporary batch runs.
5. DuckDB merge-sorts the temporary runs and writes final pixel output shards (~1 GB each).
6. Delete the pixel temporary runs.

This gives local Morton ordering inside every output file, which improves Stage 01 row-group skipping.

## CLI

```bash
uv run fis-octree stage-00 [INPUT_DIR] [OUTPUT_DIR] [options]
```

Defaults come from `foundinspace.octree.paths`:

- `INPUT_DIR`: `FIS_PROCESSED_DIR/merged/healpix`
- `OUTPUT_DIR`: `FIS_OCTREE_DIR/stage00`

Options:

- `--force`: recompute pixels already present in output
- `--batch-size N`: rows per batch (default `1000000`)
- `--v-mag F`: indexing magnitude (default from config)
- `--max-level N`: max octree level (default from config)

## Required input columns

- `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`
- `mag_abs`

For downstream sidecars and stable ordering, merged input should also include:

- `source`
- `source_id`

Optional:

- `teff` (if absent, defaults to `5800.0` for encoding)

All other columns are preserved.

## Output contract for Stage 01

Stage 01 expects:

- `render`
- `level`
- `morton_code`
- `mag_abs`

Stage 00 guarantees these columns are present on output parquet.

## Non-goals

- No global sort across all pixels (Stage 01 handles query ordering).
- No bright/medium/faint directory split at Stage 00.
- No mutation of the source merge dataset.
