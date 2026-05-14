# Stage 00: Packed Octree Staging

## Purpose

Stage 00 prepares merged HEALPix parquet for octree assembly by computing missing
octree columns and routing rows into a packed staging tree.

The input is HEALPix-sharded parquet under:

- `.../merged/healpix/{pixel}/*.parquet`

The output is an octree-shaped staging filesystem under:

- `.../octree/stage00/tree/o={octant}/.../*.parquet`

Each fragment filename keeps the source HEALPix pixel, so a pixel can be deleted
and reprocessed without scanning unrelated pixels:

- `hp448-pack-000001.parquet`
- `hp448-lim-000001.parquet`

## Packing Semantics

Every row still receives the same final octree `level` from the configured
magnitude logic. Stage 00 only changes the staging layout.

A staging node starts in packed mode. Rows whose final level is at or below that
node may be written there until the node reaches `stage00.bucket_size`. Once the
cap is reached, the node becomes lower-mag limited:

1. Stage 00 writes `_LOWER_MAG_LIMITED` in the node directory.
2. Existing `pack` fragments in that node are read once and deleted.
3. Rows whose final level equals the node depth are rewritten as `lim` fragments.
4. Fainter rows are routed into child octants, which repeat the same process.

This keeps sparse fields shallow while avoiding repeated re-indexing of the same
node. The cap is intentionally build-defining: changing it mid-run changes the
intermediate layout.

## Computed Columns

If input parquet is not already enriched, Stage 00 adds:

- `morton_code` (`uint64`)
- `render` (fixed 16-byte payload)
- `level` (`int32`)

Required raw input columns:

- `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`
- `mag_abs`

For downstream sidecars and stable identity joins, merged input should also
include:

- `source`
- `source_id`

Optional:

- `teff` (if absent, defaults inside render encoding)

All other columns are preserved. HEALPix partition columns such as `healpix`,
`healpix_id`, or `hp` are dropped because the pixel is encoded in the fragment
filename.

## Project Configuration

Required project-file values for Stage 00:

- `paths.merged_healpix_dir`
- `paths.stage00_output_dir`
- `stage00.batch_size`
- `stage00.v_mag`
- `stage00.max_level`
- `stage00.bucket_size`
- `stage00.fragment_target_rows`
- `stage00.max_open_writers`
- `stage00.compact_after_files`

Project-file path rules:

- paths may be absolute
- relative paths are resolved from the project file directory
- environment-variable expansion is not supported in TOML values

## CLI

```bash
uv run fis-octree stage-00 --project path/to/project.toml --force
```

Useful probe options:

- `--healpix 448`: process only one HEALPix directory; may be repeated.
- `--max-pixels N`: process the first `N` HEALPix directories.
- `--bucket-size N`: override `stage00.bucket_size` for this run.
- `--fragment-target-rows N`: roll physical parquet fragments at this row count.
- `--max-open-writers N`: cap concurrently open parquet writers.
- `--compact-after-files N`: compact a node/healpix/kind group after this many files.

## Output Report

Stage 00 writes `stage00-report.json` with row counts, node counts, lower-mag
limited node counts, fragment counts, split rewrites, compaction rewrites, and a
depth summary.

## Non-Goals

- No catalog reconciliation, duplicate resolution, crossmatch policy, or manual
  override logic.
- No semantic change to the final magnitude-derived octree level.
- No mutation of the source merge dataset.
