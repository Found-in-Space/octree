# Pipeline stage 00: Prepare shard parquet

## Purpose

Stage 00 turns **HEALPix- (or otherwise) sharded** star parquet from upstream into files that **stage 01** can stream: same world geometry for Morton codes, precomputed per-star fields where needed, and **per-file** sort order suitable for DuckDB queries that group by `node_id`.

Stage 00 is **not** a global spatial merge: each input file is processed independently. Octree cells still span many files; stage 01’s row source is responsible for cross-shard queries and ordering.

**Next:** [pipeline-stage-01.md](pipeline-stage-01.md) (build intermediates).

---

## Execution order

1. **`add_shard_columns`** — must run when input parquet has **no** `morton_code` (or you want to re-derive it from ICRS for consistency with `foundinspace.octree.config`).
2. **`sort_shards`** — requires `morton_code` on disk; sorts each source file’s rows and splits into magnitude bands.

Typical flow:

```text
upstream parquet (ICRS, mag_abs, …)
  → run_add_shard_columns  (adds morton_code, render, level; in-place)
  → run_sort_shards        (ORDER BY morton_code, mag_abs; band split)
  → input for stage 01
```

### Command-line entry point

Run both steps in order (same `MagLevelConfig` for render/level and for magnitude bands):

```bash
uv run fis-octree stage-00 INPUT_DIR OUTPUT_DIR [--force] [--v-mag F] [--max-level N] [--no-clear-output]
```

* **`INPUT_DIR`** — parquet tree updated **in place** with `morton_code`, `render`, `level` (recursive `**/*.parquet`).
* **`OUTPUT_DIR`** — receives sorted band outputs; by default it is **removed** first (`--no-clear-output` keeps existing contents).
* **`sort_shards`** only reads **`INPUT_DIR/*.parquet`** (files **directly** in that directory, not nested). Lay out upstream HEALPix files as siblings under `INPUT_DIR`, or adjust the code if you need `rglob`.

You can also run `python -m foundinspace.octree stage-00 …` (same options).

---

## 1. `add_shard_columns.py`

**Module:** `foundinspace.octree.sources.add_shard_columns`

**Role:** For each `.parquet` under a data directory (recursive), update files **in place** (write to `.tmp`, then replace).

### Required columns

* `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` — positions in pc in the same frame as `WORLD_CENTER` / `WORLD_HALF_SIZE_PC` in `foundinspace.octree.config`.
* `mag_abs`

### Optional columns

* `teff` — if missing, filled with `5800.0` for encoding.

### Derived columns (always written when processing)

| Column        | Type / layout | Source |
|---------------|----------------|--------|
| `morton_code` | `uint64`       | `foundinspace.octree.encoding.morton.morton3d_u64_from_xyz_arrays` — same quantization as historical pandas path (`normalize_axis` + `_spread21`). |
| `render`      | fixed 16 bytes | Cell-relative `float32` x,y,z in `[-1,1]`, `int16` mag×100, `uint8` log Teff + pad; level from `MagLevelConfig`. |
| `level`       | `int32`        | From `mag_abs` via `MagLevelConfig` (`LEVEL_CONFIG` or CLI `--v-mag` / `--max-level`). |

`morton_code` is computed from ICRS **only** (not read from input), so it stays aligned with `WORLD_HALF_SIZE_PC`, `MORTON_BITS`, and the spread/de-interleave logic used for `render`.

### Skip / force

* **Skip** if the file already has `morton_code`, `render`, and `level`, unless `force=True` / `fis-octree stage-00 … --force`.
* **`force`** drops and recomputes all three columns.

**API:** `run_add_shard_columns(data_dir, *, mag_config=..., force=..., verbose=...)`.

---

## 2. `sort_shards.py`

**Module:** `foundinspace.octree.sources.sort_shards`

**Role:** `run_sort_shards(src_root, dst_root, *, mag_config=..., clear_dst=..., verbose=...)`. For each ``*.parquet`` **file in the root of** ``src_root``, runs DuckDB `COPY` queries that:

1. Filter rows into three **magnitude bands** using `LEVEL_CONFIG` and levels 11 / 12 (same semantics as `mag_levels.py`: lower bound exclusive, upper inclusive).
2. **`ORDER BY morton_code, mag_abs`** within each band.
3. Write partitioned parquet output (e.g. ~1 GB files, zstd).

### Bands

| Band   | Predicate |
|--------|-----------|
| bright | `mag_abs <= m11_max` |
| medium | `mag_abs > m11_max AND mag_abs <= m12_max` |
| faint  | `mag_abs > m12_max` |

### Requirements

* Input must include **`morton_code`** — produced by stage 00 step 1 unless you already have a compatible column from elsewhere.

### Output layout

Under `dst_root`, per source stem: `{run_name}-bright`, `{run_name}-medium`, `{run_name}-faint` (each a directory of parquet parts after DuckDB `COPY`).

---

## Shared configuration

Both steps rely on **`foundinspace.octree.config`** for world extent and Morton depth:

* `WORLD_CENTER`, `WORLD_HALF_SIZE_PC`, `MORTON_BITS`
* `LEVEL_CONFIG` / `MagLevelConfig` for magnitude → level and band edges in `sort_shards`

Keep these identical to what stage 01 and the final octree build use.

---

## Non-goals (stage 00)

* Does not assign stars to final octree shard files (that is stage 01).
* Does not guarantee **global** Morton order across files — only **within** each output partition/file from `sort_shards`.
* Does not replace DuckDB ordering in stage 01; stage 01 still assumes the query contract in `pipeline-stage-01.md`.
