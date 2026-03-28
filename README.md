# found-in-space-octree

CLI tools for building octree pipeline artifacts from parquet star catalogs.

## CLI entrypoint

Use the installed CLI command:

```bash
uv run fis-octree --help
```

## Stage 00: per-pixel streaming enrichment

Stage 00 reads HEALPix-sharded merge output and writes enriched parquet for Stage 01.

For each HEALPix pixel directory, Stage 00:

1. Streams input rows in batches (default `1_000_000`)
2. Computes `morton_code`, `render`, and `level`
3. Writes locally sorted (by `morton_code, mag_abs`) parquet shards (~1 GB each)

Input files are never modified in-place.

**Stage 00 contract at a glance**

- **Input**: HEALPix-sharded merged parquet (`{FIS_PROCESSED_DIR}/merged/healpix/...`)
- **Required columns**: `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `mag_abs`
- **Output**: enriched parquet in `{FIS_OCTREE_DIR}/stage00` with `morton_code`, `render`, `level` added
- **Why it exists**: isolate expensive per-row enrichment into a streaming pass before cell/shard assembly

### Basic usage

```bash
uv run fis-octree stage-00 data/processed/merged/healpix data/octree/stage00
```

### Common options

```bash
uv run fis-octree stage-00 data/processed/merged/healpix data/octree/stage00 \
  --v-mag 6.5 \
  --max-level 13
```

```bash
uv run fis-octree stage-00 data/processed/merged/healpix data/octree/stage00 \
  --force \
  --batch-size 1000000
```

## Stage 01: build intermediates

Stage 01 reads Stage 00 parquet files and produces:

- per-shard `.index` and `.payload` files
- `manifest.json`

**Stage 01 contract at a glance**

- **Input**: Stage 00 parquet with precomputed `morton_code`, `render`, `level`, `mag_abs`
- **Core operation**: stream rows grouped by target cell and encode one payload blob per `(level, node_id)`
- **Output layout**: append-only shard pairs (`.index` + `.payload`) plus authoritative `manifest.json`
- **Why it exists**: convert row-oriented parquet into bounded-memory, fixed-record intermediates used by Stage 02

### Basic usage

Defaults match `foundinspace.octree.paths` and `FIS_OCTREE_DIR` / `FIS_PROCESSED_DIR` from `.env`:

- Input glob: `{FIS_OCTREE_DIR}/stage00/**/*.parquet` (default `data/octree/stage00/**/*.parquet`)
- Output dir: `{FIS_OCTREE_DIR}/stage01` (default `data/octree/stage01`)
- `--deep-shard-from-level` defaults to **99** (above typical `--max-level`, so **no** prefix sharding unless you lower it)

```bash
uv run fis-octree stage-01
```

Explicit paths and deep sharding (e.g. from level 8):

```bash
uv run fis-octree stage-01 "data/octree/stage00/**/*.parquet" data/octree/stage01 \
  --deep-shard-from-level 8
```

### With explicit shard tuning

```bash
uv run fis-octree stage-01 "data/octree/stage00/**/*.parquet" data/octree/stage01 \
  --max-level 13 \
  --deep-shard-from-level 8 \
  --deep-prefix-bits 3 \
  --batch-size 100000
```

## Stage 02: combine into `stars.octree`

Stage 02 reads Stage 01’s `manifest.json` and intermediate shards and writes the final octree file.

**Stage 02 contract at a glance**

- **Input**: Stage 01 manifest + all referenced intermediate shard files
- **Core operation**: relocate payload bytes in global DFS order, then build final shard index section
- **Output**: final `stars.octree`
- **Why it exists**: perform final file assembly without global in-memory materialization

Defaults (same `FIS_OCTREE_DIR` as Stage 01):

- Manifest: `{FIS_OCTREE_DIR}/stage01/manifest.json` (default `data/octree/stage01/manifest.json`)
- Output: `{FIS_OCTREE_DIR}/stars.octree` (default `data/octree/stars.octree`)

```bash
uv run fis-octree stage-02
```

## Detailed stage docs

For full specifications, invariants, and binary layouts:

- `docs/stage-00.md`
- `docs/stage-01.md`
- `docs/stage-02.md`
- `docs/sidecars.md` (optional Stage 01 metadata sidecars)
- `docs/reader.md` (query/read behavior for `stars.octree`)
- `docs/roadmap.md` (future format and artifact requirements)

## Help

Show command help:

```bash
uv run fis-octree --help
uv run fis-octree stage-00 --help
uv run fis-octree stage-01 --help
uv run fis-octree stage-02 --help
```
