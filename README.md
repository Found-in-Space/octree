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

- **Input**: HEALPix-sharded merged parquet (configured in the project file)
- **Required columns**: `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `mag_abs`
- **Output**: enriched parquet in the project-configured Stage 00 output directory with `morton_code`, `render`, `level` added
- **Why it exists**: isolate expensive per-row enrichment into a streaming pass before cell/shard assembly

### Project bootstrap

```bash
uv run fis-octree project init project.toml
```

`project init` writes a complete starter TOML using built-in defaults. It does not read environment variables or expand them into the generated file.

### Basic usage

```bash
uv run fis-octree stage-00 --project project.toml
```

```bash
uv run fis-octree stage-00 --project project.toml --force
```

## Stage 01: build intermediates

Stage 01 reads Stage 00 parquet files and produces:

- render shard `.index` / `.payload` files
- identifiers-order shard `.index` / `.payload` files
- `render-manifest.json`
- `identifiers-manifest.json`

**Stage 01 contract at a glance**

- **Input**: Stage 00 parquet with precomputed `morton_code`, `render`, `level`, `mag_abs`
- **Core operation**: stream rows grouped by target cell and encode both render bytes and canonical `(source, source_id)` ordering per `(level, node_id)`
- **Output layout**: append-only shard pairs for render and identifiers/order plus authoritative paired manifests
- **Why it exists**: convert row-oriented parquet into bounded-memory intermediates used by Stage 02 render assembly and Stage 02 `identifiers/order` assembly

### Basic usage

```bash
uv run fis-octree stage-01 --project project.toml
```

Project-file paths may be absolute or relative to the project file location. Build commands do not expand environment variables from TOML values.

### Shard tuning

```bash
# edit project.toml:
# [stage01]
# deep_shard_from_level = 8
# deep_prefix_bits = 3
```

## Stage 02: package the base dataset

Stage 02 reads Stage 01’s paired manifests and writes the base dataset package:

- `stars.octree`
- `identifiers.order`

**Stage 02 contract at a glance**

- **Input**: `render-manifest.json`, `identifiers-manifest.json`, and all referenced shard files
- **Core operation**: assemble the render octree, emit a matching `dataset_uuid`, and combine the canonical identifiers/order companion artifact for the same dataset
- **Output**: final `stars.octree` plus `identifiers.order`
- **Why it exists**: publish one UUID-bound base dataset package without global in-memory materialization

```bash
uv run fis-octree stage-02 --project project.toml
```

## Stage 03: named sidecars

Stage 03 reads the Stage 02 base dataset package and builds one or more named sidecar families.

The first implemented family is `meta`.

```bash
uv run fis-octree stage-03 --project project.toml
uv run fis-octree stage-03 --project project.toml --family meta
```

Each final octree now carries a mandatory descriptor block immediately after the STAR header:

- render octrees carry `dataset_uuid`
- sidecars carry `parent_dataset_uuid`, `sidecar_uuid`, and `sidecar_kind`

This lets readers validate sidecars by dataset identity before falling back to geometry checks.

## Project configuration

Operational octree build commands now use an explicit TOML project file via `--project path/to/project.toml`.

Project-file paths may be absolute or relative to the project file directory. TOML values do not support environment-variable expansion.

## Detailed stage docs

For full specifications, invariants, and binary layouts:

- `docs/stage-00.md`
- `docs/stage-01.md`
- `docs/stage-02.md`
- `docs/stage-03.md`
- `docs/sidecars.md`
- `docs/identifiers-order.md`
- `docs/reader.md` (query/read behavior for `stars.octree`)
- `docs/roadmap.md`

## Help

Show command help:

```bash
uv run fis-octree --help
uv run fis-octree stage-00 --help
uv run fis-octree stage-01 --help
uv run fis-octree stage-02 --help
uv run fis-octree stage-03 --help
```
