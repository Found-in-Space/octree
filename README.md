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

- per-shard `.index` and `.payload` files
- `manifest.json`

**Stage 01 contract at a glance**

- **Input**: Stage 00 parquet with precomputed `morton_code`, `render`, `level`, `mag_abs`
- **Core operation**: stream rows grouped by target cell and encode one payload blob per `(level, node_id)`
- **Output layout**: append-only shard pairs (`.index` + `.payload`) plus authoritative `manifest.json`
- **Why it exists**: convert row-oriented parquet into bounded-memory, fixed-record intermediates used by Stage 02

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

## Stage 02: combine into `stars.octree`

Stage 02 reads Stage 01’s `manifest.json` and intermediate shards and writes the final octree file.

**Stage 02 contract at a glance**

- **Input**: Stage 01 manifest + all referenced intermediate shard files
- **Core operation**: relocate payload bytes in global DFS order, then build final shard index section
- **Output**: final `stars.octree`
- **Why it exists**: perform final file assembly without global in-memory materialization

```bash
uv run fis-octree stage-02 --project project.toml
```

## Future Stage 02 companion + Stage 03 sidecar families

The recommended future base dataset package is:

- `stars.octree`
- one foundational `identifiers/order` companion artifact

That companion artifact preserves canonical star ordering for the render dataset so future sidecar families can be rebuilt without depending on all older pipeline outputs.

Named sidecar families should then move into an optional Stage 03 so they can be rebuilt independently of the core render octree package. See `docs/roadmap.md` and `docs/identifiers-order.md`.

## Project configuration

Operational octree build commands now use an explicit TOML project file via `--project path/to/project.toml`.

Project-file paths may be absolute or relative to the project file directory. TOML values do not support environment-variable expansion.

## Detailed stage docs

For full specifications, invariants, and binary layouts:

- `docs/stage-00.md`
- `docs/stage-01.md`
- `docs/stage-02.md`
- `docs/sidecars.md` (optional Stage 01 metadata sidecars)
- `docs/identifiers-order.md` (foundational Stage 2 companion artifact)
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
