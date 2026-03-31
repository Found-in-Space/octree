# Found in Space — Octree

Part of [Found in Space](https://foundin.space/), a project that turns real astronomical measurements into interactive explorations of the solar neighbourhood. See all repositories at [github.com/Found-in-Space](https://github.com/Found-in-Space).

This repository is the **spatial indexing pipeline**: it takes the merged star catalogue produced by [Found-in-Space/pipeline](https://github.com/Found-in-Space/pipeline) and converts it into streamable binary octree artifacts for use by the [Found-in-Space/skykit](https://github.com/Found-in-Space/skykit) viewer runtime.

## Overview

The pipeline runs in four stages:

| Stage | Input | Output | Purpose |
|-------|-------|--------|---------|
| **Stage 00** | HEALPix-partitioned merged parquet | Enriched parquet with `morton_code`, `render`, `level` | Per-row enrichment; isolates the expensive computation |
| **Stage 01** | Stage 00 parquet | Render + identifiers-order shard pairs and manifests | Converts row-oriented data into bounded-memory intermediates |
| **Stage 02** | Stage 01 manifests | `stars.octree` + `identifiers.order` | Assembles the final base dataset package |
| **Stage 03** | Stage 02 outputs | Named sidecar files (e.g. `meta`) | Enriches the base dataset with optional sidecar families |

Each render octree carries a `dataset_uuid`. Sidecars carry a `parent_dataset_uuid` so readers can validate the pairing before opening them.

## Installation

Requires Python ≥ 3.13. From the project root:

```bash
uv sync
```

## CLI

Entry point: **`fis-octree`** (or `python -m foundinspace.octree`).

```bash
uv run fis-octree --help
```

### Project configuration

All build stages require an explicit TOML project file:

```bash
uv run fis-octree project init project.toml
uv run fis-octree stage-00 --project project.toml
uv run fis-octree stage-01 --project project.toml
uv run fis-octree stage-02 --project project.toml
uv run fis-octree stage-03 --project project.toml
```

Generate a starter config:

```bash
uv run fis-octree project init project.toml
```

Paths in the project file may be absolute or relative to the project file's directory. Environment variable expansion is not supported in TOML values.

### Querying an octree

The `stats` command reads a finished `stars.octree` — either from a local path or a URL — and reports level-by-level statistics and the nearest stars to any query point:

```bash
uv run fis-octree stats path/to/stars.octree
uv run fis-octree stats https://example.com/stars.octree --nearest 20 --radius 25
uv run fis-octree stats stars.octree --meta-octree meta.octree --point "8.6,0,0"
```

## Runtime configuration

DuckDB memory and threading behaviour can be tuned at runtime via environment variables (or a `.env` file in the project root). All are optional:

| Variable | DuckDB setting | Example |
|---|---|---|
| `DUCKDB_TEMP_DIR` | `temp_directory` | `/mnt/scratch/duckdb` |
| `DUCKDB_MAX_TEMP_DIRECTORY_SIZE` | `max_temp_directory_size` | `50GB` |
| `DUCKDB_MEMORY_LIMIT` | `memory_limit` | `16GB` |
| `DUCKDB_THREADS` | `threads` | `4` |
| `DUCKDB_PRESERVE_INSERTION_ORDER` | `preserve_insertion_order` | `false` |

## Code layout

```
src/foundinspace/octree/
  _cli.py             # Click root; stage-00, stage-01, stage-02, stage-03, stats, project subcommands
  project.py          # TOML project file loading and validation
  config.py           # Build defaults (world size, Morton bits, max level)
  mag_levels.py       # Magnitude/level threshold calculations
  duckdb_util.py      # Shared DuckDB connection helper with env-variable tuning
  sources/            # Stage 00 — per-pixel enrichment (Morton code, render level)
  assembly/           # Stage 01 — shard assembly, manifests, build plan
  combine/            # Stage 02 — octree combine (DFS traversal, lookup, records)
  identifiers_order.py # Stage 02 — identifiers.order artifact assembly
  stage3.py           # Stage 03 — named sidecar families
  encoding/           # Morton code and Teff encoding utilities
  reader/             # Binary octree reader (header, index, payload, stats)
```

## Detailed stage documentation

Full specifications, invariants, and binary layouts:

- [`docs/stage-00.md`](docs/stage-00.md)
- [`docs/stage-01.md`](docs/stage-01.md)
- [`docs/stage-02.md`](docs/stage-02.md)
- [`docs/stage-03.md`](docs/stage-03.md)
- [`docs/sidecars.md`](docs/sidecars.md)
- [`docs/identifiers-order.md`](docs/identifiers-order.md)
- [`docs/reader.md`](docs/reader.md)
- [`docs/glow.md`](docs/glow.md)
- [`docs/roadmap.md`](docs/roadmap.md)

## Tests

```bash
uv run pytest
```

Tests live under `tests/` and cover stage CLIs, assembly, combine phases, reader stats, and binary format encoding.

## Development

Install the development dependencies and git hooks:

```bash
uv sync
uv run pre-commit install
```

Run the linter and formatter manually:

```bash
uv run pre-commit run --all-files
```

## License

MIT — see [LICENSE](LICENSE).
