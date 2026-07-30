# Found in Space — Octree

Part of [Found in Space](https://foundin.space/), a project that turns real astronomical measurements into interactive explorations of the solar neighbourhood. See all repositories at [github.com/Found-in-Space](https://github.com/Found-in-Space).

This repository is the **spatial indexing pipeline**: it takes the merged star catalogue produced by [Found-in-Space/pipeline](https://github.com/Found-in-Space/pipeline) and converts it into streamable binary octree artifacts for use by the [Found-in-Space/skykit](https://github.com/Found-in-Space/skykit) viewer runtime.

## Infrastructure

Project-level infrastructure for serving Found in Space data assets now lives in
[Found-in-Space/infra](https://github.com/Found-in-Space/infra). The S3 and
CloudFront Terraform root that used to live in this repository moved there as
`terraform/data-cdn`.

## How it works

The octree divides 3D space into nested cells across the 21-bit Morton address space. Each star is assigned to a level based on its absolute magnitude, not its position: the brightest stars go into the shallowest levels (largest cells), the faintest into the deepest (smallest cells). The placement threshold at each level is derived from `v_mag` (default 6.5, roughly the naked-eye limit) — a star is placed at the level whose cell half-size matches the distance from which that star would just be visible to the human eye.

At runtime, the viewer computes a visibility radius for each level. Bright-star cells have large visibility radii and are loaded from anywhere in the scene; faint-star cells have small radii and load only when the observer is nearby. This gives progressive, distance-dependent detail that mirrors how real starlight works.

## Build stages

The stage model is evolving toward a reusable staging tree followed by
materialization and packaging:

| Stage | Input | Output | Purpose |
|-------|-------|--------|---------|
| **Stage 00** | Input-sharded merged parquet | `(node, input_shard_id, kind)` staging groups | Partitions input rows into the octree staging tree |
| **Stage 01** | Stage 00 staging folders | Canonical staged parts | Sorts and compacts staged data in place |
| **Stage 02** | Sorted Stage 01 groups | `stars.octree` + `identifiers.order` | Materializes and packs the traditional/classic output |
| **Stage 03** | Stage 02 outputs | Named sidecar files (e.g. `meta`) | Builds optional sidecar families |

Stage 00 keys replaceability by upstream input shard id. That id comes from the
input directory name or root-level parquet filename stem, so HEALPix files,
batch shards, or another stable upstream layout all work; the upstream pipeline
chooses the rebuild granularity by choosing its shard layout.

Stage 00 calculates only the routing fields (`morton_code` and natural
`level`) from raw Cartesian input. Stage 01 preserves the raw position,
magnitude, and temperature fields. The classic Stage 02 output then clamps
rows below `stage02.classic_max_level` (default 14) into their ancestor node
and creates the node-relative 16-byte render record once, for that final node.
A packed final-output variant can choose different nodes from the same raw
Stage 01 rows without changing Stage 00 or Stage 01.

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

The implementation is currently being migrated toward the stage model described
in [`docs/stages.md`](docs/stages.md), so the available commands may temporarily
lag the planned stage numbering.

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
  classic.py          # Stage 02 — classic node materialization and final build
  mag_levels.py       # Magnitude/level threshold calculations
  duckdb_util.py      # Shared DuckDB connection helper with env-variable tuning
  sources/            # Stage 00 — packed octree staging
  assembly/           # Shard assembly, manifests, build plan
  combine/            # Final octree combine (DFS traversal, lookup, records)
  identifiers_order.py # identifiers.order artifact assembly
  stage3.py           # Current named sidecar family builder
  encoding/           # Morton code and Teff encoding utilities
  reader/             # Binary octree reader (header, index, payload, stats)
```

## Documentation

Current stage overview and supporting notes:

- [`docs/staged-pipeline-plan.md`](docs/staged-pipeline-plan.md)
- [`docs/stages.md`](docs/stages.md)
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
