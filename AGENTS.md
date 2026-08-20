# Agent Instructions

## Python Tooling: Use `uv`

- Use `uv` for all Python dependency and environment operations in this repository.
- Do not use `pip`, `poetry`, or `conda` commands directly.

### Standard commands

- Sync/install dependencies: `uv sync`
- Run Python entrypoints/tools: `uv run <command>`
- Run tests: `uv run pytest`
- CI: on push/PR to `main`, GitHub Actions runs `uv run pytest` with coverage (see `.github/workflows/ci.yml`).
- Add a dependency: `uv add <package>`
- Add a dev dependency: `uv add --dev <package>`

### Pre-commit (Ruff)

- Install git hooks once: `uv run pre-commit install`
- Run on all files: `uv run pre-commit run --all-files`

Hooks: `ruff-check` (lint + fix) and `ruff-format`, scoped to `src/` and `tests/`.

### Examples

- `uv run fis-octree --help`
- `uv run pytest tests/test_build_cli.py`

## Large-Dataset Architecture

- Treat every catalogue-scale data path as a bounded-memory streaming pipeline.
  Read, transform, sort, merge, checksum, and write incrementally; the full
  catalogue, an unbounded bucket, or an unbounded node set must never be loaded
  into RAM or accumulated in a general-purpose database.
- Do not default to a general-purpose mutable row store for catalogue-scale
  intermediates. In particular, avoid SQLite designs with per-cell UPSERTs or
  other random-write amplification unless measurements prove them appropriate.
  Database engines are not categorically banned: DuckDB or another optimized
  analytical engine is appropriate when it gives the best measured external
  sort/aggregation performance with controlled memory and spill behavior.
- External ordering must have a bounded-memory execution plan. It may use
  explicit sorted runs plus a bounded fan-in streaming merge or a proven
  optimized external engine such as DuckDB. Spill products must be sequential
  where practical, restart-safe, checksum-tracked, and disposable after
  publication. Any per-cell fallback must have an explicit memory bound.
- Sorting and merging reusable bucket data is shared preparation, not a STAR v1
  or STAR v2 writer responsibility. Keep common routing/run-generation/merge
  mechanics in an output-format-neutral component; keep only topology policy
  (for example v1 level capping or v2 terminal selection) and binary encoding
  in format-specific code.
- Published intermediates are immutable contributions with semantic identities.
  Reprocessing one input shard must compare its old and new contribution to
  each group and reuse the existing files whenever that contribution is
  unchanged. The fact that a HEALPix shard was rerun is not itself grounds to
  dirty every group previously touched by that shard.
- Propagate invalidation from the union of old and new changed cell
  contributions. Stop at any boundary whose semantic checksum is unchanged.
  A one-star edit should normally rebuild one sorted contribution and only its
  dependent topology/materialization partitions, not every intermediate made
  from the star's input shard.
- Use names that describe the data product or action, such as `routing`,
  `preparation`, `sorted runs`, `topology planning`, `materialization`, and
  `packing`.
