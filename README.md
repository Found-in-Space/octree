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

## Build products

The architecture is a sequence of purpose-named, reusable products:

| Product/action | Input | Output |
|---|---|---|
| **Route** | Input-sharded merged parquet | Immutable `(staging bucket, input shard, kind)` contributions |
| **Prepare** | Changed routed contributions | Canonical sorted contributions and per-cell summaries |
| **Plan topology** | Cell summaries plus a profile policy | Natural-cell to profile-cell mapping |
| **Materialize** | Sorted contributions plus topology | Canonical render and identity ranges per profile bucket |
| **Pack** | Materialized ranges and manifests | `stars.octree` and `identifiers.order` |
| **Build identity locator** | Published render plus exact identity order | Optional `identity-locator.idx` alternative index |
| **Build sidecars** | Profile identity order plus enrichment | Named sidecar artifacts such as `meta.octree` |

The numeric `stage-00` through `stage-03` commands are current compatibility
entry points, not the durable architectural vocabulary. New product contracts
and manifests should use purpose-based names. See
[`docs/streaming-pipeline.md`](docs/streaming-pipeline.md) for the normative
target and [`docs/stages.md`](docs/stages.md) for the compatibility mapping.

Routing preserves replaceability by upstream input shard id. That id comes from the
input directory name or root-level parquet filename stem, so a HEALPix file,
batch shard, or another stable upstream unit can be replaced independently.
Each shard contributes separately to each staging group. Replacing a shard
publishes a new contribution only where its semantic checksum changed; equal
old contributions remain immutable and reusable. The changed set includes old
groups from which the replacement removed every row.

Contribution checksums are computed from fixed logical Arrow batches streamed
across parquet fragment boundaries. Initial routing commits after every input
shard through a write-ahead fragment journal, and independent checksum
checkpoints make the final validation pass resumable.

Preparation keeps an explicitly bounded in-memory Arrow fast path for ordinary
groups. Oversized groups use DuckDB's disk-backed external sort, with a bounded
512 MB default when `DUCKDB_MEMORY_LIMIT` is unset. DuckDB is an execution
engine here, not a mutable catalogue store: an explicit run sorter remains an
option if representative measurements show better throughput or recovery.
Published sorted fragments are content/policy-addressed, written atomically,
and checkpointed independently.

Invalidation is checksum- and dependency-directed. A one-star change in one
HEALPix shard may require rereading that shard to discover moved rows, but an
unchanged contribution must not be sorted or materialized again. Separate
routing, ordering, render, identity, and sidecar identities stop propagation as
soon as the relevant semantic content is unchanged. The current shared state
still has a conservative downstream `clean`/`all` fallback; that is a migration
constraint, not the target model.

Routing calculates only placement fields such as `morton_code` and natural
`level`, while retaining raw position, magnitude, temperature, and identity
fields. Topology planning then selects the actual profile cell. Both classic
level capping and terminal-subtree packing feed the same materialization
machinery, which encodes node-relative coordinates once, writes aligned render
and identity streams, and uses bounded sorting and merging. DuckDB is suitable
for oversized local sorts when it is the measured best engine.

Packing consumes only materialized byte ranges and manifests. The current
monolithic artifact may still need a complete sequential rewrite after a local
change, but that rewrite reuses unchanged materialized partitions and does not
repeat routing, sorting, topology planning, or encoding.

Index packing is itself streaming. It compiles sorted node-ID streams into
content-addressed five-level topology skeletons stored in a bounded number of
spatial pack files. Payload offsets and lengths are deliberately excluded from
that cache identity, so a payload-only change reuses the topology exactly. The
default emitter writes one complete scratch index, patches each parent frontier
table once at its recorded position, and copies the completed index sequentially
into the final artifact. It performs no catalogue lookup to discover patch
destinations and never seeks backwards in the final file. A prefix-sum emitter
is available when scratch space is tighter; both emit byte-identical artifacts.

An unchanged final render/identity pair is protected by a semantic checkpoint.
When inputs and policy are unchanged, the build reuses the published files and
their UUIDs without reopening catalogue-scale intermediates.

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

Current compatibility build commands require an explicit TOML project file:

```bash
uv run fis-octree project init project.toml
uv run fis-octree stage-00 --project project.toml
uv run fis-octree stage-01 --project project.toml
uv run fis-octree stage-02 --project project.toml
uv run fis-octree stage-03 --project project.toml
```

Optional sidecars are not part of the ordinary base build. The visual-duplicate
review artifact is built only when explicitly requested from a published render
octree, its matching identity order, and the collected one-to-one evidence:

```bash
uv run fis-octree sidecars visual-duplicates \
  --project project.toml \
  --evidence ../catalogs/publications/20260515.1/catalog/fis_gaia_hip_supplemental_display_map.parquet
```

This purpose-named command defaults to
`<render-name>.visual-duplicates.octree` beside the render artifact. It is not
registered in `stage-03`, added to the starter project, or run by default.

Exact Gaia/HIP lookup is a separate optional alternative index, likewise not a
sidecar or numbered stage:

```bash
uv run fis-octree identity-locator benchmark --project project.toml
uv run fis-octree identity-locator build --project project.toml
uv run fis-octree identity-locator lookup \
  stars.identity-locator.idx identifiers.order gaia 5853498713190525696 --json
uv run fis-octree identity-locator validate \
  stars.identity-locator.idx identifiers.order \
  --report stars.identity-locator.report.json
```

The build defaults to `<render-stem>.identity-locator.idx`. It uses restartable
bounded Parquet runs and DuckDB external merge sorts, while the local/HTTP
reader performs exact finite range reads. No locator fields are added to the
project TOML, and `stage-02` never runs it automatically. See
[`docs/identity-lookup-index.md`](docs/identity-lookup-index.md). The measured
production default is 32 KiB raw pages; page size and codec remain declared in
every artifact.

`stage-02` defaults to the measured batched temporary-index emitter. The
alternative below uses less scratch space while producing identical bytes:

```bash
uv run fis-octree stage-02 --project project.toml \
  --index-emission-strategy forward
```

The default temporary-index strategy needs scratch capacity approximately equal
to the final index section in addition to the atomic final-output temporary
file. Its scratch files and topology cache live below the configured Stage 02
work directory.

These numeric names will remain usable during migration; they should not be
copied into new product or manifest names.

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

The terminal-memory testbed forms virtual packed STAR v2 subtrees over an
existing artifact and replays headset-oriented memory policies without
rewriting the octree:

```bash
uv run fis-octree terminal-memory-benchmark stars.octree \
  --sample sun:1,1,1@11 \
  --waterline 1000 \
  --chunk-stars 256
```

See [`docs/terminal-memory-testbed.md`](docs/terminal-memory-testbed.md) for
published-octree sampling, observer traces, caching, and model assumptions.

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
  _cli.py             # Click root and current compatibility commands
  project.py          # TOML project file loading and validation
  config.py           # Build defaults (world size, Morton bits, max level)
  classic.py          # Compatibility orchestration for classic materialization and packing
  materialization/    # Shared bounded run generation and merge machinery
  mag_levels.py       # Magnitude/level threshold calculations
  duckdb_util.py      # Shared DuckDB connection helper with env-variable tuning
  sources/            # Routed and sorted contribution preparation
  assembly/           # Shard assembly, manifests, build plan
  combine/            # Payload relocation and streaming topology/index packing
  identifiers_order.py # identifiers.order artifact assembly
  identity_locator/    # Exact Gaia/HIP locator format, builder, reader, benchmark
  sidecars/            # Optional purpose-named sidecar builders
  stage3.py           # Current named sidecar family builder
  encoding/           # Morton code and Teff encoding utilities
  reader/             # Binary octree reader (header, index, payload, stats)
```

## Documentation

Pipeline architecture and supporting notes:

- [`docs/streaming-pipeline.md`](docs/streaming-pipeline.md) — target
  bounded-memory, immutable-contribution architecture
- [`docs/staged-pipeline-plan.md`](docs/staged-pipeline-plan.md)
- [`docs/stages.md`](docs/stages.md)
- [`docs/sidecars.md`](docs/sidecars.md)
- [`docs/identifiers-order.md`](docs/identifiers-order.md)
- [`docs/identity-lookup-index.md`](docs/identity-lookup-index.md)
- [`docs/reader.md`](docs/reader.md)
- [`docs/star-v2.md`](docs/star-v2.md)
- [`docs/terminal-memory-testbed.md`](docs/terminal-memory-testbed.md)
- [`docs/glow.md`](docs/glow.md)
- [`docs/roadmap.md`](docs/roadmap.md)

## Tests

```bash
uv run pytest
```

Tests live under `tests/` and cover stage CLIs, assembly, combine phases, reader stats, and binary format encoding.

The isolated index-emitter benchmark covers dense and sparse 2k/8k/16k
fixtures, with an optional production-shaped 64k sparse case:

```bash
uv run python benchmarks/benchmark_combine_index.py --include-64k
```

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
