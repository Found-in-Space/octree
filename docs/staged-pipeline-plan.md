# Incremental Streaming Pipeline Plan

This document tracks migration from the current numbered compatibility commands
to the product flow defined in
[`streaming-pipeline.md`](streaming-pipeline.md). The goal is simple: when a
small part of an upstream catalogue changes, reuse every routed, sorted,
topology, materialized, and packed result whose relevant semantic content did
not change.

Numeric stage labels describe current entry points only. New contracts and
implementation work use the product names `routed contributions`, `sorted
contributions`, `profile topology`, `materialized buckets`, `packing`, and
`sidecars`.

## Current status

Implemented or available on the current work branch:

- The compatibility `stage-00` command routes directory-based or root-level
  Parquet shards into an adaptive octree-shaped staging tree.
- A shard may be a HEALPix pixel, a batch shard, or another stable upstream
  replacement unit.
- Routing preserves `input_shard_id` and the
  `(staging bucket, input shard id, kind)` contribution boundary.
- Group semantic checksums stream fixed logical Arrow batches and do not depend
  on Parquet row-group or fragment boundaries.
- Initial routing checkpoints after every shard. A write-ahead fragment journal
  handles rollback or cleanup, and the final checksum pass has independent
  group checkpoints.
- Shard replacement compares old and new contribution checksums and records
  only changed, created, or removed groups for preparation.
- The compatibility `stage-01` command sorts changed contributions into
  replaceable canonical Parquet groups.
- Ordinary groups use the bounded Arrow fast path. Oversized groups use
  DuckDB's disk-backed external sort with a bounded default memory limit.
- Sorted fragment names include their input/policy identity, publication is
  atomic, and superseded files remain live until a durable group checkpoint
  exists.
- Each completed preparation group is checkpointed independently before the
  shared state is consolidated.
- The raw Cartesian input filter computes only `morton_code` and natural
  `level`; raw position, magnitude, temperature, and identity fields remain
  available for profile materialization.
- Shared materialization run/merge helpers support bounded run generation,
  bounded fan-in merging, checkpointed group runs, and checkpointed spatial
  output partitions.
- The compatibility `stage-02` path can materialize and pack the traditional
  level-capped output as `stars.octree` plus `identifiers.order`.
- Materialization selects the final profile cell before encoding the
  node-relative render record. It preserves prepared order where possible and
  locally reorders only affected or overlapping cells.
- Disjoint cells remain Arrow-streamed; oversized local sorting may use DuckDB
  when its native external sorter is the better execution engine.
- Render artifacts, identity order, and sidecars carry UUID-backed parent
  identities.

Compatibility limitations still to remove:

- downstream preparation changes currently fall back to a shared
  `clean`/`all` invalidation marker;
- per-cell count and content summaries are not yet a complete reusable product;
- terminal-packed topology still needs the streamed count/topology design;
- profile topology and materialized dependency manifests are not yet fully
  partitioned;
- the current monolithic packer rewrites complete artifacts; and
- current numeric commands collapse several target products together.

The `clean`/`all` marker is bounded and safe, but it is not the target
architecture. The target is partitioned dependency metadata with checksum
stopping at every product boundary.

## Target products

| Product | Rebuild boundary | Output |
|---|---|---|
| Routed contributions | Input shard and staging group | Immutable raw contribution fragments |
| Sorted contributions | Changed routed contribution | Canonical rows and sorted cell summaries |
| Profile topology | Profile and changed spatial branch | Natural-cell to final-cell mapping |
| Materialized buckets | Profile and affected output partition | Aligned render and identity byte ranges |
| Packed artifacts | Output profile | `stars.octree` and `identifiers.order` |
| Sidecars | Profile, family, and affected identity partition | Schema-bearing sidecar artifacts |

The intended orchestration vocabulary is:

```text
route
prepare
materialize --profile NAME
pack --profile NAME
sidecars
build --profile NAME
```

Renaming the current CLI is deliberately separate from building these product
contracts.

## Core invariants

### Replaceable contribution boundary

For a fixed shard and tree identity, routing produces:

```text
(staging bucket, input shard id, kind) -> semantic rows
```

`kind` currently distinguishes:

- `pack`: rows held in a staging bucket that may contain deeper natural cells;
- `lim`: rows resident in a lower-magnitude-limited staging bucket.

Physical filenames, compression metadata, and row-group boundaries may differ.
Semantic row content determines identity.

A shard replacement uses the union of old and new contribution keys. It must
detect rows that moved to another group, disappeared, or appeared. Equal
contributions retain their existing immutable files; they are not republished
just because the shard was reread.

### Canonical local ordering

Preparation sorts each contribution independently, never the whole catalogue.
The primary key is:

```text
level, final_node_id, mag_abs, source, source_id
```

with:

```text
final_node_id = morton_code >> (3 * (MORTON_BITS - level))
```

Remaining columns provide a deterministic schema-order tie-break. Nulls sort
last. Arrow and the chosen external-sort engine must publish the same semantic
checksum for the same rows.

### Bounded execution

Every catalogue-scale read, transform, checksum, sort, merge, and write has an
explicit memory bound. Small contributions may use an in-memory fast path.
Large ordering uses either:

- immutable sorted runs and bounded fan-in merging; or
- an optimized analytical engine with controlled memory and spill.

DuckDB is allowed—and often preferable—for vectorized external sorting or
aggregation when representative measurements show better throughput and I/O.
It is an execution engine, not the mutable source of truth. SQLite remains
reasonable for genuinely small control metadata, but catalogue-scale indexed
UPSERT designs require evidence that they avoid random-write amplification.

### Shared materialization

STAR profiles share contribution streaming, run generation, merging,
checkpointing, encoding orchestration, and packing mechanics. Profile-specific
code owns only:

- topology policy;
- mapping from natural to final cells; and
- binary-format differences that cannot be shared.

Classic level capping and terminal-subtree packing must not grow separate
catalogue sort/merge implementations.

### Precise invalidation

Use distinct semantic identities:

- routing identity for staging membership;
- ordering identity for natural cell and canonical order;
- topology identity for cell counts and profile policy;
- render identity for encoded render inputs;
- star identity for `source` and `source_id`;
- sidecar identity for a sidecar's fields and schema.

Dirty propagation is the dependency closure of changed identities. It stops at
an equal checksum. A single global checksum or dirty flag cannot express, for
example, a sidecar-only update that leaves the render payload unchanged.

## Product contracts

### Routed contributions

Inputs:

- project and tree identity;
- one or more upstream Parquet shards;
- optional existing routed-contribution manifest.

Outputs:

- immutable `pack` and `lim` fragments containing raw and routing fields;
- per-contribution row count and semantic checksum;
- per-shard manifest of contributed group keys;
- write-ahead publication journal and recovery checkpoints.

Replacement protocol:

1. Read the replacement shard and route rows to temporary fragments.
2. Finalize every new contribution checksum.
3. Compare the union of old and new group keys.
4. Retain equal published contributions.
5. Atomically publish changed and new contributions.
6. Record removed contributions in downstream invalidation.
7. Publish the shard manifest last.
8. Garbage-collect unreachable old files after durable publication.

### Sorted contributions and cell summaries

Inputs:

- changed routed contributions;
- canonical ordering policy;
- bounded sort configuration.

Outputs:

- immutable, content/policy-addressed sorted fragments;
- sorted contribution checksum and row count;
- sorted per-natural-cell count records;
- sorted per-natural-cell content checksums; and
- independent completion checkpoint.

Preparation inspects Parquet metadata to choose a bounded Arrow path or external
sort. DuckDB currently provides the optimized large-group path. An explicit run
sorter should replace it only with benchmark evidence or a required recovery
property that DuckDB cannot provide.

Cell summaries are catalogue-scale data and therefore partitioned binary or
Parquet products, not arrays embedded in a shared JSON manifest.

### Profile topology

Inputs:

- merged cell-count summaries;
- profile policy and identity;
- previous topology partitions where available.

Outputs:

- natural-cell to final-cell mappings;
- partition root summaries and topology checksums;
- changed mapping ranges for materialization invalidation.

Classic topology applies a configured level cap.

Terminal-packed topology:

1. merges sorted natural-cell count records;
2. emits deepest-level counts ordered by node id;
3. sequentially aggregates children into each parent level;
4. selects shallowest eligible terminal roots; and
5. publishes partitioned terminal mappings and ancestor summaries.

One changed cell recomputes its containing topology partition and ancestor
spine. If a terminal boundary moves, its covered subtree is invalidated. Other
branches remain reusable.

### Materialized buckets

Inputs:

- sorted contributions;
- profile topology mapping;
- affected dependency partitions;
- render, identity, and ordering policies.

Outputs:

- render payload byte ranges;
- identity-order byte ranges from the same row stream;
- per-cell offsets, row counts, and content checksums;
- immutable group runs and spatial partition checkpoints.

Shared behavior:

1. stream contribution rows;
2. map each natural cell to its final profile cell;
3. encode coordinates relative to that final cell;
4. preserve prepared order where mapping does not disturb it;
5. write bounded local runs where cells overlap or ordering changes;
6. merge runs with bounded fan-in; and
7. publish render and identity ranges together.

The classic and terminal-packed profiles own separate materialized products
because they may assign the same star to different cells.

### Packing

Inputs:

- materialized render and identity ranges;
- topology/index information;
- output-format metadata.

Outputs:

- `stars.octree`;
- `identifiers.order`;
- profile manifest and UUID identities.

Packing performs no source routing, Parquet sorting, topology aggregation, or
row encoding. The current monolithic format may be rewritten sequentially from
changed and reused materialized partitions. Partial publication is deferred to
a separate sharded-container format decision.

### Sidecars

Each sidecar belongs to one packed profile and carries its parent dataset UUID.
Its records follow the profile's exact materialized identity order. Sidecar
schema and field identity are independent from the base render identity, so a
sidecar-only change need not invalidate topology or render materialization.

## Manifests and state

Use three layers rather than one growing mutable document.

### Tree identity

The immutable tree identity contains any setting that can change routing or
canonical natural-cell semantics:

- source dataset and schema identity;
- coordinate frame, origin, bounds, and Morton depth;
- magnitude-to-level policy;
- staging split policy; and
- canonical contribution ordering version.

Routing refuses to append or replace data when this identity differs.

### Small control manifests

Small JSON manifests contain:

- product format and policy identities;
- input and output checksum references;
- paths to partitioned dependency products;
- checkpoint status; and
- publication generation.

They are written atomically and published after referenced files.

### Partitioned dependency products

Sorted binary or Parquet records contain catalogue-scale state such as:

```text
cell key
input shard id
contribution identity
row count
ordering checksum
render checksum
identity checksum
```

These records can be compared and merged sequentially. They replace global
dirty-node arrays and avoid a catalogue-scale mutable row database.

## Change propagation examples

### One changed star in one HEALPix shard

1. Reread and route the HEALPix shard.
2. Compare every old/new contribution pair.
3. Reuse all equal contributions; include removed old groups in the changed set.
4. Prepare only changed contributions.
5. Compare old/new per-cell identities.
6. Recompute affected topology branches per profile.
7. Rematerialize dependent output partitions only.
8. Repack from changed and reused materialized ranges.
9. Rebuild sidecars only where their own identity changed.

If the star remains in one group, other groups from the same HEALPix shard stop
at contribution comparison. If the replacement is semantically identical, no
preparation runs.

### Render-encoding change without placement change

Routing and ordering identities remain unchanged. Topology remains reusable.
Affected profile buckets are re-encoded and packed. Sidecars rebuild only if
they consume changed fields or identity order.

### Sidecar-only field change

Routing, ordering, topology, and base render products remain reusable. Only the
affected sidecar partitions and final sidecar artifact change.

### Topology-policy change

Routed and sorted contributions remain reusable. The affected profile topology
and dependent materialized buckets rebuild. Other profiles remain untouched.

## Work streams

### A. Contribution immutability and replacement

Status: substantially implemented.

Remaining work:

- finish partitioning routed manifests before shared JSON state becomes large;
- expose reuse/change counts by semantic identity;
- verify replacement deletion and crash recovery on real HEALPix shards.

### B. Preparation and cell summaries

Status: deterministic local sorting, bounded external sorting, immutable
publication, and checkpoints are implemented.

Remaining work:

- emit reusable per-cell count and field-specific checksum runs;
- consume those runs through sequential partition merges;
- remove the conservative `all` propagation once dependency products exist.

### C. Profile topology

Status: classic level-cap policy exists in compatibility materialization.

Remaining work:

- make classic mapping an explicit topology product;
- replace catalogue-scale terminal count storage with sequential level files;
- partition terminal topology and ancestor summaries;
- validate subtree invalidation when terminal boundaries move.

### D. Shared materialization

Status: bounded run-generation and fan-in merge helpers exist and classic
materialization is moving onto them.

Remaining work:

- complete the output-neutral materialization API;
- drive both profiles from explicit topology mappings;
- publish aligned render/identity partition manifests;
- remove duplicated format-specific sorting and merging.

### E. Packing and sidecars

Status: complete sequential packing, UUID identities, `identifiers.order`, and
schema-bearing sidecars exist.

Remaining work:

- constrain packing inputs to materialized products only;
- reuse unchanged materialized partitions during complete repacks;
- add sidecar partition reuse by sidecar identity;
- evaluate a sharded final container separately from this migration.

### F. Naming and compatibility cleanup

Status: documentation uses purpose-based products; numeric CLI commands remain.

Remaining work:

- add purpose-based commands when product contracts are stable;
- retain numeric aliases for a documented compatibility period;
- rename paths, config sections, and reports without mixing that migration into
  topology or materialization correctness work.

## Suggested implementation order

1. Freeze sorted cell-summary and dependency-record formats.
2. Emit per-cell count and field-specific checksum runs during preparation.
3. Make classic topology an explicit mapping and prove byte-equivalent v1
   output through shared materialization.
4. Implement sequential bottom-up terminal count aggregation.
5. Implement partitioned terminal selection and changed-subtree invalidation.
6. Move terminal-packed output onto shared materialization.
7. Replace `clean`/`all` with partitioned dependency invalidation.
8. Add one-star and one-HEALPix replacement integration tests.
9. Restrict packing to materialized inputs and add reuse reporting.
10. Introduce purpose-based CLI/config names and compatibility aliases.

The v1 byte-equivalence milestone comes before reintroducing v2. It validates
the shared streaming foundation against the trusted output.

## Validation plan

Unit tests:

- equal shard input retains equal routed contribution identities;
- a changed shard dirties only changed/new/removed groups;
- Arrow and external sorting publish identical order and checksum;
- equal sorted identities stop dependency propagation;
- cell-count merges remain bounded and deterministic;
- classic topology maps deep natural cells to the configured cap;
- terminal topology selects the shallowest eligible root;
- render and identity ranges remain exactly aligned.

Integration tests:

- replace a shard with identical content and perform no downstream work;
- change one star without moving groups and rebuild only its contribution path;
- move one star between groups and invalidate the union of old/new paths;
- delete one star and invalidate its old contribution and cell;
- compare full and incremental classic artifacts byte-for-byte;
- compare full and incremental terminal-packed artifacts semantically;
- resume after interruption at each publication boundary.

Operational measurements:

- input and output rows/bytes per second;
- peak resident memory;
- temporary bytes read and written;
- open-file high-water mark;
- random-write amplification;
- reuse versus rebuild counts per product; and
- restart time from the last durable checkpoint.

## Open decisions

- Cell-summary partition depth and binary schema.
- Field-specific identity granularity versus manifest complexity.
- Topology partition depth and terminal ancestor-summary layout.
- Materialized partition size and file layout.
- Whether a sharded final container is worth the runtime and publication
  complexity.
- Compatibility lifetime for numeric commands after purpose-based commands
  exist.

## Glossary

Input shard
: One upstream Parquet unit replaceable independently, often a HEALPix pixel.

Routed contribution
: One shard's immutable rows for one staging bucket and fragment kind.

Sorted contribution
: A routed contribution in deterministic canonical order, with semantic
  checksum and cell summaries.

Natural cell
: The cell selected from a row's natural `level` and `morton_code` before an
  output-profile topology policy is applied.

Final profile cell
: The renderer-visible cell selected by classic capping or terminal packing.

Materialized bucket
: Canonical encoded render and identity ranges for final profile cells in one
  spatial partition.

Semantic checksum
: A checksum over canonical logical rows or output bytes, independent of
  Parquet metadata and incidental fragment boundaries.
