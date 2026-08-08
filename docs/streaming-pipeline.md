# Streaming Octree Pipeline

This document defines the target octree build architecture. Numeric stage names
are compatibility labels only; durable products and commands should be named by
their purpose.

## Non-negotiable properties

- Catalogue data is processed with bounded-memory streams.
- External ordering has a bounded-memory plan, using either explicit immutable
  sorted runs and fan-in merging or an optimized analytical engine when
  measurement shows that engine is faster.
- Databases are implementation choices, not architectural storage. Avoid
  catalogue-scale mutable row stores and random-write amplification. DuckDB is
  suitable for external sort or aggregation when its optimized execution,
  controlled spill, and throughput beat a custom implementation.
- Upstream input shards remain independently replaceable.
- Published intermediates are immutable and identified by semantic content.
- Dirty propagation is dependency-directed and stops at unchanged checksums.
- STAR v1 and v2 share routing, sorting, merging, materialization, and packing
  mechanics. They differ only where their topology or binary format differs.

## Choosing an execution engine

Choose custom streaming code or an analytical database engine from evidence,
not from a blanket rule. Representative benchmarks should record:

- input rows and bytes per second;
- peak resident memory;
- temporary bytes written and read;
- random-write amplification;
- open-file count;
- time to resume after interruption; and
- determinism of the published semantic checksum.

DuckDB is a strong default candidate for external sorting and vectorized
aggregation because those are its intended workloads. An explicit Arrow run
sorter is preferable only when it demonstrates a material advantage, provides
stronger recovery/reuse properties that matter operationally, or avoids an
engine limitation. SQLite remains suitable for bounded control metadata, but a
catalogue-scale design based on millions of indexed UPSERTs needs performance
evidence before adoption.

## Products

```text
upstream shards
    -> routed contributions
    -> sorted contributions + cell summaries
    -> profile topology
    -> materialized buckets
    -> packed base artifacts
    -> optional sidecars
```

### Routed contributions

The replaceable unit is one input shard's contribution to one staging group:

```text
(staging bucket, input shard id, fragment kind)
```

Routing a replacement shard may read that shard in full, because that is the
only reliable way to discover rows that moved between groups. Publication is
nevertheless selective: compare every old and new group contribution by
semantic checksum and retain the old immutable files for equal contributions.

The changed set is the union of:

- newly created group contributions;
- contributions whose semantic checksum changed; and
- old contributions absent from the replacement.

### Sorted contributions

Each changed routed contribution is converted to deterministic sorted runs.
The final merge emits, in one pass:

- canonical rows;
- a semantic content checksum;
- row count;
- sorted per-cell counts; and
- sorted per-cell content checksums.

Unchanged contributions retain their existing files and identities. A changed
star in one group must not rewrite another unchanged group from the same input
shard.

Large groups use a bounded external sort. The implementation may be explicit
spill runs plus bounded fan-in merging or DuckDB when benchmarks show that its
optimized external sorter is superior. Small groups may use an explicitly
bounded in-memory fast path.

### Profile topology

Topology maps natural cells to final profile cells without encoding rows.

- The classic profile caps cells at its configured maximum level.
- The terminal-packed profile aggregates ordered cell-count runs bottom-up and
  selects shallowest eligible terminal roots.

Terminal counts are sequential per-level or spatial-partition files. A changed
partition emits a root summary into a small ancestor spine. Only changed
branches are reconsidered; if a terminal boundary moves, its covered subtree is
the correct invalidation unit.

### Materialized buckets

Materialization is shared machinery parameterized by a topology mapping. It:

1. streams sorted contributions;
2. maps rows to final profile cells;
3. encodes coordinates against those final cells;
4. writes bounded sorted runs where mapping changed order;
5. merges runs with bounded fan-in; and
6. publishes aligned render and identity byte ranges with per-cell checksums.

Materialized products are separate per profile because v1 level capping and v2
terminal packing may place the same row in different cells.

### Packing

Packing consumes only materialized byte ranges and manifests. It must not read
source parquet or repeat routing, topology planning, row sorting, or encoding.

The current monolithic artifact can be rewritten sequentially from reused
materialized partitions after a local change. True in-place or partial
publication requires a separate sharded-container format decision.

## One-star replacement

For a single changed star in a HEALPix shard:

1. Route the replacement shard to temporary contribution files.
2. Compare each old and new contribution checksum.
3. Reuse every unchanged published contribution.
4. Sort only changed/new contributions and remove deleted contributions.
5. Compare old and new per-cell summaries.
6. Recompute only affected topology branches.
7. Rematerialize only dependent profile partitions.
8. Repack final artifacts from changed and reused materialized partitions.

A no-op replacement stops after comparison. A star whose changed fields do not
affect ordering, topology, or encoded payload should stop at the first matching
semantic checksum appropriate to those fields.

## Identity layers

Different changes affect different products. Do not use a single checksum for
all invalidation decisions.

- Routing identity covers fields that determine staging membership.
- Ordering identity covers final cell and canonical-order fields.
- Render identity covers encoded render payload inputs.
- Star identity covers `source` and `source_id` ordering/output.
- Sidecar identity covers only that sidecar's source fields and schema.

These identities allow, for example, a sidecar-only field change to avoid
rerouting or rematerializing the base render artifact.

## Publication and recovery

Every expensive product follows the same protocol:

1. derive its input and policy identity;
2. write immutable temporary runs or partitions;
3. checkpoint completed outputs;
4. verify row counts and semantic checksums;
5. publish files atomically;
6. publish the parent manifest last; and
7. garbage-collect unreachable old files only after durable publication.

Restarts validate and reuse completed immutable products. They never delete the
last published version before its replacement is complete.

## Purpose-based command model

The intended public vocabulary is:

```text
route
prepare
materialize --profile NAME
pack --profile NAME
sidecars
build --profile NAME
```

Compatibility commands using numeric stage labels may remain during migration,
but new manifests, modules, configuration, and documentation should use product
or action names.
