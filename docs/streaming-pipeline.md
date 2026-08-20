# Streaming Octree Pipeline

This document defines the octree build architecture. Durable products, commands,
paths, manifests, and state are named by their purpose.

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
group reuses every equal immutable count run. The current terminal planner
merges those runs into checkpointed per-level products and derives subtree
counts sequentially, without a catalogue-scale mutable database. Its published
terminal-map identity is still global when a terminal decision genuinely
changes; spatially partitioned terminal mappings and ancestor spines are the
remaining refinement needed to constrain that invalidation to changed branches.

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

The render index is compiled from intermediate node-ID streams rather than
discovered through random catalogue lookups. A bottom-up bounded fan-in merge
produces five-level logical skeletons in final write order. Skeletons contain
node presence, payload presence, child masks, terminal policy, and frontier
shape, but not payload bytes, star counts, lengths, or absolute offsets. They
are content-addressed and stored in a configured bounded number of spatial pack
files, allowing payload-only rebuilds to reuse the topology plan and packs.

The default emitter writes complete shards with zeroed frontier tables to a
dedicated scratch index. A bounded DFS frontier records each patch destination
when its parent is written, fills direct child offsets as children begin, and
patches each complete parent table with one positional write. It never searches
an input or index to discover that destination. The finished scratch index is
copied sequentially into the final artifact, which is never sought backwards.
The scratch index is removed after success or failure under the cache lock.

The `forward` emitter is a lower-scratch alternative. It performs a prefix-sum
prepass, writes compact child-offset streams, and emits directly to the final
artifact. Both production emitters consume the same immutable topology and
relocation streams and are byte-identical. The per-child positional-write
variant exists only as a benchmark control because its write amplification is
unbounded with the child count.

The current monolithic artifact can be rewritten sequentially from reused
materialized partitions after a local change. True in-place or partial
publication requires a separate sharded-container format decision.

The final render and `identifiers.order` pair has its own semantic checkpoint.
An exact no-op build reuses both files and their UUIDs; a crash before both
atomic publications complete leaves the checkpoint invalid and causes a safe
rebuild.

## One-star replacement

For a single changed star in a HEALPix shard:

1. Route the replacement shard to temporary contribution files.
2. Compare each old and new contribution checksum.
3. Reuse every unchanged published contribution.
4. Sort only changed/new contributions and remove deleted contributions.
5. Compare old and new per-cell summaries.
6. Recompute only affected topology branches.
7. Rematerialize only dependent profile partitions.
8. Repack final artifacts from changed and reused materialized partitions,
   reusing the index topology cache whenever node presence is unchanged.

A no-op replacement stops after comparison. A star whose changed fields do not
affect ordering, topology, or encoded payload should stop at the first matching
semantic checksum appropriate to those fields.

The current state still propagates some downstream changes with a conservative
whole-product marker, and a changed terminal-map identity can
invalidate v2 materialization globally. Those are dependency-indexing gaps, not
reasons to abandon immutable contribution or topology reuse.

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

The public vocabulary is:

```text
route
prepare
build
sidecars build
```

Topology planning, materialization, and packing remain reusable internal
products beneath `build`. Build policy, including profile selection, belongs in
the project file rather than command-line overrides.
