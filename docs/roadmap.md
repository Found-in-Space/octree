# Octree Roadmap

## Direction

The octree pipeline is moving from numbered compatibility stages to the
bounded, incremental product flow in
[`streaming-pipeline.md`](streaming-pipeline.md):

```text
routed contributions
    -> sorted contributions + cell summaries
    -> profile topology
    -> materialized buckets
    -> packed artifacts
    -> sidecars
```

The current `stage-00` through `stage-03` commands remain compatibility entry
points. Numeric labels are not the naming scheme for new products, manifests,
modules, or future commands.

## Implemented foundations

- Input shards remain independently replaceable at
  `(staging bucket, input shard id, kind)` granularity.
- Routing uses semantic contribution checksums, per-shard publication journals,
  and resumable group checkpoints.
- Preparation performs deterministic local sorting with a bounded Arrow fast
  path and DuckDB external sorting for oversized groups.
- Sorted fragments are immutable, input/policy-addressed, atomically published,
  and checkpointed before old versions are removed.
- Shared bounded run generation and fan-in merging are available for
  materialization.
- Classic materialization encodes rows relative to their selected final cell
  and checkpoints group runs and spatial partitions.
- Terminal topology uses immutable per-group and per-level count runs, bounded
  vectorized fan-in merging, sequential bottom-up subtree aggregation, and
  atomic restart checkpoints. SQLite is no longer part of this catalogue-scale
  path.
- Classic and terminal-packed outputs share materialization and packing
  machinery. A count-equivalent source change can reuse terminal topology.
- The final index compiler uses sorted topology runs and content-addressed,
  spatially packed five-level skeletons. Payload-only changes reuse the topology
  plan without source/index searches.
- The v1 writer is byte-equivalent to the legacy implementation. Its measured
  default builds a dedicated scratch index and patches one recorded frontier
  table per parent; a lower-scratch prefix-sum emitter remains available.
- Complete render/identity pairs have a durable no-op checkpoint with UUID
  reuse, atomic publication, and concurrency-safe cleanup.
- `stars.octree`, `identifiers.order`, and sidecars carry UUID-backed parent
  identities.
- Sidecars are independent, schema-bearing artifacts; `meta` is the first
  implemented family.

DuckDB is retained where its vectorized external sort or aggregation is the
measured best execution engine. The architectural constraint is bounded,
deterministic, sequential processing—not a blanket database ban. Mutable
catalogue-scale row stores and random indexed update designs still require
strong performance evidence.

## Next milestones

### 1. Complete dependency-directed summaries

Preparation still needs durable per-natural-cell counts and field-specific
semantic checksums that the shared state can merge sequentially. These products
will replace the conservative downstream `clean`/`all` marker and allow a
one-star edit to stop at an unchanged cell identity rather than merely at an
unchanged routed or sorted group.

### 2. Partition terminal topology invalidation

The streamed terminal-count and selection implementation is complete, but a
genuine terminal-map change currently has a global identity. Publish spatial
terminal-map partitions plus bounded ancestor summaries so only changed
branches invalidate dependent v2 materialization.

### 3. Refine materialized dependency manifests

Classic and terminal-packed builds already share bounded run generation,
fan-in merging, encoding, and spatial partition caches. The next step is to
connect those partitions to the field-specific cell and topology identities so
the dependency closure is exact for payload-only, identity-only, and topology
changes.

### 4. Production-accept index packing

The new v1 index writer is complete and fixture-tested across dense and sparse
topologies. Record its wall time, CPU time, temporary I/O, write counts, file
handles, and RSS on the next controlled large build. The selected batched
emitter trades one index-sized scratch file for fewer writes; the `forward`
emitter is the operational fallback when scratch capacity is constrained.

The monolithic artifact still requires a complete sequential final rewrite. A
sharded final container and true partial publication remain a separate format
decision.

### 5. Purpose-based command migration

After the remaining dependency contracts stabilize, introduce purpose-based
commands such as `route`, `prepare`, `materialize`, `pack`, `sidecars`, and
`build`. Retain the numeric commands as aliases for a documented compatibility
period. CLI and configuration renaming must not be mixed into topology or
materialization correctness work.

## Existing artifact requirements

These remain part of every profile:

- render octrees expose `dataset_uuid`;
- `identifiers.order` carries the matching parent dataset UUID and its own
  artifact UUID;
- sidecars expose `parent_dataset_uuid`, `sidecar_uuid`, and embedded schema
  metadata; and
- readers reject mismatched render, identity, and sidecar artifacts.

## Related documentation

- [`streaming-pipeline.md`](streaming-pipeline.md)
- [`staged-pipeline-plan.md`](staged-pipeline-plan.md)
- [`stages.md`](stages.md)
- [`identifiers-order.md`](identifiers-order.md)
- [`sidecars.md`](sidecars.md)
