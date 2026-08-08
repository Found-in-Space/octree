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

### 1. Reusable cell summaries

Preparation should emit sorted per-natural-cell counts and field-specific
semantic checksums alongside canonical rows. These partitioned products replace
the current conservative downstream `clean`/`all` fallback and let a one-star
change stop at unchanged groups and cells.

### 2. Explicit profile topology

Move classic level capping into a topology mapping product. Replace
catalogue-scale terminal count storage with sequential per-level or
spatial-partition aggregation, then publish changed-subtree mappings and
ancestor summaries.

### 3. Shared materialization

Drive classic and terminal-packed outputs through the same contribution
streaming, bounded run generation, fan-in merge, encoding orchestration,
checkpointing, and partition publication. Profile code should own only topology
and unavoidable format differences.

The first acceptance milestone is byte-equivalent v1 output from the shared
path. Terminal-packed v2 follows after that foundation is proven.

### 4. Dependency-directed incremental rebuilds

Replace one HEALPix shard by comparing the union of old and new immutable
contributions. Prepare only changed groups, update only affected topology
branches, rematerialize only dependent profile partitions, and stop propagation
at every equal semantic checksum.

Validation must cover a no-op replacement, one changed star, a star moving
between groups, and deletion of a star.

### 5. Packing isolation and reuse

Restrict packing to materialized byte ranges and manifests. A local change may
still rewrite the current monolithic artifact sequentially, but must reuse
unchanged materialized partitions and avoid source Parquet, sorting, topology,
or encoding work.

A sharded final container and true partial publication remain a separate format
decision.

### 6. Purpose-based command migration

After product contracts stabilize, introduce purpose-based commands such as
`route`, `prepare`, `materialize`, `pack`, `sidecars`, and `build`. Retain the
numeric commands as aliases for a documented compatibility period. CLI and
configuration renaming must not be mixed into topology or materialization
correctness work.

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
