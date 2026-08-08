# Pipeline Products and Compatibility Stages

The durable octree flow is defined by the products it publishes, not by a
numbered sequence. Numeric stage names describe the current CLI and directory
layout only. They remain compatibility labels while the implementation moves to
the purpose-based contracts in
[`streaming-pipeline.md`](streaming-pipeline.md).

The central requirement is selective reuse: changing one star in one upstream
shard must not rewrite a routed contribution, sorted group, topology partition,
or materialized bucket whose relevant semantic checksum is unchanged.

## Product flow

```text
upstream shards
    -> routed contributions
    -> sorted contributions + cell summaries
    -> profile topology
    -> materialized buckets
    -> packed base artifacts
    -> optional sidecars
```

| Product | Rebuild boundary | Durable output |
|---|---|---|
| Routed contributions | Input shard and staging group | Immutable raw row fragments |
| Sorted contributions | One changed routed contribution | Canonical rows plus cell summaries |
| Profile topology | Changed spatial partition and ancestor spine | Natural-cell to profile-cell mapping |
| Materialized buckets | Profile and affected spatial partition | Aligned render and identity byte ranges |
| Packed artifacts | Output profile | `stars.octree` and `identifiers.order` |
| Sidecars | Profile, family, and affected identity range | Schema-bearing sidecar artifacts |

## Compatibility mapping

The current commands collapse some target products together:

| Compatibility command | Current responsibility | Target action/product |
|---|---|---|
| `stage-00` | Route input shards into the adaptive staging tree | `route` / routed contributions |
| `stage-01` | Sort and compact changed staging groups | `prepare` / sorted contributions |
| `stage-02` | Plan classic or terminal topology, materialize, and pack | `materialize` plus `pack` |
| `stage-03` | Build optional sidecars | `sidecars` |

No new architecture, manifest, or module should acquire another numbered-stage
name. The intended public vocabulary is `route`, `prepare`, `materialize`,
`pack`, `sidecars`, and `build`. Renaming the existing CLI is a separate
migration and is not implied by this document.

## Shared identities and manifests

A tree identity manifest protects build-defining semantics. It should include:

- coordinate frame and coordinate convention;
- world origin, bounds, and Morton bit depth;
- magnitude-to-natural-level configuration;
- staging bucket size and routing policy;
- source and row-schema identities;
- canonical ordering policy; and
- output-profile topology and encoding versions where applicable.

Mutable progress and dependency records remain separate. Small JSON manifests
may track product identities and checkpoint locations, but catalogue-scale
cell or dependency records belong in sorted binary or Parquet partitions. One
large JSON dirty-node array is not a scalable dependency index.

Use multiple semantic identities because different changes affect different
products:

- routing identity: fields that determine staging-group membership;
- ordering identity: natural cell and canonical sort fields;
- render identity: fields used by the encoded render record;
- star identity: `source` and `source_id` ordering/output;
- topology identity: cell counts plus profile policy; and
- sidecar identity: fields and schema used by that sidecar family.

Dirty propagation stops whenever the identity relevant to the next consumer is
unchanged. The current `clean`/`all` downstream marker is a conservative
compatibility fallback, not the target contract.

## Routed contributions

Routing reads one replaceable upstream shard and publishes its contribution to
each staging group:

```text
(staging bucket, input shard id, fragment kind)
```

`input_shard_id` comes from the input directory name or root-level Parquet file
stem. A HEALPix file is a useful shard boundary, but batch files are valid too;
the upstream catalogue controls replacement granularity.

Routing requirements:

- calculate only placement fields such as `morton_code` and natural `level`;
- retain raw coordinates, photometry, temperature, identity, and sidecar input
  fields;
- never encode node-relative render coordinates before profile topology is
  selected;
- write new shard contributions to temporary paths;
- compare every old and new group by semantic checksum;
- atomically retain equal old contributions rather than rewriting them; and
- include old groups absent from the replacement in the changed set.

A replacement may read the complete shard because that is how moved and
deleted rows are discovered. It must not republish another contribution merely
because the same shard was reread.

Initial routing commits after each shard through a write-ahead fragment journal.
Independent per-group checksum checkpoints let validation resume after an
interruption without rehashing completed work.

## Sorted contributions

Preparation canonicalizes one changed routed contribution while preserving the
same `(staging bucket, input shard, kind)` replacement boundary. It does not
perform a global catalogue sort.

The current primary key is:

```text
level, final_node_id, mag_abs, source, source_id
```

where:

```text
final_node_id = morton_code >> (3 * (MORTON_BITS - level))
```

Remaining columns provide a deterministic schema-order tie-break so Arrow and
external-sort engines publish the same checksum even when all primary fields
match. Nulls sort last, and the policy identity changes if ordering semantics
change.

Preparation should emit canonical rows together with sorted per-cell row counts
and content checksums. Those summaries drive dependency-directed topology and
materialization invalidation without rescanning every row.

Ordinary groups use a bounded in-memory Arrow path. Large groups use a bounded
external sorter. DuckDB is appropriate when its vectorized scan, sort, and
controlled spill outperform a custom run merger. Explicit immutable sorted
runs and bounded fan-in merging remain appropriate when measurements show an
advantage or stronger restart reuse matters. The architectural requirement is
bounded, deterministic, sequential publication—not a ban on a particular
engine.

Published sorted fragments are immutable and content/policy-addressed. A
completed group is checkpointed before superseded fragments are garbage
collected.

## Profile topology

Topology planning maps natural cells to final cells without reading or encoding
render payloads.

- The classic profile maps cells through a configured maximum-level cap.
- The terminal-packed profile aggregates ordered cell-count runs bottom-up and
  selects the shallowest eligible terminal root.

Terminal counts are immutable sequential per-group and per-level products.
Bounded fan-in merges and bottom-up aggregation reuse equal groups, aggregate
levels, node levels, and completed terminal plans. The published terminal map
still has one global identity when a terminal decision genuinely changes.
Partitioned maps and a small ancestor spine remain the refinement needed to
limit that case to the changed subtree.

Topology policy is profile-specific. Count aggregation, partitioning,
checkpointing, and sequential merge mechanics are shared.

## Materialized buckets

Materialization is shared machinery parameterized by a profile topology. It:

1. streams sorted contributions;
2. maps natural cells to final profile cells;
3. encodes coordinates relative to the selected final cell;
4. preserves existing order where mapping does not disturb it;
5. creates bounded sorted runs for affected or overlapping cells;
6. combines those runs with bounded fan-in merging; and
7. publishes aligned render and identity ranges with per-cell checksums.

DuckDB may handle oversized local sorts when it is the measured best engine.
It is not used as a mutable catalogue row store. Disjoint cells remain on the
Arrow streaming path.

Materialized products are separate per profile because classic capping and
terminal packing can assign the same row to different cells. Completed group
runs and spatial output partitions are checkpointed, so a restart reuses
verified immutable outputs.

## Packing and sidecars

Packing consumes materialized byte ranges and manifests only. It must not read
source Parquet, repeat routing, recalculate topology, or independently rebuild
identity order.

STAR index packing compiles the sorted cell indexes bottom-up into sorted
logical topology runs. A bounded fan-in merge orders those nodes exactly as the
five-level final shards are written. Each five-level logical skeleton is
content-addressed, while the physical skeleton bytes are range-partitioned by
their level-four spatial ancestor into a configured, bounded number of
content-addressed pack files in the durable classic work directory. Skeletons
record node presence, payload presence, child masks, frontier count and terminal
policy, but deliberately exclude payload bytes, lengths, star counts and all
absolute offsets. Payload relocation is then a sequential merge with the
skeleton stream.

The selected emitter writes complete shards with zeroed frontier tables to one
dedicated scratch index. A bounded DFS stack records direct child offsets and
patches each completed parent frontier table with one positional write. The
completed index is copied to the final artifact in bounded sequential chunks;
the final artifact itself is never sought backwards. The scratch index is the
size of the final index section and is removed under the cache lock after copy
or failure. A lower-scratch prefix-sum emitter remains available for comparison:
it writes compact child-offset streams by five-level boundary and emits directly
to the final artifact. Both consume the same immutable skeleton and relocation
streams and produce byte-identical output.

`stage-02 --index-emission-strategy temp-pwrite-batched` selects the default
emitter. `--index-emission-strategy forward` selects the lower-scratch
alternative. The per-child positional-write benchmark variant is intentionally
not exposed as a production option. Because the two production emitters are
byte-identical serialization mechanics, this operational choice is excluded
from the final artifact's semantic identity.

`benchmarks/benchmark_combine_index.py` compares both production emitters and a
per-child control in isolated cold and warm processes. The acceptance matrix
uses dense and sparse 2k/8k/16k fixtures plus an optional sparse 64k case with
`node_id = i << 15`. In the reproduced sparse 64k run, batched emission took
4.852 seconds cold and 2.475 seconds warm versus 5.056 and 2.616 seconds for
forward emission. It reduced final-output writes from 64,003 to 14 and used
three batched positional writes rather than 64,002 per-child writes. Dense 16k
favored forward emission, so batched is selected for the production-shaped
sparse topology, not as a universal winner. These fixture measurements do not
replace acceptance on a controlled large build.

Intermediate manifests carry a checksum of each shard's ordered node-ID
stream. This lets payload-only changes reuse the topology plan without reading
or compiling the topology again. Legacy manifests are scanned once and receive
a local checksum checkpoint. Normal cache validation trusts immutable published
files: it checks the compact plan checksum plus skeleton header, policy, size
and pack existence metadata. Full pack hashes are verified at initial
publication rather than reread on every restart. Cache compilation, publication,
temporary cleanup and active-plan pruning share an exclusive cache lock, so one
compiler cannot delete another compiler's live files.

The classic final render/identifiers pair has a durable semantic checkpoint.
An unchanged input/policy/descriptor identity reuses both artifacts and their
UUIDs without opening the large intermediate files. The checkpoint is written
only after both atomic file replacements. A crash between the two replacements
leaves the old checkpoint invalid, so the next invocation rebuilds the pair.

The current monolithic binary may require a full sequential rewrite after a
local change because compressed payload offsets move. That is acceptable while
the packer reuses unchanged materialized partitions. True partial publication
requires a separate container-format decision.

`identifiers.order` is produced from exactly the same materialized stream as
the render payload. Every render octree carries a `dataset_uuid`; sidecars and
identity artifacts carry the matching parent identity. A sidecar family may be
rebuilt independently when only its own source identity changed.

## One-shard and one-star replacement

For a replacement HEALPix shard:

1. Route the new shard to temporary per-group contributions.
2. Compare the union of old and new group keys.
3. Reuse equal immutable contributions.
4. Prepare only changed, new, or deleted contributions.
5. Compare old and new per-cell summaries.
6. Replan only affected profile topology branches.
7. Rematerialize only dependent profile partitions.
8. Repack from changed and reused materialized products.
9. Rebuild only sidecars whose own identity changed.

A single changed star commonly alters only one routed contribution. Other
groups from the same shard stop after checksum comparison. A no-op replacement
stops before sorting. A sidecar-only field change can stop before base render
materialization.

## Publication and recovery

Every expensive product follows the same protocol:

1. derive an input and policy identity;
2. write immutable temporary runs or partitions;
3. checkpoint each completed output;
4. verify row counts and semantic checksums;
5. atomically publish files;
6. publish the parent manifest last; and
7. garbage-collect unreachable old files only after durable publication.

A restart validates completed products and continues. It never deletes the
last published version before its replacement is durable.
