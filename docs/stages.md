# Pipeline Stages

This document captures the working stage model. It is intentionally
purpose-level: the exact file names, manifests, and packing formats are still
expected to move while the architecture settles.

The durable idea is that the expensive spatial indexing work should produce a
reusable staging tree. Later stages should sort, encode, materialize, and pack
that staged data without repeatedly duplicating the whole catalogue.

The current compatibility CLI collapses materialization and packing for the
traditional level-capped output into `stage-02`. The finer-grained stages below
remain the target decomposition for additional output variants.

The implementation plan, manifest sketches, and work streams are tracked in
[`staged-pipeline-plan.md`](staged-pipeline-plan.md).

## Overview

| Stage | Purpose | Typical Input | Typical Output |
|---|---|---|---|
| Stage 00 | Partition input shards into octree staging buckets. | Merged parquet shards from the catalogue pipeline. | `(node, input_shard_id, kind)` staging groups. |
| Stage 01 | Sort and compact staged `(node, input_shard_id, kind)` groups in place. | Stage 00 staging folders. | Canonical, replaceable staged parts. |
| Stage 02 | Optionally rewrite payload bytes without re-indexing. | Stage 01 staged parts. | Updated staged payload columns or payload fragments. |
| Stage 03 | Materialize canonical per-node payload order. | Sorted staged parts. | Payload-order byte arrays plus star identity indexes. |
| Stage 04 | Pack canonical node outputs into final octree artifacts. | Stage 03 byte arrays and indexes. | `stars.octree` and companion identity/order artifacts. |
| Stage 05 | Build optional derived sidecar families. | Stage 04 base dataset plus enrichment inputs. | Named sidecar octrees such as `meta.octree`. |

## Tree Manifests

Every stage that writes into an existing tree should check the top-level tree
manifest before touching data. The manifest is the guardrail against accidentally
mixing rows indexed under different semantics.

The top-level manifest should contain build-defining identity such as:

- coordinate frame and coordinate convention
- world origin, world bounds, and Morton bit depth
- magnitude-to-level configuration
- staging bucket size and split policy
- input catalogue identity
- row schema and payload schema versions where relevant

If any setting can move a star to a different staging or final node, Stage 00
must refuse to append to the existing tree. Later stages can usually tolerate
changes that only affect payload encoding or final package layout.

Progress and dirtiness should live separately from the build identity. A mutable
stage-state manifest can record:

- completed stages
- dirty input shards
- dirty staging nodes
- fragment counts, row counts, and checksums
- stage-specific output versions

Filename markers are useful for quick scans, but the manifest should be the
source of truth. A command should never trust a filename suffix without checking
that the tree manifest matches the requested build.

## Stage 00: Partition

Stage 00 reads merged parquet shards and routes each row into the staging tree.
The durable output shape is `(node, input_shard_id, kind)`: each staging node
keeps rows grouped by the upstream shard that produced them.

The `input_shard_id` is derived from the input directory name or root-level
parquet filename stem. HEALPix files are one useful shard layout, but batch
files are valid too. Stage 00 does not reinterpret row-level HEALPix columns as
the rebuild boundary; the upstream catalogue pipeline owns the sharding
strategy and should choose stable shards that match its desired rebuild
workflow.

The important semantic point is that Stage 00 should not change which final
octree node a star belongs to. It may pack rows into a shallower staging node,
but that is only an intermediate layout choice.

Current direction:

- Require placement fields needed for routing, currently `morton_code` and
  natural `level`; the explicit raw Cartesian filter may calculate them.
- Preserve every input row and pass non-routing columns through unchanged.
- Preserve raw `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `mag_abs`, and optional
  `teff` through staging so final output profiles can choose their actual node.
- Allow routing enrichment only through an explicitly configured pre-filter
  that must preserve row count and must not create node-relative render
  coordinates.
- Keep sparse regions shallow in the staging filesystem.
- Let dense staging nodes become lower-magnitude limited only after they reach
  the configured row cap.
- Preserve enough input shard identity that one shard can be deleted and
  rebuilt without rewriting unrelated shards.
- Commit initial builds after each input shard through a write-ahead fragment
  journal. A restart rolls back only the uncommitted shard.
- Checkpoint semantic checksums per group so the final validation pass can
  resume without re-reading groups already verified.

## Stage 01: Sort And Pack

Stage 01 canonicalizes the `(node, input_shard_id, kind)` groups in place. It
sorts any unsorted fragments, compacts small fragments where useful, and marks
the result as ready for downstream materialization.

This stage should be rerunnable over the staging tree. If Stage 00 adds fresh
unsorted parts for one input shard, Stage 01 should only need to revisit the
affected folders.

Current direction:

- Preserve the `(node, input_shard_id, kind)` replaceability boundary.
- Preserve raw position and photometry columns; do not encode a 16-byte render
  record while final node placement is still profile-dependent.
- Keep the in-memory Arrow sort for ordinary groups, but route groups above the
  normal row or uncompressed-byte limits through a disk-backed DuckDB external
  sort with bounded memory.
- Use atomic temp files and renames for rewritten fragments.
- Write one small recovery checkpoint per completed group, then consolidate the
  main stage-state manifest once at completion.
- Store only bounded group summaries such as `natural_max_level`; do not store
  occupied final-node arrays in shared JSON state.
- Use a bounded `clean`/`all` downstream invalidation flag until Stage 03 owns a
  disk-backed profile-specific invalidation index.
- Use filename markers as an optimization, for example `unsorted`, `sorted`, or
  `packed`, while still validating against manifests.

## Stage 02: Re-Encode Payloads

Stage 02 is optional. It exists for the case where the spatial index is still
valid, but the render payload format changes.

For example, if the payload byte layout changes but Morton codes, final render
levels, and node membership do not, Stage 02 can rewrite payload bytes from the
existing staged rows without repeating Stage 00.

Current direction:

- Refuse to run if the requested change affects node membership or ordering
  keys.
- Keep the same selective rebuild boundary as Stage 01.
- Allow old staged row data to be converted into new payload bytes before final
  materialization.

## Stage 03: Materialize Node Payload Order

Stage 03 is where packed staging becomes canonical node payload data.

This is the natural point to split non-lower-mag-limited staging nodes into the
final magnitude-limited payload structure. Stage 00 and Stage 01 may keep a
sparse field packed high in the staging tree, but Stage 03 must materialize the
final node payloads in the order expected by the renderer and sidecar builders.

Current direction:

- Read all `(node, input_shard_id, kind)` staged parts that contribute to a
  node.
- Interleave shard fragments into canonical payload order.
- Preserve each Stage 01 group's existing canonical order when profile mapping
  does not change it; do not introduce a full-catalogue sort.
- Reorder only bounded group runs affected by output-profile level folding,
  then combine final cells with a bounded fan-in merge.
- Fan out rows from packed staging nodes into their final payload nodes when the
  final render level is deeper than the staging node.
- Encode node-relative coordinates once, after the output profile has selected
  each row's actual final node.
- Write raw payload bytes directly to disk in payload order.
- Write an identity index or order file beside those payload bytes.
- Record per-node offsets, row counts, checksums, and dirty state in a manifest.
- Checkpoint normalized group runs and completed spatial output partitions so
  interrupted materialization can resume.
- Keep bounded caches of payload and relocation file handles during final DFS
  packing; do not perform an open/close cycle for every output cell.

The useful handoff to Stage 04 is a file layout like:

- one large raw payload byte array per materialized node group
- one identity/order side file for the same star order
- one manifest describing byte ranges for each final node payload

That lets Stage 04 avoid opening parquet or re-sorting rows. It can memory-map
the raw byte arrays, read the byte range for a final node, compress that slice,
and write the final artifact payload.

## Stage 04: Pack Final Artifacts

Stage 04 turns Stage 03 node outputs into the final octree package.

The key job is packaging, not re-indexing. Stage 04 should consume canonical
payload byte ranges and identity/order ranges, compress the slices needed by the
final format, and build the final octree indexes.

Current direction:

- Memory-map Stage 03 byte arrays where practical.
- Compress final node payload slices as they are written.
- Build the final render octree shape and lookup/index structures.
- Produce the companion identity/order artifact from the Stage 03 star identity
  order.
- Avoid reading Stage 00/01 parquet unless a node needs to be rematerialized.

Partial Stage 04 rebuilds may be possible if the final package format supports
localized replacement. If not, Stage 04 can still rebuild from compact Stage 03
outputs without repeating spatial indexing.

## Stage 05: Sidecars

Stage 05 builds optional sidecar families for an existing Stage 04 dataset.
Sidecars add extra data without changing the base render octree.

The first implemented family is `meta`, but the boundary is deliberately family
oriented: additional sidecars should be able to reuse the same Stage 04 package
and identity order.

Current direction:

- Build sidecars by configured family name.
- Stamp sidecars with the parent render dataset UUID.
- Use the Stage 04 identity/order artifact to keep sidecar star order aligned
  with render payload order.
- Allow sidecar families to be rebuilt independently from the base dataset.

## Selective Rebuild Flow

The staging tree should make local rebuilds cheap:

1. Mark the affected input shard or staging node dirty.
2. Delete that input shard's staged fragments from affected
   `(node, input_shard_id, kind)` groups.
3. Re-run Stage 00 for the changed input shard.
4. Re-run Stage 01 over dirty or unsorted staged folders.
5. Re-run Stage 02 only if the payload encoding changed.
6. Re-run Stage 03 for affected materialized nodes.
7. Re-run Stage 04 as a partial pack if supported, or rebuild the final package
   from Stage 03 outputs.
8. Rebuild Stage 05 sidecars that depend on changed identity/order or enrichment
   data.

## Stable Ideas

- Stages communicate through files, not shared in-memory state.
- Expensive catalogue merge and reconciliation work belongs upstream of this
  repository.
- Stage 00/01 remain input-shard-replaceable.
- Stage 03 is the boundary where node payload order becomes canonical.
- Large stages should be bounded-memory by design.
- Render, identity/order, and sidecar artifacts should carry enough metadata for
  readers to reject mismatched files.

## CLI Shape

The final command names are expected to follow the stage numbers, but not every
stage in this document is implemented yet:

```bash
uv run fis-octree stage-00 --project project.toml
uv run fis-octree stage-01 --project project.toml
uv run fis-octree stage-02 --project project.toml
uv run fis-octree stage-03 --project project.toml
uv run fis-octree stage-04 --project project.toml
uv run fis-octree stage-05 --project project.toml
```

Build-defining paths and knobs live in the project TOML. Details that are still
being tuned should be treated as operational configuration rather than published
format guarantees.
