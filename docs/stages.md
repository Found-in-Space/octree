# Pipeline Stages

This document is intentionally purpose-level. The staging layout, shard shape,
and final binary details are still changing, so the durable contract for now is
what each stage is responsible for and where the stage boundaries sit.

## Overview

| Stage | Purpose | Typical Input | Typical Output |
|---|---|---|---|
| Stage 00 | Prepare catalogue rows for octree assembly. | Merged HEALPix parquet from the catalogue pipeline. | A packed, octree-shaped staging tree. |
| Stage 01 | Convert staged rows into bounded-memory intermediate shards. | Stage 00 staging tree. | Render and identifiers-order shard families plus manifests. |
| Stage 02 | Package the base dataset. | Stage 01 manifests and shards. | `stars.octree` and `identifiers.order`. |
| Stage 03 | Build optional derived artifact families. | Stage 02 outputs plus enrichment inputs. | Named sidecar octrees such as `meta.octree`. |

## Stage 00

Stage 00 is the bridge between the merged catalogue and the octree build. It
computes missing octree row fields such as Morton code, render payload, and
render level, then places rows into a packed staging tree.

The important semantic point is that Stage 00 should not change which final
octree node a star belongs to. It only chooses an intermediate file layout that
makes later node construction and HEALPix reprocessing cheaper.

Current direction:

- Keep sparse regions shallow in the staging filesystem.
- Let dense nodes become lower-magnitude limited only after they reach the
  configured row cap.
- Keep enough HEALPix identity in filenames or manifests that a single HEALPix
  input can be removed and rebuilt without rewriting unrelated inputs.

## Stage 01

Stage 01 turns Stage 00 rows into intermediate shard files that can be combined
without holding the full catalogue in memory.

It is responsible for grouping rows into octree cells, preserving the canonical
per-cell star order, and writing the manifest information Stage 02 needs. It
also prepares the companion identifiers-order data used by later sidecars.

Current direction:

- Treat Stage 00 as the source of octree row fields.
- Keep memory bounded by flushing shard/cell data incrementally.
- Produce intermediate artifacts that Stage 02 can combine deterministically.

## Stage 02

Stage 02 packages the base dataset. It combines Stage 01 intermediates into the
render octree and the companion identity-order artifact.

`stars.octree` is the streamable render dataset. `identifiers.order` preserves
the canonical star identity order for the same dataset so sidecars can be built
or rebuilt without depending on Stage 00/01 outputs.

Current direction:

- Keep the base render dataset immutable once published.
- Carry UUID metadata so readers can validate related artifacts.
- Keep the combine step bounded-memory and suitable for large shard sets.

## Stage 03

Stage 03 builds optional sidecar families for an existing Stage 02 dataset.
Sidecars add extra data without changing the base render octree.

The first implemented family is `meta`, but the boundary is deliberately family
oriented: additional sidecars should be able to reuse the same Stage 02 package
and identity order.

Current direction:

- Build sidecars by configured family name.
- Stamp sidecars with the parent render dataset UUID.
- Allow sidecar families to be rebuilt independently from the base dataset.

## Stable Ideas

- Stages communicate through files, not shared in-memory state.
- Expensive catalogue merge and reconciliation work belongs upstream of this
  repository.
- The octree build should remain restartable and inspectable at stage
  boundaries.
- Large stages should be bounded-memory by design.
- Render, identifiers-order, and sidecar artifacts should carry enough identity
  metadata for readers to reject mismatched files.

## CLI Shape

The command names are expected to remain stable even while internals move:

```bash
uv run fis-octree stage-00 --project project.toml
uv run fis-octree stage-01 --project project.toml
uv run fis-octree stage-02 --project project.toml
uv run fis-octree stage-03 --project project.toml
```

Build-defining paths and knobs live in the project TOML. Details that are still
being tuned should be treated as operational configuration rather than published
format guarantees.
