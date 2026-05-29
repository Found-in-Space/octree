# Octree Roadmap

## Status

The clean-break base-dataset / sidecar architecture is implemented. Stage
numbering is being revised around the packed staging tree described in
`docs/stages.md`.

The current pipeline is:

- Stage 00: `(node, healpix)` staging partition
- Stage 01: in-place staging sort and compaction
- Stage 02: optional payload re-encoding
- Stage 03: canonical payload-order materialization
- Stage 04: `stars.octree` plus identity/order packaging
- Stage 05: named sidecar families

The current format also includes UUID-backed descriptor metadata:

- render octrees carry `dataset_uuid`
- sidecars carry `parent_dataset_uuid`, `sidecar_uuid`, and `sidecar_kind`
- `identifiers.order` carries `parent_dataset_uuid` plus its own artifact UUID

## Implemented Requirements

### Render Dataset Identity

Render octrees now expose `dataset_uuid` in the mandatory descriptor block written immediately after the STAR header.

### Sidecar Parent Matching

Sidecars now expose `parent_dataset_uuid`.

Readers and stats helpers should reject a sidecar when that UUID does not match the active render dataset.

### Sidecar Version Identity

Sidecars now expose `sidecar_uuid`.

Rebuilding a sidecar family for the same render dataset produces a new `sidecar_uuid`.

### Named Sidecar Registry

Sidecar builds are configured by family name via `[[stage03.sidecars]]` in the
current implementation. In the revised stage model this responsibility moves to
Stage 05.

`meta` is the first implemented family.

### Foundational Identifiers / Order Artifact

The base dataset package emits `identifiers.order` alongside the render octree.

Its primary mapping is:

- `(level, node_id) -> ordered list of canonical star identities`

### Explicit Project Configuration

Operational build commands now require an explicit project file and reject removed legacy keys such as:

- `stage01.sidecar_fields`
- `stage02.manifest_path`
- `stage02.meta_mode`
- `stage02.meta_output_path`

## Remaining Future Work

The new architecture creates room for later extensions without changing the clean stage boundary:

- more Stage 05 sidecar families beyond `meta`
- reverse lookup artifacts derived from `identifiers.order`
- richer provenance metadata for published manifests
- additional reader helpers for sidecar discovery beyond explicit `--meta-octree`

## Related Docs

- `docs/staged-pipeline-plan.md`
- `docs/stages.md`
- `docs/identifiers-order.md`
- `docs/sidecars.md`
