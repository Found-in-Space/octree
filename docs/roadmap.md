# Octree Roadmap

## Status

The clean-break Stage 02 / Stage 03 architecture is now implemented.

The current pipeline is:

- Stage 00: enriched parquet
- Stage 01: render intermediates plus identifiers-order intermediates
- Stage 02: `stars.octree` plus `identifiers.order`
- Stage 03: named sidecar families

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

Stage 03 now builds sidecars by family name via `[[stage03.sidecars]]`.

`meta` is the first implemented family.

### Foundational Identifiers / Order Artifact

Stage 02 now emits `identifiers.order` as part of the base dataset package.

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

- more Stage 03 sidecar families beyond `meta`
- reverse lookup artifacts derived from `identifiers.order`
- richer provenance metadata for published manifests
- additional reader helpers for sidecar discovery beyond explicit `--meta-octree`

## Related Docs

- `docs/stage-01.md`
- `docs/stage-02.md`
- `docs/stage-03.md`
- `docs/identifiers-order.md`
- `docs/sidecars.md`
