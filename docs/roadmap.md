# Octree Roadmap

## Status

The current stage documents define the working v1 pipeline for:

- Stage 00 enrichment parquet
- Stage 01 intermediate shard files and manifest
- Stage 02 final `stars.octree`
- one optional metadata sidecar path

Those documents remain the source of truth for the current format and pipeline behavior.

This roadmap records forward-looking requirements that are not fully represented by the current v1 file layouts yet.

## Why This Exists

The viewer platform now needs stronger dataset identity and broader metadata support than the current single-sidecar model was designed for.

In particular, we need:

- explicit dataset identity that survives URL changes
- cache-safe versioning for both render octrees and sidecars
- support for multiple named sidecar families for the same render dataset
- compatibility checks stronger than geometry-only comparisons

## Accepted Future Requirements

### R1. Render Dataset UUID

Each render octree should expose a canonical dataset UUID in its top-level header.

That UUID is the primary identity for:

- dataset compatibility checks
- cache keys
- viewer-side dataset sharing
- sidecar parent matching

File-format version and manifest-format version are separate from dataset identity and should remain separate.

### R2. Sidecar Parent UUID

Each sidecar artifact should expose `parent_dataset_uuid`.

This UUID identifies the exact render octree dataset whose star ordering the sidecar matches.

This matters because sidecars depend on ordinal alignment, not only on broad geometric compatibility.

### R3. Sidecar UUID

Each sidecar artifact should also expose its own `sidecar_uuid`.

`sidecar_uuid` identifies one concrete version of one sidecar family for one render dataset. It allows caches to distinguish:

- two revisions of the same sidecar family for one render dataset
- different sidecar families for one render dataset

### R4. Named Multi-Sidecar Registry

The current metadata sidecar should be treated as the first concrete sidecar family, not the final architecture.

Future manifests and tooling should support several named sidecars for one render dataset, for example:

- `identifiers`
- `exoplanets`
- `stellarParameters`
- `curatedRoutes`

Each descriptor should include at least:

- sidecar kind
- index path
- payload path
- `parent_dataset_uuid`
- `sidecar_uuid`

### R5. Cache Identity

The recommended cache identity is:

- render octree cache: `dataset_uuid`
- sidecar cache: `parent_dataset_uuid`, sidecar kind, `sidecar_uuid`

This makes cache invalidation explicit and avoids coupling cache safety to URLs or filenames.

### R6. Consumer Validation

Consumers should reject a sidecar before use when its `parent_dataset_uuid` does not match the active render dataset UUID.

Geometry checks such as `max_level`, `world_center`, and `world_half_size` are still useful as secondary validation, but UUID compatibility should become the primary gate.

## Likely Implementation Shape

These requirements will probably require:

- a top-level header revision for render octrees
- a header or descriptor revision for sidecar outputs
- a manifest revision that generalizes `meta_*` paths into named sidecar descriptors
- reader and stats tooling updates so UUID compatibility is checked directly

## Related Docs

- `docs/stage-01.md`
- `docs/stage-02.md`
- `docs/sidecars.md`
- `docs/reader.md`
