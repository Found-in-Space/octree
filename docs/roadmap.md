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
- a stage boundary that makes sidecars clearly optional and rerunnable
- a foundational artifact that preserves canonical star ordering for later sidecar builds

## Recommended Stage Model

Current implementation:

- Stage 00 builds enriched parquet
- Stage 01 builds render intermediates and, optionally, the current `meta` sidecar intermediates
- Stage 02 combines the render octree and can also combine the current `meta` sidecar into `.meta.octree`

Recommended future model:

- Stage 00 builds enriched parquet
- Stage 01 builds render intermediates and the render manifest
- Stage 02 builds the final render octree and a foundational `identifiers/order` companion artifact
- Stage 03 optionally builds one or more named sidecar families and derived indices for an existing render dataset UUID

Why Stage 03 is preferable:

- it makes sidecars visibly optional rather than part of the core render path
- it allows sidecars to be rebuilt when new identifiers or enrichment data arrive
- it gives a cleaner place for multi-sidecar family tooling and validation rules
- it avoids implying that `meta` is the generic name for every sidecar

Why the `identifiers/order` artifact belongs in Stage 2:

- it is foundational enough to be part of the base dataset package
- it lets later sidecar builds work from `stars.octree` plus one canonical companion artifact
- it can also be used to derive reverse lookup indices later without reopening old pipeline stages

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

The current `meta` sidecar should be treated as the first concrete sidecar family, not the final architecture.

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

### R5. Foundational Identifiers / Order Artifact

The Stage 2 base dataset package should also include a canonical identifiers/order artifact.

Its primary mapping should be:

- `(level, node_id) -> ordered list of canonical star identities`

Its primary role is to support future sidecar-family builds. Reverse lookup indices should be derived from it rather than baked into the foundational artifact itself.

See `docs/identifiers-order.md`.

### R6. Cache Identity

The recommended cache identity is:

- render octree cache: `dataset_uuid`
- identifiers/order artifact cache: `parent_dataset_uuid` plus artifact UUID
- sidecar cache: `parent_dataset_uuid`, sidecar kind, `sidecar_uuid`

This makes cache invalidation explicit and avoids coupling cache safety to URLs or filenames.

### R7. Consumer Validation

Consumers should reject a sidecar before use when its `parent_dataset_uuid` does not match the active render dataset UUID.

Geometry checks such as `max_level`, `world_center`, and `world_half_size` are still useful as secondary validation, but UUID compatibility should become the primary gate.

### R8. Rebuild And Merge Policy

Sidecars should be immutable derived artifacts.

When new identifiers or other enrichment inputs appear, the preferred process is to rebuild the affected sidecar family for the same `parent_dataset_uuid`, using the foundational identifiers/order artifact as the canonical build input, emit a new `sidecar_uuid`, and publish the new descriptor. Existing sidecar artifacts should not be patched in place.

Within one sidecar family, upstream sources should be merged by canonical star identity before encoding. Field-collision rules should be explicit per family rather than implicit last-write-wins behavior.

### R9. `meta` Naming

`meta` is the name of the current sidecar family in the existing implementation.

It is not the generic term for sidecars, and future tooling should avoid using `meta` as a blanket flag for all sidecar families.

## Likely Implementation Shape

These requirements will probably require:

- a top-level header revision for render octrees
- a Stage 2 identifiers/order artifact with its own compact binary format
- a header or descriptor revision for sidecar outputs
- a manifest revision that generalizes `meta_*` paths into named sidecar descriptors
- reader and stats tooling updates so UUID compatibility is checked directly
- a Stage 03 CLI surface for building or rebuilding named sidecar families and derived indices independently of render-octree assembly

## Related Docs

- `docs/stage-01.md`
- `docs/stage-02.md`
- `docs/sidecars.md`
- `docs/identifiers-order.md`
- `docs/reader.md`
