# Sidecars Specification

## Purpose

Define sidecar artifacts built for a specific render dataset package.

Sidecars add per-star identity and enrichment data without changing the render
payload format in `stars.octree`.

Each sidecar is a schema-bearing artifact. A consumer should be able to open a
sidecar, validate that it belongs to the active render dataset, inspect its
embedded schema, and decode typed records. A separate "sidecar family" registry
is not part of the artifact model; names such as `meta` are sidecar artifact
names or definitions used for build/discovery.

## Product Placement

Sidecars consume a published render octree and its matching
`identifiers.order`; they do not participate in routing, topology planning, or
base materialization. The current compatibility mapping is:

- `stage-02` publishes `stars.octree` and `identifiers.order`;
- `stage-03` builds the configured sidecar families from those artifacts.

Each output profile must get separate sidecar artifacts because its node set and
identity order may differ. The purpose-based architectural action is
`sidecars`; `stage-03` is its current compatibility command.

## Core Invariants

### R1. Same Cell Identity

Each sidecar payload entry corresponds to exactly one render cell identified by:

- `level`
- `node_id`

### R2. Same Star Order

Within a cell, sidecar star order must match render star order exactly.

The canonical ordering carried by the materialized output profile is:

- `node_id`
- `mag_abs`
- `source`
- `source_id`

### R3. UUID Compatibility First

Each sidecar octree must carry:

- `parent_dataset_uuid`
- `sidecar_uuid`
- an embedded schema or schema descriptor

Consumers must validate `parent_dataset_uuid` against the active render octree
`dataset_uuid` before falling back to geometry checks.

### R4. Optionality

Sidecars are optional. Core rendering must continue to work without them.

### R5. Embedded Schema

The sidecar artifact must describe its payload schema. Discovery manifests may
list sidecar names and paths, but decoding should be driven by schema metadata
embedded in the sidecar artifact itself.

## Current `meta` Sidecar

The `meta` sidecar is built from:

- `identifiers.order`
- `identifiers_map.parquet`

Each payload blob is a gzip-compressed JSON array with one entry per star in ordinal order.

Every entry always contains:

- `source`
- `source_id`

Optional enrichment fields come from `identifiers_map.parquet` keyed by `(source, source_id)`.

Supported enrichment fields are:

- `gaia_source_id`
- `hip_id`
- `hd`
- `bayer`
- `flamsteed`
- `constellation`
- `proper_name`

`[[stage03.sidecars]]` currently configures sidecar definitions in the project
file.

For `meta`, `fields = [...]` limits which enrichment columns are emitted. `source` and `source_id` are always included.

### Enrichment Lookup

The enrichment map and the identity request stream have very different sizes
and access patterns. `identifiers_map.parquet` is normally a small curated map,
while `identifiers.order` can contain every rendered star and presents requests
in spatial/cell order rather than identity order.

The metadata encoder therefore consumes the Parquet map in bounded batches and
uses an adaptive lookup backend:

- maps whose conservative logical estimate stays within 64 MiB use an in-memory
  hash map, then stream identity requests through direct lookups;
- larger maps are promoted to a private disk-backed SQLite primary-key index;
  the in-memory entries are released and subsequent ingestion remains batched.

SQLite is used only for the overflow random-lookup case. DuckDB is effective for
bulk scans and external ordering, but a cell-at-a-time join would repeatedly
scan or rebuild a hash table over the enrichment map. A pure sort/merge join
would require writing all identity requests, sorting them by identity, joining,
then externally sorting the results back into cell order. That multi-pass design
becomes attractive only if enrichment maps grow beyond the bounded hash-map fast
path and measurements show that building the disk primary-key index dominates.

Both backends preserve input request order, duplicate-map last-row-wins behavior,
field normalization, and byte-identical gzip JSON payloads.

## Intermediate Files

The current implementation builds per-sidecar intermediate shard files under:

- `paths.stage03_output_dir/intermediates/<sidecar-name>/`

For the `meta` sidecar, shard filenames end with:

- `.meta.index`
- `.meta.payload`

These intermediates use the same shard structure as render intermediates, but with `artifact_kind = sidecar`.

## Final Artifacts

The current compatibility builder writes each selected family to:

- `paths.stage03_output_dir/<sidecar-name>.octree`

A future multi-profile directory layout must keep separate artifacts per
profile; that path migration is independent of the artifact format.

The final sidecar octree keeps the STAR top-level header and adds the mandatory descriptor block immediately after it.

For sidecars the descriptor carries:

- `artifact_kind = sidecar`
- `parent_dataset_uuid`
- `sidecar_uuid`
- embedded schema or a pointer to an embedded schema block

## Sidecar Manifest

The sidecar build writes:

- `paths.stage03_output_dir/manifest.json`

It records:

- `render_octree_path`
- `identifiers_order_path`
- `parent_dataset_uuid`
- one descriptor per built sidecar artifact

Each sidecar descriptor records:

- `name`
- `output_path`
- `parent_dataset_uuid`
- `sidecar_uuid`
- schema summary or schema block reference

## Rebuild Policy

Sidecars are immutable derived artifacts.

When enrichment inputs change:

1. keep the profile render octree unchanged
2. keep the profile `identifiers.order` artifact unchanged
3. rebuild the affected sidecar artifact for each affected profile
4. publish a new `sidecar_uuid`

Sidecar payload data can share build-time caches keyed by stable star identity,
but packaged sidecar artifacts are profile-specific. Sharing one packaged
sidecar across `classic` and `unbounded` would require profile-specific node and
item-order mapping, which defeats the simple sidecar invariant.

## Related Docs

- `docs/stages.md`
- `docs/identifiers-order.md`
- `docs/reader.md`
