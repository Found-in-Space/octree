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

### R2. Stable Render Ordinals

Sidecar records identify stars by their ordinal in render order. Dense sidecars
may encode one entry per render star in exactly that order. Sparse sidecars may
encode only annotated ordinals, sorted in ascending order.

The canonical ordering carried by the materialized output profile is:

- `node_id`
- `mag_abs`
- `source`
- `source_id`

A sparse sidecar does not publish empty payloads. Its index contains payload
nodes only for annotated cells, payload-free ancestors needed to route to those
cells, and no node for an entirely empty branch.

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

## Optional `visual-duplicates` Sidecar

The visual-duplicate sidecar is a diagnostic overlay for the collected
one-to-one Gaia-Hipparcos supplemental display evidence. It is not an ordinary
build product and does not change merge composition or the render octree.

It is built only through the purpose-named command:

```bash
uv run fis-octree sidecars visual-duplicates \
  --project project.toml \
  --evidence path/to/fis_gaia_hip_supplemental_display_map.parquet
```

The evidence input must contain:

- `gaia_source_id`
- `hip_source_id`
- `mapping_source`
- `number_of_neighbours`
- `angular_distance`

Both endpoint columns must be one-to-one. The builder rejects evidence where an
endpoint participates in more than one pair.

### Sparse payload contract

Only cells containing a rendered evidence endpoint carry a payload. Each
payload is a gzip-compressed JSON array sorted by render `ordinal`. Every record
contains:

- `ordinal`: the endpoint's ordinal in the render cell;
- `pair_id`: stable `gaia:<id>|hip:<id>` pair identity;
- `role`: `gaia` or `hip`;
- `identity`: the endpoint's stable catalog identity;
- `counterpart_identity`: the rebuild-stable identity of the other endpoint;
- `counterpart_ref`: the other endpoint's `{level, mortonCode, ordinal}` when
  it is rendered, otherwise `null`;
- `mapping_source`, `number_of_neighbours`, and
  `angular_distance_arcsec`: source-evidence context.

`counterpart_ref` omits `datasetId`: the sidecar descriptor's
`parent_dataset_uuid` supplies it. A consumer constructs a full SkyKit
`StarObjectRef` from that parent UUID and the stored cell/ordinal fields.

The adjacent report records the evidence SHA-256, identity-order artifact UUID,
parent dataset UUID, sidecar UUID, output SHA-256, payload cell count, and pair
coverage for both, one, or neither rendered endpoint.

### Bounded scan

The builder scans `identifiers.order` once. It decompresses at most one bounded
cell at a time, groups cells into a bounded byte batch, and uses vectorized
identity matching rather than constructing a Python tuple for every rendered
star. Only evidence endpoints and their resolved render references remain in
memory. The evidence row count also has an explicit configurable bound.

The proposed [`identity-lookup-index.md`](identity-lookup-index.md) locator
would replace that full identity scan with bounded exact lookups. The locator
is a reusable dataset companion rather than part of this optional sidecar.

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
