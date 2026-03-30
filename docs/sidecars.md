# Sidecars Specification

## Purpose

Define named sidecar artifacts built in Stage 03.

Sidecars add per-star identity and enrichment data without changing the render payload format in `stars.octree`.

`meta` is the first implemented sidecar family. It is not the generic name for all sidecars.

## Stage Placement

The current pipeline is:

- Stage 00: row enrichment parquet
- Stage 01: render intermediates plus identifiers-order intermediates
- Stage 02: `stars.octree` plus `identifiers.order`
- Stage 03: named sidecar families

Stage 03 consumes the Stage 02 base dataset package for one `dataset_uuid` and emits one or more sidecars for that same dataset.

## Core Invariants

### R1. Same Cell Identity

Each sidecar payload entry corresponds to exactly one render cell identified by:

- `level`
- `node_id`

### R2. Same Star Order

Within a cell, sidecar star order must match render star order exactly.

The canonical ordering carried forward from Stage 01 and Stage 02 is:

- `node_id`
- `mag_abs`
- `source_id`

### R3. UUID Compatibility First

Each sidecar octree must carry:

- `parent_dataset_uuid`
- `sidecar_uuid`
- `sidecar_kind`

Consumers must validate `parent_dataset_uuid` against the active render octree `dataset_uuid` before falling back to geometry checks.

### R4. Optionality

Sidecars are optional. Core rendering must continue to work without them.

## Current `meta` Family

The `meta` family is built from:

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

`[[stage03.sidecars]]` configures families in the project file.

For `meta`, `fields = [...]` limits which enrichment columns are emitted. `source` and `source_id` are always included.

## Intermediate Files

Stage 03 builds per-family intermediate shard files under:

- `paths.stage03_output_dir/intermediates/<family>/`

For the `meta` family, shard filenames end with:

- `.meta.index`
- `.meta.payload`

These intermediates use the same shard structure as render intermediates, but with `artifact_kind = sidecar`.

## Final Artifacts

Each final sidecar is written to:

- `paths.stage03_output_dir/<family>.octree`

The final sidecar octree keeps the STAR top-level header and adds the mandatory descriptor block immediately after it.

For sidecars the descriptor carries:

- `artifact_kind = sidecar`
- `parent_dataset_uuid`
- `sidecar_uuid`
- `sidecar_kind`

## Stage 03 Manifest

Stage 03 writes:

- `paths.stage03_output_dir/manifest.json`

It records:

- `render_octree_path`
- `identifiers_order_path`
- `parent_dataset_uuid`
- one descriptor per built family

Each family descriptor records:

- `name`
- `output_path`
- `parent_dataset_uuid`
- `sidecar_uuid`

## Rebuild Policy

Sidecars are immutable derived artifacts.

When enrichment inputs change:

1. keep the Stage 02 render octree unchanged
2. keep the Stage 02 `identifiers.order` artifact unchanged
3. rebuild the affected sidecar family
4. publish a new `sidecar_uuid`

## Related Docs

- `docs/stage-03.md`
- `docs/identifiers-order.md`
- `docs/reader.md`
