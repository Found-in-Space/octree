# Identifiers / Order Artifact

## Status

`identifiers.order` is part of the base dataset package.

Its job is to preserve the canonical ordered star identities for one render octree dataset so later sidecar families can be rebuilt without reopening earlier pipeline products.

## Product Placement

`identifiers.order` is emitted beside `stars.octree` by packing. Both consume
the same materialized cell stream, so cell membership and within-cell ordinal
order cannot diverge. The command mapping is:

- `build`: topology planning, materialization, and packing of
  `stars.octree` plus `identifiers.order`;
- `sidecars build`: named sidecars derived from that published pair.

The durable architectural products are `materialize`, `pack`, and `sidecars`.

## Primary Mapping

The artifact stores:

- `(level, node_id) -> ordered list of canonical star identities`

Each star identity is the canonical pair:

- `source`
- `source_id`

This is a forward mapping optimized for sidecar generation rather than reverse lookup.

The proposed range-addressable reverse mapping is specified separately in
[`identity-lookup-index.md`](identity-lookup-index.md). It maps exact canonical
identities back to cell records and render ordinals without requiring a client
to download or scan this complete artifact.

## Binary Layout

`identifiers.order` uses a compact binary layout:

1. file header
2. fixed-width directory sorted by `(level, node_id)`
3. payload section containing ordered identity rows

### Header

```python
HEADER_FMT = struct.Struct("<4sHH16s16sQQQQQ")
HEADER_MAGIC = b"OIOR"
HEADER_VERSION = 1
```

Fields, in order:

1. magic
2. version
3. header size
4. `parent_dataset_uuid`
5. artifact UUID
6. directory offset
7. directory length
8. payload offset
9. payload length
10. record count

### Directory Records

```python
DIRECTORY_RECORD_FMT = struct.Struct("<H2xQIQQ")
```

Each record stores:

- `level`
- `node_id`
- `star_count`
- `payload_offset` relative to the payload section
- `payload_length`

### Payload Encoding

Each payload blob stores exactly one cell's ordered canonical identities, gzip-compressed.

The uncompressed content encodes each star as:

1. `u16` length of `source`
2. UTF-8 bytes of `source`
3. `u16` length of `source_id`
4. UTF-8 bytes of `source_id`

This matches the per-cell gzip compression used by `stars.octree` and sidecar octrees.

## Why It Exists

This artifact lets the base dataset package be archived as:

- `stars.octree`
- `identifiers.order`

The sidecar product can then rebuild enrichment artifacts from that package plus
fresh enrichment inputs, without reopening routed or sorted catalogue products.

## Validation And Cache Identity

The render octree carries `dataset_uuid`.

`identifiers.order` carries:

- `parent_dataset_uuid`
- artifact UUID

Consumers and builders must treat `parent_dataset_uuid` as the primary compatibility key for the render dataset.

## Relationship To Sidecars

`identifiers.order` is not a sidecar.

It is a foundational companion artifact for the render dataset.

The first implemented sidecar family is `meta`, but the same artifact can support additional sidecar families later.

## Related Docs

- `docs/octree-spec.md`
- `docs/products.md`
- `docs/sidecars.md`
- `docs/identity-lookup-index.md`
- `docs/roadmap.md`
