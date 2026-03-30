# Identifiers / Order Artifact

## Status

`identifiers.order` is part of the implemented Stage 02 base dataset package.

Its job is to preserve the canonical ordered star identities for one render octree dataset so later sidecar families can be rebuilt without reopening earlier pipeline stages.

## Stage Placement

The current pipeline is:

- Stage 00: row enrichment parquet
- Stage 01: render intermediates plus identifiers-order intermediates
- Stage 02: final `stars.octree` plus final `identifiers.order`
- Stage 03: named sidecar families and derived indices

## Primary Mapping

The artifact stores:

- `(level, node_id) -> ordered list of canonical star identities`

Each star identity is the canonical pair:

- `source`
- `source_id`

This is a forward mapping optimized for sidecar generation rather than reverse lookup.

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

Each payload blob stores exactly one cell’s ordered canonical identities.

For each star:

1. `u16` length of `source`
2. UTF-8 bytes of `source`
3. `u16` length of `source_id`
4. UTF-8 bytes of `source_id`

The final `identifiers.order` payload section stores these rows in compact binary form.

## Why It Exists

This artifact lets the base dataset package be archived as:

- `stars.octree`
- `identifiers.order`

Stage 03 can then rebuild sidecars from that package plus fresh enrichment inputs, without depending on Stage 00 or Stage 01 outputs.

## Validation And Cache Identity

The render octree carries `dataset_uuid`.

`identifiers.order` carries:

- `parent_dataset_uuid`
- artifact UUID

Consumers and builders must treat `parent_dataset_uuid` as the primary compatibility key for the render dataset.

## Relationship To Sidecars

`identifiers.order` is not a sidecar.

It is a foundational companion artifact for the render dataset.

The first implemented Stage 03 family is `meta`, but the same artifact can support additional sidecar families later.

## Related Docs

- `docs/stage-02.md`
- `docs/stage-03.md`
- `docs/sidecars.md`
- `docs/roadmap.md`
