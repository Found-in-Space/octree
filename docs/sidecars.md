# Sidecars specification

## Purpose

Define the sidecar artifact written alongside Stage 01 intermediate octree shards.

The sidecar provides per-star identity and metadata without changing the main render payload format.

`meta` is the name of the current sidecar family in the implementation today. It should not be read as the generic word for all sidecars.

## Scope

This specification covers:

- where the sidecar is built in the pipeline
- required ordering and identity invariants
- JSON payload contract for sidecar files
- consumer lookup behavior

This specification does not cover:

- reverse/global indices (`source_id -> octree location`)
- UI/client search ranking behavior
- post-build derived artifacts

---

## Stage placement

Current implementation:

- the `meta` sidecar family is built in **Stage 01**, in the same streaming pass that writes main intermediate shards
- the final `*.meta.octree` artifact is optionally combined in **Stage 02**

Recommended future model:

- render octree production should remain Stages 00-02
- Stage 02 should also emit a foundational `identifiers/order` artifact for the same render dataset UUID
- named sidecar families should move to an optional **Stage 03**
- Stage 03 should target an already-built render dataset UUID and consume the Stage 2 base package to emit one or more sidecar artifacts for that dataset

### Rationale

- The per-star ordinal is defined by Stage 01 cell grouping and row order.
- Sidecar rows must match render rows exactly within each cell.
- A separate pass is still viable as long as it can reproduce the same row ordering from canonical inputs.
- The preferred future canonical input is the Stage 2 `identifiers/order` artifact rather than old Stage 00 or Stage 01 files.
- Moving sidecars to Stage 03 makes optionality, reruns, and multi-sidecar support much clearer.

---

## Normative requirements

### R1. Same cell identity

Each sidecar payload entry corresponds to exactly one cell identified by:

- `level`
- `node_id`

where `node_id` is the level-specific Morton prefix used by Stage 01.

### R2. Same star order

Within a cell, sidecar star order must match main payload star order exactly.

The Stage 01 row stream must use:

`ORDER BY node_id, mag_abs, source_id`

where `source_id` provides a deterministic, unique-per-row tiebreak.

### R3. Append-only writes

Sidecar index and payload files are append-only during build.

### R4. Bounded memory

The implementation must remain streaming and bounded-memory, consistent with Stage 01 invariants.

Identifier enrichment data (the identifiers map from the pipeline) is loaded once into memory at build start. This is a small lookup table (order of thousands of entries) and does not violate the bounded-memory invariant.

### R5. Optionality

The sidecar is optional. When absent, consumers must continue to function for core rendering.

---

## Files

Per shard:

- `.meta-index`
- `.meta-payload`

The current `.meta-*` naming is one concrete sidecar family for the current implementation. It should not be treated as the final long-term model for all metadata products.

---

## Index format

Use the same index header and record layout as Stage 01 index files:

- `INDEX_FILE_HDR = struct.Struct("<4sHHBBHIQQ")`
- `INDEX_RECORD = struct.Struct("<QQII")`

Semantics:

- `node_id` identifies the cell
- `payload_offset`/`payload_length` locate the sidecar cell payload blob
- `star_count` equals the number of stars in this cell

---

## Payload format

Each blob is a gzip-compressed JSON array with one entry per star in ordinal order.

### Entry schema

Each entry is a JSON object. **Canonical identity is always present** for every star:

| Field | Type | Source | Description |
|---|---|---|---|
| `source` | string | Stage 01 row stream | Catalog key from merged output (e.g. `gaia`, `hip`, `manual`) |
| `source_id` | string | Stage 01 row stream | Primary identifier for that `source` (same string as in Stage 00 parquet) |

These two fields are written for **every** star and are **not** optional.

**Enrichment fields** (optional) come from the identifiers map (`identifiers_map.parquet`), looked up by the same `(source, source_id)` key. Enrichment fields are never null — absent means not applicable.

Field semantics and provenance for `identifiers_map.parquet` are defined by the producer of that file. This document specifies only how those fields are serialized into octree metadata payloads.

| Field | Type | Source | Description |
|---|---|---|---|
| `gaia_source_id` | integer | identifiers map | Gaia DR3 source identifier |
| `hip_id` | integer | identifiers map | Hipparcos catalog number |
| `hd` | integer | identifiers map | Henry Draper catalog number |
| `bayer` | string | identifiers map | Bayer designation (e.g. `"alpha Cen"`) |
| `flamsteed` | integer | identifiers map | Flamsteed number |
| `constellation` | string | identifiers map | Constellation abbreviation |
| `proper_name` | string | identifiers map | IAU proper name |

The final object is built by starting with `{ "source", "source_id" }` and merging any non-empty enrichment fields from the map. Map entries never replace `source` / `source_id` (those columns are not part of the enrichment field set).

### Examples

Gaia-only star with no row in the identifiers map (only canonical identity):

```json
{"source":"gaia","source_id":"2081898984199228160"}
```

Hipparcos star with Gaia crossmatch and catalog identifiers in the map:

```json
{"source":"hip","source_id":"71683","gaia_source_id":2081898984199228160,"hip_id":71683,"hd":128620,"bayer":"alpha Cen","proper_name":"Rigil Kentaurus"}
```

Manual star (e.g. the Sun) with override identifiers in the map:

```json
{"source":"manual","source_id":"sun","proper_name":"Sun"}
```

### Identifier enrichment

The sidecar builder loads the identifiers map once at build start. For each star in the streaming pass, the builder starts from `(source, source_id)` from the row, then looks up that key in the map. When a match is found, the selected enrichment fields from the map entry are merged into the JSON object (on top of the always-present identity).

### Configurable identifier inclusion

`--sidecar-fields` (Stage 01) limits **which enrichment columns** from `identifiers_map.parquet` are emitted into each entry. **`source` and `source_id` are always included** regardless of that list. The table above lists the full enrichment set; any subset of those column names is valid for `--sidecar-fields`.

### Constraints

- Array length must equal `star_count` from the index record.
- Every star gets an entry with at least `source` and `source_id`. Entries are never empty objects `{}`.

---

## Manifest requirements

When the sidecar is present, each manifest shard entry must include:

- `meta_index_path`
- `meta_payload_path`

This is the current v1 manifest shape for the existing metadata sidecar. A future manifest revision should generalize this into named sidecar descriptors rather than a single hard-coded `meta_*` slot.

---

## Consumer lookup contract

Given `(level, node_id, ordinal)`:

1. locate shard for `(level, node_id)`
2. binary-search sidecar index by `node_id`
3. if record exists, read and decompress payload blob
4. JSON-parse the array and return entry at `ordinal`

---

## Future compatibility requirements

These requirements are intentionally forward-looking. They do not retroactively change the current v1 sidecar files, but they should guide the next compatible format revision.

### F1. Parent dataset identity

Each sidecar artifact should carry `parent_dataset_uuid`, which identifies the render octree dataset whose star ordering the sidecar matches.

This UUID is the primary compatibility check. Matching world geometry is useful, but it is not a substitute for explicit dataset identity.

### F2. Sidecar identity

Each sidecar artifact should also carry its own `sidecar_uuid`.

`sidecar_uuid` identifies one concrete build/version of a sidecar for a particular parent dataset. It exists so caches can distinguish:

- two versions of the same sidecar family for one render octree
- different sidecar families built for the same render octree

### F3. Named multi-sidecar model

The current `meta` sidecar should be treated as the first concrete sidecar family, not the only one.

Future builds should support multiple named sidecars such as:

- `identifiers`
- `exoplanets`
- `stellarParameters`
- `curatedRoutes`

### F4. Cache identity

The recommended cache identity for a sidecar is:

- `parent_dataset_uuid`
- sidecar kind or manifest key
- `sidecar_uuid`

### F5. Foundational build input

Future sidecar-family builds should use the Stage 2 `identifiers/order` artifact as their primary canonical build input.

That artifact should preserve:

- `parent_dataset_uuid`
- per-cell key `(level, node_id)`
- canonical ordered star identities within each cell

### F6. Manifest generalization

A future manifest revision should replace the single `meta_index_path` / `meta_payload_path` pair with named sidecar descriptors that include at least:

- sidecar kind
- index path
- payload path
- `parent_dataset_uuid`
- `sidecar_uuid`

### F7. Consumer validation

Consumers should reject a sidecar before use when its `parent_dataset_uuid` does not match the active render octree dataset UUID.

### F8. Recommended stage model

The recommended long-term stage model is:

- Stage 00: row enrichment parquet
- Stage 01: render intermediates and manifest
- Stage 02: final render octree plus foundational `identifiers/order` artifact
- Stage 03: optional sidecar-family builds and derived indices targeting an existing render dataset UUID

This keeps sidecars explicitly optional and makes it possible to rebuild them without rebuilding the render octree.

### F9. Rebuild policy

Sidecars should be treated as immutable derived artifacts.

When new identifiers or other enrichment data become available, the preferred process is:

1. keep the existing render octree unchanged
2. rebuild the affected sidecar family from the Stage 2 `identifiers/order` artifact plus canonical enrichment inputs for the same `parent_dataset_uuid`
3. emit a new `sidecar_uuid`
4. publish the new sidecar descriptor without mutating the old sidecar in place

### F10. Merge policy for existing sidecar families

Adding data to an existing sidecar family should happen at build time from source-of-truth tables keyed by canonical star identity such as `(source, source_id)`.

Recommended process:

1. define the sidecar family schema
2. load the upstream enrichment sources for that family
3. merge upstream rows by canonical star identity before encoding
4. resolve field collisions by explicit family-level rules, not silent last-write-wins
5. write a complete new sidecar artifact

For the current `meta` family, that means rebuilding `meta` with a new identifiers map or expanded enrichment tables, not patching old `.meta.octree` files in place.

### F11. Adding new sidecar families

When adding a new sidecar family:

1. choose a stable family name such as `exoplanets`
2. define its payload schema and source tables
3. define its canonical join key against render stars
4. define any merge/precedence rules across upstream sources
5. emit a descriptor with sidecar kind, `parent_dataset_uuid`, and `sidecar_uuid`

### F12. Relationship to reverse lookup

Reverse lookup indices such as:

- `canonical_star_id -> (level, node_id, ordinal)`

should be derived artifacts built from the Stage 2 `identifiers/order` artifact rather than baked into every sidecar-family specification.

---

## Non-normative implementation notes

- Reverse indices are derived artifacts and should be generated as post-build jobs when needed.
- gzip compression handles repeated JSON key names (`source`, `source_id`, and any enrichment keys) well across entries within a cell.
- The foundational Stage 2 `identifiers/order` artifact should use a compact binary layout rather than per-cell JSON because its main job is to make later sidecar-family builds cheap.
