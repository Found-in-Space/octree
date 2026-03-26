# Sidecars specification

## Purpose

Define the sidecar artifact written alongside Stage 01 intermediate octree shards.

The sidecar provides per-star identity and metadata without changing the main render payload format.

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

The sidecar is built in **Stage 01**, in the same streaming pass that writes main intermediate shards.

### Rationale

- The per-star ordinal is defined by Stage 01 cell grouping and row order.
- Sidecar rows must match render rows exactly within each cell.
- A separate pass would require re-running the query and reproducing identical ordering.

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

---

## Consumer lookup contract

Given `(level, node_id, ordinal)`:

1. locate shard for `(level, node_id)`
2. binary-search sidecar index by `node_id`
3. if record exists, read and decompress payload blob
4. JSON-parse the array and return entry at `ordinal`

---

## Non-normative implementation notes

- Reverse indices are derived artifacts and should be generated as post-build jobs when needed.
- gzip compression handles repeated JSON key names (`source`, `source_id`, and any enrichment keys) well across entries within a cell.
