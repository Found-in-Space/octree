# Sidecars specification

## Purpose

Define optional and required sidecar artifacts that are written alongside Stage 01 intermediate octree shards.

The sidecars provide per-star identity and metadata without changing the main render payload format.

## Scope

This specification covers:

- where sidecars are built in the current pipeline
- required ordering and identity invariants
- binary and JSON payload contracts for sidecar files
- consumer lookup behavior

This specification does not cover:

- reverse/global indices (`source_id -> octree location`)
- UI/client search ranking behavior
- post-build derived artifacts

---

## Stage placement

Sidecars are built in **Stage 01**, in the same streaming pass that writes main intermediate shards.

### Rationale

- The per-star ordinal is defined by Stage 01 cell grouping and row order.
- Sidecar rows must match render rows exactly within each cell.
- A separate pass would require re-running the query and reproducing identical ordering.

---

## Normative requirements

### R1. Same cell identity

For each sidecar type, each payload entry corresponds to exactly one cell identified by:

- `level`
- `node_id`

where `node_id` is the level-specific Morton prefix used by Stage 01.

### R2. Same star order

Within a cell, sidecar star order must match main payload star order exactly.

The Stage 01 row stream must use:

`ORDER BY node_id, mag_abs, stable_tiebreak`

where `stable_tiebreak` is deterministic and unique per row (for example `source_id`).

### R3. Append-only writes

Sidecar index and payload files are append-only during build.

### R4. Bounded memory

The implementation must remain streaming and bounded-memory, consistent with Stage 01 invariants.

### R5. Independent optionality

Each sidecar type is optional unless explicitly marked required by the build profile.
When absent, consumers must continue to function for core rendering.

---

## Sidecar types

## 1) Source ID sidecar (required)

The source ID sidecar enables forward lookup:

`(level, node_id, ordinal) -> source_id`

### Files

Per shard:

- `.meta-index`
- `.meta-payload`

### Index format

Use the same index header and record layout as Stage 01 index files:

- `INDEX_FILE_HDR = struct.Struct("<4sHHBBHIQQ")`
- `INDEX_RECORD = struct.Struct("<QQII")`

Semantics:

- `node_id` identifies the cell
- `payload_offset`/`payload_length` locate the sidecar cell payload blob
- `star_count` equals the number of stars in this cell

### Payload format

Each blob is gzip-compressed binary for one cell containing a dense array of `int64` source IDs in ordinal order.

Constraints:

- array length must equal `star_count`
- no missing entries are allowed
- source IDs must be canonical identifiers selected by the merge stage

---

## 2) Names sidecar (optional)

The names sidecar stores sparse human-facing identifiers (for example HIP/HD/Bayer) keyed by cell ordinal.

### Files

Per shard:

- `.names-index`
- `.names-payload`

### Index format

Use the same index layout as above.

Cells with no names metadata may be omitted entirely from `.names-index`.
A missing index record for a valid `node_id` means "no names metadata for this cell."

### Payload format

Each blob is gzip-compressed JSON and may use either representation:

- sparse object: `{"10": {"hip_id": 71683, "bayer": "alf cen"}}`
- dense list: `[null, {"hd": "HD 48915"}, ...]`

Encoding rule (per cell):

- 0 populated entries: omit cell from names sidecar
- population ratio `< DENSE_THRESHOLD`: use sparse object
- population ratio `>= DENSE_THRESHOLD`: use dense list

Default:

- `DENSE_THRESHOLD = 0.50`

Null policy:

- dense lists use `null` for absent metadata
- sparse objects do not emit absent ordinals

---

## Manifest requirements

When a sidecar is present, each manifest shard entry must include explicit paths for the sidecar pair.

Minimum fields:

- `meta_index_path` / `meta_payload_path` for Source ID sidecar
- `names_index_path` / `names_payload_path` for Names sidecar (if enabled)

Manifest metadata should also include sidecar codec metadata (for example gzip and JSON/binary payload kind).

---

## Consumer lookup contract

Given `(level, node_id, ordinal)`:

1. locate shard for `(level, node_id)`
2. binary-search sidecar index by `node_id`
3. if record exists, read and decompress payload blob
4. decode payload and return entry at `ordinal`

Names sidecar behavior:

- dense list: `entry = arr[ordinal]`
- sparse object: `entry = obj[str(ordinal)]` or missing

---

## Non-normative implementation notes

- Keeping Source ID and Names in separate sidecars preserves compact binary encoding for dense IDs and sparse JSON for optional names.
- Reverse indices are derived artifacts and should be generated as post-build jobs when needed.
