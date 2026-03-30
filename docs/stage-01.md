# Stage 01: Build intermediates

## Prerequisites

Input parquet for this stage must be prepared by **Stage 00** (see [stage-00.md](stage-00.md)). Stage 00 enriches each file with `morton_code`, precomputed `render` bytes, and `level`, and writes locally sorted parquet. Stage 01 requires all four columns (`morton_code`, `render`, `level`, `mag_abs`) to be present.

## Purpose

Stage 01 builds the **intermediate on-disk representation** used by the later packaging stages. It consumes a stream of stars that is **already sorted for the target cell order** and writes two parallel intermediate families:

* render shard files used to assemble `stars.octree`
* identifiers-order shard files used to assemble `identifiers.order`

For each family, Stage 01 writes:

* a binary **payload file** containing one compressed payload blob per cell
* a binary **index file** containing one fixed-size record per cell

It also publishes two authoritative manifests:

* `render-manifest.json`
* `identifiers-manifest.json`

Stage 01 is a **bounded-memory writer**. It must never materialize the full dataset, the full set of cells, or any cross-level/global lookup structures in RAM. The only permitted in-memory state is:

* the current input batch
* the current cell accumulator
* a bounded set of open writers / file handles
* small codec scratch buffers

The output of Stage 01 is designed so that Stage 02 can later:

* traverse render cells in DFS order
* relocate render payloads directly into the final `stars.octree`
* combine canonical ordered identities into the final `identifiers.order`
* avoid global in-memory concatenation

---

## Non-goals

Stage 01 does **not**:

* build the final `stars.octree`
* build the final `identifiers.order`
* build any Stage 03 sidecar family
* compute final payload offsets in the output octree
* build shard headers for the final octree index
* materialize ancestors or child masks globally
* keep any global table of all cells in memory

---

## Architectural invariants

### 1. Bounded memory

At no point may Stage 01 store all stars, all cells, all offsets, or all node keys in memory. The implementation must remain streaming in structure.

### 2. Append-only writes

Each intermediate payload file and index file is append-only for the duration of the build.

### 3. One index record per cell

Each cell written to a payload file must have exactly one corresponding index record.

### 4. Stable cell identity

The stable identity of a cell is:

* `level`
* `node_id`

where `node_id` is the Morton prefix for that level.

### 5. Ordered input contract

Rows arriving at the cell encoder for a given shard must already be ordered by:

`(node_id, mag_abs, source_id)`

Stage 01 must not add any secondary re-sort unless explicitly required by the payload format contract.

### 6. Shard-local order

Within each shard, index records must be strictly sorted by ascending `node_id`.

### 7. Shard ownership is total and unique

Each emitted cell belongs to exactly one shard. No duplication across shard files is allowed.

---

## Terminology

### Star row

A single input row representing one star, with precomputed render bytes from Stage 00.

### Cell

The set of stars that share the same `(level, node_id)`.

### Shard

A file pair `(payload, index)` containing all cells for one `(level, prefix_bits, prefix)` partition.

### Prefix sharding

A query and file-partitioning strategy for deep levels. Cells at a level are distributed across multiple shard files by the leading Morton bits of `node_id`. This bounds query working sets and intermediate file sizes without fragmenting node payloads. Octant sharding is the special case `prefix_bits = 3`.

### Payload blob

The compressed binary representation of a single cell.

---

## Public internal API

```python
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True, slots=True)
class BuildPlan:
    max_level: int
    deep_shard_from_level: int
    deep_prefix_bits: int
    batch_size: int


def build_intermediates(
    parquet_glob: str,
    out_dir: Path,
    *,
    plan: BuildPlan,
) -> Path:
    """Build intermediate shard files and return the path to render-manifest.json."""
```

### Function contract

`build_intermediates(...)` must:

1. create shard payload/index files under `out_dir` for shards that contain at least one cell
2. write render cell payload blobs and matching index records
3. write identifiers-order payload blobs and matching index records
4. write complete `render-manifest.json` and `identifiers-manifest.json`
5. return the path to `render-manifest.json`

It must fail atomically at shard-file granularity: partial output for a shard is permitted during execution, but the final manifest must only describe successfully completed non-empty shard files.

---

## Core types

```python
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class CellKey:
    level: int
    node_id: int

@dataclass(frozen=True, slots=True)
class ShardKey:
    level: int
    prefix_bits: int
    prefix: int

@dataclass(frozen=True, slots=True)
class EncodedCell:
    key: CellKey
    payload: bytes
    star_count: int
```

### Notes

* `CellKey` is the canonical cell identity used throughout both pipelines.
* `ShardKey` is the canonical intermediate file partition identity.
* `EncodedCell.payload` is already compressed and ready to append to the shard payload file.

---

## Input contract

Stage 01 consumes data from a row source that produces rows grouped and ordered for streaming cell flush.

### Required logical fields

Stage 01 consumes the Stage 00 output contract and expects precomputed render rows.
Every row must provide:

* `level`
* `morton_code`
* `mag_abs`
* `render` (per-star precomputed bytes)

From those fields Stage 01 derives:

* target `node_id` via `morton_code >> shift`
* `mag_abs`
* payload encoding inputs

Raw-coordinate mode is out of scope for Stage 01 in this version.

### Input ordering

For each logical shard stream, rows must arrive ordered by:

`node_id, mag_abs, source_id`

This ordering is part of the input contract. If the upstream query cannot guarantee it, it must be fixed upstream before rows reach the cell encoder.

### Determinism requirement

Stars that share the same `node_id` and identical `mag_abs` are ordered by `source_id` (string), matching `docs/sidecars.md` R2 for metadata sidecar alignment.

### Input uniqueness assumption

Each star row appears in exactly one Stage 00 shard input. Stage 01 must not duplicate rows across shard streams.

---

## Sharding model

Prefix sharding is a query and file-layout optimization. It does not fragment node payloads; each `(level, node_id)` maps to exactly one shard and is written as a single atomic record+blob.

### Shallow levels

Levels below `deep_shard_from_level` use a single shard:

* `prefix_bits = 0`
* `prefix = 0`

### Deep levels

Levels at or above `deep_shard_from_level` use prefix-based sharding with:

* `prefix_bits = deep_prefix_bits`
* `prefix = 0 .. (2^prefix_bits - 1)`

The default is octant sharding: `deep_prefix_bits = 3`, giving 8 shards per deep level.

### Membership rule

A cell with `(level, node_id)` belongs to shard `(level, prefix_bits, prefix)` iff:

* `prefix_bits == 0` and `prefix == 0`, or
* `node_id >> (3 * level - prefix_bits) == prefix`

This rule must be applied consistently by both the build and combine stages.

### Parameter guardrails

The shard plan must satisfy:

* `deep_shard_from_level >= 0`
* `deep_prefix_bits >= 0`
* for any sharded level `L`, `deep_prefix_bits <= 3 * L`
* for every shard key, `0 <= prefix < 2^prefix_bits`

At level 0, only `prefix_bits = 0` is valid.

These constraints must be validated once at startup. Violations must cause an immediate, clear error — no implicit clamping or fallback.

### Shard naming

#### Unsharded level

* `level-00.index`
* `level-00.payload`

#### Prefix-sharded level

* `level-10-p3-0.index`
* `level-10-p3-0.payload`
* ...
* `level-10-p3-7.index`
* `level-10-p3-7.payload`

Naming is a filesystem convention only. The manifest remains authoritative.

---

## Intermediate file formats

## 1. Payload file

A payload file is an append-only sequence of compressed cell payload blobs.

There is no internal table of contents. All lookup goes through the matching index file.

### Payload file invariants

* blobs are written in the same order as their corresponding index records
* each blob is a complete compressed payload for one cell
* the payload codec is declared in the manifest
* no padding or framing is required between blobs

---

## 2. Index file

An index file contains a fixed-size header followed by one fixed-size record per cell.

### Index file header struct

```python
INDEX_FILE_HDR = struct.Struct("<4sHHBBHIQQ")
```

### Header fields

* `magic: 4s` = `b"OIDX"`
* `version: u16` = `1`
* `header_size: u16` = `32`
* `level: u8`
* `prefix_bits: u8`
* `flags: u16`
* `record_size: u32`
* `prefix: u64`
* `record_count: u64`

### Header flag bits

* bit 0: payload blobs use gzip
* bit 1: records are sorted by ascending `node_id`
* all other bits reserved and must be zero

### Index record struct

```python
INDEX_RECORD = struct.Struct("<QQII")
```

### Record fields

* `node_id: u64`
* `payload_offset: u64`
* `payload_length: u32`
* `star_count: u32`

### Index file invariants

* records are strictly sorted by ascending `node_id`
* `node_id` values are unique within a file
* `payload_offset` points into the paired payload file
* `payload_length > 0`
* `star_count > 0`

No `gx`, `gy`, or `gz` are stored in the intermediate index. Those may be derived later from `(level, node_id)` if required.

---

## 3. Manifest file

The top-level manifest is JSON and is the authoritative description of the intermediate directory.

### Manifest responsibilities

The manifest must describe:

* format version
* world geometry constants needed by the payload encoder
* maximum level
* payload codec
* exact binary layouts in use
* all non-empty shard files that were successfully written
* per-shard record counts

### Example structure

```json
{
  "format": "three_dee.octree.intermediates/v1",
  "world_center": [0.0, 0.0, 0.0],
  "world_half_size_pc": 200000.0,
  "max_level": 21,
  "payload_codec": "gzip",
  "index_header_struct": "<4sHHBBHIQQ",
  "index_record_struct": "<QQII",
  "levels": [
    {
      "level": 0,
      "shards": [
        {
          "prefix_bits": 0,
          "prefix": 0,
          "index_path": "level-00.index",
          "payload_path": "level-00.payload",
          "record_count": 1
        }
      ]
    },
    {
      "level": 10,
      "shards": [
        {
          "prefix_bits": 3,
          "prefix": 0,
          "index_path": "level-10-p3-0.index",
          "payload_path": "level-10-p3-0.payload",
          "record_count": 123456
        }
      ]
    }
  ]
}
```

The manifest must only list completed non-empty shard files.

### Companion identifiers manifest

Stage 01 now publishes two manifests in the same output directory:

- `render-manifest.json`
- `identifiers-manifest.json`

Each manifest describes exactly one artifact family and its shard files. Stage 01 does not publish sidecar descriptors.

---

## Writer API

Stage 01 should implement a shard writer abstraction.

```python
class IntermediateShardWriter:
    def __init__(self, shard: ShardKey, out_dir: Path):
        ...

    def write_cell(self, cell: EncodedCell) -> None:
        ...

    def close(self) -> dict | None:
        ...
```

### `write_cell()` contract

For each cell, `write_cell()` must:

1. assert that `cell.key.level == shard.level`
2. assert that `cell.key.node_id` belongs to this shard under the shard membership rule
3. assert monotonic increase of `node_id` relative to the previous record
4. compute `payload_offset = payload_fp.tell()`
5. append `cell.payload` to the payload file
6. append one matching `INDEX_RECORD`
7. increment internal `record_count`

### `close()` contract

`close()` must:

* flush and close both files
* patch the index file header with the final `record_count`
* if `record_count == 0`, remove the shard files and return `None`
* otherwise return a manifest fragment describing the completed shard

---

## Row source and cell encoding

Stage 01 conceptually consists of three phases:

1. **row source**
2. **cell encoder**
3. **shard writer**

### Row source

The row source is responsible for producing shard-local sorted row streams. It may use DuckDB internally, but that is not part of the pipeline contract.

### Cell encoder

The cell encoder groups adjacent rows by `node_id` and emits one `EncodedCell` per cell.

```python
def iter_encoded_cells(rows: Iterator[StarRow], encoder: CellEncoder) -> Iterator[EncodedCell]:
    ...
```

### Cell encoder invariants

* only the current cell may be accumulated in memory
* flush occurs on `node_id` change or end-of-stream
* no global reordering is permitted
* the encoder must preserve the row order it receives unless the payload format explicitly requires an internal reorder

---

## Payload encoding requirements

Stage 01 receives precomputed per-star render bytes from Stage 00. It must group those rows into cells and emit one compressed payload blob per cell while preserving row order.

### 1. One encoded payload per cell

Each emitted payload blob corresponds to exactly one cell.

### 2. Self-contained decoding

A decoder that knows `(level, node_id)` and world geometry constants must be able to interpret the payload without consulting other cells.

### 3. Deterministic encoding

For the same ordered star stream into the same cell, the encoder must emit identical payload bytes.

### 4. No cross-cell state

The payload encoder must not depend on previous or future cells.

---

## Execution model

Stage 01 should iterate levels in ascending order and within each level iterate shards in ascending `prefix` order.

Recommended execution order:

1. derive shard plan for the level
2. open a writer for one shard
3. stream rows for that shard
4. emit encoded cells directly to the writer
5. close the writer
6. move to the next shard
7. after all shards complete, write the manifest

This keeps open-file counts bounded and avoids concurrent writer complexity.

Parallelization is allowed only if it preserves all invariants and does not cause unbounded file-handle or memory growth.

---

## Failure handling

### Writer crash / interruption

If the process terminates mid-shard, the partially written shard files may remain on disk but must not be listed in the manifest.

### Manifest publication

The paired manifests must be written only after all shard writers have completed successfully. Each manifest must be published atomically: write to a temporary file then rename into place.

### Restart behavior

Stage 01 supports paired-manifest resume semantics. Existing completed shards may be reused only when both manifests are present and validate against the current build plan. A half-present manifest pair must fail fast.

---

## Validation rules

Each completed shard file pair should be validated before manifest publication.

### Required validation

* payload file exists
* index file exists
* index header magic/version/layout are correct
* `record_count` in header matches actual record count in file
* index record count is non-negative and integral
* record size matches expected struct
* node IDs are strictly ascending
* every `payload_offset + payload_length` lies within the payload file size

### Optional validation

* decode a sample of payloads
* verify `star_count` consistency with source counts
* verify shard membership for all records

---

## Performance requirements

### 1. Streaming first

The implementation must favor sequential reads and writes.

### 2. No global sort inside Stage 01

If sorting is needed, it must occur upstream in the row source. Stage 01 itself is not a sorting stage.

### 3. Constant-memory cell flush

The memory footprint should scale with the largest single cell, not with the number of cells or stars overall.

### 4. Fixed-record index files

Index files must remain suitable for memory-mapped or binary-search access by Stage 02.

---

## DuckDB-facing requirements

DuckDB may be used as the row source, but the surrounding API must not depend on DuckDB-specific behavior beyond receiving an ordered stream.

### Core decision

Stage 01 operates in precomputed-render mode only for this version.
Input rows are produced by Stage 00 and include `render`, `level`, `morton_code`, and `mag_abs`.

### Query-level invariants that must be preserved

The implementation must satisfy the following query semantics.

#### 1. One query stream per target level

Stage 01 operates level-by-level. The row source for a given level must only produce stars for that level’s target cells.

#### 2. Stable cell grouping order

Rows must arrive grouped by target cell so that Stage 01 can flush a cell when `node_id` changes.

This means the query layer must guarantee ordered rows by:

`node_id, mag_abs, source_id`

or an equivalent order that preserves the same grouping and same within-cell order contract.

#### 3. No downstream reorder unless explicitly required by the payload contract

Stage 01 must not silently reorder stars inside a cell. The query order is therefore part of the Stage 01 contract.

#### 4. Input ordering is part of the spec, not an implementation accident

If the data source is a set of parquet files, the implementation must not assume that physical parquet order alone is sufficient unless that guarantee is made explicit by the upstream dataset contract. If required, the DuckDB query layer must impose the order.

### Required row shape

Input parquet is expected to contain:

* `render`
* `level`
* `morton_code`
* `mag_abs`
* `source`
* `source_id`

Stage 01 does not recompute per-star render bytes. Identity columns are required for R2 ordering and optional metadata sidecars (`docs/sidecars.md`).

### Precomputed-render query semantics

For level `L`, the row source must:

1. select only rows where `level = L`
2. derive `node_id` from `morton_code >> shift`
3. exclude rows with null magnitude
4. preserve the intended grouping/order contract for streaming cell flush

### Preserved logical query shape

The required logical query shape is:

```sql
SELECT
    render,
    source,
    source_id,
    mag_abs,
    morton_code >> :shift AS node_id
FROM read_parquet(:parquet_glob)
WHERE level = :level
  AND mag_abs IS NOT NULL
ORDER BY node_id, mag_abs, source_id
```

### Ordering contract

The required ordering is `node_id, mag_abs, source_id`. The `source_id` tiebreak matches `docs/sidecars.md` R2 so render and metadata sidecar rows stay in lockstep when `mag_abs` ties within a cell. Stage 01 does not add an extra reorder step; the DuckDB query layer must deliver rows in this order.

### Deep-level prefix / octant sharding

For prefix-sharded levels, the row source must run one query per `(level, prefix)` with an explicit prefix filter. For octant sharding, that is conceptually:

```sql
AND (morton_code >> :top_shift) = :prefix
```

where `:top_shift` selects the leading Morton bits needed for the chosen shard prefix.

The exact numeric shift must match the shard-membership rule used by the file layout.

### Query-level requirements for prefix-sharded levels

For each `(level, prefix_bits, prefix)` shard, the row source must ensure:

* only rows belonging to that shard are produced
* rows remain ordered by `node_id, mag_abs, source_id`
* no rows spill into adjacent shards

### Magnitude-band semantics

Level semantics are defined by `MagLevelConfig` (see `src/foundinspace/octree/mag_levels.py`) and materialized by Stage 00 in the `level` column. Stage 01 must use `WHERE level = :level` as the correctness contract for level selection. Null `mag_abs` rows must be excluded.

Stage 00 may use any file layout strategy (including no band split). Stage 01 correctness is defined by `WHERE level = :level`, independent of Stage 00 directory naming.

### Documentation requirement

An implementation of Stage 01 should document, in code and tests:

* the exact SQL shape for precomputed-render mode
* the `WHERE level = :level` selection semantics
* the shard prefix filter semantics
* the exact ordering contract delivered to the cell encoder

### Conformance tests for the row source

The row source layer should have explicit tests that verify:

1. rows for a level are grouped by `node_id`
2. within a node, rows are in ascending `mag_abs`
3. prefix-sharded queries contain only rows for that shard
4. null `mag_abs` rows are excluded
5. Stage 01 input rows are unique (no duplicate star rows across source shards)

## Reference pseudocode

```python
def build_intermediates(parquet_glob: str, out_dir: Path, *, plan: BuildPlan) -> Path:
    manifest_entries = []

    for level in range(plan.max_level + 1):
        shard_keys = plan_level_shards(level, plan)

        for shard in shard_keys:
            writer = IntermediateShardWriter(shard, out_dir)
            try:
                rows = iter_sorted_rows(
                    parquet_glob,
                    level=level,
                    shard=shard,
                    batch_size=plan.batch_size,
                )

                encoder = make_cell_encoder(level=level)
                for cell in iter_encoded_cells(rows, encoder):
                    writer.write_cell(cell)

                shard_manifest = writer.close()
                if shard_manifest is not None:
                    manifest_entries.append(shard_manifest)
            except Exception:
                writer.abort_if_supported()
                raise

    manifest_path = write_manifest(out_dir, plan, manifest_entries)
    return manifest_path
```

This pseudocode is normative in structure even if helper names differ.

---

## Summary of required outputs

Stage 01 must produce:

1. one payload file per non-empty shard
2. one fixed-record index file per non-empty shard
3. one manifest describing all completed non-empty shards

The outputs must satisfy all of the following:

* deterministic
* append-only generated
* bounded-memory produced
* shard-local sorted by `node_id`
* directly consumable by a bounded-memory combine stage

---

## V1 defaults

* `deep_prefix_bits = 3` (octant sharding)
* generated project files seed `deep_shard_from_level = 99`
* generated project files seed `batch_size = 100_000`
* generated project files seed `max_level = 13`
* input mode: precomputed-render only
* restart policy: fail if `out_dir` is non-empty
* manifest publish: atomic (write temp, rename)
* ordering: `node_id, mag_abs, source_id` (stable tiebreak for sidecar alignment; see `docs/sidecars.md`)

---

## CLI contract

```
uv run fis-octree stage-01 --project path/to/project.toml
```

### Project-file inputs

Stage 01 reads all build-defining paths and parameters from the project file:

* `paths.stage01_output_dir`
* `stage01.input_glob`
* `stage01.batch_size`
* `stage01.deep_shard_from_level`
* `stage01.deep_prefix_bits`
* `stage00.v_mag`
* `stage00.max_level`

Project-file path rules:

* paths may be absolute
* relative paths are resolved from the project file directory
* environment-variable expansion is not supported in TOML values

Stage 01 writes:

* `render-manifest.json`
* `identifiers-manifest.json`

### Error behavior

* Fail immediately if `paths.stage01_output_dir` exists and is non-empty.
* Fail immediately if shard plan parameters are invalid (see parameter guardrails).
* Fail immediately if input parquet is missing required columns (`render`, `level`, `morton_code`, `mag_abs`, `source`, `source_id`).
* Fail immediately if only one of the paired manifests is present in an otherwise resumable output directory.

---

## Acceptance criteria

Stage 01 is complete when all of the following are true:

* a full dataset can be processed without materializing the full index or cell set in memory
* every cell is written exactly once into exactly one shard
* every written payload blob has exactly one matching index record
* every index file is fixed-record and sorted by `node_id`
* the manifest fully describes the intermediate directory
* Stage 02 can reopen the intermediates using only the manifest and the binary file layouts defined here
* the implementation uses only bounded caches and current-cell state in memory
