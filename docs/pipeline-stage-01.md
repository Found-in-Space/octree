# Pipeline stage 01: Build intermediates

## Prerequisites

Input parquet for this stage is expected to be prepared by **pipeline stage 00** (see [pipeline-stage-00.md](pipeline-stage-00.md)): ICRS coordinates, `morton_code` derived from the same world geometry as the octree, optional precomputed `render` / `level`, and shard-local sorting suitable for streaming cell flush.

## Purpose

Stage 01 builds the **intermediate on-disk representation** used by the octree combine stage. It consumes a stream of stars that is **already sorted for the target cell order** and writes a set of append-only shard files:

* a binary **payload file** containing one compressed payload blob per cell
* a binary **index file** containing one fixed-size record per cell
* a top-level **manifest** describing all generated shard files and the binary layout

Stage 01 is a **bounded-memory writer**. It must never materialize the full dataset, the full set of cells, or any cross-level/global lookup structures in RAM. The only permitted in-memory state is:

* the current input batch
* the current cell accumulator
* a bounded set of open writers / file handles
* small codec scratch buffers

The output of Stage 01 is designed so that Pipeline 2 can later:

* traverse cells in DFS order
* relocate payloads directly into the final `stars.octree`
* perform existence and payload lookups using disk-backed fixed-record search
* avoid global in-memory concatenation

---

## Non-goals

Stage 01 does **not**:

* build the final `stars.octree`
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

`(node_id, mag_abs[, stable_tiebreak])`

Stage 01 must not add any secondary re-sort unless explicitly required by the payload format contract.

### 6. Shard-local order

Within each shard, index records must be strictly sorted by ascending `node_id`.

### 7. Shard ownership is total and unique

Each emitted cell belongs to exactly one shard. No duplication across shard files is allowed.

---

## Terminology

### Star row

A single input row representing one star, either in raw coordinate form or as a precomputed render payload fragment.

### Cell

The set of stars that share the same `(level, node_id)`.

### Shard

A file pair `(payload, index)` containing all cells for one `(level, prefix_bits, prefix)` partition.

### Prefix sharding

For deeper levels, cells may be partitioned by the leading Morton bits of `node_id`. Octant sharding is the special case `prefix_bits = 3`.

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
    use_precomputed_render: bool


def build_intermediates(
    parquet_glob: str,
    out_dir: Path,
    *,
    plan: BuildPlan,
) -> Path:
    """Build intermediate shard files and return the path to manifest.json."""
```

### Function contract

`build_intermediates(...)` must:

1. create all necessary shard payload/index files under `out_dir`
2. write all cell payload blobs and matching index records
3. write a complete `manifest.json`
4. return the path to that manifest

It must fail atomically at shard-file granularity: partial output for a shard is permitted during execution, but the final manifest must only describe successfully completed files.

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

Every row must provide enough information to derive:

* target `level`
* target `node_id`
* `mag_abs`
* payload encoding inputs

Depending on mode, those payload inputs are one of:

#### Raw-coordinate mode

* `x_icrs_pc`
* `y_icrs_pc`
* `z_icrs_pc`
* `teff` or equivalent defaultable value
* `mag_abs`

#### Precomputed-render mode

* `render`
* `mag_abs`
* `node_id`

### Input ordering

For each logical shard stream, rows must arrive ordered by:

`node_id, mag_abs[, stable_tiebreak]`

This ordering is part of the input contract. If the upstream query cannot guarantee it, it must be fixed upstream before rows reach the cell encoder.

### Determinism requirement

If exact deterministic ordering is required for stars that share the same `node_id` and identical `mag_abs`, the upstream source must define a stable tiebreak key. Stage 01 must not invent one implicitly.

---

## Sharding model

### Shallow levels

Levels below `deep_shard_from_level` use a single shard:

* `prefix_bits = 0`
* `prefix = 0`

### Deep levels

Levels at or above `deep_shard_from_level` use prefix-based sharding with:

* `prefix_bits = deep_prefix_bits`
* `prefix = 0 .. (2^prefix_bits - 1)`

For octant sharding, `deep_prefix_bits = 3` and `prefix` is the top octant.

### Membership rule

A cell with `(level, node_id)` belongs to shard `(level, prefix_bits, prefix)` iff:

* `prefix_bits == 0` and `prefix == 0`, or
* `node_id >> (3 * level - prefix_bits) == prefix`

This rule must be applied consistently by both the build and combine stages.

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
* all shard files that were successfully written
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

The manifest must only list completed shard files.

---

## Writer API

Stage 01 should implement a shard writer abstraction.

```python
class IntermediateShardWriter:
    def __init__(self, shard: ShardKey, out_dir: Path):
        ...

    def write_cell(self, cell: EncodedCell) -> None:
        ...

    def close(self) -> dict:
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
* return a manifest fragment describing the completed shard

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

The payload encoder is outside the scope of the intermediate file contract, but Stage 01 must enforce these requirements:

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

The manifest should be written only after all shard writers have completed successfully.

### Restart behavior

A restart strategy may choose to:

* delete incomplete files and rebuild them, or
* validate existing complete shard files and skip them

Any skip/reuse logic must validate file headers and record counts against the manifest or local shard metadata.

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

Index files must remain suitable for memory-mapped or binary-search access by Pipeline 2.

---

## DuckDB-facing requirements

DuckDB may be used as the row source, but the surrounding API must not depend on DuckDB-specific behavior beyond receiving an ordered stream.

### Core decision

Stage 01 should **preserve the existing two input modes** unless there is a deliberate later decision to remove one:

1. **Raw-coordinate mode**

   * input provides physical star fields such as coordinates, temperature, and magnitude
   * Stage 01 derives cell-local payload bytes itself

2. **Precomputed-render mode**

   * input already provides render-ready per-star bytes plus the target level
   * Stage 01 groups those rows into cells and writes the compressed cell payloads without changing star order

The specification for Stage 01 defines and requires support for these two modes.

### Query-level invariants that must be preserved

The implementation must satisfy the following query semantics.

#### 1. One query stream per target level

Stage 01 operates level-by-level. The row source for a given level must only produce stars for that level’s target cells.

#### 2. Stable cell grouping order

Rows must arrive grouped by target cell so that Stage 01 can flush a cell when `node_id` changes.

This means the query layer must guarantee ordered rows by:

`node_id, mag_abs[, stable_tiebreak]`

or an equivalent order that preserves the same grouping and same within-cell order contract.

#### 3. No downstream reorder unless explicitly required by the payload contract

Stage 01 must not silently reorder stars inside a cell. The query order is therefore part of the Stage 01 contract.

#### 4. Input ordering is part of the spec, not an implementation accident

If the data source is a set of parquet files, the implementation must not assume that physical parquet order alone is sufficient unless that guarantee is made explicit by the upstream dataset contract. If required, the DuckDB query layer must impose the order.

### Existing query modes to preserve

## A. Raw-coordinate mode

This mode corresponds to the existing behavior where the query produces:

* target `node_id`
* `x_icrs_pc`
* `y_icrs_pc`
* `z_icrs_pc`
* `mag_abs`
* `teff` with defaulting behavior if missing

### Raw-coordinate query semantics

For level `L`, the row source must:

1. compute `node_id` as the Morton prefix for the level
2. filter rows into the level’s magnitude band
3. exclude rows with null magnitude
4. default `teff` if absent/null
5. return rows ordered for cell streaming

### Preserved logical query shape

The required logical query shape is:

```sql
SELECT
    morton_code >> :shift AS node_id,
    x_icrs_pc,
    y_icrs_pc,
    z_icrs_pc,
    mag_abs,
    COALESCE(teff, 5800) AS teff
FROM read_parquet(:parquet_glob)
WHERE <level magnitude filter>
  AND mag_abs IS NOT NULL
ORDER BY node_id, mag_abs
```

### Raw-coordinate mode notes

* `shift = 3 * (MAX_MORTON_LEVEL - level)`
* the exact magnitude filter must preserve current level-band semantics
* Stage 01 must preserve the existing decision that rows are grouped by `node_id` and ordered by `mag_abs`
* if deterministic tiebreaking beyond `mag_abs` is required, that tiebreak must be defined explicitly rather than improvised later

## B. Precomputed-render mode

This mode corresponds to the existing behavior where parquet already contains:

* `render`
* `level`
* `morton_code`
* `mag_abs`

and Stage 01 does not recompute per-star render bytes.

### Mode detection

If both `render` and `level` columns are present, the implementation must support precomputed-render mode unless explicitly disabled by configuration.

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
    mag_abs,
    morton_code >> :shift AS node_id
FROM read_parquet(:parquet_glob)
WHERE level = :level
  AND mag_abs IS NOT NULL
ORDER BY node_id, mag_abs
```

### Important note on prior confusion

The required ordering contract is `node_id, mag_abs` (optionally followed by a stable tiebreaker if explicitly defined).

For Stage 01, the preserved contract is:

* input is already in the required order
* that order is `node_id, mag_abs` unless an explicit stable tiebreak is added
* Stage 01 does not add an extra reorder step

### Deep-level prefix / octant sharding

If a level is physically split into prefix shards, the query layer may either:

1. run one query per shard with a prefix filter, or
2. run a single query whose results are already known to be partitioned for shard-local writers

For prefix-sharded levels, the preferred explicit query form is to add a shard-membership filter. For octant sharding, that is conceptually:

```sql
AND (morton_code >> :top_shift) = :prefix
```

where `:top_shift` selects the leading Morton bits needed for the chosen shard prefix.

The exact numeric shift must match the shard-membership rule used by the file layout.

### Query-level requirements for prefix-sharded levels

For each `(level, prefix_bits, prefix)` shard, the row source must ensure:

* only rows belonging to that shard are produced
* rows remain ordered by `node_id, mag_abs[, stable_tiebreak]`
* no rows spill into adjacent shards

### Magnitude-band semantics

The magnitude-band logic used to assign stars to levels is part of the Stage 01 contract and must be implemented exactly as defined by the level configuration.

That includes:

* inclusive/exclusive bounds exactly as currently defined
* behavior for unbounded top or bottom ranges
* exclusion of null magnitude rows

These rules must be implemented exactly; approximation is not permitted.

### Documentation requirement

An implementation of Stage 01 should document, in code and tests:

* the exact SQL shape for raw-coordinate mode
* the exact SQL shape for precomputed-render mode
* the level-band filter semantics
* the shard prefix filter semantics
* the exact ordering contract delivered to the cell encoder

### Conformance tests for the row source

The row source layer should have explicit tests that verify:

1. rows for a level are grouped by `node_id`
2. within a node, rows are in ascending `mag_abs`
3. prefix-sharded queries contain only rows for that shard
4. raw-coordinate and precomputed-render modes both produce equivalent cell grouping behavior
5. null `mag_abs` rows are excluded
6. `teff` defaulting matches the current implementation contract

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
                    use_precomputed_render=plan.use_precomputed_render,
                )

                encoder = make_cell_encoder(level=level)
                for cell in iter_encoded_cells(rows, encoder):
                    writer.write_cell(cell)

                manifest_entries.append(writer.close())
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

1. one payload file per shard
2. one fixed-record index file per shard
3. one manifest describing all completed shards

The outputs must satisfy all of the following:

* deterministic
* append-only generated
* bounded-memory produced
* shard-local sorted by `node_id`
* directly consumable by a bounded-memory combine stage

---

## Acceptance criteria

Stage 01 is complete when all of the following are true:

* a full dataset can be processed without materializing the full index or cell set in memory
* every cell is written exactly once into exactly one shard
* every written payload blob has exactly one matching index record
* every index file is fixed-record and sorted by `node_id`
* the manifest fully describes the intermediate directory
* Pipeline 2 can reopen the intermediates using only the manifest and the binary file layouts defined here
* the implementation uses only bounded caches and current-cell state in memory
