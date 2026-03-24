# Pipeline 2: `combine` — Detailed Specification

## Purpose

`combine` consumes a set of intermediate shard files and produces the final `stars.octree` file.

It must:

* write payloads into the final file in global DFS order
* never materialize the full cell set in memory
* never build global offset maps in memory
* use only bounded caches and bounded per-shard state
* use disk-backed fixed-record files for all large metadata
* preserve deterministic output

This pipeline is responsible only for assembling the final octree from already-built intermediates. It does **not** generate cell payloads from stars.

---

## High-level contract

### Inputs

* `manifest.json` describing all intermediate shards
* one `.index` file per intermediate shard
* one `.payload` file per intermediate shard

### Outputs

* final `stars.octree`
* temporary relocation files, deleted on success unless debug retention is enabled

### Invariants

* intermediate shard `.index` files are sorted by `node_id`
* each intermediate record identifies a unique cell `(level, node_id)`
* payload bytes are already encoded and compressed for the final file
* combine performs byte relocation and final index construction only
* final payload section order is global DFS order
* final shard index must be self-consistent and patchable without global RAM state

---

## Scope of responsibility

`combine` is split into three phases:

1. **Phase A — payload relocation**

   * traverse cells in global DFS order
   * copy payload bytes from intermediate `.payload` files to final output
   * write relocation records to disk

2. **Phase B — final shard-index construction**

   * construct final shard index from relocation files + intermediate existence lookups
   * write shard headers, node tables, and frontier references
   * patch deferred per-shard fields as needed

3. **Phase C — final header patching and cleanup**

   * patch global file header with final offsets and lengths
   * optionally remove temporary relocation files

No phase may depend on a fully materialized in-memory representation of all cells.

---

## Core data model

### Cell identity

The stable identity of a payload-bearing cell is:

* `level: int`
* `node_id: int`

`node_id` is the Morton prefix for that level.

This tuple is the canonical lookup key throughout `combine`.

### Shard identity

Each intermediate shard is identified by:

* `level`
* `prefix_bits`
* `prefix`

For shallow levels this may be unsharded (`prefix_bits = 0`, `prefix = 0`).
For deep levels this will normally be octant-sharded (`prefix_bits = 3`).

A shard contains exactly those records whose `node_id` matches its prefix constraint.

### Final node identity

All node-existence and payload lookups in `combine` are keyed by `(level, node_id)`. Grid coordinates (`gx`, `gy`, `gz`) are derived only when needed for final shard/node encoding.

---

## File contracts used by `combine`

### Intermediate index file

Each shard `.index` file is a fixed-record file containing records sorted by `node_id`.

Per-record fields:

* `node_id: u64`
* `payload_offset: u64`
* `payload_length: u32`
* `star_count: u32`

These files are treated as disk-backed searchable arrays.

### Intermediate payload file

Each shard `.payload` file contains concatenated payload blobs.

Lookup is always through the shard `.index` file.

### Relocation file

For each intermediate shard, `combine` writes a matching relocation file during Phase A.

Per-record fields:

* `node_id: u64`
* `output_offset: u64`
* `payload_length: u32`
* `star_count: u32`

Relocation files are sorted by `node_id` because Phase A appends relocation records in DFS order while staying monotonic inside each shard.

---

## Public internal entry point

```python
@dataclass(frozen=True, slots=True)
class CombinePlan:
    max_open_files: int = 32
    lookup_cache_records: int = 65536
    retain_relocation_files: bool = False


def combine_octree(
    manifest_path: Path,
    output_path: Path,
    *,
    plan: CombinePlan = CombinePlan(),
) -> None:
    ...
```

`combine_octree()` orchestrates all phases and owns cleanup semantics.

---

## Phase A — Payload relocation

## Goal

Write the final payload section in global DFS order without storing global offset maps in memory.

## Outputs

* payload section in final `stars.octree`
* one relocation file per input shard
* `payload_end_offset`

## Required behavior

For each payload-bearing cell in global DFS order:

1. locate the source shard record
2. seek to `payload_offset` in the source `.payload` file
3. copy exactly `payload_length` bytes to the final output file
4. record the final `output_offset`
5. append a relocation record to that shard’s relocation file

The final payload offset is defined as `output_fp.tell()` immediately before the write.

No payload bytes other than the current copy buffer may be retained.

## DFS ordering requirement

The payload pass must emit cells in **global DFS order**, not merely by level and not merely by per-shard local order.

DFS order here means preorder traversal of the logical octree over payload-bearing cells.

### Consequences

* payload order is deterministic
* final file content is deterministic
* later traversal code may assume payload locality aligned with DFS

## DFS iterator contract

```python
@dataclass(frozen=True, slots=True)
class CellPayloadRef:
    shard: ShardKey
    level: int
    node_id: int
    payload_offset: int
    payload_length: int
    star_count: int


def iter_cells_dfs(manifest_path: Path) -> Iterator[CellPayloadRef]:
    ...
```

`iter_cells_dfs()` is the heart of the combine design.

It must:

* traverse the logical tree in DFS order
* use per-level/per-shard sorted index files as on-disk lookup structures
* keep only bounded cursor state and bounded caches
* never allocate a global cell array

## Acceptable implementation strategies

### Preferred strategy

Use disk-backed per-shard sequential cursors plus DFS traversal over node prefixes.

For each node prefix visited in DFS order:

* determine whether a payload-bearing cell exists at `(level, node_id)`
* if yes, emit the cell
* descend to children in octant order

Existence checks must use fixed-record binary search or bounded shard-local cursors.

### Not acceptable

* concatenate all index arrays into one global array
* compute all DFS keys globally and sort them in memory
* build a full in-memory relocation map

## File-handle policy

Phase A may keep only a bounded number of shard files open at once.

When the number of open handles would exceed `max_open_files`, handles must be closed using LRU or equivalent policy.

Reopening a file is acceptable. A file-handle cache is a bounded cache and does not violate the memory constraint.

## Copy buffer policy

Payload copying should use a bounded byte buffer, for example 64 KiB to 1 MiB.

Large payloads must be copied in chunks.

## Phase A API

```python
@dataclass(frozen=True, slots=True)
class PayloadPassResult:
    payload_end_offset: int
    relocation_files: tuple[Path, ...]


def relocate_payloads_dfs(
    manifest_path: Path,
    output_fp: BinaryIO,
    *,
    plan: CombinePlan,
) -> PayloadPassResult:
    ...
```

---

## Phase B — Final shard-index construction

## Goal

Construct and append the final shard index after payload relocation has completed.

## Inputs

* manifest
* intermediate shard index files
* relocation files produced in Phase A
* final payload section end offset

## Outputs

* index section appended to `stars.octree`
* `index_offset`
* `index_length`

## Responsibilities

Phase B is responsible for:

* determining final shard membership of all nodes
* constructing required ancestor/frontier nodes
* assigning shard ids and shard offsets deterministically
* writing final shard headers
* writing final shard node tables
* writing frontier continuation references
* patching deferred shard fields where necessary

## Critical constraint

Phase B may not build a global `all_nodes` structure in memory.

Instead, it must operate in a bounded-memory, disk-backed way.

## Node existence model

Phase B must distinguish between:

* payload-bearing nodes
* synthetic ancestor nodes introduced for structural completeness
  n
  Synthetic ancestor nodes exist in the final index even when they have no payload.

## Recommended construction model

Build the final index **tier by tier**, not by global materialization.

For each shard tier:

1. determine all nodes that belong to the tier
2. derive required ancestors/frontier nodes for that tier from child-tier summaries or source payload-bearing cells
3. write final shard records for the tier
4. emit summary data needed by the parent tier
5. discard tier-local working data before moving upward

This keeps memory bounded to the active tier.

## Required lookup abstractions

### Intermediate existence lookup

Used to determine whether a payload-bearing node exists in the intermediate data.

```python
class IntermediateLookup:
    def has_payload_node(self, level: int, node_id: int) -> bool:
        ...
```

### Relocation lookup

Used to determine the final payload offset of a payload-bearing node.

```python
class RelocationLookup:
    def get_payload(self, level: int, node_id: int) -> tuple[int, int, int] | None:
        ...
```

Return value is:

* `output_offset`
* `payload_length`
* `star_count`

Both lookups must be implemented over fixed-record disk-backed files using binary search and bounded caches.

## Final shard layout requirements

The final `.octree` shard index must match the chosen final file format exactly.

At minimum, each shard must encode:

* shard header
* entry-node references or equivalent root-of-shard addressing
* node count
* frontier count or first-frontier index
* parent-shard linkage
* node table offset
* frontier table offset
* payload base or equivalent offset fields required by the format

Per-node records must encode:

* local path
* child mask
* payload presence
* payload offset/length where applicable
* frontier / continuation status if applicable

## Parent/child linkage

### Intra-shard children

For non-frontier nodes whose children remain inside the same shard:

* child mask is derived from node existence in the final logical tree
* local child references are encoded directly

### Frontier nodes

For frontier nodes whose children continue into descendant shards:

* frontier bit/flag must be set
* continuation reference must point to the correct child shard
* local child table references must follow the final format’s rules

## Deferred patching

Some shard header fields and frontier references may require patching after initial write.

This is acceptable and expected.

Phase B may use targeted patch passes for:

* parent node index within parent shard
* continuation shard offsets
* shard table offsets if not known at first write

Patch data must remain bounded.

A disk-backed temporary patch file is acceptable if needed.

## Phase B API

```python
@dataclass(frozen=True, slots=True)
class IndexPassResult:
    index_offset: int
    index_length: int


def write_final_shard_index(
    manifest_path: Path,
    relocation_files: tuple[Path, ...],
    output_fp: BinaryIO,
    *,
    plan: CombinePlan,
) -> IndexPassResult:
    ...
```

---

## Phase C — Header patching and cleanup

## Goal

Finalize the top-level file header and clean up temporary state.

## Responsibilities

* patch `index_offset`
* patch `index_length`
* patch any global fields that were unknown when the placeholder header was written
* remove temporary relocation files unless retention is enabled

## API

```python
def finalize_octree_header(
    output_path: Path,
    *,
    index_offset: int,
    index_length: int,
    world_center: tuple[float, float, float],
    world_half_size_pc: float,
) -> None:
    ...
```

Cleanup policy is controlled by `CombinePlan.retain_relocation_files`.

---

## Required utility layer

A dedicated fixed-record file abstraction is required.

```python
@dataclass(frozen=True, slots=True)
class FileHeader:
    level: int
    prefix_bits: int
    prefix: int
    record_count: int
    record_size: int
    flags: int


class FixedRecordFile:
    def __init__(self, path: Path, header_struct, record_struct, magic: bytes):
        ...

    @property
    def header(self) -> FileHeader:
        ...

    def read_record(self, i: int) -> tuple:
        ...

    def find_u64_key(self, key: int) -> tuple | None:
        ...

    def iter_records(self) -> Iterator[tuple]:
        ...
```

This abstraction should be backed by `np.memmap` or an equivalent bounded-memory mechanism.

This utility layer is shared by:

* intermediate index lookup
* relocation lookup
* any temporary patch files using fixed records

---

## Determinism requirements

`combine` must be fully deterministic.

Given identical intermediates, it must produce byte-identical final output.

This requires:

* deterministic DFS traversal order
* deterministic shard ordering
* deterministic child octant order
* deterministic file naming and shard-id assignment
* no dependence on incidental file-system ordering
* no nondeterministic hash iteration in write order

All iteration over shards must be based on explicit sorted keys.

---

## Error handling requirements

`combine` must fail fast on structural inconsistencies.

At minimum, it must validate:

* manifest version and binary layout identifiers
* matching shard metadata between manifest and on-disk files
* monotonic sorted order in index and relocation files where expected
* no duplicate `(level, node_id)` within a shard
* payload offsets and lengths remain within source file bounds
* referenced relocation entries exist when a payload-bearing node is written to the final index
* frontier continuation targets resolve to valid child shards where required

Corruption or inconsistency must raise a clear exception and abort output finalization.

---

## Logging and progress reporting

`combine` should report progress at phase boundaries and inside long-running passes.

Recommended progress messages:

* opening manifest and validating intermediates
* Phase A started / completed
* payload cells copied count
* payload bytes copied count
* relocation files written count
* Phase B started / completed
* shards written count
* nodes written count
* patch operations completed
* final output size

Progress accounting itself must remain bounded and must not require retaining large metadata.

---

## Non-goals

The following are explicitly out of scope for `combine`:

* star quantization or render-payload generation
* SQL query planning
* resorting source stars inside a cell
* changing intermediate payload encoding
* any architecture that requires whole-world in-memory expansion

---

## Forbidden implementation patterns

The following patterns are not permitted in `combine`:

* reading all shard index files into a single concatenated array
* computing DFS keys for all cells globally and sorting them in memory
* maintaining a global dictionary of `(level, node_id) -> output_offset`
* building a global `all_nodes` array including ancestors
* using whole-dataset numpy arrays where a fixed-record file lookup is sufficient

Any implementation that depends on these patterns is non-compliant with this specification.

---

## Minimum conformance checklist

A compliant `combine` implementation must satisfy all of the following:

* payload relocation is performed in DFS order
* payload bytes are copied directly into the final file
* relocation mappings are written to disk, not retained globally in RAM
* all large metadata accesses are via disk-backed fixed-record lookups
* only bounded caches and bounded local state are used
* final shard index is built without global materialization of all nodes
* final header is patched only after payload and index sections are complete
* output is deterministic

---

## Recommended module split

```text
three_dee/build/octree_combine_pipeline.py
three_dee/build/octree_dfs.py
three_dee/build/octree_lookup.py
three_dee/build/octree_records.py
three_dee/build/octree_manifest.py
```

### Responsibilities

* `octree_combine_pipeline.py`

  * orchestration of Phases A, B, C
* `octree_dfs.py`

  * DFS traversal and per-node visitation logic
* `octree_lookup.py`

  * fixed-record file lookup and file-handle cache
* `octree_records.py`

  * binary record/header packing and constants
* `octree_manifest.py`

  * manifest parsing and validation

---

## Suggested implementation order

1. `FixedRecordFile` and manifest reader
2. shard file-handle cache
3. Phase A DFS payload relocation
4. relocation file reader/lookup
5. Phase B shard-index writer
6. patching helpers
7. Phase C final header patch and cleanup
8. end-to-end conformance tests for determinism and bounded memory

---

## Summary

Pipeline 2 `combine` is a bounded-memory assembly pipeline, not an in-memory transform.

Its central design principles are:

* stream payloads directly to the final file in DFS order
* persist offset remapping to disk immediately
* treat shard index files as searchable disk-backed arrays
* build the final shard index in a bounded-memory multi-pass manner
* patch offsets deliberately rather than buffering globally

Any implementation that departs from those principles is out of spec.
