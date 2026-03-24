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

## Detailed final shard-index format

The final index is a sequence of shard records written after the payload section.

Each shard contains:

1. a fixed-size shard header
2. a contiguous node table
3. a contiguous frontier-reference table

There is no separate global node table. Nodes are addressed relative to their containing shard.

### Conceptual model

A shard represents a 5-level block of the octree.

* `LEVELS_PER_SHARD = 5`
* every shard has an **implicit parent**
* the first explicit nodes in a shard are the children of that implicit parent
* therefore, the shard does **not** store a record for its implicit parent

This applies both to the root shard and to continuation shards.

### Shard-local node addressing

Node indices inside a shard are **1-based**.

* `0` means “no node” or “null reference”
* `1..node_count` index records in the shard-local node table

All child references that stay within the shard are shard-local node indices.

### Deterministic node ordering inside a shard

Nodes inside a shard are stored in deterministic DFS/preorder order over the shard-local subtree.

The ordering rule is:

* primary key: `local_depth`
* within a shared ancestor path, children appear in octant order `0..7`
* operationally, this is the existing packed sort-path ordering used to produce stable DFS order inside a shard

This ordering guarantees that a node’s descendants, if present in the same shard, occur later in the table than the node itself.

### Shard header

The shard header must include at least the following fields.

* `magic`

  * shard-format magic
* `version`

  * shard-format version
* `levels_in_shard`

  * fixed value `5`
* `flags`

  * reserved for future shard-level flags
* `shard_id`

  * deterministic 1-based shard identifier in final write order
* `parent_shard_id`

  * `0` for root shard, otherwise the shard id of the parent shard
* `parent_node_index`

  * shard-local node index in `parent_shard_id` that acts as the frontier/attachment point for this shard
  * `0` for the root shard
* `node_count`

  * number of node records in the node table
* `reserved`

  * reserved/padding as required by the binary layout
* `parent_global_depth`

  * global depth of the implicit parent of this shard
  * root shard uses sentinel `-1`
* `parent_grid_x`, `parent_grid_y`, `parent_grid_z`

  * grid coordinates of the shard’s implicit parent node
* `entry_nodes[8]`

  * for each octant `0..7`, the shard-local node index of that entry node, or `0` if absent
* `first_frontier_index`

  * shard-local index of the first frontier node in the node table, or `0` if there are none
* `node_table_offset`

  * absolute file offset of this shard’s node table
* `frontier_table_offset`

  * absolute file offset of this shard’s frontier-reference table
* `payload_base_offset`

  * retained only if required by the binary format; not used as a substitute for per-node payload offsets

#### Meaning of `entry_nodes[8]`

`entry_nodes` provides the shard’s top-level addressing.

For any explicit node in the shard with `local_depth = 1`, its `local_path` is exactly one octant `0..7`. That node’s shard-local index is written into `entry_nodes[octant]`.

If a top-level octant has no explicit node in this shard, its entry is `0`.

A reader enters a shard by selecting one of these entry-node pointers.

### Node record

Each node-table entry must include at least the following fields.

* `first_child`

  * shard-local node index of the first child present **within the same shard**
  * `0` if the node has no in-shard children
* `local_path`

  * packed path from the shard’s implicit parent to this node
  * encoded using 3 bits per step
  * for a node at `local_depth = d`, `local_path` contains exactly `d` octants
* `child_mask`

  * 8-bit mask over octants `0..7`
  * bit `o` set means the node has a child in octant `o` in the **logical tree**
* `local_depth`

  * depth of this node relative to the shard’s implicit parent
  * valid range `1..LEVELS_PER_SHARD`
* `flags`

  * per-node flags described below
* `reserved`

  * reserved/padding as required by the binary layout
* `payload_offset`

  * absolute file offset of the payload blob for this node, or `0` if no payload
* `payload_length`

  * payload length in bytes, or `0` if no payload

### Node flags

The following node flags are required.

* `HAS_PAYLOAD`

  * node has a payload blob in the payload section
* `HAS_CHILDREN`

  * node has at least one child in the logical tree, equivalent to `child_mask != 0`
* `IS_FRONTIER`

  * node is at `local_depth = LEVELS_PER_SHARD`, so descendants, if any, continue into child shards

No separate `HAS_FRONTIER_REF` flag is needed if frontier nodes are guaranteed to occupy the suffix of the node table and are covered by the frontier-reference table.

### Meaning of `local_path`

`local_path` is the path from the shard’s implicit parent to the node, encoded in octants.

Examples:

* a node at local depth 1 in octant 3 has `local_path = 3`
* a node at local depth 2 reached by octants `3 -> 5` has `local_path` encoding `[3, 5]`

`local_path` is a stable identity inside the shard and is used for:

* deterministic node sorting
* reconstructing child relationships
* patching parent-node references for continuation shards

### Child representation and the “next nodes” pointer model

The index does **not** store eight explicit child pointers per node.

Instead, child linkage is represented by the pair:

* `child_mask`
* `first_child`

This is the complete definition of how “next nodes” work.

#### `child_mask`

`child_mask` tells the reader **which octant children exist logically**.

Example:

* `child_mask = 0b00101001`
* children exist in octants `0`, `3`, and `5`

#### `first_child`

`first_child` points to the shard-local node index of the **first in-shard child** of the node.

Children present in the same shard are stored contiguously in octant order.

Therefore, given `first_child` and `child_mask`, a reader can enumerate the node’s in-shard children without storing eight separate pointers.

#### Enumeration rule for in-shard children

Let:

* `m = child_mask`
* `i = first_child`

If the node is **not** a frontier node:

* all children represented by bits set in `child_mask` that are in the same shard are stored contiguously beginning at node index `i`
* the child for the lowest present octant is at `i`
* the child for the next present octant is at `i + 1`
* and so on in increasing octant order

Formally, for octant `o`, if bit `o` is set and the child is in-shard, then its node-table index is:

* `first_child + popcount(child_mask & ((1 << o) - 1))`

This is the full meaning of the “next nodes” model.

There is no linked list and no separate sibling pointer chain. The node table order plus contiguity is the mechanism.

#### Constraint required for correctness

For every non-frontier node with in-shard children, the node table must satisfy:

* all in-shard children appear contiguously
* they appear in octant order
* `first_child` points to the first such child

The writer must guarantee this invariant.

### Frontier-node behavior

A frontier node is any node with:

* `local_depth = LEVELS_PER_SHARD`
* `IS_FRONTIER` flag set

A frontier node may still have `child_mask != 0`, indicating logical children exist.

However, those children are **not stored in the current shard’s node table**.

Instead:

* `first_child = 0`
* continuation is resolved through the frontier-reference table

This is a critical invariant.

### Frontier-reference table

The frontier-reference table contains one record for each frontier node in node-table order.

Its records correspond positionally to frontier nodes beginning at `first_frontier_index`.

If there are `frontier_count` frontier nodes, then:

* node `first_frontier_index` uses frontier-ref record `0`
* node `first_frontier_index + 1` uses frontier-ref record `1`
* and so on

Each frontier-ref record stores:

* absolute file offset of the continuation shard for that frontier node

If a frontier node has `child_mask = 0`, its frontier reference may be `0` or omitted by convention, but the preferred invariant is:

* only frontier nodes with actual continuation children are written into the frontier-reference table
* if the format requires positional alignment for all frontier nodes, a frontier node with no continuation uses a `0` reference

The implementation must choose one rule and apply it consistently. The preferred rule for simplicity is positional alignment for **all** frontier nodes.

### How a reader follows children

#### Case 1: ordinary node with in-shard children

Reader steps:

1. read `child_mask`
2. if `child_mask == 0`, stop
3. read `first_child`
4. enumerate set bits of `child_mask` in octant order
5. map each in-shard child octant to node-table indices using the contiguous-child formula above

#### Case 2: frontier node

Reader steps:

1. read `child_mask`
2. if `child_mask == 0`, stop
3. observe `IS_FRONTIER`
4. compute the frontier-ref table row from node index and `first_frontier_index`
5. read the continuation-shard offset
6. open that shard and use its `entry_nodes[8]` to resolve the children indicated by `child_mask`

The frontier node’s logical children are therefore obtained from the child shard’s top-level entry nodes, not from the current node table.

### Relationship between parent and continuation shard

Every non-root shard stores:

* `parent_shard_id`
* `parent_node_index`

`parent_node_index` identifies the frontier node in the parent shard whose continuation this shard represents.

The child shard’s implicit parent is exactly that frontier node.

This means:

* the child shard’s `entry_nodes[octant]` correspond to that frontier node’s octant children
* octants absent from the frontier node’s `child_mask` must have `entry_nodes[octant] = 0`

### Payload fields per node

For each node:

* `payload_offset != 0` and `payload_length > 0` iff `HAS_PAYLOAD` is set
* otherwise both are `0`

Payload offsets are absolute file offsets into the final payload section. Readers do not need `payload_base_offset` to resolve individual node payloads.

### Required invariants

The writer must guarantee all of the following.

1. Node table indices are 1-based.
2. `entry_nodes` point only to nodes with `local_depth = 1`.
3. Node table order is deterministic.
4. For non-frontier nodes, in-shard children are contiguous and ordered by octant.
5. `first_child = 0` iff there are no in-shard children.
6. `child_mask = 0` iff the node has no logical children.
7. `HAS_CHILDREN` iff `child_mask != 0`.
8. `IS_FRONTIER` iff `local_depth = LEVELS_PER_SHARD`.
9. Frontier nodes never use `first_child` for continuation; continuation is via frontier refs only.
10. `parent_node_index` of a continuation shard points to a frontier node in the parent shard.
11. Payload fields are consistent with `HAS_PAYLOAD`.
12. All offsets written into headers, node records, and frontier refs are absolute file offsets.

### Notes on implementation

The existing implementation approach of sorting shard nodes by packed sort-path and then deriving:

* `entry_nodes`
* `child_mask`
* `first_child`
* frontier node suffix
* frontier-ref positional alignment

is consistent with this specification, provided the contiguity invariants above are enforced and documented.

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
