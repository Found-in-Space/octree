# Stage 02: `combine` — Detailed Specification

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

## Pinned binary layouts

This section defines the exact binary packing for the final file and final shard index. These layouts are normative.

All integer fields are little-endian.

### Top-level file header

The final `stars.octree` file begins with a fixed 64-byte header.

```python
HEADER_FMT = struct.Struct("<4sHHQQ3ffHHf16s")
HEADER_SIZE = 64
```

Fields, in order:

1. `magic: 4s`

   * file magic
   * value: `b"STAR"`
2. `version: u16`

   * file format version
   * value: `1`
3. `flags: u16`

   * global file flags
   * currently `0`
4. `index_offset: u64`

   * absolute file offset of the shard-index section
5. `index_length: u64`

   * length in bytes of the shard-index section
6. `world_center_x: f32`
7. `world_center_y: f32`
8. `world_center_z: f32`
9. `world_half_size_pc: f32`
10. `payload_record_size: u16`

* fixed quantized render-record size in bytes
* current value: `16`

11. `max_level: u16`

* maximum octree level represented in the file

12. `mag_limit: f32`

* apparent-magnitude cutoff used to build the file

13. `reserved: 16s`

* all zero for version 1

The header is first written with `index_offset = 0` and `index_length = 0`, then patched during Phase C.

### Future header roadmap

The current version 1 header does not carry dataset identity.

A future top-level header revision should include an explicit render-dataset UUID so consumers can validate compatibility and key caches without depending on URLs or filenames.

Sidecar outputs will also need explicit UUID metadata:

- `parent_dataset_uuid` to identify the render octree they align with
- `sidecar_uuid` to identify the specific version of that sidecar artifact

Because version 1 reserves only 16 bytes, supporting both UUIDs for sidecar outputs will likely require a header version bump or another explicit descriptor layer rather than silently overloading the v1 reserved bytes.

### Final shard header

Each shard begins with a fixed 80-byte header.

```python
SHARD_HDR_FMT = struct.Struct("<4sHBBIIHHHhIII8HHQQQ2x")
SHARD_HDR_SIZE = 80
```

Fields, in order:

1. `magic: 4s`

   * shard magic
   * value: `b"OSHR"`
2. `version: u16`

   * shard format version
   * value: `1`
3. `levels_in_shard: u8`

   * fixed value `5`
4. `flags: u8`

   * shard-level flags, currently `0`
5. `shard_id: u32`

   * deterministic 1-based shard identifier in final write order
6. `parent_shard_id: u32`

   * `0` for root shard, otherwise parent shard id
7. `parent_node_index: u16`

   * shard-local node index in the parent shard that this shard continues from
   * `0` for root shard
8. `node_count: u16`

   * number of node-table entries in this shard
9. `reserved0: u16`

   * reserved, must be `0` in version 1
10. `parent_global_depth: i16`

    * global depth of the shard’s implicit parent
    * root shard uses `-1`
11. `parent_grid_x: u32`
12. `parent_grid_y: u32`
13. `parent_grid_z: u32`

    * grid coordinates of the shard’s implicit parent node
14. `entry_nodes[8]: 8 * u16`

    * shard-local node indices of the top-level explicit nodes, one per octant
15. `first_frontier_index: u16`

    * shard-local index of the first frontier node, or `0` if no frontier nodes exist
16. `node_table_offset: u64`

    * absolute file offset of the node table
17. `frontier_table_offset: u64`

    * absolute file offset of the frontier-reference table
18. `payload_base_offset: u64`

    * retained for compatibility/inspection; readers must use per-node absolute payload offsets
19. `pad: 2 bytes`

    * zero padding to 80 bytes

### Final node record

Each node-table entry is exactly 20 bytes.

```python
SHARD_NODE_FMT = struct.Struct("<HHBBBBQI")
SHARD_NODE_SIZE = 20
```

Fields, in order:

1. `first_child: u16`
2. `local_path: u16`
3. `child_mask: u8`
4. `local_depth: u8`
5. `flags: u8`
6. `reserved: u8`

   * must be `0` in version 1
7. `payload_offset: u64`
8. `payload_length: u32`

`local_path` is `u16` because the maximum local path length is 5 octants, requiring at most 15 bits.

### Final frontier-reference record

Each frontier-reference entry is exactly 8 bytes.

```python
FRONTIER_REF_FMT = struct.Struct("<Q")
FRONTIER_REF_SIZE = 8
```

Fields, in order:

1. `continuation_shard_offset: u64`

   * absolute file offset of the continuation shard
   * `0` if the frontier node has no continuation children and the positional-alignment convention is used

### Node flags

Version 1 uses the following node-flag bits:

* `HAS_PAYLOAD = 0x01`
* `HAS_CHILDREN = 0x02`
* `IS_FRONTIER = 0x04`

All other bits are reserved and must be `0`.

### Intermediate and relocation fixed-record files

The intermediate and relocation files described below are also normative fixed-record files. Their exact layouts are defined in the build/intermediate specification and must be treated as part of the on-disk contract used by `combine`.

### Width constraints implied by the layout

These layouts impose the following limits:

* `node_count <= 65535` for any one shard
* shard-local node indices are `u16`
* `local_path <= 0x7FFF`
* `entry_nodes` and `parent_node_index` are `u16`

Any implementation that would exceed those limits for a shard must fail explicitly rather than silently widening fields.

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

### DFS traversal pruning rule

`iter_cells_dfs()` must never enumerate the full conceptual `8^max_level` node space.

It may only visit:

* payload-bearing cells that actually exist in the intermediates
* strict ancestors of those payload-bearing cells that are needed to guide DFS descent

The traversal therefore operates over the **realized prefix set**, not the full theoretical tree.

### Required pruning mechanism

Before descending from candidate node `(level = L, node_id = N)` into child octant `o`, the iterator must prove that the child prefix contains at least one realized payload-bearing node at some level `D >= L + 1`.

Let the child candidate be:

* `child_level = L + 1`
* `child_node_id = (N << 3) | o`

For any deeper level `D >= child_level`, descendants of that child occupy the Morton-prefix range:

* `lo(D) = child_node_id << (3 * (D - child_level))`
* `hi(D) = ((child_node_id + 1) << (3 * (D - child_level))) - 1`

A descendant exists in shard `(D, prefix_bits, prefix)` iff there exists at least one record with `node_id` in `[lo(D), hi(D)]`.

This must be tested using lower-bound binary search on that shard’s sorted fixed-record file.

### Shard filtering before binary search

For a child prefix, `iter_cells_dfs()` must first eliminate shards that cannot possibly contain descendants.

A shard at level `D` is relevant only if:

* `D >= child_level`, and
* the shard’s own `(prefix_bits, prefix)` constraint is compatible with the child prefix

Compatibility means that the shard’s prefix range overlaps the child prefix range at level `D`.

Only relevant shards may be queried.

### Descent rule

The iterator may recurse into child octant `o` only if at least one relevant shard at some level `D >= child_level` contains a record in that descendant range.

If no relevant shard contains such a record, that child branch must be pruned immediately.

### Complexity requirement

This rule guarantees that traversal cost is proportional to the number of realized payload-bearing nodes and realized internal prefixes, not to the full size of the theoretical octree.

### Bounded caches

To avoid repeated identical binary searches, the iterator may use bounded caches for:

* prefix-existence checks
* recently used shard readers
* lower-bound positions for nearby prefixes

Such caches are permitted only if they are explicitly bounded by `CombinePlan.lookup_cache_records` or equivalent finite limits.

### Preferred operational model

The preferred implementation is prefix-driven DFS:

1. start at the conceptual root prefix
2. for octants `0..7`, test whether the child prefix has any realized descendants
3. descend only into child prefixes that pass that test
4. at each visited prefix, emit the cell if a payload-bearing node exists exactly at that `(level, node_id)`
5. continue recursively until `max_level`

This model preserves deterministic DFS order while ensuring aggressive pruning.

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

Nodes inside a shard are stored in **depth/path order**, not strict preorder DFS.

This ordering is normative.

The sort key is:

1. `local_depth` ascending
2. `local_path` ascending

where `local_path` is compared as the packed octant-path value for nodes at the same `local_depth`.

#### Important clarification

This is **not** strict preorder DFS over the shard-local subtree.

In particular:

* all depth-1 nodes appear before all depth-2 nodes
* all depth-2 nodes appear before all depth-3 nodes
* and so on

This ordering is chosen deliberately because it guarantees that:

* parents always appear before children
* all frontier nodes (`local_depth = LEVELS_PER_SHARD`) form a contiguous suffix of the node table
* immediate children of a non-frontier node are contiguous in octant order within the next depth band

The specification must not describe this order as DFS/preorder. The implementation and on-disk contract are depth-major.

#### Consequences for readers

Readers must not assume that a node’s full descendant subtree occupies a contiguous range of the node table.

Only the following contiguity guarantee is provided:

* the **immediate in-shard children** of a non-frontier node occupy a contiguous run, in octant order, beginning at `first_child`

No stronger subtree contiguity property is guaranteed.

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

Because node-table order is depth-major, the child run for a node lies within the band of nodes at `local_depth + 1`.

Children present in the same shard are stored contiguously in octant order within that band.

Therefore, given `first_child` and `child_mask`, a reader can enumerate the node’s in-shard children without storing eight separate pointers.

#### Enumeration rule for in-shard children

Let:

* `m = child_mask`
* `i = first_child`

If the node is **not** a frontier node:

* all in-shard children represented by bits set in `child_mask` are stored contiguously beginning at node index `i`
* the child for the lowest present octant is at `i`
* the child for the next present octant is at `i + 1`
* and so on in increasing octant order

Formally, for octant `o`, if bit `o` is set and the child is in-shard, then its node-table index is:

* `first_child + popcount(child_mask & ((1 << o) - 1))`

This is the full meaning of the “next nodes” model.

There is no linked list and no separate sibling pointer chain.

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
3. Node table order is deterministic and is exactly depth-major `(local_depth, local_path)` order.
4. The node table is **not** required to be strict preorder DFS.
5. For non-frontier nodes, immediate in-shard children are contiguous and ordered by octant.
6. `first_child = 0` iff there are no in-shard children.
7. `child_mask = 0` iff the node has no logical children.
8. `HAS_CHILDREN` iff `child_mask != 0`.
9. `IS_FRONTIER` iff `local_depth = LEVELS_PER_SHARD`.
10. Frontier nodes form a contiguous suffix of the node table.
11. Frontier nodes never use `first_child` for continuation; continuation is via frontier refs only.
12. `parent_node_index` of a continuation shard points to a frontier node in the parent shard.
13. Payload fields are consistent with `HAS_PAYLOAD`.
14. All offsets written into headers, node records, and frontier refs are absolute file offsets.

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
foundinspace/octree/combine/pipeline.py
foundinspace/octree/combine/dfs.py
foundinspace/octree/combine/lookup.py
foundinspace/octree/combine/records.py
foundinspace/octree/combine/manifest.py
```

### Responsibilities

* `pipeline.py`

  * orchestration of Phases A, B, C
* `dfs.py`

  * DFS traversal and per-node visitation logic
* `lookup.py`

  * fixed-record file lookup and file-handle cache
* `records.py`

  * binary record/header packing and constants
* `manifest.py`

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

Stage 02 `combine` is a bounded-memory assembly step, not an in-memory transform.

Its central design principles are:

* stream payloads directly to the final file in DFS order
* persist offset remapping to disk immediately
* treat shard index files as searchable disk-backed arrays
* build the final shard index in a bounded-memory multi-pass manner
* patch offsets deliberately rather than buffering globally

Any implementation that departs from those principles is out of spec.
