# Octree Reader — Specification

## Purpose

Read a `stars.octree` file produced by stage-02 and execute bounded spatial queries against it.

The reader must:

* parse the top-level `STAR` header and `OSHR` shard index
* navigate the shard/node tree without materializing the full index
* decompress and dequantize star payloads on demand
* support two bounded spatial queries (see §Queries)
* depend only on code within `found-in-space-octree` — no imports from `three-dee`

---

## Queries

### 1. Visible stars from a point

Given a viewpoint in world coordinates and a limiting apparent magnitude, return all stars whose apparent magnitude at that viewpoint is at or below the limit.

This is the "shell loader" query. It uses the octree's magnitude-based level structure to prune: a node at level L has half-size H(L), and its load radius is `H(L) × 10^((m_app − m_index) / 5)` where `m_index` is the file's `mag_limit`. If the minimum distance from the viewpoint to the node's AABB exceeds the load radius, the entire subtree is skipped.

```
stars_brighter_than(point, limiting_magnitude) → Iterator[Star]
```

### 2. Nearby stars from a point

Given a viewpoint and a distance in parsecs, return all stars within that distance regardless of brightness.

This prunes by AABB distance: if the minimum distance from the viewpoint to a node's axis-aligned bounding box exceeds the search distance, the subtree is skipped.

```
stars_within_distance(point, distance_pc) → Iterator[Star]
```

Both queries are naturally bounded — they descend from root entries and prune aggressively, touching only the fraction of the tree geometrically relevant to the query.

---

## Architecture

The reader is split into four layers with clear responsibilities and no circular dependencies.

```
┌──────────────────────────────────────────────────┐
│                  Spatial queries                 │  stars_brighter_than, stars_within_distance
│              foundinspace.octree.reader           │
├──────────────────────────────────────────────────┤
│              Payload decoder                     │  gzip decompress → dequantize star records
│         foundinspace.octree.reader.payload        │
├──────────────────────────────────────────────────┤
│              Index navigator                     │  shard headers, node tables, child resolution
│          foundinspace.octree.reader.index         │
├──────────────────────────────────────────────────┤
│              File header                         │  STAR header parsing, bootstrap fields
│         foundinspace.octree.reader.header         │
├──────────────────────────────────────────────────┤
│          Binary record constants                 │  shared with combine pipeline
│       foundinspace.octree.combine.records         │
└──────────────────────────────────────────────────┘
```

### Dependency rule

Each layer may import from layers below it and from `combine.records` for binary constants. No layer may import from the write pipeline modules (`assembly`, `combine.pipeline`, `combine.dfs`).

---

## Layer 1: File header (`reader.header`)

Parses the 64-byte `STAR` top-level header and validates the shard magic at `index_offset`.

### `OctreeHeader`

```python
@dataclass(frozen=True, slots=True)
class OctreeHeader:
    version: int
    index_offset: int
    index_length: int
    world_center: tuple[float, float, float]
    world_half_size: float        # parsecs
    payload_record_size: int      # bytes per quantized star record (currently 16)
    max_level: int
    mag_limit: float              # apparent-magnitude cutoff used during build
```

### `read_header`

```python
def read_header(path: Path) -> OctreeHeader:
    """Read and validate the STAR file header.

    Raises ValueError if magic is wrong, version is unsupported, or the
    shard probe at index_offset does not match OSHR magic.
    """
```

This function opens the file, reads 64 bytes, unpacks via `HEADER_FMT` from `combine.records`, probes 4 bytes at `index_offset` to confirm `OSHR` magic, and returns an `OctreeHeader`. It does not hold the file open.

`HEADER_FMT` tuple-to-field mapping (from `pack_top_level_header` in `combine.records`):

| Index | Field |
|-------|-------|
| 0 | magic (`b"STAR"`) |
| 1 | version |
| 2 | flags |
| 3 | index_offset |
| 4 | index_length |
| 5 | world_center.x |
| 6 | world_center.y |
| 7 | world_center.z |
| 8 | world_half_size |
| 9 | payload_record_size |
| 10 | max_level |
| 11 | mag_limit |
| 12 | reserved |

`index_offset` points to the root shard header (`OSHR`) for tree traversal.

---

## Layer 2: Index navigator (`reader.index`)

Provides tree traversal over the shard/node index without decompressing any payloads.

### Core types

```python
@dataclass(frozen=True, slots=True)
class Point:
    x: float
    y: float
    z: float

    def distance_to(self, other: Point) -> float: ...


@dataclass(frozen=True, slots=True)
class GridCoord:
    x: int
    y: int
    z: int


@dataclass(frozen=True, slots=True)
class NodeEntry:
    """One node in the octree index."""
    level: int
    grid: GridCoord
    center: Point
    half_size: float           # parsecs
    flags: int
    child_mask: int            # bit o set => child exists in octant o
    payload_offset: int        # absolute file offset, 0 if no payload
    payload_length: int        # compressed bytes, 0 if no payload

    @property
    def is_leaf(self) -> bool:
        """True when no children exist in any octant."""

    @property
    def has_payload(self) -> bool:
        """True when HAS_PAYLOAD flag is set and payload_length > 0."""

    def aabb_distance(self, point: Point) -> float:
        """Minimum Euclidean distance from point to this node's AABB."""
```

### `IndexNavigator`

The navigator holds an open file handle to the `.octree` file and reads shard headers / node tables on demand. It caches nothing beyond the current shard being read.

```python
class IndexNavigator:
    def __init__(self, path: Path, header: OctreeHeader) -> None:
        """Open the octree file for index reads."""

    def close(self) -> None: ...
    def __enter__(self) -> IndexNavigator: ...
    def __exit__(self, *exc) -> None: ...

    def root_entries(self) -> Iterator[NodeEntry]:
        """Yield one NodeEntry per non-empty top-level octant (up to 8)."""

    def get_child(self, parent: NodeEntry, octant: int) -> NodeEntry | None:
        """Resolve one child of parent at the given octant (0–7).

        Handles both intra-shard children (via first_child + child_mask)
        and frontier continuation (via frontier-reference table → child shard).
        Returns None if the octant has no child.
        """

    def find_node_at(self, point: Point, level: int) -> NodeEntry | None:
        """Descend from root to the node at the given level containing point.

        Returns None if the tree has no node at that position/level.
        """
```

### Geometry derivation

`NodeEntry.center` and `NodeEntry.half_size` are derived from `(level, grid)` and the file header's `world_center` / `world_half_size`:

```
n = 2^level
half_size = world_half_size / n
center.x = world_center.x + (2 × (gx + 0.5) − n) × half_size
```

Grid coordinates `(gx, gy, gz)` are decoded from the node's `local_path` relative to the shard's `parent_global_depth` and `parent_grid_{x,y,z}`, using the octant bit convention already used by the combine pipeline.

Octant bit convention (per octant value `o`):

- `x_bit = (o >> 0) & 1`
- `y_bit = (o >> 1) & 1`
- `z_bit = (o >> 2) & 1`

Path accumulation convention:

- `local_path` is built by shifting left three bits and appending each next octant.
- The shallowest octant is therefore in the highest bits; the deepest octant is in the lowest bits.
- To decode global grid coordinates, iterate octants from shallowest to deepest and shift/add one axis bit per depth step.

### Shard traversal internals

The navigator must implement:

1. **Shard header reading** — parse `OSHR` header at a given absolute offset.
   - Format: `SHARD_HDR_FMT = "<4sHBBIIHHHhIII8HHQQQ2x"` (`SHARD_HDR_SIZE = 80`)
   - Field order:
     - magic, version, levels_per_shard, flags
     - shard_id, parent_shard_id, parent_node_index
     - node_count, reserved0
     - parent_global_depth, parent_grid_x, parent_grid_y, parent_grid_z
     - `entry_nodes[8]`
     - first_frontier_index
     - node_table_offset, frontier_table_offset, payload_base_offset
   - Reader uses at minimum: `node_count`, `parent_global_depth`, `parent_grid_*`, `entry_nodes`, `first_frontier_index`, `node_table_offset`, `frontier_table_offset`.

2. **Node reading** — read one 20-byte `SHARD_NODE` record at `node_table_offset + (node_index − 1) × 20`.
   - Format: `SHARD_NODE_FMT = "<HHBBBBQI"` (`SHARD_NODE_SIZE = 20`)
   - Field order:
     - `first_child: u16`
     - `local_path: u16`
     - `child_mask: u8`
     - `local_depth: u8`
     - `flags: u8`
     - `reserved: u8`
     - `payload_offset: u64`
     - `payload_length: u32`

3. **Intra-shard child resolution** — for a non-frontier node, the child at octant `o` is at node index `first_child + popcount(child_mask & ((1 << o) − 1))`.

4. **Frontier continuation** — for a frontier node (`local_depth == LEVELS_PER_SHARD`), read the continuation shard offset from the frontier-reference table, open that shard, and resolve via its `entry_nodes[octant]`.

These are internal implementation details. The public API exposes only `root_entries`, `get_child`, and `find_node_at`.

If a shard has no entries in some octants (`entry_nodes[octant] == 0`), those octants are simply skipped (not an error).

---

## Layer 3: Payload decoder (`reader.payload`)

Reads and decompresses gzip'd star payloads, dequantizing each 16-byte record to world coordinates.

### Star record layout (16 bytes)

| Offset | Size | Type   | Field       | Description                                    |
|--------|------|--------|-------------|------------------------------------------------|
| 0      | 4    | f32    | x           | cell-relative x position, range [−1, 1]        |
| 4      | 4    | f32    | y           | cell-relative y position, range [−1, 1]        |
| 8      | 4    | f32    | z           | cell-relative z position, range [−1, 1]        |
| 12     | 2    | i16    | mag_centi   | absolute magnitude × 100                       |
| 14     | 1    | u8     | teff_log8   | log-encoded effective temperature               |
| 15     | 1    | u8     | pad         | reserved                                        |

### Dequantization

Given a `NodeEntry` with center `(cx, cy, cz)` and `half_size`:

```
world_x = cx + x × half_size
world_y = cy + y × half_size
world_z = cz + z × half_size
magnitude = mag_centi / 100.0
teff = 2000 × (50000 / 2000)^(teff_log8 / 255)
```

Temperature sentinel handling:

- `teff_log8 == 255` (`TEFF_SENTINEL`) denotes unknown temperature.
- Reader behavior: set `Star.teff = float("nan")` for sentinel records.

### Types

```python
@dataclass(frozen=True, slots=True)
class Star:
    position: Point
    magnitude: float           # absolute magnitude
    teff: float                # effective temperature (K)
    node: NodeEntry            # the node this star belongs to

    def apparent_magnitude_at(self, point: Point) -> float:
        """m_app = M_abs + 5 × (log10(d_pc) − 1), clamped to d >= 1e-6 pc."""
```

### `decode_payload`

```python
def decode_payload(
    fp: BinaryIO,
    node: NodeEntry,
    record_size: int,
) -> list[Star]:
    """Seek to node.payload_offset, read payload_length bytes, gzip-decompress,
    and dequantize each record into a Star.

    Returns an empty list if node.has_payload is False.
    """
```

This function:
1. Seeks to `node.payload_offset`
2. Reads `node.payload_length` bytes
3. Decompresses with `gzip.decompress`
4. Splits into `record_size`-byte chunks
5. Unpacks and dequantizes each record using the node's geometry
6. Returns a list of `Star` objects

---

## Layer 4: Spatial queries (`reader`)

The top-level module composes the navigator and payload decoder to implement the two bounded queries.

### `OctreeReader`

```python
class OctreeReader:
    def __init__(self, path: Path) -> None:
        """Open a stars.octree file for spatial queries.

        Reads the file header and opens an IndexNavigator.
        """

    def close(self) -> None: ...
    def __enter__(self) -> OctreeReader: ...
    def __exit__(self, *exc) -> None: ...

    @property
    def header(self) -> OctreeHeader: ...

    def stars_brighter_than(
        self,
        point: Point,
        limiting_magnitude: float,
    ) -> Iterator[Star]:
        """Yield stars visible from point at or below limiting_magnitude.

        Uses shell-based pruning: for each node at level L,
        load_radius = half_size × 10^((limiting_magnitude − mag_limit) / 5).
        Skips subtrees whose AABB distance from point exceeds load_radius.
        For each payload-bearing node within range, decompresses the payload
        and yields stars whose apparent magnitude at point ≤ limiting_magnitude.
        """

    def stars_within_distance(
        self,
        point: Point,
        distance_pc: float,
    ) -> Iterator[Star]:
        """Yield all stars within distance_pc parsecs of point.

        Prunes subtrees whose AABB distance from point exceeds distance_pc.
        For each payload-bearing node within range, decompresses the payload
        and yields stars whose Euclidean distance to point ≤ distance_pc.
        """
```

### Query implementation

Both queries follow the same pattern:

1. Iterate `root_entries()` from the navigator.
2. For each root entry, push onto a stack.
3. Pop from stack; compute the pruning predicate:
   - `stars_brighter_than`: `node.aabb_distance(point) > load_radius` where `load_radius = node.half_size × 10^((m_app − m_index) / 5)`
   - `stars_within_distance`: `node.aabb_distance(point) > distance_pc`
4. If pruned, skip.
5. If the node has a payload, decode it and yield qualifying stars.
6. Push all children (octants 0–7 via `get_child`) onto the stack.

The stack-based traversal avoids recursion depth issues on deep trees and keeps memory usage proportional to tree depth × 8 (at most `max_level × 8` entries).

Implementation detail for node expansion:

- Determine child existence from `node.child_mask`.
- For each octant bit that is set, call `get_child(node, octant)` and push non-`None` children.

---

## CLI integration

Add a `stats` subcommand to the existing `fis-octree` CLI.

```
fis-octree stats <OCTREE_PATH> [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--point X,Y,Z` | `0,0,0` | Query origin in parsecs (ICRS) |
| `--magnitude` | `6.5` | Limiting apparent magnitude for the visibility query |
| `--radius` | `10.0` | Search radius in parsecs for the proximity query |

### Output

The `stats` command runs both queries from the given point and prints:

1. **File header summary** — world center, half size, max level, mag limit, file size.

2. **Visible-stars table** — per-level breakdown of the shell-loaded result:
   - Level
   - Nodes touched at that level
   - Stars loaded (total in those nodes)
   - Stars rendered (passing magnitude filter)
   - Compressed payload bytes read

   Plus a total row.

3. **Nearest-stars table** — the N closest stars from the proximity query (default N=10):
   - Distance (pc)
   - Absolute magnitude
   - Apparent magnitude at the query point
   - Teff (K)

`stats` implementation note:

- For the visible-stars table, "Stars loaded" means all stars decoded from payload-bearing nodes that pass node-level pruning.
- "Stars rendered" means stars that also pass the per-star apparent-magnitude filter.
- To compute both values and per-level byte counts, the command should drive traversal with `IndexNavigator + decode_payload` directly rather than using only the public filtered iterator.

This output is sufficient for:
- **Integrity testing** — confirms the file is readable, the index is navigable, payloads decompress correctly, and dequantized positions are sensible.
- **Performance testing** — measures how many nodes/bytes are touched for a bounded query vs the full file.
- **Regression testing** — the deterministic pipeline guarantees stable output for stable input, so these numbers can be compared across builds.

---

## Module layout

```
src/foundinspace/octree/
    reader/
        __init__.py          # OctreeReader, Star, Point (re-exports)
        header.py            # OctreeHeader, read_header
        index.py             # IndexNavigator, NodeEntry, GridCoord
        payload.py           # decode_payload, star record constants
    _cli.py                  # add 'stats' subcommand
```

---

## Shared constants

The reader imports binary layout constants from the existing `combine.records` module:

- `HEADER_FMT`, `HEADER_SIZE`, `HEADER_MAGIC`
- `SHARD_HDR_FMT`, `SHARD_HDR_SIZE`, `SHARD_MAGIC`
- `SHARD_NODE_FMT`, `SHARD_NODE_SIZE`
- `FRONTIER_REF_FMT`, `FRONTIER_REF_SIZE`
- `HAS_PAYLOAD`, `HAS_CHILDREN`, `IS_FRONTIER`
- `LEVELS_PER_SHARD`
- `PAYLOAD_RECORD_SIZE`

And from `encoding.teff`:

- `TEFF_LO`, `TEFF_HI`, `TEFF_SENTINEL` (for dequantizing `teff_log8`)

No new binary constants are introduced. The reader is a consumer of the format defined by the write pipeline.

---

## Dependencies

The reader requires only packages already in `pyproject.toml`:

- `numpy` — distance calculations, payload dequantization
- `click` — CLI integration (already used by `_cli.py`)

No additional dependencies are needed. `rich` may optionally be added for table formatting in the `stats` command, but plain-text tables are acceptable.

---

## Error handling

- `read_header` must raise `ValueError` with a clear message if the file magic is wrong, the version is unsupported, or the shard probe fails.
- `IndexNavigator` must raise `ValueError` if a shard header has bad magic or a node read is truncated.
- `decode_payload` must return an empty list (not raise) if decompression fails on a single node, to allow the query to continue over other nodes. A warning should be logged.
- The `stats` CLI must raise `FileNotFoundError` with guidance if the octree file does not exist.

---

## Testing strategy

Unit tests should cover:

1. **Header parsing** — round-trip: pack a header with `pack_top_level_header`, write to bytes, parse with `read_header`, assert fields match.

2. **Payload decode** — construct a synthetic gzip'd payload of known star records, verify `decode_payload` returns correct world positions and magnitudes.

3. **Apparent magnitude** — verify `Star.apparent_magnitude_at` for known distance/magnitude pairs.

4. **AABB distance** — verify `NodeEntry.aabb_distance` for points inside, on the boundary, and outside the box.

5. **End-to-end** — build a small octree via the write pipeline (stages 00–02 with a tiny test catalog), then read it back with `OctreeReader` and verify both queries return expected stars. This test already has infrastructure in `tests/test_combine_e2e.py`.
