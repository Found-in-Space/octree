# STAR v2

STAR v2 adds terminal subtree packing and serialized payload star counts while
leaving the existing header, descriptor, shard header, payload codec, and star
record unchanged.

## Compatibility build policy

New project files default to:

```toml
[stage02]
star_format_version = 2
terminal_waterline = 1000
```

The command-line overrides are `--star-format-version {1,2}` and
`--terminal-waterline N`. Selecting version 1 disables terminal packing and
emits the existing byte-compatible STAR v1 index.

For v2, topology planning first writes immutable, content-addressed count runs
for each sorted input group and occupied octree level. Input is read in bounded
batches; each batch is vector-counted with NumPy and added to a levelled run
accumulator. Runs are reduced through a bounded fan-in merge, so memory and
open-file use do not grow with the catalogue. An unchanged group identity
reuses its published count files without reopening its Parquet input.

Per-level group runs are merged into immutable own-count runs. Subtree counts
are then derived bottom-up as sequential, node-ID-ordered files, and terminal
nodes are selected shallow-to-deep. Each group, aggregate level, node level,
and terminal plan has an atomic manifest and content identity, providing
restart checkpoints without a catalogue-scale mutable database. A source
change whose capped cell counts are unchanged can reuse the existing topology.

The merge uses fixed-size NumPy arrays rather than a Python record-at-a-time
heap. A bounded window from each already-sorted input is combined, sorted and
reduced in vectorized native code. This is preferable here to a DuckDB `GROUP
BY`: the inputs are already compact 16-byte sorted count records, so the merge
does not need to parse Parquet, build a general hash table, or create another
database-owned spill tree. DuckDB remains appropriate for analytical work, but
would discard the per-group immutability and fine-grained reuse that this
topology product relies on.

A node becomes terminal when:

- it has at least one natural descendant;
- its complete subtree, including its own payload, contains between one and
  `terminal_waterline` stars; and
- no shallower ancestor has already been selected.

This selects the shallowest eligible roots. Natural leaves are never marked
terminal. A node above the waterline keeps its own payload and descendants,
while eligible subtrees below it may still collapse.

All rows below a selected terminal are assigned to the terminal before render
encoding. Coordinates are therefore encoded directly against the terminal
geometry. The combined payload is ordered by absolute magnitude, then source
and source ID; null magnitudes sort last. `identifiers.order` uses the same
cell topology and ordinal order.

The waterline is build policy and is not serialized. `max_level` remains the
configured classic level cap even when no emitted node reaches that level.

## Shared index packing

STAR v1 and v2 use the same streaming index compiler. Intermediate manifests
carry a checksum of each ordered node-ID stream, and the topology-cache identity
combines those checksums with skeleton and terminal policy. It excludes payload
bytes, offsets, lengths, and star counts. Consequently, a v2 source change that
preserves terminal decisions and node presence reuses the same topology plan and
content-addressed skeleton packs even when payload counts or encoded bytes
change.

The default index emitter builds a dedicated scratch index and patches one
recorded frontier table per parent before copying the completed index forward
into the final artifact. The lower-scratch prefix-sum emitter writes the same
bytes directly. Neither performs catalogue lookup during index emission, and
the final artifact is never sought backwards.

## Binary layout

Both versions use:

- 64-byte STAR header: `<4sHHQQ3ffHHf16s>`
- 128-byte ODSC descriptor: `<4sHH16s16s16s32s40x>`
- 80-byte OSHR shard header: `<4sHBBIIHHHhIII8HHQQQ2x>`
- 8-byte frontier references: `<Q>`
- gzip-compressed payloads containing 16-byte star records: `<fffhBB>`

For a v2 artifact, both the STAR header version and every OSHR shard version
are `2`. Header and shard flags remain zero/reserved.

STAR v1 node records remain 20 bytes:

```text
<HHBBBBQI
```

STAR v2 node records are 24 bytes:

```text
<HHBBBBQII
```

The fields are:

| Field | Type | Meaning |
| --- | --- | --- |
| `first_child` | `u16` | First in-shard child index, or zero |
| `local_path` | `u16` | Morton path within the shard |
| `child_mask` | `u8` | Existing child octants |
| `local_depth` | `u8` | Depth relative to the shard parent |
| `flags` | `u8` | Node flags |
| `reserved` | `u8` | Zero |
| `payload_offset` | `u64` | Absolute compressed payload offset |
| `payload_length` | `u32` | Compressed payload bytes |
| `star_count` | `u32` | v2 only: records in this node's payload |

Node flags are:

- `HAS_PAYLOAD = 0x01`
- `HAS_CHILDREN = 0x02`
- `IS_FRONTIER = 0x04`
- `IS_TERMINAL = 0x08`

`star_count` is zero for index-only nodes, the node's own payload count for
ordinary payload-bearing nodes, and the complete collapsed-subtree count for a
terminal. A terminal must have a payload, `IS_TERMINAL`, and no children.

## Reader compatibility

The Python reader accepts STAR and shard versions 1 and 2 and rejects mixed
versions within an artifact. `NodeEntry.star_count` is `None` for v1 and an
integer for v2. `NodeEntry.is_terminal` reflects `IS_TERMINAL`; it is always
false for valid v1 artifacts.
