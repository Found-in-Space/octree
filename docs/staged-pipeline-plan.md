# Staged Pipeline Plan

This is the coordination document for the new staged octree pipeline. It tracks
the target design, implementation order, invariants, and open decisions while we
move from the current compatibility pipeline to an incremental build pipeline.

The core goal is simple: if a small part of the upstream catalogue changes, the
octree build should reuse every staged, materialized, and packaged result whose
semantic content did not change.

## Current Status

Implemented on the current work branch:

- Stage 00 can build an adaptive octree-shaped staging tree.
- Stage 00 accepts both directory-based HEALPix inputs and root-level parquet
  shard files.
- Stage 00 preserves the input shard id in output fragment names.
- Stage 00 reports group-level content checksums for current staged fragments.
- Stage 00 has been smoke-tested against a 31M-row real parquet shard.
- The current Stage 01, Stage 02, and Stage 03 commands still use the older
  compatibility path.

Not implemented yet:

- tree identity manifests
- mutable stage-state manifests
- persisted stage-state checksums for staged row groups
- replace-one-shard Stage 00 mode
- rewritten Stage 01 local sort and compaction
- dirty propagation from Stage 01 into materialized final nodes
- Stage 03 materialized payload-order files
- Stage 04 packaging from Stage 03 byte ranges
- partial Stage 04 artifact replacement

## Target Pipeline

| Stage | Role | Rebuild boundary | Output |
|---|---|---|---|
| Stage 00 | Partition input shards into staging buckets. | Input shard and staging node. | Raw staged parquet groups. |
| Stage 01 | Sort and compact staged groups. | Staging node, shard id, and fragment kind. | Canonical sorted staged groups. |
| Stage 02 | Rewrite payload bytes without changing placement. | Same as Stage 01. | Updated payload columns or fragments. |
| Stage 03 | Materialize final node payload order. | Final octree node or materialized node group. | Raw payload-order byte arrays plus identity order. |
| Stage 04 | Pack final base artifacts. | Final package or packable byte ranges. | `stars.octree` and `identifiers.order`. |
| Stage 05 | Build derived sidecars. | Sidecar family and affected identities. | Sidecar octrees such as `meta.octree`. |

The stage split is intentional:

- Stage 00 does spatial placement once.
- Stage 01 makes staged input deterministic and compact.
- Stage 03 is where final renderer order becomes canonical.
- Stage 04 is packaging, not indexing.
- Stage 05 depends on stable identity order, not on Stage 00 parquet.

## Invariants

These invariants should be enforced by manifests and tests.

### Stage 00 Placement

For a fixed input shard and fixed build identity, Stage 00 must produce the same
semantic staged row groups every time.

The physical parquet files are allowed to differ in metadata or row group
layout. The semantic content is what matters:

```text
(staging_node, input_shard_id, kind) -> rows
```

`kind` is currently:

- `pack`: rows held in a packed staging node that may include deeper final
  levels
- `lim`: rows resident at a lower-mag-limited staging node

### Shard Replaceability

An input shard must be replaceable without deleting unrelated shard data:

1. Find all Stage 00 groups with `input_shard_id`.
2. Delete those fragments.
3. Run Stage 00 for the new shard file.
4. Compare old and new group checksums.
5. Mark only changed groups dirty.

This is true for HEALPix pixel files and also for older batch-sharded parquet
files. The pipeline should use the generic term `input_shard_id` internally,
even when CLI options still say `--healpix`.

### Canonical Ordering

Stage 01 is responsible for deterministic local ordering. Stage 03 is
responsible for deterministic final node ordering.

The provisional Stage 01 sort key is:

```text
level, final_node_id, mag_abs, source, source_id
```

where:

```text
final_node_id = morton_code >> (3 * (MORTON_BITS - level))
```

This key should be reviewed before implementation. If the renderer or sidecar
builders need a different tie-breaker, the manifest version must change.

### Checksums

Checksums should be semantic, not raw parquet file checksums.

Recommended checksum layers:

- Stage 00 group checksum: canonical row content for one
  `(staging_node, input_shard_id, kind)` group.
- Stage 01 group checksum: canonical sorted content for that group.
- Stage 03 node checksum: final payload-order bytes plus identity-order bytes
  for one final node or materialized node group.
- Stage 04 artifact checksum: packaged output or packable byte range.
- Stage 05 sidecar checksum: sidecar family output for the affected identity
  order.

If a stage recomputes the same semantic checksum, dirty propagation stops there.

## Manifests

Use two classes of manifests.

### Tree Identity Manifest

The tree identity manifest is immutable for an existing tree. Any setting that
can move a star to another staging node or final node belongs here.

Proposed path:

```text
stage00/tree-manifest.json
```

Proposed fields:

```json
{
  "format": "foundinspace.octree.stage-tree/v0",
  "source_dataset": {
    "name": "...",
    "version": "...",
    "input_kind": "healpix|flat-shard"
  },
  "coordinate_frame": "icrs",
  "world_center": [0.0, 0.0, 0.0],
  "world_half_size_pc": 32768.0,
  "morton_bits": 14,
  "max_level": 14,
  "mag_level": {
    "v_mag": 6.5
  },
  "stage00": {
    "bucket_size": 1000000,
    "split_policy": "lower-mag-limited-at-row-cap",
    "row_schema_version": 0
  },
  "ordering": {
    "stage01_sort_key": "level,final_node_id,mag_abs,source,source_id"
  }
}
```

Stage 00 must refuse to append or replace in a tree if the requested project
does not match this identity.

### Mutable Stage State

Mutable state tracks what has been written, what is dirty, and what can be
reused.

Proposed path:

```text
stage00/stage-state.json
```

The exact shape can evolve, but it should track:

- input shards seen by Stage 00
- Stage 00 groups and checksums
- Stage 01 sorted groups and checksums
- dirty Stage 00 groups
- dirty Stage 01 groups
- dirty Stage 03 final nodes
- stage versions that produced each entry

Sketch:

```json
{
  "format": "foundinspace.octree.stage-state/v0",
  "input_shards": {
    "hp-449": {
      "source_path": "...",
      "source_size": 123,
      "source_mtime_ns": 123,
      "stage00_groups": [
        "o=1/o=6|hp-449|pack"
      ]
    }
  },
  "stage00_groups": {
    "o=1/o=6|hp-449|pack": {
      "node_path": "o=1/o=6",
      "input_shard_id": "hp-449",
      "kind": "pack",
      "files": [
        "tree/o=1/o=6/hphp-449-pack-000001.parquet"
      ],
      "row_count": 100000,
      "content_checksum": "sha256:...",
      "dirty": false
    }
  },
  "stage01_groups": {
    "o=1/o=6|hp-449|pack": {
      "files": [
        "tree/o=1/o=6/hphp-449-sorted-000001.parquet"
      ],
      "row_count": 100000,
      "sorted_checksum": "sha256:...",
      "dirty": false
    }
  },
  "dirty": {
    "stage00_groups": [],
    "stage01_groups": [],
    "stage03_nodes": []
  }
}
```

The manifest should be updated with atomic temp-file writes.

## Stage Contracts

### Stage 00

Inputs:

- project config
- one or more input shard parquet files
- optional existing stage tree

Outputs:

- adaptive staging tree under `stage00/tree`
- raw `pack` and `lim` parquet fragments
- Stage 00 report
- tree identity manifest
- mutable stage state entries for affected groups

Modes:

- full rebuild: empty output tree or `--force`
- shard replace: delete and rebuild one or more input shards
- dry scan: report which shards would be processed and which settings mismatch

Next implementation tasks:

- write tree identity manifest
- write stage-state manifest
- compute Stage 00 group content checksums
- add shard replacement mode
- rename internal `healpix` concepts to `input_shard` where practical

### Stage 01

Inputs:

- Stage 00 tree
- tree identity manifest
- stage-state manifest
- dirty Stage 00 groups, or all groups for first run

Outputs:

- sorted compacted staged groups
- updated Stage 01 group checksums
- dirty Stage 03 node set

Stage 01 should not do a global catalogue sort. It should operate on local
groups. DuckDB may still be used as a local sorting engine, but the query scope
should be one group or one staging node, not the full tree.

New Stage 01 MVP:

1. Walk Stage 00 groups.
2. For each dirty or unsorted group, read all fragment files.
3. Sort by the Stage 01 canonical key.
4. Write replacement sorted files to temp paths.
5. Compute sorted checksum.
6. Atomically swap files.
7. If checksum changed, mark affected final nodes dirty.

### Stage 02

Stage 02 exists only for payload byte changes that do not affect placement or
ordering.

It should refuse changes that affect:

- `morton_code`
- `level`
- `mag_abs` if used by sort order
- source identity tie-breakers

This stage can be deferred until Stage 01 and Stage 03 boundaries are stable.

### Stage 03

Inputs:

- sorted Stage 01 groups
- dirty final-node set

Outputs:

- raw payload-order byte arrays
- identity-order byte arrays or side files
- manifest of final node byte ranges
- per-node checksums

Stage 03 is where packed staging nodes are expanded into final render nodes.
Rows in a packed staging node may have deeper final `level` values. Stage 03
must route those rows to the final node payloads.

Dirty propagation:

- If a Stage 01 group checksum changes, map its rows to affected final nodes.
- Rebuild only those final node ranges.
- If rebuilt Stage 03 checksums are unchanged, Stage 04 does not need to run for
  those nodes.

### Stage 04

Inputs:

- Stage 03 byte arrays
- Stage 03 identity order
- Stage 03 manifest

Outputs:

- `stars.octree`
- `identifiers.order`

Stage 04 should not read Stage 00 or Stage 01 parquet during a normal build.

Open question:

- Can `stars.octree` support localized replacement, or should Stage 04 always
  repack the full artifact from Stage 03 outputs?

The short-term answer can be full repack. That still avoids repeating Stage 00
and Stage 01 work.

### Stage 05

Inputs:

- Stage 04 base dataset
- `identifiers.order`
- sidecar family config
- enrichment inputs

Outputs:

- sidecar octrees such as `meta.octree`

Stage 05 should rebuild by sidecar family. If the base identity order is
unchanged, sidecars can be rebuilt without touching Stage 00 through Stage 04.

## Change Propagation

Example: edit one star in one input shard.

1. Upstream pipeline rewrites the input shard.
2. Stage 00 replace mode deletes old fragments for that shard.
3. Stage 00 writes new fragments and computes group checksums.
4. Unchanged Stage 00 group checksums stop.
5. Changed Stage 00 groups mark matching Stage 01 groups dirty.
6. Stage 01 sorts only dirty groups.
7. Unchanged Stage 01 checksums stop.
8. Changed Stage 01 groups mark affected Stage 03 final nodes dirty.
9. Stage 03 rematerializes dirty final nodes.
10. Unchanged Stage 03 node checksums stop.
11. Stage 04 repacks or patches only if required.
12. Stage 05 rebuilds sidecars only if their inputs changed.

Example: change render payload encoding but not placement.

1. Stage 00 is unchanged.
2. Stage 01 ordering is unchanged.
3. Stage 02 rewrites payload bytes.
4. Stage 03 rematerializes affected payload-order ranges.
5. Stage 04 repacks the base artifact.
6. Stage 05 sidecars rebuild only if they consume changed values.

Example: change magnitude limit or max level.

1. Tree identity changes.
2. Existing Stage 00 tree is invalid.
3. Full Stage 00 rebuild is required.

## Work Streams

### A. Stage 00 Durability

Goal: make Stage 00 output reusable, replaceable, and comparable.

Tasks:

- add tree identity manifest
- add mutable stage-state manifest
- promote report-level group checksums into stage state
- implement shard replacement mode
- add tests for deterministic rebuild of the same shard
- add tests for changed shard marking only changed groups dirty

### B. Stage 01 Rewrite

Goal: replace global Stage 01 assembly with local sort and compaction.

Tasks:

- define sorted fragment filename convention
- implement local group scan
- implement local sort and compact
- implement sorted checksums
- mark affected Stage 03 nodes dirty
- keep current old Stage 01 available under a temporary compatibility path if
  needed

### C. Stage 03 Materialization

Goal: materialize final node payload and identity order from sorted staged data.

Tasks:

- define Stage 03 manifest schema
- define materialized byte array layout
- implement merge of sorted `(node, shard, kind)` groups
- fan out packed staging rows to final nodes
- write payload ranges and identity ranges
- compute per-node checksums

### D. Stage 04 Packaging

Goal: package Stage 03 outputs into final base artifacts without re-indexing.

Tasks:

- build `stars.octree` from Stage 03 ranges
- build `identifiers.order` from Stage 03 identity order
- decide full repack vs partial patching
- preserve UUID and descriptor metadata
- verify reader compatibility

### E. Stage 05 Sidecars

Goal: move sidecar builds behind the Stage 04 dataset boundary.

Tasks:

- use `identifiers.order` as the canonical order source
- build sidecars by family
- record sidecar manifests and checksums
- rebuild sidecar families independently

### F. CLI And Project Model

Goal: make the new process operable and hard to misuse.

Tasks:

- add commands for dirty-only operation
- add replace-shard options
- expose plan/dry-run commands
- keep project config strict about build-defining identity
- make command output report reused vs rebuilt work

## Suggested Implementation Order

1. Stage 00 manifests and semantic checksums.
2. Stage 00 shard replacement.
3. Stage 01 local sort and compaction MVP.
4. Stage 01 dirty-only mode.
5. Stage 03 materialization format and manifest.
6. Stage 04 full repack from Stage 03.
7. Stage 05 rebuild from Stage 04 identity order.
8. Optional Stage 02 payload re-encoding.
9. Optional Stage 04 partial patching.

This order keeps each step testable and avoids designing partial artifact
patching before the reusable staging layers are proven.

## Validation Plan

Unit tests:

- same shard input produces same Stage 00 semantic checksums
- changed input shard dirties only changed groups
- unchanged Stage 01 sorted checksum stops propagation
- Stage 03 maps packed staging rows to final nodes correctly
- sidecar parent UUID validation continues to work

Integration tests:

- build one real input shard through Stage 00
- replace it with identical input and confirm no dirty propagation
- replace it with one changed row and confirm limited dirty propagation
- full build from scratch equals build after incremental replacement

Operational checks:

- report rows in equals rows current at Stage 00
- report reused vs rebuilt groups at every stage
- report dirty sets before and after each stage
- fail fast on tree identity mismatch

## Open Decisions

- Exact Stage 01 sort key.
- Whether Stage 00 should produce content checksums while writing or in a
  separate final scan.
- Whether sorted Stage 01 files replace raw Stage 00 files in place or live in a
  parallel directory.
- Stage 03 byte array grouping: one file per final level, per subtree, or per
  materialized shard.
- Whether Stage 04 partial patching is worth the complexity for the first new
  format release.
- Whether `--healpix` should be renamed to `--shard` with `--healpix` retained
  as an alias.

## Glossary

Input shard:
: One upstream parquet unit that can be replaced independently. Usually a
  HEALPix pixel, but older pipeline outputs may be batch-sharded.

Staging node:
: A node in the adaptive Stage 00 filesystem tree. It may hold rows for final
  nodes below its own depth.

Final node:
: The renderer-visible octree node determined by a row's final `level` and
  `morton_code`.

Packed group:
: A Stage 00 group whose rows are held at a staging node shallower than some
  rows' final nodes.

Lower-mag-limited group:
: A Stage 00 group resident at a staging node that has crossed the row cap and
  routes deeper rows to child staging nodes.

Semantic checksum:
: A checksum over canonical row content or canonical output bytes, independent
  of physical parquet metadata and incidental file boundaries.
