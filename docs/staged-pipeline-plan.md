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
- Stage 00 accepts both directory-based input shards and root-level parquet
  shard files. A shard may be a HEALPix pixel, a batch shard, or any other
  stable upstream rebuild unit.
- Stage 00 preserves the input shard id in output fragment names.
- Stage 00 reports group-level content checksums for current staged fragments.
- Stage 00 has been smoke-tested against a 31M-row real parquet shard.
- Stage 00 writes tree identity and mutable stage-state manifests.
- Stage 00 supports explicit shard replacement and dirty Stage 01 group
  tracking.
- Stage 01 sorts and compacts Stage 00 groups into replaceable sorted parquet
  groups while preserving `(staging_node, input_shard_id, kind)` granularity.
- The compatibility `stage-02` path materializes the traditional/classic
  level-capped output from tracked Stage 01 groups and writes `stars.octree`
  plus `identifiers.order` through the existing binary combine pipeline.
- The raw Cartesian input filter computes only `morton_code` and natural
  `level`; raw position, magnitude, and temperature fields remain available in
  Stage 00 and Stage 01.
- Classic materialization selects the capped final node and encodes its
  node-relative render record once. Precomputed Stage 01 `render` records are
  not part of the staged-row contract.

Not implemented yet:

- the alternative packed final-output variant
- profile-oriented Stage 03 assembly and manifests
- named Stage 03 output profiles
- dedicated sidecar builds per Stage 03 output profile
- removal of the obsolete Stage 02 command and old intermediate-shard pipeline

## Target Pipeline

| Stage | Role | Rebuild boundary | Output |
|---|---|---|---|
| Stage 00 | Partition input shards into staging buckets. | Input shard and staging node. | Raw staged parquet groups. |
| Stage 01 | Sort and compact staged groups. | Staging node, shard id, and fragment kind. | Canonical sorted staged groups. |
| Stage 03 | Assemble final output profiles. | Output profile and final octree node. | `stars.octree`, `identifiers.order`, and dedicated sidecars per profile. |

The stage split is intentional:

- Stage 00 does spatial placement once.
- Stage 01 makes staged input deterministic and compact.
- Stage 03 is where final renderer order becomes canonical and packaged.
- Stage 03 builds sidecars against the exact identity order of each output
  profile.

There is no Stage 02 in the target pipeline. The old Stage 02 command combined
intermediate shards into `stars.octree`; that responsibility moves into Stage
03. The old Stage 03 command built sidecars from global Stage 02 artifacts; that
becomes a per-output-profile Stage 03 subtask.

### Output Profiles

Stage 03 builds one or more named output profiles from the same Stage 01 sorted
groups. Each profile owns its render octree, identity-order artifact, manifest,
dataset UUID, and sidecars.

Required profiles:

- `classic`: cap final output at level 14 and preserve today's octree semantics
  and binary compatibility where practical.
- `unbounded`: allow final output through `MORTON_BITS` / level 21, relying on
  magnitude placement and Stage 00 packing so very deep nodes are rare.

Recommended output layout:

```text
stage03/classic/stars.octree
stage03/classic/identifiers.order
stage03/classic/sidecars/meta.octree

stage03/unbounded/stars.octree
stage03/unbounded/identifiers.order
stage03/unbounded/sidecars/meta.octree
```

Sidecar artifacts are never shared between profiles. A profile's node set and
identity order define the sidecar order, and each sidecar artifact must carry
that profile's parent dataset UUID. Sidecars are schema-bearing artifacts: a
reader should be able to open a sidecar, validate its parent dataset UUID when
joining with a render octree, inspect the embedded schema, and decode typed
records without knowing a separate sidecar "family" registry.

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
files. The upstream catalogue pipeline owns this granularity: if it wants
HEALPix-level rebuilds it should emit stable HEALPix shards; if it emits batch
files, the octree pipeline will rebuild and checksum at batch-shard granularity.

Stage 00 derives `input_shard_id` from the input directory name or root-level
parquet filename stem. It does not derive the rebuild boundary from row-level
HEALPix columns. It preserves row columns as supplied; enrichment or
normalization is allowed only through an explicitly configured pre-filter, and
that filter must preserve row count. The raw Cartesian filter adds routing
columns only. It does not replace raw coordinates with node-relative payload
coordinates.

### Canonical Ordering

Stage 01 is responsible for deterministic local ordering. Stage 03 is
responsible for deterministic final node ordering.

The Stage 01 sort key is:

```text
level, final_node_id, mag_abs, source, source_id
```

where:

```text
final_node_id = morton_code >> (3 * (MORTON_BITS - level))
```

If the renderer or sidecar builders need a different tie-breaker, the Stage 01
manifest/state version must change.

### Checksums

Checksums should be semantic, not raw parquet file checksums.

Recommended checksum layers:

- Stage 00 group checksum: canonical row content for one
  `(staging_node, input_shard_id, kind)` group.
- Stage 01 group checksum: canonical sorted content for that group.
- Stage 03 node checksum: final render payload bytes plus identity-order bytes
  for one profile node.
- Stage 03 artifact checksum: packaged output for one profile artifact.
- Stage 03 sidecar checksum: one schema-bearing sidecar artifact for one
  profile's identity order.

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
  "morton_bits": 21,
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
        "tree/o=1/o=6/shard-hp-449-pack-000001.parquet"
      ],
      "row_count": 100000,
      "content_checksum": "sha256:...",
      "dirty": false
    }
  },
  "stage01_groups": {
    "o=1/o=6|hp-449|pack": {
      "files": [
        "tree/o=1/o=6/shard-hp-449-sorted-000001.parquet"
      ],
      "row_count": 100000,
      "sorted_checksum": "sha256:...",
      "dirty": false
    }
  },
  "dirty": {
    "stage00_groups": [],
    "stage01_groups": [],
    "stage03_profiles": {
      "classic": {
        "nodes": []
      },
      "unbounded": {
        "nodes": []
      }
    }
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
- raw `pack` and `lim` parquet fragments containing source position,
  photometry, identity, and routing fields
- Stage 00 report
- tree identity manifest
- mutable stage state entries for affected groups

Modes:

- full rebuild: empty output tree or `--force`
- shard replace: delete and rebuild one or more input shards

### Stage 01

Inputs:

- Stage 00 tree
- tree identity manifest
- stage-state manifest
- dirty Stage 00 groups, or all groups for first run

Outputs:

- sorted compacted staged groups
- updated Stage 01 group checksums
- dirty Stage 03 node set, moving to per-profile dirty sets once Stage 03
  profiles are introduced

Stage 01 should not do a global catalogue sort. It should operate on local
groups. DuckDB may still be used as a local sorting engine, but the query scope
should be one group or one staging node, not the full tree.

Stage 01 behavior:

1. Walk Stage 00 groups.
2. For each dirty or unsorted group, read all fragment files.
3. Sort by the Stage 01 canonical key.
4. Write replacement sorted files to temp paths.
5. Compute sorted checksum.
6. Atomically swap files.
7. If checksum changed, mark affected final nodes dirty.

Stage 01 must preserve the raw fields needed to encode the final render record.
It sorts rows but does not create node-relative coordinates.

### Stage 03

Inputs:

- sorted Stage 01 groups
- stage-state manifest
- output profile config
- sidecar artifact definitions
- enrichment inputs for sidecars

Outputs:

- one output directory per profile
- `stars.octree` per profile
- `identifiers.order` per profile
- sidecar octrees per profile, such as `sidecars/meta.octree`
- profile manifest with node checksums, artifact checksums, dataset UUID,
  sidecar UUIDs, and sidecar schema descriptors

Stage 03 is where packed staging nodes are expanded into final render nodes.
Rows in a packed staging node may have deeper final `level` values. Stage 03
must route those rows to final node payloads for each output profile.

Profile behavior:

- `classic` clamps output to level 14. Rows with deeper final levels are
  materialized into the corresponding level-14 node and encoded relative to
  that selected node using the Stage 03 canonical order for that profile.
- `unbounded` materializes rows at their Stage 00 final levels through level
  21 and encodes them relative to those selected nodes.
- Both profiles can use the same sidecar definitions, but each profile writes
  its own sidecar artifacts because profile identity order can differ.

Dirty propagation:

- If a Stage 01 group checksum changes, map its rows to affected final nodes for
  each profile.
- Rebuild only those profile nodes when possible.
- If rebuilt Stage 03 node checksums are unchanged, the profile artifact can be
  left untouched.

Short-term packaging behavior:

- Stage 03 may fully repack a profile artifact from Stage 03 node outputs.
- Partial binary patching can be considered later, after the profile manifest
  and node checksum model are stable.

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
9. Stage 03 rematerializes dirty final nodes for each output profile.
10. Unchanged Stage 03 node checksums stop.
11. Stage 03 repacks or patches profile artifacts only if required.
12. Stage 03 rebuilds sidecar artifacts only for profiles/sidecars whose inputs
    changed.

Example: change render payload encoding but not placement.

1. Stage 00 is unchanged.
2. Stage 01 ordering is unchanged.
3. Stage 03 rematerializes affected profile nodes from Stage 01 rows.
4. Stage 03 repacks the profile render artifact.
5. Stage 03 sidecars rebuild only if they consume changed values.

Example: change magnitude limit or max level.

1. Tree identity changes.
2. Existing Stage 00 tree is invalid.
3. Full Stage 00 rebuild is required.

## Work Streams

### A. Stage 00 Durability

Goal: make Stage 00 output reusable, replaceable, and comparable.

Status: implemented.

Remaining cleanup:

- keep Stage 00 state compatible with Stage 03 profile dirty sets as those are
  introduced

### B. Stage 01 Rewrite

Goal: replace global Stage 01 assembly with local sort and compaction.

Status: implemented.

Remaining cleanup:

- update Stage 01 dirty output from a single `stage03_nodes` list to per-profile
  dirty sets once Stage 03 profile configs exist

### C. Stage 03 Profiles And Final Assembly

Goal: build final render, identity-order, and sidecar artifacts from sorted
Stage 01 groups for each configured output profile.

Tasks:

- define output profile config, including `classic` level-14 cap and
  `unbounded` level-21 output
- define Stage 03 profile manifest schema
- map Stage 01 final nodes to profile nodes, including classic level clamping
- merge sorted `(node, shard, kind)` groups into profile node payload and
  identity order
- build profile `stars.octree` and `identifiers.order`
- build each configured sidecar artifact into the profile directory
- embed each sidecar's schema/descriptor in the sidecar artifact
- compute per-node, artifact, identity-order, and sidecar checksums
- preserve dataset UUID and descriptor metadata per profile
- verify classic profile reader compatibility against today's output semantics

### D. Deletion And Project Model Cleanup

Goal: remove obsolete old-pipeline concepts and make the new process hard to
misuse.

Delete or replace:

- `stage-02` CLI command
- `[stage02]` project config and `stage02.max_open_files`
- old Stage 01 intermediate-shard builder path, including `assembly/build.py`
  and `assembly/row_source.py`
- old Stage 03 sidecar builder that assumes global `stage02_output_path` and
  global `identifiers.order`
- old tests that only cover the removed Stage 02 command
- old assembly/combine helpers that are not reused by the new Stage 03 packer

Tasks:

- add project output-profile config
- make `stage-03` build one or all configured profiles
- make command output report reused vs rebuilt work per profile and sidecar
- expose plan/dry-run commands
- keep project config strict about build-defining identity

## Suggested Implementation Order

1. Define project output-profile config and default `classic` / `unbounded`
   profiles.
2. Define Stage 03 profile manifest and state schema.
3. Implement profile node mapping from Stage 01 groups, including classic
   level-14 clamping and unbounded level-21 output.
4. Implement profile `stars.octree` and `identifiers.order` assembly with full
   repack.
5. Implement dedicated sidecar build per profile.
6. Remove the obsolete Stage 02 CLI/config/tests and old global sidecar path.
7. Add dirty-only Stage 03 rebuild and per-profile reuse reporting.
8. Optional: add partial artifact patching after full-repack Stage 03 is stable.

This order keeps each step testable and avoids designing partial binary patching
before profile manifests and node checksums are proven.

## Validation Plan

Unit tests:

- same shard input produces same Stage 00 semantic checksums
- changed input shard dirties only changed groups
- unchanged Stage 01 sorted checksum stops propagation
- Stage 03 maps packed staging rows to profile nodes correctly
- classic profile clamps rows deeper than level 14 into level-14 nodes
- unbounded profile preserves final levels through level 21
- sidecar parent UUID validation continues to work per profile

Integration tests:

- build one real input shard through Stage 00
- replace it with identical input and confirm no dirty propagation
- replace it with one changed row and confirm limited dirty propagation
- full classic build from scratch equals classic build after incremental
  replacement
- full unbounded build from scratch equals unbounded build after incremental
  replacement

Operational checks:

- report rows in equals rows current at Stage 00
- report reused vs rebuilt groups and profile nodes at every stage
- report dirty sets before and after each stage
- fail fast on tree identity mismatch

## Open Decisions

- Stage 03 profile manifest shape.
- Stage 03 intermediate node-output grouping before packaging.
- Exact sidecar schema descriptor shape and where it lives in the sidecar
  artifact header/manifest.
- Whether partial binary patching is worth the complexity after full-repack
  Stage 03 is stable.
- Whether the compatibility `--healpix` alias should remain long term once
  `--shard` is the documented option.

## Glossary

Input shard:
: One upstream parquet unit that can be replaced independently. Usually a
  HEALPix pixel, but older pipeline outputs may be batch-sharded. Stage 00 keys
  checksums and replacement by this shard id, not by row-level HEALPix columns.

Staging node:
: A node in the adaptive Stage 00 filesystem tree. It may hold rows for final
  nodes below its own depth.

Final node:
: The renderer-visible octree node determined by a row's final `level` and
  `morton_code`.

Output profile:
: A named Stage 03 build target with its own max output level, node set,
  `stars.octree`, `identifiers.order`, sidecars, manifest, and dataset UUID.

Packed group:
: A Stage 00 group whose rows are held at a staging node shallower than some
  rows' final nodes.

Lower-mag-limited group:
: A Stage 00 group resident at a staging node that has crossed the row cap and
  routes deeper rows to child staging nodes.

Semantic checksum:
: A checksum over canonical row content or canonical output bytes, independent
  of physical parquet metadata and incidental file boundaries.
