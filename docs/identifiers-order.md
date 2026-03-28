# Identifiers / Order Artifact

## Status

This document describes a recommended future artifact for the octree base dataset package.

It is not part of the current v1 pipeline yet.

## Purpose

Define a foundational Stage 2 companion artifact that preserves the canonical per-cell star ordering for one render octree dataset.

Its primary role is:

- input to future sidecar-family builds

Secondary roles:

- source for reverse lookup index generation
- source for validation and audit tools

## Why This Exists

Today, if we want to build a new sidecar after the render octree is already published, we still depend on earlier pipeline outputs.

A canonical identifiers/order artifact would let us archive:

- `stars.octree`
- one compact companion artifact that preserves canonical star identity and order

and then build later sidecars from that base package without keeping all Stage 00 and Stage 01 files around.

## Primary Mapping

The primary mapping should be:

- `(level, node_id) -> ordered list of canonical star identities`

This is a forward mapping optimized for sidecar generation.

It should not be optimized first for:

- `star_id -> node`
- free-text search
- UI lookup ranking

Those are better handled as derived artifacts built from this one.

## Canonical Identity

Each ordered star entry should preserve canonical star identity in a stable form.

The minimum useful identity is:

- `source`
- `source_id`

That matches the current render/sidecar alignment contract and is sufficient to join later enrichment tables.

## Stage Placement

Recommended future placement:

- Stage 00: row enrichment parquet
- Stage 01: render intermediates and render manifest
- Stage 02: final render octree plus this identifiers/order artifact
- Stage 03: optional sidecar families and derived indices built from the Stage 2 base package

Why Stage 2:

- the artifact is foundational enough to belong with the base dataset package
- later sidecar builds should not depend on old intermediate files when the base package can carry the canonical ordering directly

## Design Goals

- fast sequential cell-by-cell scans
- bounded-memory sidecar generation
- compact binary representation
- compatibility and cache safety via UUIDs
- no dependence on JSON parsing for core build loops

## Non-goals

- replacing `stars.octree`
- serving as the primary reverse lookup index
- packing arbitrary metadata fields
- becoming another generic `meta` artifact

## Recommended File Structure

One workable structure is:

1. file header
2. fixed-width cell directory
3. payload section with ordered canonical identities

### File header

The header should include at least:

- artifact magic
- artifact format version
- `parent_dataset_uuid`
- artifact UUID for this identifiers/order artifact
- directory offset
- directory length
- payload offset
- payload length
- codec or compression flags
- source-kind dictionary version or equivalent identifier

### Cell directory

The directory should be sorted by `(level, node_id)` so sidecar builders can stream the file in the same logical order used by render cells.

Each fixed-width directory record should include at least:

- `level`
- `node_id`
- `star_count`
- `payload_offset`
- `payload_length`

Optional useful fields:

- flags
- per-cell digest
- block identifier when block compression is used

### Payload section

Each payload blob should store the ordered canonical identities for exactly one cell.

Recommended representation:

- compact binary records
- one record per star, in render order
- optional cell-local string pool for non-numeric identifiers

Avoid using per-cell gzip JSON for this artifact. The main goal is efficient rebuild input, not human readability.

## Recommended Identity Encoding

One workable approach is:

- compact source-kind tag
- compact id-kind tag
- numeric payload for common numeric identifiers
- string-pool reference for non-numeric or manual identifiers

That keeps Gaia/HIP-like cases compact while still supporting manual or other string identifiers.

Exact binary packing can be decided later, but the important design point is:

- optimize for compact ordered identity storage, not for arbitrary metadata extensibility

## Compression Strategy

Recommended order of preference:

1. uncompressed binary for the simplest and fastest initial implementation
2. block compression if file size becomes a problem

Avoid per-cell gzip as the default because it adds CPU overhead to the exact streaming path this artifact is supposed to make cheap.

## Why This Is Better Than A Generic “Meta Meta” File

If the artifact preserves canonical ordered identities, it is not a hacky extra sidecar. It is a first-class foundational companion artifact for the render dataset.

That is cleaner because:

- it has one crisp responsibility
- it can support many future sidecar families
- it can support reverse lookup index generation
- it avoids coupling future tooling to the current `meta` family

## Reverse Lookup As A Derived Artifact

Reverse lookup should remain a derived artifact built from the identifiers/order artifact.

That build can emit:

- `canonical_id -> (level, node_id, ordinal)`

This keeps the foundational artifact optimized for sidecar generation rather than trying to optimize both forward and reverse lookup equally.

## Sidecar Build Flow

The intended Stage 3 flow becomes:

1. open `stars.octree`
2. open the identifiers/order artifact
3. stream one cell at a time
4. join external enrichment by canonical star identity
5. write one sidecar cell for the chosen family
6. emit a new `sidecar_uuid`

## Cache Identity

Recommended cache identity:

- render octree: `dataset_uuid`
- identifiers/order artifact: `parent_dataset_uuid` plus artifact UUID
- derived sidecars: `parent_dataset_uuid`, sidecar kind, `sidecar_uuid`

## Relationship To `meta`

`meta` should remain the name of the current sidecar family.

This identifiers/order artifact is not the `meta` sidecar and should not inherit `meta` naming.

## Related Docs

- `docs/roadmap.md`
- `docs/stage-02.md`
- `docs/sidecars.md`
