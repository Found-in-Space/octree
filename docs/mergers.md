# Mergers specification

## Purpose

Define how Gaia and Hipparcos inputs are merged into one canonical Stage 00 input dataset.

The merger resolves duplicates using a cross-match table and quality rules, then emits parquet compatible with existing Stage 00 and Stage 01 processing.

## Scope

This specification covers:

- when merge occurs in the pipeline
- required inputs and outputs
- duplicate-resolution rules
- schema contract for downstream sidecars

This specification does not cover:

- reverse/global lookup indices
- catalog-specific scientific quality policy details beyond tie-break contract

---

## Stage placement

Merge occurs **before Stage 00 processing**, as a pre-step:

`raw catalogs + crossmatch -> merged canonical parquet -> stage-00 -> stage-01`

### Rationale

- Merge requires cross-catalog joins that Stage 00 file-local transforms do not perform.
- Quality metrics needed for selection are available in raw catalog data.
- Merging earlier avoids deriving `morton_code`/`render` for rows that will be dropped.

---

## Inputs

Required:

- Gaia source parquet (or equivalent table)
- Hipparcos source parquet (or equivalent table)
- cross-match table mapping Gaia IDs and Hipparcos IDs

Optional:

- additional identifier tables (HD/Bayer/other naming catalogs)

---

## Canonical identity

Each merged output row must have one canonical `source_id` used by sidecars and forward lookups.

Policy:

- Gaia-selected rows: `source_id = gaia_source_id`
- Hipparcos-selected rows without Gaia counterpart: `source_id = hip_source_id` in canonical namespace

If a namespace prefix is required to avoid collisions, it must be applied consistently before Stage 00.

---

## Duplicate resolution

## Definitions

- **Matched pair**: one Gaia row and one Hipparcos row linked by cross-match
- **Unmatched Gaia**: Gaia row not present in cross-match
- **Unmatched Hipparcos**: Hipparcos row not present in cross-match

## Rules

### M1. Keep all unmatched rows

- Keep all unmatched Gaia rows.
- Keep all unmatched Hipparcos rows.

### M2. Exactly one winner per matched pair

For each matched pair, compute quality scores and select exactly one winner.

### M3. Deterministic tie-break

If quality scores tie, apply deterministic tie-break in this order:

1. prefer Gaia
2. lower canonical `source_id`

### M4. No duplicate canonical IDs

Merged output must not contain duplicate canonical `source_id`.

### M5. Auditable decision record

For each matched pair, emit or persist decision provenance with:

- `gaia_source_id`
- `hip_source_id`
- winner catalog
- score components (or aggregate score)
- tie-break reason if applicable

---

## Quality scoring contract

The exact scoring formula is configurable, but the merger must expose:

- versioned quality policy identifier
- deterministic numeric score per candidate
- reproducible output for the same input tables

Example signals (non-normative):

- parallax uncertainty metrics
- astrometric fit quality
- photometric quality flags

---

## Output schema contract

Merged parquet must satisfy Stage 00 requirements:

- `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`
- `mag_abs`
- optional `teff`

Merged parquet should also include identity/provenance fields required by sidecars:

- `source_id` (canonical, required)
- `gaia_source_id` (nullable)
- `hip_source_id` (nullable)
- `catalog_source` (`gaia` or `hip`)

Optional naming fields:

- `hd`
- `bayer`
- other sparse designations

All extra columns must survive Stage 00 unchanged (`SELECT *` behavior in shard sorting).

---

## Validation requirements

Before publishing merged parquet:

1. assert no duplicate canonical `source_id`
2. assert all matched pairs produce exactly one winner
3. assert row count equals:
   `unmatched_gaia + unmatched_hip + matched_pairs`
4. assert required geometry/magnitude columns are non-null where mandatory
5. emit merge summary counts by category and winner catalog

---

## Operational outputs

Merger run should produce:

- canonical merged parquet dataset (input to Stage 00)
- merge report JSON with counts and policy version
- optional decision table parquet for audits/debugging

---

## Non-normative notes

- A "merge plan now, apply later" strategy adds pipeline complexity with little benefit; current recommendation is direct pre-Stage-00 merge.
- Full reverse lookup indices remain downstream derived artifacts and are not part of merge output requirements.
