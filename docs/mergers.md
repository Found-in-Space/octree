# Mergers specification

The authoritative mergers specification lives in the **pipeline repository** (`found-in-space-pipeline/docs/mergers.md`). This document summarises the invariants that the octree builder depends on.

---

## Canonical identity

Each merged row is identified by the compound key `(source, source_id)`.

- **Matched pairs** (Gaia + Hipparcos linked by crossmatch): `source = "hip"`, `source_id = <hip_number>`.
- **Gaia-only**: `source = "gaia"`, `source_id = <gaia_dr3_id>`.
- **Hipparcos-only**: `source = "hip"`, `source_id = <hip_number>`.
- **Manual adds**: `source = "manual"`, `source_id = <manual_id>` (e.g. `"sun"`).

The dense merged table does **not** carry separate `gaia_source_id`, `hip_source_id`, or `catalog_source` columns. Cross-catalog identifiers are stored in the **identifiers sidecar** (`identifiers_map.parquet`), keyed by `(source, source_id)`.

---

## Merged output schema (Stage 00 input)

The columns available to Stage 00 and the octree builder:

- `source`, `source_id` — canonical identity
- `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` — sun-centric ICRS Cartesian (parsecs)
- `mag_abs` — absolute magnitude
- `teff` — effective temperature (K)
- `quality_flags`, `astrometry_quality`, `photometry_quality`
- `ra_deg`, `dec_deg`, `r_pc` — angular position and distance

Stage 00 enrichment adds `morton_code`, `render`, `level` and preserves all original columns via `SELECT *`.

---

## Identifiers sidecar

`identifiers_map.parquet` is a wide table keyed by `(source, source_id)` containing:

- `gaia_source_id` — Gaia DR3 source ID (for HIP stars, from crossmatch)
- `hip_id` — Hipparcos catalog number
- `hd` — Henry Draper catalog number
- `bayer` — Bayer designation (e.g. `"alpha Cen"`)
- `flamsteed` — Flamsteed number
- `constellation` — constellation abbreviation
- `proper_name` — IAU proper name

Manual override stars with an `identifiers` block in their YAML are included in the same map.

The octree sidecar builder loads this map once at build start and uses it for per-star identifier enrichment during the Stage 01 streaming pass. Each metadata JSON entry still always includes the star’s canonical `source` and `source_id` from the Stage 00 row (see `docs/sidecars.md`); the map only adds optional display / cross-catalog fields.
