# Glow Octree

## Purpose

The glow octree is a spatial aggregate index carried alongside `stars.octree`. Each node stores additive summary statistics — star count, integrated luminosity, luminosity-weighted colour, and estimated mass — so that the renderer can treat unresolved cells as emissive volumes and downstream tools can query aggregate physical properties per spatial region.

`stars.octree` is a discrete-source structure: per-star payloads, magnitude-driven level assignment, point-based shell-loader query. It is the right representation for rendering individual stars but not for diffuse unresolved light at distance, nor for representing aggregate physical properties like total mass or luminosity in a volume.

The glow octree uses the same world geometry and shard/index infrastructure but changes two things:

1. **Payload semantics**: each node stores additive aggregate statistics, not a list of individual stars.
2. **Build model**: pure spatial binning with bottom-up reduction, not magnitude-driven level assignment.

### One-sentence definition

The glow octree is a sibling octree product that stores aggregate stellar statistics per spatial cell — luminosity, colour, mass, and count — enabling unresolved integrated emission rendering and volumetric physical queries.

---

## Goal

Support a smooth transition from resolved individual sources (nearby) to integrated aggregate emission (distant), and provide a lightweight volumetric representation of stellar mass and luminosity density.

Specific use cases:

- **Unresolved glow rendering**: stop octree traversal when a node is small enough on screen; render its integrated colour and intensity as a single emissive element.
- **Emissivity fields**: treat the octree as a volumetric luminosity density field for path tracing or volume rendering.
- **Mass density**: use aggregate mass per cell for gravitational lensing approximations, n-body seeding, or educational visualisation.

In practical terms:

- nearby structure continues to refine to child nodes and eventually to individual stars (from `stars.octree`)
- distant structure is rendered as integrated glow from coarser nodes
- bright stars that already appear higher in the star octree may still render as explicit sources, while deeper unresolved structure contributes only a faint local background

---

## Relationship to existing products

The glow octree is a **sibling product** of `stars.octree`, not a sidecar or extension:

- It consumes the same merged HEALPix parquet input.
- It uses the same world geometry (`WORLD_CENTER`, `WORLD_HALF_SIZE_PC`).
- It reuses shared Morton/grid helpers and the Stage 02 combine infrastructure.
- It produces a separate `glow.octree` file.

It is **not** a sidecar because its spatial structure differs from the star octree. The glow octree uses pure spatial binning (all stars assigned to leaf cells at `max_level`), while `stars.octree` uses magnitude-driven level assignment. The cell sets are therefore different, and sidecar alignment is not possible.

| Property | `stars.octree` | `glow.octree` |
|---|---|---|
| Payload | per-star 16-byte records | per-node 32-byte aggregate |
| Level assignment | magnitude-driven | pure spatial (all stars at `max_level`) |
| Parent semantics | no parent aggregation | parent = sum of children |
| Query model | shell loader (apparent magnitude) | frustum + projected size |

---

## Upstream pipeline changes

### Required: add `log_g` to merged output

The Gaia pipeline already computes `log_g` from the ESP-HS / GSP-Spec / GSP-Phot cascade (`compute_log_g_gaia` in `gaia/photometry.py`), but the call is currently commented out in `gaia/pipeline.py` and `log_g` is absent from `OUTPUT_COLS`.

To enable surface-gravity-based mass estimation in the glow octree, the following changes are needed:

1. **`pipeline/constants.py`**: add `"log_g"` to `OUTPUT_COLS`.
2. **`pipeline/gaia/pipeline.py`**: uncomment `work = compute_log_g_gaia(work)`.
3. **`pipeline/hipparcos/pipeline.py`**: ensure `compute_log_g_hip(work)` runs (it sets `log_g = NaN` for all HIP rows; safe and consistent).

Hipparcos has no spectroscopic log_g source, so HIP stars carry `log_g = NaN` and fall back to the mass-luminosity relation at glow-build time.

This is a small, low-risk change. `log_g` is also independently useful beyond glow (HR diagrams, spectral classification, evolutionary state identification).

### No other pipeline changes needed

Luminosity, colour weights, and the mass-luminosity fallback can all be derived at glow-build time from existing merged columns (`mag_abs`, `teff`) plus the newly added `log_g`.

Gaia FLAME parameters (`mass_flame`, `lum_flame`) would improve mass and luminosity estimates for a subset of stars but would require changing the upstream TAP query. This is not required for V1 and can be revisited once the glow octree is operational.

---

## Per-star derived quantities

The glow build computes four derived quantities per star from the merged parquet columns. These are computed at build time, not stored in the merge output.

### Luminosity

Bolometric luminosity in solar units:

```
BC     = bolometric_correction(teff)
M_bol  = mag_abs + BC
L_star = 10^((M_bol_sun - M_bol) / 2.5)
```

Where `M_bol_sun = 4.74` (IAU 2015 Resolution B2).

The bolometric correction `BC(Teff)` uses a polynomial fit such as Torres (2010) or Flower (1996), evaluated from the pipeline's `teff` column. When `teff` is the 5800 K default (unknown temperature), `BC ≈ −0.07`, and the resulting luminosity is a reasonable approximation for solar-type stars.

Stars with non-finite `mag_abs` are excluded from aggregation.

### Colour weight

Luminosity-weighted linear RGB triplet:

```
r_lin, g_lin, b_lin = teff_to_linear_rgb(teff)
lum_r = L_star * r_lin
lum_g = L_star * g_lin
lum_b = L_star * b_lin
```

The `teff_to_linear_rgb` function must produce values in **linear light space** (not sRGB gamma-encoded). The existing `teff_to_rgb` in `pipeline/common/photometry.py` returns sRGB 0-255 values using the Tanner Helland algorithm. The glow build must apply sRGB-to-linear conversion:

```
linear = (srgb / 255.0)^2.2
```

or use the piecewise sRGB transfer function for precision. The glow build and the discrete-star renderer must use the same underlying Teff-to-colour mapping to ensure visual continuity at the transition between resolved and unresolved rendering.

### Mass estimate

Estimated stellar mass in solar units, using a two-tier approach:

**Tier 1 — surface gravity (when `log_g` is finite and in [0, 9])**:

```
R_star = sqrt(L_star * L_sun / (4π σ T_eff⁴))
M_star = 10^(log_g) * R_star² / G
```

Where σ is the Stefan-Boltzmann constant and G is the gravitational constant, all in consistent CGS units. This gives physically grounded mass estimates for the tens of millions of Gaia stars with spectroscopic parameters.

**Tier 2 — mass-luminosity relation (fallback)**:

For stars without valid `log_g`:

```
M_star ≈ (L_star / L_sun)^(1/α)
```

Where α varies by luminosity regime:

| L / L_sun | α | Notes |
|---|---|---|
| < 0.033 | 2.3 | very low mass / M dwarfs |
| 0.033 – 16 | 4.0 | low to solar mass |
| 16 – 10⁵ | 3.5 | intermediate to high mass |
| > 10⁵ | 1.0 | very luminous; cap at ~150 M_sun |

This is known to overestimate mass for giant and subgiant stars (which have high luminosity for their mass). For aggregate mass in a cell, the error is bounded: most stars are main-sequence dwarfs, and the few giants dominate luminosity but not mass.

The mass estimate is clamped to `[0.08, 150]` M_sun to avoid unphysical values from noisy photometry.

---

## Node payload format

### Binary layout

Each node carries one fixed-size payload record:

```python
GLOW_PAYLOAD_FMT = struct.Struct("<I f fff f f I")
GLOW_PAYLOAD_SIZE = 32
```

| Offset | Field | Type | Unit | Description |
|--------|-------|------|------|-------------|
| 0 | `count` | u32 | — | number of stars in subtree |
| 4 | `sum_luminosity` | f32 | L_sun | total bolometric luminosity |
| 8 | `sum_lum_r` | f32 | — | luminosity-weighted linear red |
| 12 | `sum_lum_g` | f32 | — | luminosity-weighted linear green |
| 16 | `sum_lum_b` | f32 | — | luminosity-weighted linear blue |
| 20 | `sum_mass` | f32 | M_sun | estimated total stellar mass |
| 24 | `sum_lum_teff` | f32 | K · L_sun | Σ(L_i · Teff_i) for mean Teff derivation |
| 28 | `reserved` | u32 | — | must be 0 in version 1 |

### Additivity

All non-reserved fields are strictly additive: the parent value is exactly the sum of its children's values. This is the fundamental invariant of the glow octree.

### Derived quantities at read time

Readers derive:

```
colour      = (sum_lum_r, sum_lum_g, sum_lum_b) / sum_luminosity
intensity   = sum_luminosity
mean_teff   = sum_lum_teff / sum_luminosity
mean_mass   = sum_mass / count
```

### Possible future extension: luminosity-weighted centroid

A luminosity-weighted centroid (`centroid_x`, `centroid_y`, `centroid_z` as cell-relative f32 in [-1, 1]) would allow renderers to place glow sprites at the light-weighted center of each cell rather than the geometric center. This improves rendering quality for partially filled cells.

The centroid is **not** directly additive (it is a weighted mean), but the underlying accumulation (`Σ L_i · x_world_i`) is additive and can be used during the bottom-up pass with f64 precision. Cell-relative conversion happens on output.

This is deferred to a later version. The `reserved` field (4 bytes) is not sufficient for a 3-component centroid (12 bytes), so adding it would require a payload format version bump (e.g. 44 or 48 bytes).

### Note: add mass centroid alongside luminosity centroid

If centroid support is added, include a **mass-weighted centroid** in the same format pass, not only a luminosity-weighted centroid. Comparing them is a useful diagnostic:

- similar centroids imply light roughly traces mass in that cell
- larger offsets indicate luminosity skew (for example, a few bright giants or young massive stars displacing the luminosity center from the bulk stellar mass)

To preserve strict additivity, store additive moments during build:

- luminosity moments: `sum_lx = Σ(L_i · x_i)`, `sum_ly`, `sum_lz`
- mass moments: `sum_mx = Σ(M_i · x_i)`, `sum_my`, `sum_mz`

Then derive centroids at read time:

```
c_lum  = (sum_lx, sum_ly, sum_lz) / sum_luminosity
c_mass = (sum_mx, sum_my, sum_mz) / sum_mass
```

A useful per-node comparison metric is:

```
delta_centroid = ||c_lum - c_mass||
```

optionally normalized by node half-size:

```
delta_centroid_norm = delta_centroid / cell_half_size
```

---

## Aggregation

### Leaf cell accumulation

Every star with finite `mag_abs` is assigned to a leaf cell at `max_level` based purely on its spatial position (`x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`). There is no magnitude-based level assignment.

For each star assigned to a leaf cell:

```
cell.count         += 1
cell.sum_luminosity += L_star
cell.sum_lum_r     += L_star * r_lin
cell.sum_lum_g     += L_star * g_lin
cell.sum_lum_b     += L_star * b_lin
cell.sum_mass      += M_star
cell.sum_lum_teff  += L_star * teff
```

### Bottom-up parent reduction

For each level from `max_level − 1` down to `0`, each parent cell is the sum of its up-to-eight children:

```
parent.count         = Σ child.count
parent.sum_luminosity = Σ child.sum_luminosity
parent.sum_lum_r     = Σ child.sum_lum_r
parent.sum_lum_g     = Σ child.sum_lum_g
parent.sum_lum_b     = Σ child.sum_lum_b
parent.sum_mass      = Σ child.sum_mass
parent.sum_lum_teff  = Σ child.sum_lum_teff
```

Empty parent cells (no occupied children) are not written.

This is the complete aggregation specification. The additivity invariant guarantees that every level is self-consistent and that no information is lost or double-counted.

---

## Colour model

### Principle

**Do not aggregate temperature directly.**

A glow node must not be coloured by averaging `Teff` and then converting once to colour. Instead:

1. Convert each star's `Teff` to a linear colour representation using the same colour model as the discrete-star renderer.
2. Convert the star's brightness to a linear light weight (bolometric luminosity).
3. Multiply colour by that weight.
4. Sum those weighted colour channels per node.

Aggregation happens in **linear light space**, not in temperature space and not in display-space sRGB.

### Why

Averaging `Teff` is wrong for mixed populations because colour perception and RGB output are nonlinear with respect to temperature. Two red stars plus one blue star should not be represented by the colour of the average temperature. The correct aggregate is the sum of their emitted light contributions.

### Recommended formulation

For each star:

```
r_lin, g_lin, b_lin = teff_to_linear_rgb(teff)
L_star              = bolometric_luminosity(mag_abs, teff)

sum_lum_r += L_star * r_lin
sum_lum_g += L_star * g_lin
sum_lum_b += L_star * b_lin
sum_luminosity += L_star
```

At render time:

```
node_colour    = (sum_lum_r, sum_lum_g, sum_lum_b) / sum_luminosity
node_intensity = sum_luminosity
```

The renderer converts `node_colour` to the output colour space (sRGB or equivalent) and scales by `node_intensity` adjusted for distance.

### Mean Teff

`sum_lum_teff / sum_luminosity` gives the luminosity-weighted mean effective temperature of the node's stellar population. This is available for analytical use (HR diagrams, comparison with models) but must not be used for rendering colour. Use the linear RGB aggregate for rendering.

### Consistency requirement

The `teff_to_linear_rgb` function used by the glow build must be the same function used by the discrete-star renderer. This ensures visual continuity at the transition between resolved and unresolved rendering.

---

## Build pipeline

### Overview

The glow build is a separate pipeline from the star Stage 00/01/02 flow. It consumes the same merged HEALPix parquet and produces `glow.octree` as a sibling artifact.

```
merged HEALPix parquet ──→ glow-build ──→ glow.octree
```

### Phase 1 — leaf aggregation

Stream merged parquet in bounded-memory batches. For each batch:

1. Compute `morton_code` at `max_level` from `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` (reuse `morton3d_u64_from_xyz_arrays`).
2. Derive `node_id` at `max_level`: `morton_code >> (3 * (MORTON_BITS - max_level))`.
3. Compute per-star `L_star`, `(lum_r, lum_g, lum_b)`, `M_star` from `mag_abs`, `teff`, `log_g`.
4. GROUP BY `node_id` and SUM all aggregate fields.
5. Write partial leaf aggregates to temporary intermediate files.

After all batches are processed, merge partial aggregates for each leaf cell (sum across batches for the same `node_id`).

This phase can be implemented with DuckDB for efficient GROUP BY aggregation, consistent with the existing Stage 00 approach.

### Phase 2 — bottom-up reduction

For each level from `max_level − 1` down to `0`:

1. Read child-level cell records.
2. Derive parent `node_id` = child `node_id >> 3`.
3. Sum child records into parent records.
4. Write parent-level cell records.

This pass is bounded-memory: only two adjacent levels need to be in memory at once (children as input, parents as output). For levels with many occupied cells, DuckDB can handle the GROUP BY.

### Phase 3 — intermediate write

Write all cell records (all levels) as intermediate shard files using the same format as Stage 01: one payload file + one index file per shard.

The payload for each cell is the 32-byte `GLOW_PAYLOAD_FMT` record, gzip-compressed for format compatibility with the existing combine pipeline.

Shard planning (prefix sharding for deep levels) reuses the same `BuildPlan` parameters as the star pipeline.

### Phase 4 — combine

Reuse the existing Stage 02 combine pipeline (`combine_octree`) to assemble `glow.octree` from the intermediate shards.

The final `glow.octree` uses the same file-level structure as `stars.octree`:

- STAR header (magic `b"STAR"`, with `payload_record_size = 32`)
- ODSC descriptor block (`artifact_kind = "glow"`, dedicated `dataset_uuid`)
- Payload section in DFS order
- Shard index section

### Memory model

The build must remain bounded-memory:

- Phase 1: streaming batches + partial aggregates on disk.
- Phase 2: one level at a time in memory (occupied cells only, which is a small fraction of the theoretical 8^max_level).
- Phase 3/4: reuse existing bounded-memory Stage 01/02 machinery.

---

## Query and traversal

### Traversal rule

1. Start from root entries using the existing octree/shard navigation.
2. Reject nodes outside the camera frustum.
3. Estimate projected node size in screen space.
4. If the node is large on screen, descend to children.
5. If the node is at or below the glow threshold, stop descending and render the node's aggregate payload.

A simple first threshold: render glow when projected node diameter is approximately 1-2 pixels or less.

This gives:

- **Near camera**: deeper traversal, more spatial detail, eventually resolved to individual stars (from `stars.octree`).
- **Far from camera**: coarser traversal, integrated aggregate glow.

### Ownership and double counting

A node is rendered either as **aggregate glow** or by traversing its descendants — not both for the same ordinary stellar population. When a node is accepted as a glow node, its descendants are not rendered separately.

Bright stars rendered as discrete sources from `stars.octree` may overlap spatially with glow nodes. This is acceptable and often desirable: the direct source dominates visually, while the glow node contributes only faint unresolved background structure. If energy conservation is needed, bright-source subtraction from glow can be added as a later refinement. It is not required for V1.

---

## File format

The glow octree reuses the existing `stars.octree` binary format:

- Same STAR header structure (64 bytes).
- Same ODSC descriptor block (128 bytes) with `artifact_kind = "glow"`.
- Same shard/index layout (shard headers, node tables, frontier references).
- Same DFS payload ordering.

The differences are:

- `payload_record_size = 32` (instead of 16).
- Each node's payload is a single 32-byte `GLOW_PAYLOAD_FMT` record (gzip-compressed for intermediate compatibility).
- `artifact_kind = "glow"` in the descriptor (requires a new descriptor kind code, e.g. `GLOW_DESCRIPTOR_KIND = 3`).

Readers distinguish `glow.octree` from `stars.octree` by the descriptor's `artifact_kind` field.

### Code changes required

- `combine/records.py`: add `GLOW_DESCRIPTOR_KIND = 3` and update `_descriptor_kind_code` / `_descriptor_kind_name`.
- `combine/records.py`: `pack_top_level_header` currently hardcodes `PAYLOAD_RECORD_SIZE = 16`. This must be parameterised so the glow build can pass `payload_record_size = 32`.

---

## Configuration

The glow build reads from the octree project file:

```toml
[glow]
max_level = 13
batch_size = 1_000_000
```

Required project-file paths:

- `paths.merged_healpix_dir` (same input as Stage 00)
- `paths.glow_output_path` (e.g. `"output/glow.octree"`)
- `paths.glow_intermediate_dir` (e.g. `"output/glow-intermediates"`)

### CLI

```bash
uv run fis-octree glow --project path/to/project.toml
```

---

## Non-goals

- The glow octree does not replace `stars.octree` for individual star rendering.
- The glow octree does not support per-star lookup or identity queries.
- Bright-source subtraction for energy conservation is not required in V1.
- Bolometric correction precision beyond ~0.1 mag is not required for V1.
- The glow octree does not store spectral data beyond broadband RGB.
- The glow octree does not use magnitude-driven level assignment. Stars are never assigned to multiple levels.

---

## Implementation order

1. Add `log_g` to pipeline merged output (uncomment existing code, add to `OUTPUT_COLS`).
2. Implement bolometric correction function `BC(Teff)` (polynomial fit).
3. Implement `teff_to_linear_rgb` (linearise existing `teff_to_rgb` from `pipeline/common/photometry.py`).
4. Implement mass estimation (log_g tier + mass-luminosity fallback).
5. Add `GLOW_DESCRIPTOR_KIND` and parameterise `payload_record_size` in `combine/records.py`.
6. Implement leaf aggregation pass (streaming, DuckDB-backed).
7. Implement bottom-up reduction pass.
8. Write intermediates using existing shard writer.
9. Assemble final `glow.octree` using existing combine pipeline.
10. Add `fis-octree glow` CLI command and project config.
11. Add reader/stats support for glow octree inspection.
