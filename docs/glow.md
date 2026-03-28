# Proposal: integrated glow / render-distance luminosity contribution

## Summary

This proposal adds a second octree product alongside `stars.octree`: a **glow octree** for unresolved stellar light.

The existing `stars.octree` is a discrete-source structure. It is built around per-star payloads and queried using point-based spatial logic, especially the current visible-stars “shell loader” and nearby-stars query. That model is appropriate for rendering individual stars, but it is not the right abstraction for diffuse background light at distance. The current reader contract is explicitly star-centric and point-centric. :contentReference[oaicite:0]{index=0}

The proposed glow octree reuses the same staged octree infrastructure and the same shard/index model, but changes two things:

1. **Payload semantics**: each node stores integrated unresolved light rather than a list of individual stars.
2. **Query semantics**: traversal is driven by **view frustum + projected node size**, not by apparent-magnitude shell distance.

This should live in the **same project and repository** as the existing octree pipeline, as a sibling product rather than a separate implementation. Stage 01 already produces one payload blob per cell, and Stage 02 already assembles a payload-addressed indexed octree; that is a good architectural fit for a second payload type. :contentReference[oaicite:1]{index=1} :contentReference[oaicite:2]{index=2} The separate uploaded shard spec also matches this direction: it treats the shard structure as an index over payload regions rather than something inherently star-specific. :contentReference[oaicite:3]{index=3}

## Goal

The goal is to support a smooth transition from:

- **resolved sources** nearby and at larger apparent size, to
- **unresolved integrated emission** when a node is too small on screen to justify rendering its contents as individual stars.

In practical terms:

- nearby structure continues to refine to child nodes and eventually to stars
- distant structure is rendered as integrated glow from coarser nodes
- bright stars that already appear higher in the tree may still render as explicit sources, while deeper unresolved structure contributes only a faint local background

This is not a replacement for the star octree. It is a complementary representation for the unresolved-light regime.

## Core definition: render-distance luminosity contribution

A node’s **render-distance luminosity contribution** is the integrated light of the stellar population represented by that node, used when the node is small enough on screen that its contents should be treated as unresolved.

Traversal rule:

1. Start from root entries using the existing octree/shard navigation model.
2. Reject nodes outside the camera frustum.
3. Estimate projected node size in screen space.
4. If the node is still large on screen, descend to children.
5. If the node is at or below the glow threshold, stop descending and render the node’s integrated glow payload.

A simple first threshold is:

- render glow when projected node diameter is approximately `1–2 px` or less

This gives the intended behaviour:

- **near camera**: deeper traversal, more structure, more explicit stars
- **far from camera**: coarser traversal, aggregated glow

## Ownership and double counting

The intended ownership rule is:

- a node is rendered either as **aggregate glow**
- or by traversing its descendants
- but not both for the same ordinary stellar population

So when a node is accepted as a glow node, ordinary descendants from that node are not rendered separately.

Bright stars represented at higher levels may still render as explicit sources. This is acceptable and often desirable: the direct source dominates visually, while the glow node contributes only faint local unresolved structure around it.

## Relationship to the current star octree

The current `stars.octree` is based on a magnitude-driven level assignment and point-based query logic. Stage 00 currently enriches rows with `morton_code`, `render`, and `level`, and Stage 01 consumes that contract to write one payload blob per cell. :contentReference[oaicite:4]{index=4} :contentReference[oaicite:5]{index=5}

The glow octree should **not** inherit the current shell-loader query semantics. It should use the same index topology but a different reader contract:

- `stars.octree`: “which nodes might contain visible or nearby stars from this point?”
- `glow.octree`: “which nodes are the correct screen-space resolution to approximate unresolved light in this view?”

So this is a **shared infrastructure, different query policy** design.

## Build model

Recommended structure:

- keep the existing star pipeline unchanged
- add a sibling glow pipeline and output artifact, e.g. `glow.octree`
- reuse shared octree/shard/index code where possible
- keep the glow reader and payload definitions separate from the star reader

At a high level:

### Shared pieces
- Morton/grid helpers
- shard planning
- Stage 02-style final assembly
- shard/index reader/navigation

### Glow-specific pieces
- per-star light/colour inputs for aggregation
- node-level aggregation payload format
- frustum + projected-size traversal policy
- glow payload decode/render path

## Glow payload

A first glow payload should be compact and deterministic. Recommended contents per node:

- `sum_flux`
- `sum_r`
- `sum_g`
- `sum_b`
- optional `count`
- optional flux-weighted centroid `(cx, cy, cz)`
- optional compactness/spread term for tuning sprite size or falloff

This is enough to render a coloured emissive blob for a node without storing individual stars.

## Colour model

The current star pipeline already uses `teff` as a core part of render encoding, with a default of `5800 K` when temperature is absent. :contentReference[oaicite:6]{index=6} The glow octree should preserve that same physical/visual basis.

### Principle

**Do not aggregate temperature directly.**  
A glow node should not be coloured by averaging `Teff` and then converting once to colour.

Instead:

1. Convert each star’s `Teff` to a linear colour representation using the same colour model as the discrete-star renderer.
2. Convert the star’s brightness to a linear light weight.
3. Multiply colour by that weight.
4. Sum those weighted colour channels per node.

So aggregation happens in **linear light space**, not in temperature space and not in display-space sRGB.

### Why

Averaging `Teff` is wrong for mixed populations because colour perception and RGB output are nonlinear with respect to temperature. Two red stars plus one blue star should not be represented by the colour of the average temperature. The correct aggregate is the sum of their emitted light contributions.

### Recommended formulation

For each star:

- derive a linear RGB triplet from `Teff`
- derive a scalar light weight from magnitude / luminosity proxy
- accumulate:

```text
sum_r += w * r_lin
sum_g += w * g_lin
sum_b += w * b_lin
sum_flux += w
````

At render time:

```text
node_colour = (sum_r, sum_g, sum_b) / sum_flux
node_intensity = sum_flux
```

This preserves continuity between the discrete-star renderer and the glow renderer, because both are driven by the same per-star colour theory.

### Practical note

For visual continuity, the glow pipeline should use the **same Teff→colour function** as the star-render path. The only difference is that stars are summed before rendering rather than drawn one by one.

## Recommended first implementation

Version 1 should aim for simplicity:

* keep `stars.octree` unchanged
* add `glow.octree` as a parallel product
* use view-frustum + projected-size traversal
* use per-node aggregate linear RGB + intensity
* do not initially subtract bright stars from node aggregates
* tune thresholds visually after end-to-end testing

If later testing shows objectionable halos or energy inflation around very bright stars, bright-source subtraction can be added as a second-pass refinement. It is not required for the first implementation.

## Proposed one-sentence definition

The glow octree is a sibling octree product that renders **unresolved integrated stellar emission** by stopping traversal when a node becomes sufficiently small in screen space and using that node’s aggregate colour-weighted light contribution instead of rendering its descendants as individual stars.

```

This should be consistent with the current stage docs and reader split: Stage 00 enriches rows for Stage 01, Stage 01 writes one payload per cell, Stage 02 assembles the final octree, and the reader currently defines the existing star-specific query semantics. :contentReference[oaicite:7]{index=7} :contentReference[oaicite:8]{index=8} :contentReference[oaicite:9]{index=9} :contentReference[oaicite:10]{index=10}

If you want a tighter repo-style version next, the natural follow-on would be a `docs/glow-octree.md` with sections for payload format, traversal contract, and acceptance criteria.
