# Magnitude packing and node-loading policy

**Status:** Accepted  
**Decision:** Use full-width magnitude packing. Treat the loader radius as a
separate runtime quality/completeness control.

This decision record defines the geometry contract, the measurements that led
to the decision, and the boundary between octree packing and client loading. It
also clarifies why both the original deployment and the later half-width change
can be mathematically coherent while having very different performance.

## Executive summary

The original artefacts evaluated in this discussion use **full-width packing**:
at octree level `L`, a star's visibility radius at the index magnitude is less
than the cell's full width, `2H(L)`, and at least its half-width, `H(L)`.

The original fast loader queried only one half-width, `H(L)`. That deliberately
selected at most the nearest `2 x 2 x 2` cells in ordinary positions, but it was
not complete for the brighter half of the packed band. In the measured Orion
sample it loaded 46,305 stars and delivered 96.8% of the stars visible at
magnitude 6.5. The omitted 3.2% were not absent from the catalogue: their nodes
became eligible slightly too late as the observer approached.

The later builder implementation uses **half-width packing**. This makes the
one-half-width query complete, but moves almost every magnitude band one level
coarser. The selected cells are twice as wide and eight times the volume for the
same stellar band. In the Orion simulation this reduced the complete query from
176 payload nodes to 91, but increased decoded stars from 90,449 to 203,095.

Both layouts are valid when paired with their matching loader rule. The defect
was treating full-width-packed data as though one half-width were a complete
query. We are retaining full-width packing because its finer cells allow the
runtime to choose between a small fast core, a complete outer shell, or any
measured point between them. Half-width packing fixes that choice into the
artefact and pays substantially more star overfetch.

## Geometry and terminology

For index limiting magnitude `m_index`, a star of absolute magnitude `M` has
visibility radius:

```text
R(M) = 10 ^ ((m_index - M + 5) / 5) parsecs
```

For a root half-width `H(0)`, a level-`L` cell has:

```text
H(L) = H(0) / 2^L       # half-width
W(L) = 2H(L)            # full width
```

One octree level therefore represents:

```text
5 log10(2) = 1.505149978 magnitudes
```

“Full-width” and “half-width” in this note describe the largest visibility
radius encoded in a cell relative to that cell's geometry. They do not describe
the diameter of a star's visibility sphere.

| Packing contract | Visibility radii stored at level `L` | Complete AABB query radius | Ordinary geometric bound per level |
| --- | --- | --- | --- |
| Full width | `H(L) <= R < 2H(L)` | `2H(L)` | current cell plus all immediate neighbours; up to 27 |
| Half width | `H(L)/2 <= R < H(L)` | `H(L)` | nearest `2 x 2 x 2`; up to 8 |

An exact upper-threshold star belongs to the adjacent coarser band. This makes
the upper bound strict and preserves the cell-count guarantee. Implementations
should use a conservative floating-point boundary without duplicating stars.

The bounds are geometric, not promises that every candidate has a payload.
Sparse levels contain fewer payload nodes. Positions exactly on a cell plane,
edge, or corner can also reduce the number of distinct cells; the origin, for
example, lies on the shared corner of eight cells at each populated non-root
level.

## The two valid contracts

### Full-width packing

The original band construction assigns a star to the level satisfying:

```text
H(L) <= R(M) < 2H(L)
```

The exhaustive node predicate at the index magnitude is therefore:

```text
distanceToAABB(observer, node) < 2H(L)
```

If a node fails that predicate, even its brightest permitted star at the
observer-facing AABB boundary is not visible. No payload inspection is needed.

A query using only `H(L)` is a useful approximation, not the full contract. It
selects the current cell and the nearest neighbour in each axis. A visible star
is omitted only when both conditions hold:

1. it belongs to the brighter part of the level band and has `R > H(L)`; and
2. its node lies in the shell `H(L) < distanceToAABB < R`.

This is why an omitted star is normally loaded late on approach and unloaded
early on departure. It is not permanently missing. The effect is unrelated to
stellar colour or type: it follows absolute magnitude and observer/node
geometry. The samples contained omissions across many octree levels, although
most were close to the 6.5 apparent-magnitude limit and consequently subtle.

### Half-width packing

The later builder change assigns the same stars one level coarser:

```text
H(L)/2 <= R(M) < H(L)
```

The one-half-width predicate is now exhaustive:

```text
distanceToAABB(observer, node) < H(L)
```

This is the simplest contract if the only goal is a hard eight-cell bound.
However, each selected node is twice the linear size, and therefore eight times
the volume, of its full-width counterpart for the same magnitude band. The
number of geometric candidates remains bounded by eight, but more of those
coarse cells are payload-bearing and every selected payload tends to contain
many more irrelevant stars.

This explains why payload-node counts can rise slightly after the coarser
repack even though the query does not inspect more geometric cells: a larger
cell is more likely to be occupied. More importantly, the star count per loaded
payload rises sharply.

## Measurements

The following measurements used the public STAR v1 `c56103` artefact at an
apparent-magnitude limit of 6.5. “Fast full” is the deployed full-width packing
queried to `H`; “complete full” queries the same artefact to `2H`; “complete
half” simulates reassigning the same catalogue one level coarser and queries to
`H`.

“Logical nodes” passed the geometric predicate, “payload nodes” actually held
stars, “stars loaded” were decoded candidates, and “visible” passed the exact
per-star apparent-magnitude test.

| Observer (pc) | Strategy | Logical nodes | Payload nodes | Stars loaded | Visible | Visible but missed |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Sun `(0, 0, 0)` | Fast full, `H` | 113 | 82 | 63,582 | 13,552 | 0 |
|  | Complete full, `2H` | 113 | 82 | 63,582 | 13,552 | 0 |
|  | Complete half, `H` | 113 | 95 | 257,441 | 13,552 | 0 |
| Orion `(50, 400, -40)` | Fast full, `H` | 109 | 78 | 46,305 | 9,517 | 317 (3.22%) |
|  | Complete full, `2H` | 238 | 176 | 90,449 | 9,834 | 0 |
|  | Complete half, `H` | 109 | 91 | 203,095 | 9,834 | 0 |
| Generic `(123.4, 567.8, -910.1)` | Fast full, `H` | 102 | 71 | 13,680 | 2,103 | 114 (5.14%) |
|  | Complete full, `2H` | 257 | 192 | 33,663 | 2,217 | 0 |
|  | Complete half, `H` | 102 | 84 | 87,149 | 2,217 | 0 |

The percentages are samples, not a global error rate. In particular, the often
quoted 3% is the Orion result, not a property of the format. The origin happened
to have no omission because its alignment with the octree lattice made the
`H` and strict `2H` candidate sets equal.

The Orion full-width comparison also shows why simply changing the existing
loader to exhaustive mode was expensive: it loaded another 44,144 candidates
to recover 317 visible stars, so only 0.72% of the extra decoded stars affected
the image. The half-width repack was more expensive again: it loaded 203,095
stars, 4.39 times the original fast working set and 2.25 times the complete
full-width working set, to render the same 9,834 stars.

The omitted Orion stars in this artefact had apparent magnitudes from 5.398 to
6.5, with a median of 6.279. About 82% were fainter than magnitude 6.0 and 56.5%
were between 6.25 and 6.5. They occurred at levels 8, 9, 11, 12, 13, and 14, so
they were neither exclusively faint red dwarfs nor a terminal-level anomaly.

A separate same-Stage-0 comparison of an earlier local STAR v1 and STAR v2 pair
found the same 483 omitted stars in both formats at the Orion position. Their
natural-level distribution was 2, 23, 313, 2, 13, 115, 4, and 11 stars at levels
7 through 14 respectively. The absolute count differs from the public artefact
because the catalogue builds differ; the equality within the paired build
demonstrates that STAR v2 terminal packing did not cause the omission.

### Raising the packing magnitude

We also evaluated using 7.0 or 7.5 as the placement basis while retaining 6.5
as the normal display/query limit. This moves some stars to coarser levels and
reduces omissions, but it is a compromise rather than a geometric guarantee.

| Placement basis | Sun: payloads / loaded / missed | Orion: payloads / loaded / missed | Generic: payloads / loaded / missed |
| ---: | ---: | ---: | ---: |
| 6.5, original full | 82 / 63,582 / 0 | 78 / 46,305 / 317 | 71 / 13,680 / 114 |
| 7.0 | 89 / 101,797 / 0 | 85 / 76,019 / 139 | 78 / 25,550 / 30 |
| 7.5 | 91 / 167,368 / 0 | 87 / 128,179 / 17 | 80 / 49,195 / 2 |
| 8.00515, exact half | 95 / 257,441 / 0 | 91 / 203,095 / 0 | 84 / 87,149 / 0 |

The exact one-level shift is not 8.1. It is:

```text
6.5 + 5 log10(2) = 8.005149978
```

Using a different private placement basis also creates two meanings for the
single `mag_limit` currently serialized in the STAR header. Full-width packing
with an explicit loader policy is clearer and avoids encoding a UI safety margin
into every artefact.

## Decision and runtime control

Full-width packing is the canonical production contract. For display magnitude
`m_display`, define:

```text
scale = 10 ^ ((m_display - m_index) / 5)
loadRadius = q H(L) scale
```

where `q` is an explicit loader quality parameter:

| `q` | Behaviour at the index magnitude |
| ---: | --- |
| `1` | Fast core; up to 8 cells per ordinary level; intentionally approximate |
| `1 < q < 2` | Tunable shell; recovers progressively fainter boundary stars |
| `2` | Complete for the packed band; up to 27 cells per ordinary level |

For a UI limit `m_display`, the apparent magnitude through which the node set is
geometrically complete is:

```text
m_complete = m_display + 5 log10(q / 2)
```

At a UI limit of 6.5, `q=1` is complete through approximately 4.995, `q=1.59`
through 6.0, `q=1.78` through 6.25, and `q=2` through 6.5. This provides a
meaningful quality control without rebuilding or loading the entire outer shell
at once.

The recommended progressive policy is:

1. load the `q=1` core first;
2. rank nodes in the shell `H < distanceToAABB < 2H` by visual benefit; and
3. expand toward `q=2` as the frame, network, decode, and memory budgets allow.

For a normal full-width node, a conservative best possible apparent magnitude
can be estimated without reading the payload:

```text
m_best = m_index + 5 log10(distanceToAABB / (2H))
```

Nodes with the smallest `m_best` have the greatest chance of contributing a
bright visible star and should be prioritised. Handle zero AABB distance as an
immediate/core node rather than evaluating the logarithm.

## STAR v1, STAR v2, and responsibility boundaries

Magnitude packing is independent of the STAR container version. STAR v2
improves terminal-subtree packing; it does not by itself correct or cause the
`H` versus `2H` selector mismatch.

STAR v2 does provide useful loader metadata:

- `brightest_level` gives the brightest natural magnitude band in a node's
  complete subtree;
- `star_count` gives the number of stars in the payload; and
- `IS_TERMINAL` identifies a collapsed terminal payload.

For a v2 terminal or ancestor, derive the conservative visibility bound from
`brightest_level`, not from the coarser physical terminal-node half-width. Under
the full-width contract, natural level `B` has `R_max = 2H(B)`. This supports
safe pruning and benefit-per-star prioritisation. It is still a band bound, not
the exact minimum absolute magnitude in that payload; exact content-based
priority would require additional metadata.

The octree artefact determines:

- magnitude-to-natural-level packing;
- node geometry and topology;
- terminal aggregation and available priority metadata; and
- physical payload order, currently depth-first/Morton in
  [`packing/dfs.py`](../src/foundinspace/octree/packing/dfs.py).

The client determines:

- the chosen `q` and whether completeness is required;
- traversal and visible-benefit priority;
- request coalescing and batch size;
- decode/upload scheduling and cancellation; and
- cache and memory policy.

Breadth-first versus depth-first request order, sorting shell nodes by expected
brightness, and preventing low-value batches from occupying all request slots
are therefore primarily client-side work. Changing physical payload order or
adding exact minimum-magnitude metadata would be octree-format work, but should
follow client instrumentation and controlled benchmarks rather than be assumed
necessary.

## Implementation implications

The current code does not yet implement this accepted contract end to end:

- [`mag_levels.py`](../src/foundinspace/octree/mag_levels.py) contains the
  half-width reassignment; its use of the level `L+1` threshold makes level `L`
  satisfy approximately `H(L)/2 < R <= H(L)`.
- [`reader/__init__.py`](../src/foundinspace/octree/reader/__init__.py), the
  stats path, terminal-memory testbed, and packing benchmark all use
  `node.half_size * scale`, which is `q=1`.
- [`star-v2.md`](star-v2.md) correctly documents `brightest_level`,
  `star_count`, and terminal structure, but the version does not serialize a
  full-width/half-width policy flag.

The half-width source change and the original full-width artefacts are not
distinguishable from `mag_limit` and STAR version alone. The migration should
define full width as the canonical meaning, restore the original band
placement, make `q` explicit in readers and benchmarks, and rebuild half-width
artefacts from newly routed and prepared data. Routing and preparation must be
rerun because they contain the assigned natural `level` column; the upstream
merged catalogue and identifier map remain reusable. `stars_brighter_than`
should default to `q=2`, because its name promises a complete result;
performance clients may deliberately select a smaller `q`.

Tests must cover both halves of the contract:

1. every encoded magnitude at natural level `L` has `H(L) <= R < 2H(L)`, after
   render-magnitude quantisation and boundary handling; and
2. a complete AABB query reaches all and only the current/immediate-neighbour
   ring, with explicit lattice-boundary cases.
