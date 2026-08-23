# Star octree writing and loading specification

**Status:** Baseline specification

**Normative decision:** Writers use full-width magnitude assignment. Loaders
treat node-selection radius as a separate quality and completeness control.

This document is the canonical contract shared by octree writers and loaders.
It specifies what a conforming `stars.octree` means; it does not require the
writer and loader to share implementation code.

The rationale and measurements behind the contract live in
[`magnitude-packing-and-loading.md`](magnitude-packing-and-loading.md). Pipeline
product boundaries live in [`products.md`](products.md), and STAR v2 binary
details live in [`star-v2.md`](star-v2.md).

## 1. Shared geometry and terminology

For the index limiting magnitude `m_index`, a star of absolute magnitude `M`
has an index visibility radius:

```text
R_index(M) = 10 ^ ((m_index - M + 5) / 5) parsecs
```

For root half-width `H(0)`, an octree cell at level `L` has:

```text
H(L) = H(0) / 2^L
W(L) = 2H(L)
```

Moving one level changes the magnitude boundary by:

```text
5 log10(2) = 1.505149978 magnitudes
```

Three levels must not be conflated:

- **Natural level `N`** is selected from the star's magnitude before any output
  profile policy is applied.
- **Emitted level `E`** is the physical node that owns a payload after classic
  level capping or terminal-subtree packing.
- **Brightest level `B`** is the shallowest natural level represented by a node
  and its complete subtree. A smaller numeric level is brighter.

For an ordinary uncapped node, `N = E = B`. Profile mapping can make `E < N`,
while STAR v2 preserves `B` explicitly so a loader need not infer magnitude
semantics from the coarser emitted geometry.

“Full width” and “half width” describe visibility radius relative to cell
geometry. They do not describe a visibility-sphere diameter or the pipeline's
final `pack` product.

## 2. Writer contract

Writing determines the durable meaning and physical organization of the
artifact. Loader quality choices must not change natural level assignment.

### 2.1 Full-width natural magnitude assignment

A conforming writer assigns every interior natural magnitude band to the level
`N` satisfying:

```text
H(N) <= R_index(M) < 2H(N)
```

The upper bound is strict. A star exactly on `2H(N)` belongs to the adjacent
coarser band. The lower bound is inclusive because `H(N) = 2H(N+1)`.

The root and deepest representable natural level are saturation bands for
values outside the finite interior range. Implementations must test those two
endpoint policies separately; the interior-band invariant must not be asserted
blindly at a saturated endpoint.

Magnitude quantization and level assignment must agree. After a render
magnitude is encoded and decoded, the represented value must remain in the
assigned natural band. A writer must handle a quantization boundary
conservatively rather than allowing the stored magnitude and natural level to
contradict each other.

This is the only conforming production assignment. Half-width assignment is a
rejected artifact policy, not a loader mode.

### 2.2 Profile mapping happens after natural assignment

Natural assignment and output topology are separate writer stages:

1. routing assigns `N` and the natural spatial cell;
2. topology planning maps that cell to an emitted profile cell `E`;
3. materialization encodes positions relative to `E`; and
4. final packing serializes the materialized ranges and node metadata.

The classic profile may cap a deep natural cell at a shallower emitted level.
The terminal-packed profile may collapse a complete subtree into a shallower
terminal. Neither policy changes `N` or the full-width magnitude contract.

Profile remapping has its own trade-off: fewer and shallower nodes reduce index,
request, and traversal overhead, but each selected payload covers more space and
can contain more stars that fail the final per-star visibility test.

`identifiers.order` is emitted from the same profile-specific materialized
stream. Any change to natural assignment or profile mapping therefore rebuilds
both `stars.octree` and its matching `identifiers.order`.

### 2.3 Serialized magnitude semantics

The STAR header's `mag_limit` is `m_index`: the apparent-magnitude basis used
for natural level assignment. It is not a private placement margin and it is not
the loader's current display limit.

The format does not serialize a full-width/half-width flag. Full-width natural
assignment is part of conformance, so a loader may rely on it for a conforming
artifact.

STAR v2 serializes, for every node:

- `brightest_level = B`, the exact shallowest natural level in the node's
  complete subtree;
- `star_count`, the number of stars in the node's own payload; and
- `IS_TERMINAL`, identifying a collapsed terminal payload.

For STAR v1, `B` is unavailable. A loader uses `E` as a conservative bound. This
can load extra nodes when a capped payload contains only fainter natural bands,
but it must not omit a visible star.

### 2.4 Writer trade-offs

The writer chooses durable granularity. That decision constrains every loader
of the artifact.

| Writer policy | Spatial granularity for the same magnitude band | Payload overfetch | Loader freedom | Decision |
| --- | --- | --- | --- | --- |
| Full-width natural assignment | Finer | Lower | Loader can choose a fast core, a complete shell, or an intermediate quality | Required |
| Half-width natural assignment | One level coarser | Higher; cells have eight times the volume | A one-half-width query is complete, but the coarse placement is fixed into the artifact | Rejected |

Classic capping and terminal packing are independent of this choice. They may
coarsen emitted payload ownership for index and request efficiency, while `N`
and, in v2, `B` retain the magnitude meaning required for safe loading.

### 2.5 Writer conformance checks

A conforming writer must test at least:

1. interior natural bands satisfy `H(N) <= R_index < 2H(N)` after render
   magnitude quantization;
2. exact shared band boundaries belong to the coarser band;
3. root and deepest-level saturation behave according to their explicit
   endpoint policy;
4. profile mapping never changes the stored natural-level summary;
5. v2 `brightest_level` is the exact minimum natural level across the complete
   subtree; and
6. render and identifier artifacts retain identical cell membership and
   ordinal order.

## 3. Loader contract

Loading determines which parts of a conforming artifact are fetched and
decoded for one observer and display limit. It must not reinterpret or rewrite
the writer's magnitude bands.

### 3.1 Node-selection radius

For display limiting magnitude `m_display`, define:

```text
scale = 10 ^ ((m_display - m_index) / 5)
load_radius(node) = q H(B) scale
```

where `1 <= q <= 2` is the loader's explicit quality parameter.

For STAR v2, `B` comes from `brightest_level`. For STAR v1, use the emitted node
level `E` as a conservative substitute. An uncapped ordinary node naturally has
`B = E`.

The loader selects a node when:

```text
distanceToAABB(observer, node) < load_radius(node)
```

and prunes its complete subtree otherwise. Boundary comparisons must be
implemented conservatively with the same strict upper-bound convention as the
writer.

Node selection is only a coarse filter. After decoding a selected payload, the
loader still computes each star's exact apparent magnitude and returns or
renders only stars satisfying the requested display limit.

### 3.2 Quality and completeness

The writer always emits the same full-width artifact. `q` changes loader cost
and completeness without rebuilding it.

| `q` | Node-selection behavior at `m_index` | Geometric bound per ordinary level | Completeness |
| ---: | --- | --- | --- |
| `1` | Nearest core | Up to `2 x 2 x 2` cells | Intentionally approximate for the brighter part of a band |
| `1 < q < 2` | Progressive outer shell | Between the core and immediate-neighbour ring | Complete only through a brighter threshold |
| `2` | Complete immediate-neighbour shell | Up to `3 x 3 x 3` cells | Complete for the packed band |

For a request at `m_display`, the node set is geometrically complete through:

```text
m_complete = m_display + 5 log10(q / 2)
```

At `m_display = 6.5`, `q=1` is complete through about `4.995`, `q=1.59`
through `6.0`, `q=1.78` through `6.25`, and `q=2` through `6.5`.

An API whose name promises all matching stars, including
`stars_brighter_than`, must default to `q=2`. A performance-oriented client may
choose a smaller value only when it exposes or deliberately accepts approximate
completeness.

### 3.3 Progressive loading

A progressive client should:

1. load the `q=1` core first;
2. identify nodes in the shell between `H(B)` and `2H(B)`; and
3. expand toward `q=2` as network, decode, upload, frame, and memory budgets
   allow.

For a candidate with nonzero AABB distance, a conservative best possible
apparent magnitude is:

```text
m_best = m_index + 5 log10(distanceToAABB / (2H(B)))
```

Smaller `m_best` values indicate a greater chance of contributing a bright
visible star. A node containing the observer is an immediate core candidate and
must not evaluate the logarithm at zero.

`star_count` permits benefit-per-star or benefit-per-byte prioritization, but it
does not change the completeness predicate. Request ordering, coalescing,
concurrency, cancellation, decoded-memory admission, and cache eviction remain
runtime policies.

### 3.4 Loader trade-offs

The loader chooses transient work for one view. Unlike writer granularity, this
choice can change from frame to frame without changing the artifact.

| Loader choice | Node requests | Decoded candidates | Visible completeness | Typical use |
| --- | --- | --- | --- | --- |
| `q=1` | Lowest | Lowest | Approximate | Fast first image or constrained runtime |
| Intermediate `q` | Tunable | Tunable | Known brighter completeness threshold | Progressive refinement |
| `q=2` | Highest immediate-neighbour shell cost | Higher | Complete | Exact query, validation, or final-quality view |

Increasing `q` on a full-width artifact can decode many candidates to recover a
small number of near-limit visible stars. That cost belongs to the loader's
quality decision. Moving stars one level coarser during writing instead would
make the one-half-width query complete, but would increase payload overfetch for
every loader and remove the finer-grained option.

### 3.5 Loader conformance checks

A conforming loader must test at least:

1. `q=2` returns every star satisfying the exact apparent-magnitude predicate;
2. `q=1` is documented and measured as approximate rather than presented as an
   exhaustive query;
3. v2 terminals and ancestors use `brightest_level`, not emitted-node
   half-width, for their magnitude bound;
4. v1 emitted-node bounds are conservative under classic capping;
5. AABB comparisons cover cell-plane, edge, corner, and floating-boundary
   cases; and
6. per-star filtering is applied after payload decoding for every quality.

## 4. Responsibility boundary

| Writer/artifact responsibility | Loader/client responsibility |
| --- | --- |
| Full-width magnitude-to-natural-level assignment | Choose and expose `q` |
| Natural and emitted cell identity | Traverse selected nodes |
| Classic or terminal profile mapping | Prioritize core and shell work |
| Render and identifier ordinal alignment | Coalesce and schedule requests |
| `mag_limit` and available v2 metadata | Decode, filter, upload, cancel, and cache |
| Physical payload order | Enforce transient memory and concurrency budgets |

Changing payload order or adding more exact brightness metadata is a writer and
format decision. Breadth-first versus depth-first requests and shell priority
are loader decisions unless measurements demonstrate that the physical format
prevents an important access pattern.

## 5. Compatibility and migration

Earlier full-width artifacts queried only to `q=1` are conforming artifacts
used by an approximate loader. Their missing boundary stars do not imply absent
catalogue data.

Artifacts built with half-width natural assignment are not conforming to this
baseline and cannot be identified from `mag_limit` or STAR version alone. They
must be identified by dataset publication metadata or UUID and rebuilt before
being published as conforming artifacts.

Changing natural assignment invalidates routing and every downstream product
that contains `level`, cell membership, topology, render coordinates, or
profile-specific identity order. The upstream merged catalogue and upstream
identity source remain reusable; routed contributions, prepared products,
materialized ranges, `stars.octree`, and `identifiers.order` do not.

## 6. Related documents

- [`magnitude-packing-and-loading.md`](magnitude-packing-and-loading.md) —
  accepted decision, alternatives, measurements, and consequences
- [`products.md`](products.md) — pipeline products and invalidation boundaries
- [`star-v2.md`](star-v2.md) — terminal topology and binary metadata
- [`reader.md`](reader.md) — Python reader implementation design
- [`terminal-memory-testbed.md`](terminal-memory-testbed.md) — terminal payload
  and runtime-memory experiments
