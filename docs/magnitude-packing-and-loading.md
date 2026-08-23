# Decision record: full-width magnitude assignment and loader quality

**Status:** Accepted

**Writer decision:** Assign natural magnitude bands by full cell width.

**Loader decision:** Treat node-selection radius as a separate runtime quality
and completeness control.

The normative writer and loader contracts are defined in
[`octree-spec.md`](octree-spec.md). This record preserves the alternatives,
measurements, reasoning, and migration consequences behind that specification.

## 1. Decision summary

Writing and loading solve different problems and make different trade-offs.

The writer chooses durable spatial granularity. Full-width natural assignment
places a star at level `N` when its index visibility radius lies between that
cell's half-width and full width:

```text
H(N) <= R_index < 2H(N)
```

This keeps magnitude bands in finer spatial cells. A half-width assignment would
move almost every band one level coarser, making cells twice as wide and eight
times the volume for the same stars.

The loader chooses transient completeness and work. On the full-width artifact,
it selects nodes with:

```text
load_radius = q H(B) scale
```

where `B` is the brightest natural level represented by the node or subtree and
`1 <= q <= 2`. `q=1` is a fast approximate core, while `q=2` is the complete
immediate-neighbour shell. Intermediate values provide progressive refinement.

The earlier defect was not the full-width artifact. It was presenting the
one-half-width loader as an exhaustive query over that artifact. Keeping the
writer fine-grained and making loader quality explicit preserves both fast
first-image behavior and complete results.

## 2. Context

For index limiting magnitude `m_index`, a star of absolute magnitude `M` has:

```text
R_index(M) = 10 ^ ((m_index - M + 5) / 5) parsecs
```

For root half-width `H(0)`:

```text
H(L) = H(0) / 2^L
W(L) = 2H(L)
```

One octree level represents:

```text
5 log10(2) = 1.505149978 magnitudes
```

The original published artifacts evaluated here used full-width natural
assignment. The original fast loader selected only one half-width. That loader
was inexpensive but incomplete for the brighter part of each natural band away
from favorable lattice alignments.

A later writer change instead used half-width assignment. This made a
one-half-width loader complete, but moved the cost into every artifact and every
client by coarsening the spatial cells.

Both writer/loader pairings are mathematically coherent:

| Writer assignment | Natural-band radii at level `N` | Complete loader radius | Ordinary geometric bound |
| --- | --- | --- | --- |
| Full width | `H(N) <= R < 2H(N)` | `2H(N)` | Immediate-neighbour ring; up to 27 cells |
| Half width | `H(N)/2 <= R < H(N)` | `H(N)` | Nearest `2 x 2 x 2`; up to 8 cells |

The decision is based on the combined cost of writing and loading, not on
geometric node count alone.

## 3. Writer alternatives and trade-offs

### 3.1 Full-width natural assignment — accepted

The writer assigns the natural level satisfying:

```text
H(N) <= R_index(M) < 2H(N)
```

An exact upper-threshold star belongs to the adjacent coarser band. Writers must
handle floating and render-quantization boundaries conservatively so the
encoded magnitude remains consistent with the assigned natural level.

Advantages:

- cells are one level finer for nearly every magnitude band;
- selected payloads cover one eighth the volume of the half-width alternative;
- loaders retain a cheap `q=1` core;
- loaders can progressively expand toward completeness; and
- exact loading does not require rebuilding the artifact.

Costs:

- a complete query can inspect up to the current cell and all immediate
  neighbours at an ordinary level;
- exact loading may decode many outer-shell candidates that fail the per-star
  visibility test; and
- loader APIs must distinguish approximate quality from complete results.

### 3.2 Half-width natural assignment — rejected

The alternative writer assigns:

```text
H(N)/2 <= R_index(M) < H(N)
```

This makes a one-half-width node query exhaustive. The geometric candidate set
is bounded by the nearest `2 x 2 x 2` cells.

The cost is durable coarsening. For the same magnitude band, emitted cells are
twice the linear size and eight times the volume. More geometric candidates are
occupied, and each selected payload tends to contain substantially more stars
that fail the exact visibility test.

Half-width assignment is attractive if the only objective is a hard eight-cell
bound. It is rejected because it fixes that loader choice into the artifact and
removes the finer-grained quality options.

### 3.3 Raising the private placement magnitude — rejected

Using 7.0 or 7.5 for placement while retaining 6.5 as the normal display limit
moves some stars to coarser levels and reduces omissions from a one-half-width
loader. It is a safety margin, not a completeness guarantee.

An exact one-level shift from 6.5 is:

```text
6.5 + 5 log10(2) = 8.005149978
```

This alternative would also give the serialized `mag_limit` two meanings: the
public index magnitude and a private writer margin. The accepted contract keeps
`mag_limit` equal to the actual natural-assignment basis and exposes loader
quality directly.

### 3.4 Profile coarsening remains independent

Classic level capping and STAR v2 terminal packing happen after natural
assignment. They can reduce node, index, and request overhead by assigning
natural cells to a coarser emitted payload node.

That creates a different trade-off from magnitude assignment: profile
coarsening may reduce structural work while increasing the spatial breadth and
decoded size of a payload. It must not change the natural band. STAR v2's
`brightest_level` preserves the bound needed to load a coarsened subtree safely.

## 4. Loader alternatives and trade-offs

For display magnitude `m_display`:

```text
scale = 10 ^ ((m_display - m_index) / 5)
load_radius = q H(B) scale
```

where `B` is the brightest natural level represented by the candidate node or
subtree.

### 4.1 Fast core: `q=1`

At the index magnitude, this selects at most the nearest `2 x 2 x 2` cells in an
ordinary position. It is the smallest useful working set and is appropriate for
a fast initial image or a deliberately constrained client.

It is not complete for the brighter portion of a full-width natural band. A
visible star is omitted only when both conditions hold:

1. its visibility radius is greater than one half-width; and
2. its node lies in the shell between the core radius and the star's exact
   visibility radius.

Such a star is normally loaded late on approach and unloaded early on departure.
It is not absent from the artifact.

### 4.2 Progressive shell: `1 < q < 2`

Intermediate values recover progressively more of the outer shell. For a
display limit `m_display`, the selected node set is complete through:

```text
m_complete = m_display + 5 log10(q / 2)
```

At a display limit of 6.5, `q=1.59` is complete through approximately 6.0 and
`q=1.78` through approximately 6.25.

This is the principal benefit of the accepted writer policy: the loader can
trade network, decode, upload, and memory cost against a known completeness
threshold without changing the artifact.

### 4.3 Complete shell: `q=2`

This selects the complete immediate-neighbour shell for a normal full-width
natural band. Exact APIs must use this policy by default, then apply the exact
per-star apparent-magnitude filter after decoding.

The trade-off is candidate amplification. Many newly decoded outer-shell stars
can be just outside the requested apparent-magnitude limit. Completeness is
therefore more expensive than the number of recovered stars alone suggests.

### 4.4 Priority, batching, and memory

After the node predicate is correct, the loader still controls when and how
eligible data becomes resident. It should load the `q=1` core first and rank the
outer shell by expected visual benefit.

For nonzero AABB distance, a conservative best possible apparent magnitude is:

```text
m_best = m_index + 5 log10(distanceToAABB / (2H(B)))
```

STAR v2 adds `star_count`, so a client can combine visual benefit with decoded
cost. Request coalescing, concurrency, cancellation, upload scheduling, and
cache admission are loader trade-offs. They are not reasons to coarsen natural
magnitude assignment in the writer.

## 5. Measurements

The measurements below used the public
[`c56103e6` STAR v1 artifact](https://data.foundin.space/c56103e6-ad4c-41f9-be06-048b48ec632b/stars.octree).

The apparent-magnitude limit was 6.5. “Fast full” uses the published full-width
artifact with `q=1`. “Complete full” uses the same artifact with `q=2`.
“Complete half” simulates moving the same catalogue one natural level coarser
and querying with one half-width.

“Logical nodes” pass the geometric predicate, “payload nodes” contain stars,
“stars loaded” are decoded candidates, and “visible” pass the exact per-star
apparent-magnitude test. These counts are decision evidence, not conformance
thresholds.

| Observer (pc) | Strategy | Logical nodes | Payload nodes | Stars loaded | Visible | Visible but missed |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Sun `(0, 0, 0)` | Fast full, `q=1` | 113 | 82 | 63,582 | 13,552 | 0 |
|  | Complete full, `q=2` | 113 | 82 | 63,582 | 13,552 | 0 |
|  | Complete half | 113 | 95 | 257,441 | 13,552 | 0 |
| Orion `(50, 400, -40)` | Fast full, `q=1` | 109 | 78 | 46,305 | 9,517 | 317 (3.22%) |
|  | Complete full, `q=2` | 238 | 176 | 90,449 | 9,834 | 0 |
|  | Complete half | 109 | 91 | 203,095 | 9,834 | 0 |
| Generic `(123.4, 567.8, -910.1)` | Fast full, `q=1` | 102 | 71 | 13,680 | 2,103 | 114 (5.14%) |
|  | Complete full, `q=2` | 257 | 192 | 33,663 | 2,217 | 0 |
|  | Complete half | 102 | 84 | 87,149 | 2,217 | 0 |

The percentages are samples, not a global error rate. The origin happened to
have no omission because its alignment with the octree lattice made the strict
`q=1` and `q=2` candidate sets equal.

At Orion, complete loading of the full-width artifact decoded another 44,144
candidates to recover 317 visible stars; 0.72% of the additional candidates
affected the result. The half-width simulation decoded 203,095 stars: 4.39
times the fast working set and 2.25 times the complete full-width working set to
render the same 9,834 stars.

The omitted Orion stars had apparent magnitudes from 5.398 to 6.5, with a median
of 6.279. About 82% were fainter than 6.0, and 56.5% were between 6.25 and 6.5.
They occurred at natural levels 8, 9, 11, 12, 13, and 14. The effect follows
magnitude and observer/node geometry rather than stellar colour, type, or a
single terminal level.

A separate same-input comparison of an earlier local STAR v1 and STAR v2 pair
found the same 483 omissions at Orion under the `q=1` loader. Their natural-level
distribution was 2, 23, 313, 2, 13, 115, 4, and 11 stars at levels 7 through 14.
The equal result within that paired build demonstrates that STAR v2 terminal
packing did not cause the omission.

### Placement-margin measurements

| Placement basis | Sun: payloads / loaded / missed | Orion: payloads / loaded / missed | Generic: payloads / loaded / missed |
| ---: | ---: | ---: | ---: |
| 6.5, original full | 82 / 63,582 / 0 | 78 / 46,305 / 317 | 71 / 13,680 / 114 |
| 7.0 | 89 / 101,797 / 0 | 85 / 76,019 / 139 | 78 / 25,550 / 30 |
| 7.5 | 91 / 167,368 / 0 | 87 / 128,179 / 17 | 80 / 49,195 / 2 |
| 8.00515, exact half | 95 / 257,441 / 0 | 91 / 203,095 / 0 | 84 / 87,149 / 0 |

The intermediate margins reduce omissions by accepting progressively coarser
payloads. Only the exact one-level shift provides the half-width completeness
guarantee, at the measured payload-overfetch cost.

## 6. STAR v1 and STAR v2

Magnitude-to-natural-level assignment is independent of the STAR container
version. STAR v2 changes terminal topology and metadata; it neither creates nor
repairs a mismatch between full-width writing and a one-half-width loader.

STAR v2 provides:

- `brightest_level`, the exact brightest natural band in a node's complete
  subtree;
- `star_count`, the records in the node's payload; and
- `IS_TERMINAL`, marking a collapsed terminal payload.

For a v2 node, loader bounds and priority use `brightest_level`, not the coarser
physical terminal half-width. The metadata is a band bound rather than the exact
minimum absolute magnitude in the payload. Adding exact content-based metadata
would be a separate format decision justified by client measurements.

## 7. Consequences and migration

### Writer consequences

- Full-width assignment is the canonical meaning of a conforming artifact.
- Natural assignment is part of tree and routing identity.
- A half-width artifact must be rebuilt from newly routed and prepared data.
- Profile materialization, `stars.octree`, and `identifiers.order` must also be
  rebuilt because cell membership and ordinal order can change.
- The upstream merged catalogue and upstream identity source remain reusable.
- Format versions do not need a width-policy flag because half-width assignment
  is nonconforming rather than an alternate production mode.

### Loader consequences

- `q` is explicit and restricted to the documented quality range.
- An exhaustive API such as `stars_brighter_than` defaults to `q=2`.
- Performance clients may deliberately use `q<2` and should expose or document
  the resulting completeness threshold.
- STAR v2 traversal uses `brightest_level`; STAR v1 uses physical node level as
  a conservative fallback.
- Core-first scheduling and shell priority are runtime policies.

### Publication consequences

Full- and half-width artifacts cannot be distinguished from `mag_limit` and
STAR version alone. During migration, publication metadata or dataset UUID must
identify legacy half-width builds. A dataset must not be declared conforming to
the baseline until its natural assignment and dependent artifacts have been
rebuilt.

## 8. Responsibility boundary

The writer owns:

- magnitude-to-natural-level assignment;
- profile mapping and node topology;
- terminal aggregation and serialized priority metadata;
- render and identity ordinal alignment; and
- physical payload order.

The loader owns:

- the chosen `q` and whether approximation is acceptable;
- traversal and visible-benefit priority;
- request coalescing and batch size;
- decode/upload scheduling and cancellation; and
- cache, concurrency, and memory policy.

Changing physical payload order or adding exact minimum-magnitude metadata is
format work. Breadth-first versus depth-first requests and preventing low-value
batches from occupying request slots are loader work. Format changes should
follow instrumentation showing that loader policy alone cannot meet the target.

## 9. Related documents

- [`octree-spec.md`](octree-spec.md) — normative writer and loader contract
- [`products.md`](products.md) — pipeline product and invalidation boundaries
- [`star-v2.md`](star-v2.md) — terminal topology and binary metadata
- [`reader.md`](reader.md) — Python reader implementation design
- [`terminal-memory-testbed.md`](terminal-memory-testbed.md) — runtime memory
  experiments for terminal payloads
