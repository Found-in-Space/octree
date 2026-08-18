# Identity Lookup Index Specification

## Status

Locator v1 is implemented as the optional, dataset-scoped
`identity-locator.idx`. It is a purpose-named alternative index and does not
introduce another numbered pipeline stage. The separately described alias
index remains a future product.

The locator design is deliberately simple: sorted, independently readable
binary pages with a small navigation tree. Page size and compression codec are
declared by the artifact and selected by benchmark; identity semantics,
compatibility rules, and range-read behavior are part of the contract.

## Purpose

The published dataset currently has an efficient forward identity mapping:

```text
(level, node_id) -> ordered [(source, source_id), ...]
```

That mapping lives in `identifiers.order` and is aligned with render ordinals.
It supports streaming sidecar construction, but finding one identity requires a
full scan. The reverse locator adds:

```text
(source, source_id) -> (cell_record, ordinal) -> StarRef
```

The primary use cases are:

- resolving exact Gaia and Hipparcos identifiers without downloading or
  scanning the complete identity artifact;
- resolving names and secondary catalogue designations such as `Sirius`,
  `HD 48915`, or `alpha CMa` through a smaller alias index;
- allowing ID-driven optional products, including the visual-duplicate
  sidecar, to perform bounded point lookups instead of a full identity scan;
- supporting the same lookup implementation for local files and immutable
  HTTP objects with byte-range support.

## Product Boundaries

Identity lookup has two products with different sizes and invalidation rules:

| Product | Mapping | Expected scale | Rebuild condition |
| --- | --- | --- | --- |
| `identity-locator.idx` | Canonical dataset identity to render location | One record per rendered star | Render or `identifiers.order` identity changes |
| `identity-aliases.idx` | Human/catalogue alias to one or more canonical identities | Curated and independently extensible | Alias sources or normalization rules change |

The locator is a foundational dataset companion, not an octree sidecar. Its
lookup axis is identity rather than space, and multiple sidecar families may
reuse it. The alias artifact is separate so names and cross-catalogue mappings
can be updated without republishing the catalogue-scale locator.

Both artifacts are immutable publications. `identity-locator.idx` is the
implemented conventional locator name; consumers must still validate UUIDs
rather than infer compatibility from a filename.

## Identity And Location Model

### Canonical identity

A canonical rendered identity is the exact pair:

```text
(source, source_id)
```

The initial numeric namespaces are `gaia` and `hip`. Namespace descriptors
define their key codec. Gaia identifiers must remain unsigned 64-bit integers
or exact decimal strings throughout ingestion and lookup. They must never pass
through floating-point representation.

Cross-catalogue evidence does not merge canonical rendered identities. If a
Gaia record and a Hipparcos record both occur in the render, they remain two
separately addressable objects even when evidence says that they may represent
the same astronomical object.

### Star reference

The public result is a dataset-scoped star reference:

```text
StarRef {
  dataset_uuid,
  level,
  morton_code,
  ordinal
}
```

The locator stores the more compact intermediate value:

```text
CellOrdinalRef {
  cell_record: u32,
  ordinal: u32
}
```

`cell_record` is the zero-based record number in the fixed-width
`identifiers.order` directory. The client resolves it to `(level, node_id)` by
reading that directory record, then treats `node_id` as the cell Morton code.
The locator is therefore compatible only with the exact `identifiers.order`
artifact UUID recorded in its header.

Locator v1 requires the identity directory to contain fewer than `2^32`
records. A builder must fail explicitly rather than truncate if that limit is
exceeded. A wider reference requires a new declared value codec.

## Locator Binary Organization

The locator is one range-addressable binary object:

```text
header
namespace directory
root and internal navigation pages
sorted leaf pages
integrity metadata
```

All integers are little-endian. All offsets are absolute byte offsets from the
start of the locator file. Readers must reject offsets or lengths outside the
published object length.

### Header

The header must expose enough information for a client to bootstrap lookup
without reading any leaf data:

- format magic and version;
- header length and total object length;
- locator artifact UUID;
- parent render dataset UUID;
- exact `identifiers.order` artifact UUID;
- namespace-directory offset and length;
- declared page size;
- supported page and key codecs;
- build identity or content checksum.

Locator v1 uses a 160-byte header with the magic `OILR`. Its packed
little-endian struct is:

```text
<4sHHIQ16s16s16sQQIIII32sQQ12x
```

It records format version, header and total lengths, locator UUID, parent
render UUID, exact `identifiers.order` UUID, namespace-directory range and
count, decoded page size, navigation and leaf codec masks, deterministic
32-byte build identity, and integrity-footer range. The locator UUID is derived
from the exact source SHA-256, parent UUIDs, format settings, and builder
algorithm. Equal inputs and settings therefore produce byte-identical output.

### Namespace directory

Each namespace descriptor contains:

- canonical namespace name;
- key codec, initially `u64-decimal` for `gaia` and `hip`;
- value codec, initially `cell-record-u32-ordinal-u32`;
- record count;
- root page offset and encoded length;
- minimum and maximum key, when applicable;
- page compression codec;
- namespace content checksum.

Descriptors are fixed 128-byte records with packed struct
`<16sHHHHQQQQQQQII32s8x`. Locator v1 publishes exactly the independently
navigable `gaia` and `hip` descriptors, including when one is empty.

Namespaces are independent trees. A numeric Gaia lookup never reads
Hipparcos pages, and adding a future namespace does not change the encoding of
existing namespace keys.

### Navigation pages

The baseline navigation structure is a read-only B+tree. Internal pages contain
sorted separator keys and absolute child page ranges:

```text
(separator_key, child_offset, child_length)
```

Every page starts with a 64-byte `OILP` envelope using
`<4sHHBBHIIII32s4x`. It declares kind, codec, entry count, encoded and decoded
lengths, and the SHA-256 of its encoded body. Navigation records use `<QQQ>`
for upper-fence key, absolute child offset, and child length. Navigation pages
are always raw in v1.

A namespace-specific radix directory may replace the internal B+tree only when
it preserves the same range-read and validation guarantees and benchmarks show
a material improvement. It is not required for locator v1.

### Numeric leaf pages

Gaia and Hipparcos leaves are sorted numerically by `source_id`. The baseline
decoded record is fixed-width:

```text
source_id:   u64
cell_record: u32
ordinal:     u32
```

This is 16 bytes per rendered identity before page compression. Duplicate keys
within one namespace are invalid if they resolve to different locations.
Repeating the same key and location is also rejected so publication remains
canonical.

Leaf records use little-endian `<QII>`. Leaf pages are independently encoded
and checksummed. Compression must never
span multiple pages. A reader fetches and decodes one candidate leaf, performs
a binary search, and either returns the exact value or proves that the key is
absent.

The supported production page-size candidates are 32 KiB and 64 KiB decoded;
leaf codecs are raw and deterministic gzip (`compresslevel=1`, `mtime=0`). The
selected size and codec are recorded in the artifact rather than assumed by
readers.

The v2 production benchmark selected 32 KiB raw as the committed default. Its
141.1 microsecond reader-cold median was lowest. The 64 KiB raw result was
within the 5% tie window, after which 32 KiB raw won on p95 transferred bytes
(78,912 versus 135,008). See
[`identity-locator-v2-benchmark.md`](identity-locator-v2-benchmark.md).

The file ends with a 64-byte `OILF` footer using `<4sHHQ32s16x`. It records the
hashed prefix length and SHA-256 of every preceding byte.

### Future string-key namespaces

The namespace model permits an exact UTF-8 key codec later. Such leaves should
use sorted, length-delimited or front-coded strings and the same page envelope.
Numeric catalogue identifiers must continue to use numeric ordering instead of
lexicographic decimal-string ordering.

## Network Lookup Protocol

A remote exact lookup follows these steps:

1. Fetch and validate the locator header and namespace descriptor.
2. Parse the user designation into an explicit namespace and exact key.
3. Traverse the cached root and fetch only the required internal page ranges.
4. Fetch and decode one candidate leaf page.
5. Binary-search the leaf for the exact key.
6. Range-read the referenced fixed-width `identifiers.order` directory record.
7. Expand the result to a dataset-scoped `StarRef`.

After header and root caching, the target is two small locator range requests
plus the identity-directory record or its containing cache page. Implementations
may coalesce adjacent reads, but correctness must not depend on servers
supporting multipart range responses.

Remote publications must:

- support HTTP byte ranges;
- use stable immutable URLs or stable validators such as strong ETags;
- serve the binary object with `Content-Encoding: identity`, because internal
  pages already define their compression and offsets address the stored bytes;
- expose the required range and validator headers through CORS for browser
  clients;
- retain a stable object length for the lifetime of a publication.

Client page caches must include the artifact UUID or strong validator in their
cache key. Cached pages from different locator publications must never be mixed.

Local readers use the identical traversal with positional file reads instead
of HTTP ranges.

The Python HTTP source is deliberately stricter than the general octree
reader. Every request is finite and must return an exact `206` range with a
matching `Content-Range`, identity transport encoding, stable object length,
and a strong ETag or stable Last-Modified validator. Each reader owns a bounded
page/range cache.

## Alias Index

The alias index maps a normalized search key to one or more postings. A posting
contains at least:

```text
AliasPosting {
  namespace,
  source_id,
  alias_kind,
  original_label,
  provenance,
  rank
}
```

Optional evidence fields may include catalogue release, mapping method,
angular separation, confidence, and ambiguity counts. A posting may cache a
`StarRef`, but the canonical identity remains authoritative and the cached
reference must match the alias artifact's parent locator UUID.

The first alias kinds should include:

- proper name;
- Gaia, Hipparcos, and Henry Draper designations;
- Bayer designation;
- Flamsteed designation;
- other explicitly sourced catalogue designations.

Alias keys are not unique. Proper names, Bayer designations, components, and
historical catalogue relationships can all return multiple candidates. The
reader returns a ranked candidate list and must not silently choose or merge a
result.

### Normalization

Normalization rules are versioned and recorded in the alias artifact. The
baseline rules are:

- Unicode normalization and case folding for human names;
- whitespace and catalogue-prefix normalization;
- catalogue-specific parsing of numeric identifiers;
- defined equivalence between spelled and Greek-letter Bayer forms;
- preservation of the original display spelling in each posting.

A bare number has no implicit catalogue namespace and must not be guessed.
Numeric catalogue fields must be parsed exactly, without floating-point
conversion.

The alias artifact may initially be small enough to download and search in
memory. If it grows beyond the configured bootstrap bound, it uses the same
independently readable page envelope and a lexicographically sorted B+tree.
Prefix suggestions may scan adjacent pages. Fuzzy search requires a separate,
explicit n-gram or equivalent index and is outside locator v1.

## Query Resolution

The complete local resolution flow is:

```text
input text
  -> explicit catalogue parser or normalized alias key
  -> one or more canonical (source, source_id) identities
  -> exact identity locator
  -> dataset-scoped StarRef results
```

Exact catalogue syntax takes precedence over free-text alias matching. Search
results should expose why each candidate matched and any visual-duplicate or
cross-catalogue relationship available for that rendered object.

An external name service such as SIMBAD or Sesame is an optional fallback, not
a dependency of local lookup and not the canonical source of dataset identity.
When local resolution fails, a client may ask an external resolver for known
catalogue identifiers and retry those identifiers locally. External coordinates
may be offered as a separately labelled proximity search, but must not silently
create an exact identity match. External results and caches retain provenance
and retrieval time.

## Bounded Build Plan

The implemented locator build is a bounded external-ordering job:

1. Stream the `identifiers.order` directory and cell payloads once.
2. Emit fixed-width `(source_id, cell_record, ordinal)` runs partitioned by
   namespace.
3. Sort each run under an explicit memory bound.
4. Merge runs with bounded fan-in, rejecting duplicate identities.
5. Encode sorted leaf pages and checksums sequentially.
6. Build internal pages bottom-up from leaf fence keys and byte ranges.
7. Publish the immutable artifact and a build report atomically.

The scan recognizes Gaia, HIP, and manual rows through a vectorized structural
fast path and falls back to scalar decoding only for cells containing a future
namespace. It parses numeric IDs directly as canonical ASCII unsigned 64-bit
decimals. Runs and merge rounds are atomically checkpointed with fixed Arrow
schemas and semantic checksums, and compatible work resumes automatically.

The build does not accumulate catalogue identities in RAM or insert them one at
a time into a general-purpose mutable database. Spill runs are restart-safe,
checksum-tracked, and disposable after successful publication.

When practical, locator run generation may share the canonical materialized
identity stream used by packing. It remains an optional purpose-named product;
ordinary render packing does not require it.

## Public API And CLI

The Python interfaces are:

```python
build_identity_locator(IdentityLocatorBuildConfig) -> IdentityLocatorBuildResult
IdentityLocatorReader(locator_source, identifiers_order_source)
IdentityLocatorReader.lookup(source, source_id) -> StarRef | None
StarRef(dataset_uuid, level, morton_code, ordinal)
```

The purpose-named CLI is:

```bash
fis-octree identity-locator build --project project.toml
fis-octree identity-locator benchmark --project project.toml
fis-octree identity-locator lookup LOCATOR IDENTIFIERS SOURCE SOURCE_ID --json
fis-octree identity-locator validate LOCATOR IDENTIFIERS --report BUILD_REPORT
```

Build defaults to `<render-stem>.identity-locator.idx` with an adjacent report.
Benchmark produces an adjacent benchmark report. Neither command adds project
TOML fields or runs from `stage-02`.

Alias construction is a separate bounded ingestion of curated identifier maps
and cross-catalogue evidence. It resolves every posting to an exact canonical
identity and reports unresolved, ambiguous, malformed, and precision-invalid
rows.

## Validation And Invalidation

A reader must reject a locator when either of these differs from the active
dataset package:

- parent render dataset UUID;
- `identifiers.order` artifact UUID.

An alias reader must additionally validate its parent locator artifact UUID.

The locator build report should include:

- format and builder version;
- all parent and output UUIDs;
- source and output hashes;
- namespace counts and key ranges;
- leaf and internal page counts;
- decoded and encoded byte totals;
- duplicate and rejected identity counts;
- configured memory, run, page-size, and merge-fan-in bounds;
- measured build and lookup-validation timings.

Every publication must be tested with present and absent keys at namespace
boundaries, page boundaries, and random sampled locations. Sampled results must
round-trip through `identifiers.order` and match the canonical identity at the
resolved ordinal.

## Sizing And Performance Targets

At 1.47 billion rendered identities, the uncompressed numeric locator records
alone are approximately 23.5 GB (`16 * record_count`). This is acceptable as an
immutable optional artifact only because clients never download it wholesale
for point lookup.

The initial performance targets are:

- one small bootstrap read per artifact publication;
- no more than two locator page reads after the root is cached;
- one independently decodable leaf page per exact lookup;
- bounded-memory streaming construction;
- deterministic byte-identical output for identical inputs and build settings;
- useful page caching for both local and HTTP readers.

The benchmark command compares 32 KiB and 64 KiB pages, raw and independently
gzip-compressed leaves, reader-cold and warm local exact-range lookups, present
and absent keys, and uniform versus clustered Gaia/HIP samples. Selection uses
lowest median cold latency; results within 5% are tied and ranked by p95
transferred bytes, artifact size, then 64 KiB gzip. A minimal perfect hash or
specialized radix directory is a future alternative only if it materially
improves measured size or request latency without weakening exact membership
validation or range-read behavior.

## Related Documents

- [`identifiers-order.md`](identifiers-order.md)
- [`identity-locator-v2-benchmark.md`](identity-locator-v2-benchmark.md)
- [`sidecars.md`](sidecars.md)
- [`reader.md`](reader.md)
- [`stages.md`](stages.md)
- [`streaming-pipeline.md`](streaming-pipeline.md)
