**User**

If you look at this pipeline of @docs/pipeline-stage-00.md and @docs/pipeline-stage-01.md, we currently work on a single catalog. However, we also have brighter stars from the hipparcos catalog. Some of these are not covered by the gaia data, but some are - so we need to find the right stage where merge the two datasets. 

It's most likely in stage 00 as this is where we have the full source data which includes some quality metrics that allow us to determine whether the gaia or hipparcos data is better quality - but it could be that we also create a 'merge plan' and merge at a later stage. 

Deduplication will be based on source id - we have a mapping table of source ids from both datasets. 

---

**Cursor**

Let me also explore the codebase to understand what's already in place around catalog handling and source IDs.

Good question. Having read both pipeline specs and explored the codebase, here's my analysis.

## Current state

The pipeline is completely catalog-agnostic. It expects parquet with `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `mag_abs`, and optional `teff`. There's no concept of `source_id` or catalog provenance anywhere. Stage 01 explicitly assumes **input uniqueness**: "each star row appears in exactly one Stage 00 shard input" -- so deduplication must happen before rows reach stage 01.

## Where the merge should happen

**Recommendation: a new step before the current stage 00**, or as a new first step within stage 00 (before `add_shard_columns`). Here's the reasoning:

### Why before `add_shard_columns`

1. **Quality metrics are only available in raw source data.** The columns you need for the Gaia-vs-Hipparcos quality decision (parallax error, photometric flags, astrometric excess noise, etc.) exist in the upstream catalog parquet. Once `add_shard_columns` runs, the pipeline only cares about `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `mag_abs`, `teff`. Quality columns survive as passthrough (since `sort_shards` does `SELECT *`), but there's no reason to compute `morton_code` and `render` bytes for stars you'll discard.

2. **Stage 00 processes files independently.** `add_shard_columns` iterates each `.parquet` file and updates it in place. `sort_shards` reads each file from the root directory independently. Neither step does cross-file joins. The Gaia-Hipparcos merge inherently requires a cross-catalog join via the mapping table -- this is a fundamentally different operation that doesn't fit the current per-file processing model.

3. **The output contract is simple.** The merge step just needs to produce parquet files with the standard schema. Everything downstream works unchanged.

### Why not between `add_shard_columns` and `sort_shards`

You *could* do it there -- quality columns are still present since `add_shard_columns` only adds columns in-place. But you'd be computing `morton_code`, `render`, and `level` for Hipparcos stars that will later be discarded as duplicates. That's wasted work, and it means the merge step needs to understand and handle those derived columns.

### Why not at stage 01

Stage 01 only sees `morton_code`, `render`, `level`, `mag_abs`. Quality metrics are gone (they're not in the query). Stage 01 is also designed as a streaming bounded-memory writer -- adding join/dedup logic there would violate its architectural invariants.

### Why a "merge plan" approach is probably not worth it

A merge plan (decide at stage 00, apply later) would require:
- Carrying `source_id` through the pipeline as a column
- Persisting the plan as a side artifact
- Applying exclusion filters at query time in stage 01's row source

This adds complexity across multiple stages for something that's really a one-time data preparation decision. The dedup decision doesn't benefit from any information computed by stage 00 (morton codes, render bytes) -- it's purely about catalog quality metrics that exist in the raw data.

## Suggested design for the merge step

A new step, conceptually **"stage 00-pre"** or just the first sub-step of stage 00:

```
upstream Gaia parquet + Hipparcos parquet + cross-match table
  → merge_catalogs (new step)
  → unified parquet (standard schema)
  → run_add_shard_columns (existing)
  → run_sort_shards (existing)
  → input for stage 01
```

The merge logic (likely as a DuckDB query):

1. **Load the cross-match table** (Gaia `source_id` <-> Hipparcos `source_id` mapping)
2. **For matched stars**: compare quality metrics (e.g. parallax_over_error, astrometric_excess_noise, phot_bp_rp_excess_factor -- whatever you determine is appropriate) and pick the winner
3. **Unmatched Hipparcos stars**: keep (these are the ones not covered by Gaia)
4. **Unmatched Gaia stars**: keep (the vast majority)
5. **Output**: unified parquet files in the standard schema (`x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `mag_abs`, `teff`)

Since Hipparcos is only ~120k stars and the cross-match table is similarly small, this step would be fast compared to processing the full Gaia catalog. You could do it entirely in DuckDB with something like:

```sql
-- Identify which source to keep for each matched pair
WITH match_decisions AS (
    SELECT
        xm.gaia_source_id,
        xm.hip_source_id,
        CASE WHEN <gaia_quality_score> >= <hip_quality_score>
             THEN 'gaia' ELSE 'hip' END AS keep
    FROM crossmatch xm
    JOIN gaia g ON g.source_id = xm.gaia_source_id
    JOIN hipparcos h ON h.source_id = xm.hip_source_id
)
-- Then UNION ALL: unmatched Gaia + Gaia winners + unmatched Hip + Hip winners
```

The output parquet would then be placed in the same directory structure that `add_shard_columns` expects, and the rest of the pipeline runs unchanged.

## One consideration: provenance column

You might want to add a `catalog_source` column (e.g., `'gaia'` / `'hip'`) to the merged output for debugging and traceability. Since `sort_shards` does `SELECT *`, this would flow through to stage 01's input parquet (though stage 01 wouldn't use it). It costs very little and makes it easy to verify the merge later.

---

**User**

That is the next question - the idea is that in a very similar format to the star table, we also write a 'metadata' table - in that table, we have compressed json payload for each star appearing in the same order as in the main octree - that way we can quickly load metadata from the extra file if we want to retrieve gaia source ids, hips as well as bayer, HD etc. 

---

**Cursor**

This is a really clean design. Let me make sure I understand the architecture correctly and think through how it fits the pipeline.

That's a very natural extension of the current architecture. The existing structure actually lends itself well to this -- let me walk through how it would integrate.

## How it maps to the current pipeline

Right now, the data flows through a clean pipeline:

```
row_source → (node_id, render) tuples
  → encoder groups by node_id → EncodedCell (gzipped render bytes)
    → writer appends to .payload + .index
```

A metadata sidecar would extend this to produce **two payloads per cell in the same pass**:

```
row_source → (node_id, render, metadata_columns...) tuples
  → encoder groups by node_id → (EncodedCell, EncodedMetaCell)
    → writer appends render to .payload, metadata to .meta-payload
```

### The single-pass constraint is critical

The stars must appear in identical order in both files. The current row source in `row_source.py` orders by `(node_id, mag_abs)` with no stable tiebreak -- the pipeline-stage-01 spec explicitly calls this out:

```67:68:docs/pipeline-stage-01.md
`(node_id, mag_abs[, stable_tiebreak])`
```

If you produced metadata in a separate pass, you'd need to guarantee DuckDB gives you the same row ordering across two independent queries for rows with identical `(node_id, mag_abs)`. That's not safe. Both payloads need to come from the same query, same pass, same row stream.

This also gives you a natural reason to finally define that stable tiebreak -- `source_id` is an excellent candidate, since it's unique per star and you'll be fetching it anyway for the metadata.

### What changes in each stage

**Row source** (`row_source.py`): Currently selects only `node_id` and `render`:

```49:50:src/foundinspace/octree/assembly/row_source.py
    query = (
        f"SELECT (morton_code >> {shift}) AS node_id, render "
```

Would also need to select the metadata columns. Since `sort_shards.py` does `SELECT *`, any columns present in the upstream parquet survive through to stage 01's input files -- so the metadata columns just need to exist in the pre-stage-00 input.

**Encoder** (`encoder.py`): Currently accumulates `render` bytes and gzip-compresses them. Would need a parallel accumulator that collects per-star metadata dicts and produces gzip-compressed JSON per cell. The two accumulators flush together on `node_id` change.

**Writer** (`writer.py`): Currently writes one `.payload` + one `.index` per shard. Would additionally write a `.meta-payload` file. The index question is interesting -- see below.

### Index design: shared vs. separate

You have a choice:

**Option A -- Extend the index record** to add `meta_payload_offset` and `meta_payload_length`:

```python
INDEX_RECORD = struct.Struct("<QQII QI")  # + meta_offset(u64) + meta_length(u32)
```

Pro: single index lookup gives you both file positions. Con: changes the binary format, bumps the version, and the metadata columns are wasted space if you never look them up.

**Option B -- Separate `.meta-index` file** with the same structure (same header, same `node_id` ordering, its own offsets/lengths). This is what "very similar format to the star table" suggests.

Pro: the metadata layer is fully independent and optional -- you can build an octree without it. The main index format doesn't change. Con: two index lookups per cell if you want both (though the second is a binary search on the same `node_id`, so it's cheap).

**Option C -- No separate meta index**, just guarantee that meta-payload cells are in the same order as the main index. Use the main index's `node_id` to know which cell you're at, and a simple offset table or sequential read for the meta file.

Given your description ("very similar format"), **Option B** seems like the right fit. The metadata layer is a complete parallel file set (`meta-payload` + `meta-index`) with the same binary layout conventions, just different payload contents. It's optional, self-describing, and doesn't touch the existing format at all.

### What the metadata payload would look like

Per cell, you'd produce something like:

```json
[
  {"gaia_source_id": 123456789, "hip": 12345, "hd": "HD 1234", "bayer": "α Cen"},
  {"gaia_source_id": 987654321},
  ...
]
```

Gzipped. Stars in the same order as in the render payload. Sparse fields (not every star has a Bayer designation) can just be omitted from that star's object, keeping the JSON compact.

### Size considerations

The render payload is extremely compact: 16 bytes per star, gzipped. The metadata JSON will be significantly larger per star -- a Gaia `source_id` alone is a 19-digit integer. For a cell with hundreds of stars, compressed JSON will probably be 10-50x larger than the render payload. That's fine for an on-demand sidecar, but worth being aware of for disk planning.

You could also consider a more compact encoding than JSON (e.g., a parquet blob per cell, or msgpack), but gzipped JSON is simple, debuggable, and the compression ratio on repetitive JSON keys is quite good.

### How this connects to the Hipparcos merge

This design dovetails nicely with the earlier merge discussion. The pre-stage-00 merge step would produce unified parquet with columns like:

| Column | Type | Notes |
|--------|------|-------|
| `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` | float | Required by pipeline |
| `mag_abs`, `teff` | float | Required/optional by pipeline |
| `source_id` | int64 | Gaia or Hipparcos ID (canonical) |
| `gaia_source_id` | int64 | Null for Hip-only stars |
| `hip_id` | int32 | Null for Gaia-only stars |
| `hd` | string | Null if absent |
| `bayer` | string | Null if absent |
| `catalog_source` | string | `'gaia'` / `'hip'` |

These columns flow through stage 00 untouched (since `sort_shards` copies everything), and stage 01's row source selects them alongside `render` to build the metadata sidecar. The pipeline stays clean: the render path only ever touches its four columns, and the metadata path is a parallel layer that rides the same row stream.

---

**User**

Since we want to include gaia_source_id for these, it's probably more efficient like you suggest, but in general, say we were to drop the gaia source id, or also provide other data, would it not make more sense to map by ordinal?

{
  0: {...},
  10: {...}
}

assuming sparse data?


---

**Cursor**

Yes, absolutely. The sparse ordinal map is much better for this kind of data.

Consider the numbers: a typical cell might have hundreds of stars, but the fraction with Hipparcos IDs is tiny (~120k out of ~1.8 billion Gaia stars), Bayer designations even tinier (~1,500 stars total), HD numbers modest (~360k). For the vast majority of cells, zero stars will have any of these identifiers.

A dense array for a cell with 400 stars where only 1 has a name:

```json
[{}, {}, {}, /* ...397 more empty objects... */, {"bayer": "α Cen", "hip": 71683}]
```

Even gzipped, that's a lot of wasted structure. The sparse ordinal map:

```json
{"399": {"bayer": "α Cen", "hip": 71683}}
```

Is dramatically more compact, and semantically clearer -- the presence of a key *means* something.

### Taking it further: skip empty cells entirely

With a separate meta-index file (Option B from earlier), you don't need to write a meta-payload entry for cells where no star has metadata. The meta-index would simply have fewer records than the main index. A lookup miss (binary search on `node_id` finds nothing in the meta-index) tells you "no star in this cell has metadata" -- no I/O into the meta-payload at all.

For a dataset with ~1.8 billion stars and maybe ~400k that have any interesting identifier, the vast majority of cells at deep levels would have zero metadata entries. The meta-payload and meta-index files would be tiny compared to the main files.

### What about `gaia_source_id`?

This is the key design fork. `gaia_source_id` is dense -- virtually every star has one. If you include it in this same sparse-ordinal JSON layer, you've lost the sparsity advantage: every star gets an entry, every key is just a sequential integer, and the ordinal map degenerates back into a dense array with extra overhead from stringified integer keys.

Two clean ways to handle that:

**Option 1 -- Keep `gaia_source_id` out of the metadata sidecar entirely.** If the source ID is only needed for cross-referencing (e.g., "look up this star in the Gaia archive"), you could store it as a separate dense binary layer -- a flat array of `int64` values, one per star, same order as the render payload. Fixed-width, no JSON overhead, compresses well. This would be a third file type (`.source-id-payload`), not mixed into the sparse JSON.

**Option 2 -- Just accept the density for `gaia_source_id`.** Include it in the sparse JSON map. Yes, most cells become dense, but the gzipped JSON of `{"0": {"gaia_source_id": 123...}, "1": {"gaia_source_id": 456...}, ...}` still compresses reasonably well since the keys and field names are highly repetitive. Simpler to implement, one metadata format to deal with.

Option 1 is more space-efficient and lets you keep the sparse JSON truly sparse for the rare identifiers. Option 2 is simpler. Depends on whether the source ID lookup is a hot path (where binary fixed-width matters) or a cold "click on a star" path (where a few extra bytes in compressed JSON don't matter).

### The ordinal itself

One small detail: the ordinal is the star's zero-based position within the cell's render payload, which is stable because the ordering is defined (`mag_abs` ascending within a cell). So the brightest star in the cell is ordinal 0, next brightest is 1, etc. This means the stars most likely to have names (bright stars = low magnitude) will have low ordinals, which is a nice property for the integer keys.

---

**User**

Could it be optional - say if more than 50% have values, then we switch from mapping to list? Consuming applications can just check type, so the lookup code is very simple

---

**Cursor**

Yes, that's elegant. The consumer logic is trivial:

```python
if isinstance(cell_meta, list):
    entry = cell_meta[ordinal]       # dense: direct index
elif isinstance(cell_meta, dict):
    entry = cell_meta.get(str(ordinal))  # sparse: key lookup
```

Or in JavaScript/TypeScript on the client side:

```typescript
const entry = Array.isArray(cellMeta) ? cellMeta[ordinal] : cellMeta[ordinal];
```

Where in JS it's actually the same operation -- array index and object property lookup both work with numeric keys. So the consumer code may not even need the branch depending on the language.

### The encoding decision

At flush time, the encoder counts how many stars in the cell have metadata vs. total stars. If the ratio crosses the threshold, it switches format:

- **>= 50% populated**: emit a JSON list, length equals `star_count`. Stars with no metadata get `null` (or `{}` depending on your preference -- `null` is 4 bytes vs 2 for `{}`).
- **< 50% populated**: emit a JSON object keyed by ordinal string.
- **0% populated**: skip the cell entirely (no entry in the meta-index).

The threshold is a per-cell decision, so you naturally get the right format at each level of the octree. Shallow cells (bright stars, more likely to have names) will tend toward dense. Deep cells (faint stars, Gaia-only) will mostly be skipped or sparse.

### One nuance: `null` entries in the dense case

In the dense list, you want a compact way to say "no metadata for this star." `null` is the most compact JSON token for this:

```json
[null, null, {"hip": 71683, "bayer": "α Cen"}, null, {"hd": "HD 1234"}, null]
```

That compresses very well -- runs of `null` are highly repetitive.

### No format flag needed

Since JSON is self-describing (object vs. array is detectable by the first byte after decompression: `{` vs `[`), you don't even need a flag in the index record or header. The consumer just decompresses and checks the type. Clean and simple.

---

**User**

So we should always have one sidecar with all the data source IDs so we can later use for lookups - I guess there is a question of whether we index on octree / location or actual source id - or we write both. Any thoughts?

---

**Cursor**

Good question. These are two fundamentally different access patterns, and they have very different design implications.

### The two directions

**Forward (octree → source_id)**: "I clicked on this star at cell `(level, node_id)`, ordinal 3. What is it?"
This is the interactive/rendering hot path. You already know the spatial position. You need identity.

**Reverse (source_id → octree position)**: "Find Gaia DR3 5853498713190525696 in the octree."
This is search/navigation. You have an identifier. You need the spatial location.

### Forward is straightforward

This is the dense source_id sidecar we've been discussing. Same cell structure as the main octree, one `int64` per star. Produced in the same pass during stage 01. At 8 bytes per star, gzip-compressed per cell, it's modest in size. Lookup is: find cell in meta-index → decompress → read the `int64` at position `ordinal`. Fast, simple, done.

### Reverse is a different beast

A reverse index for ~1.8 billion stars, mapping `source_id → (level, node_id, ordinal)`, is fundamentally a key-value lookup, not a spatial structure. At roughly 24 bytes per entry (8B source_id + 1B level + 8B node_id + 4B ordinal + padding), that's ~43 GB uncompressed. It doesn't benefit from the octree's spatial partitioning at all.

More importantly, the access pattern is completely different. The octree is traversed spatially (DFS, frustum culling, LOD). A reverse lookup is a single-key probe. Trying to shoehorn it into the octree file format doesn't buy you anything.

### My suggestion: write both, but as separate artifacts

**1. Forward sidecar** (produced during stage 01, part of the octree file set):
- Dense source_id array per cell, same order as render payload
- Octree-indexed (same `.meta-index` / `.meta-payload` structure)
- For click-to-identify

**2. Reverse index** (produced as a post-build step, separate artifact):
- Sorted by `source_id`
- A flat binary file of fixed-size records: `(source_id, level, node_id, ordinal)`
- Binary search for lookup
- Or just a DuckDB/SQLite table — it's a classic database problem

The reverse index doesn't need to follow the octree file conventions. It could be as simple as:

```sql
CREATE TABLE star_index (
    source_id  BIGINT PRIMARY KEY,
    level      TINYINT,
    node_id    BIGINT,
    ordinal    INTEGER
);
```

Built by scanning the forward sidecar after the octree is complete, or produced as a side output during stage 01.

### For client-side search, you probably don't need the full reverse index

Users won't search by raw Gaia source_id in most cases. They'll search by name: "Sirius", "α Centauri", "HD 48915". The stars with human-meaningful identifiers number maybe ~400k at most. A search index for just those is tiny — a few MB of JSON or a small SQLite database. You could produce this from the sparse metadata sidecar as a post-processing step.

So the practical architecture might be three layers:

| Artifact | Size | Produced | Use case |
|----------|------|----------|----------|
| Forward source_id sidecar | moderate | Stage 01 (same pass) | Click on star → get ID |
| Sparse name/identifier sidecar | small | Stage 01 (same pass) | Click on star → get name, HD, Bayer |
| Search index (named stars only) | tiny (~few MB) | Post-build | Search "Sirius" → navigate to star |

The full reverse index (source_id → position for all 1.8B stars) is only needed if you have a use case like "paste a Gaia source ID, jump to that star." That's a power-user feature you can serve from a database rather than shipping to the client.

---

**User**

Ok - so we only need the 'forward' octree sidecar - in any case the 'full' index can be built from that. 

---

**Cursor**

Exactly right. The forward sidecar is the primary artifact, and any reverse index is just a derived view of it. You can always scan the sidecar after the build and produce whatever lookup structure you need -- sorted binary file, SQLite table, a small JSON index of named stars, etc. No need to bake that into the pipeline.

So the deliverables from stage 01 become:

1. **Main octree** (existing): `.index` + `.payload` per shard — render bytes
2. **Source ID sidecar**: `.meta-index` + `.meta-payload` per shard — dense source IDs, same cell order
3. **Sparse names sidecar**: `.names-index` + `.names-payload` per shard — sparse ordinal map or dense list (auto-switching), only cells with at least one identified star

Or you could fold 2 and 3 together into a single sidecar where the JSON includes source_id alongside names -- the density threshold would push it to list format in most cells anyway since every star has a source_id. That's simpler (one sidecar, not two), at the cost of JSON overhead on source_id vs. raw `int64`.

Either way, the key architectural point is clean: the forward sidecar is produced in the same pass as the main octree, shares the same cell ordering contract, and everything else is downstream.

---

**User**

At what stage in our current process should we build the sidecar?

---

**Cursor**

Stage 01 -- in the same pass that builds the main octree intermediates. It's the only place that works cleanly.

Here's why:

**The ordering contract is the hard constraint.** The sidecar must have stars in exactly the same order as the render payload. That order is determined by the DuckDB query in `row_source.py` -- `ORDER BY node_id, mag_abs`. If you try to build the sidecar in a separate pass, you need to guarantee identical row ordering across two independent queries. As we discussed earlier, without a stable tiebreak, stars with the same `(node_id, mag_abs)` could come back in different order. Even with a tiebreak, running the same expensive query twice is wasteful.

**The row source already touches every star.** Stage 01 streams every row through `iter_sorted_rows` → `iter_encoded_cells` → `write_cell`. The metadata columns (source_id, plus any name/identifier columns) just need to ride along in that same stream. The encoder produces two payloads per cell instead of one, the writer appends to two file pairs instead of one. No extra DuckDB query, no second pass over the data.

**It can't be earlier.** Stage 00 doesn't know the final cell assignment -- it sorts and splits by magnitude band, but the actual `(level, node_id, ordinal)` identity of each star isn't determined until stage 01 derives `node_id` from `morton_code >> shift` and groups rows into cells. The sidecar's ordinals are meaningless without that grouping.

**It can't be later.** After stage 01, the render payloads are opaque compressed blobs. There's no way to recover which star is which without re-running the query. You'd need to correlate back to the source parquet, re-derive the ordering, and hope it matches. Fragile and pointless when you can just do it in the same pass.

### What changes in stage 01

The modifications are contained:

- **`row_source.py`**: `SELECT` adds the metadata columns alongside `render`. Yield tuples become `(node_id, render, meta_dict)` instead of `(node_id, render)`.
- **`encoder.py`**: `_flush_cell` produces both a render `EncodedCell` and a meta `EncodedCell` (the JSON payload, gzipped). The sparse-vs-dense threshold decision happens here at flush time.
- **`writer.py`**: `IntermediateShardWriter` manages a second file pair (`.meta-index` + `.meta-payload`) alongside the existing pair. Same `write_cell` contract, same ordering guarantees, same `node_id` in both index files.
- **`manifest.json`**: Gains `meta_index_path` and `meta_payload_path` fields per shard entry.
- **`build.py`**: No structural change -- the loop is the same, it just writes two things per cell instead of one.

The metadata columns need to exist in the stage 01 input parquet, which means they need to survive stage 00. Since `sort_shards.py` does `SELECT *`, any columns present in the upstream parquet pass through automatically. So the only upstream requirement is that the pre-stage-00 data (or the merged Gaia+Hipparcos output) includes the identifier columns.

