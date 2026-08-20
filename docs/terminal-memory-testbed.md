# Terminal-packed octree memory testbed

`fis-octree benchmark terminal-memory` evaluates terminal-packed STAR v2
layouts without building or rewriting a STAR v2 artifact. It extracts complete
subtrees from an existing STAR v1 octree, preserves the natural logical cells
and magnitude ordering, forms virtual terminal nodes, and replays observer
views through three policies:

- `v1`: current external tree and payloads.
- `terminal-monolithic`: a selected logical payload materializes its complete
  terminal subtree.
- `terminal-magnitude-chunked`: a terminal-wide absolute-magnitude ordering is
  cut using the terminal AABB and materialized in independent chunks.
- `terminal-logical-chunked`: each selected original logical payload
  materializes only its independently decompressible safe magnitude-prefix
  chunks.

Natural leaf payloads are not marked terminal. A terminal is created only when
an existing node has descendants and its complete subtree contains no more than
the configured waterline.

## Published-octree samples

Use a sample point and level that bound a complete, tractable subtree:

```console
uv run fis-octree benchmark terminal-memory \
  https://data.foundin.space/c56103e6-ad4c-41f9-be06-048b48ec632b/stars.octree \
  --sample sparse-10k:10000,1,1@9 \
  --waterline 512 \
  --waterline 1000 \
  --waterline 2000 \
  --chunk-stars 128 \
  --chunk-stars 256 \
  --chunk-stars 512 \
  --cache-dir /tmp/fis-terminal-memory-cache
```

The first run reads every payload in the sampled subtree to obtain exact star
counts and magnitude ordering. `--cache-dir` stores that extraction, so
subsequent policy and trace sweeps do not access the published payloads again.
The cache identity includes the source, dataset UUID, STAR index offsets, and
local file size/modification time where applicable.

Without `--trace`, each sample is evaluated as a single observer view at its
sample point and the artifact's index magnitude.

## Observer traces

A trace is an ordered JSON `views` array:

```json
{
  "views": [
    {
      "name": "sun-6.5",
      "observer_pc": [0, 0, 0],
      "limiting_magnitude": 6.5
    },
    {
      "name": "outbound-1",
      "observer_pc": [50, 0, 0],
      "limiting_magnitude": 6.5
    },
    {
      "name": "outbound-2",
      "observer_pc": [100, 0, 0],
      "limiting_magnitude": 5.0
    }
  ]
}
```

Pass it with `--trace path/to/trace.json`. Trace replay is stateful: raw
payloads and index records accumulate, while decoded entries use the configured
LRU budget. This exposes route-dependent growth and reuse that a single-view
estimate cannot.

## Memory model

The default model follows the current SkyKit browser pipeline:

| Allocation | Bytes per star |
| --- | ---: |
| Decompressed payload cache | 16 |
| Decoded positions, temperature, magnitude | 17 |
| Default scaled render-position copy | 12 |
| Aggregate renderer CPU geometry | 17 |
| GPU geometry | 17 |
| Live total | 79 |

The replay also includes:

- 20-byte STAR v1 external index records.
- 24-byte assumed STAR v2 records with star counts.
- Configurable terminal-directory records, defaulting to 24 bytes per natural
  logical payload.
- Unbounded raw-payload and index-record caches, matching current behavior.
- A configurable decoded LRU, defaulting to 64 MiB.
- Compressed input plus duplicate raw inflate chunks for the largest concurrent
  entry wave.
- Prior live cell, CPU geometry, and GPU geometry overlap when the active set
  changes.

`resident_bytes` is the estimated long-lived state after a view settles.
`peak_bytes` adds inflate scratch and view-transition overlap.
`minimum_geometry_rebuild_bytes` assumes one aggregate rebuild per changed
view; progressive delivery can rebuild more often.
`max_storage_entry_rows` and `max_atomic_live_bytes` isolate the largest
single payload or chunk admission from the total active set.

Each waterline plan includes up to 32 of its largest terminal roots with their
logical key, center, half-size, row count, payload count, and depth span. These
coordinates can be used to construct traces that deliberately enter or pass
near packed terminals.

## Interpreting results

The waterline and chunk size answer different questions:

- Waterline controls how much external tree structure is eliminated and the
  maximum monolithic terminal size.
- Chunk size controls the atomic fetch, inflate, decode, and render admission
  when the terminal is range-addressable.

`admissible_rows` is a conservative magnitude prefix based on the nearest point
of each original logical cell. It cannot discard a potentially visible nearby
star. `active_rows / admissible_rows` is reported as the overfetch ratio.
Terminal-wide magnitude chunks use the terminal root's nearest distance
instead. This directly measures the extra memory caused by losing the tighter
logical-cell bounds; a terminal containing the observer cannot safely truncate
its global absolute-magnitude stream.

Compressed sizes for monolithic terminals are the sum of their existing v1
members. Chunk compressed sizes are proportional estimates from the containing
v1 payload. The testbed therefore treats request bytes as secondary; a physical
STAR v2 prototype is still required to measure recompression and range
coalescing exactly.

Index memory is a record-level model, not a final shard-layout prediction.
Sample results are scoped to the extracted subtrees and should not be presented
as whole-catalogue totals.

Use `--json` for machine-readable output suitable for plotting or comparing
committed benchmark runs.
