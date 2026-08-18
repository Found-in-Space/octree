# Identity Locator v1 — Dataset v2 Benchmark

## Result

The second locator-v1 iteration uses compact numeric leaves with ID deltas,
page-local cell dictionaries, bit-packed ordinals, and absolute key
checkpoints every 32 records. The production logical page capacity remains
2,048 records (32 KiB in the semantic `<QII>` representation).

The benchmark input was
`fog-pipeline-test-20260730-2c5c660/products/identifiers-v2.order`:

- source SHA-256: `40ff6378684c5439f979e6615a570bfc9024ba661536dd53f01a24c3f762dcb3`
- parent dataset UUID: `74d8e0ae-6fcc-4c54-934f-42f5f2b8c550`
- identifiers/order UUID: `b08409c1-f960-40a3-acd7-5d7540ebc65d`
- Gaia keys: 1,467,646,815
- HIP keys: 117,954

The resulting compact production artifact is 7,825,090,541 bytes, down from
23,547,351,216 bytes: a 66.77% reduction. Its exact SHA-256 is
`61bc0add282d43e4923c1a091e5a305b40118102d5346f0ab9614f0e2d8320a5`
and its locator UUID is `d8dc88ce-9f3b-565f-92fc-173f2286efd8`.

A 4,096-page uniform Gaia sample plus every HIP page had projected the selected
compact layout at 7,817,855,406 bytes, within 0.10% of the complete artifact.
The same sampling method projected deterministic gzip at 11,336,531,773 bytes;
its projection differed from the actual full gzip candidate by only 0.0033%.

## Restart-Block Choice

All candidates used 2,048-record leaves. Size is a full-artifact projection;
timings are direct warm leaf-body lookups over the sampled production pages.

| Restart block | Projected size (bytes) | Present median (µs) | Present p95 (µs) |
| ---: | ---: | ---: | ---: |
| 32 | 7,817,855,406 | 23.97 | 32.95 |
| 64 | 7,652,811,704 | — | — |
| 128 | 7,570,289,853 | — | materially worse tail |

Block 32 costs about 165 MB over block 64, or 2.2% of the compact artifact,
while bounding each point lookup to at most 31 decoded deltas. That latency
bound was selected over the small additional size reduction.

## Production Lookup Result

The final artifact used the same deterministic 36-lookup workload and three
repetitions as the first iteration.

| Metric | First iteration, raw | Compact block 32 |
| --- | ---: | ---: |
| Reader-cold median | 141.124 µs | 137.042 µs |
| Reader-warm median | 64.244 µs | 47.220 µs |
| p95 transferred bytes | 78,912 | 60,059 |
| Average encoded leaf | 32,831.96 bytes | 10,894.42 bytes |

The size reduction therefore did not require a point-lookup latency regression
on the production sample.

## Encoding Checks

A complete streaming pass over all 1,467,646,815 Gaia keys proved that every
key has seven zero low bits, so Gaia pages can losslessly omit those bits.
The `identifiers.order` directory contains 5,607,279 cells; the largest cell
has 39,410 stars. Therefore current production values need at most 23 bits for
the cell record and 16 bits for the ordinal, while the self-describing page
format still supports their full `u32` domains.

The compact implementation preserves the semantic namespace checksum over the
original little-endian `<QII>` tuples. Every leaf has its own envelope checksum,
and the artifact retains its whole-prefix SHA-256 footer.

The complete publication validation decoded and checked all 716,683 leaf
pages, 527 navigation pages, and 1,467,764,769 records. It verified tree
ordering and fences, reference bounds, both semantic namespace checksums, the
whole-prefix checksum, 20 present-key round trips, and four absent-key proofs.

## First-Iteration Baseline

The initial test compared raw and deterministic gzip leaves at 32 and 64 KiB.
It selected 32 KiB raw for latency, producing the 23,547,351,216-byte artifact
with SHA-256
`805cf60ff2ed501aa90e88dae3838236c09367a6ba506ed566c0bf08628b01e8`.
That implementation is preserved in repository commit `708a1d9`; it is not a
supported alternate reader format.
