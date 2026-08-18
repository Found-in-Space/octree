# Identity Locator v2 Production Benchmark

## Result

The production benchmark on 2026-08-18 selected **32 KiB raw** for locator v1.
It had the lowest reader-cold exact-lookup median. The 64 KiB raw candidate was
within the 5% tie window, so the lower p95 transferred-byte result selected
32 KiB raw.

The benchmark input was
`fog-pipeline-test-20260730-2c5c660/products/identifiers-v2.order`:

- source SHA-256: `40ff6378684c5439f979e6615a570bfc9024ba661536dd53f01a24c3f762dcb3`
- parent dataset UUID: `74d8e0ae-6fcc-4c54-934f-42f5f2b8c550`
- identifiers/order UUID: `b08409c1-f960-40a3-acd7-5d7540ebc65d`
- Gaia keys: 1,467,646,815
- HIP keys: 117,954

## Candidate Results

Cold means a new reader and empty per-reader range cache. Warm measurements
reuse one reader. Each candidate used three repetitions of deterministic
uniform, clustered, present, and absent workloads.

| Page / codec | Size (bytes) | Cold median (µs) | Warm median (µs) | p95 bytes | SHA-256 |
| --- | ---: | ---: | ---: | ---: | --- |
| 32 KiB raw | 23,547,351,216 | 141.124 | 64.244 | 78,912 | `805cf60ff2ed501aa90e88dae3838236c09367a6ba506ed566c0bf08628b01e8` |
| 32 KiB gzip | 11,336,907,055 | 184.117 | 107.414 | 63,193 | `9d13db853ae99d7adfe15c1b99fc09e27c869a2b17a3058927a3cdc359c63f10` |
| 64 KiB raw | 23,515,782,624 | 145.813 | 74.814 | 135,008 | `bf29210940a2a27177a319844442e0da622d4d755e059c536ef5a6465f35c0c7` |
| 64 KiB gzip | 11,255,374,649 | 227.944 | 156.563 | 103,493 | `06bf070daa688df1aac7335e4352eddcbc277b7d7694fd8a8a21550d2b127057` |

Gzip candidates used independently decodable deterministic pages with
`compresslevel=1` and `mtime=0`.

## Workload Medians

Times below are microseconds. Absent boundary lookups terminate from namespace
bounds after bootstrap; present lookups traverse the B+tree, decode one leaf,
and read one fixed-width identity-directory record.

| Page / codec | Uniform present cold | Clustered present cold | Uniform absent cold | Clustered absent cold |
| --- | ---: | ---: | ---: | ---: |
| 32 KiB raw | 229.622 | 269.691 | 52.627 | 50.148 |
| 32 KiB gzip | 317.667 | 424.935 | 50.567 | 49.747 |
| 64 KiB raw | 236.798 | 373.598 | 54.828 | 50.349 |
| 64 KiB gzip | 403.595 | 640.236 | 52.293 | 50.068 |

## Structural Acceptance

Every candidate passed whole-prefix SHA-256 validation, every encoded page
checksum, full tree ordering/fence/count validation, namespace content hashes,
20 deterministic present-key round trips through original identity payloads,
and four absent boundary proofs. The winning artifact has:

- locator UUID: `12a79f48-1099-57b3-9d84-5b5078361164`
- 716,683 leaf pages and 527 navigation pages
- 23,484,236,304 decoded record bytes
- zero duplicate, rejected, or skipped-namespace rows

Build-phase timings in the machine-readable reports are scoped to the
successful resumed invocation. The shared scan and merge checkpoints survived
two deliberate/observed restarts; candidate encoding plus exhaustive validation
ranged from about 19 to 58 minutes depending on candidate and whether shared
merge work was completed in that invocation.
