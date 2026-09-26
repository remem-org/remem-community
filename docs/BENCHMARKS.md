# Benchmarks: Go against Rust on one host

Plan Phase 13's performance criterion is write throughput and search latency
within **2×** of Rust Remem at `pre-go-freeze`, measured on the same host. Spec
§55 is the bar and parity is not: a Go number better than Rust's is not a goal,
and a worse one is a finding to explain rather than a number to hide.

| | Rust (mean of 3 runs) | Go (mean of 3 runs) | Go against Rust | Criterion |
|---|---|---|---|---|
| Write path | 464.0 creates/s | 278.1 creates/s | 1.67× slower | **met** |
| Vector search, `normal` | 21.66µs | 137.8µs | 6.4× slower | **not met** |
| Vector search, `phantom_heavy` | 21.57µs | 156.1µs | 7.2× slower | **not met** |
| Vector search, `selective_filter` | 61.38µs | 351.3µs | 5.7× slower | **not met** |

Writes meet the criterion and search does not. What remains of the search gap,
and why, is [below](#what-the-search-gap-is-made-of).

## Host and method

- **Host:** Intel Xeon W-2135 (6 cores, no SMT), 62 GB, Linux 7.2.4, btrfs on
  NVMe. Rust's data directories and Go's are both under `.benchmark-data/`, on
  the same filesystem. Rust's runs and Go's write-path runs had the host to
  themselves. Go's final vector runs shared it with two single-threaded recall
  measurements, and alternated with runs of the code before the change, so the
  before and after figures saw the same load.
- **Rust:** the frozen tree at `pre-go-freeze` (`1fa61f9`), built and run in
  `remem-ci-builder:pre-go-freeze` (Rust 1.85, ONNX Runtime 1.18.1), Criterion
  0.5.1 with 100 samples. Its benchmarks are `benches/write_path.rs` and
  `benches/vector_retrieval.rs`.
- **Go:** natively, `bench/` at the end of Stage 4. `BenchmarkWritePath` with
  `-benchtime=10x` (one operation is 1,000 synced creates), and
  `BenchmarkVectorRetrieval` with `-benchtime=5s`. Every run used a fresh data
  directory.
- **What is compared.** Criterion reports a mean per operation and records
  per-sample means, never per-request times, so **means are the primary
  comparison**. Go also records every request's duration and reports p50 and
  p99. Those are listed beside Rust's *worst sample mean* (run 3, the one run
  whose sample data was copied out, see the plan's Task 4.2 notes). That is
  labelled for what it is, and **it is not a p99**.

## Write path

1,000 creates in 20 synchronised waves of 50 through each server's in-process
HTTP router. Durable writes, the real embedding model, discovery jobs enqueued
and not run, HNSW at M 16 and construction ef 200 on both sides.

| Run | Rust creates/s | Go creates/s | Go p50 request | Go p99 request |
|---|---|---|---|---|
| 1 | 449.3 | 277.9 | 137.5 ms | 202.0 ms |
| 2 | 476.3 | 278.1 | 137.9 ms | 198.9 ms |
| 3 | 466.6 | 278.4 | 136.9 ms | 200.0 ms |
| **Mean** | **464.0** | **278.1** | | |

Rust's worst sample in run 3 took 3.690 s for 1,000 creates (271 creates/s); its
mean sample took 2.143 s. Go's mean operation took 3.596 s.

Go started Stage 4 at about 121 creates/s, 3.8× behind. Three changes closed it,
each profiled before it was made:

| Change | Creates/s | What the profile showed |
|---|---|---|
| Stage 4 start | ~121 | |
| `perf(write)`: fsyncs shared across concurrent writes, cheaper cosine | ~182 | Every synced commit fsynced alone under the store's write mutex; the tenant gate was held through the sync; 94% of an HNSW insert was cosine distance recomputing norms |
| `perf(hnsw)`: a neighbour list overflows by half before it is pruned | 278 | 100% of the remaining lock wait was an HNSW insert holding its tenant's graph lock, and 81% of an insert was the pruning heuristic |

The pruning change raised recall rather than trading it away. The measurements
are in `docs/architecture/vector.md`.

## Vector retrieval

200 memories with 4-dimensional vectors, a page of 10, HNSW at M 8, construction
ef 64, search ef 64, L2. `phantom_heavy` deletes four records in five first.
`selective_filter` makes one in five long-term and searches for long-term
memories only.

Mean per query:

| Fixture | Rust run 1 / 2 / 3 | Go run 1 / 2 / 3 | Rust mean | Go mean |
|---|---|---|---|---|
| `normal` | 21.82 / 21.48 / 21.67 µs | 141.9 / 125.1 / 146.4 µs | 21.66 µs | 137.8 µs |
| `phantom_heavy` | 21.49 / 21.73 / 21.50 µs | 153.2 / 157.5 / 157.7 µs | 21.57 µs | 156.1 µs |
| `selective_filter` | 61.79 / 61.27 / 61.08 µs | 294.9 / 414.8 / 344.1 µs | 61.38 µs | 351.3 µs |

Go's per-query distribution, beside Rust's worst sample mean:

| Fixture | Go p50 (runs 1 / 2 / 3) | Go p99 (runs 1 / 2 / 3) | Rust worst sample mean (not a p99) |
|---|---|---|---|
| `normal` | 0.140 / 0.127 / 0.140 ms | 0.503 / 0.498 / 0.504 ms | 23.894 µs |
| `phantom_heavy` | 0.143 / 0.144 / 0.143 ms | 0.530 / 0.511 / 0.510 ms | 22.045 µs |
| `selective_filter` | 0.212 / 0.366 / 0.349 ms | 1.369 / 1.005 / 1.177 ms | 62.721 µs |

`selective_filter` varies more between Go runs than anything else measured here,
295 to 415 µs, which is why its runs are listed rather than only averaged.

Before Stage 4's search changes, measured the same way, Go took 198.9, 186.3 and
537.3 µs, allocating 617 times a query on `normal`. The changes:

- **`perf(search)`, the attribute row and the fingerprint.** Rows were decoded
  into a map, and the cursor fingerprint reflected over every vector component.
  Row decoding went from 7 allocations to 3, and the fingerprint from 391
  allocations to 8. The query moved little, because neither was where the time
  went.
- **`perf(search)`, the page's reads.** Each result's vector was read and never
  used. Every point read also built and tore down a Pebble iterator of its own,
  about 2 µs where a reused iterator's seek costs 0.9 µs. `normal` fell 31% and
  `selective_filter` 35%.

## What the search gap is made of

Profiled on `normal` after Stage 4's changes, at 149 µs a query. Shares are of
the time spent inside `memory.Service.search`:

| Where the time goes | Share |
|---|---|
| The HNSW walk (`hnsw.Index.Search`) over 200 nodes at ef 64 | 31% |
| Decoding each result's record body (protobuf) | 20% |
| Decoding each candidate's attribute row, to settle `archived = false` | 11% |
| Point reads through the request's iterator, rows and bodies together | 15% |
| The cursor fingerprint (a SHA-256 over the query and its vector) | 8% |
| Converting records to results | 4% |

The garbage collector took a further 15% of CPU on other threads.

No single cause is left. What remains is the shape of the design, not a defect:
a Go search pins an LSM snapshot, reads and decodes a stored row for every
candidate it filters and a stored body for every result, and hashes the query so
a cursor can be checked against it. Rust was not profiled, so this document does
not say where Rust's 21.7 µs goes.

Closing the rest would take a resident per-tenant cache of decoded records and
attribute rows, invalidated on write. That is a design change with its own
invalidation risk, not a tuning step, and it is recorded as follow-up work
rather than done in this phase. It would also be measured differently: at 200
records this fixture measures per-query overhead and nothing about scale. The
scale measurements are in `docs/architecture/vector.md`.

## Differences between the two measurements

- **Where search is measured.** Rust's benchmark calls its query engine directly.
  Go's calls `memory.Service.Search`, the nearest equivalent, which also does the
  following. Rust does none of the first three:
  - calls the embedder, a fixed 4-dimensional one here;
  - pins a Pebble snapshot in a paging session;
  - fingerprints the query so a cursor can be checked against it;
  - reads and decodes each result's record, which Rust's query engine also
    returns.
- **Where state lives.** Go's reads go through an LSM snapshot and decode
  protobuf. Rust's benchmark runs with durable writes off, and its read path was
  not profiled here.
- **Containers.** Rust ran in a container and Go natively, on the same
  filesystem. Neither benchmark crosses a network.
- **`phantom_heavy`.** Rust leaves deleted records in the HNSW graph as
  phantoms, which a search has to widen past. Go has none by construction
  (`docs/architecture/vector.md`), so this fixture measures that claim rather
  than a repair.
- **The write path's search effort.** Go's is ef 64 and Rust's fixture is 50.
  Creates never search, so this does not reach the numbers.
- **Embedding.** Both sides run `all-MiniLM-L6-v2`, under different ONNX
  Runtime versions (Rust's 1.18.1, Go's 1.29.0). Rust takes the first token's
  output (CLS pooling); Go averages all token outputs (mean pooling, §II.10
  row 16). Pooling is a vector average against a full model run.
- **Samples.** Rust's Criterion takes 100 samples of each benchmark. Go takes 10
  operations of the write path and runs vector retrieval for 5 seconds.
