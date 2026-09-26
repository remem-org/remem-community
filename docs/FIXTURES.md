# Fixture corpora

Four deterministic reference corpora for the Go rewrite (`.superpowers/sdd/2026-09-01-go-rewrite-implementation-plan/`,
Task 0.3; profile table from Part I.7 Q24), minus `legacy-format` (ruling
R12, deferred to Phase 12). They exist for the differential harness (Part
IV.2): the Rust and Go implementations import the same corpus and their
outcomes are compared, so "same seed, same corpus, byte for byte" is what
makes a divergence between them mean something.

Every corpus is built straight against a `StorageEngine`
(`crates/remem-server/src/fixtures/`) rather than posted over the REST API.
`services::connection_manager` rejects self-edges and an archived edge
source, and `CreateMemoryRequest` has no field for `created_at`, `archived`,
or `flashbulb_until` — the pathological corpus needs all of these, and they
are exactly the states a long-lived deployment accumulates that the write
path is designed to prevent.

## Profile table

| Profile | Records | Edges | Notes |
|---|---|---|---|
| `tiny` | 50 | 120 | Unit tests, fast CI |
| `typical` | 10,000 | 35,000 | Differential harness default |
| `large` | 250,000 | 875,000 | Performance and residency validation. **Defined, not generated** — see below |
| `pathological` | 5,000 | 8,000 | Every case in [Edge cases](#edge-cases) below |

`large`'s edge count is 250,000 records at `typical`'s own ratio (35,000 /
10,000 = 3.5 edges/record) — "defined, not generated" still means the
definition is a real number, not a placeholder.

## Determinism

Same `--seed`, same profile, byte-identical corpus, checked by
`crates/remem-server/src/fixtures/fixture_tests.rs`:

- every record and edge-case id is `Uuid::new_v5(FIXTURE_NAMESPACE,
  "<seed>:<profile>:<name>")`, never `new_v4` (which reads the OS RNG and
  can never repeat);
- every timestamp derives from `BASE_TS_MS` — a fixed constant
  (`1_700_000_000_000`), never `now_ms()` — plus a fixed offset, so two runs
  a year apart still produce identical bytes;
- every synthetic embedding is drawn from
  `rand::rngs::StdRng::seed_from_u64(seed)`, consumed in a fixed order.
  Vectors are synthetic on purpose: no ONNX model, no running server — a
  fixture's job is to exercise the export/import path, and the differential
  harness compares both implementations against the same vectors, so
  semantic realism buys nothing.

The pinned value for a corpus is **the corpus digest**
(`remem-fixture --profile <p> --seed <n> --data-dir <dir> --print-digest`,
printed as `digest: <sha256 hex>`), not a checksum of the exported `.rsnap`
file. The file's own header carries `created_at_unix_ms` (wall-clock, set by
`snapshot::export` at export time) and the crate version, so two exports of
the same corpus a day apart are byte-different files even though they
describe the same data — the digest never sees either field. It is computed
directly from the engine: every record, vector and outgoing edge reachable
from it, sha256'd in physical-key order (`fixtures::corpus_digest`).

## Edge cases (`pathological`)

Every one of these is asserted against, by reading back through the engine,
in `pathological_contains_every_documented_edge_case`
(`crates/remem-server/src/fixtures/fixture_tests.rs`):

- unicode content — multi-byte characters, a combining mark (`e` + U+0301
  rather than precomposed `é`), and emoji
- an empty tag list
- a 120-byte tag — past the tag index's hardwired `max_token_length` of 100
  (`engine::index::inverted::InvertedIndexConfig::default`); stored in the
  record payload, silently excluded from the tag index. Go has no such limit
  since Phase 8: a long term is folded to a bounded key rather than dropped, so
  this tag filters exactly on import (§II.10 row 2)
- an orphan edge whose target record was hard-deleted — the target is
  `put`, edged, then removed with the raw `StorageEngine::delete` (KV only,
  never `remove_from_indexes`, which also strips every edge touching the
  node — that's `cleanup_archived`'s real production path and would delete
  the very edge this case exists to leave behind); the target was never
  added to the time index either, so nothing else in the corpus ever
  enumerates it
- a self-edge — `add_edge(k, k, ...)`; unreachable over REST
  (`services/connection_manager.rs:59` rejects `source_id == target_id`)
- `importance` at exactly `0.0` and at exactly `1.0`
- `health` at exactly `0.0`
- `flashbulb_until` in the past and in the future, both relative to the
  fixed `BASE_TS_MS` anchor
- archived records both inside and past the 30-day `cleanup_archived`
  window (`updated_at` = `BASE_TS_MS - 5 days` and `BASE_TS_MS - 40 days`);
  both have no vector, mirroring `MemoryRepository::retire_vector_in`
  retiring an archived memory's vector from the similarity index

## Generating a corpus

```bash
# Build the remem-fixture binary once (inside the CI container — cargo
# cannot run on the host, see crates/remem-server/CLAUDE.md).
docker run --rm --security-opt label=disable \
  -v "$PWD":/workspace -v remem-ci-target:/cargo-target \
  -e CARGO_TARGET_DIR=/cargo-target -e LD_LIBRARY_PATH=/ort-libs -e ORT_LIB_LOCATION=/ort-libs \
  remem-ci-builder:latest cargo build -p remem-server --bin remem-fixture

# Build + export one profile, and print its corpus digest.
docker run --rm --security-opt label=disable \
  -v "$PWD":/workspace -v remem-ci-target:/cargo-target \
  -e CARGO_TARGET_DIR=/cargo-target -e LD_LIBRARY_PATH=/ort-libs -e ORT_LIB_LOCATION=/ort-libs \
  -w /workspace remem-ci-builder:latest \
  /cargo-target/debug/remem-fixture \
    --profile tiny --seed 42 \
    --data-dir /workspace/fixtures/tiny \
    --out /workspace/fixtures/tiny.rsnap \
    --print-digest
```

`--data-dir` and `--out` land under `/fixtures/` at the repo root by
convention, which is gitignored (`.gitignore`) — generated corpora and
`.rsnap` exports are never committed. `--profile large` refuses immediately
(see below).

The exact commands run to produce the pinned values in this document (seed
`42` throughout, `remem-fixture` built exactly as shown above):

```bash
remem-fixture --profile tiny         --seed 42 --data-dir fixtures/tiny         --out fixtures/tiny.rsnap         --print-digest
remem-fixture --profile typical      --seed 42 --data-dir fixtures/typical      --out fixtures/typical.rsnap      --print-digest
remem-fixture --profile pathological --seed 42 --data-dir fixtures/pathological --out fixtures/pathological.rsnap --print-digest
```

`large` was **not** run — the command it would be is recorded for the
record, but this tool refuses it outright:

```bash
remem-fixture --profile large --seed 42 --data-dir fixtures/large --out fixtures/large.rsnap
# error: the `large` profile is defined (250000 records, 875000 edges) but is
# deliberately not generated in Phase 0 (multi-GB, not committed to git, not
# published as a release artefact) -- see docs/FIXTURES.md
```

## Pinned values (seed 42)

| Profile | Corpus digest (sha256) | Exported records | Exported vectors | Missing vectors | Exported edges | `.rsnap` size |
|---|---|---|---|---|---|---|
| `tiny` | `f01bb1e7913bd0f19b81772bd8b51cbbf04fe0c5ec654ae71a07983b61fd673b` | 50 | 50 | 0 | 120 | 73,380 B (~72 KB) |
| `typical` | `04f0f612b3ffca37d5149e716ca9364ab71907c503b5bb7ed84c8736ea7a0f58` | 10,000 | 10,000 | 0 | 35,000 | 14,598,093 B (~14 MB) |
| `pathological` | `f871e5e37caf051d484bbf10d35df497cd3302c8b68b80bb65b47e71f43f4027` | 5,000 | 4,998 | 2 | 8,000 | 7,259,212 B (~7 MB) |
| `large` | not generated | — | — | — | — | — |

`pathological`'s 2 missing vectors are exactly its two archived edge-case
records (see [Edge cases](#edge-cases)) — every other record in every
profile carries a vector.

**The `.rsnap` file itself is not byte-reproducible** even at a fixed seed:
its header carries `created_at_unix_ms` (wall-clock, stamped by
`snapshot::export` at export time) and `source_version` (the crate
version). Re-running the commands above today will produce a `.rsnap` file
with different header bytes but the *same* corpus digest — that agreement,
not a file checksum, is what "the fixture didn't drift" means. The digests
above are independently reproduced by
`same_seed_produces_the_same_corpus_digest` in
`crates/remem-server/src/fixtures/fixture_tests.rs`, which builds a profile
into two separate temp directories and asserts the digests match.

## Publishing

Publishing a `fixtures-v1.tar.zst` release artifact is out of scope for
Task 0.3 (outward-facing; ruling R10) — this document exists so a future
task can reproduce the exact bytes above, not so any of them are shipped
from here.
