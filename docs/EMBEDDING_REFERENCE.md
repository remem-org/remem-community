# Embedding reference vectors

`fixtures/embedding_reference.json` is a cross-repository contract file
(`.superpowers/sdd/2026-09-01-go-rewrite-implementation-plan/`, Task 0.6,
ruling R9): 200 fixed input strings, each embedded once by the real
`EmbeddingService` (`crates/remem-server/src/embedding/`) and written out
with its full-precision `f32` vector. It exists to answer one question for
the Go rewrite's highest-risk task (Phase 3 Task 3.3): does the Go/cgo/ONNX
embedder produce numerically the same vectors as this codebase's Rust
fastembed integration, for the same model and the same inputs.

The Go implementation reads this file directly as
`internal/embedding/testdata/embedding_reference.json` and re-embeds each
`text`, comparing the two vectors by cosine similarity.

## Model and provenance

Every vector was produced by fastembed 3.14.1's `all-MiniLM-L6-v2` (384
dims) — the only embedding model this codebase runs
(`crates/remem-server/src/snapshot/export.rs`'s `VECTOR_MODEL_ID` names the
same model for the same reason: there is exactly one). The file's top-level
`model_id`, `dim`, `generated_by` (the `remem-server` crate version that
produced it), and `generated_at_unix_ms` record which build made it.

## File shape

```json
{
  "model_id": "all-MiniLM-L6-v2",
  "dim": 384,
  "generated_by": "remem-server 0.1.0",
  "generated_at_unix_ms": 1234567890000,
  "cases": [
    { "text": "the sky is blue", "sha256": "<hex of the utf-8 text>", "vector": [0.0123, ...] }
  ]
}
```

`sha256` is the hex-encoded SHA-256 of `text`'s UTF-8 bytes. It exists so
the Go side can assert it is comparing the same string it thinks it is,
rather than trusting array order to survive a file that crossed
repositories (encoding round-trips, editor whitespace normalization, and
JSON re-serialization have all silently mutated a string in this codebase's
history before).

`vector` is written at full `f32` precision — not rounded, not passed
through a shortened float format. See "Why 0.9999, not exact equality"
below for why that precision matters.

## The 200 cases

The case list lives as a literal array in
`crates/remem-server/src/bin/embedding_reference/cases.rs`, not generated
at runtime from a seed: a literal is reviewable directly and cannot drift
when a random-number generator's implementation changes across a Rust
toolchain upgrade. It covers, with the minimum count noted for each
(several categories cross-cut — the empty string is also "a single word",
the lone space is also "a whitespace string" — so the totals below sum to
exactly 200 rather than leaving headroom):

- 40 ordinary English sentences (5-30 words)
- 20 single words (including the empty string)
- 20 strings with leading/trailing/interior whitespace runs (including a lone space)
- 20 non-ASCII strings: CJK, Cyrillic, Arabic, accented Latin, mixed-script
- 20 emoji and emoji-with-modifier sequences
- 20 long strings (300-600 characters), which exercise truncation at the
  model's 512-token limit
- 20 punctuation / code fragments / URLs / JSON snippets
- 10 numeric and date-like strings
- 20 near-duplicate strings (10 pairs), each pair differing by one
  character — this is where a pooling bug shows up as vectors that are
  individually plausible but wrongly ordered relative to each other
- 10 repeated-token strings (`"a a a a a ..."`)

`crates/remem-server/src/bin/embedding_reference/main.rs`'s own test,
`the_case_list_meets_its_coverage_contract`, checks the list is exactly 200
strings, all unique, and that the empty string, a non-ASCII string, a
≥300-character string, and a leading/trailing-whitespace string are all
present.

## Why 0.9999 cosine, not exact equality

The Go side compares vectors by cosine similarity against a 0.9999
threshold rather than requiring bit-for-bit equality. Two independent ONNX
Runtime builds (Rust's `ort` crate here, Go's cgo binding there) are not
guaranteed to select identical kernels for the same op graph, and `f32`
accumulation order in matrix multiplication and pooling is not associative
— summing the same numbers in a different order produces a different
last-bit result. Both are legitimate floating-point behavior, not a bug,
so exact equality is the wrong bar. 0.9999 is tight enough to catch a real
implementation divergence (wrong pooling strategy, wrong normalization,
wrong tokenizer, truncation at the wrong length) while tolerating kernel-
and accumulation-order noise.

This is also why this generator asserts full precision on the way out:
every digit of `f32` mantissa rounded away here is spent from the same
0.9999 budget before the Go implementation is even involved.

## Regenerating

Requires the ONNX model. `cargo` never runs on the host in this repo (no
`protoc`, `build.rs` panics) — use the same container mounts
`scripts/test-in-container.sh` uses:

```bash
docker run --rm --security-opt label=disable \
  -v "$PWD":/workspace \
  -v remem-ci-target:/cargo-target \
  -v "$PWD/.test-cache/fastembed":/models \
  -e CARGO_TARGET_DIR=/cargo-target \
  -e FASTEMBED_CACHE_PATH=/models \
  -e LD_LIBRARY_PATH=/ort-libs \
  -e ORT_LIB_LOCATION=/ort-libs \
  remem-ci-builder:latest \
  cargo run -p remem-server --bin embedding-reference -- --out fixtures/embedding_reference.json
```

`.test-cache/fastembed` is populated by `scripts/test-in-container.sh`,
which extracts it from a running `remem-server` container's baked-in model
cache if it is not already present.

Regenerating changes every vector's low-order bits even when nothing about
the model or the case list changed (ONNX Runtime and hardware both affect
`f32` accumulation order) — treat this file as an artifact to regenerate
deliberately and review as a diff, not something to casually rerun.
