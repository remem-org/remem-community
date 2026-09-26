# Records, embeddings and retrieval

How a memory is stored, how it is turned into a vector, and how it is found
again. Spec §60 asks these documents to explain invariants rather than APIs, so
that is what this one does; signatures are in the code.

Landed in Phase 3.

## What is authoritative

| Concern | Authority | Everything else |
|---|---|---|
| What a memory says | the record body in the record key space | the projection an API returns is a view, not the truth |
| What a memory means, numerically | the canonical vector in the vector key space | every index is derived from it and is rebuildable |
| Which model produced a vector | the `model_id` stamped on that vector | nothing infers it from the running configuration |
| Whether a memory is retrievable | the `archived` field on the record | search filters on it; the vector survives archiving |
| The order of results | the distance under the configured metric | a score is derived from a distance, never the reverse |

## Canonical and derived, in this phase

| Data | Class | Maintenance | Rebuild source |
|---|---|---|---|
| Record body | canonical | transactional | — |
| Canonical vector | canonical | transactional | — |
| The flat index | *is* the canonical vectors | none | — |

`flat` has no state of its own, and that is the whole reason the walking
skeleton uses it. There is no derived structure to fall out of step with the
canonical rows, no rebuild that can be stale, and no index insert that can fail
after the record has committed. The parts of search that can be wrong are
narrowed down to the parts this phase is actually building.

`flat.Rebuild` is the import and repair exception. It consumes and validates
the complete replacement source before staging replacement and removal of the
tenant's vector rows in one transaction. A source or encoding failure leaves
the canonical vector set unchanged; readers observe either the old set or the
complete replacement.

The cost is linear in a tenant's corpus per query. Phase 7 buys the scale back
against a reference that is known to be right — which is why `flat` gets no
approximation, no tuning parameter and no heuristic. The moment it acquires one
it stops being able to say what the right answer was.

## Atomicity

One transaction contains the record body and its canonical vector. Nothing else
exists yet; when attribute rows, text postings and edges arrive they join the
same transaction (spec §12). A reader never observes half of a write.

`txn.Tx` wraps one `storage.Batch` and exists for three reasons, none of them
abstraction for its own sake:

- a `Batch` has no error on `Set`, so a caller cannot tell a staged write from a
  rejected one;
- a `Batch` can be committed twice, which for a caller that retries means
  re-applying stale writes over fresh ones;
- durability is a per-`Commit` argument on a `Batch`, which makes it a decision
  every call site takes rather than one an operator configures.

A batch of memories commits whole or not at all. A partial batch would leave an
agent with no way to know which of its twenty facts were stored.

## Embedding

**Mean pooling over the attention mask, then L2 normalisation.** Both halves of
that sentence are load-bearing, and both were wrong at some point during Phase 3.

The model's tokenizer configures fixed padding to 128 tokens, so a five-word
memory arrives as 128 ids of which 123 are `[PAD]`. A mask built from the length
of the id slice marks all 128 as real; the model then attends to the padding,
and every vector comes out wrong — at the right width, at the right norm, and
in the wrong place. Nothing but a comparison against known-good vectors catches
that. The mask comes from the tokenizer, the tokenizer's padding is stripped,
and each batch is padded to its own longest sequence.

Normalisation happens after pooling, never before: normalising each token and
then averaging is a different operation with an equally plausible result.

### Remem does not pool the way Rust did

Rust's vectors come from fastembed 3.14.1, which takes token 0 of
`last_hidden_state` — CLS pooling. That is not what all-MiniLM-L6-v2 was
trained for. Measured over the 200 reference strings:

| pooling | unrelated memories (median cos) | near-duplicates | separation |
|---|---|---|---|
| CLS | 0.626 (p95 0.785) | 0.947 | 0.32 |
| mean | 0.074 | 0.851 | **0.78** |

Under CLS two unrelated memories already score 0.63, which makes
`search.similarity_threshold` unusable — the same defect class the behaviour
baseline records at REM-74, where `1/(1+d)` floored at one third. Go mean-pools.
Recorded as plan §II.10 row 16, with its consequence: a Rust snapshot's vectors
are not importable as-is, and Phase 12 re-embeds on import.

The 200-vector fixture is still a gate, on the CLS *projection* of Go's raw
model output. That holds the model file, every tokenizer decision, the tensor
layout and the mask exactly — everything except the one thing decided on
purpose. It passes at cosine 1.0000000 on all 200.

### Model identity travels with every vector

Nothing in Rust Remem recorded which model produced a vector, so changing the
model left every stored vector the right width and the wrong meaning, and search
degraded without ever failing. Here a vector cannot be stored without a model
id — the codec refuses it — and a vector whose declared dimension disagrees with
its value count is corruption rather than merely shorter.

### There is no fallback embedder

A build without the ONNX tag fails to start. The alternative is a server that
starts, produces meaningless vectors, and stamps the real model's name on every
one of them; a durable corpus filled that way cannot be told from a good one
afterwards.

## Scoring: two numbers, deliberately

A **distance** orders results. A **score** is what a caller thresholds on. The
behaviour baseline (§1.1, §1.3, §1.4) records what happens when the two are
confused, and Remem keeps the distinction:

- `Score` is recovered cosine where the metric permits it — exact, because
  embeddings are unit-norm on the way in, so `cos = 1 − d/2` under squared L2.
  It is comparable across requests and is what a threshold acts on.
- `FusedScore` is the value the ordering was decided by. A plain semantic search
  has a single step and no rank fusion, so it is `1/(1+d)` and sits on a
  completely different scale from a hybrid search's.

Both are returned. A client that re-sorts by `Score` gets a different order than
the server intended; a client that shows `FusedScore` to a user is showing a
number that means nothing outside that one response. Returning only one would
make somebody guess which.

**Dot product reports no cosine at all.** The index makes no normalisation
guarantee under that metric, so any cosine derived from it would be a similarity
for vectors that might not be unit-length — a number that looks calibrated and
is not. `CosineFromOK` returns false rather than a plausible value.

## Archiving is a retirement, not a deletion

An archived memory keeps its record, its canonical vector and its export entry.
It leaves search and stays fetchable only when a caller asks for it. Plan §II.10
row 8 turns Rust's opt-in hard delete into a rule: a heuristic must never
destroy user data, and "why did my memory disappear" has to have an answer other
than "it did not".

Hard deletion exists and is a parameter a caller passes, never a threshold
something crosses.

### Why archived memories are filtered by widening

The canonical vector survives archiving — re-embedding on un-archive would be
work with no purpose — so the index still holds it and something has to exclude
it.

The index's filter is a `func(id.ID) bool` and cannot read a record. Rather than
give it one, which would put a storage read inside the scan's inner loop, a
search asks for `limit × widen_max_factor` candidates and filters afterwards.
That is exactly what the widening budget is for. Phase 5 replaces it with a
pushed-down predicate over the attribute row, at which point the widening
becomes an optimisation rather than the mechanism.

## Failure semantics

| Situation | Answer | Why |
|---|---|---|
| A record body that will not decode | `Corruption` naming the key | canonical data is never replaced by a zero value; returning an empty record would silently swap a memory for nothing |
| A body whose id disagrees with its key | `Corruption` | the body carries an id only because an export ships bodies without keys; a disagreement means something wrote a record under the wrong key, and answering with it returns one memory when another was asked for |
| A vector whose record is gone | skipped, silently, during a search | the canonical record is the authority on what exists, and an index catching up is normal rather than an error |
| A stored vector of the wrong width | the search fails | it means a vector from another model reached the index, and comparing the overlapping components would produce a ranking nobody could explain |
| A record with no vector | returned by `Get`, invisible to search | it is what an import carrying no embeddings, or a write whose embedder failed, leaves behind |

## Reads are snapshot-consistent

`Scan` reads from a `storage.Snapshot`, not from the store. Every page of one
listing therefore sees the same state: a record written between page one and
page ten can neither be skipped nor returned twice. This is what makes ordering
by a mutable field safe to page (plan §II.9), and it is a genuine improvement
over Rust, which reached only read-committed because its memtable holds a single
version per key.

## Versions

**A vector's version is the model that produced it.** Every canonical vector
carries a model id and a dimension. A search refuses a tenant whose vectors name
a different model from the one this server runs, because comparing them would
return confident nonsense that nothing about their width or norm would reveal.
That stamp is also why a Rust snapshot is re-embedded on import rather than
copied: the model name is the same, and the pooling is not (plan §II.10 row 16).

## What this phase deliberately does not decide

- **Lifecycle.** Importance, health, recall counts and retention policies are
  Phase 4's, in protobuf field numbers 20–49. `archived` is here early because
  Phase 3 needs a retirement.
- **Filter pushdown.** `vector.Filter` is a function and becomes a predicate in
  Phase 5. Designing the predicate language before the attribute store exists
  would repeat the mistake `docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md` §5
  records: a DSL that could not express the fields the product filters on.
- **Rank fusion.** There is one retrieval source, so there is nothing to fuse.
  `FusedScore` is reported separately from the first version so that adding a
  second source later changes a number's scale rather than a response's shape.
- **Listing.** `Scan` exists on the repository and is not exposed. Ordering by
  five fields is what Pebble snapshots unlock (plan §II.10 row 3), and it
  belongs with the attribute store that makes those fields indexed.
