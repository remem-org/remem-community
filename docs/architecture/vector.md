# Vector search

What is authoritative, what is derived, what a rebuild restores, and what a
search says when it cannot answer completely.

Phase 7 added the approximate index. Phase 3's exact one stays in the tree
permanently, and the two are held to the same contract suite.

## Canonical and derived

| Data | Class | Maintenance | Rebuilt from |
|---|---|---|---|
| Canonical vector (`vector` space) | **canonical** | transactional, with its record | — |
| HNSW node record (`vector_index` space) | derived | **asynchronous** | canonical vectors |

This is plan §II.4's table, and the vector index is the one row in it maintained
outside the write transaction. Spec §16 requires the canonical vector values to
be persisted independently of the derived structure so that a damaged or
incompatible index is rebuildable; spec §56 warns against putting large
transient objects in the write path. The consequence a user sees: an index
insert that fails never fails a write.

## The membership rule

**A memory is in the index if and only if its canonical vector row exists.**

Everything else in this document follows from that sentence.

A node record holds a record id, a layer number and neighbour lists. It does not
hold the embedding. The embedding lives once, in the canonical row, and is read
into memory when a tenant is materialised.

Two consequences, and they are the reason for the rule rather than side effects
of it.

**Storage.** For a corpus of 250,000 memories the embeddings are about 400 MB.
Node records are about 75 MB. Duplicating the embeddings into the index would
have made it about 450 MB — the index roughly six times larger and the
customer's total storage close to double, on the largest data the system holds.

**Deletion is exact, including through a crash.** Rust Remem kept its deleted
set in a memory-mapped entry that every checkpoint erased. After a restart each
hard-deleted vector came back as a phantom that consumed index memory for ever
and let `get_vector` serve a stale embedding (`PROJECT_REVIEW` §2.1 #4). Here
there is no deleted set to lose. A hard delete removes the canonical vector
inside the record's own transaction; from that moment the memory is unfindable,
whether or not the process lives long enough to tidy the graph.

This is why **the plan's Task 5 is not implemented as written.** It asks for
"deletion as a tombstone plus periodic repair — and, unlike Rust, tombstones are
durable". Making the tombstone durable would fix the Rust defect. Making
membership canonical removes the class it belongs to: there is no tombstone to
write, no repair sweep to schedule, and no window in which a deleted memory's
embedding is still on disk. It is the same move Phase 6 made when it derived
in-edges from canonical out-edges and removed the CSR finalisation race by
construction.

## Materialisation reconciles in both directions

A tenant's graph is built in memory on first use, from two sequential scans —
the canonical vectors and the node records — joined by record id. The join is
also the repair:

- a node record naming a record with **no canonical vector** is dropped. This is
  how a delete that never reached the index is completed.
- a canonical vector with **no node record** is inserted, and the insert is
  persisted. This is how an index write lost to a crash is undone, so a stored
  memory is never permanently unfindable.
- a node record that **cannot be decoded** is deleted, and its record is then
  covered by the second rule. Leaving it would make the tenant report itself
  degraded at every materialisation for ever, since the same row would fail to
  decode every time.

A materialisation that found unreadable records reports the tenant **degraded**
and asks for a rebuild. The results are complete again by the end of that load,
but the links that pointed *into* the unreadable nodes are gone, so the graph is
no longer the structure the configured parameters describe. Reporting "may be
incomplete" when it might be complete is the safe direction; the reverse is the
silent wrong answer.

## Versions and ownership

`internal/vector` owns two key spaces. The `vector` space holds canonical vectors,
written in the record's own transaction. The `vector_index` space holds HNSW node
records, and only `internal/vector/hnsw` reads or writes them.

They are versioned differently, because only one of them is canonical:

- **A canonical vector** is encoded through `internal/codec`, so the directory-wide
  `record_envelope` version covers it. It also carries the model id and the
  dimension, and that stamp is what makes a vector from another model a refusal
  rather than a wrong answer.
- **A node record** starts with its own format byte, `NodeFormatVersion`. It is a
  per-row version, not a directory-wide one, for the reason
  `attr.RowFormatVersion` is: a node record this binary cannot read is one
  unreadable row of a derived index. That calls for a rebuild, not a refusal to
  open the database, and materialisation already drops such a record and
  reinserts its vector.

## Residency and the memory budget

Graphs are held per tenant and evicted least-recently-used against
`vector.resident_budget_mb` (512 MB by default). A tenant with an operation in
flight is skipped rather than waited for; if everything resident is busy, the
budget is exceeded and `Resident()` says so rather than blocking.

**Eviction can never lose work.** Every change to a graph is written to its node
records before the call that made it returns, so a dropped graph is re-readable
and never re-derivable-from-nothing. That is what makes eviction safe at any
moment, and it is also why a node write that fails is survivable: the next
materialisation reconciles it.

The budget is bytes rather than a tenant count because tenants are not the same
size. A process serving one corpus of a million memories and nine hundred of a
thousand each should spend its memory on the one that needs it.

## Determinism

**A record's layer is a function of its id**, not of a random draw:
`floor(-ln(u) / ln(M))` with `u` derived from an FNV-1a hash of the 16 id bytes.
The id is hashed rather than used directly because a UUIDv7's leading 48 bits
are a timestamp, and records created in the same millisecond would otherwise
land on the same layer together.

Two reasons. Invariant 9 forbids a random draw inside a deterministic apply
path, and Phase 14 replicates index maintenance. More immediately, it is what
makes a rebuilt graph comparable with an incrementally built one: both assign
every record the same height, so any difference between them comes from
insertion order alone. Measured: over 2,000 vectors and 100 queries, the two
agree on the nearest memory every time and their top-10 sets are identical.

Neighbour lists are **not** byte-identical between the two, and are not meant to
be. A rebuild inserts in id order; an incremental index inserts in arrival
order.

## Refusal versus degradation

They are different failures and are treated differently.

- A **damaged derived index** degrades: the search answers, `truncated` is set,
  and a rebuild is asked for through `vector.Rebuilder`. A rebuild fixes it.

  Since Phase 9 that request is a durable job rather than a log line: the
  composition root's implementation logs *and* enqueues `vector.rebuild`,
  single-flighted per tenant so a search loop cannot queue a second, and a
  worker runs it. The search that discovered the damage still does not do the
  work — that would charge one user for everyone's repair — and the trigger only
  fires on a tenant the index has already worked around, so the rebuild ends the
  degradation rather than causing it. `remem-admin vector rebuild` remains the
  offline form, for a server that is stopped.
- A **model mismatch** is refused. If a tenant's canonical vectors name a
  different model from the one this server runs — or name two different models
  between them — searching returns confident nonsense that nothing about the
  vectors' width or norm would reveal. No rebuild can fix it, because the
  canonical data is what is wrong. The corpus must be re-embedded.

## Recall, and what the measurements mean

Recall is measured against `flat`, which is the definition of correct: it scans
every vector the tenant owns and returns the true nearest neighbours. That is
why `flat` stays in the tree permanently rather than being replaced.

**Recall depends on the corpus, not only on the parameters**, and the spread is
large enough that quoting a single number would be misleading. Measured over
10,000 vectors, recall@10, at the defaults `M=16, ef_construction=200`:

| Corpus | ef=16 | ef=64 | ef=128 | ef=512 |
|---|---|---|---|---|
| Uniform on the unit sphere, 384 dims | 0.212 | 0.507 | 0.694 | 0.982 |
| Uniform on the unit sphere, 64 dims | 0.593 | 0.907 | 0.983 | 1.000 |
| Clustered, 384 dims, 200 topics | 1.000 | 1.000 | 1.000 | 1.000 |

Uniformly distributed points in 384 dimensions are the worst case any proximity
graph can be given: every pair is near-orthogonal, so the true nearest neighbour
is barely nearer than the hundredth, and there is little structure for a graph to
exploit. It is also not what a memory corpus looks like. Reaching 0.982 at
ef=512 on that corpus is the evidence that the graph itself is sound and the
loss at low ef is search effort rather than a defect.

Text embeddings are clustered by topic, which is the row the product runs on.

The tests therefore gate on a clustered corpus at the real embedding width and
record the uniform numbers as the measured worst case. The corpus's parameters
are stated in `internal/vector/recall_test.go` so the number is reproducible
rather than merely reported.

## A neighbour list overflows before it is pruned (Phase 13)

A node's neighbour list may grow to its bound and half again before the
diversity heuristic prunes it back to the bound. The heuristic compares every
kept neighbour against every candidate, so running it is quadratic in the bound,
and profiled in Phase 13 it was 81% of an insert: it ran on every neighbour the
new node pushed one past its bound. An insert holds its tenant's graph lock, so
that cost was also what capped concurrent writes.

Measured on one host, 384 dimensions, recall@10 at ef=64 on the clustered corpus:

| | insert | 10,000 | 25,000 | 50,000 |
|---|---|---|---|---|
| Prune on every overflow (before) | 3.76 ms | 0.997 | 0.975 | 0.902 |
| Overflow by half, then prune | 1.08 ms | 0.999 | 0.985 | 0.926 |
| Keep the closest bound (Rust's rule) | 1.14 ms | 0.8965 | — | — |

Recall *rose*, because a search meets the extra edges before they are pruned, and
it rose on the uniform corpus too (0.459 to 0.554 at ef=64, 0.977 to 0.995 at
ef=512). Rust's rule is as cheap and fails the 0.95 gate at 10,000: the heuristic
is what the recall is made of, and how often it runs is what it costs. How far a
list may overflow barely matters. At 50,000 an eighth of the bound measured
0.915 and a quarter 0.924, against a half's 0.926.

What it costs is edges. At 10,000 vectors the mean layer-0 list is 39.2 links
against 32.0, so node records, which hold nothing but neighbour lists, are about
a fifth larger. The resident graph grows 3%, because the vectors dominate it.

## Recall depends on density, not size (Phase 13)

`TestRecallOverAQuarterMillion` failed in the first CI run that included it, at
0.615 against the plan's 0.93, and nothing but that CI job said so. The index was
not the cause. The test's corpus had a fixed twenty topics, so a quarter million
vectors meant 12,500 a topic, and a topic is a Gaussian cloud in 384 dimensions:
the uniform worst case above, at a smaller scale.

Measured on one host, recall@10 at ef=64, after the pruning change:

| Vectors | 20 topics | 500 vectors a topic |
|---|---|---|
| 10,000 | 0.999 (500 a topic) | 0.999 |
| 25,000 | 0.985 | — |
| 50,000 | 0.926 | 0.998 |
| 100,000 | 0.826 | — |
| 250,000 | 0.660 | 0.990 |

The pruning rule before Phase 13 measured the same shape on the same host: 0.975,
0.902, 0.801 and 0.626 at 25,000, 50,000, 100,000 and 250,000 vectors on twenty
topics. So the fall is older than the pruning change and not caused by it.

Holding the density, recall barely moves from ten thousand vectors to a quarter
million; holding the topic count, it falls with every doubling. What makes a
search hard is how many points it has to tell apart once it has found the right
region, not how many regions there are. The test now gates at the 10,000-vector
test's density, 500 a topic. The twenty-topic figures stay here as the measured
cost of a corpus that crowds one subject, and are not gated.

## Two completion criteria bought down in Phase 7; one met in Phase 13

**The 250,000-vector `large` fixture does not exist.** The plan records it at
line 897 as "defined but not generated". Recall at that scale is measured
against a deterministically generated corpus instead. That proves the algorithm
holds at that size; it does not prove agreement with the corpus Rust Remem
produced — and Phase 12 split that criterion for reasons §II.10 row 16 makes
permanent.

**Rebuild resumes (met in Phase 13).** Phases 7 and 9 left it restartable and
not resumable: the `vector.rebuild` handler ignored its `Checkpointer` and an
interruption cost a whole re-run. What made it cheap to fix was already here.
Materialisation inserts every canonical vector that has no node record, in key
order, so the rebuild is now two halves:

1. **Destructive and atomic:** drain the source, replace the canonical vectors,
   clear the node records, save a cursor saying so. Every refusal the source can
   produce happens in the drain, before any of the tenant's data is touched.
2. **A materialisation in committed batches:** place the node records that
   exist, insert the canonical vectors that have none, saving the cursor after
   each commit.

The cursor carries a phase and nothing positional: what remains after the first
half is a function of the store, so a crash between a batch's commit and its
save repeats nothing. A cursor this binary did not write starts from the
beginning, because a whole rebuild is always a correct answer.

The batches go into the tenant's **resident** graph under its lock, not into a
private one published at the end. A live write mid-rebuild then inserts into the
same graph with the same node numbering; a private graph would have left it
landing in a stale one whose numbers collide with the rebuild's. A rebuild that
fails part-way drops the tenant from memory, so a partial graph is never served
as healthy — the next reader's materialisation completes it.

Layers come from record ids and insertion is in id order, so a resumed rebuild
ends with the graph an uninterrupted one builds: `TestCrashDuringRebuildResumes`
compares the node records of the two **as bytes** over 3,000 vectors, and
counts that the resume inserted only what was missing. `flat` has nothing to
resume — its rebuild is a single replacement commit, which is Invariant 5's other
branch — and accepts the options and ignores them.

## Where the plan was corrected

| Plan says | What was built | Why |
|---|---|---|
| Task 5: durable tombstones plus a periodic repair sweep | No tombstone and no sweep; membership is the canonical vector | Removes the phantom class by construction rather than mitigating it, and keeps the index six times smaller |
| "Interfaces produced: none new — that is the test of whether the interface was right" | `vector.Index` gained `Health` | The interface was not quite right. Task 6 requires a damaged index to serve degraded and say so, and `[]Hit` has nowhere to put that. Health is per tenant, not per query, so a cheap separate call fits it better than widening every result |
| Task 1: port graph construction from `engine/index/hnsw.rs` | Written against the paper, with the level drawn from the record id | The Rust implementation's chunk, manifest, CRC and dirty-tracking concerns are all dropped by the plan itself; what is left is the algorithm. Deriving the level removes a random draw from a path Phase 14 will replicate |
| Recall@10 ≥ 0.95 over 10,000 vectors at ef=64 | Gated on a stated clustered corpus; uniform-random numbers recorded, not gated | Measured, not assumed: the same index and parameters score 1.000 and 0.507 on two corpora of the same size and width |
