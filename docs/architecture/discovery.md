# Relationship discovery

What is authoritative, what is derived, where the strategy boundary is, and what
this deliberately does not do.

Phase 11. Spec §25 (continuous relationship discovery), §22–24 (jobs), §47 and
§61 Invariant 9 (no LLM or network call in a deterministic apply path);
implementation plan §Phase 11, §II.10 rows 5, 7, 8, 10 and 17.

---

## What this replaces

Rust Remem discovers relationships on every memory write. It stores the memory,
then does a `try_send` into a bounded `tokio::sync::mpsc`
(`api/routes/memories.rs:324-350`). When the channel is full the task is
discarded, `dropped_discovery_count` is incremented, one line is logged, and the
request returns 201. The memory exists, it has no relationships, and nothing
durable records that it was owed any.

That is plan §II.10 row 5, and it is the defect the job framework was built for.

**The enqueue is staged into the memory's own transaction.** A failed write
leaves no orphan job; a successful one cannot lose its follow-up work. There is
no in-memory queue anywhere on this path and no capacity to overflow — the
queue's capacity is the disk.

Everything else in this document is in service of that sentence.

---

## Authoritative and derived

| | |
|---|---|
| **The job row** | Canonical. Nothing reconstructs it (plan §II.4) and there is no rebuild path for the job space. That is what makes "nothing is dropped" a property rather than a hope |
| **A discovered edge** | Canonical, and indistinguishable from one a user created — see below |
| **The derived in-edge** | Derived from the out-edge, as for every edge (Phase 6) |
| **A `Candidate`** | Derived and never stored. It exists for the duration of one strategy call |

**A discovered edge carries no "discovered" flag, on purpose.** Such a flag would
be a flag some code path is entitled to act on — and the obvious action is
deleting edges the corpus no longer justifies, which is a heuristic destroying
user-visible data. Plan §II.10 row 8 forbids exactly that in the delete
direction for memories, and an edge is no different. The flag would also be a
second durable fact discovery would have to keep true through a rebuild, an
import and a threshold change.

The consequence is stated rather than hidden: **there is no way to ask which
relationships were discovered and which were asserted.** Both are relationships
between the same two memories, both were written by this server, and both mean
the same thing to a traversal.

---

## The pipeline

```
  a memory write commits
          │  (same transaction)
          ▼
    discovery.similar job — subjects: the memories that write created
          │
          ▼
  ┌─── per subject ──────────────────────────────────────────┐
  │  read the record        (missing → skip; archived → skip) │
  │                                                           │
  │  propose:  vector search (top_k, self filtered out)       │
  │            one traversal out of the subject, depth 2      │
  │            bounded by MaxCandidates                       │
  │                                                           │
  │  describe: VectorSim, TextSim, GraphDist, AgeDelta        │
  │            — every candidate, all four                    │
  │                                                           │
  │  evaluate: Strategy → proposed edges                      │
  │                                                           │
  │  write:    drop pairs already linked in either direction  │
  │            one transaction                                │
  │                                                           │
  │  checkpoint what is left                                  │
  └───────────────────────────────────────────────────────────┘
```

### A source proposes; a signal describes

`Candidate` carries every signal spec §25 lists. The shipped strategy reads the
first. That is what keeps a future LLM-evaluating strategy a new `Strategy`
rather than a rewrite of retrieval.

Carrying a signal is not the same as spending budget to find candidates, and
conflating the two is the mistake `internal/text` recorded in Phase 8 in its own
words — *a step is a source* — when a tag filter meant to narrow instead
contributed its own ranked list into the fusion.

**Vector proposes.** Discovery *is* nearest-neighbour search.

**The traversal proposes**, and it is free: the same walk is what fills
`GraphDist`, so the candidates it contributes cost nothing beyond the record read
every candidate needs. What it buys is the case an approximate index misses — a
memory the graph already knows about that HNSW's entry point did not lead to.

**Text does not propose.** A keyword proposal means running a whole memory's
content as a BM25 query, once per memory written, and content is bounded at a
megabyte. `TextSim` is a property of a *pair* and needs no index at all: it is
Jaccard over the same tokeniser the inverted index uses, so it is symmetric,
bounded in [0, 1], and correct with `text.enabled` off. A BM25 score has none of
those three properties — `internal/query`'s `deriveScore` says the same thing
about mixing it with a cosine.

**Time does not propose.** "Written near this one" is adjacency, not evidence,
and a temporal window would spend the candidate budget on memories unrelated by
construction. `AgeDelta` is computed from records already in hand, and it is
unsigned: "written before" and "written after" are the same adjacency, so a sign
would be one more way to read the signal wrongly for nothing gained.

### What enqueues discovery

Two writes do, and both stage the job into their own transaction.

**A create.** One job per write transaction, carrying its subjects — a batch of
eight memories is one job with eight subjects, not eight jobs, because the
queue's cost is per row and the work is per subject either way.

**A content change.** `memory.Update` enqueues one job for the one memory whose
content moved. The condition is a content change and nothing else: discovery is
a vector search over a memory's meaning, and an edit to tags, source, policy or
any affective value moved no meaning, so there is nothing for a neighbour search
to reconsider. Rewriting the content to the identical string is not a change
either — the record is compared, not the request.

**What an update deliberately does not do is retire the edges the old content
justified.** A discovered edge carries no marker distinguishing it from a user's
(above), and §II.10 row 8 forbids a heuristic deleting user-visible data. So an
edited memory *accumulates*: it keeps the neighbours its old wording earned and
gains the ones its new wording deserves. That is the "relationships never
shrink" bound under Open bounds, and an update widens it rather than introducing
it.

### One snapshot

The traversal and every record read go through one pinned view, so a memory
written while a subject is being considered is either wholly considered or wholly
absent. This is the correction Phase 6 made for `graph.Traverse` and Phase 8 for
the keyword step, for the same reason.

---

## The threshold is a cosine, and this is where Rust parity ends

Rust compares `distance_to_score(d) = 1/(1+d)` against
`auto_discovery_threshold = 0.7` (`connection_manager.rs:139-142`). Over unit
vectors under squared L2 that is `d ≤ 3/7`, which is **cosine ≥ 0.786** — not
0.7. Rust's own source records why that quantity should not be thresholded
(`types.rs:545-548`, REM-74): `1/(1+d)` floors at 1/3 for orthogonal vectors and
never approaches zero, so a relevance threshold against it is decoration.

Go decided this in Phase 3 and has shipped it since: cosine is the reported
relevance everywhere, `1/(1+d)` orders and nothing else. Discovery thresholds the
cosine.

**Edge parity with Rust is therefore not achievable, and it would not be
achievable at any other number either.** Plan §II.10 row 16 already records that
absolute similarity is not comparable across the two pooling schemes: unrelated
pairs sit at 0.626 median cosine in Rust's CLS space and 0.074 in Go's
mean-pooled one. Auto-discovery thresholds exactly the quantity row 16 excludes
from comparison.

This is plan §II.10 row 17. The completion criterion "discovery produces the same
edges as Rust on the `typical` fixture" is **bought down, not met**, and
`test/differential/COVERAGE.md` says so. What is still comparable, and is
compared, is the behaviour rather than the output: at most `top_k` edges per
subject, no edge below the threshold, no self-link, no edge crossing a tenant,
and a second run that changes nothing.

**The strength is the similarity.** An edge's strength ranks traversal (§II.10
row 7), so writing the cosine into it is what makes "closest first" survive into
`find_related`.

### What 0.7 actually selects, measured

Ten memories over three topics plus one unlike anything else, embedded by the
real model, every ordered pair scored:

| | pairs | max | median | at or above 0.7 |
|---|---|---|---|---|
| same topic | 20 | 0.8318 | 0.6552 | 5 |
| different topic | 70 | **0.2142** | 0.0098 | **0** |

**The threshold sits in a gap 0.49 wide.** The lowest pair it links is 0.7038 and
the highest across-topic pair in seventy is 0.2142, so nothing about the number
is delicate: anything from roughly 0.25 to 0.70 selects the same edges on this
corpus.

Two things follow that are worth an operator knowing.

**Discovery links near-paraphrases, not topic-mates.** Four sentences about raft
consensus produced three edges out of twelve ordered pairs; the two that did not
link sit at 0.65. `similar_to` means "says nearly the same thing", not "is about
the same subject" — and a user expecting the latter will find the graph sparser
than they assumed.

**And it is the measurement that makes §II.10 row 17 more than a technicality.**
Rust's effective threshold is cosine ≥ 0.786 in a CLS-pooled space where row 16
measured *unrelated* pairs at 0.626 median and 0.785 at the 95th percentile — so
Rust's threshold sits essentially on top of the p95 of pairs that have nothing to
do with each other. The two implementations are not merely thresholding different
numbers: one of those thresholds separates the populations and the other barely
does.

---

## Atomicity boundaries

| Boundary | What commits together |
|---|---|
| The memory write | The records, their derived index rows, and the discovery job |
| One subject's edges | All of that subject's new edges, or none of them |
| A checkpoint | Its own transaction, after a subject's edges have committed |

A subject that fails does not roll back the subjects before it. The checkpoint
records what is left, so a retry resumes rather than restarting — every subject
is idempotent, so restarting would be safe and merely wasteful.

---

## Failure semantics

**A vanished subject is skipped, not failed.** A memory hard-deleted between its
write and its discovery is an ordinary race. Failing would retry six times and
leave a `Failed` row an operator has to read to discover that nothing was wrong.
It is counted as `missing` in the metrics, which is where a *rate* of them shows
up.

**A subject with no comparable vector is skipped and logged.** It is not work a
retry can fix.

**Anything else propagates**, and the queue decides: retry with backoff until the
attempts are spent, then `Failed` carrying the reason.

**Archived memories neither discover nor are discovered.** An archived memory is
retired from retrieval, so an edge into one manufactures a relationship to
something no ordinary read reaches — and traversal would then spend its node
budget on it, which is the defect Phase 6 fixed for hard deletes.

**A checkpoint that cannot be saved does not fail the run.** It costs a resumed
job some repeated work, and every subject is idempotent.

---

## Idempotence, and how it is held

Delivery is at-least-once, so a handler runs twice in ordinary operation. The
obligation is asserted rather than asked for: `TestEveryRegisteredHandlerIsIdempotent`
compares the whole keyspace after two runs against the keyspace after one.

**A pair already linked in either direction is left entirely alone.** Two things
follow.

`graph.Service.Add` *replaces*. A `similar_to` written over a hand-made
`contradicts` would be a heuristic destroying an assertion. Skipping the pair
costs one already-existing relationship and loses nothing: the two memories are
connected either way.

And rewriting even a *discovered* edge would move its `created_at`, so the
keyspace after two runs would differ from the keyspace after one. The edge count
would not show it; the keyspace comparison does.

**Both directions, unlike Rust.** Rust checks the source's out-neighbours only
(`connection_manager.rs:155-166`), so discovering A and later discovering B
produces both `A→B` and `B→A` for a relation that is symmetric. Checking both
makes `similar_to` cost one row rather than two, and makes discovery idempotent
*across subjects* and not merely across runs.

The check is sixteen keyed point reads — eight relationship types, two
directions — rather than a neighbour scan. A neighbour read is capped, and a hub
memory with more edges than the cap would report "not linked" for a pair that is,
silently refreshing an edge on every run. Sixteen reads is bounded by the type
table rather than by the corpus.

### The check is *inside* the transaction, and that is the whole of it

Reading "these two are not linked" and then writing an edge is a
read-modify-write, and two of them race. Phase 11's end-to-end run found it on
the first try: two invoices written together, discovered by two workers at once,
both told the pair was unlinked, both writing — connected in both directions one
millisecond apart. No unit test could see it, because a test that runs the two
subjects one after the other is testing the case where reading before writing is
enough.

`graph.Service.Linked` therefore reads through the caller's transaction and,
when the answer is "not linked", expects every one of those sixteen keys absent
at commit. The loser of the race conflicts instead of writing, `txn.Do` re-runs
the body, and the second pass is told the truth. The decision lives entirely
inside the retried body; nothing is carried in from a view taken before it.

A per-tenant gate would have made the race unlikely and would have been the wrong
fix. The write path already says why, about `txn.Gate`: the gate "removes the
contention rather than absorbing it; it is not what makes this correct." Here it
would also do nothing across nodes, which is what Phase 14 makes real.

---

## Determinism

Nothing here reads a wall clock to make a decision, makes a network call, or asks
a language model. Spec §47 and Invariant 9 forbid such calls inside deterministic
apply, and **running discovery as a job places it outside apply by
construction** — which is what makes a future LLM-evaluating strategy legal at
all.

Within a run, the strategy sorts by similarity and breaks ties by record id, so
the same corpus produces the same edges on every run and, in Phase 14, on every
node. The one thing that is *not* deterministic is which candidates an
approximate index proposes; that is HNSW's property, it is why `flat` exists as
the oracle, and it changes which edges are found rather than which are chosen.

---

## Bounds and constants

| Name | Value | Source |
|---|---|---|
| `DefaultThreshold` | `0.7` cosine, inclusive | `config.rs:274`. The number is Rust's; the quantity is Go's |
| `DefaultTopK` | `5` | `config.rs:275` |
| `MaxCandidates` | `64` | Go's. Bounds the union of both sources per subject |
| `neighbourDepth` | `2` | Go's. Deep enough for `GraphDist` to distinguish a neighbour from a neighbour's neighbour |
| `Unreachable` | `-1` | `GraphDist` for a candidate the traversal did not reach. Zero would mean "the subject itself" |
| `MaxSubjects` | `1000` | Bounds one job's payload. The real bound is how much work one claim represents |
| `DefaultSubjectsPerJob` | `100` | What a backfill puts in one job |

`discovery.threshold` and `discovery.top_k` are configurable, unlike Phase 8's
tokeniser settings, and the distinction is worth stating because the two look
alike. A tokeniser setting decides what is written into the postings key space,
so a corpus written under one and queried under another answers differently for
older and newer memories. A discovery threshold decides which edges are created
*next*; the edges already on disk stay true statements about the corpus, so
lowering it adds relationships rather than invalidating any.

---

## Backfill

Discovery is enqueued by the write that creates a memory, so a corpus written
before Phase 11 — or while `discovery.enabled` was off, or during an outage that
exhausted a job's attempts — holds memories nothing ever considered.

```bash
remem-admin discovery backfill --data-dir ./data [--tenant <id>] [--per-job <n>]
```

**Nothing notices on its own, and that is a decision.** There is no "was this
memory discovered" flag to sweep on, for the reason a discovered edge carries no
flag: it would be a second durable fact discovery would have to keep true.

**It queues work rather than doing it.** The rows are ordinary discovery jobs, so
the server drains them at worker speed with the same leases, retries and
checkpoints. That is also the only thing it can do with the server stopped:
comparing embeddings is the running server's job, and the command holds Pebble's
exclusive lock while it runs.

Running it twice is safe and is still work: the second pass over an unchanged
memory finds its neighbours already linked and writes no edge, but it does pay
for the vector search that established that.

**Discovery is not a recurring job**, for the same reason.

---

## Versions and ownership

**Discovery owns no key space.** The edges it writes belong to `internal/graph`
and are written through `graph.Service`, in the graph's format. Its job rows
belong to `internal/jobs`.

**What it does own is its job payload.** The payload starts with a version byte,
`payloadVersion`, currently 1 (`internal/discovery/payload.go`). A payload with a
version this binary does not know is refused by number, not guessed at: a job
queued by one binary may be run by the next.

## What is not here

**Client-supplied graph extraction is REM-115**, deliberately absent rather than
half-built — the disposition consolidation got as REM-113.

Plan Task 4 asks for a `graph_extraction` block on a create request: entities
that become their own memory records, and typed relationships between them,
idempotent by entity name within a tenant. Rust implements it at
`api/routes/memories.rs:560`, inline in the create request, swallowing every
error into a `tracing::warn!`.

Neither shape is obviously right, which is why it is a ticket rather than a
guess. Inline is a latency cliff on the write path — a request carrying a hundred
entities becomes a hundred and one embedding calls before the 201 returns — and
Rust's failure mode is silent and partial. A durable job fixes both and
introduces a race the feature's own workflow can see: a client that stores a
memory and immediately reads its connections finds none yet. The ticket records
the two dispositions and the three things that have to be decided.

The `graph_extraction` field does not exist on Go's create request, so nothing is
silently ignored.

**Consolidation is REM-113**, and is not registered as a no-op.

**No second strategy ships.** `Strategy` has one implementation, and per the rule
Phase 8 set for `text.Index` and Phase 9 for `jobs.Queue`, a contract suite over
a single implementor would duplicate this package's unit tests. The interface
exists because spec §25 requires the seam, not in anticipation of a second
implementation this phase could have written.

---

## Metrics

`remem_discovery_*`, tenant-labelled, in the core build:

- `subjects_total{tenant,outcome}` — `linked`, `unlinked`, `skipped`, `missing`.
  The one worth alerting on is `missing`: ordinary at a trickle, and something
  else at a rate.
- `edges_created_total{tenant}`
- `candidates_considered{tenant}` — how wide the union got, per subject. It is
  what says whether `MaxCandidates` is binding.
- `run_duration_seconds{tenant}` — one job, which may carry many subjects.

**There is deliberately no dropped counter.** Rust has one because its queue is a
bounded channel that discards work. Here it would be a constant zero dressed as
a measurement.

---

## Open bounds

**A memory's relationships never shrink**, and since the update path arrived
that is load-bearing rather than merely tolerated. Discovery adds edges and
nothing retires them, so a corpus that drifts keeps relationships its own
contents no longer justify. Retiring them is a heuristic deleting user-visible
data (§II.10 row 8), which is why it is not done here — but it means
`similar_to` is a claim about the corpus at *each* time the memory's content was
written — at its creation and at every edit that changed it — and never a claim
about the corpus now.

**Which candidates an approximate index proposes is not stable across a
rebuild.** A rebuilt HNSW is deterministic (Phase 7: a record's layer comes from
its id), so a rebuild proposes the same candidates as a rebuild — but an
incrementally built index and a rebuilt one can differ at the margins, and a
subject discovered under one may have found a neighbour the other would not.
Already-linked pairs are left alone, so this adds edges over time rather than
changing any.
