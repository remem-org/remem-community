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

For the same reason the node-record commits on the live path are not fsynced:
the canonical vector in the record's own transaction is what durability is owed
to, and a node record a crash loses is one a materialisation inserts again, so
a second synced commit per created memory would buy nothing a restart does not
already give. Pebble's log is sequential, so an unsynced batch is made durable
by the next synced commit from any writer: a crash loses the most recent
node-record writes, never one in the middle.

That repair covers a node record that is *missing*, and not one that is
*stale*. When a content update's node-record write is lost, the record still
holds neighbour lists chosen for the old vector. Materialisation sees the
canonical vector covered and does not place it again, so the memory stays
findable but is linked by its old meaning until a rebuild. The window is not
new: it always lay between the record's synced commit and the node commit that
followed it. Not syncing the node commit widens it to "until the next synced
commit".

## Asynchronous is now literal: the queue, the bound, the window

For a long time "asynchronous" here meant only "outside the write
transaction". The insert still ran inline, after the commit and before the
response: the tenant's graph write lock, about a millisecond of HNSW work at 384
dimensions, and a second commit, all of it on the request path. The document
said one thing and the code did another, which is what
the server speed plan records as finding 1 and
fixes as plan item 14.

`internal/vector/indexqueue` closes the gap. It is a `vector.Index` and a
`vector.Maintainer` wrapped around the configured index, so nothing below the
composition root knows it exists — the write path still asks the index whether
it needs telling about writes, and still tells it after the commit. Only
`Indexed` and `Removed` are deferred. `Search`, `Stats`, `Health`, `Insert` and
`Delete` pass straight through, which is what keeps `Insert`'s read-your-write
contract and the shared suite in `internal/vector/vectortest` intact.
`Rebuild` passes through after draining its own tenant's shard: it replaces
that tenant's node records wholesale, so running it against a backlog would let
an operation that arrived first edit a structure the rebuild had already
declared finished. Another tenant's backlog is not waited for — one tenant's
repair has no business waiting on another's writes.

**Ordering is in enqueue order per tenant, and that is what the sharding is
for.** An insert followed by a remove of the same record must apply in that
order: the other way round, the remove is a no-op whose post-condition already
holds, and the insert behind it leaves a deleted memory findable for ever. So
each tenant hashes to exactly one of two workers, and each worker has one FIFO.
Order within a tenant is the channel's. Two tenants never wait for each other
unless they share a worker.

The promise is about enqueue order, not about commit order. A write commits its
record and *then* tells the index, and the window between those two steps is as
old as the `Indexed` call: two writers racing inside it reached the inline index
in whichever order the scheduler gave them, and they reach the queue the same
way. Nothing here widens that window and nothing here closes it. What the queue
adds is that an operation, once enqueued, is never reordered against its
tenant's other enqueued operations.

**The bound is backpressure, not loss.** Each worker's channel holds
`vector.index_queue_size` operations, 1024 by default and 1,048,576 at most —
the channel is allocated at that depth per worker and each slot holds an
embedding, so the upper end is there to catch a slipped digit. When it is full a create
waits for a worker, under its own context, rather than returning with its
vector unindexed. Rust Remem's equivalent did a `try_send` and counted what it
dropped (plan §II.10 row 5); dropping is survivable here too, because
materialisation repairs it, but making it the ordinary response to load would
turn a busy server into one whose index is quietly incomplete and say so only
in a counter.

**The visibility window is what a caller sees, which is why the queue is off by
default.** With `vector.async_indexing = true` a new memory becomes
vector-searchable a moment *after* its create returns, so a search issued
immediately after a create may not find it until the queue has drained — in the
ordinary case a fraction of a millisecond, and under full-queue backpressure as
long as the queue takes. Keyword search is unaffected: the inverted index is
written in the record's own transaction. `Flush` is how anything that needs the
window closed closes it, and it is what the tests use rather than a sleep.

`Settle` closes the window for one tenant: it is `Flush` narrowed to that
tenant's shard, offered as the optional `vector.Settler` interface so that a
synchronous index does not implement it at all and a caller then waits for
nothing. `internal/discovery` is the reader that has to wait — it is handed each
memory exactly once, so a window it merely tolerated would cost it a
relationship rather than a retry — and it waits once per job, before the first
subject's search, because the subject list is fixed before the loop and every
subject's write committed before the job could be leased. A failed settle fails
the job for the framework to retry.

What the wait guarantees is that everything the queue had *accepted* for the
tenant before the call has been applied. What it cannot guarantee is a write
that has committed but has not been offered to the queue yet. A discovery job
row is staged into the memory's own transaction, while `Indexed` is called after
that transaction commits — and an enqueue onto a full shard blocks inside that
call until a worker makes room, with `internal/memory` dropping the error either
way. A job leased while the write path is still in there settles a queue that
has never been told about the insert. The window is therefore the microseconds
between the commit and an enqueue that finds room, and the whole back-pressure
wait when it does not; an enqueue that gives up on a context deadline loses the
pair outright, until the next materialisation repairs the index. Closing it
would mean the enqueue happening before the job row becomes visible, which is a
change to how a write is composed and is not made here.

**Nothing is lost by a failure, and no loss is invisible.** An operation that
does not reach the index is logged at warn with its tenant and record id and
counted in `remem_vector_index_failures_total` under one of three kinds:
`apply` for one the index refused, `enqueue` for one a cancelled caller or a
stopped queue never accepted, and `shutdown` for one the stop abandoned. The
third is there because an accept can race `Close`: an enqueue checks the stop
channel and then sends, so an operation can be accepted onto a shard whose
worker has already decided to stop. Each worker therefore drains its channel to
exhaustion after the stop and counts what it finds, which is what keeps
"accepted" and "either applied or counted" the same set. There is no retry
loop: the canonical
vector is committed before the operation is queued, so what a lost one costs is
one insert at that tenant's next materialisation, and a worker retrying under
the graph lock would spend the tenant's write lock on work the reconciliation
does for free. `remem_vector_index_queue_depth` says how full the queue is and
`remem_vector_index_apply_lag_seconds` how wide the window actually is.

The shutdown drains, bounded at thirty seconds, after the listener stops
accepting and before the store closes. A drain that runs out of time logs what
it abandoned, because what it abandoned is node records — and the next
materialisation of each tenant inserts them, exactly as it would after a crash
at the same moment.

Then it waits, for thirty seconds more, for the workers to be *gone*. Two
budgets because they bound two different things. The first bounds work the queue
still owes, which a materialisation would redo for nothing, so giving up on it
is right. The second bounds a worker that is inside an insert at this moment,
and giving up on that one means closing Pebble under a goroutine about to commit
into it — which was a `panic(ErrClosed)` until `batch.apply` learned to refuse a
closed store the way `Set` and `Delete` always have.

`vector.async_indexing = false`, the default, keeps the insert on the request
path, which is what it did before this existed. A search that misses a memory
acknowledged a moment ago while reporting `truncated: false` is the silent short
answer the rest of this document refuses, and between agents sharing a tenant it
is a lost read-your-writes. So the window is a trade an operator opts into, and
`openspec/changes/async-vector-indexing` is the design that removes it: a
pending set scanned exactly beside the graph, so a write is findable from its
acknowledgement whether or not it has been placed.

## The membership rule

**A memory is in the index if and only if its canonical vector row exists. Its
entries are its passage rows when it has any, and otherwise its canonical vector,
as passage 0.**

Everything else in this document follows from those two sentences.

The first says *whether*: a hard delete removes the canonical vector in the
record's own transaction, and from that moment no entry of the memory exists,
whatever passage rows or node records are left behind. The second says *which*.
A memory whose content fits one passage is indexed by its canonical vector, as it
always was. A longer memory is indexed by its passages, each embedded with the
rest of its segment in view (`docs/architecture/retrieval.md`), and its canonical
vector — the document vector, a mean over its first segment — is not an entry:
indexing it would let a vague whole-memory match outrank the passage that holds
the answer, and it is the mean its passages already cover.

An index entry is a `vector.Key{ID, Passage}`. A memory's entries are always
numbered `0…n-1` with no gaps, because its passage set is only ever replaced
whole, so "every entry of this memory" is found by asking for passage 0, 1, …
until one is missing: a node graph needs no per-memory index of its own.
`vector.Store` owns the rule. `ScanEntries` merges the canonical and passage key
spaces under it, both of which sort by record id, and every index reads through
it; `StagePassages` is the one writer of passage rows, which the record indexer
calls.

A node record holds a record id, a passage number, a layer number and neighbour
lists. It does not hold the embedding. The embedding lives once, in the canonical
row or the passage row, and is read into memory when a tenant is materialised.
A node for passage 0 is written in node format 1, byte for byte as before
passages existed; a node for any later passage is format 2, which adds the
passage number. A passage-0 entry keeps the layer it always had, because the
layer is hashed from the record id alone for passage 0 and from the id and the
passage number otherwise.

## A search returns memories

`Search` returns up to `k` distinct memories, each at the distance of its
nearest entry, and names that entry. `k` counts memories however many passages
one of them has.

`flat` keeps the nearest entry per memory over its scan, and is the definition of
the grouped answer. HNSW groups inside the bottom-layer walk: its result set
holds one candidate per memory, the nearest of its passages seen so far, while
exploration still follows every passage's links, and the stopping rule compares
against the worst of `ef` distinct memories. So `ef` bounds memories, and a
memory with a thousand passages near the query cannot fill the walk's result set
with itself.

Grouping in the index rather than the query executor is the design's choice, and
`TestAThousandPassageMemoryCannotCrowdAFilteredPage` holds why: in the executor,
one rejected thousand-passage memory spends a limit-10 search's whole widening
budget on itself and returns an empty page marked truncated. Rank fusion is the
other reason: it adds a contribution for every time a memory appears in a
source's list, so a memory listed once per passage would outweigh an equally
relevant shorter one (`TestHybridSearchFusesAMultiPassageMemoryOncePerSource`).

Two consequences, and they are the reason for the rule rather than side effects
of it.

**Storage.** At 1,024 float32 dimensions a vector is 4 KiB, and a memory costs
one vector for its canonical row plus one per passage when it has more than one:
about one per 256 tokens of content. A 1,737-memory agent-browsing corpus of
about 7.3 million tokens is about 29,000 passages, or 120 MB of
passage vectors beside 7 MB of canonical ones. (Phase 7 measured the model of
its time: 400 MB of 384-wide embeddings for 250,000 one-vector memories, and
about 75 MB of node records.) Node records hold no vectors, so duplicating the
embeddings into the index would roughly double the customer's storage on the
largest data the system holds. Storing the vectors as the int8 values they
already are would cut all of this by four, losslessly, and is a separate change.

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
- a canonical vector that is **empty, or of another width** than the tenant's,
  is left out of the graph altogether — whether it has a node record or not. The
  arena holds one width, so such a row cannot sit beside the rest; no embedder
  produces one through a vector row the encoder accepted, so it means a corrupt
  or hand-edited store. It is dropped rather than inserted because inserting it
  answers *every* search with an error instead of this one record with none, and
  it is dropped from the pending work too rather than left for the caller,
  because the insert that would finish it is the insert that cannot succeed.

The width the last rule measures against is the width **most** of the tenant's
vectors have, decided from the canonical scan before anything is placed, with a
tie broken towards the narrower so that two servers reading one corpus agree. It
is deliberately not the first row's: one corrupt record must never be the record
that decides what the tenant is. When the first vector placed fixed the width, a
single mislaid row sorting ahead of the rest made every good row the mismatch, so
the tenant kept that one vector and answered every query asked at its real width
with an error — the opposite of the intended cost.

## Two kinds of damage, two remedies

Either finding leaves the tenant **degraded**, because reporting "may be
incomplete" when it might be complete is the safe direction and the reverse is
the silent wrong answer. What differs is what an operator should do next, so
`Health`'s reason says which it is, and only one of them schedules a rebuild:

- **unreadable or mislevelled node records** are repairable. The canonical
  vectors behind them are intact, the load re-inserts them, and a rebuild
  derives the whole graph from them afresh. The results are complete again by
  the end of that load, but the links that pointed *into* the unreadable nodes
  are gone, so the graph is no longer the structure the configured parameters
  describe until the rebuild finishes. This is the case that **asks for a
  rebuild**.
- a **vector the arena cannot hold** is not repairable. A rebuild rewrites the
  same row at the same width and finds it unplaceable again, so asking for one
  would queue a job at every materialisation for ever — throttled, never
  resolving — and change nothing. The tenant is degraded and no rebuild is
  scheduled for it; the reason names the count and the tenant's width and says
  the records must be re-embedded. A rebuild that meets such a row therefore
  **leaves the tenant degraded when it finishes**, rather than clearing the flag
  because a repair ran.

A tenant can be in both states at once, and then the reason says both.

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

**A graph's size is a running count, and a full walk is its definition.** What
a tenant is charged is `72 × live nodes + 4 × (arena capacity + every links
and back list entry)`. The graph keeps the last two terms as counts. They are
adjusted wherever a list's length or the arena's capacity changes, so measuring
after a write costs three loads, not a walk. The walk this replaced cost ~4 ms
per insert at 90,000 nodes, held a read lock the next writer queued behind, and
appears to have slowed the inserts between walks as well. Removing it took
writes-only throughput in one tenant from 82 to 255 a second at 32 concurrent
writers over 90,000 vectors, measured on 2026-10-04.

- **The count is derived state, held to an oracle.** The walk is kept as
  `recountResidentBytes`, which production never calls. A property test checks
  the count against it after every insert, replacement and removal on a graph
  small enough to prune and relink, and again after materialisation and rebuild.
  A wrong count fails no request, it just evicts at the wrong time, so the test
  is the only thing that can catch it.
- **Lengths change in three helpers only.** `setList`, `appendList` and
  `dropFromList` adjust the count. A guard test parses the package and refuses
  any assignment to a node's lists or the arena's chunks outside the few
  functions that keep the count by hand: `alloc`, materialisation, `grow`, and
  the decoder, whose nodes are not in a graph yet.
- **A write measures under the lock it already holds, and charges whatever
  happened.** The size is read while the write still holds the graph exclusively,
  and charged afterwards under the index-wide lock alone. It is charged even when
  the node records fail to commit, because the graph in memory changed either way
  and memory is what the budget bounds. Each measurement carries a sequence number
  taken under the graph's lock, and an older one never overwrites a newer charge:
  two writes measure in lock order but may charge in either.
- **Nothing durable.** The counts exist only in memory. Every materialisation
  rebuilds them from the records it places.

## How a resident graph stores its vectors, and how it compares them

These are the two halves of plan item 15, and they are worth reading together
because the first exists to make the second worth having.

**Vectors live in the graph, not in the nodes.** A resident graph holds one
chunked arena — 4,096 vectors to a chunk, each chunk a single `[]float32` of
`chunk × dim` — and a node holds the index of its slot. A node used to own a
`[]float32` of its own, which is one allocation and one pointer per record, and
the pointer is followed on *every* distance computation: a search at ef 64 over
ten thousand records does thousands of them, each landing on a cache line that
has nothing to do with the last. In the arena the vectors of records inserted
near each other in time share lines, and a quarter-million-record graph holds
sixty-odd allocations rather than a quarter of a million.

Chunked rather than one slice, because a single arena would copy every vector it
held each time it grew. The *first* chunk doubles within itself, from one vector
up to the full 4,096, so a tenant holding three vectors costs three vectors —
which matters because the residency budget above is enforced against what the
arena actually holds, and most tenants are small. Every chunk after it is
allocated full and copies nothing, because a graph that filled the first one
holds four thousand records and a chunk is proportionate to it by then. Either
way a copy is bounded by one chunk and the rest of the arena never moves.

A removed record's slot goes on a free list and is handed out again, for the same
reason its node number is: a graph that churned a million records must not hold
a million vectors for the thousand it still has. `dim` is fixed by the first
vector a graph sees and a mismatch is refused, exactly as before.

The doubling is the one cost. Over the first chunk's life it allocates about one
extra vector per vector inserted, so bytes allocated per insert rise while live
bytes and live allocations fall: measured on `BenchmarkInsert384Cosine`, +11% of
allocated bytes at the same allocation count, and past the first 4,096 records
the arena allocates once per 4,096 inserts instead of once per insert.

Storage layout cannot change an answer, and this one does not: the arena
reproduces the search digests (`internal/vector/hnsw/search_digest_test.go`)
bit for bit.

**The dot product is in assembly on arm64.** `distance.dot32` — the inner loop
of every cosine distance, which is half of an insert and the whole of a search
walk's arithmetic — is a NEON kernel (`distance/dot_arm64.s`): sixteen floats an
iteration into four 4-lane accumulators with fused multiply-adds, reduced
pairwise, with a scalar tail for widths that are not a multiple of sixteen (384
and 768 have none). Every other architecture, and any build tagged `purego`,
runs the four-accumulator scalar loop in `distance/dot_generic.go`. Measured on
an M1 Pro at 384 dimensions: 157.8 ns to 38.1 ns for the dot and 161.6 ns to
45.2 ns for the whole `CosineWithNorms` call. End to end on an idle host,
`BenchmarkInsert384Cosine` fell 53% (1131 µs to 534 µs) and
`BenchmarkSearch384Cosine` 49% (782 µs to 403 µs), both interleaved against the
same commit's scalar build at `-benchtime=2s -count=6`.

Spec §55 forbids vector optimisation that has not been measured, and this one
waited until it was: the server-wide profile behind the server speed plan
found the scalar loop was still half of an insert's CPU.

**The two kernels do not give the same last bit, and that is the whole cost.** A
sum of float32s in a different order is a different sum. Measured over the
digest harness's 800 queries on a 3,000-record graph: the two kernels build the
*identical* graph, return the identical candidates for every query, and differ
in a distance by at most 1.79e-07 — three ULPs of the dot product it came
from. Four candidate positions out of 56,200 come back in a different order, and
in every one of them the two candidates that swapped are within one
representable float32 step of each other, which is below what the two kernels
agree to. So the digest is recorded per kernel rather than loosened: each kernel
must still be exact against itself. An index built on one machine and queried on
another of a different architecture gets answers that agree to that bound, not
to the bit — which was never on offer and is not what recall or relevance is
measured to.

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

## Search effort comes in steps, and the caller is told so

A walk runs at `ef = max(ef_search, k)`. A query that asks for fewer candidates
than `ef_search` therefore pays for exactly the same traversal and is handed less
of what that traversal found, which is pure waste for the one caller that widens:
`query.Executor.runVector` under a filter it cannot push down used to ask for 30
candidates, settle them, come up short, and ask for 60 — two traversals that had
both found the same 64 nodes.

`vector.Granular` is how an index says where its steps are. `hnsw.Index` returns
`ef_search`; `flat` does not implement the interface at all, because it scans
every vector and every `k` costs the same, so its granularity is honestly one.
A widening caller rounds every budget up to the step and takes the candidates it
has already paid for. It does not round *past* `MaxBudget`: that bound is a
promise about how many candidates a page may be built from, and `truncated` means
the promise was spent.

This changes no answer, only how many rounds it takes to get one. See
`docs/architecture/query.md`, "What the vector search stopped paying twice for".

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

## Recall at 1,024 dimensions, with passages (passage-vectors)

The model became `pplx-embed-context-v1-0.6b`, so the recall tests moved to its
width (`recallDim` is `embedding.Dim`); every figure above this section was taken
at 384. Measured on one host, recall@10, at the defaults `M=16,
ef_construction=200`:

| Corpus | ef=16 | ef=64 | ef=512 |
|---|---|---|---|
| Clustered, 10,000 vectors, 500 a topic | 0.892 | 0.999 | — |
| Uniform on the unit sphere, 10,000 vectors | — | 0.424 | 0.974 |
| Clustered, 250,000 vectors, 500 a topic | — | 1.000 | — |
| **Passages:** 5,000 memories, 100 a topic, 1–32 passages each (82,792 entries) | 0.956 | 1.000 | — |

The width barely moves the clustered figures (0.887 and 0.997 at 384; 0.990 at a
quarter million) and worsens
the uniform worst case, as more dimensions should: a uniform cloud is harder the
wider it is, and 0.974 at ef=512 still shows the graph reaching everything.

The passage row is the one this change adds, and its recall counts **memories**:
of the exact top ten memories, each ranked by its nearest passage, the fraction
the grouped HNSW search also returned. Its corpus is shaped like contextual
passages — a memory's passages cluster tightly around it, because each was
embedded knowing the whole of it (the spike measured 0.94–0.99 cosine between a
document's passages and its document vector) — and the passage count per memory
runs from one to thirty-two, so long memories crowd the space the way real ones
do. It is gated at 0.95 at ef=64 (`TestPassageRecallAtScale`, behind the `recall`
tag), with a 300-memory version in the everyday suite
(`TestPassageRecallAgainstFlat`: 0.990 and 1.000).

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
