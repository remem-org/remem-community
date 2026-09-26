# Attributes, the query layer and listing

How Remem decides which memories to return without reading the memories, what a
page of results actually promises, and what `truncated` means.

Packages: `internal/attr` (the indexed field layer), `internal/query` (the model,
planner, executor, fusion and cursor), and `internal/memory`'s `List`.

Spec §17 asks for filters evaluated without materialising records and §18 for
temporal access through ordered keys. Plan §II.9 asks for snapshot-consistent
paging. `docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md` §3 asks for a query that
serialises. This document answers all four and records the three places Phase 5
departed from the plan.

## What is authoritative

| Thing | Where | Rebuildable? |
|---|---|---|
| Record body, including the lifecycle fields | record space | **No.** It is canonical |
| Attribute row | attr-row space | Yes, from the record body |
| Attribute slot index entry | attr-index space | Yes, from the attribute row |
| Slot table | `/slots`, system space | No — without it no row on disk decodes |
| Cursor | nowhere; it is a token a client holds | — |

Both derived rows are maintained inside the write transaction that writes the
record (spec §12), so a reader never observes a record without its row.

## The economy this exists for

A record body is protobuf and may be kilobytes. Deciding "is this archived, and
is its importance above 0.7" by decoding one is what turns a listing of ten into
a scan of a hundred thousand. The attribute row is tens of bytes, fixed-width
per slot, and answers both questions.

The shape everywhere is: **an access path narrows, the row decides, and the
caller reads only the page it returns.** Listing ten memories from a corpus of
any size costs one row read per candidate examined and exactly ten record-body
reads, at every page including the last. `TestListReadsOnlyThePageItReturns`
measures this against a counting store rather than asserting it.

## The slot table

Slots 0–9 keep the numbers Rust Remem gave them (`services/attrs.rs`), so an
imported corpus lands on the same addresses. Slots 10 and 11 are new.

| Slot | Name | Type | Indexed | Note |
|---|---|---|---|---|
| 0 | `archived` | bool | no | Two values; a postings list buys nothing |
| 1 | `policy` | string | no | Three values over a corpus; the index would be three enormous runs |
| 2 | `importance` | f32 | **yes** | |
| 3 | `created_at` | u64 | **yes** | The only immutable ordering |
| 4 | `accessed_at` | u64 | no | |
| 5 | `access_count` | u32 | no | |
| 6 | `health` | f32 | **yes** | Rust leaves this unindexed |
| 7 | `emotional_valence` | f32 | no | Rust indexes this |
| 8 | `arousal` | f32 | no | Rust indexes this |
| 9 | `next_attention_at` | u64 | **yes** | The due-time index Phase 10's sweeps walk |
| 10 | `updated_at` | u64 | **yes** | New |
| 11 | `last_recalled_at` | u64 | **yes** | New |

A slot number is an address on disk: never reused, never renumbered. Retire a
slot and add a new one — the declaration survives retirement because the *type*
is the width a reader needs to skip past a value it no longer has a name for.

Two slots changed meaning from Rust, and neither costs an import anything,
because attribute rows are derived and an import re-projects them rather than
carrying them:

- **Slot 1 was `memory_type`, a u8 enum, and is now `policy`, a string.** Plan
  §II.6 replaces the enum with named retention policies, so adding a retention
  behaviour is data rather than a new variant every match arm has to learn about.
- **`health` is indexed here and `valence`/`arousal` are not.** Ordering by
  health is one of the five orderings this phase promises; nothing orders by the
  affective pair, and an index nothing reads is a write per record for nothing.

`Indexed` is the expensive column: one extra key per record on every write and
one deletion on every update. An unindexed slot still settles predicates from
the row, for free, because the row is read anyway.

### Every declared slot is populated

`TestEverySlotIsPopulated` (ported from Rust's `every_schema_slot_is_projected`)
holds it. A declared slot the projection skips has an index with no entries, so
ordering a listing by it returns nothing at all — which a user reads as "I have
no memories", not as a bug.

That is why the record gained its lifecycle fields (protobuf 22–30) in this
phase rather than in Phase 10, which owns the behaviour that moves them.
Carrying the field without the behaviour costs nine values in a protobuf;
declaring a slot without the field costs an empty index.

### `Fields.WithDefaults` keys on the policy and nothing else

Every record written from Phase 5 declares a policy and no record written before
does, which makes the policy the one honest signal for "this group was never
set". A field-by-field zero check would be wrong in a way that ships: **health 0
always archives** (plan §II.10 row 8), so reading a fully-decayed record's 0 as
"unset, therefore 100" silently resurrects it. An importance of 0 is equally a
value a caller may set on purpose.

### Two float values are normalised before they reach an index

`attr.F32` folds both, because both arrive from arithmetic rather than from a
caller and no input validation upstream catches either.

- **NaN becomes 0.** It order-encodes above every finite value, so one of them
  poisons every range query over the slot. This is Rust's `finite` backstop.
- **−0.0 becomes +0.0.** They are the same number and different bit patterns, so
  an index that kept the distinction holds one value at two positions, and a
  record at the lower one is invisible to a filter for 0. A property test found
  this; nobody types −0.0, and the resulting index sorts, round-trips and
  returns records.

## Atomicity

| Operation | Boundary |
|---|---|
| Write a record | One transaction: body, canonical vector, attribute row, every indexed slot's entry |
| Update a record | Same transaction, and it **deletes the old index entry** |
| Delete a record | Same transaction: body, vector, row, every entry |
| Backfill a batch | One transaction: up to 1,000 records' rows *and* the migration cursor |

The write path cannot forget the rows: `attr.Indexer` satisfies `record.Indexer`
and is a repository option, so a new write path gets index maintenance without
opting in. A record whose row was never written exists and cannot be listed.

### The stale entry is the failure worth naming

An update that writes the new index entry without deleting the old one leaves a
record at a rank it no longer holds — at importance 0.9 forever, though its
importance is 0.1. The entry points at a record that exists, so nothing detects
it, and a listing of the most important memories keeps returning it.

Verification on read is the second line of defence, not the first: `select`
re-reads every candidate's row and drops one whose value no longer matches the
entry that produced it. That makes a stale entry *harmless*, which is not the
same as *absent* — it is still read, still costs a row read, and still counts
against the walk's effort budget. Deleting it is what keeps the index the size
of the corpus rather than the size of its history.

Verification catches a stale entry. Nothing here catches a **missing** one: a
record whose entry was never written simply does not surface. That asymmetry is
why the backfill blocks start-up.

## Access paths and widening

The planner is deliberately small and has no cost model: a vector present means
a semantic step, its absence means an ordered walk, and an ordering the caller
named is honoured rather than optimised away. A cost model that silently changed
the order of a listing would change a contract.

A filtered search asks its access path for more candidates than it needs,
because some are discarded by predicates the path cannot answer. The budget
starts at 3× the requested limit and doubles to `search.widen_max_factor` (32×),
then reports `Truncated`.

**A listing widens to its own, larger bound**, `search.list_max_factor` (128×),
since Phase 13. That is Rust's pair of numbers (`config.rs:290-300`), and Rust's
reason holds here unchanged: a listing candidate costs one attribute-row read on
an index already being walked, where widening a search re-runs a similarity walk.
Sharing the search's number truncates ordinary listings, because at 32× a page of
five under a filter keeping one candidate in thirty cannot be filled from several
hundred records. Until Phase 13 Go shared it, and the Phase 13 behaviour-baseline
audit found the gap. `TestAListingWidensToItsOwnBound` needs 401 candidates for a
page of five and returned two marked `Truncated` before the fix.

The two paths widen differently, and the difference is not incidental:

- **The ordered walk resumes.** Doubling the budget buys new candidates rather
  than re-examining the ones already rejected, so widening to a bound costs that
  bound and not nearly twice it.
- **The vector search restarts.** A nearest-neighbour index answers "the k
  nearest" and there is no position to continue from. The repeated work is the
  price of the access path.

### `truncated` means one thing

The widening budget was spent before the requested number of matches was found,
so matches may exist that were never looked at. It is never a stand-in for "there
are none", and never for an index that is still being built — an incomplete index
does not serve at all.

`Truncated`, `HasMore` and a short page are three separate facts because a caller
that cannot distinguish them eventually renders the third as the first, and a
user reads that as their data having gone.

## What a page of results promises

Every page of a listing uses the same pinned snapshot. The memory service owns
that snapshot in a paging session, bounded to 128 concurrent sessions and
expired after five minutes without a continuation request. This makes all five
orderings exact across pages: later writes and changes to mutable ordering
values are outside the pinned view, so they can neither duplicate nor omit a
record from that listing.

Finishing a listing, expiry, or service shutdown closes its snapshot. Capacity
is refused with `Unavailable` instead of evicting a live session. A continuation
renews the inactivity deadline; an abandoned client therefore pins storage for
at most five minutes.

`TestListIsStableAcrossConcurrentWrites` drives 1,000 interleaved creations
through a paged `created_at` listing and asserts every original record comes back
exactly once.

## The cursor

Opaque, versioned, base64url. It carries the ordering slot, direction, ordering
value, record id, and an unpredictable paging-session identity. The service
binds that identity to the tenant and query.

It carries **no physical key**. A physical key contains the tenant prefix, so a
token would both disclose the tenant name and be editable to address another
tenant's rows (plan §II.9). Every resume is rebuilt from the value and the id
inside the tenant the request already resolved to.

Five refusals, all `errs.Invalid` rather than `NotFound` — the token is
malformed *for this request*, and "not found" would suggest presenting it
somewhere else might work:

- A token from another tenant.
- A token for a different ordering. Resuming under a different sort would walk a
  different index from a position that means nothing in it, and return an
  arbitrary slice of the corpus that looks like a page.
- A token at an unknown version. The fields after the version byte are
  positional, so misreading one resolves to a real but wrong position.
- A token whose session expired or belongs to a previous server process.
- A token presented with different ordering or archive visibility.

## Rank fusion

RRF at k = 60: a record's contribution is `1/(k + rank + 1)` from each list it
appears in, summed. Fixed rather than configurable, because it is a
compatibility surface with Rust and a tuned k would make every differential
comparison a comparison of two tunings.

Float32, accumulated in list order, ties broken on the record id ascending —
matching `engine/query/merge.rs` exactly. Float64 would put ties in different
places, and without the deterministic tiebreak two identical queries return
different truncated result *sets*, not merely different orders.

Rank fusion needs no score normalisation, which is the point. A vector index
returns distances, an inverted index returns term frequencies and a traversal
returns depths; every choice of conversion onto a common scale is a silent
re-weighting of the sources.

### Two numbers, and why relevance excludes the graph

`Score` is relevance in [0, 1], comparable across requests. `FusedScore` is what
the ordering was decided by, a function of rank rather than similarity, not
comparable across requests. Both are reported so neither has to be guessed at.

Relevance is the best **non-graph** source's score, falling back to the graph's
only when it is the sole evidence. Being one hop from an anchor memory is
context, not evidence that the content matches the query, and letting it set
relevance reports 0.5 for an unrelated neighbour — which is the number a caller
then puts a threshold against (REM-74).

A single-step search is not fused at all: `FusedScore` is that step's own
ordering value (`1/(1+d)` for a vector search, the scale Rust's single-source
path reports), because running RRF over one list would replace a meaningful
distance with a rank-derived number that is not comparable to anything.

## A query is a value

`query.Query` holds no closure, no index handle and no open iterator, and it
round-trips through JSON without loss — predicates included, through a tagged
union with an `op` discriminator and a typed value.

This is the property `docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md` §3 identifies as
missing from the Rust implementation and as a precondition for sharding. It has
to be right before any consensus code exists rather than after.

`Tags` and `RelatedTo` were on the struct before the indexes that answer them
existed, and were refused by the planner by name until they did — a planner that
ignored them silently would have returned the unfiltered corpus. `RelatedTo`
became a graph step in Phase 6 and `Tags` became a filter in Phase 8.

`Tags` is worth a note, because the obvious wiring is wrong: a tag adds **no**
access path. It is settled per candidate, and only a keyword search pushes it
down into its own step as a required term. Giving it a step made it a *source* —
every tagged memory became a candidate whether or not the rest of the query
reached it — which is recorded in `docs/architecture/text.md`.

## Failure semantics

| Failure | Behaviour |
|---|---|
| An attribute row will not decode | The candidate is dropped from the walk. The row is derived; one damaged sidecar must not take a listing offline |
| An attribute row is missing on a write | The write proceeds. Stale entries survive and are verified away on read until a rebuild |
| A record body is missing for a returned hit | The hit is skipped. The record is the authority on what exists |
| An index entry is stale | Verified away on read, at the cost of one row read |
| An index entry is missing | **Not detectable.** The record does not surface — which is what the backfill exists to prevent |
| A cursor is refused | `errs.Invalid`, naming why, so a client restarts the listing rather than retrying |

## Plan corrections

Three, all decided with the user.

### 1. The attribute backfill blocks start-up: `StrategyRebuild`

Phase 4 shipped three strategies. This adds a fourth, and the reason is the
asymmetry above: a background step transforms data that is already readable, so
a request arriving mid-migration reads either the old shape or the new and both
are answerable. **A rebuild step produces an access path, and a half-built access
path does not return older answers — it returns fewer.**

There is no honest way to report that. `Truncated` means the widening budget was
spent and the plan's own completion criteria forbid overloading it; a second flag
puts the burden on every client, and the ones that forget show users a wrong
answer that looks right.

So the cost is paid where it is visible: the server refuses traffic until the
index exists, and upgrade downtime is proportional to corpus size. `Split` puts a
rebuild step on the start-up side, still by position — a rebuild step registered
after a background one waits its turn, because it needs the version that step
produces.

This was verified against the real binary. A live directory of seven memories had
its attribute rows deleted and its manifest left current, so no migration was
planned; the server started, `GET /api/v1/memories` returned an empty page
reporting `truncated: false, has_more: false`, and every record was still
fetchable by id. Winding the manifest back to `attr_schema=1` instead made the
same binary run `0001-attribute-rows` at start-up, process 7 records, and serve
the listing again.

`RewritesData` is false: the step writes into the attribute row and index spaces
only and reads bodies without modifying them, so a pre-migration backup would
preserve nothing it touches. If it ever starts rewriting bodies, the flag moves
with it.

### 2. Paging sessions hold the snapshot object

The first implementation treated a process-local snapshot counter as a restore
check. It reset on ordinary restart and could accept a restored store once its
counter caught up, so it proved neither claim. A sequence number also cannot
reopen a Pebble snapshot.

The service now retains the snapshot object itself in a bounded session. This
delivers the exact cross-page view §II.9 requires while limiting pinned storage
by both capacity and inactivity expiry. Tokens deliberately do not survive a
restart: the snapshot they name no longer exists, so the continuation is
refused and the client starts a new listing.

### 3. Search's widening became an optimisation rather than the mechanism

Phase 3 excluded archived memories by asking the index for far more candidates
than it needed and reading each candidate's **record** to find out whether it was
archived. `internal/vector`'s own comment predicted the change. It is now a
predicate settled from the attribute row: tens of bytes, no protobuf decode, the
same answer. The widening survives, bounded at 32×, doing what it should always
have done — stopping a filtered search returning short.

## What this phase deliberately does not decide

- **No keyword or graph step.** `Query.Tags` and `Query.RelatedTo` are declared
  and refused. Phases 8 and 6.
- **No cost model.** One question, one access path.
- **No lifecycle behaviour.** The lifecycle fields are written and never moved;
  `next_attention_at` is populated as zero, meaning "due now", which is the safe
  reading of a record nothing has scheduled. Phase 10 computes it.
- **`memory.Orderings` is a product decision, not a storage one.**
  `next_attention_at` is an access path and is not offered: sorting memories by
  when a background job intends to look at them next is a number that means
  nothing to a user and that Phase 10 is free to redefine.
- **No per-tenant slot tables.** One registered table for the directory, as
  Phase 4's `schema.OpenSlots` gate assumes.

## Ranked paging

Phase 13 put a cursor on four more surfaces, and there are two mechanisms behind
the one opaque token. A caller never learns which kind it holds. This document
has to, because their guarantees differ, and conflating them here would be the
mistake even though conflating them on the wire is right.

| Surface | Mechanism | Order | Survives a restart |
|---|---|---|---|
| Listing, `GET /memories` | Keyset inside a snapshot session | The ordering slot, then the id | No |
| Search | Ranked session | The fused ranking | No |
| `related` | Ranked session | Path strength | No |
| History | Keyset | Newest first, by time then sequence | **Yes** |
| Connections | Keyset | Relationship type, then the far endpoint | **Yes** |

A keyset cursor names a durable position in a key range. It needs no session,
pins nothing, cannot exhaust a budget, and outlives the process. What it does
not give is a frozen view: an event or an edge written between two pages appears
if it sorts after the cursor and not otherwise. For an append-only stream and a
neighbour list that is the right trade; `graph.md` records the connections walk.

A ranked cursor names a position in a ranking that exists only in memory, which
is why it dies with the process.

### Why not a keyset on the score

A keyset on the fused score is not merely worse than a session. It is wrong.

RRF's fused score is a function of the candidate set: a memory's contribution is
`1/(k + rank + 1)` from each list it appears in, and a list's ranks depend on how
deep that list was read. Deepening a search to serve page two changes the ranks,
and so the fused scores, of results page one already returned. A page boundary
expressed as "fused score below 0.0312" names a position in a ranking that no
longer exists by the time it is presented. The scale is not even fixed across
modes: a single-step search reports `1/(1+d)` where a fused one reports a
rank-derived number (see "Two numbers" above).

Offset paging works and is still wrong in a smaller way. It is quadratic in page
depth — page *n* re-runs the search to depth *n × limit* — and for the same
rescoring reason a memory can land on two pages or on none. Rust offers no search
paging, so offset would not have been parity either.

### Materialise once, slice after

The first page runs the search once, over a snapshot pinned in a paging session,
to a depth of `max(limit, search.max_page_depth)`. The ranking is kept as
`(id, score, fused score, sources)` — never the bodies. Each later page is a
slice of it, so a session costs one search, not one per page.

**Bodies are read per page, through the same pinned snapshot.** That keeps a
session to tens of bytes per hit rather than the size of the memories it names
— measured below at about 270 KiB for a 2,000-hit session — and it is why page
three sees the corpus page one saw: a memory deleted in between is still there
to be read, because the snapshot is.

A session is bound to the tenant and to a fingerprint of the normalised request:
query text, search type, tags, every predicate, `related_to` and its traversal
inputs, and `include_archived`. **Page size is deliberately not in the
fingerprint**, so a caller may take ten and then fifty. A token presented against
a different request is refused with the message listing already uses.

Ranked sessions share listing's budget of 128, because they pin the same scarce
thing; two budgets would mean two exhaustion messages for one resource. A ranked
session's idle window is shorter — 60 seconds against listing's five minutes —
because searches arrive far more often than listings and each pins a snapshot
for the same window. Both numbers are measured below rather than chosen.

### `truncated` on a paged search

It means the depth bound was reached: the ranking was materialised to
`search.max_page_depth`, and there may be matches below that line nothing ranked.
It is set on **every** page of that session, the last included, so a final page
can report `has_more: false` and `truncated: true` together — and both are true.

It is never a stand-in for "there are none". A search whose matches ran out
before the bound reports a short last page with `has_more: false` and
`truncated: false`, and that combination is the only one meaning "that was
everything".

A cursor whose position lies beyond the materialised ranking — which a
well-behaved client cannot produce — is refused with "start the search again"
rather than answered with an empty page, which would read as the end.

## Ranked paging measurements (2026-09-11)

Task 3.9 measured the search service with a temporary in-module harness,
`internal/tools/pagedepth`, deleted after the run. This measures resource cost
and automatic consumption to exhaustion, **not human paging habits or semantic
relevance**. No finite cutoff exhausted every semantic or hybrid search: without
a relevance floor, those modes can rank the entire corpus. A default depth is
therefore an explicit resource tradeoff, not a discovered universal stopping
point.

The host was an Intel Xeon W-2135, six exposed CPUs, Linux amd64, Go 1.27.1,
`GOMAXPROCS=6`. Each corpus used its own Pebble directory with a 256 MiB block
cache, the default exact flat cosine index, and 384-dimensional unit vectors
from `embeddingtest`'s deterministic bag-of-words embedder. This excludes ONNX
inference, HTTP transport, HNSW approximation and real-world text distributions.
The service, transactions, canonical vectors, attribute rows, BM25 postings,
fusion, snapshots and cursors were the production implementations. There were
no concurrent corpus writes during paging; snapshot-induced retention of old
SSTables under writes was not measured.

For corpus sizes 1,000, 10,000 and 100,000, record `i` contained:
`knowledge topic%03d family%02d record%06d durable searchable memory`, with
arguments `i % 100`, `i % 10`, `i`. IDs were generated normally. Queries `q`
from 0 through 99 chose, by `q % 5`, respectively `topic%03d(q)`,
`family%02d(q % 10)`, `knowledge`, `record%06d(q * 7)`, and `absentterm`.
Thus each mode had twenty topic, family, corpus-wide, single-record and
missing-term requests: 100 requests over 44 distinct query strings, with common
and missing terms deliberately repeated. The same queries ran at depth bounds 200, 500, 1,000,
2,000 and `corpus size + 1` ("full"), with 200 results per page, following every
cursor until `has_more` became false: 4,500 measured search sessions in total.
Searches used `explain=false`, no extra filters, and no archived records.

A GC preceded each first page. First-page elapsed time includes ranking
materialisation and reading the first 200 bodies; later pages read their own
bodies through the pinned snapshot. Allocation counts use the process-wide
`TotalAlloc` difference around that first call, not retained heap. Timing is one
pass over each query set, with a warm shared block cache after earlier queries;
short configuration checks and the separate idle-load experiment overlapped
parts of the run. Treat the figures as this host's measured costs, not isolated
latency benchmarks or a production SLA. Quantiles are nearest-rank p50/p95.

### Idle expiry and the shared session budget

A second run used the 1k corpus, depth 1,000, hybrid searches returning an initial
page of ten, and one listing held open for the whole experiment. It advanced the
injected clock through 180 seconds, opening and abandoning either one or two
searches each second. These are explicit synthetic arrival rates, not production
traffic measurements.

| Ranked idle TTL | New searches/second | Accepted | Refused | First refusal at elapsed second |
|---|---:|---:|---:|---:|
| 30s | 1 | 180 | 0 | none |
| 30s | 2 | 360 | 0 | none |
| 60s | 1 | 180 | 0 | none |
| 60s | 2 | 360 | 0 | none |
| 300s | 1 | 127 | 53 | 127 |
| 300s | 2 | 127 | 233 | 63 |

`paging.ranked_ttl` defaults to **60s**: the longest tested window that accepted
every arrival in both scenarios, allowing more time between pages than 30s.
This is a workload-informed policy choice, not a measurement of user think time.
At the two-per-second workload, 120 abandoned ranked sessions plus the listing
fit below the shared limit of 128. More abandoned searches, or more concurrent
listings, can still fill it; TTL is not admission-rate control.

The limit is shared by listing, search and traversal. Active continuations renew
their own idle deadline, exhaustion releases a session immediately, and unused
sessions expire on the one-second sweep or the next acquisition. Listings keep
their five-minute idle window. Changing the ranked TTL never changes their
deadline. Expired cursors are refused and require a fresh search.

### At capacity, the least recently used idle session is reclaimed (Phase 13)

Until Phase 13 a full budget **refused a new first page**. The measurement above
chose the TTL so that one or two abandoned searches a second fit, and said
plainly that TTL is not admission-rate control. What it did not weigh is that the
budget is **one budget for every tenant**. Phase 13's concurrency test —
ten tenants, a thousand operations in flight — answered 125 requests with 503
across all ten, and the reduction is two tenants and a loop:
`TestOneTenantsAbandonedPagesDoNotRefuseAnother` has one tenant read 128 first
pages whose results continue and never ask for the second, after which the other
tenant's first listing was refused. About three such searches a second — far
inside the per-tenant rate limit of 100 — held every tenant's search, listing and
related-memory first pages at 503 until the sessions aged out. The rate limiter
cannot see it, because it counts requests, not sessions left behind.

Decided with the user: at capacity the registry reclaims the **least recently
used session no request is reading through**, and opens the new one in its place.

- **The bound still holds.** At most 128 snapshots are pinned, as before.
  Reclaiming is a retirement, which closes the snapshot at once because the
  session has no readers.
- **A refusal still exists, and means something narrower.** Only a session with
  no reader in flight is reclaimable, so when all 128 have a request reading
  through them at that instant, a new session is refused with 503 — "all 128
  paging sessions are being read". That is the memory bound doing its job, and
  `TestAFullBudgetOfActiveReadersStillRefuses` holds it.
- **Least recently used, not first opened.** Every open, continuation and release
  stamps a use counter, so a client actively paging through a long result keeps
  moving to the back of the queue and is never the one a newcomer displaces.
  A counter rather than a time, because two uses at one clock reading must still
  order — and a tie broken by map iteration would reclaim at random.
- **The cost is an early expiry, and it is the refusal clients already handle.**
  A cursor whose session was reclaimed is refused as Invalid, "missing or expired:
  it was idle past its deadline, reclaimed for a newer request, or issued before a
  restart". The TTL remains the longest a cursor is guaranteed to wait for, not a
  promise that it will; under load the guarantee is shorter, and the client starts
  its search again, exactly as it does after a restart.

The alternatives were a per-tenant share of the budget, which isolates tenants
but hands a single-tenant deployment a fraction of its own server unless the
share adapts; reclaiming from the tenant holding the most sessions, which is
fairer and makes the eviction order a policy of its own; and keeping the refusal
and documenting it. Reclaiming the oldest idle session fixes the starvation for
every tenant count with the least new policy.

An operator can set `paging.ranked_ttl = "2m"` in TOML,
`REMEM_PAGING_RANKED_TTL=2m`, or `--paging.ranked_ttl=2m`. Nonpositive durations
are rejected at startup. The setting applies to both search and related-memory
rankings because both retain the same kind of snapshot-backed session.

### Exhaustion and materialisation cost

With the depth set to corpus size plus one, all nine corpus/mode combinations
finished without truncation. These are the observed consumption distributions
and end-to-end times to drain every page:

| Corpus | Mode | Depth p50 / p95 / max | Drain ms p50 / p95 |
|---:|---|---:|---:|
| 1,000 | semantic | 1,000 / 1,000 / 1,000 | 21.5 / 33.9 |
| 1,000 | keyword | 10 / 1,000 / 1,000 | 0.5 / 29.8 |
| 1,000 | hybrid | 1,000 / 1,000 / 1,000 | 33.2 / 44.7 |
| 10,000 | semantic | 10,000 / 10,000 / 10,000 | 372.3 / 407.6 |
| 10,000 | keyword | 100 / 10,000 / 10,000 | 3.5 / 329.9 |
| 10,000 | hybrid | 10,000 / 10,000 / 10,000 | 425.2 / 548.0 |
| 100,000 | semantic | 100,000 / 100,000 / 100,000 | 2992.2 / 3265.3 |
| 100,000 | keyword | 1,000 / 100,000 / 100,000 | 40.1 / 2568.4 |
| 100,000 | hybrid | 100,000 / 100,000 / 100,000 | 3117.2 / 3888.7 |

First-page milliseconds, p50 / p95, at every measured bound. "Full" is corpus
size plus one. Every semantic/hybrid request consumed `min(bound, corpus size)`;
the keyword column also gives its consumed-depth median and count reporting
`truncated` out of 100. Hitting the bound conservatively reports truncation,
even when its last result happens to be the corpus's last match.

| Corpus | Bound | Semantic ms | Keyword ms | Hybrid ms | Keyword depth p50; truncated / 100 |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 200 | 11.6 / 14.1 | 0.3 / 6.3 | 9.5 / 20.0 | 10; 20 |
| 1,000 | 500 | 13.6 / 16.6 | 0.5 / 13.5 | 16.3 / 24.1 | 10; 20 |
| 1,000 | 1000 | 18.9 / 21.2 | 0.5 / 15.9 | 17.7 / 27.1 | 10; 20 |
| 1,000 | 2000 | 16.9 / 21.1 | 0.3 / 9.1 | 13.0 / 27.9 | 10; 0 |
| 1,000 | full | 11.1 / 19.3 | 0.5 / 16.6 | 18.6 / 30.2 | 10; 0 |
| 10,000 | 200 | 63.6 / 73.2 | 4.1 / 29.0 | 49.8 / 79.3 | 100; 40 |
| 10,000 | 500 | 71.3 / 75.3 | 4.2 / 31.6 | 71.4 / 96.8 | 100; 40 |
| 10,000 | 1000 | 77.3 / 81.6 | 4.0 / 36.4 | 79.3 / 106.6 | 100; 40 |
| 10,000 | 2000 | 87.4 / 93.7 | 3.9 / 47.7 | 83.1 / 129.3 | 100; 20 |
| 10,000 | full | 160.9 / 181.0 | 3.5 / 120.2 | 187.9 / 319.2 | 100; 0 |
| 100,000 | 200 | 442.2 / 604.1 | 8.3 / 206.1 | 553.3 / 710.6 | 200; 60 |
| 100,000 | 500 | 592.3 / 605.7 | 13.8 / 168.0 | 607.1 / 766.9 | 500; 60 |
| 100,000 | 1000 | 600.8 / 611.3 | 18.1 / 167.7 | 614.7 / 769.5 | 1000; 60 |
| 100,000 | 2000 | 617.0 / 629.8 | 18.9 / 174.3 | 628.1 / 773.1 | 1000; 40 |
| 100,000 | full | 1348.9 / 1444.4 | 19.2 / 899.9 | 1444.2 / 2202.1 | 1000; 0 |

First-call transient allocation, p50 in MiB (1 MiB = 1,048,576 bytes):

| Corpus | Bound | Semantic | Keyword | Hybrid |
|---:|---:|---:|---:|---:|
| 1,000 | 200 | 3.01 | 0.09 | 3.06 |
| 1,000 | 500 | 3.46 | 0.09 | 3.59 |
| 1,000 | 1000 | 4.19 | 0.09 | 4.40 |
| 1,000 | 2000 | 4.19 | 0.09 | 4.44 |
| 1,000 | full | 4.19 | 0.09 | 4.41 |
| 10,000 | 200 | 17.56 | 0.69 | 17.76 |
| 10,000 | 500 | 18.10 | 0.69 | 18.36 |
| 10,000 | 1000 | 19.05 | 0.69 | 19.42 |
| 10,000 | 2000 | 20.84 | 0.69 | 21.40 |
| 10,000 | full | 33.08 | 0.69 | 34.94 |
| 100,000 | 200 | 163.14 | 1.63 | 163.80 |
| 100,000 | 500 | 163.67 | 2.04 | 164.81 |
| 100,000 | 1000 | 164.63 | 2.76 | 166.60 |
| 100,000 | 2000 | 166.42 | 2.76 | 168.58 |
| 100,000 | full | 326.34 | 2.75 | 343.18 |

A small retained ranking does not imply a small retrieval allocation. The flat
index still scans every canonical vector; at 100k, even depth 200 allocated
about 163 MiB for a semantic first page. The depth setting bounds retained
ranking and candidate materialisation beyond a page, not the underlying index's
entire search cost. Full ranking also makes continuation work proportional to
all returned bodies, which the bounded session avoids.

### Retained memory and the depth default

On the 100k corpus, a final sweep opened hybrid searches for `knowledge`,
returned ten results and retained their cursors without continuing them. The
clock stayed fixed while sessions accumulated. Post-GC heap growth relative to
an empty service is below, in KiB; per-session figures divide the 128-session
measurement. This includes registry/snapshot overhead and residual process
allocation, so the larger sample is more useful than the single-session delta.
It excludes historical SSTable retention and uses `explain=false`.

| Depth | 1 session KiB | 16 sessions KiB | 64 sessions KiB | 128 sessions KiB | KiB/session at 128 |
|---:|---:|---:|---:|---:|---:|
| 200 | 11.8 | 467.4 | 1843.6 | 3709.7 | 29.0 |
| 500 | 57.4 | 1076.6 | 4336.6 | 8700.4 | 68.0 |
| 1000 | 134.3 | 2165.2 | 8665.2 | 17338.9 | 135.5 |
| 2000 | 269.4 | 4312.5 | 17296.6 | 34594.3 | 270.3 |

At every tested depth, request 129 returned `Unavailable`; advancing the clock
by the configured 300 seconds reclaimed capacity and allowed another search.
The chosen 2,000-hit bound retained 35,424,512 bytes (33.8 MiB) at 128 open
sessions, about 270 KiB each, versus 8,909,216 bytes (8.5 MiB) at depth 500.

`search.max_page_depth` defaults to **2,000**. This is the smallest tested bound
that completely serves the largest corpus's 1,000-match topic queries without
the conservative equality truncation signal. At 100k it reduced keyword
truncation from 60 to 40 requests out of 100, while hybrid first-page p50 moved
from 607.1 to 628.1 ms and p95 from 766.9 to 773.1 ms. The additional retained
heap at a full session registry was about 25.3 MiB. That is the selected
coverage/memory tradeoff; broader keyword and all large-corpus semantic/hybrid
rankings still truncate. The data do not establish that users stop at 2,000,
nor that this is optimal for another corpus or vector implementation.

The effective materialised target is `max(requested page size,
search.max_page_depth)`: the setting cannot make the first requested page
shorter by itself. The normal `search.max_limit` of 200 bounds that exception.
`truncated` stays true throughout a session that reaches its target, even on
its final page where `has_more` is false. Raise the depth when deeper ranked
retrieval is required, accounting for the first-page work and retained heap;
listing remains the ordered exhaustive corpus access path.

Set `search.max_page_depth = 4000` in TOML,
`REMEM_SEARCH_MAX_PAGE_DEPTH=4000`, or `--search.max_page_depth=4000` to change
it. Zero and negative values are rejected at startup. Both paging settings
follow the normal flags > environment > file > defaults precedence and appear
in the redacted startup configuration; the composition root passes them to the
memory service. Search and related-memory rankings use the same depth default.
