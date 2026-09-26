# The graph

How relationships between memories are stored, why the reverse index is derived,
what bounds a traversal, and what "related" means when a user asks for it.

Package: `internal/graph`, with the access path in `internal/query`, the
user-facing operations in `internal/memory`, the REST surface in
`internal/api/http/connections.go` and the checker in `cmd/remem-admin`.

Spec §14 asks for relationships as first-class data with both directions
indexed, bounded traversal, type filtering and relationship updates. Plan §II.10
row 7 asks for ranking that uses edge strength (REM-83). This document answers
both and records the five places Phase 6 departed from the plan.

## What is authoritative

| Thing | Where | Rebuildable? |
|---|---|---|
| Out-edge (from, type, to) → strength, timestamps, metadata | edge-out space | **No.** It is canonical |
| In-edge (to, type, from) → the same bytes | edge-in space | Yes, from the out-edges |

Both rows are staged into the same transaction (spec §12), so a reader never
observes an edge without its reverse entry or the reverse without its edge. A
decode failure on an out-edge is `errs.Corruption` naming the key, never a zero
value quietly substituted (plan §II.4).

There is no edge id. An edge's identity is the triple (from, type, to): two
memories may be connected several ways, and each way is a separate edge with its
own strength. An id would make an edge a thing that can be pointed at, and
nothing in Remem points at one.

## The derived row holds a copy, and what that costs

The in-edge row stores the out-edge's value verbatim rather than being an empty
pointer row.

The alternative costs a read per neighbour. `In(to)` and every `MinStrength`
filter would have to fetch the out-edge row to recover the strength — precisely
the per-candidate record read Phase 5 removed from search — and a traversal in
the `In` or `Both` direction pays it once per node examined, inside the bound
that is supposed to make traversal cheap.

Two consequences follow, and they are easy to conflate:

**The rebuild copies rather than re-encodes.** `RebuildIn` writes each stored
value through unchanged. Decoding and re-encoding would drop any protobuf field a
newer binary wrote and this one has never heard of, which is exactly what a
rolling upgrade must not do to data it is only relaying — and the loss would be
invisible to every query, because the edge still decodes and still ranks.
`TestRebuildKeepsFieldsThisBinaryDoesNotUnderstand` is what holds it; a byte
comparison of two edges this binary wrote cannot, because a deterministic
re-encoding reproduces every field it knows about.

**The encoding is deterministic anyway, for a different reason.** `Edge.meta` is
a protobuf map and Go randomises map field order on marshal unless told not to.
`proto.MarshalOptions{Deterministic: true}` makes an edge's stored bytes a
function of the edge and of nothing else: two nodes handed the same edge write
the same row, which is what Invariants 8 and 9 require of anything inside a
replicated apply path (Phase 14), and re-importing a snapshot produces the same
database rather than one that merely holds the same edges (Phase 12).

An earlier draft of this phase claimed determinism is what makes the rebuild
byte-identical. It is not — the verbatim copy is. The claim was caught by making
the rebuild re-encode and watching the byte comparison pass unchanged.

## Durable formats introduced here

**Relationship type numbers.** Two bytes of every edge key. The eight names and
their order are Rust Remem's, so an imported corpus needs no remapping:

| Number | Name |
|---|---|
| 1 | `related_to` |
| 2 | `caused_by` |
| 3 | `part_of` |
| 4 | `references` |
| 5 | `contradicts` |
| 6 | `supports` |
| 7 | `similar_to` |
| 8 | `derived_from` |

Zero is unspecified and refused. A number is never reused and a retired type
stays retired — its number is what a reader needs in order to skip past a
relationship it has no name for. The names, not the numbers, travel over the API
and in JSON: a durable number in a wire format is one nobody reading a request
body can read.

**The edge body**, `record.v1.Edge`, under codec byte 3. It carries strength,
timestamps and metadata, and nothing else: both endpoints and the type are in the
key, and a second copy in the value is a second thing that can disagree with the
first — the rule `record.v1.Record` already follows for tenant and namespace.

**Versions.** Neither the edge key nor the edge body carries a version of its
own. The key layout is under the directory-wide `key_encoding` version. The body
is a protobuf message inside the record envelope, under `record_envelope`. A
relationship number is never reused and never renumbered, which is what lets a
reader skip a type it has no name for rather than misread it. The derived
in-edge row copies the body, so it moves with the same two versions.

## Traversal

### Best-first, not breadth-first

**A correction to plan §Phase 6's "bounded BFS" wording**, and the substance of
the REM-83 fix.

A traversal is bounded by a node budget, and how that budget is spent *is* the
ranking. Breadth-first spends it on the nearest hops: a memory with five hundred
weak direct neighbours consumes the whole budget on those, and the strongly
connected memory two hops out is never reached. Sorting the results afterwards
sorts a sample already chosen on the wrong criterion.

Worse, a breadth-first walk's path strength is whichever path it happened to find
first, which depends on the order the store yields edges in. That number would
move after a compaction, could not have a threshold put against it, and could not
be compared against the Rust reference in Phase 13.

The frontier is therefore a max-heap ordered by descending path strength — the
product of the edge strengths along a path. Each node reports the strongest chain
that reaches it; `MaxNodes` truncates to the strongest N rather than the
shallowest N; and `Truncated` means "weaker connections exist that were not
reached".

`TestTheNodeBudgetIsSpentOnTheStrongestConnections` pins it, and it was verified
by making the frontier a FIFO: the 0.95-then-0.95 two-hop node is then crowded
out of a five-node budget by forty 0.1 direct edges — precisely the shape Phase
11's bulk discovery creates.

### Every node within the depth bound is reached

Best-first changes how the reached set is **ranked**, never what it
**contains**. `MaxDepth` is hops from the anchor. With the node budget not
binding, the walk returns exactly the memories a breadth-first walk to the same
depth returns, which is what makes reachability comparable with Rust (§II.10
row 7 authorises the ranking change, not a reachability one).

**Until Phase 13 it did not hold.** The walk remembered each node's strongest
path, and expanded a node only at that path's depth. A node reached strongly at
two hops and weakly at one sat at the bound when `MaxDepth` was 2, and its
neighbour — two hops from the anchor through the weak edge — was never
returned. Rust returns it. It was found by reading the two walks side by side
for the Phase 13 reachability surface, before the harness had compared a
single graph.

The walk now remembers, per node, the strongest path at each depth. A path is
followed unless another path to the same node is at least as strong and no
deeper. One strongest and one shallowest path per node is not enough: a
middling path can lose to the first on strength and to the second on depth, and
still be the only one that reaches a further node strongly within the bound.
Depth is capped at 5, so this is at most six numbers per node reached.

`TestTraversalReachesEveryNodeWithinMaxDepth` holds the case.
`TestTraversalMatchesAHopBoundedReference` holds the family: a property test
over arbitrary graphs against the plainest possible reference (strongest product
over paths of exactly k hops, k up to the bound). It checks the reached set,
that each node is reported once, and each node's strength. Against the unfixed
walk it failed after 757 cases.

### Why it terminates

Strengths lie in [0, 1], so a path's product is non-increasing as the path grows.
The heap pops strongest first, so the first time a node is popped it holds the
strongest path within the bound, and that is the path it is reported by. A later,
weaker pop at a shallower depth is expanded but not reported again. A path is
pushed only when it **strictly** beats every path to that node at the same depth
or shallower. Going round a cycle multiplies by at most 1 and adds a hop, so it
never qualifies, which is what makes a cycle terminate without a visited-set
special case.

### Bounds

| Constant | Value | Why |
|---|---|---|
| `DefaultNeighbourLimit` | 100 | There is no unbounded neighbour read |
| `MaxNeighbourLimit` | 1000 | |
| `DefaultMaxDepth` | 1 | Rust's `find_related` default |
| `MaxTraversalDepth` | 5 | Rust's cap, kept |
| `DefaultMaxNodes` | 200 | New — Rust has no node bound, which is REM-83's other half |
| `MaxTraversalNodes` | 10000 | A traversal is never unbounded |

A bound above a ceiling is **refused by name, not clamped**. A caller who asked
for twenty hops and silently got five has a different picture of their graph than
the one they were given.

### What traversal does not do

It reads no record bodies. A per-node existence check would cost a record read
inside the bound that makes traversal cheap. An edge pointing at a deleted record
therefore survives the walk, is dropped where records are materialised, and is
reported by `remem-admin graph verify` — which is where an orphan is meant to
surface.

## Atomicity boundaries

| Operation | What commits together |
|---|---|
| `Add`, `Update` | The out-edge and the in-edge, in the caller's transaction |
| `Remove`, `RemoveAll` | Both rows of every edge removed |
| Hard delete of a memory | The record, its vector, its attribute rows **and every edge touching it** |
| `RebuildIn` | Bounded batches, not the whole rebuild |

`Add` and `Update` read through the transaction and make the write conditional
on what they read (`tx.Expect`), which is the posture the completed-phase review
settled for every read-modify-write in the system. `Update` rewrites the whole
row to change one field, so without it a concurrent writer's metadata would be
silently replaced.

`RebuildIn` gives up atomicity of the whole rebuild deliberately: a batch the
size of a large tenant's reverse index is a memory cost proportional to the
corpus and a commit nothing can make progress against. A half-rebuilt derived
index is repaired by running the rebuild again; an out-of-memory commit repairs
nothing.

**A hard delete takes the memory's relationships with it.** This is new in Phase
6 and is not merely tidiness: leaving them would have traversal spend its node
budget reaching a record that no longer exists, so "memories related to this one"
would silently return fewer than it was asked for and nothing would say why. The
cost is one staged deletion per edge the record has — bounded by its degree, on
a rare and explicit operation.

## Rebuild and repair

Invariant 3: a derived index is rebuilt, never repaired. `RebuildIn` therefore
**clears** the reverse index before repopulating it, rather than reconciling it.
An entry nothing derives is removed by construction — including the orphan a
crash between the two key spaces leaves, which no reconciliation that only looked
at real edges would ever see.

The rebuild is tenant-scoped. Rebuilding every tenant is `tenant.ForEach` at the
call site, which is the single audited cross-tenant path (Invariant 1), and
`internal/graph` is not on that guard's allowlist.

While it runs the reverse index is incomplete, so reads in the `In` direction
under-report. Phase 9 registered it as the `graph.rebuild_in` job type and
Phase 12 gives it a CLI verb.

**Neither takes the tenant out of service for the duration, and that is an
outstanding debt rather than a settled decision.** For the reason
`internal/schema`'s `StrategyRebuild` exists — a half-built access path does not
return older answers, it returns fewer — a tenant being rebuilt should refuse
`direction=in` reads rather than under-report them. What keeps the exposure
small today is that nothing triggers this rebuild on its own: unlike the vector
and text indexes, the reverse index has no health signal and no self-repair
path, so a run is always something an operator asked for.

## What `verify` reports, and what it cannot

`remem-admin graph verify` walks both spaces and names five kinds of finding:

| Finding | Meaning | Repairable by rebuild? |
|---|---|---|
| `missing_reverse` | An edge with no reverse entry; `In` under-reports | Yes |
| `orphan_reverse` | A reverse entry with no edge; `In` reports a relationship that does not exist | Yes |
| `reverse_disagrees` | The two copies hold different bytes | Yes |
| `missing_record` | An edge to a memory that does not exist | **No** |
| `self_edge` | A memory connected to itself | **No** |

It repairs nothing. The last two are decisions about user data — remove the edge,
or restore the memory — and a checker that quietly made them would be destroying
relationships an operator never saw.

The report is capped at 1,000 findings but the **counts stay complete**: "here
are the first thousand of 4.2 million" is useful and "here are a thousand" is
not.

It opens the directory **read-only**, which is two decisions rather than one.
Verify never writes, so the mode matches what it does; and read-only refuses a
path that holds no database instead of creating one. Opening read-write answered
"no tenants found" for a mistyped `--data-dir`, which reads as a clean corpus —
found by the Phase 6 end-to-end verification.

It cannot run against a live directory: Pebble holds the lock, so the server must
be stopped or the command pointed at a backup checkpoint under `.backups/`. Exit code 2 means "worked, found problems" — zero on a broken corpus
would make it useless as a check, and an error would be indistinguishable from a
directory it could not open.

## Three things `related` reports separately

| Fact | Means |
|---|---|
| A short page, `has_more: false` | That was every reachable memory |
| `has_more: true` | The traversal reached more than `limit` asked for |
| `truncated: true` | The node budget was spent; more weakly connected memories were never *reached* |

`has_more` is about the page and `truncated` is about the walk, and they move
independently. Since Phase 13, `truncated` also reports that the ranking reached
`search.max_page_depth` before the walk was exhausted — the bound a search
session has, for the same reason.

### Paging `related`: the ranking is materialised, the walk is never resumed

Until Phase 13 this section said there was deliberately no cursor, because a
traversal has no stable position to resume at: the strongest path to a node can
change when any edge on it changes, so a cursor that *continued* a walk would be
a promise the walk cannot keep across a write.

That is still true, and it is why the cursor is shaped the way it is. The walk
runs **once**, over a snapshot pinned in a paging session. The reached set is
ranked by path strength and kept; each page is a slice of that ranking, and its
bodies are read through the same snapshot. Nothing is ever resumed, so the only
promise the cursor makes — "the rest of *this* ranking" — is one the session can
keep. An edge strengthened between pages does not reorder a ranking already
handed out, which is what a caller paging through one needs.

The cost is the ranked session's: the cursor dies with the session, after sixty
idle seconds or a restart, and the refusal tells the caller to start again. The
mechanism is `query.md`'s "Ranked paging"; `related` and search share it, and the
depth bound with it.

### Paging connections: a keyset, in two ranges

A connections listing is the opposite choice. It walks one record's edge keys,
and those are ordered — `(from, type, to)` for an out-edge, `(to, type, from)`
for an in-edge — so a position is simply `(relationship type, far endpoint)`, and
a cursor naming it is exact by construction. It needs no session, pins no
snapshot, and **survives a restart**. The price is that it is not a frozen view:
an edge added between pages appears if it sorts after the cursor and not
otherwise. The listing is ordered by relationship type and then by the far
memory's id — not by strength and not by time — because that is the key order,
and an order the keys do not have would need a session to hold it.

**`direction=both` is every out-edge, then every in-edge.** The two live in
different key spaces, `SpaceEdgeOut` and `SpaceEdgeIn`, and there is no ordering
comparable across them to merge on: an out-edge's far endpoint is its target and
an in-edge's is its source. So the cursor carries which range it is in beside the
position, and a caller paging both directions sees every outgoing relationship
before the first incoming one. That is stated here because it is otherwise a
surprise on page two.

Two cases at the boundary are handled rather than left to fall out. When the
out-range ends exactly on a page boundary, the in-range is probed so that
`has_more` stays true if anything points at the memory — otherwise a caller would
stop one page short with every incoming relationship unseen. And a cursor from one
direction presented under another is refused, since an in-range position means
nothing to an out-only walk.

The distinction was missing until the Phase 6 end-to-end verification found it:
five of thirty-three related memories came back with `truncated: false` and no
way to tell that from a memory with exactly five neighbours.

`GET /connections` on a memory that does not exist is 404, not an empty list, for
the same reason: an empty list is what a memory with no relationships returns,
and a caller who cannot tell those apart reads a typo as a memory with no
connections.

## Ranking, and what a score means

`GET /api/v1/memories/{id}/related` reports each memory's path strength as its
score. That number is already on the comparable 0..1 scale — it is a product of
unit-interval strengths — so it is taken unmodified. Inventing a conversion, a
depth decay say, would be a re-weighting nothing asked for.

When a semantic query is combined with an anchor the two steps are fused, and
relevance then comes from the **content match** rather than the path strength.
Being one hop from a memory is context, not evidence that its content matches the
query, and letting proximity set relevance reports a plausible number for an
unrelated neighbour — which is the number a caller then puts a threshold against.
That rule is `query.deriveScore`, stated since Phase 5 and exercised over a real
graph step since Phase 6.

The graph step does **not** widen, unlike the vector step. A traversal already
spends its budget on the strongest connections available, so asking for more
would reach *weaker* ones — the opposite of what widening buys a
nearest-neighbour index, where more candidates are simply more of the same
quality.

## Corrections to the plan, all decided with the user

1. **The derived in-edge row duplicates the edge body** rather than being an
   empty pointer row, so `In()` and every strength filter cost one iterator
   instead of a read per neighbour.
2. **Traversal is best-first by path strength**, not the plan's breadth-first.
3. **`Traverse` returns a `Traversal` carrying `Truncated`**, not the plan's
   `[]Reached`. The plan's own test requires a walk that hit its node bound to
   report that it did, and a slice has nowhere to put it. Same class of
   correction as Phase 4's `Context.Checkpoint`.
4. **Neighbour and traversal reads take a `Reader`**, so the query executor
   traverses through the same pinned snapshot it walks the attribute index with.
   `graph.Service` is the tenant-from-context, KV-backed form over the same
   functions.
5. **Phase 6 ships a REST surface and no MCP tools.** The plan's Files table
   lists no delivery surface, which would leave the phase unreachable until Phase
   11's discovery job and unverifiable against the real binary. The MCP tools are
   deferred to a phase with a reason to spend a model's context on them. Phase 13
   was that phase: `find_related` takes the traversal inputs `related` does, and
   `get_memory` gained `include_connections`.

Two smaller decisions taken without escalation: a hard delete removes the
memory's edges (above), and `graph.Update` on an edge that does not exist is
`errs.NotFound` rather than an implicit create — a caller who meant to
strengthen an existing relationship and instead invented one has stated
something about their corpus they did not mean.

## What Phase 6 does not do

No relationship discovery — that is Phase 11, and it is the workload these bounds
were chosen for. **Since Phase 11** it exists, in `internal/discovery`, and it
added one method here: `Service.Linked` answers whether two records are connected
in either direction and holds that answer to the caller's commit. It is a graph
concern rather than a discovery one because only this package builds edge keys,
and it is conditional rather than a plain read because two writers that both read
"not linked" both write — which is what an end-to-end run found, a pair connected
in both directions one millisecond apart. No path enumeration: spec §14 lists "path discovery" among
operations to include *eventually*, and `Reached.Via` carries only the last edge
on the strongest path, not the path itself. No rebuild job registration and no
`remem-admin rebuild` verb; this phase ships the function they both call. No MCP
tools.

**Since Phase 9**, `RebuildIn` is registered as the `graph.rebuild_in` job type
and can be triggered with `POST /api/v1/admin/jobs/graph.rebuild_in/run`. The
CLI verb is still Phase 12's. Nothing schedules it automatically: the reverse
index is written inside the record's own transaction and has no health signal to
report damage through, so there is nothing for a self-repair trigger to observe
— which is the point of maintaining it transactionally rather than
asynchronously.
