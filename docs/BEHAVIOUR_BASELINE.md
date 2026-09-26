# Behaviour baseline

This is a frozen record of how Rust Remem behaves, written for someone
implementing Go Remem who cannot run the two side by side.

Its purpose is narrow: when Go does something different, this document should
let you decide whether that difference is a bug or an intended divergence. So
every entry says what a caller observes, cites the code that decides it, and —
where the source explains its own reasoning — preserves that reasoning rather
than paraphrasing it away.

It is a record, not a review. Nothing here argues that a behaviour is good, and
nothing here proposes changing it. The Go rewrite deliberately diverges in
several of these places; the plan's Part II.10 is where those divergences are
argued. This document is only what they diverge *from*.

Line numbers are from commit `8b20856` on `rem-go-phase0`. Each citation quotes
enough surrounding code to survive a later shift.

---

## 1. Search scoring

### 1.1 `score` and `fused_score` are two different numbers, and the response is ordered by the one that is not relevance

`SearchResult` carries both (`services/types.rs:400-419`):

```rust
/// Relevance, 0-1, comparable across search types.
///
/// Note this is deliberately *not* monotonic with response order: results
/// are ordered by `fused_score`.
pub score: f32,

/// Rank-fusion value that determined ordering. A function of rank position,
/// not similarity — not comparable across requests.
pub fused_score: f32,
```

**What a caller sees.** A hybrid search can return a result with `score: 0.82`
above one with `score: 0.91`. That is not a bug. `fused_score` put them in that
order because the 0.82 result was found by two indexes and the 0.91 result by
one, and reciprocal-rank fusion rewards agreement between sources. A client that
re-sorts by `score` will get a different order than the server intended, and a
client that shows `fused_score` to a user is showing a number that means nothing
outside that one response.

### 1.2 `score` comes from the best non-graph source

`SearchResult::from_sources` (`services/types.rs:421-452`):

```rust
/// Relevance is the best content-matching source's score. Graph proximity
/// is deliberately excluded unless it is the only contribution: being one
/// hop from the anchor memory is context, not evidence that the content
/// matches the query, and letting it set relevance would report 0.5 for an
/// unrelated neighbour.
```

The implementation takes the maximum over sources where
`s.source != SourceName::Graph`, falls back to the maximum over all sources when
that set is empty, and clamps to `[0, 1]`.

**What a caller sees.** Ask for memories related to a given memory, and a
neighbour whose content has nothing to do with your query reports a low `score`
even though it is one hop away and ranks well. Graph depth 1 would otherwise
normalise to `1/(1+1) = 0.5`, and every direct neighbour in the corpus would
report exactly 0.5 relevance regardless of content.

### 1.3 Relevance is recovered cosine, not the fusion score

Two conversions exist, and they are used for different things
(`services/types.rs:528-562`):

```rust
/// Monotonic in distance, so it orders correctly under every metric — but it is
/// an ordering value, not a calibrated similarity.
pub fn distance_to_score(distance: f32) -> f32 {
    1.0 / (1.0 + distance)
}
```

```rust
/// This is preferred over [`distance_to_score`] as the reported relevance
/// because `1/(1+d)` floors at 1/3 for orthogonal vectors and never approaches
/// zero, which makes it a poor threshold (REM-74).
pub fn cosine_from_distance(metric: DistanceMetric, distance: f32) -> Option<f32>
```

Recovery is exact because embeddings are L2-normalised on the way in, so for
unit vectors `squared_l2 = 2 − 2·cos`:

| Metric | Recovery | Reported as cosine? |
|---|---|---|
| `L2` (squared, the configured default) | `1 − d/2`, clamped | yes |
| `Cosine` | `1 − d`, clamped | yes |
| `DotProduct` | — | no; the index does not guarantee normalisation, so it falls back to `1/(1+d)` |

`vector_relevance` prefers cosine and falls back to `distance_to_score` when the
metric cannot give one.

**What a caller sees.** Two unrelated memories score near 0.0, not 0.33. That
matters because it is what makes a relevance threshold usable at all: under
`1/(1+d)`, orthogonal vectors floor at one third, so "reject anything below 0.4"
rejects almost nothing.

### 1.4 One special case where `fused_score` is not a rank value

`services/search_engine.rs:118-124`:

```rust
// Invariant: without `related_to` this plan has a single step
// (vector only), which bypasses RRF entirely — `fused_score` is then
// exactly `1/(1+distance)`, not a rank-based value. Adding a second
// step to this path (e.g. a future tag or content step) would
// silently convert `fused_score` to RRF's rank-based scale.
```

**What a caller sees.** A plain semantic search returns `fused_score` values on
a completely different scale from a hybrid search's. Nothing in the response
says which one you got.

---

## 2. `truncated`

`services/search_engine.rs:23-31`:

```rust
/// `truncated` is the difference between "these are all the memories that
/// match" and "this is as far as the search looked". A filtered search used to
/// return the second while looking like the first (REM-78).
```

A search that loses candidates to a filter widens and retries. A step that
reaches `search.widen_max_factor` times its target stops and sets the flag. A
second path sets it too (`services/search_engine.rs:228-232`):

```rust
// A deferred tag filter can shrink the page below the limit with
// nothing left to widen against, which is exactly the shape of
// incompleteness `truncated` exists to report.
let truncated = truncated || (dropped_by_deferred_tags && results.len() < query.limit);
```

**What a caller sees.** `truncated: true` with three results and `limit: 10`
does not mean only three memories match. It means the search stopped looking.
Treating a short page as "that is everything" is wrong, and narrowing the filter
or raising the limit may surface more.

---

## 3. `access_count` counts recall sessions, not round trips

`services/recall.rs:32-37`:

```rust
/// A flag, not a counter: a second recall inside the same flush window is a
/// no-op on the count, which is what makes `access_count` measure recall
/// sessions rather than round trips. A client that fetches a memory and then
/// updates it registers one recall, not two.
```

**What a caller sees.** Fetch a memory ten times in quick succession and
`access_count` rises by one, not ten. Wait for the flush window to pass between
fetches and it rises by one each time. The number is therefore a function of
timing as well as of use.

Search does not record a recall at all — it discovers memories rather than
addressing them — but it does report recalls other operations have recorded and
not yet written (`services/search_engine.rs:165-171`):

```rust
// Search discovers memories rather than addressing them, so it
// records no recall of its own -- but it must still report the
// recall other operations have recorded and not yet written, or
// the same memory shows a different use count depending on which
// endpoint asked. `peek`, not `take`: a read must never consume a
// recall no write has applied.
```

---

## 4. Recall is deliberately not durable

`services/recall.rs:1-12`:

```rust
//! A recall is telemetry: `access_count`, `accessed_at`, `last_recalled_at`
//! and the health reinforcement that follows a retrieval. It feeds promotion
//! and active forgetting, both heuristics. Writing it durably on every
//! retrieval made a read append to the WAL and fsync inside the engine's
//! global write lock -- full record-rewrite cost, and read/write contention,
//! to protect data whose loss costs a promotion one recall later.
//!
//! So recalls accumulate here and are folded into the record the next time it
//! is written, whether by the flush task or by any ordinary update.
```

**What a caller sees.** A process that dies before the flush loses the recalls
in its window. The visible cost is that a memory is promoted to long-term one
recall later than it would have been, and that its health decays from a slightly
lower floor.

This is one of the places the Go design reverses course, so the rationale above
is worth keeping intact: it was a considered trade against the cost of a WAL
append and fsync under a global write lock, not an oversight.

---

## 5. Listings

### 5.1 `SortBy` has exactly one variant, and the reason is cursor stability

`services/types.rs:92-104`:

```rust
/// One variant, deliberately. An ordering key has to be immutable, or a
/// record can move between one page and the next and be returned twice or
/// skipped entirely; `created_at` is written once and never reassigned.
/// Ordering by recency of retrieval was removed for exactly that reason --
/// reading a memory changed where it sat in the list (REM-79).
pub enum SortBy {
    #[default]
    CreatedAt,
}
```

**What a caller sees.** There is no way to list by importance, by health, or by
recency of access. This is the second behaviour the Go rewrite deliberately
diverges from, and the stated reason is the one to hold it to: any replacement
ordering key must be stable under paging, or the guarantee this variant exists
to protect is gone.

### 5.2 `order=desc` with a non-zero `offset` is rejected outright

`api/routes/memories.rs:228-255`:

```rust
/// `read-consistency` requires that paging never hands back a memory the
/// caller already received. An offset cannot honour that descending: records
/// are appended in creation order, so a descending offset boundary shifts by
/// one for every memory written after the page was read, and the caller sees
/// a duplicate. A cursor names a position rather than a count, and the
/// ordering attribute never changes, so it holds in both directions -- which
/// is why descending paging is offered through a cursor and refused through
/// an offset.
///
/// Passing both is refused rather than resolved by precedence: silently
/// preferring one would page from somewhere the caller did not ask for.
```

Two distinct validation errors result:

- cursor together with a non-zero offset — `"cursor and offset are two ways to
  say where a page starts: pass one, not both"`
- `order=desc` with a non-zero offset — `"order=desc does not support a non-zero
  offset: page descending results with cursor instead"`

**What a caller sees.** A 400 with a message naming the supported route, not a
silently mis-paged result. `order=desc` with `offset=0` is fine; it is only the
combination with a non-zero offset that fails.

`SortOrder::Ascending` is the default, and that too is deliberate
(`services/types.rs:106-116`): ascending page boundaries keep their meaning as
the corpus grows, descending ones shift by one for every record written after
the first page was read. The type is kept separate from `SortBy` because
"leaving the direction implicit is what let `list_recent_memories` return the
*oldest* memories for as long as it did."

---

## 6. Archiving retires a memory from retrieval; the record survives

`services/memory_manager.rs:313-337`. The memory is stored with `archived` set,
its timestamp and tag index entries are kept on purpose, it is tagged
`__archived__`, and only then is it retired from the vector index:

```rust
// Keep timestamp/tag index entries so cleanup_archived can find the
// archived record later; user-facing reads filter `archived=true`.
```

The retirement comes last, and the ordering is load-bearing:

```rust
// Retire *after* the record is archived, never before: a
// retirement that lands while the write then fails leaves a live
// memory that search can no longer find, with nothing to signal
// it. This order fails the other way -- the memory is archived,
// the `archived` predicate still excludes it, and it costs one
// candidate slot until cleanup removes it.
```

A retirement failure is logged, not raised.

**What a caller sees.** An archived memory disappears from search results and
costs nothing on later searches. Its record, its timestamp-index entry and its
tags all remain until `cleanup_archived` hard-deletes it, so it still occupies
storage and still appears in a raw export. Between archiving and cleanup, a
failed retirement means the memory consumes one search candidate slot per query
without ever being returned.

---

## 7. MCP drops per-source evidence unless `explain` is set

`api/mcp/tools.rs:340-347`:

```rust
/// Every field here is context the model pays for on each search, so the
/// default keeps `score` (which it can act on) and a compact `matched` list of
/// contributing indexes, and drops the per-source evidence. `explain: true`
/// returns the full breakdown. Kept identical to the copy in
/// `crates/remem-mcp/src/tools.rs`.
```

The REST surface does the same by a different route: `sources` is
`skip_serializing_if = "Vec::is_empty"`, and `SearchResult::without_sources`
clears it.

**What a caller sees.** The same search returns a different JSON shape over MCP
than over REST, and a different shape over MCP depending on `explain`. Code that
assumes `sources` is present will break on the default path.

Note the last line: the shaping function is duplicated between the in-process
MCP server and the stdio binary, and is required to stay identical.

---

## 8. Constants

Every value below was read from the source at `8b20856`, not copied from the
plan. All ten named in the task brief agree with the code.

| Constant | Value | Where |
|---|---|---|
| Daily importance decay factor | `0.995` (~0.5%/day) | `services/lifecycle_manager.rs:251` |
| `RECALL_HEALTH_BOOST` | `10.0`, clamped into `[0, 100]` | `services/recall.rs:24`, applied at `:67` |
| `CLEANUP_AGE_DAYS` | `30` | `services/attrs.rs:38` |
| `FLASHBULB_AROUSAL_THRESHOLD` | `0.8` (`arousal >= 0.8`) | `services/memory_manager.rs:114` |
| `FLASHBULB_PROTECTION_MS` | `30 * 86_400_000` (30 days) | `services/memory_manager.rs:115` |
| `auto_discovery_threshold` | `0.7` | `config.rs:274` |
| `auto_discovery_top_k` | `5` | `config.rs:275` |
| `rrf_k` | `60` | `engine/query/mod.rs:53` |
| `widen_max_factor` | `32` | `config.rs:290` |
| `list_max_factor` | `128` | `config.rs:300` |
| `DEFAULT_RUN_BUDGET` | `10_000` | `services/lifecycle_manager.rs:38` |
| `SWEEP_EFFORT_FACTOR` | `8` | `services/lifecycle_manager.rs:45` |

`list_max_factor` is not in the brief's list but belongs here, because its
divergence from `widen_max_factor` is deliberate and the reason is recorded
(`config.rs:291-299`):

```rust
// Deliberately larger than the search bound above, because a
// unit of listing effort is much cheaper than a unit of search
// effort: a listing candidate costs one sidecar-row read on an
// index already being walked, where a widened search re-runs a
// similarity traversal. Sharing a number because the two bounds
// share a shape would truncate ordinary listings -- a filter
// keeping one candidate in thirty is common (a memory type and
// an importance band together), and at 32 a page of five could
// not be filled from several hundred records.
```

`SWEEP_EFFORT_FACTOR` multiplies the run budget rather than replacing it
(`services/lifecycle_manager.rs:96`): a sweep may examine up to
`run_budget * SWEEP_EFFORT_FACTOR` — 80,000 records by default — to find at most
`run_budget` to act on.

**What a caller sees, for the flashbulb rule.** Store a memory with
`arousal: 0.8` and it is promoted to long-term immediately and is immune to
decay for thirty days. Store it with `arousal: 0.79` and neither happens. The
threshold is `>=`, and there is no gradation around it.

The promotion *overrides* the requested type rather than merging with it
(`services/memory_manager.rs:118-125`): `memory_type` becomes `LongTerm`
whatever the caller asked for, so a request saying `short_term` with
`arousal: 0.9` produces a long-term memory and the response reports the type it
got, not the type it asked for.
