# Text and hybrid search

What is authoritative, what is derived, what is rebuildable, where the
atomicity boundaries are, and what a caller is told when something is wrong.
Phase 8. Package `internal/text`, with the query layer's step in
`internal/query`.

## What is canonical, and what is derived

**Content and tags are canonical.** They live in the record body, and nothing
else in the system can reconstruct them.

**Every byte in the `text` key space is derived** — postings, per-record term
rows, per-tenant corpus statistics. All of it comes from record bodies and is
rebuilt rather than repaired (Invariant 3). Losing the whole space costs
searchability and loses no memory.

## The one structural difference from the vector index

The text index is maintained **inside the record's own transaction** (spec §12),
through `record.Indexer`. A memory and its postings land together or not at all.

That removes by construction the drift class Phase 7 had to design the vector
index against. There is no window in which a stored memory is unfindable by a
word it contains, no phantom posting for a memory that failed to commit, and no
repair sweep. It is the same move Phase 6 made with in-edges and Phase 7 made
with node records: prefer a structure that cannot fall out of step over one that
is checked for having fallen out of step.

The one way this index can be incomplete is an **interrupted rebuild**, and that
is exactly what the rebuild marker reports.

## Durable formats

Everything below is written once and read forever. None of it is renumbered.

### The term

A term is the tokeniser's output as UTF-8. Terms are bounded at
`MaxTermBytes = 128`: a longer token is stored as its first 96 bytes truncated
to a rune boundary, a `0x00` separator, and 16 hex characters of the SHA-256 of
the whole token. Index and query fold identically, so the lookup stays **exact**
rather than becoming a prefix match.

Truncating alone would fold two long tokens sharing a prefix into one term —
a prefix match wearing an exact match's clothes. Dropping the token instead
would make a memory unfindable by the one word it actually contains, which is
the Rust behaviour §II.10 row 2 exists to remove.

### Reserved rows

The tokeniser emits only letters, digits, marks and symbols, so no term it
produces begins with a byte below `0x20`. That is what lets the space hold rows
that are not postings without a second key space or a second parser.

| Prefix | Row | Key |
|---|---|---|
| — | a content posting | `text ‖ len ‖ term ‖ record id` |
| `0x01` | a tag posting | `text ‖ len ‖ 0x01 + tag ‖ record id` |
| `0x00 'd'` | a record's term list | `text ‖ len ‖ 0x00 'd' ‖ record id` |
| `0x00 's'` | the tenant's corpus statistics | `text ‖ len ‖ 0x00 's' ‖ id.Zero` |

`TestNoContentTermCanImpersonateAReservedRow` and its property-test counterpart
are what keep that true.

### Values

| Row | Value |
|---|---|
| Posting | `uvarint(tf) ‖ uvarint(docLen)` |
| Term row | `uvarint(docLen) ‖ uvarint(n) ‖ n × (uvarint(len) ‖ term)`, terms sorted and de-duplicated |
| Statistics | `uvarint(documents) ‖ uvarint(totalLength) ‖ byte(rebuilding)` |

Term frequency and document length are in the posting because that is what makes
scoring a scan of the matches instead of a read per candidate.

The term row is sorted so the bytes are a function of the term *set* rather than
of tokeniser order. That is what makes a rebuilt row comparable with an
incrementally written one byte for byte, and it is Invariants 8 and 9 for the
apply path Phase 14 replicates.

## Tokenisation, and why it is not configurable

`text.min_token_length`, `text.max_token_length` and `text.remove_stop_words`
were removed from configuration in this phase. They were not performance dials:
each decided which terms were written into the postings key space, so changing
one on a live corpus produces memories written before the change that are
findable by a term and memories written after that are not — silently, with
nothing in the results to show it.

Tokenisation therefore sits beside RRF's `k = 60` and `widen_max_factor = 32`
as a constant, on the same rule: **it changes what a query returns rather than
how fast it returns**. A config file or environment variable still naming one of
the three is refused at start-up with the reason. `text.enabled` stays, because
turning the index off is a deployment decision rather than a change to what a
query means.

**The rules are a versioned format, since Phase 13.** A constant still changes
when the library under it does. Phase 13 moved `golang.org/x/text` from v0.25.0
to v0.39.0 for GO-2026-5970, an infinite loop on invalid UTF-8 reachable from
`TagTerm` and `tokenize`. The trigger cannot reach them in practice: JSON decoding
turns invalid UTF-8 into U+FFFD, and protobuf refuses it. On Go 1.27 the new
version also normalises with Unicode 17 tables where the old one used Unicode 15.
Normalisation is stable for characters both versions know, so only text holding
characters that Unicode 16 or 17 assigned can come out differently. That text
was indexed under the old terms and would be searched for under the new ones.

So `text_index` is a durable format like the key encoding: absent means the rules
before Phase 13, and 1 means v0.39.0's. Migration `0002-text-index-unicode-17`
queues one `text.rebuild` job per tenant. It is instant, because it only queues,
and resumable, because each job commits with the cursor. A keyword search over a
tenant still being rebuilt reports `truncated: true` in the meantime, as it
does for any rebuild. A binary built with Go 1.25 or 1.26 uses v0.39.0's Unicode
15 tables and still writes version 1, so a directory can move between Go builds
of one Remem version and change terms only for those same characters. Recording
the toolchain's Unicode version in the format would close that, and has not been
judged worth it.

The rules, in `internal/text/tokenize.go`:

- **NFC normalisation, then case folding.** A decomposed accent and a
  precomposed one are one term.
- **Latin-script runs** break at anything that is not a letter, digit or
  combining mark. `INV-2024-8871` becomes `inv`, `2024`, `8871`, and a query for
  the whole string produces the same three.
- **CJK runs** are indexed as unigrams *and* overlapping bigrams. Unigrams alone
  make a two-character query match nearly every Japanese memory; bigrams alone
  make a one-character query match nothing. Both costs roughly twice the postings
  on CJK text and buys complete recall with a bigram's precision. A dictionary
  segmenter would beat it and would put a vocabulary in the write path of every
  memory — a decision for evidence rather than for a rewrite.
- **Emoji clusters are one term**: a base symbol with its skin-tone modifier,
  variation selector, or zero-width-joined parts. 👍🏽 is one term, not a thumb
  and a modifier nobody searches for.
- **Combining marks continue a word.** NFC composes only what has a precomposed
  form; Devanagari, Arabic and Thai keep theirs separate, and a mark treated as a
  separator would cut those words up at every vowel sign.
- **A small stop list**, deliberately small: a long one answers "The Who", "to be
  or not to be" and "IT department" with nothing. A query made entirely of stop
  words keeps them, because an optimisation that turns a query into no query has
  stopped optimising.
- **No stemming.** It changes recall in ways that must be measured before they
  are adopted, and it would be a second durable decision baked into every posting.

**The rules relax when they would leave nothing.** A memory whose whole content
is "the and of", or "a", would otherwise have no postings at all and could not be
found by the only words it contains — a memory that exists and cannot be found.
The query side already had that fallback; the index side did not, and Phase 8's
end-to-end run stored exactly that text and searched for exactly that text and
got nothing back. Both sides now loosen at the same point, in the same function,
which is the part that matters: whatever makes a document unindexable makes the
query that would have found it unanswerable.

Tags go through case folding and the length bound and are otherwise stored
whole. A tag is a label somebody chose, and splitting "release notes" into two
terms would make it match a memory tagged "notes".

## Scoring

BM25, with `k1 = 1.2` and `b = 0.75`, per tenant.

- **Document frequency** is counted by the same scan that scores, so it is free.
- **Inverse document frequency** is `ln(1 + (N − df + 0.5)/(df + 0.5))`. The `+1`
  inside the logarithm is the standard guard against a negative weight: a term in
  more than half the corpus would otherwise score *against* the documents
  containing it.
- **Two floors** guard arithmetic that would otherwise fail silently rather than
  loudly. `avgdl` floors at 1, because a zero divides the length normalisation
  into an infinity that sorts arbitrarily. The document count floors at the
  term's own frequency, because a count below it makes the IDF numerator negative
  and turns the ranking inside out. Neither should ever fire — the totals are
  exact — but a corpus being repaired from a damaged statistics row can reach
  them, and graceful is better than inverted.

Ties break on the record id, ascending. Without a deterministic tiebreak the
order falls back to map iteration and two identical queries return different
truncated result *sets*, not merely different orders.

## Corpus statistics: exact, and what that costs

BM25 needs two per-tenant running totals — the document count and the summed
document length, whose ratio is `avgdl`. They live in one row per tenant, staged
into the record's own transaction and **conditionally committed**, so no
increment is ever lost.

**The alternative was rejected on what it degrades.** An unguarded
read-modify-write cannot lose a memory, only an increment — but a document count
that drifts low is an inverse document frequency wrong for every query from then
on, with nothing anywhere reporting it and no repair an operator has a reason to
run. That is a slow ranking decay nobody can diagnose, and ranking is what a
customer judges the product by.

**The contention that buys is removed, not absorbed.** A conditional row every
write in a tenant touches produces conflicts that have nothing to do with the
caller. Measured on the in-memory store — the worst case, because a commit is
microseconds so the window a conflict opens in is most of the operation:

| Concurrent writers into one tenant | Attempts per write | Writes refused |
|---|---|---|
| 1 | 1.00 | 0 of 16 |
| 4 | 1.16 | 0 of 64 |
| 16 | 1.59 | 0 of 256 |
| 32 | 1.82 | 1 of 512 |
| 64 | 1.97 | 8 of 1024 |

Raising the retry budget only moves those numbers. `txn.Gate` — a striped,
deterministic per-tenant lock taken before the transaction — removes them:
every count exact, one attempt per write at every concurrency above, and the
whole sixty-four-writer run *faster* (14 ms against 27 ms) because the retries
were pure waste. Writes into different tenants never wait for each other.

`txn.Do` keeps a jittered, eight-attempt retry for the conflicts the gate cannot
see: a genuine concurrent update of the same memory, or a writer outside the
service. An unjittered loop keeps contending writers in lockstep, which is why
the backoff is randomised.

**The two halves are tested apart, and the reason is worth keeping.** The test
that asserts an exact count takes the gate, exactly as the write path does. A
second test drives the ungated path, *tolerates a refusal*, and asserts the
count equals what actually committed — the weaker claim and the important one,
because the gate is process-local and the day a second writer exists that is the
guarantee that remains. The two were originally one test, which drove the
ungated path and asserted no write is ever refused: a property this table says
the retry does not have. It passed for a phase because the ungated conflict
window is narrow on an idle machine, and failed one run in four under coverage
instrumentation, which widens it. Assert of a mechanism only what that mechanism
provides.

**Phase 14 inherits no debt from the gate.** Raft gives every write a total order
through the leader's log, so the ordering this provides on one node is the
ordering consensus provides on many. It is the local stand-in, not a shortcut.

Because a retried body runs more than once, every write path computes what a
retry must not redraw — record ids, timestamps, embeddings — **before** the
transaction opens. A second attempt minting new ids would store different
memories than the first attempted to, which Invariant 9 forbids outright.

## Cost

One prefix scan per term, plus one read of the corpus statistics. Nothing reads
a record body and nothing reads an attribute row.

Measured: **a five-match query touches six stored entries** — five postings and
the statistics row — and touches six whether the corpus holds five thousand
memories, twenty thousand, or the plan's hundred thousand. The last figure was
taken once by hand; the suite asserts the *proportionality* rather than a
threshold, because a threshold is a number somebody eventually raises and "it did
not grow with the corpus" is the claim itself.

The honest qualification: a term half the corpus contains has half the corpus as
its postings, and scanning them is proportional to *its* matches rather than to
the query's. Stop words remove the worst of it and nothing removes all of it — a
term that unselective is also one that tells the ranking almost nothing, which is
what its inverse document frequency then says about it.

Rust scans every record on every keyword query (§II.10 row 1, the defect REM-29
never fixed).

## Tags

Tags are ordinary terms in a reserved namespace (§II.10 row 2). One index, one
key space, one scan per term. Rust's `tag_index_can_answer` and its hardwired
100-byte limit are both gone, and with them the case where an unanswerable filter
silently fell back to scanning the payload.

**A tag is a filter, so it narrows every step — and adds none.**

Getting that second half wrong is the defect Phase 8's end-to-end run found, and
it is worth stating what it looked like. A tag can only be answered from the
inverted index, so the obvious reading of "a tag filter narrows exactly like any
other term" was to add a keyword step whenever tags were present. But **a step is
a source**: adding one made the tag contribute its own ranked list into the
fusion, so every tagged memory became a candidate whether or not anything else in
the query had reached it. A semantic search for "distributed consensus and raft
leader election" with a tag filter answered with "marzipan recipes from a
Bavarian bakery" — first, at relevance 0.077, above a memory it scored 0.114.

A second, quieter half of the same mistake: a required term that *scores*
promotes memories carrying the tag and nothing else above memories that matched
the query. So:

- A required term **gates**. A candidate lacking it is not a hit.
- When there are optional terms it contributes **no score**, and a candidate
  matching none of them is not a hit either.
- When there are none it scores, because "everything tagged finance" is an
  ordinary request and record-id order is not an answer to it.

Tags therefore narrow through `settle`, one read per surviving candidate against
a key holding no value at all — paid only when a filter is present. A keyword
search additionally pushes them down as required terms, so *that* step does not
spend its budget on candidates the filter will drop. That is an optimisation of
one step, not the mechanism.

A required term nobody carries returns nothing, and means it.

## Search modes

`semantic | keyword | hybrid`, **required** on every surface. A search that omits
it is refused by name, listing the three.

Rust defaults to `semantic` over REST and `hybrid` over MCP — the same request
answered differently depending on which door it came through. There is no default
here because the three answer materially different questions and the right one is
the caller's to know: `keyword` runs no model and cannot miss a spelling,
`semantic` cannot find an invoice number, `hybrid` costs both. A default would
make one of them the silent answer to "I did not think about it", and the plan's
own criterion — the same memory scoring within 0.05 under `semantic` and `hybrid`
— only means anything if a caller knows which they asked for.

This is a breaking change to `POST /api/v1/memories/search` and to the
`search_memories` MCP tool, both of which the plan classified as "preserve
semantically". Requiring a field that was optional is a redesign of both.

## Where relevance comes from

`Hit.Score` is the best source's score, and the sources are **ranked**: vector,
then text, then graph, then attr.

They are not three measurements of one quantity. A cosine is calibrated —
unrelated memories near zero, near-duplicates near one, on the same scale in
every request, which is what makes a threshold usable (baseline §1.3, REM-74). A
BM25 sum is not: it is unbounded, it rises with how rare the query's words happen
to be in *this* tenant's corpus, and the same memory matching the same words
scores differently after the corpus grows. Graph proximity is last for the reason
the baseline gives: being one hop from an anchor is context, not evidence that
the content matches.

That ordering makes the plan's completion criterion true by construction rather
than by luck: a memory found by both indexes scores **identically** under
`semantic` and `hybrid`, not merely within 0.05. Verified by inverting the
preference, at which point one memory scores 0.900 under semantic and 0.155 under
hybrid.

`query.TextRelevance` squashes a BM25 sum into `[0, 1)` for reporting only. It is
monotonic, so it cannot change an order, and it is not a similarity: 0.5 means
the sum equalled the constant, not that the match is half as good as a perfect
one. The raw sum is what the ordering uses and what `explain` reports as the
step's own value.

## Failure semantics

| Situation | What happens |
|---|---|
| A record's term row will not decode | Not an error. Refusing the write would make one damaged sidecar row leave the record permanently unwritable; the stale postings survive until a rebuild. Same judgement `attr.Indexer` makes about a damaged attribute row |
| The statistics row will not decode | The totals restart from this write rather than the write failing. Scores are wrong until a rebuild; memories are not lost |
| A rebuild is interrupted | The marker stays set. `Health` reports the tenant degraded and every keyword search reports `truncated: true`, with one warning logged naming the tenant and the reason |
| A posting inside one term's range belongs to another term | `errs.Corruption`. The range is built from the length-prefixed term, so it cannot happen without a key written by something else; scoring it would attribute one term's matches to another |
| `text.enabled` is off | `keyword` and `hybrid` are refused by name, and so is a tag filter. Never answered as a semantic search — a caller who asked for an exact-term search and got a meaning-based one has no way to tell |
| A search with no terms | Refused. An empty query returning the corpus is how a client bug becomes a full scan |

`text.Index.Health` exists from the start rather than being retrofitted. Phase 7
predicted `vector.Index` would need no new methods and then had to add exactly
this one, because a damaged index that serves a short page silently is
indistinguishable from a small corpus. `TestKeywordSearchOverARebuildingIndexSaysSo`
pins the one caller.

## Rebuild

`remem-admin text rebuild --data-dir <path> [--tenant <id>]`.

**Restartable, not resumable.** The order is mark, clear, repopulate, unmark. An
interruption leaves the marker set, so `Health` reports degraded and every search
says its answer may be incomplete until somebody runs it again. Resuming from a
cursor belongs to `jobs.Checkpointer`, and inventing a second progress format
here would be a durable decision taken to save one operator one re-run — the
same disposition Phase 7 settled on.

Phase 9 landed that `Checkpointer` and the `text.rebuild` job still does not use
it: the handler runs this method and takes the whole rebuild as its unit of
work, so an interruption still costs a whole re-run. Wiring the cursor through is
a change to the handler rather than to this function, and is worth doing when a
corpus makes the re-run expensive.

**Byte-identical** to an incrementally maintained index, which is a stronger
claim than "holds the same terms". Two indexes with the same terms and different
bytes cannot be compared by an audit, cannot be compared between replicas in
Phase 14, and hide exactly the class of difference — a document length off by
one, a frequency counted twice — that moves ranking without changing which
memories are found.

**It cannot lose a memory.** Its input is the canonical record bodies and its only
outputs are rows in the text space.

The statistics row is the one write here that is unconditional, because a rebuild
holds the data directory under Pebble's exclusive lock: there is no concurrent
writer whose increment it could lose, so a conditional write could only fail. The
marker is cleared in the same write that installs the totals, so no search sees a
complete index reporting an empty corpus.

The command opens the directory read-only first and read-write second, so a
mistyped `--data-dir` is refused rather than becoming a new empty database that
reports a clean corpus — the Phase 6 finding, applied to the second command that
writes.

**The search that discovered the damage never does the work** — that would
charge one user for everyone's repair — but since Phase 9 the server does repair
itself: the degraded read enqueues a durable `text.rebuild` job, single-flighted
per tenant so a search loop cannot queue a second, and a worker runs it. This
command is the offline form, needed because a stopped server still needs a
repair path and a job queue is not reachable with the process down. With the
server running, `POST /api/v1/admin/jobs/text.rebuild/run` is the same repair
without the data directory.

The trigger only fires on a tenant that is *already* degraded — the marker means
an earlier rebuild was interrupted — so the rebuild does not cause the
incompleteness, it ends it.

## What the end-to-end run found

Three defects that no unit test caught, all found by driving the real binary
against a real corpus with the real model. Two of them are the reason this
document's "Tags" and "Tokenisation" sections read as they do.

**A tag filter added candidates instead of only narrowing.** A semantic search
for "distributed consensus and raft leader election" restricted to a tag
answered with "marzipan recipes from a Bavarian bakery" — first, at relevance
0.077, above a memory it had scored 0.114. The cause was giving the tag a
keyword step of its own, which made it a source; the second half was a required
term that scored, which promoted memories carrying the tag and nothing else.

The fixture missed it twice over, and both misses are worth naming. Every memory
in it was returned by every search, because the limit was above the corpus size —
so any "the filter did not change the answer" property held trivially. And the
first regression test written for it *passed against the broken code*, because
the two memories tie under rank fusion and the tiebreak happened to fall the
right way. The guards that ship assert the causes instead: a query that is not a
keyword query plans no text step, and a required term gates without scoring.
Each fails when its half of the fix is reverted.

**A memory made only of stop words could not be found by its own words.** Stored
"the and of", searched "the and of", got nothing: the query side had a fallback
that kept stop words when removal emptied the query, and the index side had none,
so the memory had no postings at all. Both sides now relax together.

**A query with no searchable terms blamed the wrong thing.** Searching for "a",
or for pure punctuation, was refused with a message about an empty query. The
refusal is right — answering "no matches" for a query the index cannot represent
is the conflation §II.10 row 2 exists to remove — but it now says the query
produced no searchable terms, which is what actually happened.

What the run confirmed: `search_type` refused by name on both surfaces; a
keyword search finding `INV-2024-8871` and running no model to do it; CJK by
bigram and by single character, emoji, and accented Latin, all matching; a
360-byte tag filtering exactly and not colliding with one sharing its first 96
bytes; a tag filter narrowing identically under all three modes; **the same
memory scoring identically under `semantic` and `hybrid` across 200 comparisons
over 20 queries, to nine decimal places**; MCP reporting `matched` as the indexes
that actually found each hit; cross-tenant isolation on every new surface
including the tag filter; a hard delete taking its postings with it; an archived
memory leaving keyword search and staying fetchable; an interrupted rebuild
serving zero results while reporting `truncated: true` and logging one warning
naming the tenant and the command to run; and that command clearing it.

One thing the run demonstrated rather than tested: changing the tokeniser makes
every posting on disk describe the old rules, and `remem-admin text rebuild` is
what brings a corpus forward. The fix for the stop-word defect changed the
tokeniser mid-verification, and the rebuild was how the corpus was made
consistent with the new binary.

## Plan corrections

Recorded here because the plan says otherwise, and each was decided against the
plan's own tests.

1. **The index maintains itself through `record.Indexer`**, not through the
   plan's `Add(ctx, tx, t, id, text, tags)` called by the write path.
   `internal/record` already argues it: a write path that calls the indexer
   itself "is a call site that can be forgotten, and a forgotten one leaves a
   record that exists and cannot be listed". A record already carries its content
   and tags, so the plan's extra parameters were two copies of what is in `rec`.
   `record.WithIndexer` therefore accumulates instead of replacing.

2. **Reads take a `Reader`.** `Search(ctx, t, terms, k, filter)` cannot
   participate in a fused query: the vector step, the graph step and the
   attribute filter all read through one pinned snapshot, and a text step reading
   through the store would rank a page against a corpus the rest of the query
   cannot see. The correction Phase 6 made for `graph.Traverse`, for the reason.

3. **Required and optional terms**, not one list. "A tag filter narrows exactly
   like any other term" is the goal, not the mechanism — a term contributes rank
   and a filter removes candidates, and no index can infer which from the term
   alone.

4. **`text.Index` is a concrete type, not an interface.** `vector.Index` is an
   interface because `flat` and `hnsw` both exist and `flat` is the definition of
   correct that `hnsw` is verified against. One implementor here would mean a
   contract nobody tests and a shared suite duplicating this package's unit
   tests. Extracting one when a second implementation arrives is mechanical.

5. **Rebuild is a command, not a job.** The job framework was Phase 9; this is
   the disposition Phase 7 gave the vector rebuild, for the same reason. Phase 9
   has since registered `text.rebuild` as a job type as well, and the command
   stays for the case a job cannot serve: a stopped server.

## What is not here

No stemming, no synonyms, no phrase queries, no field weighting (BM25F), no
highlighting, and no second text index. Each is a recall or ranking change that
should be measured before it is adopted, and several would be durable decisions
baked into every posting on disk.
