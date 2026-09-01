# Changelog

## Unreleased

### Browsing and background maintenance cost what they do, not what you have

Listing memories, listing connections, and every background maintenance sweep
used to cost work proportional to everything in the store. Asking for ten
memories examined all of them; each of the five maintenance sweeps read every
memory in full, on its own schedule, to find the handful that needed anything.
The longer a store had been in use, the slower it got — which is backwards.

All three now cost work proportional to what was actually asked for.

#### Added

- **Continuation tokens on memory listing.** `GET /api/v1/memories` accepts
  `cursor` and returns `next_cursor`. A cursor names a position in the
  ordering rather than a count to skip, so page ten costs the same as page
  one, and memories written mid-sequence cannot shift the boundary and hand
  you something twice.

- **`order=desc` is now fully pageable.** Descending listings were previously
  limited to a single page, because a descending *offset* boundary moves every
  time a memory is created. A cursor does not move, so newest-first browsing
  now pages like any other. `order=desc` with a non-zero `offset` is still
  rejected — the cursor is the supported route, and the error says so.

- **Completeness signals on listings.** Every listing response carries
  `has_more` and `truncated`, with the same names and meanings the search
  response already used:

  | `has_more` | `truncated` | Meaning |
  |---|---|---|
  | `false` | `false` | That was everything. |
  | `true` | `false` | More to come — pass `next_cursor`. |
  | `true` | `true` | The server stopped at its effort bound. More may exist; it cannot say. |

  The third state is the one that matters. Previously a short page could mean
  either "that was everything" or "we gave up looking", and callers had no way
  to tell them apart.

- **Continuation tokens on connection listing.** `GET /api/v1/connections`
  accepts `cursor` and returns `next_cursor`, `has_more` and `truncated`, with
  the same meanings.

- **`search.list_max_factor`** — how far a listing walks before reporting a
  page truncated, as a multiple of the page requested. Defaults to `128`,
  deliberately higher than the search bound: a listing candidate costs one
  sidecar-row read on an index already being walked, where a widened search
  re-runs a similarity traversal.

#### Changed

- **BREAKING — `GET /api/v1/connections` no longer reports `total`, and no
  longer accepts `offset`.** Counting the connections in a store means walking
  all of them, which is the cost the cursor exists to avoid. Use `has_more` to
  know whether another page follows, and `next_cursor` to fetch it. The memory
  listing dropped its own `total` in an earlier release for the same reason.

- **Archiving a memory removes it from the browsing order.** Archived memories
  were invisible to callers but still examined on every page request, so a
  store that had archived heavily paid for memories it had already retired.
  Archiving now retires the memory from the ordering index as well as the
  similarity index, so archiving is a genuine cleanup. Archived memories remain
  fully reachable by the cleanup sweep until it deletes them.

- **Maintenance sweeps select the memories that are due.** Each memory records
  when it next needs attention, and each sweep walks that schedule rather than
  the whole store. A sweep with nothing due now completes without reading or
  writing any memory. Sweeps also run under a per-run budget, so a large
  backlog — after an outage, or on the first run following the upgrade — is
  drained across several runs instead of one unbounded pass competing with
  live traffic. What a run does not reach stays due and is picked up next time.

- The behaviour of each sweep is unchanged: the same memories expire, get
  promoted, decay, are forgotten, and are cleaned up, on the same terms as
  before. Only the cost of finding them changed.

#### Upgrade notes

Opening a data directory written by a previous version performs a one-time
pass that records a maintenance schedule for every existing memory. It runs at
startup, behind the existing format-version gate, and is safe to re-run: an
upgrade interrupted part-way completes on the next start.

This pass is not optional. Until it has run, memories written by the previous
version carry no schedule, and maintenance cannot reach them.

#### About the numbers

The cost improvements above are stated structurally on purpose. There is no
read-path benchmark in this project — the benchmark harness covers the write
path only — so nothing here is a throughput or latency measurement.

What is measured is **work performed**, asserted in CI as read counts over
test fixtures:

- one page of ten memories: **12** attribute-row reads over a 500-memory
  fixture, where the previous implementation performed 500;
- the same page over a 200-memory and an 800-memory fixture: **identical**
  read counts;
- one page of connections: **zero** payload reads, where the previous
  implementation deserialized two full records per connection;
- a maintenance sweep with nothing due: **zero** payload reads.

These are deterministic counts, not timings, and they fail loudly if a walk
stops being bounded. They are not a claim about requests per second.

### Fixed

- **CI could not lint.** The benchmark target added alongside the write-path
  harness had no stub in the CI image's dependency-caching layer, and cargo
  parses the whole manifest before fetching — so every branch failed at that
  layer before any check ran. Unrelated to the work above, but it is why the
  lint fixes below appear now.

- Added the `is_empty` companions and `Default` implementation that clippy
  requires on `SegmentedBTreeIndex`, `SegmentedInvertedIndex`,
  `PartitionedHnswIndexes`, `BlockCache` and `Level`. Pre-existing; surfaced
  once the linter could run again.
