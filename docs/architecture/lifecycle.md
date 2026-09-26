# Lifecycle

What happens to a memory when nobody is looking at it — and the audit trail that
says why. This document records invariants and the decisions behind them, not
APIs; the code is the API.

Landed in Phase 10. Packages: `internal/lifecycle`, `internal/events`,
`internal/lifecycle/policykv`.

---

## What is canonical and what is derived

| | |
|---|---|
| **The event stream** (`keys.SpaceEvent`) | **Canonical.** Nothing rebuilds it: an event is the record of something that happened, and there is no index to recompute it from. It is exported with the corpus and never reconstructed |
| **`next_attention_at`** (attribute slot 9) | **Derived**, and the one derived value whose loss is *safe*: zero reads as "due now", so a missing or stale schedule costs an extra visit rather than a memory that silently drops out of maintenance forever |
| **The recall counters on the record** | **Denormalised.** `access_count`, `accessed_at`, `last_recalled_at` and the health a recall reinforces are folded from the stream. The events are the authority |
| **Policy overrides** (`/policies/<tenant>`) | **Canonical.** Operator configuration, like the tenant directory row it sits beside |

---

## The shape

Every memory carries one `next_attention_at` — the earliest instant at which any
transition could have work to do. One job per tenant walks that index once per
run, and for each due memory folds its outstanding recalls, asks its policy what
is owed, applies all of it in one transaction, writes the events, and lets the
repository recompute the schedule.

Rust has the same shared attention time (`services/attrs.rs:44-51`) and then
walks it **five times**, each sweep discarding the memories the other four own.
The defect is five walks of one index, not five indexes, and the fix is to ask
each due memory what it is owed rather than asking the corpus five separate
questions. `TestOneWalkPerTenantPerRun` counts the iterators.

### Atomicity boundaries

- **One transaction per memory.** The record, its derived rows, its events and
  its trimmed stream land together or not at all. A batch would be fewer commits
  and would make one conflicting memory discard the work done for the other 255
  — and the conflict is the ordinary case, because the memories a sweep touches
  are the memories users touch.
- **The sweep always loses a race.** Its write is conditional on the record it
  read, so a concurrent user write wins, nothing lands, `next_attention_at` is
  unmoved, and the next run takes the memory. A background pass must never make
  a user's write fail.
- **A recall is one appended key** in its own small transaction, committed
  durably. See "Recall" below for why the fsync is paid and what it is not.

---

## The policies

Retention is a named policy on the record (plan §II.6), not an enum branch. The
constants were read from `remem-development` at `pre-go-freeze`, and **two of
them do not match what §II.6's table claims to be copying**:

| Policy | TTL | Promote at | Importance decay | Health decay | Archive at health | Cleanup after |
|---|---|---|---|---|---|---|
| `short_term` | from the record; **none** by default | 3 recalls → `long_term` | 1.0 (none) | **8.0**/day | ≤ 0 | 30d |
| `long_term` | none | — | 0.995/day | **2.0**/day | ≤ 0 | 30d |
| `pinned` | none | — | 1.0 (none) | 0 (none) | — | never |

- **Health decay is 8.0 and 2.0**, not §II.6's 0 and 1.0
  (`lifecycle_manager.rs:420-423`). The plan's numbers would make a short-term
  memory without a TTL immortal and let a long-term one survive a hundred
  untouched days instead of fifty.
- **`short_term` has no default TTL in Go, and that is a divergence from Rust,
  not a copy of it.** §II.6 gives it one hour, and so does Rust: a short-term
  memory created without a TTL gets 3,600 seconds at create
  (`services/memory_manager.rs:149`). A probe of the frozen `pre-go-freeze` image
  returned `ttl: 3600`. Phase 10 read `types.rs:1139-1143`, which only *reads*
  the field, and recorded the opposite; Phase 13's differential work found the
  mistake. The decision stands, and was taken again with the user on 2026-09-13
  now that it is a divergence rather than a copy. `short_term` is the *default
  policy* and nothing in the product sets a TTL, so the default would archive
  every memory an hour after it was written unless it was recalled three times.
  What retires a short-term memory in Go is health. `docs/PARITY.md` records it.
- **A TTL is a short-term memory's and nothing else's** (Phase 13). Rust drops
  the TTL from any memory that ends up long-term (`memory_manager.rs:149-150`)
  and expires only short-term memories (`types.rs:1135-1145`). Go now does both:
  a create that ends up under another policy keeps no TTL, and a TTL a record
  carries under another policy is inert to expiry and to the schedule. Before
  Phase 13 a flashbulb memory, a promotion at birth, kept its requested TTL and
  was archived when it ran out, thirty days of protection notwithstanding. The
  differential harness's lifecycle surface found that on every seed. A policy's
  own default TTL, which only an operator can set, still applies.
- **Importance decay applies to long-term only** (`:265`).
- **Decay reasons in whole truncated days**, and a pass under one writes nothing
  (`:279-282`). That is not a rounding convenience: without it an hourly sweep
  rewrites the whole corpus twenty-four times a day.

An **unrecognised policy is inert** — no decay, no expiry, no archive. That is
the rolling-upgrade case, and applying somebody else's retention to a memory
whose rules this binary cannot read would archive records on rules their owner
never chose.

### Flashbulb is a window on the record, not a temporary policy

`arousal >= 0.8` at creation sets `policy = long_term` overriding what the caller
asked for, and `ProtectedUntil = created_at + 30d`, during which decay and
forgetting skip the record. The threshold is inclusive and there is no gradation
around it.

§II.6 expresses this as "the `pinned` policy for 30 days, then revert". Saying it
that way needs two more durable fields — what to revert to, and when — to carry
what one timestamp carries, and it makes a record's policy a value that changes
by itself, which nothing else in the system does. `pinned` stays a real policy a
user can set, and is now genuinely different from flashbulb rather than an
approximation of it.

### Per-tenant overrides

Stored at the untenanted system key `/policies/<tenant>`, beside the directory
row. **Not a field on that row**, because `internal/keys` imports
`internal/tenant`, so `internal/tenant` cannot import `internal/lifecycle` —
putting them there would mean either a second declaration of the `Policy` struct
or moving retention semantics into the package whose job is isolation identity.

An override states a **whole policy**, not a patch: "no TTL" and "TTL unstated"
are different facts and a partial durable record cannot hold them apart. The
patching happens at the API edge, where JSON null and JSON absent are
distinguishable. `PATCH /api/v1/tenants/{id}/policies` patches over the
*resolved* policy, so an operator changing one number does not restate the other
six.

---

## The audit stream

Seven kinds, and each is a durable name: `recalled`, `promoted`, `expired`,
`archived`, `restored`, `updated`, `hard_deleted`. A retired kind stays retired.

**`updated` is the one kind a caller causes directly.** Every other kind is
something the lifecycle did to a memory; this one is something a person or an
agent did. Its `Before`/`After` name **the fields that moved and never their
values**: storing the previous content would double the corpus for every edited
memory and put memory content in a second durable place, and the field names
answer "why is this different" without either cost. It is appended inside the
record's own transaction rather than after it — unlike a recall, whose failure
costs one promotion, this row is the only thing that will ever say why a
memory's content changed, and a row saying a memory changed beside a memory that
did not is worse than no row.

A kind is validated on `Append` and deliberately *not* on `Restore`, which is
what makes adding one forward-compatible: an older binary importing a snapshot
that carries `updated` preserves the row verbatim rather than dropping it
(Invariant 4 permits refusing newer-on-disk, never losing it silently).

**There is no `consolidated`.** Consolidation does not ship in Phase 10 —
REM-113 tracks an LLM-backed implementation — and a declared kind nothing writes
is the same defect as a declared attribute slot nothing populates.

**There is no `decayed`, and that is the decision worth stating.** Decay is a
continuous function of time, not a transition. `importance`, `health`,
`last_decay_at` and `last_health_check_at` say exactly what it has done and
when, deterministically, so a row per memory per pass would restate the record —
**365 million rows a year at a million memories**. What decay *causes* is an
archive, and that is an event.

Expiry writes one `expired` event rather than `expired` followed by `archived`.
Two rows for one transition double the stream for no information: they are two
*causes* of retirement, not two steps of it, and both carry `archived: true`.

**Content never enters the stream**, and the encoder enforces it with a blunt
length bound. An audit row is read by operators, shipped to log aggregators, and
kept long after the memory it describes was deleted.

### Retention, enforced where the walk already is

Events are trimmed per subject by the sweep, inside the transaction it was
already opening for that memory. The key is subject-major
(`<subject:16><ts:8><seq:4>`), so "every event older than T in this tenant" would
be a walk of the whole event space; this is a bounded scan of one prefix, and
every live memory is due at least daily, so every subject is reached.

A **hard delete** removes the subject's whole stream and *then* writes the
`hard_deleted` event, in that order, in one transaction. The single row that
survives is the answer to "why did my memory disappear".

---

## Recall

Rust refused durable recall for a measured reason (`services/recall.rs:1-12`): a
read appended to the WAL and fsynced **inside the engine's global write lock**,
at full record-rewrite cost. Both halves of that cost are removed rather than
accepted.

- **It is not a record rewrite, and it takes no global write lock.** A record
  write is a body, an attribute row, and two index entries per indexed slot,
  under a conditional commit a concurrent writer can lose. A recall event is
  **one key**, appended where nothing else can be writing, so it never reads
  first and never conflicts.
- **It does fsync**, and an earlier version of this document said otherwise. The
  claim was that an unsynced commit still reaches the write-ahead log and
  therefore survives the process dying. Phase 10's end-to-end run disproved it
  with a `kill -9`: the recall was gone. Pebble applies an unsynced batch to the
  memtable and buffers the log, and nothing guarantees the bytes have reached the
  operating system when the call returns.

The coalescing window bounds the cost to one commit per memory per window — a
second recall inside it writes nothing at all — and Pebble group-commits
concurrent syncs, so a busy server pays far fewer fsyncs than it serves recalls.
The sync flag follows `storage.sync_writes`, so a recall is exactly as durable as
a memory: an operator who has turned durability off has turned it off for
everything, which is a decision they can reason about, where a read path quietly
keeping its own weaker setting is not.

### What counts as a recall

Addressing a memory by id. Search does not — it discovers memories rather than
addressing them (behaviour baseline §3) — and neither does budgeted recall, for
the same reason: the ranking chose, not the caller.

### The coalescing window

`access_count` counts recall **sessions**. A recall event is written only if the
subject's newest recall is older than `lifecycle.recall_window` (30s, matching
Rust's flush interval). Rust's window is a process-local timer, so a restart
resets it and two recalls either side of one count twice; ours is decided against
the stream, so it is a function of the data.

### The fold, and the watermark

`last_recalled_at` **is** the watermark: every recall event strictly newer than
it is unfolded. That is exact precisely because of the window — two recalls of
one subject are at least one window apart, so no two can share a millisecond and
be split by a strict comparison. A second durable cursor would be a second thing
that can disagree with the first.

The fold runs **before** the pass decides. Expiry reads `access_count` to choose
between promoting a memory and archiving it as unused, so a decision taken first
would archive a memory that had just been read.

### Reads peek, and never consume

A recall does not touch the record, and the record is next visited when it is
*owed* something — for anything that decays, a day away. So the stored counters
lag by up to a day, not by a sweep interval. Phase 10's end-to-end run is what
showed it: two recorded recalls, three completed sweeps, `access_count` still
zero.

Every read therefore folds outstanding recalls into the copy it returns, without
writing — Rust's answer and its reason
(`services/search_engine.rs:165-171`): "a read must never consume a recall no
write has applied". The seek is into a range that is empty for every memory
nobody has recalled since the last sweep, which is nearly all of them.

**What this deliberately does not fix:** ordering a listing by
`last_recalled_at` walks the attribute index, and the index holds the *stored*
value. Correcting the returned records would produce a page ordered by one number
and displaying another, which is worse than a page consistently one visit behind.

---

## Scheduling

`NextAttention` is a **pure function of the record and the policy table** — every
arm is a stored timestamp plus a constant, and it reads no clock. That puts it
outside Invariant 8 by construction and makes a rebuilt attribute row comparable
with an incrementally maintained one. Plan §Phase 10's signature takes a `now`;
Rust's does not, and it does not need to.

It is computed by `record.Repo.Put` through a `record.Scheduler`, not by each
write path. A write path that computed its own schedule is one that can omit it,
and the symptom is silent and permanent: a memory that never decays, never
expires and appears in no sweep.

**`MaxAttentionInterval` (30 days) caps every schedule.** A `pinned` memory has
nothing owed and would otherwise be scheduled for never — at which point a later
policy change could never reach it, because no walk visits a record due at the
end of time.

### The lifecycle never moves `updated_at`

It is a content-edit timestamp and one of the five orderings Phase 5 ships. Rust
has to move it on archive because `cleanup_archived` selects on it; Go schedules
cleanup from `archived_at`, so the rule holds everywhere without an exception.

`memory.Update` moves it, and that is the field doing its job rather than an
exception to the rule: an edit *is* the content edit the timestamp is named for.
The rule is about the lifecycle's own passes, which must leave a listing ordered
by `updated_at` alone.

### An edit reschedules the record it edits

`record.Repo.Put` recomputes `next_attention_at` through the scheduler on every
write, so a policy or TTL change made through `memory.Update` takes effect on
**that record** at once. A write path that computed the schedule itself is a
write path that can omit it, and this is the payoff for putting it in the
repository instead.

What "reschedules" means is narrower than it sounds, and the narrowing is the
scheduler being right rather than a gap. `EarliestTransition` is a function of
*whether* a transition is owed, not of how fast it runs: `short_term` and
`long_term` decay health at 8.0 and 2.0 points a day, so both are due a whole
day out and moving a record between them does not move its schedule at all. Only
`pinned`, which is owed nothing, falls through to the `MaxAttentionInterval`
cap — and a TTL is the other thing that genuinely moves the date.

This does **not** close the bound below. One record's own policy field is not a
tenant's policy table.

### Known bound: a policy change reaches a memory when the memory is next visited

The schedule is computed from the policy in force when the record was last
written. **Shortening** a retention therefore takes effect on the old schedule
for records already archived — verified end to end: a cleanup shortened from 60s
to 5s deleted the memory at 64 seconds, not at 5.

For a live memory the lag is bounded by `MaxAttentionInterval`, and for the
common direction (lengthening) there is no lag that matters. For a shortened
cleanup — which is the compliance-shaped request — the lag is the previous
retention. Closing it needs a rescheduling pass over the tenant's records, which
Phase 10 does not build. **REM-114.**

---

## The run

- **`DefaultRunBudget` 10,000** memories acted on per run
  (`lifecycle_manager.rs:38`). A bound on a single run, never on the work.
- **`SweepEffortFactor` 8** bounds candidates examined. In Rust it does most of
  the work, because each of five sweeps discards four in five; here one pass has
  something owed to nearly every due memory, so it rarely binds.
- **A run that spends its budget enqueues its own continuation**, carrying the
  position it stopped at. Every record written before Phase 10 has
  `next_attention_at = 0` — due now — so the first sweep after an upgrade meets
  the whole corpus, and at the sweep interval alone a million memories would take
  a hundred hours to become scheduled. It terminates because the cursor moves
  strictly forward over a finite range.
- **A memory owed nothing is not written.** Not even a corrected schedule, which
  would cost a record rewrite to save a row read. It stays due and the next run
  reads its row again, which is the cheapest read in the system.

**There is no migration.** Slot 9 has been declared and populated since Phase 5,
so the access path already exists and every row holds 0 — which reads as "due
now" and is exactly right. A `StrategyRebuild` step would be upgrade downtime to
precompute what the first sweep computes correctly anyway.

---

## Failure semantics

| | |
|---|---|
| A recall that cannot be appended | Logged; the read succeeds. The memory the caller asked for is in hand, and failing the request would trade the thing they wanted for the thing that measures it |
| A policy row that will not parse | `errs.Corruption`, never an empty table. Falling back to the built-ins would apply the wrong retention to a tenant that had deliberately chosen otherwise |
| An event row that will not parse | `errs.Corruption`, never skipped. An audit that omitted the entry it could not read would report a memory as untouched at the moment the trail says otherwise |
| An event of an unknown kind, read back | Returned verbatim. That is a newer binary's event during a rolling upgrade, and hiding it is worse than naming one we cannot interpret |
| A tenant's policy table that cannot be read at write time | The built-ins are used for that write, with one warning. The sweep re-reads the table every run and corrects it |
| A sweep that loses a conflict | Skipped, left due, counted. The user's write wins |
| A continuation that cannot be enqueued | Logged; the backlog drains at the sweep interval instead. Failing the job that just did ten thousand memories' work would undo none of it and retry all of it |

---

## Configuration

`lifecycle.enabled`, `lifecycle.sweep_interval` (1h), `lifecycle.run_budget`
(10,000), `lifecycle.recall_window` (30s), `lifecycle.event_retention` (90d).

**Three settings were retired** and are refused by name at start-up:
`lifecycle.decay_half_life`, `lifecycle.promotion_threshold` and
`lifecycle.archive_at_health`. Each described a model that named policies
replace, and each is now a field on a policy, per tenant. Keeping them would
leave a global dial that silently loses to a policy — the "two names, one
setting" defect Phase 9's end-to-end run found.

The scheduler's tick is **derived from the shortest registered interval**, not
from a setting. It used to come from `jobs.retention`, which held only while the
reaper was the one recurring type; Phase 10's run found a sweep asked to run
every five seconds and looked at once a minute.

---

## Metrics

`remem_lifecycle_transitions_total{tenant,kind}`,
`remem_lifecycle_sweep_duration_seconds{tenant}`,
`remem_lifecycle_due_backlog{tenant}`.

The backlog gauge is the one to alert on: it is what a run left behind, and zero
is the healthy steady state.

There is deliberately **no** `events_written` counter. The first version had one
and it was `transitions_total` under another name — same loop, same value — while
its name promised coverage it did not have, since the commonest event by far is a
recall the sweep never sees.

---

## What Phase 10 does not do

- **Consolidation.** REM-113. Absent from the codebase and the admin surface, not
  registered as a no-op.
- **Durable schedule state for the job scheduler.** "This type last ran at T" is
  still in process memory, so a restart makes every recurring type due at once —
  which for repair-shaped work is right rather than merely tolerated.
- **A rescheduling pass after a policy change.** REM-114; see the known bound
  above.
- **The differential harness's lifecycle axis.** Decay arithmetic is compared
  against `fixtures/decay_reference.json`, generated by a standalone `rustc`
  program that transcribes four expressions from the frozen source with their
  line numbers. What that buys is Rust's arithmetic under the same f32 semantics
  and the same integer truncation; what it does not buy is a comparison of which
  memories the sweeps *select*. Phase 13 owns that, and
  `test/differential/COVERAGE.md` says so.
