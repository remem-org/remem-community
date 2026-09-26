# Schema and migrations

How a Remem directory moves forward when the binary that opens it is newer than
the binary that wrote it, and what is guaranteed while that is happening.

Spec §20 asks for migrations that are explicit, ordered, resumable, idempotent,
observable, testable and crash-safe, in three strategies. Invariant 5 asks for
every durable transition to state whether it is resumable or atomic. This
document answers both, and records the three places Phase 4 departed from the
plan and why.

Package: `internal/schema`. Its neighbours are `internal/version` (which
numbers the formats), `internal/storage` (which now knows how to copy itself),
and `internal/record` (which carries the hook a lazy migration runs through).

## What is authoritative

| Thing | Where | Rebuildable? |
|---|---|---|
| Format manifest | `/format/<name>`, system space | No. It is the answer to "may this binary open this directory" |
| Attribute slot table | `/slots`, system space | No. Without it no attribute row on disk can be decoded |
| Migration state | `/migration/<id>`, system space | No. It is where a crashed run resumes from |
| A record's `schema_version` | inside the record body | No. Without it nothing can tell whether a record is behind |
| Pre-migration backups | `<data_dir>/.backups/pre-<unix>/` | Derived, but never regenerable after the fact |

All four durable rows are untenanted: they describe the whole directory, and
they have to be readable before any tenant is known — which at open time is
always. The manifest is fixed-width; the other three are bare protobuf, matching
the convention `internal/tenant/tenantkv` sets for system rows. `internal/codec`'s
envelope frames the *record* key space, and a system row is not a record.

## The two version scales

They are different numbers and must never be confused (spec §19.3, plan §II.5b).

- **Storage format versions** are Remem's: key encoding, record envelope,
  snapshot format, attribute slot schema, cluster compatibility. Five,
  independently versioned, in `internal/version`. A `Migration` moves these.
- **User schema versions** are a tenant's. Tenant A may be at 15 while tenant B
  is at 8, and neither implies anything about the bytes on disk. A
  `RecordUpgrade` moves these.

`TestUserSchemaVersionIsIndependentOfStorageFormat` runs a storage format
migration and asserts that neither the tenant row nor the record body moved.

## The three strategies

Spec §20 requires all three, and the difference between them is where the cost
lands.

| Strategy | Cost | When it runs |
|---|---|---|
| `Instant` | None — a new optional field, a new key namespace, a feature flag | Inside `server.New`, before anything listens |
| `Lazy` | Paid per read, by the reader | On every `record.Repo.Get` of a record that is behind |
| `Background` | Proportional to the corpus, paid once | Alongside serving, after the listener is up |

**The plan does not say when the runner runs; this does.** Instant and lazy
steps complete before the port is bound, because everything downstream assumes
them and they are cheap. Background steps run alongside serving, because
blocking start-up on one would make upgrade downtime proportional to corpus
size, which is the thing the strategy exists to avoid.

The split is at the **first background step, by position, not by strategy**
(`schema.Split`). An instant step registered after a background one requires the
version that background step produces; promoting it to start-up would run it
against a directory not yet migrated. Everything from the first background step
onward keeps the plan's order and waits its turn.

A background migration that fails does not stop the server. The directory is at
the version the completed steps left it at, which is a version this binary can
serve. Taking the process down would be the wrong answer to "a migration needs
looking at" — the failure is in the state row, and the next start resumes from
its cursor.

## Ordering comes from declarations, not from list position

A `Migration` declares `Requires` (subsystem → the exact version that must be on
disk) and `Advances` (subsystem → the version in force afterwards).
`Registry.Plan` derives the order from those.

`Requires` is exact, not "at least". A step written against version 2 has not
been thought about against version 3, and running it there is a guess.

The plan is a **total order**: ties break by migration id, so two runs of the
same binary against the same directory produce the same sequence. That is not
cosmetic — a plan that varied between runs would resume a crashed migration into
a different sequence than the one that crashed.

Two bounds, in opposite directions:

- **A gap** — the target cannot be reached — is `errs.MigrationRequired`, naming
  the format and both versions, refused before anything listens. The alternative
  is serving a directory nothing has migrated: reads that return something
  plausible from bytes this binary interprets differently than the binary that
  wrote them.
- **Overshoot** — a step producing a version beyond the target — is never
  selected. "Plan from here to there" means it.

Six more refusals land at `NewRegistry`, where each is a start-up failure naming
the step rather than a half-migrated directory: a duplicate id, two steps
producing the same version of one format, a step that does not move forward, an
unknown subsystem name, a step advancing past what this binary writes, and a
step with no `Run` or no `Strategy`.

## Resumable versus atomic (Invariant 5)

This is the table Invariant 5 asks for.

| Transition | Guarantee |
|---|---|
| One step's work | **Resumable.** Checkpointed; a crash resumes from the last committed cursor |
| Work + cursor | **Atomic.** One transaction, always |
| Manifest advance + step marked done | **Atomic.** One transaction, always |
| A whole plan | **Resumable, not atomic.** Steps land one at a time, and a plan interrupted halfway leaves the directory at the last completed step |
| The backup | **Atomic from the reader's side.** A Pebble checkpoint is consistent by construction; a partial one is never observable |
| A lazy upgrade's write-back | **Best-effort.** Lost, it is redone on the next read |

### Why the checkpoint takes the transaction

The plan gives `Context` a `Progress func(cursor []byte, processed uint64)` and
then requires cursor and work to commit together. A callback that only records
cannot do that. **This is Phase 4's first departure from the plan.**

`Context.Checkpoint(ctx, tx, cursor, processed)` therefore takes the caller's
transaction, stages the cursor into it, and commits it. A step commits through
`Checkpoint` and never calls `tx.Commit` itself, so there is no way to commit
work without the cursor that describes it. The invariant is structural rather
than a convention a future migration author has to remember.

One level down, the runner advances its in-memory state **only after** that
commit succeeds. Advancing first would leave a failed commit with a cursor
claiming the work was done — invisible until a resume skipped the records.

### Why finishing a step is one transaction

The manifest advance and the done marker commit together. Apart, one order
leaves a directory claiming a version whose step did not finish; the other
leaves the manifest behind with the step skipped, so the version can never be
reached and the next open refuses the directory naming a gap nothing can close.

### Every step must be safe to rerun

This is the contract, inherited verbatim from Rust's `migrations.rs`. A crash
anywhere inside a step brings the runner back to it with the originals wherever
the last checkpoint left them, and the step runs again from that cursor. A step
that is not rerunnable from its own checkpoints is a bug in the step.

`Advances` sets `Current`, `MinReader` and `MinWriter` alike. That is the
conservative default `CurrentFormat` already documents: a bump made without
thinking about compatibility locks older binaries out rather than letting them
in, and letting them in wrongly is the expensive direction.

## The backup is a checkpoint, not a file copy

**Phase 4's second departure from the plan.** The plan said to port Rust's
recursive directory copy verbatim. Rust owned its own WAL and SSTable tree; Go's
data directory *is* an open Pebble database, and a file-by-file copy of one
captures the LOCK file and a write-ahead log mid-write. The result may refuse to
open, which is the one failure a pre-migration backup cannot have, because an
operator has already planned around it being there.

`storage.Backupper` is an optional interface asking the engine for a consistent
copy. Pebble answers with `db.Checkpoint(dest, WithFlushedWAL())`; the in-memory
store answers with a `storage.Dump`, which is what makes the runner's backup
path exercisable in an ordinary unit test.

`WithFlushedWAL` is load-bearing, not defensive. The Pebble adapter writes with
`NoSync` — durability is the transaction's decision, taken once by the operator
through `storage.sync_writes` — so recent writes live in a WAL buffer that has
not reached the file a checkpoint copies. The first checkpoint written here came
back *without the record that had just been written*, and only reopening it
noticed.

Everything else is Rust's policy, kept for Rust's reasons:

- **`<data_dir>/.backups/pre-<unix>/`, inside the data directory.** A sibling is
  not guaranteed to be on the same volume when `data_dir` is a bind mount; on a
  container's writable overlay the backup disappears with `docker compose down`.
- **Once per plan**, before the first step declaring `RewritesData`, never for a
  step that only adds rows.
- **Never auto-deleted.** Disk is cheaper than the corpus.
- **`REMEM_SKIP_MIGRATION_BACKUP=1`** opts out. An environment variable rather
  than a configuration key, because it is a decision about one upgrade rather
  than a standing property of the deployment.
- **A backup that fails aborts the plan with the data untouched.** Proceeding
  would rewrite a corpus having told an operator it was protected.
- **A store that cannot copy itself is refused**, not silently skipped.

Two additions. Nothing is backed up when the directory holds no user data — a
manifest and a tenant row are not data, and a fresh install would otherwise
accumulate empty directories nothing ever removes. And a destination that
already exists is treated as "already taken" rather than as a failure: two
restarts inside one second is what a crash loop looks like, and refusing there
turns a recoverable upgrade into a stuck one.

## The lazy upgrade, and the read that must not fail

**Phase 4's third departure from the plan**, or rather a decision the plan
leaves open. It says an old record is "upgraded, written back, and returned"; it
does not say what happens when the write-back fails.

The upgrade itself is **not optional** — returning a half-upgraded record hands
the caller something it reads as current and is not, so a failing
`RecordUpgrade` fails the read. The **persistence is best-effort**: it happens
in its own transaction, without an fsync, and a failure is logged rather than
propagated.

The write-back uses the ordinary record projection path, so the body and every
derived attribute row change atomically. It also expects the exact body that was
read. A concurrent update or deletion therefore turns persistence into a
best-effort conflict instead of being overwritten or resurrected; the caller
still receives the correctly upgraded in-memory value.

A read that failed because a cache-fill failed would fail on a read-only store,
which is exactly how `remem-admin inspect` opens a directory — and during
shutdown, and while a disk is full. The upgraded record is correct either way;
the only cost of a lost write-back is doing it again next time.

Boundaries, stated because each is a place someone will reasonably expect the
opposite:

- **Only `Get` upgrades.** `Scan` does not: a listing that rewrote every record
  it walked would turn a page of results into a page of writes, and a background
  migration is the tool for a whole corpus.
- **A record from the future is left alone.** One written by a newer binary at a
  schema this one has never heard of is readable — protobuf keeps the fields
  this binary does not understand — and refusing it would take a mixed-version
  deployment down on the read path.
- **The chain must be contiguous.** A gap is refused at construction: a record
  at a version nothing can leave would be re-examined on every read, forever,
  silently.

### The debt this creates

The write-back is a non-deterministic side effect on a read path, which
Invariant 9 forbids inside a replicated apply path. Nothing replicates in Phase
4. **Phase 14 must move the write-back off the apply path** — most likely by
enqueuing it as a job rather than performing it inline. The code says so where
it happens; this is the record.

Phase 9 built the queue that would carry it, and the shape it would take is the
one the index-repair triggers already use: the read path enqueues and returns,
and a worker does the work. That is a change to make deliberately when Phase 14
arrives, not one to make because the mechanism now exists — an upgrade that is
currently one write on one read would become a durable row and a scheduling
decision for every stale record in the corpus.

## The slot registry

Ported from Rust's `engine/attr/schema.rs`, with its wording kept because the
wording is the guidance an operator acts on.

An attribute row is a packed blob addressed by slot number and skipped by
declared width. The table is what turns those bytes back into named values, so:

- a slot number is **never reused** — it is an address on disk, and reusing one
  repoints every row written under the old meaning;
- a slot is **retired, never dropped** — "retire slots, never drop them" — because
  the declaration is the width a reader needs to skip past a value it no longer
  has a name for;
- a type or name change is refused: "retire the slot and add a new one instead";
- a retired slot cannot be revived, and is never an access path;
- a slot above `MaxEncodableSlot` (2039) is refused at registration.

Two additions beyond the Rust original. A duplicate slot *name* is refused,
because two slots answering to one name is a query that cannot be resolved. And
`SlotString` exists, variable-width and length-prefixed in the row, because
Phase 5's table needs it for `policy` where Rust used an enum byte.

`MaxEncodableSlot` belongs to Phase 5's row header, which stores its presence
bitmap's length in one byte. It is restated in `slots.go` because registration is
where a violation can still be a build failure; at encode time it is a silent
truncation on a cast. Phase 5 confirms or moves that one number in one place.

**Phase 4 owns the mechanism; Phase 5 owns the content.** The composition root
does not call `OpenSlots` yet, because there is no table to register until
`internal/attr` exists. Phase 5 adds that one call.

## Observability

Spec §20 requires migrations to be observable. Three things carry it:

- **Structured logs** — one line per step start and completion, naming the step,
  its strategy, where it resumed from, and what it advanced.
- **The durable state row** — `processed_count`, `last_progress_at` and the
  error that stopped the last attempt, so an operator finds out why without the
  log line that scrolled past.
- **Metrics** — `migration_running` and `migration_failures_total`, always.

`migration_progress_ratio` and `migration_records_remaining` stay unset unless a
step sets `Context.Total`. Most steps cannot know their own size in advance, and
a ratio invented by one that does not is a number a dashboard treats as real.

## Fixtures

`internal/schema/testdata/legacy-format/store.kvdump` is a whole directory at an
older format, held as a `storage.Dump` stream rather than as a checked-in Pebble
directory. A Pebble directory would be a fixture whose format is *Pebble's*
version rather than Remem's: a Pebble upgrade would break it, and reviewing a
change to it would mean reading a binary blob.

Regenerate with:

```bash
go test ./internal/schema -run TestGenerateTheLegacyFixture -update-fixtures
```

The generator is a flagged test rather than a script, so whatever writes the
fixture is compiled against the same types that read it. Record ids are
deterministic, so a regenerated fixture does not diff as a whole new file, and
`TestTheLegacyFixtureIsBehind` asserts the fixture is actually behind — without
it, a regenerated fixture could quietly become current and
`TestLegacyFixtureMigratesForward` would pass by migrating nothing.

## What Phase 4 deliberately did not decide

- **The first real migration.** `schema.Builtin` is empty, which is correct for
  a phase that built the machinery. Phase 5 adds the first, for attribute slots
  10 and 11.
- **Per-tenant user-schema divergence.** A `RecordUpgrade` chain has one target,
  not one per tenant. Tenant rows already carry `schema_version` so that
  introducing it later is additive; nothing reads it before Phase 13.
- **Migration under consensus.** Spec §20.3 requires resume across cluster
  failover. The state row carries what that needs; running a migration on a
  replicated log is Phase 16's.
- **Undoing a migration.** There is no down-step and there will not be one. Spec
  §59 is explicit that a downgrade is never automatic, because it means
  rewriting data the older binary cannot read. The backup is the way back.
