# Background jobs

What is authoritative, what is derived, what the delivery contract is, where the
atomicity boundaries are, and what a caller and an operator are each told when
something is wrong. Phase 9. Package `internal/jobs`, with the built-in handlers
in `internal/server` and the administration surface in `internal/api/http`.

## The contract, first, because it is what handlers are written against

**Delivery is at-least-once. Every handler must be idempotent.**

A job runs twice for three ordinary reasons:

- a lease expires while a slow handler is still working, and another worker
  reclaims the job;
- a process dies between the handler returning and the completion committing;
- a retry re-runs a handler that failed after doing half its work.

Exactly-once delivery over a durable queue requires the handler's writes and the
queue's completion to be one transaction. Remem's handlers write far more than
one transaction should hold — a rebuild of a large tenant is thousands of
batches — so the honest trade is at-least-once plus idempotent handlers.

The obligation is **asserted rather than requested**.
`TestEveryRegisteredHandlerIsIdempotent` runs every registered type twice on one
payload and compares the whole keyspace, and a type with no fixture in that
test's table fails it **by name**. Both halves were verified by breaking them:
deleting a fixture entry produces the named failure, and making a handler write
a run-numbered row produces the keyspace mismatch.

## What is canonical, and what is derived

**Job state is canonical** (plan §II.4). Nothing reconstructs it. A lost job row
is lost work, and that is precisely the Rust defect this framework replaces: the
discovery queue was a bounded `mpsc` that dropped work under load and counted
the drops. There is **no rebuild path for the job space** and there must not be
one.

Everything a job *produces* is either canonical (the edges discovery will write
in Phase 11) or a derived index the handler rebuilds. The queue itself never
repairs anything.

Two later spaces sit beside the queue and are canonical for the same reason.
**A pause** (`keys.SpaceJobPause`, `0x0D`) is an operator's decision and nothing
derives it — a suppression that could be reconstructed from elsewhere would be
one somebody set that nothing recorded. **An attempt's history**
(`keys.SpaceJobRun`, `0x0E`) is an account of something that happened, and a
past execution cannot be recomputed from the present. Both are *bounded* rather
than kept for ever, which is a different thing from being derived: the reaper
removes them on the retention, and what is gone is gone rather than rebuildable.

Neither needed a migration. An absent pause row reads as "not paused" and an
absent history reads as "no attempts retained", so an older data directory is
already correct under the new code and no existing job row's decoding changed.

## Six states over three durable key partitions

`internal/keys` shipped three job-state bytes in Phase 2 — `JobPending 0x01`,
`JobRunning 0x02`, `JobDone 0x03` — and they are a durable format. Spec §23 asks
for six states. They are not in conflict: the key byte is a **scan partition**
and the body's state is the finer state within it. The mapping is fixed.

| Logical state | Key partition | Encoded due time | The scan it serves |
|---|---|---|---|
| `Pending`, `Retry` | `JobPending` | `RunAt` | claiming: pending and due at or before now |
| `Running` | `JobRunning` | `Lease.ExpiresAt` | reclamation: leases that have lapsed |
| `Completed`, `Failed`, `Cancelled` | `JobDone` | `UpdatedAt` | reaping: finished before the retention |

Each of the three scans the queue performs is therefore a **forward scan that
stops at the first row it does not want**, which is what putting the partition
ahead of the time was for. `Retry` gets no partition of its own because a retry
*is* a pending job with a later due time and a non-zero attempt count; splitting
them would make the claim scan read two ranges and merge them by due time, for
nothing.

The key and the body say the same two things twice, and both halves are written
in one transaction. `Unmarshal` refuses a row where they disagree, as
`errs.Corruption`. Trusting either half over the other would put a job in a scan
that never reaches it or in one that reaches it for ever.

`JobDueRange` builds its exclusive upper bound at `due+1` with a zero id, not at
`due`. The id trails the due time, so a bound built at the bound itself silently
drops every job due exactly then with a non-zero id — which is all of them.

## Atomicity

**Every state transition is one transaction**: the old key is deleted and the
new one written together. A job is never in two partitions and never in none,
including across a crash at any point.

**Every transition is conditional.** The old row must still be exactly as it was
read (`tx.Expect`), so a concurrent claimer, reaper or canceller loses rather
than overwrites.

**Every transition re-reads the durable row** rather than trusting the caller's
copy. Two operations on one job — a checkpoint saved while the renewal timer is
moving the lease — therefore compose instead of discarding each other. Building
the next state from the caller's copy makes whichever lands second win, silently
and only under load; that is what
`TestACheckpointAndARenewalDoNotOverwriteEachOther` pins.

**`Enqueue` stages into the caller's transaction.** That is the shape Phase 11
needs and the reason the queue is durable at all: a discovery job written in the
same transaction as the memory means a failed write leaves no orphan job and a
successful one cannot lose its follow-up. **Phase 11 shipped exactly that**, and
proved it the way such claims have to be proved here — a pair of memories stored
and the process `kill -9`ed before any worker could claim the job, which came
back after the restart and did its work.

## Exclusivity: what makes it correct, and what makes it fast

Correctness is the **conditional commit**. Two claimers that pick the same job
produce one winner and one `errs.Conflict`, and the loser moves to the next
candidate — a conflict here means somebody else is doing the work, which is not
a failure to report.

Throughput is **`txn.Gate`**, the striped per-tenant lock Phase 8 introduced.
Conflict-driven exclusivity converges slowly when sixteen workers poll one
tenant; the gate removes the contention rather than absorbing it. It is the
local stand-in for the total order raft provides in Phase 14, exactly as it is
for the corpus statistics.

A guard that is never exercised is a guard nobody has verified, so
`TestClaimIsExclusive` runs **twice** — once through the gate and once with
`WithoutGate()`. Deleting the `Expect` makes the second run fail with fifteen
claimers colliding while the first still passes, which is the evidence for which
of the two is load-bearing.

## Leases and fencing

A lease carries an `Owner` and a `Token` from the first commit, even though
there is one node. That is spec §24's "abstractions compatible with future
distributed execution": cross-node claiming becomes a change to how a lease is
granted and never a change to a handler, where retrofitting fencing later would
mean auditing every handler there is.

**Renewal moves the row**, because the running partition is ordered by lease
expiry. A renewal that only updated a field would leave a live job at the head
of the reclamation scan while claiming to be alive. It deliberately keeps the
token: "the token changed" must mean "you lost the job" and never "time passed".

**Reclamation keeps the attempt** the claim charged. That is what stops a
handler that hangs every time from looping for ever — each reclamation is
followed by a claim that charges another attempt, and the job that runs out is
`Failed` by the reclamation rather than returned to the queue.

**The fence is not decoration.** The key alone is nearly enough: a reclaimed job
is almost always at a different key, so a stale worker's read fails before the
token is consulted. Nearly. A running key encodes the lease expiry, so two
successive owners collide on one key whenever an NTP step backwards or a lease
duration changed across a restart puts the second expiry at the first's instant.
`TestTheFenceHoldsWhenTheNewOwnerLandsOnTheSameKey` constructs that row directly
— a forward-only fake clock cannot produce it, and a defect nobody can reach in
a test is a defect that ships — and watches complete, fail, renew, save and
release all be refused.

**Release is the shutdown path and refunds the attempt.** A process stopping is
not a job failing, and a job that lost an attempt to every rolling restart would
eventually be `Failed` having never once broken.

## Retry

Windows double from one second and stop at ten minutes, and the delay is drawn
uniformly from `[window/2, window)`.

Each bound answers a different failure. Windows that do not grow are a tight
loop against whatever broke. Windows that do not stop growing are a queue that
silently goes stale — ten minutes is where the right escalation becomes a person
rather than another doubling. Half jitter rather than none, because unjittered
backoff keeps every worker that failed together failing together, colliding
again at each doubling; half rather than full, because full jitter sometimes
draws a near-zero delay and defeats the point.

The shift is **bounded rather than computed**. An attempt count large enough to
shift past a duration's width produces zero, which turns the backoff into a
tight loop at exactly the moment things are going worst.

The delay is drawn **outside** the transaction that stores it. A transaction
body may run more than once, and Invariant 9 forbids a random draw inside a path
Phase 14 replicates, so the value is decided first and the transaction writes an
already-decided one.

A retry keeps the previous failure's message and a completion clears it. An
operator looking at a running job should be able to see it has been here before;
a job that finally succeeded should not still be carrying why it once did not.

## Checkpoints

A handler is given a `Checkpointer` and not the queue. Recording progress is the
only thing it may do to its own job — a handler that could complete or re-lease
its work would be a second place the state machine lives.

`Save` requires a lease: without that check any caller could overwrite a pending
job's cursor, recording progress against a run nobody is performing.

A checkpoint survives a failure, a reclamation and a restart, which is what makes
a retry cheaper than a first run rather than a repeat of it.

**The `*Job` a handler is given is its own copy, taken at launch.** Read its
`Checkpoint` once, at the start, to decide where to resume; `Save` does not
update it, and must not. The pool's own copy is rewritten wholesale by every
transition — each renewal, each save — under the running job's mutex, and
handing the handler that pointer meant every read of its own job raced the
renewal ticker. A struct copied mid-read can yield a checkpoint's slice header
from one version and its length from another. Phase 13's `make cover` found
it; Phase 9's tests never had a handler that read its job while a renewal ran,
and `TestAHandlerReadingItsJobDoesNotRaceTheRenewal` now does.

## The worker pool

`Run` owns every goroutine it starts — dispatcher, renewal ticker, gauge
refresh, one per running job — and returns only when all of them have finished
(spec §57). A pool goroutine outliving `Run` would keep a data directory open
after shutdown reported success.

**The dispatcher takes a worker slot before it claims.** A claimed job waiting in
a channel for a worker is a lease held for nothing, and at scale it is a queue
reclaiming its own work while that work sits in memory.

**Tenants are walked round-robin from where the last poll stopped**, bounded at
32 a tick, over a directory listing reused for ten seconds. Restarting at the
first tenant in id order starves everything after the first tenant that can fill
the pool; re-reading the whole directory every second is a scan per tick that
grows with the customer count. A tenant provisioned in between waits up to ten
seconds for its first job.

**Lapsed leases are reclaimed before new work is claimed**, so a busy queue
cannot starve its own retries.

**A panicking handler fails its own job and nothing else.** Letting it through
takes the process down, which turns one bad payload into an outage of every
background workload — and the job that caused it comes back on the next start
and does it again.

**Shutdown drains rather than stops.** A handler told to stop needs a moment to
record where it got to, and a shutdown that cancelled and exited would throw
that away on every restart. What has not returned inside the budget keeps its
lease, which lapses on its own, and the next process reclaims it. The
bookkeeping after a handler returns runs on a context the cancellation cannot
reach — otherwise the completion that failed *because of* the drain would leave
the job running until its lease lapsed, the one outcome the drain exists to
avoid.

**Cancellation is cooperative and costs no extra read.** `Renew` already
re-reads the durable row, so a job somebody cancelled is discovered there and
the handler's context is cancelled. The cancellation transaction already
recorded the attempt, including counts reported on this node before the cancel.

## The scheduler

It writes rows; the pool runs them. Recurring work is then identical to work
anything else enqueued — same retries, same leases, same visibility in the
admin surface. Rust ran five sweeps on five schedules, each with its own loop
and its own idea of failure.

Fan-out is `tenant.ForEach`, the single audited cross-tenant path, because
recurring work is per tenant by definition: there is no such thing as a
maintenance job for everybody.

**One outstanding job per tenant and type.** A recurring job that runs slower
than its interval must not queue a second copy behind the first — a tenant
permanently behind then accumulates work faster than it can do it, and every
copy makes the next one slower.

**A pause suppresses scheduling, and only scheduling.** An operator may stop a
recurring type for one tenant until a required deadline. Work already queued or
running is untouched and finishes through the ordinary lifecycle, and a manual
run still queues — pausing outstanding work would conflate schedule control with
the per-job cancellation that exists precisely for it, and would do it without
anybody naming the job.

The expiry is required and bounded at `jobs.MaxPauseDuration` (seven days), and
it is evaluated against the reader's own clock. That is what makes a pause
self-clearing: nothing has to run for one to lift, so a process that died
holding a pause does not leave one in force. A boolean pause with no expiry was
rejected — it suppresses repair indefinitely and reports nothing, which is the
failure mode this whole framework replaces. Past a week the honest action is to
turn the subsystem off in configuration, where it is visible on every start.

A type nothing schedules cannot be paused, and the refusal says so. Accepting
one would report success and change nothing, and an operator acting on that
answer would believe work had stopped that never ran on a timer.

**The race is closed by the conditional commit, not by the gate.** A tick that
has already read "not paused" and is about to write is exactly the window an
operator pausing lands in. `Queue.SubmitUnlessPaused` therefore reads the pause
*inside* the transaction that writes the job and expects the pause key absent at
commit: a pause written in between makes the commit conflict and the job is
never written. This is the shape Phase 11 gave `graph.Service.Linked`, for the
same reason — a per-tenant gate would make the race unlikely rather than
impossible and would do nothing across nodes, and `txn.Gate`'s own comment says
so. `TestAPauseWrittenMidTickLosesTheJobRatherThanRacingIt` drives that window
deterministically and was verified to fail with the `Expect` removed.

**Its schedule state is in memory**, so a restart makes every recurring type due
at once. For repair-shaped work — which is what recurring work is in this phase
— running it on start-up is right rather than merely tolerable, and the
outstanding-job check stops a restart loop from piling up copies. A durable
schedule belongs with Phase 10's per-tenant maintenance, where "this tenant was
last swept at T" is a fact about the tenant rather than about this process.

## Cost

| Operation | Cost |
|---|---|
| Enqueue | one key, staged into a transaction the caller already has |
| Claim | a forward scan of one tenant's due pending rows, bounded at 1,024 examined, plus one transaction per job taken |
| Renew, save, complete, fail, release | one point read and one two-key transaction |
| Reclaim | a forward scan of one tenant's expired running rows, bounded by the caller's limit |
| Reap | a forward scan of one tenant's terminal rows older than the retention, deleted in one transaction |
| `Get` by id | a scan of one tenant's three partitions |
| `HasOutstanding` | a scan of one tenant's two live partitions |
| Gauge refresh | a scan of one tenant's two live partitions, capped at 100,000 rows, for 32 tenants every 30 seconds |

**`Get` is a scan, and that is a decision.** The key is ordered by partition and
due time, so a job cannot be addressed by its id alone. A second row per job
mapping id to its current key would have to be written and deleted by every
transition — roughly a third more write amplification on the queue's hottest
path — to make a rare administrative lookup O(1). The cost is paid on the rare
operation instead, and the retention is what keeps it bounded. Nothing on the
claim, completion or checkpoint path comes through it: each of those already
holds the job it is acting on, and therefore its key.

## Retention

Terminal rows are kept for `jobs.retention` (24h by default) and then reaped.

The retention is the one setting that keeps two things true at once: the audit
trail stays long enough to answer "what happened to my rebuild", and the queue's
scans stay bounded. The reaper is itself a recurring job — which is what gives
the scheduler and its per-tenant fan-out a real user in this phase.

**One retention, one reaper, three spaces.** The same pass removes finished job
rows, attempt history past the window, and pause rows that expired before it.
Attempt history is the one job space whose row count grows with *time* rather
than with work outstanding, so it is the one that most needs the bound; a second
thing removing it on a second schedule would be a second number an operator has
to know. Expired pause rows are housekeeping rather than correctness — expiry is
honoured on every read — but without the sweep a tenant keeps one dead row per
pause anybody ever set.

## Attempt history

A job row aggregates an attempt count and a terminal state. It cannot say what
each retry *did*, and at-least-once delivery makes a retry ordinary rather than
exceptional — so a job that failed twice and then succeeded is three executions
and three different answers to "what happened".

**One durable row per attempt**, carrying the job's id and type, the attempt
number, start and finish, the outcome, the error where there is one, the node
that ran it, and the handler's own `records_processed` and `records_changed`.
The type is copied onto the row because history is read by type and the job row
it came from may already have been reaped.

**Each row describes one attempt, never a total across them.** Summing an
attempt's counts across a job's retries does not produce a count of distinct
records — at-least-once delivery means some were visited more than once — and
nothing presents it as one.

**A count nobody reported is absent, not zero.** "This attempt changed nothing"
and "nobody counted" are different answers, and a zero standing in for the
second is a number an operator will act on. The counts come from the handler
through `jobs.Checkpointer` and from nowhere else: only the handler knows what a
record is — a vector, a memory, a subject, a deleted row — and inferring one from
a checkpoint, an elapsed time or the size of an index afterwards produces a
figure that looks exact and is not. `Processed` and `Changed` are absolute
totals for the attempt rather than increments, so a handler counting as it goes
simply calls them again.

**Partial counts survive a failure.** A handler that broke after doing half its
work did that half, and its row says so. The lifecycle pass and discovery report
on their failure paths for exactly this reason.

**The attempt row and its queue transition commit together.** Completion,
failure/retry and cancellation each use one conditional transaction for the
queue row and history row. A crash cannot leave a terminal or retry state
without its attempt. The same lease fence protects both writes: a worker whose
claim was reclaimed cannot move the queue row or add a second account of it.
If the transaction fails, neither write lands and the lease can expire for a
safe retry.

Cancelling a running job writes its `cancelled` attempt in the same transaction
as the terminal state. On the node running the current lease, the pool snapshots
any counts it has reported so far; a cancellation issued elsewhere, or one
whose local worker holds an older lease token, keeps those fields unavailable.

History is read newest first with a tenant-bound opaque cursor. A page returns
at most 500 attempts and scans at most 10,000 rows, even with a sparse type
filter. If the scan bound is reached, `has_more` and `next_cursor` allow the
operator to continue through older rows, including a page with no matches.

## Failure semantics

| Situation | What happens |
|---|---|
| Handler returns an error, attempts left | `Retry`, back in the pending partition after a jittered backoff, checkpoint kept, error recorded |
| Handler returns an error, no attempts left | `Failed`, terminal, error recorded |
| Handler panics | Treated as an error. The pool survives; the job records the panic |
| Handler's type is not registered | The job fails visibly rather than looking done — the shape of an upgrade that removed a handler while rows of that type were queued |
| Lease lapses, attempts left | Reclaimed to `Pending` with a **new** fencing token |
| Lease lapses, no attempts left | `Failed`, naming the attempt and that the worker stopped without completing or failing it |
| Stale worker acts on a reclaimed job | `errs.Conflict`, for every one of complete, fail, renew, save and release |
| Process shuts down mid-handler | Drain window, then `Release`: back to `Pending`, attempt refunded, checkpoint kept |
| Job cancelled while running | The queue and one `Cancelled` attempt commit together; the worker learns at its next renewal and stops without adding a duplicate |
| A poll or a scheduler tick fails | Logged and retried on the next tick. Stopping would turn a transient read error into an outage of every background workload at once |
| A row that does not decode | `errs.Corruption` naming the key. Never a zero value: a job silently reset would be re-run from the start with no attempt count |

## The built-in types

| Type | What it does | Attempts | Recurring |
|---|---|---|---|
| `vector.rebuild` | rebuild the approximate index from the canonical vectors | 2 | no |
| `text.rebuild` | rebuild the keyword index from the record bodies | 2 | no |
| `graph.rebuild_in` | rebuild the derived in-edge index from the canonical out-edges | 2 | no |
| `jobs.reap` | remove finished job rows past the retention | 2 | every `retention / 4` |

The rebuilds get two attempts rather than six: a rebuild that failed twice has
met something a third pass will meet as well, and it is expensive enough that
leaving it `Failed` where an operator can see it beats four more runs.

All four are idempotent by construction. The three rebuilds read canonical rows
and write only derived ones, so running one twice produces the same index as
running it once — Invariant 3 in operational form. The reaper deletes rows
already past their retention, and deleting an absent row is not an error.

**They live in `internal/server`.** Each is a closure two lines long over a
`Rebuild` method that already exists. In `internal/jobs` they would make the job
framework import every index in the system, and `internal/discovery` — which
imports `jobs` to enqueue — would close the cycle. In `internal/vector`,
`internal/text` and `internal/graph` they would make three index packages depend
on the job framework to gain nothing.

Phase 11's `discovery.similar` is the second exception after the lifecycle pass:
real domain logic, living in `internal/discovery`, importing `internal/jobs` to
be a handler. The boundary guard now names `internal/discovery` among the
packages `internal/jobs` may not import, and it was verified to catch the
violation before the package existed.

## Self-repair

Two warnings that Phases 7 and 8 could only log now enqueue durable jobs as
well: `internal/server`'s "the vector index needs rebuilding" and
`internal/query`'s "a keyword search was served from an incomplete index".

**Running the repair automatically is safe**, which is worth stating because
"the server rebuilds its own indexes under load" sounds like the opposite.
Neither trigger fires unless the tenant is *already* degraded — the vector index
reports damage it has already worked around, and the text marker means an
earlier rebuild was interrupted. The rebuild does not cause the degradation; it
ends it. Declining to schedule it would leave a tenant answering incompletely
until somebody read a log line.

**It does not cost the search that discovered the damage.** The request is
single-flighted per tenant and type in memory, so the read path pays one map
lookup, and a goroutine the server owns does the durable check and the write.
Dropping a duplicate request there is safe and is not the bounded-channel defect
this framework replaces: the request is "repair this tenant", it is idempotent,
and the next search raises it again. What is never dropped is work somebody
asked for.

`remem-admin vector rebuild` and `remem-admin text rebuild` stay. A stopped
server still needs a repair path, and a job queue is not reachable with the
process down.

## Administration

Five routes under `/api/v1/admin`, each needing a credential that is **not bound
to one tenant** and acting on the tenant the request resolved to — so an
operator names it with `X-Remem-Tenant` exactly as everywhere else. There is no
"all tenants" listing, for the reason Invariant 1 exists: once an unscoped
variant is available, something calls it.

| Route | Answer |
|---|---|
| `GET /admin/jobs` | one tenant's jobs, `state` and `type` repeatable, waiting first |
| `GET /admin/jobs/types` | what this binary can run, with descriptions |
| `GET /admin/jobs/{id}` | one job |
| `POST /admin/jobs/{id}/cancel` | 200, or 409 if it already finished |
| `POST /admin/jobs/{type}/run` | 202 with the queued job, 409 naming the one already outstanding |
| `POST /admin/jobs/{type}/pause` | 200 with the pause, 422 on an expiry that is missing, past or too long |
| `POST /admin/jobs/{type}/resume` | 200, and 200 again for a type that was not paused |
| `GET /admin/jobs/history` | retained attempts, newest first, `type` repeatable, with the retention window and an opaque `cursor`/`has_more` when another page exists |

`run` is what `remem-admin` cannot be: the CLI needs the data directory, and the
directory is held under Pebble's exclusive lock while the server runs. It
answers **202 and not 201**: the job is queued, not done, and for a rebuild the
work is minutes away. A type with one already outstanding is a 409 that **names
that job and its state**, because "something of this type is already
outstanding" otherwise sends an operator to a listing to find out which.

A pause takes `expires_at` (RFC 3339) or `duration_seconds`, and refuses both
together: two answers to one deadline is a request whose author did not decide,
which is the class of mistake `config.Load` already refuses by name. A refused
pause writes nothing — a half-written suppression suppresses, and nothing
reports it.

**`GET /admin/jobs/types` is the pause status surface.** A pause is a fact about
a registered type in a tenant, so it is reported where that type is described,
rather than at a route somebody has to know to ask. An expired pause reports
neither `paused` nor an expiry, because it is not one.

A resume of a type that is not paused is a 200. The post-condition the operator
asked for — this type is scheduled again — holds either way, and a pause that
expired while the console was open must not answer the resume button with an
error.

**History discloses the window that bounds it.** Without it, an empty page and a
short retention are indistinguishable, and a client cannot tell "nothing has run"
from "we do not keep it that long".

**The payload and the checkpoint are reported as byte counts, never as
content.** A payload is handler-defined and a checkpoint is a cursor into
somebody's corpus; the rule that memory content stays out of logs is worth
keeping in the surfaces next to them, and a byte count answers the question an
operator actually has.

A malformed `state` or `type` filter is refused by name rather than matching
nothing: an empty page is exactly what a working filter over an empty queue
looks like, and the two must not be confusable. A process without the framework
answers 503 with a sentence, not 404 — the difference between "this build does
not run jobs" and "you typed the URL wrong" is one an operator needs.

## Metrics

Spec §50's list, under `remem_jobs_*`: pending, running, failures, retries and
processing time, labelled by tenant and type.

Pending and running are gauges refreshed on their own 30-second ticker, over
their own rotation cursor, so the gauge pass and the dispatcher do not drag each
other's position around. **A label set that has emptied is set back to zero**
rather than left alone: a Prometheus gauge nothing writes keeps its last reading
for ever, so a backlog that cleared would go on being reported as a backlog —
which is how an operator learns to stop trusting the alert. Removing the zeroing
loop makes `TestTheQueueDepthGaugesFollowTheQueue` report three pending jobs
over an empty queue.

Failures and retries are counted apart because they are different operational
facts: retries say the queue is working through something, failures say it gave
up. One counter for both would make a healthy retry loop look like an outage.

## Configuration

| Setting | Default | What it decides |
|---|---|---|
| `jobs.workers` | 4 | handlers running at once in this process |
| `jobs.lease_duration` | 1m | how long a claim lasts; renewed at a third of it |
| `jobs.max_retries` | 5 | retries, so a job runs at most six times |
| `jobs.poll_interval` | 1s | how often the dispatcher looks for work |
| `jobs.retention` | 24h | how long a finished row, an attempt's history and an expired pause are kept before reaping |
| `jobs.drain_timeout` | 15s | how long a shutdown waits for handlers to checkpoint |

Two bounds are constants rather than settings, for the reason the tokeniser's
are. `jobs.MaxPauseDuration` (seven days) decides how long repair may be
missing before nobody remembers it was — an operator who can raise it has a dial
that turns a bounded suppression back into a permanent one, which is the thing
the expiry exists to prevent. `jobs.MaxRunListLimit` (500) bounds a history
page, and a deeper page is a question about a corpus rather than about a job.

`jobs.queue_capacity` is **retired** and refused by name. It bounded an
in-memory channel; a durable queue's capacity is the disk, and the bounded
channel that silently dropped its overflow is exactly what this framework
replaces. Keeping the name would leave an operator with a dial that cannot do
anything, which is the failure the three retired tokeniser settings exist to
warn about.

## Durable formats

**The job row value**, `proto/job/v1/job.proto` → `internal/jobs/pb`. Protobuf,
unframed: the key's space byte already says what the value is, where the record
envelope exists so a *body encoding* can be replaced under a value several
codecs share. Field numbers are never reused.

The tenant, the job id and the partition are **in the key and not in the value**
— a second copy is a second thing that can disagree with the first. The
namespace the job *acts on* is in the body, because that is the handler's input
rather than the row's address.

It is **not** a fifth independently versioned durable format. Protobuf's
field-number evolution carries it forward, which is the footing the tenant
directory and the session registry are already on; the four versioned formats
are the ones whose meaning cannot be evolved that way.

**The pause row and the attempt row**, `job.v1.Pause` and `job.v1.Run` in the
same file and on the same footing. A pause is addressed by the type name it
suppresses, which is the whole remainder of its key and therefore needs no
length prefix: nothing follows it. An attempt is addressed by when it finished
and then by its own id, so the reaper walks the oldest first and a history page
reads the newest backwards — the only two things that read that space.

Both rows carry a copy of what their key already says, and both are checked
against it on the way back: a pause stored under one type name whose body claims
another was not written by this code, and reading either over the other would
suppress a type nobody paused.

**Job type names** are durable: a name is written into every row of that type
and read back by a registry lookup. `[a-z0-9_.]`, 1–64 bytes. A retired type
name stays retired.

## Priority, stated because the field is on disk and does almost nothing

`Priority` is persisted and it orders **the jobs within one claim batch** —
nothing more. It cannot order the queue, because the key is ordered by due time
and priority is not in it. Saying so here is cheaper than an operator
discovering it from behaviour. Spec §23 lists priority as "if eventually
required"; making the key order by it is that eventual work, and it is a durable
key change.

## What the end-to-end run found

Task 9.14 drove the whole framework by hand against the real binary, the real
model and a real Pebble directory. It earned its keep three times, and none of
the three was reachable from a unit test.

**A second name for one setting, resolved by luck.** The run exported
`REMEM_SERVER_API_KEY` and the server then refused its own operator credential:
"the presented credential is not one this server issued". A `REMEM_API_KEY` left
over in the shell — a legacy Rust name that `legacyEnv` maps to the same setting
— had overwritten it, because `readEnv` writes both into one map and
`os.Environ()` has no defined order. Both variables were plainly visible in the
environment and nothing said anything. `config.Load` now refuses when two names
set one setting to different values, naming both; identical values are accepted,
because a deployment that set the belt and the braces is exactly what the legacy
names exist to protect. It is the package's own doctrine applied to itself: a
setting an operator believes is in force and is not is the thing this
configuration system exists to prevent.

**A refusal about the wrong subject.** A tenant-bound credential asking about
background jobs was told "administering *tenants* requires a credential that is
not bound to one tenant" — a sentence that sends somebody to look at the wrong
thing. `crossTenant` was written for the tenant routes and reused by the jobs
routes, which is the ordinary way a helper's message goes stale. It now names
what the caller asked for.

**A warning about a job that had just succeeded.** The shutdown log of a
successful reaper carried `a lease could not be renewed … holds no lease to
renew`. The renewal pass takes a snapshot of what is running and then renews
each one; a handler that finishes in between leaves a job whose completion has
already dropped its lease. Nothing was damaged — and that is the point. One
warning per job that finishes near a renewal tick is how an operator learns that
this server's warnings do not mean anything, which is the same failure as a
gauge that never returns to zero. The pass now skips a job whose lease is gone,
inside the running job's mutex so the check is exact rather than hopeful, and
`renewAll` reports what it did so the case is testable.

What the run confirmed: the pool starting and naming its owner; the registry
answering over HTTP with all four types and their descriptions; a job queued
through `run`, claimed, completed and visible in the audit trail within a
second; a second `run` of an outstanding type refused with 409; a tenant-bound
credential refused with 403 on every route; a job for a second tenant queued by
header and invisible to the first tenant's listing; **the whole self-repair loop
— half a tenant's HNSW node records overwritten with rubbish, a search answering
correctly and reporting `truncated: true`, the warning logged, a durable
`vector.rebuild` scheduled, run and completed, and the next search reporting
`truncated: false`** — with no second repair queued for the same damage; a job
queued and then `kill -9` before it ran, still there and completed after the
restart; cancellation of a pending job and a 409 on cancelling it again; the
scheduler fanning out one reaper per tenant on its tick; each reaper removing
only its own tenant's rows; `remem_jobs_*` present in `/metrics` with the
running gauge returning to zero on the pass after the job finished; and a
graceful SIGTERM stopping the pool before the process.

## Plan corrections

Four, each decided against the plan's own tests or its own package layout.

**A. `Claim` takes an explicit tenant.** The plan's signature is
`Claim(ctx, owner, types, n)`. There is no key range spanning tenants ordered by
due time — the tenant leads every key (§II.2) — so a cross-tenant claim would be
a `tenant.ForEach` fan-out wearing the costume of one scan, with Invariant 1
holding by convention inside it rather than by signature. The fan-out lives in
the dispatcher, which is allowed to walk tenants. `TestJobsAreTenantScoped`
becomes structural rather than aspirational.

**B. `Queue` is a concrete type, not an interface.** The plan lists it among the
interfaces with a mandatory contract suite. Phase 8 settled the rule for
`text.Index` and it applies here: a suite over a single implementation
duplicates that package's unit tests, and there is exactly one queue — over
`storage.KV`, which is the interface that genuinely has two implementations and
already has its suite. Spec §24's "abstractions compatible with future
distributed execution" is satisfied by the *data model*, which is where the plan
itself puts it. Extract an interface and a suite when the second implementation
arrives.

**C. The built-in handlers are registered by the composition root**, not by
`internal/jobs` and not by the packages they rebuild. The reasoning is above.

**D. There is a fourth built-in type: `jobs.reap`.** The plan registers three
rebuilds and describes a scheduler with nothing to schedule. Terminal rows are
retained for audit and something has to remove them; a recurring per-tenant
reaper is that something, and it gives the scheduler's fan-out a real user in
this phase rather than in Phase 10.

## What is not here

**No distributed leases.** Spec §24 is explicit: do not implement them until
multi-node work begins. The owner and the fencing token are the shape that makes
them additive later.

**No durable schedule state.** See the scheduler, above.

**No priority ordering of the queue.** See priority, above.

**No cross-tenant listing.** Invariant 1.

**No job dependencies, no fan-out/fan-in, no workflow.** Nothing in the product
needs a job to wait for another one, and a dependency graph is a durable format
that would have to be right the first time.

**No permanent pause.** Every pause carries an expiry, bounded at a week. The
thing an operator reaches for a permanent pause to do — stop this subsystem —
is `lifecycle.enabled`, `discovery.enabled` and their neighbours, which are
visible on every start rather than in a row somebody wrote once.

**No pause that stops a running job.** That is
`POST /admin/jobs/{id}/cancel`, which names the job.

**No global pause across tenants.** Invariant 1, and the same reasoning as the
missing cross-tenant listing: once an unscoped variant exists, something calls
it.

**No count the framework derived for a handler.** A count nobody reported stays
unavailable. Deriving one from a checkpoint, an elapsed time or the size of an
index afterwards would be a number that looks exact and is not, which is worse
than no number because an operator acts on it.

**No history of a job's *total* effect across attempts.** Each row is one
attempt. Delivery is at-least-once, so a sum over attempts is not a count of
distinct records, and presenting one would be arithmetic the data does not
support.
