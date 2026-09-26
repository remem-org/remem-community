package jobs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// DefaultLease is how long a claim lasts before it may be reclaimed.
//
// A minute is long enough that an ordinary handler never has to renew twice and
// short enough that a process killed mid-job has its work back within a minute.
const DefaultLease = time.Minute

// claimScanCap bounds how many pending rows one claim examines.
//
// The pending range is already bounded to work that is due, so in a healthy
// queue the scan ends long before this. It matters when a type filter excludes
// most of a backlog: without a cap, a pool that handles one type would walk
// every due job of every other type on every poll.
const claimScanCap = 1024

// Queue is the durable job queue.
//
// # Why it is a concrete type
//
// The plan lists it among the interfaces needing a contract suite. There is one
// implementation and there is no second one coming: the queue is a queue over
// storage.KV, and storage.KV is the interface with two implementations and its
// own suite. Phase 8 settled the same question for text.Index — a suite over a
// single implementor duplicates that package's unit tests.
//
// Spec §24's "abstractions compatible with future distributed execution" is
// satisfied by the data model instead, which is where the plan itself puts it:
// a lease carries an owner and a fencing token from the first commit, so
// cross-node claiming changes how a lease is granted and never changes a
// handler.
type Queue struct {
	kv   storage.KV
	clk  clock.Clock
	gate *txn.Gate
	sync bool

	lease time.Duration
}

// QueueOption configures a queue.
type QueueOption func(*Queue)

// WithoutGate disables the per-tenant lock.
//
// It exists for one test. Exclusivity is a property of the conditional commit,
// and the gate is a throughput optimisation over it; a guard that is never
// exercised is a guard nobody has verified, so TestClaimIsExclusive runs once
// with the gate and once without to prove which of the two is load-bearing.
func WithoutGate() QueueOption { return func(q *Queue) { q.gate = nil } }

// WithSyncWrites sets commit durability, from storage.sync_writes.
func WithSyncWrites(sync bool) QueueOption { return func(q *Queue) { q.sync = sync } }

// WithLease sets how long a claim lasts.
func WithLease(d time.Duration) QueueOption {
	return func(q *Queue) {
		if d > 0 {
			q.lease = d
		}
	}
}

// NewQueue returns a queue over kv.
func NewQueue(kv storage.KV, clk clock.Clock, opts ...QueueOption) *Queue {
	q := &Queue{kv: kv, clk: clk, gate: txn.NewGate(), sync: true, lease: DefaultLease}
	for _, o := range opts {
		o(q)
	}
	return q
}

// Lease is how long a claim lasts.
func (q *Queue) Lease() time.Duration { return q.lease }

// Enqueue stages a job into the caller's transaction.
//
// This is the shape Phase 11 needs and the reason the queue is durable at all:
// a discovery job is enqueued in the same transaction as the memory write, so a
// failed write leaves no orphan job and a successful one cannot lose its
// follow-up work. Rust dropped that work into a bounded channel and counted the
// overflow.
//
// It fills in what the caller left blank — the id, the timestamps, the state
// and the attempt bound — and does so once: called again on the same job by a
// retried transaction body, it stages the same row rather than a new one.
func (q *Queue) Enqueue(ctx context.Context, tx txn.Tx, j *Job) error {
	const op = "jobs.Enqueue"

	now := q.clk.Now().UTC()
	if j.ID == id.Zero {
		j.ID = id.New()
	}
	if j.Namespace == "" {
		j.Namespace = tenant.DefaultNamespace
	}
	if j.State == 0 {
		j.State = Pending
	}
	if j.RunAt.IsZero() {
		j.RunAt = now
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = DefaultMaxAttempts
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = now
	}
	if j.UpdatedAt.IsZero() {
		j.UpdatedAt = now
	}
	if j.State != Pending && j.State != Retry {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"a job is enqueued pending, not %s; a running or finished job is written by a transition", j.State))
	}

	key, err := j.Key()
	if err != nil {
		return err
	}
	value, err := Marshal(j)
	if err != nil {
		return err
	}

	// Absent at commit, so re-enqueuing an id that is already queued is a
	// conflict rather than a silent overwrite of somebody's attempt count.
	tx.Expect(key, nil, false)
	tx.Set(key, value)
	j.stored = key
	return nil
}

// Submit enqueues a job in a transaction of its own.
//
// It is the form for everything that is not already inside a write: the
// scheduler, an administrative run, and the index-repair triggers.
func (q *Queue) Submit(ctx context.Context, j *Job) error {
	if j.Tenant == "" {
		return errs.E(errs.Invalid, "jobs.Submit",
			errors.New("a job requires a tenant (Invariant 1)"))
	}
	unlock := q.lock(j.Tenant)
	defer unlock()

	tx := txn.New(q.kv, txn.Sync(q.sync))
	defer tx.Close()

	if err := q.Enqueue(ctx, tx, j); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Claim moves up to n due jobs into the running state and leases them to owner.
//
// # The tenant is a parameter, and that is a correction to the plan
//
// The plan's signature has no tenant. There is no key range that spans tenants
// ordered by due time — the tenant leads every key (§II.2) — so a cross-tenant
// claim would be a fan-out over tenant.ForEach wearing the costume of one scan,
// with Invariant 1 holding by convention inside it rather than by signature.
// The fan-out lives in the dispatcher, which is allowed to walk tenants.
//
// # Exclusivity
//
// Each job moves in its own transaction, conditional on the pending row still
// being exactly as it was read. Two claimers that pick the same job produce one
// winner and one errs.Conflict, and the loser simply moves to the next
// candidate — a conflict here means somebody else is doing the work, which is
// not a failure to report. The per-tenant gate above removes the contention
// rather than absorbing it; it is not what makes this correct.
func (q *Queue) Claim(ctx context.Context, t tenant.ID, owner string, types []Type, n int) ([]*Job, error) {
	const op = "jobs.Claim"

	if t == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"a claim requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	if owner == "" {
		return nil, errs.E(errs.Invalid, op, errors.New("a claim requires an owner to lease to"))
	}
	if n <= 0 {
		return nil, nil
	}

	unlock := q.lock(t)
	defer unlock()

	now := q.clk.Now().UTC()
	candidates, err := q.due(ctx, t, keys.JobPending, now, types, n)
	if err != nil {
		return nil, err
	}

	// Priority orders one batch. The scan is in due order, so a stable sort by
	// priority keeps the oldest first within each priority.
	sort.SliceStable(candidates, func(a, b int) bool {
		return candidates[a].job.Priority > candidates[b].job.Priority
	})

	out := make([]*Job, 0, n)
	for _, c := range candidates {
		if len(out) == n {
			break
		}
		leased := c.job.Clone()
		leased.State = Running
		leased.Attempts++
		leased.UpdatedAt = now
		leased.Lease = &Lease{
			Owner:     owner,
			ExpiresAt: now.Add(q.lease),
			Token:     id.New(),
		}
		if err := q.move(ctx, c.key, c.value, leased); err != nil {
			if errs.Is(err, errs.Conflict) {
				continue // somebody else took it, which is not a failure
			}
			return out, err
		}
		out = append(out, leased)
	}
	return out, nil
}

// Complete records that the handler returned successfully.
func (q *Queue) Complete(ctx context.Context, j *Job) error {
	return q.finish(ctx, j, Completed, "")
}

// candidate is one row the scan is considering, with the bytes a conditional
// move needs.
type candidate struct {
	key   []byte
	value []byte
	job   *Job
}

// due scans one partition for rows at or before the bound.
//
// It is the shared shape of all three of the queue's scans: claiming reads the
// pending partition due now, reclamation reads the running partition expired
// now, and reaping reads the done partition finished before the retention.
func (q *Queue) due(ctx context.Context, t tenant.ID, part keys.JobState,
	bound time.Time, types []Type, want int,
) ([]candidate, error) {
	lower, upper := keys.JobDueRange(t, tenant.DefaultNamespace, part, unixMilli(bound))
	it := q.kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	var out []candidate
	examined := 0
	for ok := it.First(); ok; ok = it.Next() {
		if examined++; examined > claimScanCap {
			break
		}
		key := append([]byte(nil), it.Key()...)
		value := append([]byte(nil), it.Value()...)
		j, err := Unmarshal(key, value)
		if err != nil {
			return nil, err
		}
		if !wanted(j.Type, types) {
			continue
		}
		out = append(out, candidate{key: key, value: value, job: j})
		if want > 0 && len(out) == want {
			break
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func wanted(typ Type, types []Type) bool {
	if len(types) == 0 {
		return true
	}
	for _, w := range types {
		if w == typ {
			return true
		}
	}
	return false
}

// move rewrites a job at a new key, conditional on the old row being untouched.
//
// The delete and the set are one transaction, which is what makes "a job is in
// exactly one partition" true rather than nearly true: a crash between them
// would otherwise leave a job that is both claimable and running, or neither.
func (q *Queue) move(ctx context.Context, oldKey, oldValue []byte, next *Job) error {
	newKey, err := next.Key()
	if err != nil {
		return err
	}
	newValue, err := Marshal(next)
	if err != nil {
		return err
	}

	tx := txn.New(q.kv, txn.Sync(q.sync))
	defer tx.Close()

	tx.Expect(oldKey, oldValue, true)
	tx.Delete(oldKey)
	tx.Set(newKey, newValue)
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	next.stored = newKey
	return nil
}

// lock takes the per-tenant gate, or does nothing when it is disabled.
func (q *Queue) lock(t tenant.ID) func() {
	if q.gate == nil {
		return func() {}
	}
	return q.gate.Lock(string(t))
}
