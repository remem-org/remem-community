package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// DefaultListLimit is how many jobs a listing returns when the caller does not
// say.
const DefaultListLimit = 50

// MaxListLimit bounds a listing.
const MaxListLimit = 500

// Filter narrows a listing.
type Filter struct {
	// States restricts the listing. Empty means every state.
	States []State
	// Types restricts the listing. Empty means every type.
	Types []Type
	// Limit bounds the result. Zero takes DefaultListLimit.
	Limit int
}

func (f Filter) wants(j *Job) bool {
	if !wanted(j.Type, f.Types) {
		return false
	}
	if len(f.States) == 0 {
		return true
	}
	for _, s := range f.States {
		if s == j.State {
			return true
		}
	}
	return false
}

// partitions is which key partitions a filter has to read.
//
// A listing restricted to running jobs should not walk the audit trail, and
// this is what keeps that true: the partition a state lives in is the scan it
// selects.
func (f Filter) partitions() []keys.JobState {
	if len(f.States) == 0 {
		return keys.AllJobStates()
	}
	seen := map[keys.JobState]bool{}
	var out []keys.JobState
	for _, part := range keys.AllJobStates() {
		for _, s := range f.States {
			if s.Partition() == part && !seen[part] {
				seen[part] = true
				out = append(out, part)
			}
		}
	}
	return out
}

// List returns one tenant's jobs, pending first and finished last.
//
// The order is the partition order, which is also the order an operator wants:
// what is waiting, what is running, what happened. Within a partition it is due
// order, so the oldest waiting job and the most recently finished one are each
// at the end an operator looks for them at.
func (q *Queue) List(ctx context.Context, t tenant.ID, f Filter) ([]*Job, error) {
	const op = "jobs.List"

	if t == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"listing jobs requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	limit := f.Limit
	switch {
	case limit <= 0:
		limit = DefaultListLimit
	case limit > MaxListLimit:
		limit = MaxListLimit
	}

	out := make([]*Job, 0, limit)
	for _, part := range f.partitions() {
		if len(out) == limit {
			break
		}
		lower, upper := keys.JobStateRange(t, tenant.DefaultNamespace, part)
		it := q.kv.NewIterator(lower, upper)
		for ok := it.First(); ok; ok = it.Next() {
			j, err := Unmarshal(append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...))
			if err != nil {
				_ = it.Close()
				return nil, err
			}
			if !f.wants(j) {
				continue
			}
			out = append(out, j)
			if len(out) == limit {
				break
			}
		}
		err := it.Error()
		if closeErr := it.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Get returns one job by id.
//
// # Why this is a scan
//
// The key is ordered by partition and due time, so a job cannot be addressed by
// its id alone. The alternative is a second row per job mapping id to its
// current key, which every transition would have to write and delete — roughly
// a third more write amplification on the queue's hottest path, to make a rare
// administrative lookup O(1).
//
// The cost is paid here instead, and it is bounded: one tenant's queue depth
// plus its unreaped terminal rows, which the reaper keeps in hand. Nothing on
// the claim, completion or checkpoint path comes through here — each of those
// already holds the job it is acting on, and therefore its key.
func (q *Queue) Get(ctx context.Context, t tenant.ID, jid id.ID) (*Job, error) {
	const op = "jobs.Get"

	if t == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"reading a job requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	for _, part := range keys.AllJobStates() {
		lower, upper := keys.JobStateRange(t, tenant.DefaultNamespace, part)
		it := q.kv.NewIterator(lower, upper)
		var found *Job
		for ok := it.First(); ok && found == nil; ok = it.Next() {
			_, _, _, _, got, err := keys.ParseJob(it.Key())
			if err != nil {
				_ = it.Close()
				return nil, err
			}
			if got != jid {
				continue
			}
			found, err = Unmarshal(append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...))
			if err != nil {
				_ = it.Close()
				return nil, err
			}
		}
		err := it.Error()
		if closeErr := it.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
		if found != nil {
			return found, nil
		}
	}
	return nil, errs.E(errs.NotFound, op, fmt.Errorf("no job %s in tenant %s", jid, t))
}

// Cancel stops a job.
//
// A job that has not started is cancelled outright. A job that is running is
// cancelled where it stands: its row moves to the terminal partition, and the
// worker holding it discovers that at its next lease renewal — which re-reads
// the row anyway, so cooperative cancellation costs no extra read. The cancel
// transaction records the attempt; the handler then has its context cancelled
// and cannot add a duplicate outcome.
//
// A job that has already finished is refused by name rather than silently
// accepted: "cancelled" and "completed twenty minutes ago" are different
// answers and an operator is entitled to the difference.
func (q *Queue) Cancel(ctx context.Context, t tenant.ID, jid id.ID) error {
	return q.cancelWithCounts(ctx, t, jid, id.Zero, nil, nil)
}

// cancelWithCounts records handler totals only when the local lease token
// still matches the current claim. A stale local worker may not describe a
// newer attempt of the same job.
func (q *Queue) cancelWithCounts(ctx context.Context, t tenant.ID, jid id.ID,
	localLease id.ID, processed, changed *uint64,
) error {
	const op = "jobs.Cancel"

	j, err := q.Get(ctx, t, jid)
	if err != nil {
		return err
	}
	if j.State.Terminal() {
		return errs.E(errs.Conflict, op, fmt.Errorf(
			"job %s already finished as %s and cannot be cancelled", jid, j.State))
	}

	now := q.clk.Now().UTC()
	var run *Run
	if j.State == Running && j.Lease != nil {
		if !localLease.IsZero() && j.Lease.Token != localLease {
			processed, changed = nil, nil
		}
		started := j.Lease.Token.Time()
		if started.IsZero() || started.After(now) {
			started = j.UpdatedAt
		}
		run = &Run{
			ID: id.New(), JobID: j.ID, Tenant: j.Tenant, Namespace: j.Namespace,
			Type: j.Type, Attempt: j.Attempts, StartedAt: started.UTC(),
			FinishedAt: now, Outcome: Cancelled, Error: "cancelled by an operator",
			Owner:            j.Lease.Owner,
			RecordsProcessed: processed, RecordsChanged: changed,
		}
	}
	return q.transitionWithRun(ctx, j, func(next *Job) {
		next.State = Cancelled
		next.Lease = nil
		next.UpdatedAt = now
		next.LastError = "cancelled by an operator"
	}, run)
}

// Reap removes terminal job rows that finished before the cutoff, up to limit,
// and reports how many it removed.
//
// The audit trail is what keeps "what happened to my rebuild" answerable after
// the fact, and it is also what makes [Get] a scan. Retention is the one
// setting that keeps both true: rows stay long enough to be read and not long
// enough to accumulate. The done partition is ordered by finish time, so this
// is a forward scan that stops at the first row still inside the retention.
func (q *Queue) Reap(ctx context.Context, t tenant.ID, before time.Time, limit int) (int, error) {
	const op = "jobs.Reap"

	if t == "" {
		return 0, errs.E(errs.Invalid, op, errors.New(
			"reaping requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	if limit <= 0 {
		return 0, nil
	}

	unlock := q.lock(t)
	defer unlock()

	stale, err := q.due(ctx, t, keys.JobDone, before, nil, limit)
	if err != nil {
		return 0, err
	}
	if len(stale) == 0 {
		return 0, nil
	}

	tx := txn.New(q.kv, txn.Sync(q.sync))
	defer tx.Close()
	for _, c := range stale {
		// Conditional, like every other move: a row somebody rewrote between
		// the scan and the commit is not this reaper's to delete.
		tx.Expect(c.key, c.value, true)
		tx.Delete(c.key)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(stale), nil
}

// Counts is how much work one tenant has outstanding, by type.
type Counts struct {
	// Pending counts what is waiting, including what is waiting to retry.
	Pending map[Type]int
	// Running counts what is leased to a worker.
	Running map[Type]int
	// Capped reports that the scan stopped at its bound, so the numbers are
	// floors rather than totals. A gauge that quietly under-reports is worse
	// than one that says it gave up.
	Capped bool
}

// countScanCap bounds a metrics refresh.
//
// A queue deeper than this is a queue with a problem the exact number will not
// help with, and walking it every refresh would make the metric the thing
// keeping the store busy.
const countScanCap = 100_000

// Counts reports what one tenant has outstanding.
//
// It reads only the live partitions. The gauges answer "is the queue keeping
// up", which is a question about work outstanding — counting the audit trail as
// well would make a metrics refresh proportional to everything that has ever
// run.
func (q *Queue) Counts(ctx context.Context, t tenant.ID) (Counts, error) {
	const op = "jobs.Counts"
	if t == "" {
		return Counts{}, errs.E(errs.Invalid, op, errors.New(
			"counting jobs requires a tenant: there is no unscoped read path (Invariant 1)"))
	}

	out := Counts{Pending: map[Type]int{}, Running: map[Type]int{}}
	seen := 0
	for _, part := range []keys.JobState{keys.JobPending, keys.JobRunning} {
		lower, upper := keys.JobStateRange(t, tenant.DefaultNamespace, part)
		it := q.kv.NewIterator(lower, upper)
		for ok := it.First(); ok; ok = it.Next() {
			if seen++; seen > countScanCap {
				out.Capped = true
				break
			}
			j, err := Unmarshal(append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...))
			if err != nil {
				_ = it.Close()
				return Counts{}, err
			}
			if part == keys.JobPending {
				out.Pending[j.Type]++
			} else {
				out.Running[j.Type]++
			}
		}
		err := it.Error()
		if closeErr := it.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return Counts{}, err
		}
	}
	return out, nil
}
