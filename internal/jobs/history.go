package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs/pb"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"google.golang.org/protobuf/proto"
)

// DefaultRunListLimit is how many attempts a history page returns when the
// caller does not say.
const DefaultRunListLimit = 50

// MaxRunListLimit bounds a history page.
//
// History is the one job surface whose row count grows with time rather than
// with work outstanding, so it is the one that most needs a bound. A page
// larger than this is a question about a corpus rather than about a job.
const MaxRunListLimit = 500

// Run is one execution attempt, retained for the configured job retention.
//
// # One row per attempt, and not a total across them
//
// Delivery is at-least-once, so a job that runs three times has visited some
// records three times. Summing these counts across a job's attempts therefore
// does not produce a count of distinct records and must not be presented as
// one — each row describes what one attempt did, which is the question an
// operator looking at a retry actually has.
type Run struct {
	// ID addresses this attempt. It is drawn when the attempt ends, outside
	// the transaction that stores it (Invariant 9).
	ID id.ID
	// JobID is the job this attempt belongs to. Several runs share one, and
	// the job row it names may already have been reaped.
	JobID     id.ID
	Tenant    tenant.ID
	Namespace tenant.Namespace
	Type      Type

	// Attempt is the run number the queue charged, counting from one.
	Attempt uint32

	StartedAt  time.Time
	FinishedAt time.Time

	// Outcome is the logical state the attempt ended in: Completed, Retry,
	// Failed or Cancelled. It is the attempt's outcome and not the job's — a
	// Retry row is an attempt that failed with attempts left.
	Outcome State
	// Error is why, empty on success.
	Error string
	// Owner is the node that ran it.
	Owner string

	// RecordsProcessed and RecordsChanged are what the handler reported for
	// this attempt, and are nil when it reported nothing. Nil is not zero, and
	// nothing may fill one in from a checkpoint, an elapsed time or the size
	// of an index afterwards.
	RecordsProcessed *uint64
	RecordsChanged   *uint64
}

// Duration is how long the attempt took.
func (r *Run) Duration() time.Duration {
	if r.StartedAt.IsZero() || r.FinishedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) {
		return 0
	}
	return r.FinishedAt.Sub(r.StartedAt)
}

// RunFilter narrows a history page.
type RunFilter struct {
	// Types restricts the page. Empty means every type.
	Types []Type
	// Limit bounds it. Zero takes DefaultRunListLimit.
	Limit int
	// BeforeKey resumes strictly before this run key. It is validated against
	// the requested tenant before it is used as an iterator bound.
	BeforeKey []byte
}

// RunPage is one bounded slice of the retained history.
type RunPage struct {
	Runs    []*Run
	NextKey []byte
	HasMore bool
}

// RecordRun stores one standalone attempt row. Worker outcomes use
// transitionWithRun so their history commits atomically with queue state; this
// method remains for administrative import and callers recording history that
// has no corresponding live queue transition.
func (q *Queue) RecordRun(ctx context.Context, r *Run) error {
	const op = "jobs.RecordRun"

	if r.Tenant == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"an attempt belongs to a tenant (Invariant 1)"))
	}
	if r.ID == id.Zero {
		r.ID = id.New()
	}
	if r.Namespace == "" {
		r.Namespace = tenant.DefaultNamespace
	}
	if r.FinishedAt.IsZero() {
		r.FinishedAt = q.clk.Now().UTC()
	}
	key := keys.JobRun(r.Tenant, tenant.DefaultNamespace, unixMilli(r.FinishedAt), r.ID)
	value, err := marshalRun(r)
	if err != nil {
		return err
	}

	unlock := q.lock(r.Tenant)
	defer unlock()

	tx := txn.New(q.kv, txn.Sync(q.sync))
	defer tx.Close()
	// Absent at commit. The id is fresh, so a collision is not an expected
	// event — which is exactly why it must not silently overwrite an attempt
	// somebody else recorded.
	tx.Expect(key, nil, false)
	tx.Set(key, value)
	return tx.Commit(ctx)
}

// RunsPage returns one bounded page of retained attempts, newest first.
//
// # Why it reads backwards
//
// The key is ordered by finish time, and an operator opening a history wants
// the last thing that happened. Reading the range forwards and reversing it
// would mean walking every retained attempt to answer a question about the
// most recent ten.
//
// A type filter is applied during the walk rather than through a second index.
// The range is already bounded by the retention, and a history keyed by type
// would be a second ordering to keep in step with the first for a scan that is
// bounded either way.
func (q *Queue) RunsPage(ctx context.Context, t tenant.ID, f RunFilter) (RunPage, error) {
	const op = "jobs.Runs"

	if t == "" {
		return RunPage{}, errs.E(errs.Invalid, op, errors.New(
			"reading job history requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	limit := f.Limit
	switch {
	case limit <= 0:
		limit = DefaultRunListLimit
	case limit > MaxRunListLimit:
		limit = MaxRunListLimit
	}

	lower, upper := keys.JobRunRange(t, tenant.DefaultNamespace)
	if len(f.BeforeKey) > 0 {
		cursorTenant, cursorNS, _, _, err := keys.ParseJobRun(f.BeforeKey)
		if err != nil || cursorTenant != t || cursorNS != tenant.DefaultNamespace {
			return RunPage{}, errs.E(errs.Invalid, op, errors.New("history cursor does not belong to this tenant"))
		}
		upper = f.BeforeKey
	}
	it := q.kv.NewIterator(lower, upper)

	page := RunPage{Runs: make([]*Run, 0, limit)}
	examined := 0
	var lastScanned, lastReturned []byte
	for ok := it.Last(); ok; ok = it.Prev() {
		if examined >= runScanCap {
			page.HasMore = true
			page.NextKey = lastScanned
			break
		}
		examined++
		key := append([]byte(nil), it.Key()...)
		r, err := unmarshalRun(append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...))
		if err != nil {
			_ = it.Close()
			return RunPage{}, err
		}
		if !wanted(r.Type, f.Types) {
			lastScanned = key
			continue
		}
		if len(page.Runs) == limit {
			page.HasMore = true
			page.NextKey = lastReturned
			break
		}
		page.Runs = append(page.Runs, r)
		lastReturned = key
		lastScanned = key
	}
	err := it.Error()
	if closeErr := it.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return RunPage{}, err
	}
	return page, nil
}

// runScanCap bounds one history page's walk.
const runScanCap = 10_000

// ReapRuns removes attempts that finished before the cutoff, up to limit, and
// reports how many it removed.
//
// It is the same shape as [Queue.Reap] over the same retention, and for the
// same reason: the rows stay long enough to be read and not long enough to
// accumulate. The range is ordered by finish time, so this is a forward scan
// that stops at the first attempt still inside the window.
func (q *Queue) ReapRuns(ctx context.Context, t tenant.ID, before time.Time, limit int) (int, error) {
	const op = "jobs.ReapRuns"

	if t == "" {
		return 0, errs.E(errs.Invalid, op, errors.New(
			"reaping requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	if limit <= 0 {
		return 0, nil
	}

	unlock := q.lock(t)
	defer unlock()

	lower, upper := keys.JobRunBeforeRange(t, tenant.DefaultNamespace, unixMilli(before))
	it := q.kv.NewIterator(lower, upper)
	type row struct{ key, value []byte }
	var stale []row
	for ok := it.First(); ok && len(stale) < limit; ok = it.Next() {
		stale = append(stale, row{
			key:   append([]byte(nil), it.Key()...),
			value: append([]byte(nil), it.Value()...),
		})
	}
	err := it.Error()
	if closeErr := it.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, err
	}
	if len(stale) == 0 {
		return 0, nil
	}

	tx := txn.New(q.kv, txn.Sync(q.sync))
	defer tx.Close()
	for _, r := range stale {
		tx.Expect(r.key, r.value, true)
		tx.Delete(r.key)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(stale), nil
}

func marshalRun(r *Run) ([]byte, error) {
	const op = "jobs.marshalRun"

	if _, err := ParseType(string(r.Type)); err != nil {
		return nil, err
	}
	if _, known := stateNames[r.Outcome]; !known {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"attempt outcome %d is not a job state", uint8(r.Outcome)))
	}
	row := &pb.Run{
		JobId:            r.JobID.Bytes(),
		Type:             string(r.Type),
		Attempt:          r.Attempt,
		StartedAtUnixMs:  unixMilli(r.StartedAt),
		FinishedAtUnixMs: unixMilli(r.FinishedAt),
		Outcome:          uint32(r.Outcome),
		Error:            r.Error,
		Owner:            r.Owner,
		Namespace:        string(r.Namespace),
		RecordsProcessed: r.RecordsProcessed,
		RecordsChanged:   r.RecordsChanged,
	}
	out, err := deterministic.Marshal(row)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the attempt: %w", err))
	}
	return out, nil
}

func unmarshalRun(key, value []byte) (*Run, error) {
	const op = "jobs.unmarshalRun"

	t, ns, finished, rid, err := keys.ParseJobRun(key)
	if err != nil {
		return nil, err
	}
	var row pb.Run
	if err := proto.Unmarshal(value, &row); err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the attempt row at %x does not decode: %w", key, err))
	}
	jid, err := id.FromBytes(row.GetJobId())
	if err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the attempt at %x names a malformed job id", key))
	}
	r := &Run{
		ID:               rid,
		JobID:            jid,
		Tenant:           t,
		Namespace:        ns,
		Type:             Type(row.GetType()),
		Attempt:          row.GetAttempt(),
		StartedAt:        fromUnixMilli(row.GetStartedAtUnixMs()),
		FinishedAt:       fromUnixMilli(finished),
		Outcome:          State(row.GetOutcome()),
		Error:            row.GetError(),
		Owner:            row.GetOwner(),
		RecordsProcessed: row.RecordsProcessed,
		RecordsChanged:   row.RecordsChanged,
	}
	if sub := row.GetNamespace(); sub != "" {
		r.Namespace = tenant.Namespace(sub)
	}
	return r, nil
}
