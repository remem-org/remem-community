package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs/pb"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"google.golang.org/protobuf/proto"
)

// MaxPauseDuration bounds how long a recurring type may be suppressed.
//
// A pause is a deliberate hole in the repair schedule — the recurring types are
// index rebuilds, the lifecycle pass and the reaper — so the question is not
// "how long might an operator want" but "how long may the work be missing
// before nobody remembers it was". A week is longer than any incident and
// shorter than the memory of having declared one; past that the honest action
// is to turn the subsystem off in configuration, where it is visible on every
// start.
//
// It is also why there is no permanent pause. A boolean with no expiry
// suppresses repair indefinitely and reports nothing, which is the failure mode
// the whole framework exists to replace.
const MaxPauseDuration = 7 * 24 * time.Hour

// Pause is an operator's suppression of one recurring type's scheduling.
//
// It suppresses the *schedule* and nothing else: work already queued or running
// finishes, and a manual run still queues. A pause that cancelled outstanding
// work would conflate schedule control with per-job cancellation, which is a
// separate operation with a separate audit trail.
type Pause struct {
	Tenant    tenant.ID
	Namespace tenant.Namespace
	Type      Type

	// ExpiresAt is when the suppression lifts. It is required, bounded by
	// [MaxPauseDuration], and compared against the reader's own clock — so a
	// process that died holding a pause does not leave one in force.
	ExpiresAt time.Time
	// SetBy names the credential that asked for it.
	SetBy string
	SetAt time.Time
}

// Active reports whether the pause is still in force at now.
func (p *Pause) Active(now time.Time) bool {
	return p != nil && now.Before(p.ExpiresAt)
}

// Pause suppresses scheduled enqueueing of one type for one tenant until the
// given instant.
//
// The expiry is required rather than optional and is validated before anything
// is written: a refused pause leaves no state, because a half-written
// suppression suppresses and nothing reports it.
func (q *Queue) Pause(ctx context.Context, t tenant.ID, typ Type, until time.Time, by string) (*Pause, error) {
	const op = "jobs.Pause"

	key, err := pauseKey(op, t, typ)
	if err != nil {
		return nil, err
	}
	now := q.clk.Now().UTC()
	switch {
	case until.IsZero():
		return nil, errs.E(errs.Invalid, op, errors.New(
			"a pause needs an expiry: a suppression that never lifts outlives the memory of setting it"))
	case !until.After(now):
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"the pause expiry %s is not in the future; it is now %s",
			until.UTC().Format(time.RFC3339), now.Format(time.RFC3339)))
	case until.Sub(now) > MaxPauseDuration:
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"the pause would last %s, and the longest this server supports is %s; "+
				"to stop this work for longer, turn its subsystem off in configuration",
			until.Sub(now).Round(time.Second), MaxPauseDuration))
	}

	p := &Pause{
		Tenant: t, Namespace: tenant.DefaultNamespace, Type: typ,
		ExpiresAt: until.UTC(), SetBy: by, SetAt: now,
	}
	value, err := marshalPause(p)
	if err != nil {
		return nil, err
	}

	unlock := q.lock(t)
	defer unlock()

	tx := txn.New(q.kv, txn.Sync(q.sync))
	defer tx.Close()
	tx.Set(key, value)
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// Resume lifts a pause.
//
// Resuming what is not paused is a success that writes nothing: the
// post-condition — this type is not suppressed — already holds, which is the
// same reading [storage.KV.Delete] takes of deleting an absent key. An operator
// clicking resume on a pause that expired while they were looking at it should
// not be told the server is broken.
func (q *Queue) Resume(ctx context.Context, t tenant.ID, typ Type) error {
	const op = "jobs.Resume"

	key, err := pauseKey(op, t, typ)
	if err != nil {
		return err
	}
	unlock := q.lock(t)
	defer unlock()

	tx := txn.New(q.kv, txn.Sync(q.sync))
	defer tx.Close()
	tx.Delete(key)
	return tx.Commit(ctx)
}

// PauseOf returns the pause in force for one type, or [errs.NotFound].
//
// An expired row reads as not found rather than as an expired pause. Expiry is
// evaluated here, against the clock, because that is what makes a pause
// self-clearing: nothing has to run for one to lift.
func (q *Queue) PauseOf(ctx context.Context, t tenant.ID, typ Type) (*Pause, error) {
	const op = "jobs.PauseOf"

	key, err := pauseKey(op, t, typ)
	if err != nil {
		return nil, err
	}
	value, err := q.kv.Get(ctx, key)
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return nil, errs.E(errs.NotFound, op, fmt.Errorf(
				"job type %s is not paused in tenant %s", typ, t))
		}
		return nil, err
	}
	p, err := unmarshalPause(key, value)
	if err != nil {
		return nil, err
	}
	if !p.Active(q.clk.Now()) {
		return nil, errs.E(errs.NotFound, op, fmt.Errorf(
			"job type %s is not paused in tenant %s; its pause expired at %s",
			typ, t, p.ExpiresAt.Format(time.RFC3339)))
	}
	return p, nil
}

// Pauses returns the pauses still in force for one tenant, in type order.
//
// Expired rows are skipped rather than deleted. The row is small, the reaper
// removes it on its next pass, and a listing that wrote to the store to answer
// a question would make reading the status surface a mutation.
func (q *Queue) Pauses(ctx context.Context, t tenant.ID) ([]*Pause, error) {
	const op = "jobs.Pauses"

	if t == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"listing pauses requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	now := q.clk.Now()
	lower, upper := keys.JobPauseRange(t, tenant.DefaultNamespace)
	it := q.kv.NewIterator(lower, upper)

	var out []*Pause
	for ok := it.First(); ok; ok = it.Next() {
		p, err := unmarshalPause(append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...))
		if err != nil {
			_ = it.Close()
			return nil, err
		}
		if p.Active(now) {
			out = append(out, p)
		}
	}
	err := it.Error()
	if closeErr := it.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ReapPauses removes pause rows that expired before the cutoff, up to limit.
//
// Expiry is already honoured on every read, so this is housekeeping rather than
// correctness: without it a tenant accumulates one dead row per pause anybody
// ever set. The space is small enough that the whole of it is scanned.
func (q *Queue) ReapPauses(ctx context.Context, t tenant.ID, before time.Time, limit int) (int, error) {
	const op = "jobs.ReapPauses"

	if t == "" {
		return 0, errs.E(errs.Invalid, op, errors.New(
			"reaping requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	if limit <= 0 {
		return 0, nil
	}

	unlock := q.lock(t)
	defer unlock()

	lower, upper := keys.JobPauseRange(t, tenant.DefaultNamespace)
	it := q.kv.NewIterator(lower, upper)
	type row struct{ key, value []byte }
	var stale []row
	for ok := it.First(); ok && len(stale) < limit; ok = it.Next() {
		key := append([]byte(nil), it.Key()...)
		value := append([]byte(nil), it.Value()...)
		p, err := unmarshalPause(key, value)
		if err != nil {
			_ = it.Close()
			return 0, err
		}
		if p.ExpiresAt.Before(before) {
			stale = append(stale, row{key: key, value: value})
		}
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
		// Conditional, like every other removal: a pause somebody renewed
		// between the scan and the commit is not this reaper's to delete.
		tx.Expect(r.key, r.value, true)
		tx.Delete(r.key)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(stale), nil
}

// SubmitUnlessPaused enqueues a scheduled job, and reports whether it did.
//
// # Why the pause is read inside the transaction
//
// The obvious implementation reads the pause, finds none, and submits. That
// races an operator pausing in between, and the job it enqueues is the one the
// operator was trying to stop. A per-tenant gate would make the race unlikely
// rather than impossible and would do nothing across nodes — txn.Gate's own
// comment says it "removes the contention rather than absorbing it; it is not
// what makes this correct."
//
// So the answer "not paused" is expressed as a condition on the commit: the
// pause key is expected *absent* at commit, and a pause written in between
// therefore loses the job rather than racing it. This is the shape Phase 11
// gave graph.Service.Linked, for the same reason.
//
// It is the scheduler's enqueue and nothing else's. [Submit] is the manual
// path — an operator asking for this work now — and a pause suppresses the
// schedule, not the operator.
func (q *Queue) SubmitUnlessPaused(ctx context.Context, j *Job) (bool, error) {
	const op = "jobs.SubmitUnlessPaused"

	key, err := pauseKey(op, j.Tenant, j.Type)
	if err != nil {
		return false, err
	}

	unlock := q.lock(j.Tenant)
	defer unlock()

	value, err := q.kv.Get(ctx, key)
	switch {
	case err == nil:
		p, perr := unmarshalPause(key, value)
		if perr != nil {
			return false, perr
		}
		if p.Active(q.clk.Now()) {
			return false, nil
		}
		// Expired: the row is still there and still says what it says, so the
		// commit is conditioned on it rather than on absence. A pause renewed
		// in between changes the bytes and the commit conflicts.
		return q.submitConditional(ctx, j, key, value, true)
	case errs.Is(err, errs.NotFound):
		return q.submitConditional(ctx, j, key, nil, false)
	default:
		return false, err
	}
}

// submitConditional writes the job and the pause condition in one transaction.
func (q *Queue) submitConditional(ctx context.Context, j *Job, pause, expect []byte, present bool) (bool, error) {
	tx := txn.New(q.kv, txn.Sync(q.sync))
	defer tx.Close()

	tx.Expect(pause, expect, present)
	if err := q.Enqueue(ctx, tx, j); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		if errs.Is(err, errs.Conflict) {
			// A conflict here is almost always the race this method exists for
			// — somebody paused the type while we were deciding — and the
			// operator wins: the job is not written and the next tick asks
			// again. It is checked rather than assumed, because Enqueue also
			// conditions on the job's own key and a conflict reported as a
			// pause would be a job silently not scheduled.
			if paused, perr := q.pausedNow(ctx, j.Tenant, j.Type); perr == nil && paused {
				return false, nil
			}
			return false, err
		}
		return false, err
	}
	return true, nil
}

// pausedNow re-reads the pause without the gate, for the conflict path above.
func (q *Queue) pausedNow(ctx context.Context, t tenant.ID, typ Type) (bool, error) {
	_, err := q.PauseOf(ctx, t, typ)
	switch {
	case err == nil:
		return true, nil
	case errs.Is(err, errs.NotFound):
		return false, nil
	default:
		return false, err
	}
}

// pauseKey validates the scope and builds the row address.
func pauseKey(op string, t tenant.ID, typ Type) ([]byte, error) {
	if t == "" || !t.Valid() {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"tenant %q is not a valid tenant id; a pause is per tenant (Invariant 1)", t))
	}
	if _, err := ParseType(string(typ)); err != nil {
		return nil, err
	}
	return keys.JobPause(t, tenant.DefaultNamespace, string(typ)), nil
}

func marshalPause(p *Pause) ([]byte, error) {
	out, err := deterministic.Marshal(&pb.Pause{
		Type:            string(p.Type),
		ExpiresAtUnixMs: unixMilli(p.ExpiresAt),
		SetBy:           p.SetBy,
		SetAtUnixMs:     unixMilli(p.SetAt),
	})
	if err != nil {
		return nil, errs.E(errs.Invalid, "jobs.marshalPause", fmt.Errorf("encoding the pause: %w", err))
	}
	return out, nil
}

func unmarshalPause(key, value []byte) (*Pause, error) {
	const op = "jobs.unmarshalPause"

	t, ns, typ, err := keys.ParseJobPause(key)
	if err != nil {
		return nil, err
	}
	var row pb.Pause
	if err := proto.Unmarshal(value, &row); err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the pause row at %x does not decode: %w", key, err))
	}
	// The key is the address and the body is what the row says about itself. A
	// row whose two halves disagree was not written by this code, and reading
	// either over the other would suppress a type nobody paused.
	if row.GetType() != typ {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the pause stored under %s says it pauses %s", typ, row.GetType()))
	}
	return &Pause{
		Tenant:    t,
		Namespace: ns,
		Type:      Type(typ),
		ExpiresAt: fromUnixMilli(row.GetExpiresAtUnixMs()),
		SetBy:     row.GetSetBy(),
		SetAt:     fromUnixMilli(row.GetSetAtUnixMs()),
	}, nil
}
