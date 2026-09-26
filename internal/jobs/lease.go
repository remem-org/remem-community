package jobs

import (
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// DefaultMaxAttempts bounds how many times a job runs.
//
// It counts runs, not retries: the configuration setting is `jobs.max_retries`
// and defaults to five, so the sixth run is the last one.
const DefaultMaxAttempts uint32 = 6

// transition is the one path by which a stored job changes.
//
// Everything a worker does to a job it holds — completing it, failing it,
// renewing its lease, saving its checkpoint, releasing it at shutdown — is a
// fenced read-modify-write through here, and there are three reasons it is one
// function rather than five.
//
// It re-reads the durable row rather than trusting the caller's copy, so two
// transitions on one job compose instead of overwriting each other: a
// checkpoint saved while the renewal timer is moving the lease keeps both.
//
// It checks the fencing token, so a worker that has lost its job to a
// reclamation is refused rather than allowed to complete work somebody else has
// since redone. At-least-once delivery makes that an ordinary event, not a
// pathological one.
//
// And it moves the row in one transaction, so the invariant that a job is in
// exactly one partition survives a crash at any point.
func (q *Queue) transition(ctx context.Context, j *Job, apply func(next *Job)) error {
	return q.transitionWithRun(ctx, j, apply, nil)
}

// transitionWithRun moves the queue row and, when supplied, records the
// attempt in the same durable transaction. A finished attempt and its history
// are one fact: neither should survive without the other.
func (q *Queue) transitionWithRun(ctx context.Context, j *Job, apply func(next *Job), run *Run) error {
	const op = "jobs.transition"

	if j.Tenant == "" {
		return errs.E(errs.Invalid, op, errors.New("a job transition requires a tenant"))
	}
	if len(j.stored) == 0 {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"job %s has never been stored, so there is no row to move", j.ID))
	}

	unlock := q.lock(j.Tenant)
	defer unlock()

	value, err := q.kv.Get(ctx, j.stored)
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return errs.E(errs.Conflict, op, fmt.Errorf(
				"job %s is no longer where this worker left it: it was reclaimed, cancelled or already finished", j.ID))
		}
		return err
	}
	cur, err := Unmarshal(j.stored, value)
	if err != nil {
		return err
	}
	if err := fence(op, j, cur); err != nil {
		return err
	}

	next := cur.Clone()
	apply(next)
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
	tx.Expect(j.stored, value, true)
	tx.Delete(j.stored)
	tx.Set(newKey, newValue)
	if run != nil {
		if run.ID == id.Zero || run.Tenant != j.Tenant || run.JobID != j.ID ||
			run.Type != j.Type || run.Attempt != next.Attempts || run.Outcome != next.State {
			return errs.E(errs.Invalid, "jobs.transition", errors.New(
				"an atomic attempt record must identify the transitioned job, attempt and outcome"))
		}
		if run.Namespace == "" {
			run.Namespace = tenant.DefaultNamespace
		}
		if run.FinishedAt.IsZero() {
			run.FinishedAt = q.clk.Now().UTC()
		}
		runKey := keys.JobRun(run.Tenant, tenant.DefaultNamespace, unixMilli(run.FinishedAt), run.ID)
		runValue, err := marshalRun(run)
		if err != nil {
			return err
		}
		tx.Expect(runKey, nil, false)
		tx.Set(runKey, runValue)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	next.stored = newKey
	*j = *next
	return nil
}

// fence refuses a worker whose claim has been superseded.
//
// The key alone is nearly enough — a reclaimed job is at a different key, so
// the read above usually fails first — but only nearly. A job reclaimed and
// immediately re-claimed with the same lease duration lands on the *same* key,
// and the previous owner's completion would then delete a row somebody else now
// owns. The token is what makes that impossible, and it is why a reclamation
// draws a new one.
func fence(op string, held, cur *Job) error {
	if held.Lease == nil {
		return nil // the caller is not claiming to hold a lease
	}
	if cur.Lease == nil {
		return errs.E(errs.Conflict, op, fmt.Errorf(
			"job %s is no longer leased; this worker's claim has been reclaimed", held.ID))
	}
	if cur.Lease.Token != held.Lease.Token {
		return errs.E(errs.Conflict, op, fmt.Errorf(
			"job %s is leased to %s under a newer token; this worker's claim was reclaimed",
			held.ID, cur.Lease.Owner))
	}
	return nil
}

// finish moves a job to a terminal state.
func (q *Queue) finish(ctx context.Context, j *Job, state State, lastErr string) error {
	return q.finishWithRun(ctx, j, state, lastErr, nil)
}

func (q *Queue) finishWithRun(ctx context.Context, j *Job, state State, lastErr string, run *Run) error {
	if !state.Terminal() {
		return errs.E(errs.Invalid, "jobs.finish", fmt.Errorf("%s is not a terminal state", state))
	}
	now := q.clk.Now().UTC()
	return q.transitionWithRun(ctx, j, func(next *Job) {
		next.State = state
		next.Lease = nil
		next.UpdatedAt = now
		if lastErr != "" {
			next.LastError = lastErr
		}
		// A retry keeps the previous failure's message, so an operator looking
		// at a running job can see it has been here before. A job that finally
		// succeeds should not still be carrying it.
		if state == Completed {
			next.LastError = ""
		}
	}, run)
}

// CompleteWithRun atomically completes a job and records its execution.
func (q *Queue) CompleteWithRun(ctx context.Context, j *Job, run *Run) error {
	return q.finishWithRun(ctx, j, Completed, "", run)
}

// Renew extends a running job's lease.
//
// It moves the row, because the running partition is ordered by lease expiry —
// a renewal that only updated a field would leave the job sitting at the head
// of the reclamation scan while claiming to be alive.
//
// The fencing token is deliberately unchanged. Only a reclamation draws a new
// one, and that is what makes "the token changed" mean "you lost the job"
// rather than "time passed".
func (q *Queue) Renew(ctx context.Context, j *Job) error {
	const op = "jobs.Renew"
	if j.Lease == nil {
		return errs.E(errs.Invalid, op, fmt.Errorf("job %s holds no lease to renew", j.ID))
	}
	now := q.clk.Now().UTC()
	return q.transition(ctx, j, func(next *Job) {
		next.Lease.ExpiresAt = now.Add(q.lease)
		next.UpdatedAt = now
	})
}

// Release returns a running job to the queue without charging the attempt.
//
// It is the shutdown path. A process stopping is not a job failing, and a job
// that lost an attempt to every rolling restart would eventually be Failed
// having never once broken — so the attempt the claim charged is given back.
// The checkpoint stays, so the next run resumes rather than restarts.
func (q *Queue) Release(ctx context.Context, j *Job) error {
	now := q.clk.Now().UTC()
	return q.transition(ctx, j, func(next *Job) {
		next.State = Pending
		next.Lease = nil
		next.RunAt = now
		next.UpdatedAt = now
		if next.Attempts > 0 {
			next.Attempts--
		}
	})
}

// Reclaim returns jobs whose leases have lapsed, up to limit, and reports how
// many it moved.
//
// It is the counterpart of at-least-once delivery: a worker that died, paused
// or was killed holds a lease nothing will ever renew, and without this the job
// stays running for ever. The scan is the running partition ordered by expiry,
// so it stops at the first lease that is still alive.
//
// A reclaimed job keeps the attempt the claim charged it. That is what stops a
// handler that hangs every time from looping for ever: each reclamation is
// followed by a claim that charges another attempt, and the job that runs out
// is Failed here rather than returned to the queue. A job nothing can finish
// and nothing gives up on is a worker slot leaking at every poll.
func (q *Queue) Reclaim(ctx context.Context, t tenant.ID, limit int) (int, error) {
	const op = "jobs.Reclaim"

	if t == "" {
		return 0, errs.E(errs.Invalid, op, errors.New(
			"reclaiming requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	if limit <= 0 {
		return 0, nil
	}

	unlock := q.lock(t)
	defer unlock()

	now := q.clk.Now().UTC()
	lapsed, err := q.due(ctx, t, keys.JobRunning, now, nil, limit)
	if err != nil {
		return 0, err
	}

	reclaimed := 0
	for _, c := range lapsed {
		next := c.job.Clone()
		next.UpdatedAt = now
		if next.Attempts >= next.MaxAttempts {
			next.State = Failed
			next.Lease = nil
			next.LastError = fmt.Sprintf(
				"the lease expired on attempt %d of %d and there are no attempts left; "+
					"the worker holding it stopped without completing or failing it",
				next.Attempts, next.MaxAttempts)
		} else {
			next.State = Pending
			next.Lease = nil
			next.RunAt = now
			next.LastError = fmt.Sprintf("the lease expired on attempt %d; the job was reclaimed", next.Attempts)
		}
		if err := q.move(ctx, c.key, c.value, next); err != nil {
			if errs.Is(err, errs.Conflict) {
				continue // the owner renewed or finished it first, which is the good case
			}
			return reclaimed, err
		}
		reclaimed++
	}
	return reclaimed, nil
}
