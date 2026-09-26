package jobs

import (
	"context"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
)

// MaxCheckpointBytes bounds a progress cursor.
//
// A cursor is a position, not a working set: a key to resume after, an offset,
// a count. Anything larger is state the handler should be keeping where it can
// read it back, not on a row every claim scan pays to skip.
const MaxCheckpointBytes = 64 << 10

// Checkpointer is how a handler records progress.
//
// It is an interface rather than a method on Job because what a handler is
// given must not be a way to move the job: a handler that could complete or
// re-lease its own work would be a second place the state machine lives. The
// counts below are the same principle applied to telemetry — a handler may say
// what it did and may not say what became of its job.
//
// # Why the framework cannot count for the handler
//
// Only the handler knows what a record is. A rebuild's unit is a vector, the
// lifecycle pass's is a memory, discovery's is a subject, and the reaper's is a
// row it deleted. Inferring a count from a checkpoint, an elapsed time or the
// size of an index afterwards produces a number that looks exact and is not,
// which is worse than no number: an operator acts on it.
type Checkpointer interface {
	// Save records the handler's progress cursor durably.
	Save(ctx context.Context, cursor []byte) error

	// Processed records how many records this attempt has examined, and
	// Changed how many of those it wrote. Both are absolute totals for *this
	// attempt* rather than increments, so a handler counting as it goes simply
	// calls them again and the last call wins.
	//
	// Neither has a default. A handler that never calls one leaves that count
	// unavailable in its history rather than zero, because "this attempt
	// changed nothing" and "nobody counted" are different answers.
	//
	// They record into the attempt and touch no durable row of their own: the
	// figures are committed with the attempt's history when it ends, including
	// when it ends by failing — a handler that broke after doing half its work
	// did that half.
	Processed(n uint64)
	Changed(n uint64)
}

// Save records a running job's progress.
//
// It is a fenced read-modify-write like every other transition, so a checkpoint
// saved while the renewal timer is moving the lease keeps both, and a worker
// whose job has been reclaimed is refused rather than writing progress into
// somebody else's run.
func (q *Queue) Save(ctx context.Context, j *Job, cursor []byte) error {
	const op = "jobs.Save"
	if j.Lease == nil {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"job %s is not leased to anybody; only the worker holding a job records its progress", j.ID))
	}
	if len(cursor) > MaxCheckpointBytes {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"the checkpoint is %d bytes, the limit is %d; a cursor is a position, not a working set",
			len(cursor), MaxCheckpointBytes))
	}
	saved := append([]byte(nil), cursor...)
	now := q.clk.Now().UTC()
	return q.transition(ctx, j, func(next *Job) {
		next.Checkpoint = saved
		next.UpdatedAt = now
	})
}

// checkpointer is what a handler is given: the one operation it may perform on
// its own job.
//
// It goes through the running job's mutex, because the renewal ticker is
// touching the same row from another goroutine.
type checkpointer struct {
	queue   *Queue
	running *runningJob
}

func (c checkpointer) Save(ctx context.Context, cursor []byte) error {
	return c.running.withJob(func(j *Job) error {
		// A handler saving as it is cancelled is the normal case at shutdown,
		// so the write must not be cancelled along with it.
		return c.queue.Save(context.WithoutCancel(ctx), j, cursor)
	})
}

func (c checkpointer) Processed(n uint64) { c.running.report(&c.running.processed, n) }
func (c checkpointer) Changed(n uint64)   { c.running.report(&c.running.changed, n) }
