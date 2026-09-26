package txn

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
)

// Retry bounds, and the reasoning behind the numbers.
//
// A conflict here is contention on a row, not a queue to join: nothing is held
// between attempts, so a retry succeeds as soon as it reads a value that
// survives to its own commit. What decides whether it converges is therefore
// not how many times it tries but how far apart the tries are — a tight loop is
// a thundering herd, in which every loser retries into the next winner's commit
// and the whole set of writers keeps colliding.
//
// The backoff is randomised for the same reason: an unjittered one keeps
// contending writers in lockstep, so they collide at every doubling instead of
// spreading out. Eight attempts with a ceiling of 10ms bounds the added latency
// at roughly 40ms in the worst case, which is under the embedding call that
// precedes every write it protects.
const (
	// MaxAttempts is how many times [Do] runs a body whose commit conflicts.
	MaxAttempts = 8

	baseBackoff = 100 * time.Microsecond
	maxBackoff  = 10 * time.Millisecond
)

// Do runs body inside a transaction and commits it, retrying a conflict.
//
// # Why this exists
//
// Until Phase 8 every conditional write in Remem was on a row one record owns:
// two writers collided only when they were genuinely updating the same memory,
// and reporting that is right. The text index introduces the first row that
// *every* write in a tenant touches — the corpus statistics BM25 scores against
// — and it is conditionally committed because a lost increment is an inverse
// document frequency that is silently wrong from then on.
//
// Ordinary concurrent ingestion therefore now produces conflicts that have
// nothing to do with the caller, and this is where they are absorbed. The retry
// is cheap and correctly placed: everything expensive about a write — the
// embedding call above all — happens before the transaction is opened, so an
// attempt re-stages rows and re-commits, and never re-runs the model.
//
// # What body must guarantee
//
// body may run more than once, so it must stage from values computed outside
// it. Anything drawn inside it that a retry would draw differently — a new
// identifier, a clock reading — makes the second attempt write a different
// record than the first, and Invariant 9 forbids exactly that in a path Phase
// 14 will replicate. Every caller computes ids, timestamps and vectors before
// the first attempt.
//
// A body that has no conditional write in it never conflicts, and pays nothing
// for being run through here.
func Do(ctx context.Context, kv storage.KV, body func(tx Tx) error, opts ...Option) error {
	const op = "txn.Do"

	var last error
	for n := range MaxAttempts {
		err := attempt(ctx, kv, body, opts)
		if err == nil {
			return nil
		}
		if !errs.Is(err, errs.Conflict) {
			return err
		}
		last = err
		if err := pause(ctx, n); err != nil {
			return err
		}
	}
	return errs.E(errs.Conflict, op, fmt.Errorf(
		"this write met a concurrent one %d times running: %w", MaxAttempts, last))
}

// attempt is one try, in its own function so the deferred Close runs between
// attempts rather than after all of them.
func attempt(ctx context.Context, kv storage.KV, body func(tx Tx) error, opts []Option) error {
	tx := New(kv, opts...)
	defer tx.Close()

	if err := body(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// pause waits a randomised interval before the next attempt, and gives up
// immediately if the caller has gone away.
//
// A cancelled context returns Unavailable rather than the conflict: the write
// did not lose a race, its caller stopped waiting, and reporting contention for
// a client disconnect sends an operator looking in the wrong place.
func pause(ctx context.Context, n int) error {
	window := baseBackoff << n
	if window > maxBackoff {
		window = maxBackoff
	}
	timer := time.NewTimer(rand.N(window))
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return errs.E(errs.Unavailable, "txn.Do", ctx.Err())
	}
}
