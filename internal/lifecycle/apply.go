package lifecycle

import (
	"strconv"
	"time"

	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/record"
)

// Note is one transition the pass decided on: an event waiting for a subject
// and a transaction.
//
// It is not an [events.Event] because the pass does not know the tenant or the
// subject — it has a record, and turning that into a stream address is the
// caller's job. Splitting it here keeps [Apply] a pure function of the record
// and the instant, which is what makes every test in this package a table
// rather than a fixture.
type Note struct {
	Kind          events.Kind
	Actor, Reason string
	Before, After map[string]string
}

// Result is what one pass decided.
type Result struct {
	// Changed means the record was mutated and must be written back.
	Changed bool
	// Delete means the record is past its retention and is to be removed,
	// along with its vector, its edges and its event stream. It is the only
	// destructive outcome in this package, it follows an archive that already
	// happened, and no heuristic reaches it (plan §II.10 row 8).
	Delete bool
	// Notes are the events to write, in the order they happened.
	Notes []Note
}

// Archived reports whether this pass retired the memory, for a caller that
// wants to know without inspecting the record it just handed over.
func (r Result) Archived() bool {
	for _, n := range r.Notes {
		if n.Kind == events.Archived || n.Kind == events.Expired {
			return true
		}
	}
	return false
}

func (r *Result) note(n Note) {
	r.Changed = true
	r.Notes = append(r.Notes, n)
}

// Apply decides what a record is owed at this instant and applies it in place.
//
// # One pass, not five
//
// Rust runs five sweeps, each walking the same shared due-time index and each
// discarding the memories the other four own (services/attrs.rs:44-51). Here
// one pass asks one record what it is owed and does all of it, in the order the
// five sweeps run in: expiry, then importance decay, then active forgetting.
// Cleanup is the archived branch, which every other transition skips.
//
// # It takes the instant rather than a clock
//
// One `now` for a whole run, passed down. That is what makes a run reproducible
// and what keeps this function testable as a table — and it is Invariant 8,
// which forbids a wall-clock read in durable business logic.
//
// # It writes nothing
//
// It mutates the record it is given and returns the events that go with it. The
// caller commits both in one transaction, so a memory that was archived and an
// audit row saying so land together or not at all.
func Apply(p *Policies, rec *record.Record, now time.Time) Result {
	var out Result
	pol := p.For(rec)

	// A policy this binary does not recognise is inert (see [inert]): every
	// threshold below is nil or neutral, so nothing fires. Returning early is
	// the same answer, stated where a reader will look for it.
	if !p.Known(rec.Fields.Policy) {
		return out
	}

	// An archived memory has exactly one thing left to happen to it.
	if rec.Fields.Archived {
		cleanup(pol, rec, now, &out)
		return out
	}

	if expire(pol, rec, now, &out) {
		// Expiry either archived the memory or promoted it into a policy whose
		// decay is not this one's. Either way, deciding the rest of this pass
		// from the policy that no longer applies would be applying rules the
		// memory has just stopped being governed by.
		return out
	}

	// Flashbulb protection defers decay and forgetting wholesale
	// (memory_manager.rs:115). It does not defer expiry: a TTL the caller set
	// is a statement about the memory's life, not about its decay.
	if now.Before(rec.Fields.ProtectedUntil) {
		return out
	}

	decayImportance(pol, rec, now, &out)
	decayHealth(pol, rec, now, &out)
	archive(pol, rec, now, &out)
	return out
}

// wholeDays is the elapsed time in truncated days.
//
// Whole days, because Rust reasons in them (lifecycle_manager.rs:279) and a
// pass under a day writes nothing. That is not a rounding convenience: without
// it every sweep rewrites every record it visits, an hourly sweep rewrites the
// whole corpus twenty-four times a day, and no comparison with Rust agrees to
// better than a fraction of a day's factor.
func wholeDays(from, to time.Time) int {
	if from.IsZero() || !to.After(from) {
		return 0
	}
	return int(to.Sub(from) / Day)
}

func f32s(v float32) string { return strconv.FormatFloat(float64(v), 'g', -1, 32) }
