package jobs

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
)

// Handler runs one job.
//
// # The obligation
//
// Delivery is at-least-once, so a handler must be idempotent: running it twice
// on one payload must leave the same end state as running it once. That is not
// a style preference — a lease that lapses under a slow handler, a process that
// dies between the handler returning and the completion committing, and a retry
// after a partial failure each produce a second run in ordinary operation.
// TestEveryRegisteredHandlerIsIdempotent asserts it for every registered type.
//
// A handler is given a Checkpointer rather than the queue, so that recording
// progress is the only thing it can do to its own job. A handler that could
// complete or re-lease its work would be a second place the state machine
// lives.
//
// It must return when its context is cancelled. Cancellation means either the
// process is shutting down or somebody cancelled the job; in both cases the
// right response is to checkpoint and return, and the framework decides what
// happens to the row.
type Handler interface {
	Handle(ctx context.Context, j *Job, cp Checkpointer) error
}

// HandlerFunc adapts a function to [Handler].
type HandlerFunc func(ctx context.Context, j *Job, cp Checkpointer) error

// Handle runs the function.
func (f HandlerFunc) Handle(ctx context.Context, j *Job, cp Checkpointer) error {
	return f(ctx, j, cp)
}

// Entry is one registered job type.
//
// It carries the scheduling policy as well as the handler, so that "how often
// does this run, and how many times does it try" is answered where the type is
// declared rather than at whichever call site happened to enqueue it. Two
// enqueues of one type with different attempt bounds is a class of confusion
// that has no upside.
type Entry struct {
	Type    Type
	Handler Handler

	// Description is what the admin surface shows. It is for an operator
	// deciding whether to cancel something, so it says what the job does to
	// the data rather than what the code is called.
	Description string

	// MaxAttempts bounds how many times a job of this type runs. Zero takes
	// DefaultMaxAttempts.
	MaxAttempts uint32

	// Priority orders this type within a claim batch.
	Priority int8

	// Every, when positive, makes the type recurring: the scheduler enqueues
	// one job per tenant per interval, and never a second while one is still
	// outstanding.
	Every time.Duration
}

// Registry maps a job type to the handler that runs it.
//
// Registration happens once, at composition, and is then read-only in practice
// — but the read side is concurrent (every worker resolves a type on every job)
// so it is guarded rather than documented as safe.
type Registry struct {
	mu      sync.RWMutex
	entries map[Type]Entry
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[Type]Entry)}
}

// Register adds a job type.
//
// A duplicate is refused rather than replacing the first. A silent replacement
// would send work enqueued against one handler to another — which, for the
// rebuild handlers, means a job that says it repaired an index and did not.
func (r *Registry) Register(e Entry) error {
	const op = "jobs.Registry.Register"

	if _, err := ParseType(string(e.Type)); err != nil {
		return err
	}
	if e.Handler == nil {
		return errs.E(errs.Invalid, op, fmt.Errorf("job type %s has no handler", e.Type))
	}
	if e.Every < 0 {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"job type %s has a negative interval; use zero for a type that is not recurring", e.Type))
	}
	if e.MaxAttempts == 0 {
		e.MaxAttempts = DefaultMaxAttempts
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[e.Type]; exists {
		return errs.E(errs.Conflict, op, fmt.Errorf(
			"job type %s is already registered; a second registration would silently take over its queued work", e.Type))
	}
	r.entries[e.Type] = e
	return nil
}

// Lookup returns the entry for a type.
func (r *Registry) Lookup(t Type) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[t]
	return e, ok
}

// Handler resolves the handler for a type.
//
// An unregistered type is [errs.NotFound] rather than a no-op, because a job
// whose type nothing handles must not look like a job that has been done. It is
// what an upgrade that removed a handler leaves behind while rows of that type
// are still queued, and the job should sit there failing visibly.
func (r *Registry) Handler(t Type) (Handler, error) {
	e, ok := r.Lookup(t)
	if !ok {
		return nil, errs.E(errs.NotFound, "jobs.Registry.Handler", fmt.Errorf(
			"no handler is registered for job type %s in this binary", t))
	}
	return e.Handler, nil
}

// Types returns every registered type, in name order.
func (r *Registry) Types() []Type {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Type, 0, len(r.entries))
	for t := range r.entries {
		out = append(out, t)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

// Entries returns every registered entry, in type order.
//
// The order is stable because the admin surface renders this list, and a
// listing whose order changes per process is a listing nobody can diff.
func (r *Registry) Entries() []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Entry, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Type < out[b].Type })
	return out
}

// Recurring returns the entries the scheduler drives, in type order.
func (r *Registry) Recurring() []Entry {
	var out []Entry
	for _, e := range r.Entries() {
		if e.Every > 0 {
			out = append(out, e)
		}
	}
	return out
}
