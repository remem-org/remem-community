// Package vector owns Remem's canonical embeddings and the interface every
// index over them satisfies.
//
// The distinction it exists to hold is plan §II.4's: a canonical vector is
// canonical data, written in the same transaction as the record it belongs to
// and never discarded on a read failure; an index over those vectors is
// derived, and is rebuilt rather than repaired. `flat` blurs the two on
// purpose — it *is* an exact scan of the canonical rows, so it has no separate
// structure to fall out of step — and that is exactly why it is the reference
// implementation Phase 7's HNSW is verified against.
//
// # Every operation takes a tenant
//
// Not from the context, here, but as a parameter. That looks like a departure
// from Invariant 1's "the tenant travels in the context" and is not: an index
// is also driven by rebuild and migration jobs, which legitimately work across
// tenants (through tenant.ForEach) and would otherwise have to forge a context
// per tenant. The scope is still explicit and still impossible to omit.
package vector

import (
	"context"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Hit is one search result.
type Hit struct {
	ID id.ID
	// Distance is in the index's metric, smallest first. It is not a score:
	// internal/vector/distance converts, and the conversion depends on the
	// metric.
	Distance float32
}

// Filter decides whether a candidate may be returned.
//
// It is a function in Phase 3 and becomes a pushed-down predicate in Phase 5.
// That is deliberate: the walking skeleton needs *a* filter, and designing the
// predicate language before the attribute store exists would repeat the mistake
// docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md §5 records — a predicate DSL that
// could not express the fields the product actually filters on.
//
// A nil Filter accepts everything.
type Filter func(id.ID) bool

// Allows reports whether f accepts rid, treating a nil filter as "accept".
func (f Filter) Allows(rid id.ID) bool { return f == nil || f(rid) }

// Source yields one tenant's canonical vectors, for [Index.Rebuild].
//
// The method is Scan rather than ForEach on purpose. ForEach is reserved for
// tenant.Directory.ForEach, the single cross-tenant path in Remem, and the
// guard in internal/arch that polices who may call it works by name. Reusing
// the name for a tenant-scoped walk would mean either a false failure or an
// allowlist entry that quietly excuses the package from the real rule.
type Source interface {
	Scan(ctx context.Context, t tenant.ID, fn func(id.ID, []float32) error) error
}

// Health says whether an index can currently answer completely for one tenant.
//
// It is a separate call rather than a field on every Hit or a second return
// value from Search, and the reason is what the state is *about*: an index is
// damaged for a tenant, not for a query. Every search that tenant makes is
// affected identically until a rebuild finishes, and asking once per search
// step costs one map lookup where widening the result type would have rewritten
// every implementation and the whole contract suite to carry a flag that is
// constant across a request.
//
// The cost of that choice is that a caller can forget to ask, and would then
// present incomplete results as complete. TestSearchOverADegradedIndexSaysSo
// pins the one caller there is.
type Health struct {
	// Degraded reports that this tenant's searches may be incomplete. It is
	// deliberately "may be": an index that has been repaired in flight might
	// well return everything, and a caller that believes a complete answer is
	// partial loses nothing, where the reverse is the silent wrong answer this
	// codebase keeps refusing to ship.
	Degraded bool
	// Reason says what is wrong, in a sentence an operator can act on. It is
	// empty when Degraded is false.
	Reason string
}

// Maintainer is implemented by an index that must be told about canonical
// writes, because it keeps a structure of its own alongside them.
//
// `flat` does not implement it and cannot: it *is* an exact scan of the
// canonical rows, so there is nothing to tell it. `hnsw` does. The composition
// root asks for the interface rather than for a named implementation, which is
// what keeps "which index is configured" from becoming a branch in the write
// path.
//
// Both methods run after the record's transaction has committed (plan §II.4):
// the vector index is the one asynchronously maintained index, so a failure
// here degrades search and never fails a write.
type Maintainer interface {
	// Indexed makes an already-committed canonical vector searchable.
	Indexed(ctx context.Context, t tenant.ID, rid id.ID, v []float32) error
	// Removed drops a record whose canonical vector has already been deleted.
	Removed(ctx context.Context, t tenant.ID, rid id.ID) error
}

// RebuildOptions is what a caller may tell [Index.Rebuild] beyond its source.
type RebuildOptions struct {
	// Cursor is the progress a previous, interrupted rebuild of this tenant
	// saved. Nil starts from the beginning. A cursor an implementation does not
	// recognise also starts from the beginning: resuming is an optimisation, and
	// a whole rebuild is always a correct answer to "rebuild this".
	Cursor []byte
	// Save records progress durably. Nil means nobody is keeping it, and the
	// rebuild runs as one unit of work.
	Save func(ctx context.Context, cursor []byte) error
	// BatchSize is how many records one committed step inserts. Zero takes
	// the implementation's default; tests set it to make interruption cheap.
	BatchSize int
}

// RebuildOption adjusts a rebuild.
//
// Options rather than a wider signature, because an index that is atomic by
// construction — flat, whose rebuild is one replacement commit — has nothing to
// resume and should not have to say so at every call site.
type RebuildOption func(*RebuildOptions)

// WithProgress resumes from cursor and records progress through save. A job
// handler passes its own checkpoint and its Checkpointer's Save.
func WithProgress(cursor []byte, save func(context.Context, []byte) error) RebuildOption {
	return func(o *RebuildOptions) {
		o.Cursor = cursor
		o.Save = save
	}
}

// WithBatchSize sets how many records one committed step inserts.
func WithBatchSize(n int) RebuildOption {
	return func(o *RebuildOptions) { o.BatchSize = n }
}

// BuildRebuildOptions applies opts, for an implementation to read.
func BuildRebuildOptions(opts ...RebuildOption) RebuildOptions {
	var o RebuildOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Rebuilder is told that a tenant's derived index should be rebuilt.
//
// It is an interface with one method rather than a direct enqueue so that this
// package does not depend on the job framework — the composition root supplies
// an implementation that logs and queues a `vector.rebuild` job (Phase 9).
//
// What it must not become is a rebuild started inline: the caller discovering
// the damage is answering a user's search, and repairing there would charge one
// user for everyone's repair. Whatever implements this must not block either,
// for the same reason.
type Rebuilder interface {
	ScheduleRebuild(t tenant.ID, reason string)
}

// RebuilderFunc adapts a function to [Rebuilder].
type RebuilderFunc func(t tenant.ID, reason string)

// ScheduleRebuild calls f.
func (f RebuilderFunc) ScheduleRebuild(t tenant.ID, reason string) { f(t, reason) }

// Stats describes an index's state for one tenant.
type Stats struct {
	// Vectors is how many vectors the index holds.
	Vectors int
	// Dim is the width of those vectors, or zero when the index is empty.
	Dim int
	// Rebuilding reports whether a rebuild is in progress. A search served
	// while this is true is incomplete, which is what a response's
	// `truncated` flag means (plan §II.4).
	Rebuilding bool
}

// Index is an approximate or exact nearest-neighbour index over one tenant's
// vectors.
//
// Every implementation runs the shared suite in internal/vector/vectortest.
// That is not optional: `flat` is the definition of correct here, and an
// approximate index that does not agree with it on the small cases has a bug
// rather than an approximation.
type Index interface {
	// Insert adds or replaces a vector.
	Insert(ctx context.Context, t tenant.ID, rid id.ID, v []float32) error

	// Delete removes a vector. Removing an absent one is not an error: the
	// post-condition already holds.
	Delete(ctx context.Context, t tenant.ID, rid id.ID) error

	// Search returns up to k nearest vectors, nearest first, subject to filter.
	Search(ctx context.Context, t tenant.ID, q []float32, k int, filter Filter) ([]Hit, error)

	// Rebuild replaces the tenant's index from src. It is how a derived index
	// recovers (Invariant 3), and an interrupted one must leave the index in a
	// state a second Rebuild completes — either unchanged, or resumable from the
	// cursor it last saved through [WithProgress] (Invariant 5).
	Rebuild(ctx context.Context, t tenant.ID, src Source, opts ...RebuildOption) error

	// Stats describes the tenant's index.
	Stats(ctx context.Context, t tenant.ID) (Stats, error)

	// Health reports whether this tenant's searches can be complete.
	//
	// It must be cheap enough to call on every search: Stats is not, because
	// counting an exact index's vectors means scanning them.
	Health(ctx context.Context, t tenant.ID) (Health, error)
}
