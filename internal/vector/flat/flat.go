// Package flat is the exact vector index: it scans every vector a tenant owns
// and returns the true nearest neighbours.
//
// It is the reference implementation, and it is meant to stay that way. Phase
// 7's HNSW is verified against it, so `flat` gets no approximation, no tuning
// parameter and no heuristic — the moment it acquires one, it stops being able
// to say what the right answer was.
//
// # It has no state of its own
//
// A flat index *is* the canonical vector rows. There is no derived structure to
// fall out of step with them, no rebuild that can be stale, and no insert that
// can fail after the record committed. That is the whole reason the walking
// skeleton uses it: the parts of search that can be wrong are narrowed down to
// the parts Phase 3 is actually building.
//
// The cost is linear in the tenant's corpus, per query. That is the trade the
// plan makes on purpose — correctness first, and Phase 7 buys the scale back
// against a reference that is known to be right.
package flat

import (
	"container/heap"
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// Index is an exact scan over a tenant's canonical vectors.
type Index struct {
	store  *vector.Store
	metric distance.Metric
}

var _ vector.Index = (*Index)(nil)

// New returns a flat index over store, comparing with metric.
func New(store *vector.Store, metric distance.Metric) *Index {
	if metric == 0 {
		metric = distance.L2
	}
	return &Index{store: store, metric: metric}
}

// Insert writes a canonical vector.
//
// It carries the running model's identity, because a vector without one cannot
// be told apart from a vector produced by a model that has since been replaced.
// The transactional write path — a memory and its vector committing together —
// goes through the record repository instead; this is the rebuild and
// maintenance path.
func (x *Index) Insert(ctx context.Context, t tenant.ID, rid id.ID, v []float32) error {
	const op = "flat.Insert"
	if err := checkScope(t, op); err != nil {
		return err
	}
	if len(v) == 0 {
		return errs.E(errs.Invalid, op, errors.New("an empty vector cannot be indexed"))
	}
	return x.store.Put(ctx, t, tenant.DefaultNamespace, rid, &vector.Vector{
		ModelID: modelOf(ctx),
		Dim:     len(v),
		Values:  v,
	})
}

func (x *Index) Delete(ctx context.Context, t tenant.ID, rid id.ID) error {
	const op = "flat.Delete"
	if err := checkScope(t, op); err != nil {
		return err
	}
	return x.store.Delete(ctx, t, tenant.DefaultNamespace, rid)
}

// Search returns up to k nearest vectors, nearest first.
//
// The filter is applied before a candidate can occupy a slot, so k counts
// results rather than candidates: an index that truncated first and filtered
// afterwards would return fewer results than it holds, without saying so.
func (x *Index) Search(ctx context.Context, t tenant.ID, q []float32, k int, filter vector.Filter) ([]vector.Hit, error) {
	const op = "flat.Search"

	if err := checkScope(t, op); err != nil {
		return nil, err
	}
	if k <= 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("a search must ask for at least one result"))
	}
	if len(q) == 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("the query vector is empty"))
	}

	// A max-heap of the k best so far: the worst kept hit is at the root, so
	// deciding whether a candidate belongs is one comparison and evicting it is
	// one pop. Sorting the whole corpus instead would be O(n log n) in the
	// tenant's entire memory count for every query.
	best := &worstFirst{}
	err := x.store.Scan(ctx, t, func(rid id.ID, v []float32) error {
		if !filter.Allows(rid) {
			return nil
		}
		d, err := distance.Compute(x.metric, q, v)
		if err != nil {
			return errs.E(errs.KindOf(err), op, fmt.Errorf("comparing against record %s: %w", rid, err))
		}
		if best.Len() < k {
			heap.Push(best, vector.Hit{ID: rid, Distance: d})
			return nil
		}
		if d < (*best)[0].Distance {
			(*best)[0] = vector.Hit{ID: rid, Distance: d}
			heap.Fix(best, 0)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Drain the heap: it pops worst-first, so filling the slice backwards puts
	// the nearest hit at index zero.
	out := make([]vector.Hit, best.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(best).(vector.Hit)
	}
	return out, nil
}

// Rebuild replaces the tenant's vectors from src.
//
// Every existing vector is removed first, so the result is what src holds and
// not the union of the two. A rebuild that merged would "recover" a derived
// index into a state the canonical data never had, which is worse than not
// recovering — Invariant 3 is that a derived index is rebuildable, not that it
// is repairable.
//
// For a flat index the source *is* the canonical store, so this is only
// meaningful when src is something else: an import, or a repair from a
// snapshot. It is on the interface because Phase 7's HNSW needs it.
//
// Options are accepted and have nothing to act on. The replacement is a single
// commit, so this rebuild is provably atomic — Invariant 5's other branch — and
// an interrupted one has left nothing to resume.
func (x *Index) Rebuild(ctx context.Context, t tenant.ID, src vector.Source, _ ...vector.RebuildOption) error {
	const op = "flat.Rebuild"

	if err := checkScope(t, op); err != nil {
		return err
	}
	if src == nil {
		return errs.E(errs.Invalid, op, errors.New("a rebuild needs a source"))
	}

	replacements := make(map[id.ID]*vector.Vector)
	if err := src.Scan(ctx, t, func(rid id.ID, v []float32) error {
		if len(v) == 0 {
			return errs.E(errs.Invalid, op, errors.New("an empty vector cannot be indexed"))
		}
		replacements[rid] = &vector.Vector{
			// Deliberately whatever the context carries, which is usually
			// nothing: an empty model id makes vector.Store.Replace keep the
			// model already recorded for that record rather than overwrite it.
			// A rebuild reads coordinates through vector.Source and cannot know
			// what produced them, so claiming to know would be a lie the corpus
			// then carries for ever.
			ModelID: rawModelOf(ctx),
			Dim:     len(v),
			Values:  append([]float32(nil), v...),
		}
		return nil
	}); err != nil {
		return err
	}
	return x.store.Replace(ctx, t, tenant.DefaultNamespace, replacements)
}

func (x *Index) Stats(ctx context.Context, t tenant.ID) (vector.Stats, error) {
	const op = "flat.Stats"
	if err := checkScope(t, op); err != nil {
		return vector.Stats{}, err
	}

	var s vector.Stats
	err := x.store.Scan(ctx, t, func(_ id.ID, v []float32) error {
		s.Vectors++
		s.Dim = len(v)
		return nil
	})
	return s, err
}

// Health is always healthy, and that is a statement rather than a stub.
//
// A flat index has no derived structure: it *is* the canonical vector rows. It
// therefore has nothing that can fall out of step with them, nothing that can
// fail to load and nothing to rebuild — which is precisely why it is the oracle
// the approximate index is measured against. A canonical read that fails is an
// error, not a degradation, and Search reports it as one.
func (x *Index) Health(ctx context.Context, t tenant.ID) (vector.Health, error) {
	const op = "flat.Health"
	if err := checkScope(t, op); err != nil {
		return vector.Health{}, err
	}
	return vector.Health{}, nil
}

func checkScope(t tenant.ID, op string) error {
	if t == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"a vector index operation requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	return nil
}

// modelKey carries the running model's identity into Insert without adding it
// to the vector.Index interface, which deals in plain float slices because that
// is all an index needs to know.
type modelKey struct{}

// WithModel returns ctx carrying the model id that [Index.Insert] stamps onto
// the vectors it writes.
func WithModel(ctx context.Context, modelID string) context.Context {
	return context.WithValue(ctx, modelKey{}, modelID)
}

// rawModelOf is the model the context carries, or empty. Rebuild uses it; see
// the comment there.
func rawModelOf(ctx context.Context) string {
	m, _ := ctx.Value(modelKey{}).(string)
	return m
}

func modelOf(ctx context.Context) string {
	if m := rawModelOf(ctx); m != "" {
		return m
	}
	// Unknown rather than empty: a vector with no model id cannot be stored at
	// all (codec refuses it), and "unknown" is a value a later audit can find
	// and a later migration can act on.
	return vector.UnknownModel
}

// worstFirst is a max-heap of hits by distance.
type worstFirst []vector.Hit

func (h worstFirst) Len() int           { return len(h) }
func (h worstFirst) Less(i, j int) bool { return h[i].Distance > h[j].Distance }
func (h worstFirst) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *worstFirst) Push(x any)        { *h = append(*h, x.(vector.Hit)) }
func (h *worstFirst) Pop() any {
	old := *h
	n := len(old)
	last := old[n-1]
	*h = old[:n-1]
	return last
}
