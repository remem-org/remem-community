package hnsw

import (
	"context"
	"errors"
	"sync"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// Options configure an index.
type Options struct {
	// Params are the graph's shape and the search's effort. Zero fields take
	// the values in [Defaults].
	Params Params
	// Metric is how vectors are compared. Zero means L2.
	Metric distance.Metric
	// ResidentBudgetBytes bounds the memory the resident graphs may hold
	// together. Zero means [DefaultResidentBudget].
	ResidentBudgetBytes int64
	// ModelID is the model this server embeds with. When it is set, a tenant
	// whose canonical vectors name another model is refused rather than
	// searched — see [Index.Health].
	ModelID string
	// Rebuilds is told when a tenant's index was found damaged. Nil means
	// nobody is told, which is survivable because a materialisation repairs
	// what it can, and unhelpful, because nothing then restores the structure.
	Rebuilds vector.Rebuilder
}

// Index is the approximate vector index.
//
// It holds one graph per tenant, materialised on demand and evicted under a
// byte budget. Everything it writes is a node record in the vector-index key
// space; it never writes a canonical vector except through [Index.Insert] and
// [Index.Rebuild], which are the two entry points that exist to *put* vectors
// into a tenant rather than to index ones already there.
type Index struct {
	kv      storage.KV
	store   *vector.Store
	params  Params
	metric  distance.Metric
	modelID string
	budget  int64

	rebuilds vector.Rebuilder

	// onInsert is called once per record a rebuild inserts. It is nil in
	// production and exists so a test can count what a resumed rebuild redid.
	onInsert func()

	// mu guards the residency cache: the map, the LRU order and the byte
	// total. It is never held while a graph is being read or written.
	mu      sync.Mutex
	tenants map[tenant.ID]*resident
	order   []tenant.ID
	bytes   int64
}

var (
	_ vector.Index      = (*Index)(nil)
	_ vector.Maintainer = (*Index)(nil)
	_ vector.Granular   = (*Index)(nil)
)

// New returns an index over kv.
func New(kv storage.KV, opts Options) (*Index, error) {
	p := opts.Params.withDefaults()
	if err := p.Validate(); err != nil {
		return nil, err
	}
	m := opts.Metric
	if m == 0 {
		m = distance.L2
	}
	budget := opts.ResidentBudgetBytes
	if budget <= 0 {
		budget = DefaultResidentBudget
	}
	return &Index{
		kv:       kv,
		store:    vector.NewStore(kv),
		params:   p,
		metric:   m,
		modelID:  opts.ModelID,
		budget:   budget,
		rebuilds: opts.Rebuilds,
		tenants:  map[tenant.ID]*resident{},
	}, nil
}

// Insert writes a canonical vector and indexes it.
//
// It does both, exactly as flat.Insert does, so that the two implementations of
// vector.Index mean the same thing by the same call: after Insert returns, a
// search finds the vector, and after a restart it still does. The production
// write path does not come through here — a memory and its vector commit
// together in one transaction, and [Index.Indexed] is what tells the graph
// afterwards.
func (x *Index) Insert(ctx context.Context, t tenant.ID, rid id.ID, entries []vector.Entry) error {
	const op = "hnsw.Insert"
	if err := checkScope(t, op); err != nil {
		return err
	}
	if err := vector.CheckEntries(entries); err != nil {
		return errs.E(errs.Invalid, op, err)
	}
	if err := x.store.PutEntries(ctx, t, tenant.DefaultNamespace, rid, x.stampedModel(), entries); err != nil {
		return err
	}
	return x.Indexed(ctx, t, rid, entries)
}

// Indexed makes a memory's already-committed entry set searchable, replacing
// whatever entries the graph held for it.
func (x *Index) Indexed(ctx context.Context, t tenant.ID, rid id.ID, entries []vector.Entry) error {
	const op = "hnsw.Indexed"
	if err := checkScope(t, op); err != nil {
		return err
	}
	if err := vector.CheckEntries(entries); err != nil {
		return errs.E(errs.Invalid, op, err)
	}
	r, err := x.acquire(ctx, t)
	if err != nil {
		return err
	}
	defer x.release(r)

	r.mu.Lock()
	// No defensive copy: allocSlot copies the vector into the graph's arena,
	// so the caller's slice is never retained.
	dirty, err := r.g.setEntries(rid, entries)
	if err != nil {
		// A failed insert may still have changed the graph — a replacement
		// removes the old nodes first — and the budget bounds memory, not
		// success.
		sz := r.measure()
		r.mu.Unlock()
		x.charge(r, sz)
		return errs.E(errs.KindOf(err), op, err)
	}
	// Unsynced, unlike the record batch this follows. A node record is derived,
	// and what a crash must not lose is the canonical vector, which the main
	// batch already committed durably. Losing these neighbour lists costs the
	// next materialisation an insert — it reconciles a canonical vector with no
	// node record — and costs no memory. Paying a second fsync on the write
	// path to avoid that work is the wrong trade.
	tx := txn.New(x.kv, txn.Sync(false))
	staged := x.stage(tx, t, r.g, dirty)
	sz := r.measure()
	r.mu.Unlock()
	defer tx.Close()

	// Charged whether or not the node records commit: the graph in memory
	// changed either way, and memory is what the budget bounds.
	x.charge(r, sz)
	if staged == 0 {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	x.evict()
	return nil
}

// Delete removes a memory: its canonical rows and every node of it together.
func (x *Index) Delete(ctx context.Context, t tenant.ID, rid id.ID) error {
	const op = "hnsw.Delete"
	if err := checkScope(t, op); err != nil {
		return err
	}
	if err := x.store.DeleteEntries(ctx, t, tenant.DefaultNamespace, rid); err != nil {
		return err
	}
	return x.Removed(ctx, t, rid)
}

// Removed drops a record whose canonical vector has already been deleted.
//
// It writes the neighbour lists it repaired, and correctness does not depend on
// that landing. The record's canonical vector is already gone, so nothing can
// materialise the node again whatever happens next — which is the property this
// package is built around, and the reason there is no tombstone to write and no
// sweep to run.
func (x *Index) Removed(ctx context.Context, t tenant.ID, rid id.ID) error {
	const op = "hnsw.Removed"
	if err := checkScope(t, op); err != nil {
		return err
	}
	r, err := x.acquire(ctx, t)
	if err != nil {
		return err
	}
	defer x.release(r)

	r.mu.Lock()
	dirty := r.g.removeRecord(rid)
	if len(dirty) == 0 {
		r.mu.Unlock()
		return nil // the post-condition already holds
	}
	tx := txn.New(x.kv, txn.Sync(false))
	x.stage(tx, t, r.g, dirty)
	sz := r.measure()
	r.mu.Unlock()
	defer tx.Close()

	// Charged before the commit, for the reason Indexed gives.
	x.charge(r, sz)
	return tx.Commit(ctx)
}

// Search returns up to k distinct memories, nearest first, each at the distance
// of its nearest entry.
//
// The walk groups by memory (see [graph.searchGrouped]), so ef bounds memories
// rather than passages and one memory with a thousand passages cannot crowd the
// rest out of the candidate list. The filter is applied before a candidate may
// occupy a slot, and the candidate list widens until k results survive it or
// the whole graph has been examined. That is what makes k count results rather
// than candidates on an approximate index: a search that took the first ef
// candidates and filtered afterwards would return three results out of ten and
// give no sign that seven were discarded rather than absent.
func (x *Index) Search(ctx context.Context, t tenant.ID, q []float32, k int, filter vector.Filter) ([]vector.Hit, error) {
	const op = "hnsw.Search"

	if err := checkScope(t, op); err != nil {
		return nil, err
	}
	if k <= 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("a search must ask for at least one result"))
	}
	if len(q) == 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("the query vector is empty"))
	}

	r, err := x.acquire(ctx, t)
	if err != nil {
		return nil, err
	}
	defer x.release(r)

	r.mu.RLock()
	defer r.mu.RUnlock()

	ef := x.params.EfSearch
	if ef < k {
		ef = k
	}
	// One result slice for the whole widening loop: a retry starts the page
	// again from the first candidate, so it wants the same k slots emptied, not
	// another k allocated.
	hits := make([]vector.Hit, 0, k)
	for {
		found, err := r.g.nearestMemories(q, ef)
		if err != nil {
			return nil, errs.E(errs.KindOf(err), op, err)
		}
		hits = hits[:0]
		for _, c := range found {
			n := r.g.nodes[c.node]
			if n == nil || !filter.Allows(n.key.ID) {
				continue
			}
			hits = append(hits, vector.Hit{ID: n.key.ID, Passage: n.key.Passage, Distance: c.dist})
			if len(hits) == k {
				return hits, nil
			}
		}
		// Either the graph has been exhausted or there is nothing more to
		// find; in both cases what has been gathered is the whole answer.
		if len(found) < ef || ef >= r.g.live {
			return hits, nil
		}
		ef *= 2
	}
}

// SearchGranularity is ef_search: the walk runs at max(ef_search, k), so a
// search for fewer candidates than this costs the same and returns less.
//
// It is what [vector.Granular] documents, and the number a filtered query's
// widening budget rounds up to.
func (x *Index) SearchGranularity() int { return x.params.EfSearch }

// Stats describes the tenant's index.
func (x *Index) Stats(ctx context.Context, t tenant.ID) (vector.Stats, error) {
	const op = "hnsw.Stats"
	if err := checkScope(t, op); err != nil {
		return vector.Stats{}, err
	}
	r, err := x.acquire(ctx, t)
	if err != nil {
		return vector.Stats{}, err
	}
	defer x.release(r)

	r.mu.RLock()
	defer r.mu.RUnlock()
	return vector.Stats{Vectors: r.g.live, Dim: r.g.dim, Rebuilding: r.rebuilding}, nil
}

// Health reports whether this tenant's searches can be complete.
//
// It materialises the tenant, because a tenant that has never been read is a
// tenant whose index nobody has looked at — and answering "healthy" for one is
// the reassuring lie this method exists to avoid.
func (x *Index) Health(ctx context.Context, t tenant.ID) (vector.Health, error) {
	const op = "hnsw.Health"
	if err := checkScope(t, op); err != nil {
		return vector.Health{}, err
	}
	r, err := x.acquire(ctx, t)
	if err != nil {
		return vector.Health{}, err
	}
	defer x.release(r)

	r.mu.RLock()
	defer r.mu.RUnlock()
	switch {
	case r.rebuilding:
		return vector.Health{Degraded: true, Reason: "the vector index is being rebuilt for this tenant"}, nil
	case r.degraded != "":
		return vector.Health{Degraded: true, Reason: r.degraded}, nil
	}
	return vector.Health{}, nil
}

// stampedModel is what Insert writes onto a canonical vector.
func (x *Index) stampedModel() string {
	if x.modelID != "" {
		return x.modelID
	}
	// Unknown rather than empty: the codec refuses a vector with no model id at
	// all, and "unknown" is a value a later audit can find.
	return vector.UnknownModel
}

func checkScope(t tenant.ID, op string) error {
	if t == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"a vector index operation requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	return nil
}
