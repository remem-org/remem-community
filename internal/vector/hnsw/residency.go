package hnsw

import (
	"context"
	"sync"

	"github.com/remem-org/remem-go/internal/tenant"
)

// DefaultResidentBudget is how much memory the resident graphs may hold when
// nothing configures it: 512 MiB, which is roughly 300,000 384-dimensional
// vectors and their links.
const DefaultResidentBudget int64 = 512 << 20

// resident is one tenant's graph and everything the cache needs to decide
// whether to keep it.
type resident struct {
	t tenant.ID

	// mu guards g and the fields below it. A search takes it for reading, so
	// searches for one tenant run concurrently; an insert or a removal takes
	// it for writing.
	mu     sync.RWMutex
	g      *graph
	loaded bool
	// degraded is empty when the tenant's answers are complete, and otherwise
	// says why they are not. It is what [Index.Health] reports and what makes a
	// response say truncated rather than quietly returning fewer results.
	degraded   string
	rebuilding bool

	// bytes is what this graph was last measured at, and pin counts the
	// operations currently using it. Both are guarded by Index.mu, not by mu:
	// eviction reads them while the graph itself is busy.
	bytes int64
	pin   int
}

// acquire returns the tenant's resident graph, materialising it if it is not
// already in memory, and pins it against eviction until release is called.
//
// Materialisation is per tenant and lazy because a process serving a thousand
// tenants cannot hold a thousand graphs, and because the tenant a request
// arrives for is the only one it needs. It is single-flight: two concurrent
// searches for a cold tenant read the corpus once.
func (x *Index) acquire(ctx context.Context, t tenant.ID) (*resident, error) {
	x.mu.Lock()
	r, ok := x.tenants[t]
	if !ok {
		r = &resident{t: t}
		x.tenants[t] = r
	}
	r.pin++
	x.touch(t)
	x.mu.Unlock()

	r.mu.RLock()
	loaded := r.loaded
	r.mu.RUnlock()
	if loaded {
		return r, nil
	}

	r.mu.Lock()
	if r.loaded {
		r.mu.Unlock()
		return r, nil
	}
	g, res, err := x.load(ctx, t)
	if err != nil {
		r.mu.Unlock()
		x.release(r)
		return nil, err
	}
	r.g = g
	r.loaded = true
	if res.Corrupt > 0 {
		r.degraded = degradedReason
	}
	r.mu.Unlock()

	x.mu.Lock()
	x.bytes -= r.bytes
	r.bytes = g.residentBytes()
	x.bytes += r.bytes
	x.mu.Unlock()

	if res.Corrupt > 0 && x.rebuilds != nil {
		x.rebuilds.ScheduleRebuild(t, degradedReason)
	}
	x.evict()
	return r, nil
}

func (x *Index) release(r *resident) {
	x.mu.Lock()
	if r.pin > 0 {
		r.pin--
	}
	x.mu.Unlock()
}

// resize re-measures a graph after it changed, so the budget tracks reality
// rather than whatever the tenant cost when it was first loaded.
func (x *Index) resize(r *resident) {
	r.mu.RLock()
	var size int64
	if r.g != nil {
		size = r.g.residentBytes()
	}
	r.mu.RUnlock()

	x.mu.Lock()
	x.bytes += size - r.bytes
	r.bytes = size
	x.mu.Unlock()
}

// touch marks a tenant as most recently used.
//
// The order is a plain slice rather than a linked list because the number of
// resident tenants is bounded by the budget divided by the smallest useful
// graph — hundreds, not millions — and a slice of that size is faster to walk
// than a list is to maintain.
func (x *Index) touch(t tenant.ID) {
	for i, e := range x.order {
		if e == t {
			x.order = append(x.order[:i], x.order[i+1:]...)
			break
		}
	}
	x.order = append(x.order, t)
}

// evict drops least-recently-used tenants until the resident set is inside its
// budget.
//
// Eviction is free and can never lose work, which is the property that makes it
// safe to do at any moment: every change to a graph is written to its node
// records before the call that made it returns, so a dropped graph is
// re-readable from disk and not re-derivable from nothing. A tenant with an
// operation in flight is skipped rather than waited for.
func (x *Index) evict() {
	x.mu.Lock()
	defer x.mu.Unlock()

	for x.bytes > x.budget {
		victim := -1
		for i, t := range x.order {
			if r, ok := x.tenants[t]; ok && r.pin == 0 && r.loaded {
				victim = i
				break
			}
		}
		if victim < 0 {
			return // everything resident is in use; the budget is exceeded and honest about it
		}
		t := x.order[victim]
		r := x.tenants[t]
		x.bytes -= r.bytes
		delete(x.tenants, t)
		x.order = append(x.order[:victim], x.order[victim+1:]...)
	}
}

// forget drops a tenant from memory, so the next acquire re-reads it. It is how
// a rebuild publishes: the new graph is on disk, and the old one in memory is
// simply no longer the truth.
func (x *Index) forget(t tenant.ID) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if r, ok := x.tenants[t]; ok {
		x.bytes -= r.bytes
		delete(x.tenants, t)
	}
	for i, e := range x.order {
		if e == t {
			x.order = append(x.order[:i], x.order[i+1:]...)
			break
		}
	}
}

// Resident reports how many tenants are held in memory and what they are
// measured at. It exists for the residency test and for an operator asking why
// the process is the size it is.
func (x *Index) Resident() (tenants int, bytes int64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.tenants), x.bytes
}

// degradedReason is the sentence an operator sees when a materialisation found
// node records it could not read. It says what was done as well as what is
// wrong, because "index corrupt" alone reads as data loss and none has
// occurred: the canonical vectors are intact and the graph was repaired from
// them.
const degradedReason = "the vector index had unreadable node records and was repaired from the canonical vectors; " +
	"its structure is not the one the configured parameters describe until it is rebuilt"

// Budget is the byte budget the resident set is held to. It is exposed so the
// residency test asserts against the number actually in force rather than the
// one it hoped was in force.
func (x *Index) Budget() int64 { return x.budget }
