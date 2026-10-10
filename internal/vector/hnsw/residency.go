package hnsw

import (
	"context"
	"fmt"
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

	// measured numbers each measurement of g, and is guarded by mu: a
	// measurement is taken while the write that changed the graph still holds
	// it. See [resident.measure].
	measured uint64

	// bytes is what this graph was last charged at, charged is the measurement
	// that charge came from, and pin counts the operations currently using it.
	// All three are guarded by Index.mu, not by mu: eviction reads them while
	// the graph itself is busy.
	bytes   int64
	charged uint64
	pin     int
}

// size is one measurement of a resident graph: what it costs, and where that
// measurement falls in the order of the graph's writes.
type size struct {
	bytes int64
	seq   uint64
}

// measure takes the graph's size. The caller holds mu for writing — the lock
// the write that changed the graph already holds — so measuring costs no lock
// of its own and nothing waits behind it. It is O(1): the graph keeps its size
// as running counts.
func (r *resident) measure() size {
	r.measured++
	var b int64
	if r.g != nil {
		b = r.g.residentBytes()
	}
	return size{bytes: b, seq: r.measured}
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
	reason := degradedReason(res)
	r.g = g
	r.loaded = true
	r.degraded = reason
	sz := r.measure()
	r.mu.Unlock()
	x.charge(r, sz)

	// Only the repairable damage asks for a repair. A vector the arena cannot
	// hold leaves the tenant degraded and nothing else: a rebuild would rewrite
	// the same row at the same width and find it unplaceable again, so asking
	// for one here would queue a job on every materialisation for ever, at
	// whatever rate the server throttles rebuilds to, and never resolve.
	if res.Corrupt > 0 && x.rebuilds != nil {
		x.rebuilds.ScheduleRebuild(t, reason)
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

// charge moves the budget to a graph's measured size, so it tracks reality
// rather than whatever the tenant cost when it was first loaded.
//
// It takes only the index-wide lock. The measurement was taken under the
// graph's write lock by the write that changed it; re-taking the graph's lock
// here to measure afterwards was the read lock finding 14 E7 profiled writers
// waiting behind.
//
// Two writes to one tenant measure in the order they held the graph, and may
// charge in either order. A measurement older than the one already charged is
// therefore dropped: applying it would set the budget back to a graph that no
// longer exists.
func (x *Index) charge(r *resident, sz size) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if sz.seq <= r.charged {
		return
	}
	x.bytes += sz.bytes - r.bytes
	r.bytes = sz.bytes
	r.charged = sz.seq
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

// degradedReason is what [vector.Health] reports for a tenant one
// materialisation found something wrong with, and it is empty — the tenant is
// not degraded — when it found nothing.
//
// It distinguishes the two kinds of damage because the operator's next move
// differs. Unreadable node records have already been repaired and a rebuild
// finishes the job; a vector the arena cannot hold has not been and cannot be,
// and the sentence has to say so, or an operator reads "rebuild" and runs one
// that changes nothing. Both at once is a real state and reads as both.
func degradedReason(res loadResult) string {
	switch {
	case res.Corrupt > 0 && res.Unplaceable > 0:
		return repairedReason + "; and separately, " + unplaceableReason(res.Unplaceable, res.Width)
	case res.Corrupt > 0:
		return repairedReason
	case res.Unplaceable > 0:
		return unplaceableReason(res.Unplaceable, res.Width)
	}
	return ""
}

// repairedReason is the sentence an operator sees when a materialisation found
// node records it could not read. It says what was done as well as what is
// wrong, because "index corrupt" alone reads as data loss and none has
// occurred: the canonical vectors are intact and the graph was repaired from
// them.
const repairedReason = "the vector index had unreadable node records and was repaired from the canonical vectors; " +
	"its structure is not the one the configured parameters describe until it is rebuilt"

// unplaceableReason is the sentence for canonical vectors the arena cannot
// hold. It names the count and the tenant's width because those are what an
// operator needs to find the rows, and it says plainly that a rebuild is not
// the remedy: the vectors themselves are the wrong shape, so only re-embedding
// the records they belong to brings them back into the index.
func unplaceableReason(n, width int) string {
	if width == 0 {
		return fmt.Sprintf("all %d of this tenant's canonical vectors are empty, so there is nothing to index; "+
			"a rebuild cannot fix this and the records must be re-embedded", n)
	}
	return fmt.Sprintf("%d of this tenant's canonical vectors are empty or of another width than the tenant's %d "+
		"and cannot be searched until they are re-embedded; a rebuild cannot fix this, "+
		"because the vector arena holds one width and these rows are not of it", n, width)
}

// Budget is the byte budget the resident set is held to. It is exposed so the
// residency test asserts against the number actually in force rather than the
// one it hoped was in force.
func (x *Index) Budget() int64 { return x.budget }
