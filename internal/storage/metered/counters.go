package metered

import (
	"encoding/binary"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// maxScopes bounds the counter cache. A scope is one tenant's one space, so the
// bound is a tenant count, not an operation count; past it the cache stops
// growing and lookups take the uncached path, which is slower and still right.
const maxScopes = 1 << 16

// counters caches one labelled counter per key scope — the tenant, namespace and
// space bytes every key opens with.
//
// It exists because measurement said so. Without it a counted write cost about
// 240ns more than an uncounted one on the in-memory store, over the plan's
// threshold of 200ns, and the cost was two allocations: the tenant bytes turned
// into a label string, and the variadic label slice. Keying the cache by the
// scope's raw bytes removes both, because a map index expression of the form
// m[string(b)] does not allocate.
type counters struct {
	vec *prometheus.CounterVec

	mu      sync.RWMutex
	byScope map[string]prometheus.Counter
}

func newCounters(vec *prometheus.CounterVec) *counters {
	return &counters{vec: vec, byScope: map[string]prometheus.Counter{}}
}

// counter returns the counter a key's operations are recorded on.
func (c *counters) counter(k []byte) prometheus.Counter {
	scope := scopeOf(k)
	if scope == nil {
		// Unparseable, so not cached: a key that does not decode is exactly the
		// one whose bytes cannot be trusted to name a stable scope.
		t, op := labels(k)
		return c.vec.WithLabelValues(t, op)
	}

	c.mu.RLock()
	ctr, ok := c.byScope[string(scope)]
	c.mu.RUnlock()
	if ok {
		return ctr
	}

	t, op := labels(k)
	ctr = c.vec.WithLabelValues(t, op)
	c.mu.Lock()
	if len(c.byScope) < maxScopes {
		c.byScope[string(scope)] = ctr
	}
	c.mu.Unlock()
	return ctr
}

// scopeOf returns the prefix of k up to and including its space byte, or nil
// when k does not have that shape. It mirrors keys' layout —
// <uvarint tenant length><tenant><uvarint namespace length><namespace><space> —
// and decides only where the scope ends; labels still decides what it means, so
// a scope whose space byte is unknown is cached under the label keys gives it.
func scopeOf(k []byte) []byte {
	end := 0
	for range 2 { // tenant, then namespace
		n, w := binary.Uvarint(k[end:])
		if w <= 0 || uint64(len(k)-end-w) < n {
			return nil
		}
		end += w + int(n)
	}
	if end >= len(k) {
		return nil
	}
	return k[:end+1]
}
