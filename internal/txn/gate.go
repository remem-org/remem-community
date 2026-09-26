package txn

import (
	"hash/fnv"
	"sync"
)

// Gate serialises writers that contend on a row every write in a scope
// touches.
//
// # Why a lock, when the write is already conditional
//
// The conditional commit is what makes the corpus statistics exact: a lost
// increment is an inverse document frequency that is silently wrong from then
// on. What it is not is a way to make contention go away — it turns a lost
// update into a conflict, and a conflict into a retry.
//
// Measured on the in-memory store, which is the worst case because a commit is
// microseconds and the window a conflict can open in is most of it: sixteen
// writers into one tenant cost 1.6 attempts per write; thirty-two cost 1.8 and
// failed one write in five hundred; sixty-four failed eight in a thousand.
// A write refused because of an index's bookkeeping is not a cost a user should
// pay, and raising the attempt budget only moves the number.
//
// Taking the lock before the transaction removes the contention rather than
// absorbing it. The cost is that writes into one tenant commit one at a time —
// which is what the durability fsync already does, and which is far above what
// the embedding call ahead of every write can feed. Writes into *different*
// tenants do not wait for each other, which is the property that matters for a
// multi-tenant server.
//
// # Phase 14 does not inherit a debt from this
//
// A process-local lock looks like something a replicated system would have to
// replace. It is the reverse: raft gives every write a total order through the
// leader's log, so the ordering this provides on one node is the ordering
// consensus provides on many. This is the local stand-in, not a shortcut around
// the problem.
type Gate struct {
	stripes [gateStripes]sync.Mutex
}

// NewGate returns a gate. It is a constructor rather than a usable zero value
// because a Gate holds mutexes and must never be copied, and a constructor
// returning a pointer is the shape that says so.
func NewGate() *Gate { return &Gate{} }

// gateStripes is how many mutexes a gate holds.
//
// Striped rather than one lock per key, so the structure is bounded and needs
// no reference counting or eviction — a map of live locks keyed by tenant is a
// leak in a system that provisions tenants. Two tenants landing on one stripe
// wait for each other, which costs throughput and never correctness, and with
// 256 stripes it is rare enough not to matter.
const gateStripes = 256

// Lock takes the gate for key and returns the function that releases it.
//
// It is deliberately shaped for `defer unlock()` at the top of a write, so that
// every path out of that write — including a panic — releases it.
func (g *Gate) Lock(key string) (unlock func()) {
	mu := &g.stripes[stripe(key)]
	mu.Lock()
	return mu.Unlock
}

// stripe is deterministic and unseeded, so which tenants share a stripe is a
// property of the code rather than of the process. A seeded hash would make one
// deployment's contention profile unreproducible in another, and the only thing
// a seed would buy — that two particular tenants do not collide everywhere —
// costs throughput at worst and never correctness.
func stripe(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % gateStripes
}
