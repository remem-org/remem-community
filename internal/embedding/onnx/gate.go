package onnx

import "sync"

// passGate admits one forward pass at a time, and admits a waiting query before
// a waiting document pass.
//
// It exists because a document can be very long and a query is what a user is
// waiting on. A 1 MiB memory is about seventy full segments, each a pass of
// many seconds; with a plain mutex a search arriving behind it waits for the
// whole document. Here each pass is admitted separately, so a long document
// yields to queries between its passes, and a search waits for at most the one
// pass already running. Document passes keep their order among themselves; a
// steady stream of queries can delay a write, which is the right way round.
//
// Fairness among queries, and among documents, is whatever sync.Cond gives,
// which is no promise at all. Each kind is batched before it gets here, so the
// waiters are few.
type passGate struct {
	mu      sync.Mutex
	cond    *sync.Cond
	busy    bool
	queries int // queries waiting to be admitted
	waiting int // passes of either kind waiting, for tests
}

func newPassGate() *passGate {
	g := &passGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// acquire blocks until this pass may run.
func (g *passGate) acquire(query bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.waiting++
	if query {
		g.queries++
	}
	for g.busy || (!query && g.queries > 0) {
		g.cond.Wait()
	}
	if query {
		g.queries--
	}
	g.waiting--
	g.busy = true
}

// release ends the running pass.
func (g *passGate) release() {
	g.mu.Lock()
	g.busy = false
	g.mu.Unlock()
	g.cond.Broadcast()
}

// waiters reports how many passes are waiting to be admitted.
func (g *passGate) waiters() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waiting
}
