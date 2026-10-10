package text

import (
	"bytes"
	"slices"
)

// The two heaps keyword search runs on: the cursor heap that merges the terms'
// postings in record-id order, and the bounded selection that keeps the best
// page. They are here rather than in search.go only to keep that file under the
// repository's size limit; [termMerge.rank] is what uses both.

// --- the cursor heap ---------------------------------------------------------

// The heap below sifts by hand on a slice of cursor indices rather than through
// container/heap, for the reason internal/vector/hnsw's two heaps do: that
// package's interface takes its elements as `any`, so a push and a pop each box
// a value and allocate, and this heap is pushed and popped per candidate. The up
// and down loops are container/heap's own with the comparison written out.
//
// Indices rather than cursors or postings because a cursor's key changes every
// time it advances: the heap is ordered on the record id at the cursor's head,
// which lives in [termMerge.postings], and copying a key into the heap would
// mean keeping the copy in step with the cursor.

// beforeCursor reports whether cursor a's head record sorts before cursor b's.
//
// The tiebreak on the cursor index is not cosmetic. Two terms can carry the same
// record — that is the whole point of the merge — and without a total order the
// heap's shape would depend on the order equal keys happened to be pushed in,
// which is a merge that visits the same corpus differently on different runs.
func (m *termMerge) beforeCursor(a, b int) bool {
	if c := bytes.Compare(m.postings[m.terms[a].at].rid[:],
		m.postings[m.terms[b].at].rid[:]); c != 0 {
		return c < 0
	}
	return a < b
}

func (m *termMerge) pushCursor(i int) {
	m.active = append(m.active, i)
	m.upCursor(len(m.active) - 1)
}

func (m *termMerge) popCursor() int {
	n := len(m.active) - 1
	m.active[0], m.active[n] = m.active[n], m.active[0]
	m.downCursor(0, n)
	i := m.active[n]
	m.active = m.active[:n]
	return i
}

func (m *termMerge) upCursor(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !m.beforeCursor(m.active[j], m.active[i]) {
			break
		}
		m.active[i], m.active[j] = m.active[j], m.active[i]
		j = i
	}
}

func (m *termMerge) downCursor(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && m.beforeCursor(m.active[j2], m.active[j1]) {
			j = j2
		}
		if !m.beforeCursor(m.active[j], m.active[i]) {
			break
		}
		m.active[i], m.active[j] = m.active[j], m.active[i]
		i = j
	}
}

// --- the bounded selection ---------------------------------------------------

// topHits keeps the best hits seen and no more.
//
// It is a heap ordered so that the *worst* of the kept hits is at the root: a
// candidate is compared against that one and either discarded or swapped in, so
// the selection costs one comparison for the candidates that lose — which is
// nearly all of them — and holds the page rather than the corpus. Sorting every
// candidate to return ten of them is the cost this removes.
type topHits struct {
	limit int
	hits  []Hit
}

func newTopHits(limit int) topHits {
	// A limit far above what the corpus can offer is a caller's upper bound,
	// not a promise to fill it, so the capacity starts small and grows into
	// what the merge actually produces.
	return topHits{limit: limit, hits: make([]Hit, 0, min(limit, 64))}
}

// push offers one candidate. A candidate no better than the worst hit already
// kept costs exactly one comparison, which is what makes the selection bounded.
func (h *topHits) push(hit Hit) {
	if len(h.hits) < h.limit {
		h.hits = append(h.hits, hit)
		h.up(len(h.hits) - 1)
		return
	}
	if !worseHit(h.hits[0], hit) {
		return
	}
	h.hits[0] = hit
	h.down()
}

// up lifts the hit at i while it is worse than its parent, which is what keeps
// the worst of the kept hits at the root.
func (h *topHits) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !worseHit(h.hits[i], h.hits[parent]) {
			return
		}
		h.hits[i], h.hits[parent] = h.hits[parent], h.hits[i]
		i = parent
	}
}

// down moves the root down until the worst hit is back at the top.
func (h *topHits) down() {
	for i := 0; ; {
		child := 2*i + 1
		if child >= len(h.hits) {
			return
		}
		if right := child + 1; right < len(h.hits) && worseHit(h.hits[right], h.hits[child]) {
			child = right
		}
		if !worseHit(h.hits[child], h.hits[i]) {
			return
		}
		h.hits[i], h.hits[child] = h.hits[child], h.hits[i]
		i = child
	}
}

// ranked is the kept hits, best first.
func (h *topHits) ranked() []Hit {
	slices.SortFunc(h.hits, compareHits)
	return h.hits
}

// compareHits is the result order: by score, descending, and by record id,
// ascending, where two scores are equal.
//
// The tiebreak is not cosmetic. Without a deterministic one the order of tied
// hits falls back to whatever the selection happened to visit, and two
// identical queries return different truncated result *sets* rather than merely
// different orders — the failure query.Fuse records for the same reason.
func compareHits(a, b Hit) int {
	switch {
	case a.Score > b.Score:
		return -1
	case a.Score < b.Score:
		return 1
	}
	return bytes.Compare(a.ID[:], b.ID[:])
}

// worseHit reports whether a ranks below b.
func worseHit(a, b Hit) bool { return compareHits(a, b) > 0 }
