package discovery_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Two memories written together are two subjects, and the worker pool runs
// them at once. Both then ask "is this pair already linked", both are told no
// because neither has committed, and both write — leaving A→B and B→A for a
// relationship that is symmetric.
//
// TestASymmetricPairIsLinkedOnce cannot see this: it runs the two subjects one
// after the other, which is the case where reading before writing is enough.
// The defect was found by driving the real binary — two invoices came back
// linked in both directions, one millisecond apart.
//
// Twenty pairs rather than one, because a race that reproduces once in five
// runs is a test that passes four times out of five.
func TestConcurrentDiscoveryOfAPairLinksItOnce(t *testing.T) {
	const pairs = 20
	h := newHarness(t)

	type pair struct{ a, b id.ID }
	all := make([]pair, 0, pairs)
	for i := range pairs {
		// Each pair gets its own two dimensions, so pairs are exactly
		// orthogonal to each other and only a memory's own partner clears the
		// threshold. Angles in one plane would not do: twenty of them wrap
		// round and neighbouring pairs become each other's candidates.
		a := h.store(t, "acme", fmt.Sprintf("pair %d, first half", i), inPlane(pairs, i, 0))
		b := h.store(t, "acme", fmt.Sprintf("pair %d, second half", i), inPlane(pairs, i, angleFor(0.999)))
		all = append(all, pair{a: a.rid, b: b.rid})
	}

	// One barrier for the whole batch, so the two halves of every pair are in
	// flight at the same moment.
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	run := func(subject id.ID) {
		defer done.Done()
		payload, err := discovery.EncodePayload([]id.ID{subject})
		if err != nil {
			t.Error(err)
			return
		}
		j := &jobs.Job{ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
			Type: "discovery.similar", Payload: payload}
		start.Wait()
		if err := h.disc.Handle(context.Background(), j, nil); err != nil {
			t.Errorf("discovering %s: %v", subject, err)
		}
	}
	for _, p := range all {
		done.Add(2)
		go run(p.a)
		go run(p.b)
	}
	start.Done()
	done.Wait()

	for i, p := range all {
		out := len(h.outEdges(t, "acme", p.a)) + len(h.outEdges(t, "acme", p.b))
		if out != 1 {
			t.Fatalf("pair %d has %d edges between its two halves, want 1: "+
				"a symmetric relationship written twice is a duplicate, and the second run "+
				"of either subject would then never converge", i, out)
		}
	}
}

// inPlane is a unit vector lying in the two dimensions belonging to pair i, at
// the given angle within that plane. Two vectors from different planes have
// cosine exactly zero.
func inPlane(pairs, i int, theta float64) []float32 {
	v := make([]float32, 2*pairs)
	v[2*i] = float32(math.Cos(theta))
	v[2*i+1] = float32(math.Sin(theta))
	return v
}

// The same race, one pair at a time, asserted on the shape rather than the
// count: whichever subject wins, the pair is connected in exactly one
// direction and the loser wrote nothing.
func TestTheLoserOfAConcurrentPairWritesNothing(t *testing.T) {
	h := newHarness(t)
	a := h.store(t, "acme", "one half", unit(0))
	b := h.store(t, "acme", "the other half", unit(angleFor(0.999)))

	var wg sync.WaitGroup
	for _, subject := range []id.ID{a.rid, b.rid} {
		wg.Add(1)
		go func(s id.ID) {
			defer wg.Done()
			payload, err := discovery.EncodePayload([]id.ID{s})
			if err != nil {
				t.Error(err)
				return
			}
			if err := h.disc.Handle(context.Background(), &jobs.Job{
				ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
				Type: "discovery.similar", Payload: payload,
			}, nil); err != nil {
				t.Errorf("discovering %s: %v", s, err)
			}
		}(subject)
	}
	wg.Wait()

	fromA := h.outEdges(t, "acme", a.rid)
	fromB := h.outEdges(t, "acme", b.rid)
	switch {
	case len(fromA) == 1 && len(fromB) == 0:
		if fromA[0].To != b.rid || fromA[0].Type != graph.SimilarTo {
			t.Fatalf("A's edge is %+v", fromA[0])
		}
	case len(fromB) == 1 && len(fromA) == 0:
		if fromB[0].To != a.rid || fromB[0].Type != graph.SimilarTo {
			t.Fatalf("B's edge is %+v", fromB[0])
		}
	default:
		t.Fatalf("the pair has %d edges out of A and %d out of B, want one edge in total",
			len(fromA), len(fromB))
	}
}
