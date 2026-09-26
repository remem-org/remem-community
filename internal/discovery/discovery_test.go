package discovery_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
)

// The two numbers Rust names, held to exactly — the threshold inclusively, and
// the bound.
//
// The vectors are built from angles, so each candidate's cosine against the
// subject is the number in its name rather than whatever an embedder happened
// to produce. 0.70 links because the comparison is `>=`, as Rust's is
// (connection_manager.rs:141); 0.6999 does not.
func TestThresholdAndTopKMatchRust(t *testing.T) {
	h := newHarness(t)
	subject := h.store(t, "acme", "the subject", unit(0))

	cosines := []float64{0.99, 0.95, 0.90, 0.85, 0.80, 0.75, 0.70, 0.6999, 0.5, 0.1}
	byID := map[id.ID]float64{}
	for _, c := range cosines {
		m := h.store(t, "acme", fmt.Sprintf("candidate at %v", c), unit(angleFor(c)))
		byID[m.rid] = c
	}

	must(t, h.run(t, "acme", subject.rid))

	edges := h.outEdges(t, "acme", subject.rid)
	if len(edges) != discovery.DefaultTopK {
		t.Fatalf("discovery wrote %d edges, want the top %d", len(edges), discovery.DefaultTopK)
	}
	for _, e := range edges {
		cos, known := byID[e.To]
		if !known {
			t.Fatalf("discovery linked %s, which is not a candidate this test stored", e.To)
		}
		if cos < discovery.DefaultThreshold {
			t.Fatalf("discovery linked a candidate at cosine %v, below the %v threshold",
				cos, discovery.DefaultThreshold)
		}
		if e.Type != graph.SimilarTo {
			t.Fatalf("discovery wrote a %s edge, want similar_to", e.Type)
		}
		// The strength is the similarity, which is what makes traversal's
		// "closest first" survive into find_related (plan §II.10 row 7).
		if diff := float64(e.Strength) - cos; diff > 1e-5 || diff < -1e-5 {
			t.Fatalf("the edge to a candidate at cosine %v has strength %v", cos, e.Strength)
		}
	}
}

// The threshold at the boundary, on its own, because a top-5 bound hides it:
// with only two candidates the bound cannot be what excluded either.
func TestTheThresholdIsInclusive(t *testing.T) {
	h := newHarness(t)
	subject := h.store(t, "acme", "the subject", unit(0))
	on := h.store(t, "acme", "exactly on the threshold", unit(angleFor(float64(discovery.DefaultThreshold))))
	under := h.store(t, "acme", "just under it", unit(angleFor(0.6999)))

	must(t, h.run(t, "acme", subject.rid))

	linked := map[id.ID]bool{}
	for _, e := range h.outEdges(t, "acme", subject.rid) {
		linked[e.To] = true
	}
	if !linked[on.rid] {
		t.Errorf("a candidate exactly at %v was not linked; the comparison is >=",
			discovery.DefaultThreshold)
	}
	if linked[under.rid] {
		t.Errorf("a candidate at 0.6999 was linked; it is below the threshold")
	}
}

// A memory never links to itself. It is not merely useless: graph.Edge.Validate
// refuses one because every traversal would then find its anchor among its own
// neighbours at every depth.
func TestSelfDiscoveryIsExcluded(t *testing.T) {
	h := newHarness(t)
	subject := h.store(t, "acme", "the only memory there is", unit(0))

	must(t, h.run(t, "acme", subject.rid))

	for _, e := range h.outEdges(t, "acme", subject.rid) {
		if e.To == subject.rid {
			t.Fatalf("discovery linked %s to itself", subject.rid)
		}
	}
}

// Invariant 1, on this path. The other tenant's memory is an exact duplicate of
// the subject, so a leak would be the strongest possible hit rather than a
// marginal one.
func TestDiscoveryIsTenantScoped(t *testing.T) {
	h := newHarness(t)
	subject := h.store(t, "acme", "distributed consensus and raft", unit(0))
	stranger := h.store(t, "globex", "distributed consensus and raft", unit(0))

	must(t, h.run(t, "acme", subject.rid))

	for _, e := range h.outEdges(t, "acme", subject.rid) {
		if e.To == stranger.rid {
			t.Fatalf("discovery linked acme's memory to globex's %s", stranger.rid)
		}
	}
	if edges := h.outEdges(t, "globex", stranger.rid); len(edges) != 0 {
		t.Fatalf("a run for acme wrote %d edges into globex", len(edges))
	}
}

// Delivery is at-least-once, so this is the handler's obligation rather than a
// nice property. It is asserted over the whole keyspace, because an edge whose
// created_at moved on the second run is a duplicate by every measure that
// matters even though the edge count did not change.
func TestDiscoveryIsIdempotent(t *testing.T) {
	h := newHarness(t)
	subject := h.store(t, "acme", "the subject", unit(0))
	for _, c := range []float64{0.95, 0.9, 0.5} {
		h.store(t, "acme", fmt.Sprintf("candidate at %v", c), unit(angleFor(c)))
	}

	must(t, h.run(t, "acme", subject.rid))
	first := h.keyspace(t)
	edges := len(h.outEdges(t, "acme", subject.rid))
	if edges == 0 {
		t.Fatal("the first run wrote no edges, so a second one proves nothing")
	}

	h.clk.Advance(time.Hour)
	must(t, h.run(t, "acme", subject.rid))
	second := h.keyspace(t)

	if len(second) != len(first) {
		t.Fatalf("a second run left %d keys where the first left %d", len(second), len(first))
	}
	for k, v := range first {
		got, ok := second[k]
		if !ok {
			t.Fatalf("a second run removed a key the first wrote")
		}
		if got != v {
			t.Fatalf("a second run rewrote a key the first wrote; an edge's created_at moving is a duplicate by every measure that matters")
		}
	}
}

// A relationship is a fact about a pair, and similar_to is symmetric. Rust
// checks the source's out-neighbours only (connection_manager.rs:155-166), so
// discovering A and later discovering B gives it both A→B and B→A.
func TestASymmetricPairIsLinkedOnce(t *testing.T) {
	h := newHarness(t)
	a := h.store(t, "acme", "one half of a near-duplicate pair", unit(0))
	b := h.store(t, "acme", "the other half of it", unit(angleFor(0.98)))

	must(t, h.run(t, "acme", a.rid))
	must(t, h.run(t, "acme", b.rid))

	forward := h.outEdges(t, "acme", a.rid)
	back := h.outEdges(t, "acme", b.rid)
	if len(forward) != 1 {
		t.Fatalf("A points at %d memories, want exactly B", len(forward))
	}
	if len(back) != 0 {
		t.Fatalf("B points at %d memories; the pair was already linked the other way", len(back))
	}
}

// A heuristic must not destroy an assertion. graph.Service.Add replaces, so a
// similar_to written over a hand-made contradicts would silently reverse what
// the user said about the two memories.
func TestDiscoveryNeverOverwritesAnExistingEdge(t *testing.T) {
	h := newHarness(t)
	a := h.store(t, "acme", "the claim", unit(0))
	b := h.store(t, "acme", "the near-identical counter-claim", unit(angleFor(0.99)))
	h.link(t, "acme", a.rid, b.rid, graph.Contradicts, 0.9)

	must(t, h.run(t, "acme", a.rid))

	edges := h.outEdges(t, "acme", a.rid)
	if len(edges) != 1 {
		t.Fatalf("the pair has %d edges, want the one the user made", len(edges))
	}
	if edges[0].Type != graph.Contradicts || edges[0].Strength != 0.9 {
		t.Fatalf("the user's edge became %s at %v", edges[0].Type, edges[0].Strength)
	}
}

// Both directions: an archived memory is retired from retrieval, so linking one
// manufactures a relationship to something no ordinary read reaches.
func TestArchivedMemoriesNeitherDiscoverNorAreDiscovered(t *testing.T) {
	archived := func(r *record.Record) {
		r.Fields.Archived = true
		r.Fields.ArchivedAt = r.CreatedAt
	}

	t.Run("an archived candidate is not linked", func(t *testing.T) {
		h := newHarness(t)
		subject := h.store(t, "acme", "the subject", unit(0))
		gone := h.store(t, "acme", "a retired near-duplicate", unit(angleFor(0.99)), archived)

		must(t, h.run(t, "acme", subject.rid))

		for _, e := range h.outEdges(t, "acme", subject.rid) {
			if e.To == gone.rid {
				t.Fatalf("discovery linked the archived memory %s", gone.rid)
			}
		}
	})

	t.Run("an archived subject discovers nothing", func(t *testing.T) {
		h := newHarness(t)
		subject := h.store(t, "acme", "the retired subject", unit(0), archived)
		h.store(t, "acme", "a live near-duplicate", unit(angleFor(0.99)))

		must(t, h.run(t, "acme", subject.rid))

		if edges := h.outEdges(t, "acme", subject.rid); len(edges) != 0 {
			t.Fatalf("an archived memory grew %d relationships", len(edges))
		}
	})
}

// A memory hard-deleted between its write and its discovery is an ordinary
// race. Failing would retry six times and leave a Failed row an operator has to
// read to discover that nothing was wrong.
func TestAVanishedSubjectIsSkippedNotFailed(t *testing.T) {
	h := newHarness(t)
	present := h.store(t, "acme", "still here", unit(0))
	h.store(t, "acme", "its near-duplicate", unit(angleFor(0.99)))

	// A subject that was never stored stands for one deleted before the job ran.
	if err := h.run(t, "acme", id.New(), present.rid); err != nil {
		t.Fatalf("a vanished subject failed the job: %v", err)
	}
	if edges := h.outEdges(t, "acme", present.rid); len(edges) == 0 {
		t.Fatal("the run stopped at the vanished subject instead of continuing to the live one")
	}
}

// Every signal is populated for every candidate, including the three the
// shipped strategy ignores. That is what keeps a future strategy a new
// implementation rather than a change to retrieval.
func TestCandidatesCarryEverySignal(t *testing.T) {
	var seen []discovery.Candidate
	h := newHarness(t, func(d *discovery.Deps) {
		d.Strategy = capturingStrategy{onto: &seen}
	})

	subject := h.store(t, "acme", "raft consensus leader election in a cluster", unit(0))
	h.clk.Advance(48 * time.Hour)
	near := h.store(t, "acme", "raft consensus leader election explained", unit(angleFor(0.9)))
	// Connected but distant in the vector space, so it can only have arrived
	// through the traversal — which is what makes GraphDist meaningful here.
	far := h.store(t, "acme", "marzipan recipes from a Bavarian bakery", unit(angleFor(0.05)))
	h.link(t, "acme", subject.rid, far.rid, graph.RelatedTo, 0.9)

	must(t, h.run(t, "acme", subject.rid))

	byID := map[id.ID]discovery.Candidate{}
	for _, c := range seen {
		byID[c.Record.ID] = c
	}
	n, ok := byID[near.rid]
	if !ok {
		t.Fatal("the nearest memory was not a candidate")
	}
	f, ok := byID[far.rid]
	if !ok {
		t.Fatal("the graph neighbour was not a candidate; the traversal proposes as well as measures")
	}

	// Vector similarity: recovered from the index for the searched candidate,
	// computed from the stored vectors for the one the traversal proposed.
	if diff := float64(n.VectorSim) - 0.9; diff > 1e-5 || diff < -1e-5 {
		t.Errorf("the near candidate's VectorSim is %v, want 0.9", n.VectorSim)
	}
	if diff := float64(f.VectorSim) - 0.05; diff > 1e-5 || diff < -1e-5 {
		t.Errorf("the graph-proposed candidate's VectorSim is %v, want 0.05", f.VectorSim)
	}
	// Text similarity: shared terms over the union, and zero for a pair with
	// none in common.
	if n.TextSim <= 0 {
		t.Errorf("the near candidate shares four words with the subject and scored TextSim %v", n.TextSim)
	}
	if f.TextSim != 0 {
		t.Errorf("a candidate sharing no terms scored TextSim %v", f.TextSim)
	}
	// Graph distance: one hop for the linked memory, unreachable for the rest.
	if f.GraphDist != 1 {
		t.Errorf("a directly linked memory has GraphDist %d, want 1", f.GraphDist)
	}
	if n.GraphDist != discovery.Unreachable {
		t.Errorf("an unconnected memory has GraphDist %d, want %d", n.GraphDist, discovery.Unreachable)
	}
	// Temporal context, unsigned: order is not a signal.
	if n.AgeDelta != 48*time.Hour {
		t.Errorf("the near candidate's AgeDelta is %v, want 48h", n.AgeDelta)
	}
}

// capturingStrategy records what retrieval produced and links nothing, so a
// test can assert about candidates without the edges changing the corpus under
// it.
type capturingStrategy struct{ onto *[]discovery.Candidate }

func (c capturingStrategy) Name() string { return "capturing" }

func (c capturingStrategy) Evaluate(_ context.Context, _ *record.Record,
	cands []discovery.Candidate) ([]graph.Edge, error) {
	*c.onto = append(*c.onto, cands...)
	return nil, nil
}

// A run interrupted partway resumes from where it stopped rather than from the
// start, which is what jobs.Checkpointer is for.
func TestAnInterruptedBatchResumes(t *testing.T) {
	h := newHarness(t)
	subjects := make([]id.ID, 4)
	for i := range subjects {
		subjects[i] = h.store(t, "acme", fmt.Sprintf("memory %d", i), unit(angleFor(0.99))).rid
	}

	payload, err := discovery.EncodePayload(subjects)
	must(t, err)
	cp := &recordingCheckpointer{}
	j := &jobs.Job{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: "discovery.similar", Payload: payload,
	}
	must(t, h.disc.Handle(context.Background(), j, cp))

	if len(cp.saved) != len(subjects)-1 {
		t.Fatalf("a four-subject job saved %d checkpoints, want one after each but the last", len(cp.saved))
	}
	// The cursor moves strictly forward, and every checkpoint is the payload
	// encoding of what is left.
	for i, c := range cp.saved {
		rest, err := discovery.DecodePayload(c)
		must(t, err)
		want := subjects[i+1:]
		if len(rest) != len(want) {
			t.Fatalf("checkpoint %d holds %d subjects, want %d", i, len(rest), len(want))
		}
		for k := range want {
			if rest[k] != want[k] {
				t.Fatalf("checkpoint %d holds %s at %d, want %s", i, rest[k], k, want[k])
			}
		}
	}

	// Resuming from the last checkpoint does the remaining subject and no other.
	j.Checkpoint = cp.saved[len(cp.saved)-1]
	before := h.keyspace(t)
	must(t, h.disc.Handle(context.Background(), j, nil))
	if len(h.keyspace(t)) != len(before) {
		t.Fatal("resuming rewrote work the first pass had already done")
	}
}

// A job with no tenant is refused rather than answered for whichever tenant the
// process happens to be serving (Invariant 1).
func TestAnUnscopedJobIsRefused(t *testing.T) {
	h := newHarness(t)
	payload, err := discovery.EncodePayload([]id.ID{id.New()})
	must(t, err)
	j := &jobs.Job{ID: id.New(), Type: "discovery.similar", Payload: payload}

	if err := h.disc.Handle(context.Background(), j, nil); err == nil {
		t.Fatal("a discovery job with no tenant ran")
	}
}

// TestARunReportsTheSubjectsItConsideredAndTheEdgesItWrote holds the counts the
// attempt history shows. The unit is a subject, not a candidate: the candidates
// a run scores are the cost of considering a subject rather than the work, and
// a run's "changed" is the edges it wrote — which is what a second run over the
// same subjects produces none of.
func TestARunReportsTheSubjectsItConsideredAndTheEdgesItWrote(t *testing.T) {
	h := newHarness(t)
	// Two near-identical memories: the second's run links it to the first.
	first := h.store(t, "acme", "the invoice was paid on tuesday", unit(angleFor(0.99))).rid
	second := h.store(t, "acme", "the invoice was paid on tuesday", unit(angleFor(0.99))).rid
	_ = first

	payload, err := discovery.EncodePayload([]id.ID{second})
	must(t, err)
	cp := &recordingCheckpointer{}
	must(t, h.disc.Handle(context.Background(), &jobs.Job{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: "discovery.similar", Payload: payload,
	}, cp))

	if cp.processed == nil || *cp.processed != 1 {
		t.Fatalf("the run reported %v subjects, want 1", cp.processed)
	}
	if cp.changed == nil || *cp.changed != 1 {
		t.Fatalf("the run reported %v edges, want 1", cp.changed)
	}

	// A second run over the same subject writes no edge, and says so rather
	// than leaving the count unavailable: nothing changed is a real answer.
	again := &recordingCheckpointer{}
	must(t, h.disc.Handle(context.Background(), &jobs.Job{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: "discovery.similar", Payload: payload,
	}, again))
	if again.changed == nil || *again.changed != 0 {
		t.Fatalf("a second run reported %v edges, want 0", again.changed)
	}
	if again.processed == nil || *again.processed != 1 {
		t.Fatalf("a second run reported %v subjects, want 1", again.processed)
	}
}
