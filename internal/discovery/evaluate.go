package discovery

import (
	"context"
	"sort"

	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/record"
)

// Similarity is the strategy that ships: two memories are related when their
// content vectors are close enough.
//
// # It uses one of the four signals, on purpose
//
// [Candidate] carries vector similarity, text similarity, graph distance and
// age delta, and this reads the first. That is not an oversight to be tidied
// away — it is what makes the seam real. A strategy that weighs graph proximity,
// or that asks a language model whether two memories contradict each other, is a
// new implementation of [Strategy] and touches nothing upstream of it. Spec §25
// lists all five signal families for exactly that reason, and §47 with
// Invariant 9 is why such a strategy has to run here, in a job, rather than on
// a write path.
//
// # The threshold is a cosine
//
// Rust compares 1/(1+d) against 0.7 (connection_manager.rs:139-142), which over
// unit vectors under squared L2 is cosine >= 0.786. Its own source says why that
// quantity should not be thresholded (types.rs:545-548, REM-74). Go reports
// cosine as relevance on every surface and thresholds it here, and the
// consequence — that the two implementations cannot produce the same edge set —
// is recorded in docs/architecture/discovery.md and as plan §II.10 row 17.
//
// # The strength is the similarity
//
// An edge's strength ranks traversal (plan §II.10 row 7), so writing the cosine
// into it is what makes "closest first" survive into `find_related`. A discovered
// edge is otherwise indistinguishable from a user's, deliberately: an edge
// carrying a "discovered" flag would be an edge some code path is entitled to
// delete, and a heuristic that deletes user-visible data is what §II.10 row 8
// forbids.
type Similarity struct {
	// Threshold is the cosine at or above which two memories are linked. The
	// comparison is inclusive, as Rust's is, so a corpus sitting exactly on the
	// number links rather than not.
	Threshold float32
	// TopK bounds how many relationships one subject gains per run.
	TopK int
}

// NewSimilarity builds the strategy, taking [DefaultThreshold] and
// [DefaultTopK] for anything unset.
func NewSimilarity(threshold float32, topK int) *Similarity {
	if threshold <= 0 {
		threshold = DefaultThreshold
	}
	if topK <= 0 {
		topK = DefaultTopK
	}
	return &Similarity{Threshold: threshold, TopK: topK}
}

// Name is the durable strategy name, for logs and metrics.
func (s *Similarity) Name() string { return "similarity" }

// Evaluate returns at most TopK similar_to edges, strongest first.
//
// It sorts rather than trusting the order it was given. The candidate list is a
// union of two sources and the graph-proposed half arrives in traversal order,
// so "the vector index already ranked these" is true of some of them and not of
// the rest — and a strategy that assumed it would silently prefer whichever
// source happened to come first.
func (s *Similarity) Evaluate(_ context.Context, subject *record.Record, cands []Candidate) ([]graph.Edge, error) {
	if subject == nil || len(cands) == 0 {
		return nil, nil
	}

	eligible := make([]Candidate, 0, len(cands))
	for _, c := range cands {
		if c.Record == nil || c.Record.ID == subject.ID {
			continue
		}
		if c.VectorSim < s.threshold() {
			continue
		}
		eligible = append(eligible, c)
	}

	// Ties broken by id, so the same corpus produces the same edges on every
	// run and on every node. Invariant 9 asks that of any path Phase 14
	// replicates, and it is also what makes a rebuild comparable with an
	// incrementally discovered graph.
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].VectorSim != eligible[j].VectorSim {
			return eligible[i].VectorSim > eligible[j].VectorSim
		}
		return eligible[i].Record.ID.String() < eligible[j].Record.ID.String()
	})
	if len(eligible) > s.topK() {
		eligible = eligible[:s.topK()]
	}

	out := make([]graph.Edge, 0, len(eligible))
	for _, c := range eligible {
		out = append(out, graph.Edge{
			From:     subject.ID,
			To:       c.Record.ID,
			Type:     graph.SimilarTo,
			Strength: clamp01(c.VectorSim),
		})
	}
	return out, nil
}

func (s *Similarity) threshold() float32 {
	if s.Threshold <= 0 {
		return DefaultThreshold
	}
	return s.Threshold
}

func (s *Similarity) topK() int {
	if s.TopK <= 0 {
		return DefaultTopK
	}
	return s.TopK
}

func clamp01(v float32) float32 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}
