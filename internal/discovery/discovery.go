// Package discovery finds relationships between memories, durably.
//
// # What it replaces
//
// Rust Remem discovers relationships too. A memory write does a `try_send` into
// a bounded channel, and when that channel is full the task is discarded, a
// counter is incremented, one line is logged, and the memory is stored with no
// relationships and no record that it was owed any
// (api/routes/memories.rs:324-350). That is plan §II.10 row 5, and it is the
// specific defect the job framework was built for.
//
// Here the enqueue is staged into the memory's own transaction. A failed write
// leaves no orphan job; a successful one cannot lose its follow-up work. There
// is no in-memory queue anywhere on this path and no capacity to overflow: the
// queue is the disk.
//
// # A source proposes, a signal describes
//
// [Candidate] carries every signal spec §25 lists — vector similarity, text
// similarity, graph neighbourhood and temporal context — and the strategy that
// ships uses the first. That is what keeps a future LLM-evaluating strategy a
// new [Strategy] rather than a rewrite of the pipeline.
//
// Carrying a signal is not the same as spending budget to find candidates, and
// conflating the two is the mistake internal/text recorded in Phase 8 when a
// tag filter that was meant to narrow instead contributed its own ranked list
// into the fusion. Two things propose candidates here: the vector index, which
// is what discovery *is*, and one bounded traversal out of the subject, which
// is free because it is also what fills GraphDist. Text does not propose,
// because a keyword proposal means running a whole memory's content as a BM25
// query once per memory written; TextSim is a property of a *pair* and is
// computed directly from the two contents. Time does not propose, because
// "written near this one" is adjacency rather than evidence.
//
// # No LLM, no network, no clock in a decision
//
// Spec §47 and Invariant 9 forbid such calls inside deterministic apply, and
// running discovery as a job places it outside apply by construction. Nothing
// in this package makes a network call, and the only clock read is the edge
// timestamp, which comes from the injected [clock.Clock] through graph.Service.
//
// # Everything is scoped to one tenant
//
// The handler is given one tenant on the job. This package is not on the
// tenant.ForEach allowlist and must not be: the fan-out belongs to the
// scheduler and to the backfill command.
package discovery

import (
	"context"
	"time"

	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/record"
)

// DefaultThreshold is the cosine at or above which two memories are linked.
//
// The number is Rust's `auto_discovery_threshold` (config.rs:274). The quantity
// is not. Rust compares 1/(1+d) against it (connection_manager.rs:139-142),
// which over unit vectors under squared L2 is cosine >= 0.786 — and Rust's own
// source says why that is the wrong thing to threshold on (types.rs:545-548,
// REM-74): 1/(1+d) floors at 1/3 for orthogonal vectors and never approaches
// zero. Go reports cosine as relevance everywhere and thresholds it here.
//
// The consequence is that the two implementations cannot produce the same edge
// set, and it would not be fixed by choosing a different number: plan §II.10
// row 16 records that absolute similarity is not comparable across the two
// pooling schemes at all — unrelated pairs sit at 0.626 median cosine in Rust's
// CLS space and 0.074 in Go's mean-pooled one. See docs/architecture/discovery.md.
const DefaultThreshold = 0.7

// DefaultTopK is how many relationships one memory may gain per run.
// config.rs:275.
const DefaultTopK = 5

// MaxCandidates bounds the union of everything the sources propose.
//
// Without it, a memory in a densely connected neighbourhood costs a traversal's
// worth of record reads while a memory in a sparse one costs six. A bound makes
// one subject's work predictable regardless of the shape of the corpus around
// it.
const MaxCandidates = 64

// neighbourDepth is how far the one traversal reaches.
//
// Two hops, so GraphDist can tell a neighbour from a neighbour's neighbour —
// which is the least a signal named "graph distance" has to distinguish for a
// future strategy to be able to use it at all.
const neighbourDepth = 2

// Unreachable is the GraphDist of a candidate the traversal did not reach.
//
// Negative rather than zero, because zero is a real distance: it is the subject
// itself. A future strategy weighing graph proximity must be able to tell "not
// connected" from "the same memory", and a sentinel of 0 makes those one value.
const Unreachable = -1

// Candidate is one memory discovery is considering linking the subject to,
// described by every signal spec §25 lists.
//
// All four are populated for every candidate, including the three the shipped
// strategy ignores. That is the point: a strategy that weighs graph proximity
// or recency is then a new [Strategy] implementation, not a change to candidate
// retrieval — which is what spec §25 asks for when it says discovery must not
// be hard-coded into storage-layer operations.
type Candidate struct {
	// Record is the candidate memory. It is a record and not a memory.Memory
	// deliberately: internal/memory is a service over records, and a domain
	// package depending on it is the cycle internal/lifecycle predicted in
	// writing when it named this phase.
	Record *record.Record

	// VectorSim is the cosine between the subject's and the candidate's
	// canonical content vectors, in [0, 1] after clamping. It is recovered from
	// the index's distance where the search produced one and computed from the
	// stored vectors otherwise, and the two agree exactly: for unit vectors
	// squared L2 is 2 - 2·cos.
	VectorSim float32

	// TextSim is the Jaccard similarity of the two contents' term sets, under
	// the same tokeniser the inverted index uses.
	//
	// It is a pair quantity, not a query result, so it needs no index and is
	// correct with text.enabled off. A BM25 score would not do here even if it
	// were free: it is unbounded, it rises with how rare the words happen to be
	// in this tenant's corpus, and it is not symmetric between two memories.
	TextSim float32

	// GraphDist is the number of hops from the subject, or [Unreachable].
	GraphDist int

	// AgeDelta is how far apart the two memories were created, always
	// non-negative. Order is not a signal — "written before" and "written
	// after" are the same adjacency — so the sign carries nothing and its
	// absence removes a way to read it wrongly.
	AgeDelta time.Duration
}

// Strategy decides which relationships a subject's candidates justify.
//
// It is the seam spec §25 asks for. Everything upstream — retrieving
// candidates, filling their signals, bounding the work — is shared; everything
// about *what makes two memories related* is here. A strategy that consults a
// language model is a new implementation of this interface and touches nothing
// else, and because discovery runs as a job it is outside deterministic apply
// by construction (spec §47, Invariant 9).
//
// The edges it returns are proposals. The handler is what refuses a pair that
// is already linked, and a strategy must not assume its output is written
// verbatim.
type Strategy interface {
	// Name is the durable name of this strategy, for logs and metrics.
	Name() string

	// Evaluate returns the edges cands justify for subject. Returning none is
	// an ordinary answer, not an error.
	Evaluate(ctx context.Context, subject *record.Record, cands []Candidate) ([]graph.Edge, error)
}
