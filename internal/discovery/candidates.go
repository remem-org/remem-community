package discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// gather returns the memories worth considering as relatives of subject, each
// described by every signal spec §25 lists.
//
// # Two sources propose; four signals describe
//
// The vector index proposes, because discovery *is* nearest-neighbour search.
// One traversal out of the subject proposes as well, and it is free: the same
// walk is what fills GraphDist, so the candidates it contributes cost nothing
// beyond the record reads every candidate needs. What it buys is the case the
// approximate index misses — a memory the graph already knows about that HNSW's
// entry point did not lead to.
//
// Text does not propose. A keyword proposal means running a whole memory's
// content as a BM25 query, once per memory written, and content is bounded at a
// megabyte. TextSim is a property of a *pair* and needs no index at all. Time
// does not propose either: "written near this one" is adjacency, and spending
// the candidate budget on a window whose contents are unrelated by construction
// is the mistake internal/text recorded in Phase 8 — a step is a source.
//
// # One snapshot
//
// The traversal and every record read go through one pinned view, so a memory
// written while this runs is either wholly considered or wholly absent. That is
// the correction Phase 6 made for graph.Traverse and Phase 8 for the keyword
// step, applied here for the same reason.
func (d *Discoverer) gather(ctx context.Context, sc Scope, subject *record.Record) ([]Candidate, error) {
	const op = "discovery.gather"

	vec := subject.Vectors[record.VectorContent]
	if vec == nil || len(vec.Values) == 0 {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"memory %s has no canonical content vector, so there is nothing to compare it against", subject.ID))
	}

	snap := d.deps.KV.NewSnapshot()
	defer func() { _ = snap.Close() }()

	// Order matters: the vector hits lead, so if the bound bites it bites the
	// candidates the ranking was least likely to choose.
	proposed, sims, err := d.propose(ctx, sc, subject, vec.Values, snap)
	if err != nil {
		return nil, err
	}
	hops, err := d.neighbourhood(ctx, sc, subject.ID, snap)
	if err != nil {
		return nil, err
	}
	for _, rid := range hops.order {
		if len(proposed) >= d.maxCandidates() {
			break
		}
		if _, seen := sims[rid]; seen {
			continue
		}
		sims[rid] = notSearched
		proposed = append(proposed, rid)
	}

	subjectTerms := termSet(subject.Content)
	out := make([]Candidate, 0, len(proposed))
	for _, rid := range proposed {
		rec, err := d.deps.Repo.GetFrom(ctx, snap, rid)
		if err != nil {
			// A candidate that has been hard-deleted since the index last saw
			// it is an ordinary race, not damage: the index is derived and the
			// record is canonical, so the record wins.
			if errs.Is(err, errs.NotFound) {
				continue
			}
			return nil, err
		}
		// Archived memories are retired from retrieval. An edge into one
		// manufactures a relationship to something no ordinary read reaches,
		// and traversal would then spend its node budget on it.
		if rec.Fields.Archived {
			continue
		}

		sim, ok := sims[rid]
		if !ok || sim == notSearched {
			if sim, ok = cosineBetween(vec.Values, rec.Vectors[record.VectorContent]); !ok {
				// No comparable vector — a record stored before embedding, or
				// one from another model. It cannot be a similarity candidate,
				// and reporting a fabricated zero would let a future strategy
				// read "unrelated" where the truth is "unknown".
				continue
			}
		}

		out = append(out, Candidate{
			Record:    rec,
			VectorSim: sim,
			TextSim:   jaccard(subjectTerms, termSet(rec.Content)),
			GraphDist: hops.distance(rid),
			AgeDelta:  absDuration(rec.CreatedAt.Sub(subject.CreatedAt)),
		})
	}
	return out, nil
}

// notSearched marks a candidate the vector index did not return, so its cosine
// is computed from the stored vectors instead. It is outside [-1, 1], so it
// cannot be mistaken for a similarity.
const notSearched float32 = -2

// propose runs the vector search, excluding the subject by filter rather than
// by asking for one extra result and dropping it.
//
// Rust asks for top_k+1 and skips its own key (connection_manager.rs:130-136).
// A filter says the same thing without depending on the subject actually being
// in its own top k+1 — which it is not, for a tenant whose index is mid-repair.
func (d *Discoverer) propose(ctx context.Context, sc Scope, subject *record.Record,
	q []float32, snap storage.Snapshot,
) ([]id.ID, map[id.ID]float32, error) {
	_ = snap // the index reads its own state; the snapshot pins the records.

	notSelf := vector.Filter(func(rid id.ID) bool { return rid != subject.ID })
	hits, err := d.deps.Index.Search(ctx, sc.Tenant, q, d.topK(), notSelf)
	if err != nil {
		return nil, nil, err
	}

	order := make([]id.ID, 0, len(hits))
	sims := make(map[id.ID]float32, len(hits))
	for _, h := range hits {
		if h.ID == subject.ID {
			// Belt and braces. A self-link is the one edge that is never
			// meaningful, and an index that ignored the filter must not be able
			// to produce one.
			continue
		}
		if _, seen := sims[h.ID]; seen {
			continue
		}
		sim, ok := distance.CosineFromOK(d.metric(), h.Distance)
		if !ok {
			sims[h.ID] = notSearched
		} else {
			sims[h.ID] = clamp01(sim)
		}
		order = append(order, h.ID)
	}
	return order, sims, nil
}

// hopMap is what one traversal found, by node.
type hopMap struct {
	depth map[id.ID]int
	order []id.ID
}

func (h hopMap) distance(rid id.ID) int {
	if d, ok := h.depth[rid]; ok {
		return d
	}
	return Unreachable
}

// neighbourhood walks out from the subject once, in both directions.
//
// Both directions because a relationship is a fact about a pair: a memory
// something else points at is as much a neighbour as one it points at, and
// following out-edges alone would make GraphDist depend on which of the two was
// written first.
func (d *Discoverer) neighbourhood(ctx context.Context, sc Scope, start id.ID,
	snap storage.Snapshot,
) (hopMap, error) {
	out := hopMap{depth: map[id.ID]int{}}
	if d.deps.KV == nil {
		return out, nil
	}
	walk, err := graph.Traverse(ctx, snap, graph.Scope{Tenant: sc.Tenant, Namespace: sc.Namespace},
		start, graph.TraverseOpts{
			MaxDepth:  neighbourDepth,
			MaxNodes:  d.maxCandidates(),
			Direction: graph.Both,
		})
	if err != nil {
		return hopMap{}, err
	}
	for _, r := range walk.Reached {
		if r.ID == start {
			continue
		}
		if _, seen := out.depth[r.ID]; seen {
			continue
		}
		out.depth[r.ID] = r.Depth
		out.order = append(out.order, r.ID)
	}
	return out, nil
}

// cosineBetween is the similarity of two canonical vectors, or false when there
// is no honest answer.
//
// Two vectors from different models describe different spaces, and comparing
// the first 384 components of one with another's is a number nobody can
// explain. That is why every vector carries its model id (plan §II.10 row 10),
// and it is why this refuses rather than truncating.
func cosineBetween(subject []float32, other *vector.Vector) (float32, bool) {
	if other == nil || len(other.Values) != len(subject) {
		return 0, false
	}
	d := distance.CosineDistance(subject, other.Values)
	return clamp01(1 - d), true
}

// termSet is the content's distinct terms, under the same tokeniser the
// inverted index uses.
//
// The same tokeniser matters: a discovery signal that split text differently
// from the index would report a similarity nothing else in the system could
// reproduce, and the tokeniser is fixed in code for exactly that class of
// reason (plan §Phase 8).
func termSet(content string) map[string]struct{} {
	terms := text.Tokenize(content)
	out := make(map[string]struct{}, len(terms))
	for _, t := range terms {
		out[t] = struct{}{}
	}
	return out
}

// absDuration is how far apart two instants are, whichever came first.
//
// Order carries nothing here: "written before" and "written after" are the same
// adjacency, and a signed delta would be one more way to read the signal
// wrongly for no information gained.
func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// jaccard is the overlap of two term sets over their union.
//
// It is symmetric, bounded in [0, 1] and computable from the two contents
// alone — the three properties a BM25 score does not have. A BM25 sum is
// unbounded, it rises with how rare the words happen to be in this tenant's
// corpus, and it is a query-to-document quantity rather than a document-to-
// document one. internal/query says the same thing about mixing the two in
// deriveScore.
func jaccard(a, b map[string]struct{}) float32 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	// Intersect over the smaller set, so the cost is min(|a|, |b|).
	small, large := a, b
	if len(large) < len(small) {
		small, large = large, small
	}
	shared := 0
	for t := range small {
		if _, ok := large[t]; ok {
			shared++
		}
	}
	if shared == 0 {
		return 0
	}
	union := len(a) + len(b) - shared
	return float32(shared) / float32(union)
}
