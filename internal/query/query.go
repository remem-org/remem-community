// Package query is Remem's query model, planner, executor and rank fusion.
//
// # A query is a value
//
// [Query] holds no closure, no index handle and no open iterator, and it
// round-trips through JSON without loss. That is the property
// docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md §3 identifies as missing from the
// Rust implementation and as a precondition for sharding: a query that cannot
// be serialised cannot be planned on one node and executed on another, so the
// shape has to be right before any consensus code exists rather than after.
// TestQueryRoundTripsThroughJSON is what keeps it right.
//
// # Three things a caller must be able to tell apart
//
// "There are no more results", "there are more results, here is where to
// resume", and "I stopped looking before I ran out". [Result] reports them as
// three separate facts — a short page with HasMore false, a NextCursor, and
// Truncated — because a caller that cannot distinguish them eventually renders
// the third as the first, and a user reads that as their data having gone.
//
// Truncated means one thing only: the widening budget was spent before the
// requested number of matches was found. It is never a stand-in for an empty
// result, and never for an index that is still being built — an index that is
// incomplete does not serve at all (see internal/schema's StrategyRebuild).
package query

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
)

// Query is one request for records, stated declaratively.
//
// The fields describe *what* is wanted; the planner decides how to get it. That
// separation is what lets the same query be answered by an ordered index walk
// here and by a fan-out across shards later, without the caller changing.
type Query struct {
	Tenant    tenant.ID        `json:"tenant"`
	Namespace tenant.Namespace `json:"namespace,omitempty"`

	// Text is the query string. It is embedded by the caller into Vector for
	// the semantic step, and is analysed into terms here for the keyword step.
	Text string `json:"text,omitempty"`

	// Keyword asks for a keyword step over Text and Tags. Its presence is what
	// adds an inverted-index step, exactly as Vector's presence adds a semantic
	// one and RelatedTo's adds a graph one — the three combine into one fused
	// ranking, which is what `hybrid` is.
	//
	// A tag filter implies it: a tag is an ordinary term in that index (plan
	// §II.10 row 2), so there is nowhere else for one to be answered.
	Keyword bool `json:"keyword,omitempty"`

	// Vector is the embedded query. Its presence is what makes this a search
	// rather than a listing.
	Vector []float32 `json:"vector,omitempty"`

	// Tags every returned record must carry, all of them.
	//
	// They narrow rather than merely score, which is the difference between a
	// filter and a term, and they are answered by the same inverted index as
	// the content words — one key space, one scan per term. Rust needed a
	// second structure with its own length limits, and an unanswerable filter
	// there silently fell back to scanning the payload (plan §II.10 row 2).
	Tags []string `json:"tags,omitempty"`

	// RelatedTo anchors a graph traversal: the memories connected to this one.
	// Its presence is what adds a graph step, exactly as Vector's presence adds
	// a semantic one, and the two combine into a fused ranking.
	RelatedTo *id.ID `json:"related_to,omitempty"`
	// RelatedDepth is how many hops the traversal follows. Zero means
	// graph.DefaultMaxDepth; above graph.MaxTraversalDepth is refused rather
	// than clamped, because a caller who asked for twenty hops and silently got
	// five has a different picture of their graph than the one they were given.
	RelatedDepth int `json:"related_depth,omitempty"`
	// RelatedTypes restricts the traversal to these relationship types. Empty
	// means every type.
	RelatedTypes []graph.RelationshipType `json:"related_types,omitempty"`
	// RelatedDirection is which way edges are followed. The zero value is
	// graph.Out.
	RelatedDirection graph.Direction `json:"related_direction,omitempty"`
	// RelatedMinStrength prunes edges weaker than this before they are
	// followed, so a weak edge does not cost a node out of the traversal's
	// budget.
	RelatedMinStrength float32 `json:"related_min_strength,omitempty"`

	// Preds are the conditions every returned record satisfies, settled from
	// the attribute row. The slice is a conjunction; use attr.Not and attr.And
	// for anything else.
	Preds attr.Preds `json:"preds,omitempty"`

	// OrderBy is the slot supplying the order of a listing. It is ignored by a
	// search, whose order is the fused rank.
	OrderBy uint16 `json:"order_by"`
	Desc    bool   `json:"desc,omitempty"`

	Limit int `json:"limit"`

	// Cursor resumes a previous page. It is opaque to a caller and carries no
	// physical key — see cursor.go.
	Cursor *Cursor `json:"cursor,omitempty"`

	// Explain asks for the per-source contributions behind each hit.
	Explain bool `json:"explain,omitempty"`
}

// Validate refuses a query that cannot be answered, before any work is done.
func (q *Query) Validate() error {
	const op = "query.Validate"

	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
	}
	switch {
	case q.Tenant == "":
		return bad("a query requires a tenant: there is no unscoped read path (Invariant 1)")
	case q.Limit <= 0:
		return bad("a query limit must be greater than zero")
	case q.Keyword && q.Text == "" && len(q.Tags) == 0:
		return bad("a keyword search needs a query or a tag; an empty one that returned the corpus " +
			"would turn a client bug into a full scan")
	}

	for _, tag := range q.Tags {
		if strings.TrimSpace(tag) == "" {
			return bad("a tag filter must name a tag")
		}
	}

	if q.RelatedTo != nil {
		if q.RelatedTo.IsZero() {
			return bad("a related-memory query needs a memory to relate to")
		}
		if q.RelatedDepth < 0 || q.RelatedDepth > graph.MaxTraversalDepth {
			return bad("a traversal goes at most %d hops, not %d", graph.MaxTraversalDepth, q.RelatedDepth)
		}
		if !q.RelatedDirection.Valid() {
			return bad("%q is not a direction", q.RelatedDirection)
		}
		for _, typ := range q.RelatedTypes {
			if !typ.Valid() {
				return bad("%q is not a relationship type; the types are %v",
					typ, graph.RelationshipNames())
			}
		}
	}
	return nil
}

// keyword reports whether this query has an inverted-index step.
//
// # A tag filter does not add one, and that is the whole point
//
// It used to. A tag can only be answered from the inverted index, so adding the
// step when tags were present looked like the obvious reading of "a tag filter
// narrows exactly like any other term". It is the wrong one, and the end-to-end
// run of Phase 8 is what showed it: a semantic search for "distributed consensus
// and raft leader election" with a tag filter returned "marzipan recipes from a
// Bavarian bakery" *first* — a memory the unfiltered search ranked nowhere,
// promoted above one it had actually found.
//
// The reason is that a step is a *source*. Adding one makes the tag contribute
// its own ranked list into the fusion, so every tagged memory becomes a
// candidate whether or not anything else in the query reached it. That is the
// opposite of filtering.
//
// So the step belongs to a keyword search alone. Tags narrow every step through
// [Query.tagTerms], settled per candidate, and a keyword search additionally
// pushes them down as required terms so its own budget is not spent on
// candidates the filter will drop.
func (q *Query) keyword() bool { return q.Keyword }

// searchReq is the keyword step's request.
//
// Analysing here rather than at the caller keeps a Query the plain, portable
// value it is meant to be: it carries the words a user typed, and the binary
// that executes it applies the same tokeniser that wrote the postings it is
// about to read. A Query carrying pre-folded terms would be a Query whose
// meaning depended on which binary produced it.
func (q *Query) searchReq(limit int) text.SearchReq {
	req := text.SearchReq{Limit: limit, Required: q.tagTerms()}
	if q.Keyword {
		// Only a keyword search scores by content. A semantic search that
		// happens to carry a tag filter gets a step that narrows and nothing
		// else — see [Query.tagTerms].
		req.Optional = text.QueryTerms(q.Text)
	}
	return req
}

// tagTerms is the query's tags as index terms.
//
// # Tags filter every step, and add none
//
// A tag is a filter, and a filter removes candidates. It is settled per
// candidate, against a key that holds no value at all, and that cost is paid
// only when a filter is present and only for candidates that have already
// survived everything else — which is why it does not undo what Phase 5 removed
// from search.
//
// A keyword search pushes the same terms down into its own step as required
// terms, so that step does not spend its budget on candidates the filter will
// drop. That is an optimisation of one step, not the mechanism: see
// [Query.keyword] for what happened when it was treated as the mechanism.
func (q *Query) tagTerms() []string {
	if len(q.Tags) == 0 {
		return nil
	}
	out := make([]string, 0, len(q.Tags))
	for _, tag := range q.Tags {
		out = append(out, text.TagTerm(tag))
	}
	return out
}

// traverseOpts is the traversal the query asks for.
//
// The node budget is the widening budget rather than the plain limit: a
// traversal whose neighbours are then filtered by predicate has to reach more
// than it returns, for the same reason a filtered listing widens (see
// planner.go). It is capped at graph.MaxTraversalNodes so a large limit cannot
// turn one request into an unbounded walk.
func (q *Query) traverseOpts(budget int) graph.TraverseOpts {
	if budget > graph.MaxTraversalNodes {
		budget = graph.MaxTraversalNodes
	}
	return graph.TraverseOpts{
		MaxDepth:    q.RelatedDepth,
		MaxNodes:    budget,
		Types:       q.RelatedTypes,
		Direction:   q.RelatedDirection,
		MinStrength: q.RelatedMinStrength,
	}
}

// MarshalJSON writes the query. The vector is written as-is: it is numbers,
// and a base64 encoding of the same floats would be shorter and unreadable.
func (q Query) MarshalJSON() ([]byte, error) {
	type alias Query
	return json.Marshal(alias(q))
}

// UnmarshalJSON reads a query written by [Query.MarshalJSON].
func (q *Query) UnmarshalJSON(b []byte) error {
	type alias Query
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return errs.E(errs.Invalid, "query.Query.UnmarshalJSON", err)
	}
	*q = Query(a)
	return nil
}

// namespace defaults a query that named none.
func (q *Query) namespace() tenant.Namespace {
	if q.Namespace == "" {
		return tenant.DefaultNamespace
	}
	return q.Namespace
}
