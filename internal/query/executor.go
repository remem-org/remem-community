package query

import (
	"context"
	"fmt"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// Executor runs a plan against the indexes.
//
// It returns record ids and scores and never a record body. That is the whole
// economy of the attribute row: deciding which ten records out of a hundred
// thousand to return costs row reads, and the caller then reads exactly ten
// payloads. An executor that materialised records would put the cost back.
type Executor struct {
	slots  *schema.Slots
	index  vector.Index
	metric distance.Metric
	texts  *text.Index

	// textRebuilds is where a degraded keyword index reports itself. Nil means
	// the discovery ends as a log line, which is what it was before Phase 9.
	textRebuilds text.Rebuilder
}

// ExecutorOption supplies an index the executor can run without.
type ExecutorOption func(*Executor)

// WithTextIndex turns on the keyword step. Without it a keyword query is
// refused by name rather than answered emptily — an empty page from a corpus
// that is not empty is the failure this layer exists to avoid.
//
// It is an option rather than a fourth parameter because two of the four would
// be nil at most call sites, and a constructor whose arguments are mostly nil
// is one nobody can read.
func WithTextIndex(ix *text.Index) ExecutorOption {
	return func(e *Executor) { e.texts = ix }
}

// WithTextRebuilder is where a degraded keyword index reports itself.
//
// The executor is the only place that asks text.Health, so it is the only place
// that learns a tenant's index is incomplete. Without this the discovery ends
// as a log line somebody has to read.
func WithTextRebuilder(r text.Rebuilder) ExecutorOption {
	return func(e *Executor) { e.textRebuilds = r }
}

// NewExecutor builds an executor. index may be nil in a deployment with no
// vector search; a query that needs one is then refused by name rather than
// answered emptily.
func NewExecutor(slots *schema.Slots, index vector.Index, metric distance.Metric,
	opts ...ExecutorOption) *Executor {
	e := &Executor{slots: slots, index: index, metric: metric}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Run executes a plan and returns one page.
//
// r is the read surface every step shares, and it is a snapshot for a listing:
// one page of results is read from one pinned state, so a record cannot be
// half-seen while it is being written. Across pages the service retains this snapshot —
// see the guarantees stated on [Cursor].
func (e *Executor) Run(ctx context.Context, r attr.Reader, q *Query, plan Plan) (Result, error) {
	const op = "query.Executor.Run"

	if len(plan.Steps) == 0 {
		return Result{}, errs.E(errs.Invalid, op, fmt.Errorf("a plan with no access path answers nothing"))
	}
	if q.Cursor != nil {
		if err := q.Cursor.checkUsable(q); err != nil {
			return Result{}, err
		}
	}
	if len(q.Tags) > 0 && e.texts == nil {
		return Result{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"this build has no text index, so a tag filter cannot be answered; "+
				"tags are ordinary terms in that index rather than a special case"))
	}

	lists := make([]RankedList, 0, len(plan.Steps))
	out := Result{}
	for _, step := range plan.Steps {
		switch step.Kind {
		case StepAttrScan:
			page, err := e.runAttrScan(ctx, r, q, plan, step)
			if err != nil {
				return Result{}, err
			}
			out.Truncated = out.Truncated || page.truncated
			out.HasMore = out.HasMore || page.hasMore
			out.Examined += page.examined
			out.NextCursor = page.next
			lists = append(lists, RankedList{Source: step.Source, Items: page.items})
		case StepVector:
			page, err := e.runVector(ctx, r, q, plan, step)
			if err != nil {
				return Result{}, err
			}
			out.Truncated = out.Truncated || page.truncated
			out.Examined += page.examined
			lists = append(lists, RankedList{Source: step.Source, Items: page.items})
		case StepText:
			page, err := e.runText(ctx, r, q, plan, step)
			if err != nil {
				return Result{}, err
			}
			out.Truncated = out.Truncated || page.truncated
			out.HasMore = out.HasMore || page.hasMore
			out.Examined += page.examined
			lists = append(lists, RankedList{Source: step.Source, Items: page.items})
		case StepGraph:
			page, err := e.runGraph(ctx, r, q, plan, step)
			if err != nil {
				return Result{}, err
			}
			out.Truncated = out.Truncated || page.truncated
			out.HasMore = out.HasMore || page.hasMore
			out.Examined += page.examined
			lists = append(lists, RankedList{Source: step.Source, Items: page.items})
		default:
			return Result{}, errs.E(errs.Invalid, op, fmt.Errorf("access path %d is not one this binary runs", step.Kind))
		}
	}

	if plan.Fused || len(lists) > 1 {
		out.Hits = Fuse(lists, plan.Target)
	} else {
		out.Hits = single(lists[0], plan.Target)
	}
	if !q.Explain {
		for i := range out.Hits {
			out.Hits[i].Sources = nil
		}
	}
	return out, nil
}

// stepPage is what one access path produced.
type stepPage struct {
	items     []RankedItem
	examined  int
	truncated bool
	hasMore   bool
	next      *Cursor
}

// runAttrScan walks an ordered slot index, widening its effort budget until it
// has a full page or the budget runs out.
//
// The widening is why a filtered listing does not return short. A predicate
// matching one row in twenty means a page of ten needs two hundred candidates
// examined, and asking the index for ten would return one or two with no
// indication that more exist. Each round *resumes* the walk rather than
// restarting it, so doubling the budget buys new candidates instead of
// re-examining the ones already rejected — which is the difference between
// widening costing 32× and costing 63×.
func (e *Executor) runAttrScan(ctx context.Context, r attr.Reader, q *Query, plan Plan, step Step) (stepPage, error) {
	sel := attr.Select{
		Tenant:    q.Tenant,
		Namespace: q.namespace(),
		Slots:     e.slots,
		Slot:      step.Slot,
		Desc:      step.Desc,
		Preds:     q.Preds,
		After:     q.Cursor.position(),
		Limit:     plan.Target,
	}

	var out stepPage
	budget := plan.StartBudget
	var last *attr.Position

	for {
		sel.Limit = plan.Target - len(out.items)
		sel.Effort = budget - out.examined
		if sel.Limit <= 0 || sel.Effort <= 0 {
			break
		}
		page, err := attr.Run(ctx, r, sel)
		if err != nil {
			return stepPage{}, err
		}
		out.examined += page.Examined
		for _, rid := range page.IDs {
			// A listing has no relevance to report: the order is the answer,
			// and inventing a score for it would be a number a client could
			// sort by and get a different order than the server intended.
			out.items = append(out.items, RankedItem{ID: rid, Score: 0})
		}
		if page.Next != nil {
			last = page.Next.Clone()
			sel.After = last
		}

		switch {
		case len(out.items) >= plan.Target:
			// A full page. There may or may not be more; the next request finds
			// out, which is cheaper than examining one more candidate now to
			// answer a question the caller may never ask.
			out.hasMore = true
		case page.Exhausted:
			// The range ended. This is the one case where a short page means
			// "that was everything".
		case out.examined >= plan.MaxBudget:
			out.truncated = true
		default:
			if budget < plan.MaxBudget {
				budget *= 2
				if budget > plan.MaxBudget {
					budget = plan.MaxBudget
				}
				continue
			}
			out.truncated = true
		}
		break
	}

	if last != nil && (out.hasMore || out.truncated) {
		out.next = &Cursor{
			Tenant: q.Tenant, Slot: step.Slot, Desc: step.Desc,
			Value: last.Value, ID: last.ID,
		}
	}
	return out, nil
}

// runVector searches canonical vectors, widening k until the predicates it
// cannot push down have left a full page.
//
// Unlike an ordered walk this restarts rather than resumes: a nearest-neighbour
// index answers "the k nearest", and there is no position to continue from. The
// repeated work is the price of the access path, and it is bounded by the same
// 32× factor.
func (e *Executor) runVector(ctx context.Context, r attr.Reader, q *Query, plan Plan, step Step) (stepPage, error) {
	const op = "query.Executor.runVector"

	if e.index == nil {
		return stepPage{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this build has no vector index, so a semantic search cannot be answered"))
	}

	// An index that cannot answer completely for this tenant makes the page
	// incomplete however the widening below goes, so the flag is settled before
	// the search rather than inferred from its results. This is the one caller
	// of vector.Health, and it is the caller the interface's own documentation
	// promises is pinned by a test: an index damaged for a tenant would
	// otherwise return a short page that reads exactly like a small corpus.
	health, err := e.index.Health(ctx, q.Tenant)
	if err != nil {
		return stepPage{}, err
	}

	var out stepPage
	out.truncated = health.Degraded
	budget := plan.StartBudget
	for {
		hits, err := e.index.Search(ctx, q.Tenant, q.Vector, budget, nil)
		if err != nil {
			return stepPage{}, err
		}
		out.examined = len(hits)
		out.items = out.items[:0]

		for _, h := range hits {
			ok, err := e.settle(ctx, r, q, h.ID)
			if err != nil {
				return stepPage{}, err
			}
			if !ok {
				continue
			}
			out.items = append(out.items, RankedItem{
				ID:    h.ID,
				Score: distance.Relevance(e.metric, h.Distance),
				// 1/(1+d), which is the scale Rust's single-source path
				// reports and the one the differential harness compares
				// (behaviour baseline §1.4).
				Ordering: distance.Score(h.Distance),
				Distance: h.Distance,
			})
			if len(out.items) == plan.Target {
				break
			}
		}

		switch {
		case len(out.items) >= plan.Target:
			return out, nil
		case len(hits) < budget:
			// The index returned fewer candidates than asked for, so it has no
			// more to give. The page is short because the corpus is, not
			// because the budget ran out.
			return out, nil
		case budget >= plan.MaxBudget:
			out.truncated = true
			return out, nil
		}
		budget *= 2
		if budget > plan.MaxBudget {
			budget = plan.MaxBudget
		}
	}
}

// runText looks the query's terms up in the inverted index, widening its limit
// until the predicates it cannot push down have left a full page.
//
// It widens by restarting rather than resuming, like the vector step and for
// the same reason: a ranked index answers "the best k", and there is no
// position to continue from. The repeated work is bounded by the same 32×
// factor, and it is cheaper here than there — re-reading a term's postings
// costs the term's matches, not the corpus.
func (e *Executor) runText(ctx context.Context, r attr.Reader, q *Query, plan Plan, step Step) (stepPage, error) {
	const op = "query.Executor.runText"

	if e.texts == nil {
		return stepPage{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this build has no text index, so a keyword search cannot be answered"))
	}

	// An index that cannot answer completely for this tenant makes the page
	// incomplete however the widening goes, so the flag is settled before the
	// search rather than inferred from its results. This is the caller
	// text.Health's documentation promises is pinned by a test: a rebuilding
	// index would otherwise return a short page that reads exactly like a small
	// corpus. It is the lesson Phase 7 paid for, applied to the second index it
	// applies to.
	health, err := e.texts.Health(ctx, r, q.Tenant, q.namespace())
	if err != nil {
		return stepPage{}, err
	}
	if health.Degraded {
		obs.Logger(ctx).Warn("a keyword search was served from an incomplete index",
			"tenant", q.Tenant.String(), "reason", health.Reason)
		if e.textRebuilds != nil {
			e.textRebuilds.ScheduleRebuild(q.Tenant, health.Reason)
		}
	}

	out := stepPage{truncated: health.Degraded}
	budget := plan.StartBudget
	for {
		hits, err := e.texts.Search(ctx, r, q.Tenant, q.namespace(), q.searchReq(budget))
		if err != nil {
			return stepPage{}, err
		}
		out.examined = len(hits)
		out.items = out.items[:0]

		for _, h := range hits {
			ok, err := e.settle(ctx, r, q, h.ID)
			if err != nil {
				return stepPage{}, err
			}
			if !ok {
				continue
			}
			out.items = append(out.items, RankedItem{
				ID: h.ID,
				// Squashed for reporting, never for ordering: the ordering is
				// the raw BM25 sum, and Score is what a caller sees.
				Score:    TextRelevance(h.Score),
				Ordering: h.Score,
			})
			if len(out.items) == plan.Target {
				break
			}
		}

		switch {
		case len(out.items) >= plan.Target:
			return out, nil
		case len(hits) < budget:
			// The index had fewer matches than the budget asked for, so there
			// is nothing more to widen into. The page is short because the
			// matches are few, not because the search stopped looking.
			return out, nil
		case budget >= plan.MaxBudget:
			out.truncated = true
			return out, nil
		}
		budget *= 2
		if budget > plan.MaxBudget {
			budget = plan.MaxBudget
		}
	}
}

// runGraph traverses outward from the query's anchor and returns the memories
// it reached, strongest path first.
//
// It reads through the same r the rest of the query does, so the neighbourhood
// it walked and the attribute rows it filtered that neighbourhood with describe
// one state. That is why graph.Traverse takes a Reader rather than a store.
//
// Unlike the vector step this does not widen. A traversal already spends its
// node budget on the strongest connections available, so asking for more would
// mean reaching *weaker* ones — the opposite of what widening does for a
// nearest-neighbour index, where more candidates are simply more of the same
// quality. The budget is set once, from the plan's maximum, and truncation is
// reported from the walk itself.
func (e *Executor) runGraph(ctx context.Context, r attr.Reader, q *Query, plan Plan, step Step) (stepPage, error) {
	const op = "query.Executor.runGraph"

	if q.RelatedTo == nil {
		return stepPage{}, errs.E(errs.Invalid, op,
			fmt.Errorf("a graph step needs a memory to relate to"))
	}
	sc := graph.Scope{Tenant: q.Tenant, Namespace: q.namespace()}
	tr, err := graph.Traverse(ctx, r, sc, *q.RelatedTo, q.traverseOpts(plan.MaxBudget))
	if err != nil {
		return stepPage{}, err
	}

	out := stepPage{examined: len(tr.Reached), truncated: tr.Truncated}
	out.items = make([]RankedItem, 0, min(len(tr.Reached), plan.Target))
	for i, reached := range tr.Reached {
		ok, err := e.settle(ctx, r, q, reached.ID)
		if err != nil {
			return stepPage{}, err
		}
		if !ok {
			continue
		}
		out.items = append(out.items, RankedItem{
			ID: reached.ID,
			// Path strength is already on the comparable 0..1 scale: it is a
			// product of unit-interval strengths, so it needs no conversion and
			// gets none. Inventing one — a depth decay, say — would be a
			// re-weighting nothing asked for.
			Score:    reached.PathStrength,
			Ordering: reached.PathStrength,
		})
		if len(out.items) == plan.Target {
			// A full page with nodes still unexamined. Whether any of them
			// survive the predicates is unknown, and saying "there may be more"
			// is the honest direction to be wrong in: the alternative is a
			// caller who asked for five of thirty-three related memories
			// reading the five as all of them. It is the same answer the
			// attribute scan gives for the same reason.
			out.hasMore = i+1 < len(tr.Reached)
			break
		}
	}
	return out, nil
}

// settle evaluates the query's predicates against a candidate's attribute row.
//
// This is the pushdown that replaces Phase 3's post-hoc filter: the row is tens
// of bytes and the record body is kilobytes, so a candidate that fails a filter
// costs a small read rather than a protobuf decode. A candidate with no row is
// dropped — it cannot be shown to satisfy the filter, and returning it would
// mean a filtered search returning records that do not match.
func (e *Executor) settle(ctx context.Context, r attr.Reader, q *Query, rid id.ID) (bool, error) {
	ns := q.namespace()

	// The tag filter, settled per candidate for the same reason the predicates
	// are: it removes candidates, and every step produces candidates. See
	// Query.tagTerms for why it is not left to the keyword step alone.
	if terms := q.tagTerms(); len(terms) > 0 {
		ok, err := e.texts.HasTerms(ctx, r, q.Tenant, ns, rid, terms)
		if err != nil || !ok {
			return false, err
		}
	}

	if len(q.Preds) == 0 {
		return true, nil
	}
	value, err := r.Get(ctx, attr.RowKey(q.Tenant, ns, rid))
	if errs.Is(err, errs.NotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	row, err := attr.DecodeRow(value, e.slots)
	if err != nil {
		return false, nil
	}
	return attr.Match(row, q.Preds), nil
}

// single turns one step's ranked list into hits without fusing it.
//
// The step's own order is preserved and FusedScore is left at zero, which says
// what is true: nothing fused, so there is no fused value. Running RRF over one
// list would put a rank-derived number there that looks comparable to a hybrid
// search's and is not.
func single(list RankedList, limit int) []Hit {
	if limit > 0 && len(list.Items) > limit {
		list.Items = list.Items[:limit]
	}
	hits := make([]Hit, 0, len(list.Items))
	for rank, item := range list.Items {
		sources := []Contribution{{
			Source: list.Source, Score: item.Score, Rank: rank, Distance: item.Distance,
		}}
		hits = append(hits, Hit{
			ID:         item.ID,
			Score:      deriveScore(sources),
			FusedScore: item.Ordering,
			Distance:   item.Distance,
			Sources:    sources,
		})
	}
	return hits
}

// Snapshot opens a read view for one page, so that every step of one query
// sees the same state.
//
// It is here rather than at the call site because forgetting it is invisible:
// the query still returns results, and the inconsistency shows up as a record
// that appears in a listing and cannot then be fetched.
func Snapshot(kv storage.KV) storage.Snapshot { return kv.NewSnapshot() }
