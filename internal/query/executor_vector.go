package query

import (
	"context"
	"fmt"
	"math"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// The vector step: the similarity walk, the widening budget a filtered search
// sizes from its observed pass rate, and the bookkeeping that settles each
// candidate against the filters at most once per query.

// runVector searches canonical vectors, widening k until the predicates it
// cannot push down have left a full page.
//
// Unlike an ordered walk this restarts rather than resumes: a nearest-neighbour
// index answers "the k nearest", and there is no position to continue from. The
// repeated work is the price of the access path, and it is bounded by the same
// 32× factor.
//
// # Two things keep that price down (plan item 16)
//
// A candidate's settle decision is remembered for the life of the request, so a
// round that restarts the walk does not read and decode an attribute row the
// previous round already read. That was finding 2's "pays twice": with one
// record in five passing, a page of ten cost about ninety row reads where seventy
// distinct candidates were ever examined.
//
// And the next budget is informed by the pass rate this round measured rather
// than being a blind doubling, so a selective filter reaches its target in one
// or two rounds instead of log2 of them. Both the first budget and every later
// one are rounded up to the index's [vector.Granular] step, because a budget
// below that step buys fewer candidates for exactly the same traversal.
//
// Neither changes the answer. The page is the first Target candidates that
// survive the filter, taken in the walk's own order, and widening only ever
// extends that order — so reaching the fill point in one round rather than three
// produces the same page from the same candidates. What does change is
// stepPage.examined, which counts the candidates the final round was offered:
// a round that asked for the whole granularity was offered more of them.
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

	// Only a query that removes candidates can see one twice: one with nothing to
	// settle fills its page from the first round or has run out of corpus, so it
	// would pay for the record and never read it.
	var decided settled
	if q.filters() {
		decided.at = make([]settledAt, 0, plan.Target)
	}

	budget := e.firstBudget(plan)
	for {
		hits, err := e.index.Search(ctx, q.Tenant, q.Vector, budget, nil)
		if err != nil {
			return stepPage{}, err
		}
		out.examined = len(hits)
		out.items = out.items[:0]

		for at, h := range hits {
			ok, err := e.settleOnce(ctx, r, q, at, h.ID, &decided)
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
		budget = e.nextBudget(budget, len(hits), len(out.items), plan)
	}
}

// Widening a vector search (plan item 16).
const (
	// widenSafety is the margin on a pass-rate estimate. The rate is measured on
	// the candidates nearest the query, and the ones further out are a different
	// sample; a round that asks for exactly the estimate therefore comes up
	// short about as often as not, and coming up short costs a whole extra walk.
	widenSafety = 1.25
	// minPassRate floors the measured rate. A round in which nothing passed
	// would otherwise divide by zero, and a round in which one candidate in a
	// hundred passed would ask for more of the corpus than MaxBudget allows
	// anyway. One in sixty-four reaches MaxBudget in a single step for every
	// limit this server serves, which is the behaviour wanted: a filter that
	// selective is going to be truncated, and finding that out in one round is
	// cheaper than in five.
	minPassRate = 1.0 / 64.0
)

// firstBudget is how many candidates the first round asks for.
//
// It is the plan's start budget raised to the index's granularity, because below
// that the index does the same work and returns less, and lowered to MaxBudget,
// because MaxBudget is a promise about how many candidates a page may be built
// from and the truncated flag means that promise was spent.
//
// A plan that states no maximum is not clamped: the loop's own
// budget >= MaxBudget test already stops such a plan after one round, and
// clamping would turn its first round into a search for nothing.
func (e *Executor) firstBudget(plan Plan) int {
	budget := max(plan.StartBudget, e.efStep)
	if plan.MaxBudget > 0 && budget > plan.MaxBudget {
		budget = plan.MaxBudget
	}
	return budget
}

// nextBudget is how wide the next round asks, from the pass rate this one saw.
//
// examined candidates left passed of them standing, so the filter keeps about
// p = passed/examined, and filling the rest of the page needs about
// (Target-passed)/p more candidates. Asking for that many plus a margin reaches
// the target in one more round where doubling needs log2 of them. It never asks
// for less than double, so the guarantee the old loop gave — the budget grows
// geometrically and stops at MaxBudget — is unchanged.
func (e *Executor) nextBudget(budget, examined, passed int, plan Plan) int {
	p := minPassRate
	if examined > 0 {
		if measured := float64(passed) / float64(examined); measured > p {
			p = measured
		}
	}
	estimate := math.Ceil(float64(plan.Target-passed) / p)
	next := examined + int(math.Ceil(estimate*widenSafety))
	return min(max(next, e.efStep, 2*budget), plan.MaxBudget)
}

// filters reports whether this query removes candidates after an index has
// produced them.
//
// It is the question "can one candidate be examined twice", asked by the vector
// step before it decides to remember its settle decisions: a query with nothing
// to settle fills its page from the first round or has run out of corpus, so
// there is nothing for a second round to repeat.
//
// It lives here rather than beside Query's other derivations because it is a
// question only the widening asks.
func (q *Query) filters() bool { return len(q.Preds) > 0 || len(q.Tags) > 0 }

// settled is one request's settle decisions, indexed by the candidate's position
// in the index's answer.
//
// Position rather than identity, because a widening round asks the same index the
// same question with a larger budget and gets the previous answer extended: the
// candidate at position i is the one that was at position i before. That is
// checked rather than assumed — the id is compared, and a candidate that has
// moved is simply settled again — which buys a slice where a map keyed by record
// id would cost three allocations and a hash of sixteen bytes per lookup, on a
// path that runs once per candidate.
//
// A zero settled remembers nothing, which is what a query with no filter wants.
type settled struct {
	at []settledAt
}

type settledAt struct {
	id   id.ID
	pass bool
}

// decision is the remembered decision for position i, if what is remembered
// there is about rid.
func (s *settled) decision(i int, rid id.ID) (pass, known bool) {
	if i < len(s.at) && s.at[i].id == rid {
		return s.at[i].pass, true
	}
	return false, false
}

// record remembers that rid, at position i, settled to pass.
//
// Everything remembered past i is dropped: a candidate that moved has moved
// every candidate behind it, so those positions no longer name what they did.
// Positions are always recorded in increasing order from zero, so i is never
// past the end; a record that would be is dropped rather than misplaced.
func (s *settled) record(i int, rid id.ID, pass bool) {
	if s.at == nil || i > len(s.at) {
		return
	}
	s.at = append(s.at[:i], settledAt{id: rid, pass: pass})
}

// settleOnce is [Executor.settle] with a memory of its own answers.
//
// The decision is a function of the candidate and the request's pinned read view,
// so it cannot change between the rounds of one request — which is what makes
// remembering it sound rather than merely cheap.
func (e *Executor) settleOnce(ctx context.Context, r attr.Reader, q *Query, at int, rid id.ID,
	decided *settled) (bool, error) {
	if pass, known := decided.decision(at, rid); known {
		return pass, nil
	}
	ok, err := e.settle(ctx, r, q, rid)
	if err != nil {
		return false, err
	}
	decided.record(at, rid, ok)
	return ok, nil
}
