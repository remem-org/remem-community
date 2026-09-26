package query

import (
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/schema"
)

// Widening bounds (plan §Global Constraints).
//
// A filtered search asks its access path for more candidates than it needs,
// because some will be discarded by predicates the access path cannot answer.
// How many more is the widening factor. It starts at 3 and doubles to 32, and
// both numbers are here rather than in configuration for the same reason RRF's
// k is: they change what a query returns, so an operator turning a dial would
// be changing results rather than tuning performance.
const (
	// WidenStartFactor is the first budget: three candidates examined per
	// result asked for.
	WidenStartFactor = 3
	// WidenMaxFactor is where widening stops and Truncated is reported.
	WidenMaxFactor = 32
	// ListMaxFactor is WidenMaxFactor for a listing, and deliberately larger
	// (Rust's config.rs:290-300). A listing candidate costs one attribute-row
	// read on an index already being walked, where widening a search re-runs a
	// similarity traversal, so sharing the search's number truncates ordinary
	// filtered listings: at 32 a page of five under a filter keeping one
	// candidate in thirty cannot be filled from several hundred records.
	ListMaxFactor = 128
)

// StepKind is an access path.
type StepKind uint8

const (
	// StepAttrScan walks one attribute slot's ordered index. It is the
	// listing path, and the only path whose output order is the final order.
	StepAttrScan StepKind = iota + 1
	// StepVector searches canonical vectors for nearest neighbours.
	StepVector
	// StepGraph traverses outward from an anchor record.
	StepGraph
	// StepText looks the query's terms up in the inverted index.
	StepText
)

// Step is one access path in a plan, with the predicates it can settle itself.
type Step struct {
	Kind StepKind
	// Slot is the ordering slot, for an attribute scan.
	Slot uint16
	Desc bool
	// Source is the label contributions from this step carry.
	Source Source
}

// Plan is what the executor runs.
//
// It is a value with no handles in it, for the same reason [Query] is: a plan
// produced on one node and executed on another is what sharding needs, and
// building that property in later means rebuilding the planner.
type Plan struct {
	Steps []Step

	// Target is how many results the query asked for.
	Target int
	// StartBudget and MaxBudget are how many candidates the execution may
	// examine, first and at most.
	StartBudget int
	MaxBudget   int

	// Fused reports whether the result order comes from rank fusion rather
	// than from a single step's own order. A listing is not fused; a search
	// over more than one index is.
	Fused bool
}

// Planner chooses access paths.
//
// It holds the slot table because "is this slot indexed" is the question that
// decides whether an ordering is an access path or a sort over the corpus, and
// the answer is the registered table's rather than a constant.
type Planner struct {
	slots    *schema.Slots
	widenMax int
	listMax  int
}

// NewPlanner returns a planner over a registered slot table.
//
// widenMax is search.widen_max_factor and listMax is search.list_max_factor;
// zero means [WidenMaxFactor] and [ListMaxFactor]. They are parameters rather
// than constants read here because the settings exist and a setting that
// silently does nothing is worse than no setting: an operator who changes it
// and sees no effect concludes the number is not the one that matters. Raising
// either costs candidates examined per result; lowering it makes a selective
// filter report Truncated sooner.
func NewPlanner(slots *schema.Slots, widenMax, listMax int) *Planner {
	if widenMax < WidenStartFactor {
		widenMax = WidenMaxFactor
	}
	if listMax < WidenStartFactor {
		listMax = ListMaxFactor
	}
	return &Planner{slots: slots, widenMax: widenMax, listMax: listMax}
}

// Slots is the table the planner resolves orderings against.
func (p *Planner) Slots() *schema.Slots { return p.slots }

// Plan chooses how to answer q.
//
// The choice is deliberately small and deliberately explicit. There is no cost
// model: a vector present means a semantic step, its absence means an ordered
// walk, and an ordering the caller named is honoured rather than optimised
// away. A cost model that silently changed the order of a listing would change
// a contract, and the place to add one is when there is more than one path to
// choose between for the same question.
func (p *Planner) Plan(q *Query) (Plan, error) {
	const op = "query.Planner.Plan"

	if err := q.Validate(); err != nil {
		return Plan{}, err
	}

	plan := Plan{
		Target:      q.Limit,
		StartBudget: q.Limit * WidenStartFactor,
		MaxBudget:   q.Limit * p.widenMax,
	}

	if len(q.Vector) > 0 {
		plan.Steps = append(plan.Steps, Step{Kind: StepVector, Source: SourceVector})
	}
	if q.keyword() {
		plan.Steps = append(plan.Steps, Step{Kind: StepText, Source: SourceText})
	}
	if q.RelatedTo != nil {
		plan.Steps = append(plan.Steps, Step{Kind: StepGraph, Source: SourceGraph})
	}
	if len(plan.Steps) > 0 {
		// One step needs no fusion: its own order is the answer, and running
		// rank fusion over a single list would replace a meaningful distance
		// with a rank-derived number that is not comparable to anything.
		//
		// Two do. A semantic search anchored on a memory is the case the
		// score-derivation rule was written for: graph proximity is context,
		// and the relevance a caller thresholds against must come from what
		// actually matched the query (see result.go).
		plan.Fused = len(plan.Steps) > 1
		return plan, nil
	}

	def, ok := p.slots.Get(q.OrderBy)
	switch {
	case !ok:
		return Plan{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"slot %d is not a field this build knows how to order by", q.OrderBy))
	case def.Retired:
		return Plan{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"%q is retired and rows written since carry no value for it", def.Name))
	case !def.Indexed:
		// Refused rather than answered by sorting: sorting would mean reading
		// every record the tenant owns to return ten, and it would do so
		// without saying that is what it did.
		return Plan{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"%q is not an indexed field, so ordering by it would mean reading every memory "+
				"you own to return %d", def.Name, q.Limit))
	}

	// A listing widens to its own bound, larger than a search's.
	plan.MaxBudget = q.Limit * p.listMax
	plan.Steps = append(plan.Steps, Step{
		Kind: StepAttrScan, Slot: q.OrderBy, Desc: q.Desc, Source: SourceAttr,
	})
	return plan, nil
}

// OrderingSlots are the fields a listing may be ordered by, in slot order.
// It is what an API surface validates a caller's `order_by` against, and what
// an error message lists when one is refused.
func (p *Planner) OrderingSlots() []schema.SlotDef { return p.slots.IndexedSlots() }

// SlotByName resolves a caller's field name to a slot, refusing anything that
// is not an access path.
func (p *Planner) SlotByName(name string) (uint16, error) {
	const op = "query.Planner.SlotByName"

	def, ok := p.slots.Lookup(name)
	if !ok || def.Retired || !def.Indexed {
		available := make([]string, 0, len(p.slots.IndexedSlots()))
		for _, d := range p.slots.IndexedSlots() {
			available = append(available, d.Name)
		}
		return 0, errs.E(errs.Invalid, op, fmt.Errorf(
			"%q is not a field memories can be ordered by; the available orderings are %v", name, available))
	}
	return def.Slot, nil
}
