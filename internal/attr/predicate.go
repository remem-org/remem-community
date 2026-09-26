package attr

import (
	"fmt"
	"strings"
)

// Pred is a condition on an attribute row.
//
// It is a pushed-down predicate: it is settled from the sidecar row, not from
// the record, so a filter costs a small fixed read per candidate rather than a
// protobuf decode. That is the difference between filtering a hundred thousand
// records and reading a hundred thousand records.
//
// Slot reports the single slot a predicate is about, when it is about one. That
// is what lets a planner turn a condition on the ordering slot into narrower
// walk bounds instead of a scan-and-discard. A predicate spanning several slots
// reports false and is settled per candidate, which is correct but not cheap.
type Pred interface {
	// Eval reports whether row satisfies the predicate. A row that does not
	// carry the slot does not satisfy it: absent is not a value, and treating
	// it as a zero would make every record written before a slot existed match
	// `importance == 0`.
	Eval(row *Row) bool

	// Slot is the slot this predicate constrains, and whether it constrains
	// exactly one.
	Slot() (uint16, bool)

	// String renders the predicate for `explain` output and errors.
	String() string
}

// Eq matches rows whose slot holds exactly v.
func Eq(slot uint16, v Value) Pred { return &eqPred{slot: slot, value: v} }

// Range matches rows whose slot falls between lo and hi.
//
// A nil bound is unbounded on that side. The inclusivity flags are separate
// parameters rather than an encoded Bound type because both ends are set at
// every call site anyway, and a three-state enum per end reads worse than a
// flag at the one place it is written.
func Range(slot uint16, lo, hi *Value, loIncl, hiIncl bool) Pred {
	p := &rangePred{slot: slot, loIncl: loIncl, hiIncl: hiIncl}
	if lo != nil {
		v := *lo
		p.lo = &v
	}
	if hi != nil {
		v := *hi
		p.hi = &v
	}
	return p
}

// And matches rows satisfying every operand. No operands matches everything,
// which is what an empty filter means.
func And(preds ...Pred) Pred { return &andPred{preds: preds} }

// Not inverts a predicate.
//
// It reports no slot even when its operand does: the negation of "importance
// between 0.4 and 0.6" is two disjoint ranges, and narrowing a walk to either
// one of them would silently drop the other half of the matches.
func Not(p Pred) Pred { return &notPred{pred: p} }

type eqPred struct {
	slot  uint16
	value Value
}

func (p *eqPred) Eval(row *Row) bool {
	v, ok := row.Get(p.slot)
	return ok && v == p.value
}
func (p *eqPred) Slot() (uint16, bool) { return p.slot, true }
func (p *eqPred) String() string       { return fmt.Sprintf("slot%d == %s", p.slot, p.value) }

type rangePred struct {
	slot   uint16
	lo, hi *Value
	loIncl bool
	hiIncl bool
}

func (p *rangePred) Eval(row *Row) bool {
	v, ok := row.Get(p.slot)
	if !ok {
		return false
	}
	if p.lo != nil {
		c := v.Compare(*p.lo)
		if c < 0 || (c == 0 && !p.loIncl) {
			return false
		}
	}
	if p.hi != nil {
		c := v.Compare(*p.hi)
		if c > 0 || (c == 0 && !p.hiIncl) {
			return false
		}
	}
	return true
}
func (p *rangePred) Slot() (uint16, bool) { return p.slot, true }
func (p *rangePred) String() string {
	lo, hi := "-inf", "+inf"
	if p.lo != nil {
		lo = p.lo.String()
	}
	if p.hi != nil {
		hi = p.hi.String()
	}
	open, close := "(", ")"
	if p.loIncl {
		open = "["
	}
	if p.hiIncl {
		close = "]"
	}
	return fmt.Sprintf("slot%d in %s%s, %s%s", p.slot, open, lo, hi, close)
}

type andPred struct{ preds []Pred }

func (p *andPred) Eval(row *Row) bool {
	for _, q := range p.preds {
		if !q.Eval(row) {
			return false
		}
	}
	return true
}

// Slot reports a slot only when every operand names the same one, which is the
// case a planner can still narrow a walk with: "importance >= 0.5 and
// importance < 0.9" is one range.
func (p *andPred) Slot() (uint16, bool) {
	var slot uint16
	found := false
	for _, q := range p.preds {
		s, ok := q.Slot()
		if !ok {
			return 0, false
		}
		if found && s != slot {
			return 0, false
		}
		slot, found = s, true
	}
	return slot, found
}
func (p *andPred) String() string {
	parts := make([]string, len(p.preds))
	for i, q := range p.preds {
		parts[i] = q.String()
	}
	return "(" + strings.Join(parts, " and ") + ")"
}

type notPred struct{ pred Pred }

func (p *notPred) Eval(row *Row) bool   { return !p.pred.Eval(row) }
func (p *notPred) Slot() (uint16, bool) { return 0, false }
func (p *notPred) String() string       { return "not " + p.pred.String() }

// Match reports whether row satisfies every predicate. An empty list matches.
func Match(row *Row, preds []Pred) bool {
	for _, p := range preds {
		if !p.Eval(row) {
			return false
		}
	}
	return true
}

// bounds returns the walk bounds a predicate implies on its own slot, as
// encoded index-key values, and whether each end is inclusive.
//
// It is deliberately conservative in one direction only: a bound it cannot
// derive is reported as unbounded, which walks more of the index than
// necessary and returns the same answer. A bound derived too tightly would
// return fewer records than match, which is the failure that looks like data
// loss. Eval re-checks every candidate against the row regardless, so these
// bounds are an optimisation and never the decision.
func bounds(p Pred, slot uint16) (lo, hi []byte, loIncl, hiIncl bool) {
	loIncl, hiIncl = true, true
	switch q := p.(type) {
	case *eqPred:
		if q.slot != slot {
			return nil, nil, true, true
		}
		b := q.value.OrderBytes()
		return b, b, true, true
	case *rangePred:
		if q.slot != slot {
			return nil, nil, true, true
		}
		if q.lo != nil {
			lo, loIncl = q.lo.OrderBytes(), q.loIncl
		}
		if q.hi != nil {
			hi, hiIncl = q.hi.OrderBytes(), q.hiIncl
		}
		return lo, hi, loIncl, hiIncl
	case *andPred:
		for _, sub := range q.preds {
			sLo, sHi, sLoIncl, sHiIncl := bounds(sub, slot)
			lo, loIncl = tightenLow(lo, loIncl, sLo, sLoIncl)
			hi, hiIncl = tightenHigh(hi, hiIncl, sHi, sHiIncl)
		}
		return lo, hi, loIncl, hiIncl
	default:
		return nil, nil, true, true
	}
}
