package attr

import (
	"encoding/json"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/schema"
)

// Predicates serialise, and that is a structural requirement rather than a
// convenience.
//
// docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md §3 records the property Rust Remem
// lacks: a query that holds closures or index handles cannot be sent anywhere,
// so it cannot be planned on one node and executed on another. Everything in
// this file exists so that a filter is data. A Pred that could not survive
// JSON would put the whole query layer back where Rust's is.
//
// The wire shape is a tagged union — an "op" discriminator and a typed value —
// because a predicate read back with the wrong type is a filter that silently
// matches the wrong records.

// MarshalJSON writes a value as its type and its content.
func (v Value) MarshalJSON() ([]byte, error) {
	w := valueJSON{Type: v.typ.String()}
	switch v.typ {
	case schema.SlotBool:
		b, _ := v.AsBool()
		w.Value = b
	case schema.SlotU8, schema.SlotU32, schema.SlotU64:
		n, _ := v.AsUint()
		w.Value = n
	case schema.SlotF32:
		f, _ := v.AsFloat32()
		w.Value = f
	case schema.SlotString:
		s, _ := v.AsString()
		w.Value = s
	default:
		return nil, errs.E(errs.Invalid, "attr.Value.MarshalJSON",
			fmt.Errorf("an untyped value has no representation"))
	}
	return json.Marshal(w)
}

// UnmarshalJSON reads a value written by [Value.MarshalJSON].
func (v *Value) UnmarshalJSON(b []byte) error {
	const op = "attr.Value.UnmarshalJSON"

	var w valueJSON
	if err := json.Unmarshal(b, &w); err != nil {
		return errs.E(errs.Invalid, op, err)
	}
	raw, err := json.Marshal(w.Value)
	if err != nil {
		return errs.E(errs.Invalid, op, err)
	}
	switch w.Type {
	case schema.SlotBool.String():
		var x bool
		if err := json.Unmarshal(raw, &x); err != nil {
			return errs.E(errs.Invalid, op, err)
		}
		*v = Bool(x)
	case schema.SlotU8.String():
		var x uint8
		if err := json.Unmarshal(raw, &x); err != nil {
			return errs.E(errs.Invalid, op, err)
		}
		*v = U8(x)
	case schema.SlotU32.String():
		var x uint32
		if err := json.Unmarshal(raw, &x); err != nil {
			return errs.E(errs.Invalid, op, err)
		}
		*v = U32(x)
	case schema.SlotU64.String():
		var x uint64
		if err := json.Unmarshal(raw, &x); err != nil {
			return errs.E(errs.Invalid, op, err)
		}
		*v = U64(x)
	case schema.SlotF32.String():
		var x float32
		if err := json.Unmarshal(raw, &x); err != nil {
			return errs.E(errs.Invalid, op, err)
		}
		*v = F32(x)
	case schema.SlotString.String():
		var x string
		if err := json.Unmarshal(raw, &x); err != nil {
			return errs.E(errs.Invalid, op, err)
		}
		*v = Str(x)
	default:
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"%q is not a slot type this binary knows; a value read at the wrong type is a filter "+
				"that matches the wrong records", w.Type))
	}
	return nil
}

type valueJSON struct {
	Type  string `json:"t"`
	Value any    `json:"v"`
}

// Preds is a conjunction of predicates that round-trips through JSON.
//
// It is a named slice rather than []Pred so it can carry the unmarshaller: Go
// cannot decode into an interface without being told which implementation, and
// the "op" discriminator is what tells it.
type Preds []Pred

// MarshalJSON writes each predicate in its tagged form.
func (p Preds) MarshalJSON() ([]byte, error) {
	out := make([]json.RawMessage, 0, len(p))
	for _, q := range p {
		b, err := marshalPred(q)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return json.Marshal(out)
}

// UnmarshalJSON reads predicates written by [Preds.MarshalJSON].
func (p *Preds) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return errs.E(errs.Invalid, "attr.Preds.UnmarshalJSON", err)
	}
	out := make(Preds, 0, len(raw))
	for _, r := range raw {
		q, err := unmarshalPred(r)
		if err != nil {
			return err
		}
		out = append(out, q)
	}
	*p = out
	return nil
}

type predJSON struct {
	Op     string            `json:"op"`
	Slot   uint16            `json:"slot,omitempty"`
	Value  *Value            `json:"value,omitempty"`
	Lo     *Value            `json:"lo,omitempty"`
	Hi     *Value            `json:"hi,omitempty"`
	LoIncl bool              `json:"lo_incl,omitempty"`
	HiIncl bool              `json:"hi_incl,omitempty"`
	Preds  []json.RawMessage `json:"preds,omitempty"`
	Pred   json.RawMessage   `json:"pred,omitempty"`
}

func marshalPred(p Pred) ([]byte, error) {
	const op = "attr.marshalPred"

	switch q := p.(type) {
	case *eqPred:
		v := q.value
		return json.Marshal(predJSON{Op: "eq", Slot: q.slot, Value: &v})
	case *rangePred:
		return json.Marshal(predJSON{
			Op: "range", Slot: q.slot, Lo: q.lo, Hi: q.hi, LoIncl: q.loIncl, HiIncl: q.hiIncl,
		})
	case *andPred:
		subs := make([]json.RawMessage, 0, len(q.preds))
		for _, sub := range q.preds {
			b, err := marshalPred(sub)
			if err != nil {
				return nil, err
			}
			subs = append(subs, b)
		}
		return json.Marshal(predJSON{Op: "and", Preds: subs})
	case *notPred:
		b, err := marshalPred(q.pred)
		if err != nil {
			return nil, err
		}
		return json.Marshal(predJSON{Op: "not", Pred: b})
	default:
		// A predicate implemented outside this package cannot be sent
		// anywhere, so it is refused here rather than silently dropped from a
		// filter — a filter with a missing clause returns more than it should.
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"predicate %T is not one this package can serialise", p))
	}
}

func unmarshalPred(b []byte) (Pred, error) {
	const op = "attr.unmarshalPred"

	var w predJSON
	if err := json.Unmarshal(b, &w); err != nil {
		return nil, errs.E(errs.Invalid, op, err)
	}
	switch w.Op {
	case "eq":
		if w.Value == nil {
			return nil, errs.E(errs.Invalid, op, fmt.Errorf("an equality predicate carries no value"))
		}
		return Eq(w.Slot, *w.Value), nil
	case "range":
		return Range(w.Slot, w.Lo, w.Hi, w.LoIncl, w.HiIncl), nil
	case "and":
		subs := make([]Pred, 0, len(w.Preds))
		for _, r := range w.Preds {
			q, err := unmarshalPred(r)
			if err != nil {
				return nil, err
			}
			subs = append(subs, q)
		}
		return And(subs...), nil
	case "not":
		q, err := unmarshalPred(w.Pred)
		if err != nil {
			return nil, err
		}
		return Not(q), nil
	default:
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"%q is not a predicate this binary knows", w.Op))
	}
}
