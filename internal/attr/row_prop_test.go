package attr_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/schema"
	"pgregory.net/rapid"
)

// Property tests on the two encodings this package owns: the packed row, and
// the order-preserving index value.
//
// CLAUDE.md makes them mandatory for every serialisation round trip and for
// index invariants, and the reason is visible in both cases here. A row that
// misparses one slot still decodes into a plausible row of plausible numbers,
// and an ordering that is subtly wrong still produces bytes that sort — just
// not in the order of the values they came from.

func TestARowRoundTripsForAnyValues(t *testing.T) {
	table := attr.MustTable()

	rapid.Check(t, func(rt *rapid.T) {
		want := drawRow(rt, table)

		encoded, err := want.Encode(table)
		if err != nil {
			rt.Fatalf("encode: %v", err)
		}
		got, err := attr.DecodeRow(encoded, table)
		if err != nil {
			rt.Fatalf("decode: %v", err)
		}
		if got.SchemaVersion != want.SchemaVersion {
			rt.Fatalf("schema version %d, want %d", got.SchemaVersion, want.SchemaVersion)
		}
		if got.Len() != want.Len() {
			rt.Fatalf("decoded %d slots, want %d", got.Len(), want.Len())
		}
		for _, slot := range want.Slots() {
			w, _ := want.Get(slot)
			g, ok := got.Get(slot)
			if !ok {
				rt.Fatalf("slot %d did not survive", slot)
			}
			if g != w {
				rt.Fatalf("slot %d decoded as %s, want %s", slot, g, w)
			}
		}
	})
}

// The index invariant: byte order is value order, for every pair of values of
// one type. This is what makes "the ten most important memories" a bounded scan
// rather than a sort over everything the tenant owns, and an implementation
// that gets it subtly wrong still round-trips and still looks sorted.
func TestOrderEncodingPreservesOrder(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		typ := rapid.SampledFrom([]schema.SlotType{
			schema.SlotBool, schema.SlotU8, schema.SlotU32, schema.SlotU64,
			schema.SlotF32, schema.SlotString,
		}).Draw(rt, "type")

		a := drawValueOfType(rt, typ, "a")
		b := drawValueOfType(rt, typ, "b")

		byValue := a.Compare(b)
		byBytes := bytes.Compare(a.OrderBytes(), b.OrderBytes())

		if sign(byValue) != sign(byBytes) {
			rt.Fatalf("%s and %s compare %d by value and %d by bytes: an index over this slot "+
				"would return records in an order that is not the order of their values",
				a, b, byValue, byBytes)
		}
	})
}

// A string value's bytes must never contain an unescaped terminator, or two
// different strings would produce index entries that collide — one silently
// overwriting the other's.
func TestStringValuesCannotCollideInAnIndexKey(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.String().Draw(rt, "a")
		b := rapid.String().Draw(rt, "b")

		sameBytes := bytes.Equal(attr.Str(a).OrderBytes(), attr.Str(b).OrderBytes())
		if sameBytes != (a == b) {
			rt.Fatalf("%q and %q encode to %v identical bytes; two different values sharing an "+
				"encoding means one index entry overwrites the other", a, b, sameBytes)
		}
	})
}

func drawRow(rt *rapid.T, table *schema.Slots) *attr.Row {
	row := attr.NewRow(rapid.Uint32().Draw(rt, "schema_version"))
	for _, def := range table.Defs() {
		if !rapid.Bool().Draw(rt, "present") {
			continue
		}
		row.Set(def.Slot, drawValueOfType(rt, def.Type, def.Name))
	}
	return row
}

func drawValueOfType(rt *rapid.T, typ schema.SlotType, label string) attr.Value {
	switch typ {
	case schema.SlotBool:
		return attr.Bool(rapid.Bool().Draw(rt, label))
	case schema.SlotU8:
		return attr.U8(rapid.Uint8().Draw(rt, label))
	case schema.SlotU32:
		return attr.U32(rapid.Uint32().Draw(rt, label))
	case schema.SlotU64:
		return attr.U64(rapid.Uint64().Draw(rt, label))
	case schema.SlotF32:
		// Infinities and the signed zeroes are drawn deliberately: they are
		// exactly where an order-preserving float encoding goes wrong, and
		// where a wrong one still produces plausible bytes. Drawing -0.0 is
		// what found the normalisation this constructor now does; NaN is drawn
		// through the same constructor, which folds it — see
		// TestTheUnindexableFloatsAreFolded.
		return attr.F32(rapid.OneOf(
			rapid.Float32(),
			rapid.SampledFrom([]float32{
				0, float32(math.Copysign(0, -1)),
				float32(math.Inf(1)), float32(math.Inf(-1)),
				math.SmallestNonzeroFloat32, math.MaxFloat32, -math.MaxFloat32,
			}),
		).Draw(rt, label))
	case schema.SlotString:
		// Including NULs, which is the case the escaping exists for.
		return attr.Str(rapid.OneOf(
			rapid.String(),
			rapid.SampledFrom([]string{"", "\x00", "a\x00b", "a\x00\x00b", "\xff", "a\x00\xffb"}),
		).Draw(rt, label))
	default:
		rt.Fatalf("no generator for slot type %s", typ)
		return attr.Value{}
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// The two float values that have to be normalised before they reach an index,
// both of which arrive from arithmetic rather than from a caller.
func TestTheUnindexableFloatsAreFolded(t *testing.T) {
	// NaN order-encodes above every finite value, so one poisons every range
	// query over its slot.
	if got, ok := attr.F32(float32(math.NaN())).AsFloat32(); !ok || got != 0 {
		t.Errorf("a NaN importance became %v (typed=%t), want 0", got, ok)
	}

	// Negative zero is the same number as positive zero and a different bit
	// pattern, so an index that kept the distinction would hold one value at
	// two positions — and a record at the lower one would be invisible to a
	// filter for 0.
	negZero := attr.F32(float32(math.Copysign(0, -1)))
	if negZero != attr.F32(0) {
		t.Errorf("-0.0 and +0.0 are distinct values here; a memory stored at one would not be " +
			"found by a filter for the other")
	}
	if !bytes.Equal(negZero.OrderBytes(), attr.F32(0).OrderBytes()) {
		t.Error("-0.0 and +0.0 encode to different index positions")
	}
}
