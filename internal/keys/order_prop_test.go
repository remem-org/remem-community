package keys_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/keys"
	"pgregory.net/rapid"
)

// Property tests for the order-preserving encoding (spec §46.4).
//
// The deterministic cases in order_test.go check the values a person thinks
// of. These check the values a person does not: subnormals, values that differ
// in one mantissa bit, strings of NULs. Both are needed — a table of examples
// cannot cover a float, and a generator cannot express intent.

func TestFloatEncodingPreservesOrder(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.Float64().Draw(rt, "a")
		b := rapid.Float64().Draw(rt, "b")
		if math.IsNaN(a) || math.IsNaN(b) {
			rt.Skip("NaN has no order")
		}

		ea := keys.PutFloat64(nil, a)
		eb := keys.PutFloat64(nil, b)

		want := cmpFloat(a, b)
		if got := sign(bytes.Compare(ea, eb)); got != want {
			rt.Fatalf("order lost: %v vs %v encoded to %x vs %x (want %d, got %d)", a, b, ea, eb, want, got)
		}
	})
}

func TestFloat32EncodingPreservesOrder(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.Float32().Draw(rt, "a")
		b := rapid.Float32().Draw(rt, "b")
		if math.IsNaN(float64(a)) || math.IsNaN(float64(b)) {
			rt.Skip("NaN has no order")
		}

		ea := keys.PutFloat32(nil, a)
		eb := keys.PutFloat32(nil, b)

		want := cmpFloat(float64(a), float64(b))
		if got := sign(bytes.Compare(ea, eb)); got != want {
			rt.Fatalf("order lost: %v vs %v encoded to %x vs %x", a, b, ea, eb)
		}
	})
}

func TestFloatEncodingRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		v := rapid.Float64().Draw(rt, "v")
		if math.IsNaN(v) {
			rt.Skip("NaN")
		}
		got, rest, err := keys.GetFloat64(keys.PutFloat64(nil, v))
		if err != nil {
			rt.Fatal(err)
		}
		if len(rest) != 0 {
			rt.Fatalf("decoder left %d bytes behind", len(rest))
		}
		// Signbit as well as value: +0.0 and -0.0 are equal under == and are
		// different values in an index.
		if got != v || math.Signbit(got) != math.Signbit(v) {
			rt.Fatalf("round trip changed %v into %v", v, got)
		}
	})
}

func TestFloat32EncodingRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		v := rapid.Float32().Draw(rt, "v")
		if math.IsNaN(float64(v)) {
			rt.Skip("NaN")
		}
		got, _, err := keys.GetFloat32(keys.PutFloat32(nil, v))
		if err != nil {
			rt.Fatal(err)
		}
		if got != v || math.Signbit(float64(got)) != math.Signbit(float64(v)) {
			rt.Fatalf("round trip changed %v into %v", v, got)
		}
	})
}

func TestIntEncodingPreservesOrderAndRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.Int64().Draw(rt, "a")
		b := rapid.Int64().Draw(rt, "b")

		ea, eb := keys.PutInt64(nil, a), keys.PutInt64(nil, b)
		if got, want := sign(bytes.Compare(ea, eb)), cmpInt(a, b); got != want {
			rt.Fatalf("order lost: %d vs %d encoded to %x vs %x", a, b, ea, eb)
		}

		got, _, err := keys.GetInt64(ea)
		if err != nil {
			rt.Fatal(err)
		}
		if got != a {
			rt.Fatalf("round trip changed %d into %d", a, got)
		}
	})
}

func TestUintEncodingPreservesOrderAndRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.Uint64().Draw(rt, "a")
		b := rapid.Uint64().Draw(rt, "b")

		ea, eb := keys.PutUint64(nil, a), keys.PutUint64(nil, b)
		want := 0
		switch {
		case a < b:
			want = -1
		case a > b:
			want = 1
		}
		if got := sign(bytes.Compare(ea, eb)); got != want {
			rt.Fatalf("order lost: %d vs %d", a, b)
		}

		got, _, err := keys.GetUint64(ea)
		if err != nil {
			rt.Fatal(err)
		}
		if got != a {
			rt.Fatalf("round trip changed %d into %d", a, got)
		}
	})
}

func TestStringEncodingPreservesOrderAndRoundTrips(t *testing.T) {
	// Bytes rather than valid UTF-8: a tag or a source field can hold
	// anything, and the encoding must not assume otherwise.
	gen := rapid.SliceOfN(rapid.Byte(), 0, 32)

	rapid.Check(t, func(rt *rapid.T) {
		a := string(gen.Draw(rt, "a"))
		b := string(gen.Draw(rt, "b"))

		ea, eb := keys.PutString(nil, a), keys.PutString(nil, b)
		want := sign(bytes.Compare([]byte(a), []byte(b)))
		if got := sign(bytes.Compare(ea, eb)); got != want {
			rt.Fatalf("order lost: %q vs %q encoded to %x vs %x", a, b, ea, eb)
		}

		got, rest, err := keys.GetString(ea)
		if err != nil {
			rt.Fatal(err)
		}
		if got != a {
			rt.Fatalf("round trip changed %q into %q", a, got)
		}
		if len(rest) != 0 {
			rt.Fatalf("decoder left %d bytes behind", len(rest))
		}
	})
}

func TestStringEncodingStopsAtItsTerminator(t *testing.T) {
	// The property that makes a composite key readable: whatever follows the
	// encoded value comes back untouched, whatever the value contained.
	gen := rapid.SliceOfN(rapid.Byte(), 0, 32)

	rapid.Check(t, func(rt *rapid.T) {
		v := string(gen.Draw(rt, "v"))
		tail := gen.Draw(rt, "tail")

		enc := append(keys.PutString(nil, v), tail...)
		got, rest, err := keys.GetString(enc)
		if err != nil {
			rt.Fatal(err)
		}
		if got != v {
			rt.Fatalf("round trip changed %q into %q", v, got)
		}
		if !bytes.Equal(rest, tail) {
			rt.Fatalf("the bytes after the value were disturbed: %x, want %x", rest, tail)
		}
	})
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	case math.Signbit(a) != math.Signbit(b):
		// -0.0 and +0.0 compare equal but are distinct index entries.
		if math.Signbit(a) {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
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
