package keys_test

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
)

func TestNegativeZeroSortsBelowPositiveZero(t *testing.T) {
	neg := keys.PutFloat64(nil, math.Copysign(0, -1))
	pos := keys.PutFloat64(nil, 0)
	if bytes.Compare(neg, pos) > 0 {
		t.Fatal("-0.0 must not sort above +0.0")
	}
}

// The ordering that matters, spelled out end to end. If this list encodes into
// ascending byte order, the attribute index is sound for float values.
func TestFloat64OrderingAcrossTheWholeRange(t *testing.T) {
	ordered := []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-1e10, -1.5, -1.0, -0.5,
		-math.SmallestNonzeroFloat64,
		math.Copysign(0, -1),
		0,
		math.SmallestNonzeroFloat64,
		0.5, 1.0, 1.5, 1e10,
		math.MaxFloat64,
		math.Inf(1),
	}
	assertAscending(t, ordered, func(v float64) []byte { return keys.PutFloat64(nil, v) })
}

func TestFloat32OrderingAcrossTheWholeRange(t *testing.T) {
	ordered := []float32{
		float32(math.Inf(-1)),
		-math.MaxFloat32, -1.5, -1.0,
		-math.SmallestNonzeroFloat32,
		float32(math.Copysign(0, -1)),
		0,
		math.SmallestNonzeroFloat32,
		1.0, 1.5,
		math.MaxFloat32,
		float32(math.Inf(1)),
	}
	assertAscending(t, ordered, func(v float32) []byte { return keys.PutFloat32(nil, v) })
}

func TestInt64OrderingCrossesZero(t *testing.T) {
	// Two's complement puts -1 (0xFF..FF) above +1 as raw bytes. This is the
	// case the sign flip exists for.
	ordered := []int64{
		math.MinInt64, -1 << 40, -256, -1, 0, 1, 256, 1 << 40, math.MaxInt64,
	}
	assertAscending(t, ordered, func(v int64) []byte { return keys.PutInt64(nil, v) })
}

func TestUint64Ordering(t *testing.T) {
	ordered := []uint64{0, 1, 255, 256, 1 << 32, math.MaxUint64}
	assertAscending(t, ordered, func(v uint64) []byte { return keys.PutUint64(nil, v) })
}

func TestBoolOrdering(t *testing.T) {
	if bytes.Compare(keys.PutBool(nil, false), keys.PutBool(nil, true)) >= 0 {
		t.Fatal("false must sort below true")
	}
}

func TestStringOrderingMatchesGo(t *testing.T) {
	// Every pair, not just neighbours: an encoding can preserve a chain and
	// still invert a distant pair.
	values := []string{
		"", "\x00", "\x00\x00", "\x00a", "a", "a\x00", "a\x00b", "aa", "ab", "b",
		"cat", "cats", "z", "\xff", "é",
	}
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	assertAscending(t, sorted, func(v string) []byte { return keys.PutString(nil, v) })
}

func TestStringEncodingHandlesEmbeddedNUL(t *testing.T) {
	// An unescaped NUL would terminate the field early and collide with a
	// different value.
	got, rest, err := keys.GetString(keys.PutString(nil, "a\x00b"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "a\x00b" {
		t.Fatalf("NUL round trip failed: %q", got)
	}
	if len(rest) != 0 {
		t.Fatalf("decoder left %d bytes behind", len(rest))
	}

	// The collision the escape prevents, stated directly.
	if bytes.Equal(keys.PutString(nil, "a\x00b"), keys.PutString(nil, "a")) {
		t.Fatal("\"a\\x00b\" and \"a\" encoded identically")
	}
}

func TestStringDecoderStopsAtItsOwnTerminator(t *testing.T) {
	// A value in a key is followed by more key. The decoder must hand back
	// exactly what follows the terminator.
	enc := keys.PutString(nil, "term")
	enc = append(enc, 0xAA, 0xBB)

	got, rest, err := keys.GetString(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != "term" || !bytes.Equal(rest, []byte{0xAA, 0xBB}) {
		t.Fatalf("GetString = %q, rest %x", got, rest)
	}
}

func TestDecodersRejectTruncatedInput(t *testing.T) {
	cases := map[string]func([]byte) error{
		"uint64":  func(b []byte) error { _, _, err := keys.GetUint64(b); return err },
		"int64":   func(b []byte) error { _, _, err := keys.GetInt64(b); return err },
		"float64": func(b []byte) error { _, _, err := keys.GetFloat64(b); return err },
		"float32": func(b []byte) error { _, _, err := keys.GetFloat32(b); return err },
		"bool":    func(b []byte) error { _, _, err := keys.GetBool(b); return err },
		"string":  func(b []byte) error { _, _, err := keys.GetString(b); return err },
	}
	for name, decode := range cases {
		t.Run(name, func(t *testing.T) {
			// Bytes that came out of the store and cannot be read are
			// corruption, not a caller mistake.
			if err := decode(nil); !errs.Is(err, errs.Corruption) {
				t.Fatalf("empty input: want Corruption, got %v", err)
			}
		})
	}
}

func TestStringDecoderRejectsAnUnterminatedValue(t *testing.T) {
	if _, _, err := keys.GetString([]byte("abc")); !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption for an unterminated string, got %v", err)
	}
	if _, _, err := keys.GetString([]byte{'a', 0x00, 0x7F}); !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption for a bad escape, got %v", err)
	}
	if _, _, err := keys.GetBool([]byte{0x02}); !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption for a bool that is neither, got %v", err)
	}
}

func TestNaNRoundTripsAndSortsAbovePositiveInfinity(t *testing.T) {
	// NaN has no order, so it cannot be placed correctly — but it must not
	// corrupt the index or fail to decode. Above +inf is the least surprising
	// place for it.
	nan := keys.PutFloat64(nil, math.NaN())
	inf := keys.PutFloat64(nil, math.Inf(1))
	if bytes.Compare(nan, inf) <= 0 {
		t.Fatal("NaN must land above +inf rather than among real values")
	}
	got, _, err := keys.GetFloat64(nan)
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(got) {
		t.Fatalf("NaN did not round trip: %v", got)
	}
}

func TestEncodersAppendRatherThanAllocate(t *testing.T) {
	// A composite index entry is several values in one key. Every encoder
	// appends so that costs one allocation, not one per field.
	dst := make([]byte, 0, 64)
	before := cap(dst)

	dst = keys.PutUint64(dst, 1)
	dst = keys.PutInt64(dst, -1)
	dst = keys.PutFloat64(dst, 1.5)
	dst = keys.PutFloat32(dst, 1.5)
	dst = keys.PutBool(dst, true)
	dst = keys.PutString(dst, "tail")

	if cap(dst) != before {
		t.Fatalf("encoding six values into a 64-byte buffer reallocated (cap %d -> %d)", before, cap(dst))
	}
	if len(dst) != 8+8+8+4+1+6 {
		t.Fatalf("unexpected encoded length %d", len(dst))
	}
}

func TestRoundTripsInSequence(t *testing.T) {
	// Decoders must hand the remainder on, or a composite key cannot be read
	// back at all.
	var b []byte
	b = keys.PutUint64(b, 7)
	b = keys.PutInt64(b, -7)
	b = keys.PutFloat64(b, -2.5)
	b = keys.PutFloat32(b, 2.5)
	b = keys.PutBool(b, true)
	b = keys.PutString(b, "end")

	u, b, err := keys.GetUint64(b)
	mustDecode(t, err)
	i, b, err := keys.GetInt64(b)
	mustDecode(t, err)
	f64, b, err := keys.GetFloat64(b)
	mustDecode(t, err)
	f32, b, err := keys.GetFloat32(b)
	mustDecode(t, err)
	bl, b, err := keys.GetBool(b)
	mustDecode(t, err)
	s, b, err := keys.GetString(b)
	mustDecode(t, err)

	if u != 7 || i != -7 || f64 != -2.5 || f32 != 2.5 || !bl || s != "end" {
		t.Fatalf("round trip lost information: %v %v %v %v %v %q", u, i, f64, f32, bl, s)
	}
	if len(b) != 0 {
		t.Fatalf("%d bytes left over", len(b))
	}
}

func mustDecode(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// assertAscending checks every pair, not only neighbours: an encoding can
// preserve a chain and still invert a distant pair.
func assertAscending[T any](t *testing.T, ordered []T, encode func(T) []byte) {
	t.Helper()
	enc := make([][]byte, len(ordered))
	for i, v := range ordered {
		enc[i] = encode(v)
	}
	for i := 0; i < len(ordered); i++ {
		for j := i + 1; j < len(ordered); j++ {
			if bytes.Compare(enc[i], enc[j]) >= 0 {
				t.Errorf("order lost: %v (%x) must sort below %v (%x)",
					ordered[i], enc[i], ordered[j], enc[j])
			}
		}
	}
}
