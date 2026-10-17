package vector_test

import (
	"bytes"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/vector"
	"pgregory.net/rapid"
)

func TestAPassageRowRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		start := rapid.Uint32().Draw(rt, "start")
		end := start + rapid.Uint32Range(0, 1<<20).Draw(rt, "length")
		if end < start {
			end = start // overflow: a span at the very top of the range
		}
		dim := rapid.IntRange(1, 64).Draw(rt, "dim")
		values := rapid.SliceOfN(rapid.Float32Range(-1, 1), dim, dim).Draw(rt, "values")
		in := vector.PassageRow{Start: start, End: end,
			Vector: &vector.Vector{ModelID: "model-" + rapid.StringMatching(`[a-z0-9]{1,12}`).Draw(rt, "model"), Dim: dim, Values: values}}

		b, err := vector.EncodePassage(in)
		if err != nil {
			rt.Fatalf("encode: %v", err)
		}
		out, err := vector.DecodePassage(b)
		if err != nil {
			rt.Fatalf("decode: %v", err)
		}
		if out.Start != in.Start || out.End != in.End || out.Vector.ModelID != in.Vector.ModelID ||
			out.Vector.Dim != in.Vector.Dim || len(out.Vector.Values) != dim {
			rt.Fatalf("round trip lost something: %+v against %+v", out, in)
		}
		for i := range values {
			if out.Vector.Values[i] != values[i] {
				rt.Fatalf("value %d is %v, want %v", i, out.Vector.Values[i], values[i])
			}
		}
		// The framing after the span is exactly a canonical vector's, so one
		// decoder reads both and they cannot drift apart.
		canonical, err := vector.Encode(in.Vector)
		if err != nil || !bytes.Equal(b[9:], canonical) {
			rt.Fatal("a passage row's vector is not framed as a canonical vector")
		}
	})
}

func TestAMalformedPassageRowIsCorruption(t *testing.T) {
	good, err := vector.EncodePassage(vector.PassageRow{Start: 3, End: 9,
		Vector: &vector.Vector{ModelID: "m", Dim: 2, Values: []float32{0.6, 0.8}}})
	if err != nil {
		t.Fatal(err)
	}
	backwards := append([]byte(nil), good...)
	copy(backwards[1:9], []byte{0, 0, 0, 9, 0, 0, 0, 3})
	newer := append([]byte(nil), good...)
	newer[0] = 2

	for name, b := range map[string][]byte{
		"empty":            {},
		"no span":          good[:5],
		"no vector":        good[:9],
		"truncated vector": good[:len(good)-3],
		"a newer format":   newer,
		"an inverted span": backwards,
	} {
		if _, err := vector.DecodePassage(b); !errs.Is(err, errs.Corruption) {
			t.Errorf("%s: %v, want Corruption", name, err)
		}
	}
}

func TestAnInvertedSpanIsNotEncoded(t *testing.T) {
	_, err := vector.EncodePassage(vector.PassageRow{Start: 9, End: 3,
		Vector: &vector.Vector{ModelID: "m", Dim: 1, Values: []float32{1}}})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("an inverted span was encoded: %v", err)
	}
}

func FuzzDecodePassage(f *testing.F) {
	good, _ := vector.EncodePassage(vector.PassageRow{Start: 3, End: 9,
		Vector: &vector.Vector{ModelID: "m", Dim: 2, Values: []float32{0.6, 0.8}}})
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		row, err := vector.DecodePassage(b)
		if err != nil {
			if !errs.Is(err, errs.Corruption) {
				t.Fatalf("%x refused as %v, want Corruption", b, err)
			}
			return
		}
		if row.Start > row.End || row.Vector == nil || len(row.Vector.Values) != row.Vector.Dim {
			t.Fatalf("%x decoded to an impossible row %+v", b, row)
		}
		again, err := vector.EncodePassage(row)
		if err != nil {
			t.Fatalf("a decoded row does not re-encode: %v", err)
		}
		if back, err := vector.DecodePassage(again); err != nil || back.Start != row.Start || back.End != row.End {
			t.Fatalf("a re-encoded row does not decode to itself: %v", err)
		}
	})
}
