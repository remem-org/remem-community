package text

import (
	"bytes"
	"slices"
	"testing"
	"unicode/utf8"

	"pgregory.net/rapid"
)

// Every durable row in this package round-trips, for every value it can hold.
// The table tests above cover the shapes a person thinks of; this covers the
// ones a corpus produces.

func TestPostingRoundTripsForEveryValue(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		want := posting{
			TermFreq: rapid.Uint32().Draw(t, "term_freq"),
			DocLen:   rapid.Uint32().Draw(t, "doc_len"),
		}
		got, err := decodePosting(encodePosting(want))
		if err != nil {
			t.Fatalf("decoding %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
}

func TestStatsRoundTripForEveryValue(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		want := Stats{
			Documents:   rapid.Uint64().Draw(t, "documents"),
			TotalLength: rapid.Uint64().Draw(t, "total_length"),
			Rebuilding:  rapid.Bool().Draw(t, "rebuilding"),
		}
		got, err := decodeStats(encodeStats(want))
		if err != nil {
			t.Fatalf("decoding %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
}

// A term row round-trips as the sorted, de-duplicated set it stores, and its
// bytes depend on that set and nothing else. The second half is what makes
// "the rebuilt index is byte-identical" a property of the encoding rather than
// of the order a map happened to iterate in.
func TestDocRowRoundTripsAndIsOrderIndependent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		terms := rapid.SliceOfN(rapid.String(), 0, 24).Draw(t, "terms")
		row := docRow{DocLen: rapid.Uint32().Draw(t, "doc_len"), Terms: terms}

		encoded := encodeDocRow(row)
		got, err := decodeDocRow(encoded)
		if err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if got.DocLen != row.DocLen {
			t.Fatalf("doc length: got %d, want %d", got.DocLen, row.DocLen)
		}

		want := slices.Clone(terms)
		slices.Sort(want)
		want = slices.Compact(want)
		if len(want) == 0 {
			want = []string{}
		}
		if !slices.Equal(got.Terms, want) {
			t.Fatalf("terms: got %q, want %q", got.Terms, want)
		}

		shuffled := slices.Clone(terms)
		for i, j := range rapid.SliceOfN(rapid.IntRange(0, max(len(terms)-1, 0)), len(terms), len(terms)).
			Draw(t, "swaps") {
			if len(shuffled) > 0 {
				shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
			}
		}
		if !bytes.Equal(encoded, encodeDocRow(docRow{DocLen: row.DocLen, Terms: shuffled})) {
			t.Fatal("the same term set encoded differently after a reordering")
		}
	})
}

// The tokeniser's output is what a key holds. Whatever it is given, every term
// it produces must be storable: non-empty, within the key bound, valid UTF-8,
// and outside the reserved range the three non-posting rows live in.
func TestEveryTermIsStorable(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		in := rapid.String().Draw(t, "content")
		for _, term := range Tokenize(in) {
			switch {
			case term == "":
				t.Fatalf("%q produced an empty term", in)
			case len(term) > MaxTermBytes:
				t.Fatalf("%q produced a %d-byte term", in, len(term))
			case term[0] < 0x20:
				t.Fatalf("%q produced the reserved term %q", in, term)
			case !utf8.ValidString(term):
				// Runs are cut on rune boundaries and a folded long term is
				// truncated to one, so a term is always text — which is what
				// keeps a log line holding one readable.
				t.Fatalf("%q produced the invalid term %q", in, term)
			}
		}
	})
}
