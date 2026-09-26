package text

import (
	"bytes"
	"slices"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

// These are durable formats. Every row written by a shipped binary is read by
// every later one, so a round trip that loses a field is a corpus that answers
// differently after an upgrade.

func TestPostingRoundTrips(t *testing.T) {
	for _, want := range []posting{
		{TermFreq: 1, DocLen: 1},
		{TermFreq: 3, DocLen: 412},
		{TermFreq: 65535, DocLen: 1 << 20},
	} {
		got, err := decodePosting(encodePosting(want))
		if err != nil {
			t.Fatalf("decoding %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("posting round trip: got %+v, want %+v", got, want)
		}
	}
}

func TestDocRowRoundTrips(t *testing.T) {
	in := docRow{DocLen: 17, Terms: []string{"café", "東京", "\x01weather", "zebra"}}
	// The row stores a term *set*, sorted, so the round trip returns the set
	// rather than the order the tokeniser emitted. That is deliberate — see
	// TestDocRowEncodingIsIndependentOfInputOrder — and nothing reads the row
	// for order.
	want := []string{"\x01weather", "café", "zebra", "東京"}

	got, err := decodeDocRow(encodeDocRow(in))
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if got.DocLen != in.DocLen {
		t.Fatalf("doc length: got %d, want %d", got.DocLen, in.DocLen)
	}
	if !slices.Equal(got.Terms, want) {
		t.Fatalf("terms: got %q, want %q", got.Terms, want)
	}
}

// The encoding must be a function of the term set alone. A rebuild that
// re-derives a row has to produce the same bytes as the incremental write did,
// or "the rebuilt index is byte-identical" is a claim about map iteration
// order rather than about the index.
func TestDocRowEncodingIsIndependentOfInputOrder(t *testing.T) {
	a := encodeDocRow(docRow{DocLen: 4, Terms: []string{"beta", "alpha", "gamma", "alpha"}})
	b := encodeDocRow(docRow{DocLen: 4, Terms: []string{"gamma", "alpha", "beta"}})
	if !bytes.Equal(a, b) {
		t.Fatalf("two spellings of one term set encoded differently:\n  %x\n  %x", a, b)
	}
}

func TestStatsRoundTrip(t *testing.T) {
	want := Stats{Documents: 12345, TotalLength: 9876543, Rebuilding: true}
	got, err := decodeStats(encodeStats(want))
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if got != want {
		t.Fatalf("stats round trip: got %+v, want %+v", got, want)
	}
}

// A truncated or corrupt row must be reported, never guessed at. Derived data
// that silently decodes to zero is a corpus that reports itself empty.
func TestATruncatedRowIsRefused(t *testing.T) {
	full := encodeDocRow(docRow{DocLen: 9, Terms: []string{"alpha", "beta"}})
	for cut := 1; cut < len(full); cut++ {
		if _, err := decodeDocRow(full[:cut]); err == nil {
			t.Fatalf("a doc row truncated to %d of %d bytes decoded without error", cut, len(full))
		}
	}
	if _, err := decodePosting(nil); err == nil {
		t.Fatal("an empty posting decoded without error")
	}
	if _, err := decodeStats([]byte{0x01}); err == nil {
		t.Fatal("a truncated statistics row decoded without error")
	}
}

// The three reserved rows must sort clear of every posting a word could
// produce, and a scan of one term must not run into another's.
func TestReservedRowsCannotCollideWithPostings(t *testing.T) {
	const (
		tn tenant.ID        = "acme"
		ns tenant.Namespace = tenant.DefaultNamespace
	)
	rid := id.New()

	stats := statsKey(tn, ns)
	doc := docKey(tn, ns, rid)
	tag := postingKey(tn, ns, TagTerm("weather"), rid)
	word := postingKey(tn, ns, "weather", rid)

	all := [][]byte{stats, doc, tag, word}
	for i := range all {
		for j := i + 1; j < len(all); j++ {
			if bytes.Equal(all[i], all[j]) {
				t.Fatalf("two distinct rows share a key: %x", all[i])
			}
		}
	}

	// "cat" must not scan into "cats": the term is length-prefixed precisely
	// so a prefix of one term is not a prefix of its range.
	lower, upper := keys.TextTermRange(tn, ns, "cat")
	cats := postingKey(tn, ns, "cats", rid)
	if bytes.Compare(cats, lower) >= 0 && bytes.Compare(cats, upper) < 0 {
		t.Fatal(`a posting for "cats" falls inside the range scanned for "cat"`)
	}
}
