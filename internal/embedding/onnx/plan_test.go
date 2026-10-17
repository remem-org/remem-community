package onnx

import (
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding"
)

// These run in every `go test`, without the model: they hold the arithmetic the
// reference test can only hold all at once.

func TestPassagesCoverTheTokensInOrder(t *testing.T) {
	for _, n := range []int{1, 255, 256, 257, 512, 4096, 4097, 10135} {
		p := planDocument(n)
		if want := (n + 255) / 256; len(p.passages) != want {
			t.Fatalf("%d tokens: %d passages, want %d", n, len(p.passages), want)
		}
		next := 0
		for i, s := range p.passages {
			if s.start != next || s.end-s.start > embedding.PassageTokens || s.end <= s.start {
				t.Fatalf("%d tokens: passage %d is %v after %d", n, i, s, next)
			}
			next = s.end
		}
		if next != n {
			t.Fatalf("%d tokens: passages end at %d", n, next)
		}
	}
}

// max(1, ceil((n - 2) / 14)) segments for n passages, each at most sixteen,
// overlapping by two, covering every passage.
func TestSegmentsOverlapByTwoAndCoverEveryPassage(t *testing.T) {
	for n := 1; n <= 100; n++ {
		p := planDocument(n * embedding.PassageTokens)
		want := 1
		if n > 2 {
			want = max(1, (n-2+13)/14)
		}
		if len(p.segments) != want {
			t.Fatalf("%d passages: %d segments, want %d", n, len(p.segments), want)
		}
		for i, s := range p.segments {
			if s.last-s.first > embedding.SegmentPassages || s.last <= s.first {
				t.Fatalf("%d passages: segment %d is %v", n, i, s)
			}
			if i > 0 && p.segments[i-1].last-s.first != embedding.SegmentOverlap {
				t.Fatalf("%d passages: segments %d and %d overlap by %d", n, i-1, i, p.segments[i-1].last-s.first)
			}
		}
		if p.segments[len(p.segments)-1].last != n {
			t.Fatalf("%d passages: the last segment ends at %d", n, p.segments[len(p.segments)-1].last)
		}
		for i := range n {
			if p.owner(i) < 0 {
				t.Fatalf("%d passages: passage %d has no segment", n, i)
			}
		}
	}
}

// The ownership the forty-passage reference case was generated with: segments
// [0,16) [14,30) [28,40), and each overlap passage goes to the segment where it
// has context on both sides.
func TestAnOverlapPassageTakesTheSegmentWhereItIsFarthestFromAnEdge(t *testing.T) {
	p := planDocument(40 * embedding.PassageTokens)
	want := map[int]int{13: 0, 14: 0, 15: 1, 16: 1, 28: 1, 29: 2, 30: 2}
	for passage, seg := range want {
		if got := p.owner(passage); got != seg {
			t.Errorf("passage %d owned by segment %d, want %d", passage, got, seg)
		}
	}
}

func TestASequenceJoinsPassagesWithSeparatorsAndRecordsWhereEachLanded(t *testing.T) {
	ids := make([]int64, 600)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	p := planDocument(len(ids))
	s := buildSequence(0, 0, ids, p)
	if len(s.ids) != 600+2 {
		t.Fatalf("%d ids for three passages and two separators", len(s.ids))
	}
	for k, sp := range s.spans {
		if k > 0 && s.ids[sp.start-1] != separatorID {
			t.Fatalf("passage %d is not preceded by a separator", k)
		}
		if s.ids[sp.start] != ids[p.passages[k].start] {
			t.Fatalf("passage %d starts with the wrong id", k)
		}
	}
	if s.ids[0] == separatorID || s.ids[len(s.ids)-1] == separatorID {
		t.Fatal("a separator at the start or end of a sequence")
	}
}

func TestAPassNeverExceedsItsRowsOrItsTokens(t *testing.T) {
	lengths := []int{4111, 15, 4111, 300, 2000, 2100, 16, 900, 1, 4111}
	passes := packPasses(lengths, 4, embedding.SegmentTokens)
	seen := map[int]bool{}
	for _, pass := range passes {
		width := 0
		for _, i := range pass {
			width = max(width, lengths[i])
			if seen[i] {
				t.Fatalf("sequence %d in two passes", i)
			}
			seen[i] = true
		}
		if len(pass) > 4 || len(pass)*width > embedding.SegmentTokens {
			t.Fatalf("a pass of %d rows at width %d", len(pass), width)
		}
	}
	if len(seen) != len(lengths) {
		t.Fatalf("%d of %d sequences packed", len(seen), len(lengths))
	}
	// A full segment always runs alone.
	for _, pass := range passes {
		for _, i := range pass {
			if lengths[i] == embedding.SegmentTokens && len(pass) != 1 {
				t.Fatalf("a full segment shares a pass with %d others", len(pass)-1)
			}
		}
	}
}

// torch.round is half to even; math.Round is not, and would move every
// component that lands exactly on a half by one level.
func TestQuantisationRoundsHalfToEven(t *testing.T) {
	cases := map[float64]float32{0.5: 0, 1.5: 2, 2.5: 2, -0.5: 0, -1.5: -2, 126.5: 126, 127: 127, 200: 127, -200: -128}
	for y, want := range cases {
		if got := int8Level(y); got != want {
			t.Errorf("int8Level(%v) = %v, want %v", y, got, want)
		}
	}
	v := quantise([]float64{10, -10, 0})
	if math.Abs(float64(v[0])+float64(v[1])) > 1e-6 || v[2] != 0 || math.Abs(float64(norm2(v))-1) > 1e-6 {
		t.Fatalf("quantise(10, -10, 0) = %v; want a unit vector of +a, -a, 0", v)
	}
}

func norm2(v []float32) float64 {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	return math.Sqrt(s)
}

// Pooling reads only the positions it is given: a separator between two spans,
// and the padding after them, carry real numbers that must never reach a mean.
func TestMeanOverPoolsOnlyTheGivenSpans(t *testing.T) {
	const width, dim = 6, 2
	// Two rows; row 1 is [a a SEP b pad pad], where SEP and pad hold 100s.
	hidden := make([]float32, 2*width*dim)
	row1 := []float32{1, 2, 3, 4, 100, 100, 5, 6, 100, 100, 100, 100}
	copy(hidden[width*dim:], row1)

	got := meanOver(hidden, 1, width, dim, span{0, 2}, span{3, 4})
	if want := []float64{3, 4}; got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("mean over [0,2) and [3,4) = %v, want %v", got, want)
	}
	if got := meanOver(hidden, 1, width, dim, span{3, 4}); got[0] != 5 || got[1] != 6 {
		t.Fatalf("mean over [3,4) = %v, want [5 6]", got)
	}
}
