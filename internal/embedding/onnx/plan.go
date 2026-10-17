package onnx

import (
	"math"
	"sort"

	"github.com/remem-org/remem-go/internal/embedding"
)

// This file is the arithmetic of turning a document's token ids into model
// inputs and back into vectors. None of it touches ONNX Runtime or the
// tokenizer, so it builds without the onnx tag and its unit tests run in every
// `go test` — the parts the native libraries are needed for are only the parts
// that need them.
//
// The construction is design decision 2 of openspec/changes/passage-vectors,
// and scripts/pplx_reference.py is its independent implementation. Change one
// and the reference test fails until the other agrees.

// separatorID is "<|endoftext|>", which joins passages inside a segment and is
// also the pad token. A separator and a pad therefore share an id, and only the
// attention mask tells them apart: nothing here ever looks for this id in a
// sequence to find a boundary. Boundaries are positions, recorded when the
// sequence is built.
const separatorID = 151643

// span is a half-open range of positions [start, end).
type span struct{ start, end int }

// segment is a run of consecutive passages embedded in one forward pass.
type segment struct{ first, last int } // passages [first, last)

// plan is how one document becomes forward passes.
type plan struct {
	passages []span // token ranges of the document's ids, in order
	segments []segment
}

// planDocument cuts n token ids into passages of embedding.PassageTokens and
// groups them into segments of at most embedding.SegmentPassages that overlap by
// embedding.SegmentOverlap. A document of no tokens still has one empty passage,
// which the caller replaces with something embeddable.
func planDocument(n int) plan {
	var p plan
	for i := 0; i < n; i += embedding.PassageTokens {
		p.passages = append(p.passages, span{i, min(i+embedding.PassageTokens, n)})
	}
	if len(p.passages) == 0 {
		p.passages = []span{{0, 0}}
	}
	stride := embedding.SegmentPassages - embedding.SegmentOverlap
	count := max(1, ceilDiv(len(p.passages)-embedding.SegmentOverlap, stride))
	for s := range count {
		first := s * stride
		p.segments = append(p.segments, segment{first, min(first+embedding.SegmentPassages, len(p.passages))})
	}
	return p
}

func ceilDiv(a, b int) int {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

// owner is the segment a passage takes its vector from: the one in which it lies
// farthest from an edge, so its context is the widest available, with a tie
// going to the earlier segment. Only a passage in an overlap has a choice.
func (p plan) owner(passage int) int {
	best, bestDist := -1, -1
	for i, s := range p.segments {
		if passage < s.first || passage >= s.last {
			continue
		}
		d := min(passage-s.first, s.last-1-passage)
		if d > bestDist {
			best, bestDist = i, d
		}
	}
	return best
}

// sequence is one forward pass's row: the ids of one segment, joined by
// separators, and where each passage landed in it.
type sequence struct {
	doc     int // which input text
	segment int // which of its segments
	ids     []int64
	spans   []span // spans[i] is passage segment.first+i
}

// buildSequence lays out segment s of a document: p₀ SEP p₁ SEP … SEP pₖ, the
// shape the model was trained on, with no separator before the first passage
// or after the last.
func buildSequence(doc, s int, ids []int64, p plan) sequence {
	seg := p.segments[s]
	out := sequence{doc: doc, segment: s}
	for i := seg.first; i < seg.last; i++ {
		if i > seg.first {
			out.ids = append(out.ids, separatorID)
		}
		start := len(out.ids)
		out.ids = append(out.ids, ids[p.passages[i].start:p.passages[i].end]...)
		out.spans = append(out.spans, span{start, len(out.ids)})
	}
	return out
}

// packPasses groups sequences into forward passes, each holding at most
// maxRows rows and at most maxTokens padded positions (rows × the longest).
//
// The token bound is what keeps a full segment alone. Measured on the
// development host, a pass of several 4K-token segments is no faster per token
// than one at a time and costs about 4 GiB of activations for each, so the
// bound is one full segment's width (embedding.SegmentTokens). Short sequences
// — queries, one-passage memories — are where batching still pays, and they
// pack many to a pass.
//
// Sequences are sorted longest first so that each pass pads to a width close to
// what its rows need. The order is a pure function of the lengths and the
// input positions, so the same inputs always make the same passes.
func packPasses(lengths []int, maxRows, maxTokens int) [][]int {
	order := make([]int, len(lengths))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return lengths[order[a]] > lengths[order[b]] })

	var passes [][]int
	var cur []int
	width := 0
	for _, i := range order {
		if len(cur) > 0 && (len(cur)+1 > maxRows || (len(cur)+1)*width > maxTokens) {
			passes = append(passes, cur)
			cur, width = nil, 0
		}
		if len(cur) == 0 {
			width = lengths[i] // the longest, since the order is descending
		}
		cur = append(cur, i)
	}
	if len(cur) > 0 {
		passes = append(passes, cur)
	}
	return passes
}

// meanOver averages the token states of row positions [s.start, s.end) of a
// pass's hidden state, in float64 so that a sum over four thousand tokens does
// not lose the low bits the int8 rule then rounds.
func meanOver(hidden []float32, row, width, dim int, spans ...span) []float64 {
	v := make([]float64, dim)
	n := 0
	for _, s := range spans {
		for pos := s.start; pos < s.end; pos++ {
			off := (row*width + pos) * dim
			for d, x := range hidden[off : off+dim] {
				v[d] += float64(x)
			}
		}
		n += s.end - s.start
	}
	if n > 0 {
		for d := range v {
			v[d] /= float64(n)
		}
	}
	return v
}

// quantise applies the model's output rule and normalises.
//
// round(tanh(x)·127), clamped to [-128, 127], is what the model was trained
// with (st_quantize.py), and cosine over those values is what training made
// meaningful; the raw pooled floats are not. The rounding is half to even,
// because that is what torch.round does — math.Round would move a value that
// lands exactly on .5, and the reference test would catch it as a drift of one
// in some component of some vector.
//
// The values are kept as float32 and normalised, so every downstream consumer
// of a unit vector is unchanged. Storing them as int8 is a separate decision.
func quantise(v []float64) []float32 {
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = int8Level(math.Tanh(x) * 127)
	}
	return embedding.Normalise(out)
}

// int8Level rounds a scaled value to its int8 level, half to even, and clamps.
func int8Level(y float64) float32 {
	return float32(max(-128, min(127, math.RoundToEven(y))))
}
