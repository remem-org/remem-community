// Package embedding turns text into vectors, and is the most dangerous package
// in Remem to get subtly wrong.
//
// A mistake here does not crash and does not look wrong. Mean-pool without the
// attention mask, or normalise before pooling instead of after, and the output
// is still 1,024 unit-length floats that a dimension check and a norm check both
// accept — but it is a different vector space from the one the stored corpus
// lives in, and the only symptom is search results that are slightly worse than
// they should be, forever. That is why the test that matters here compares
// against vectors an independent Python implementation of the model card's
// pipeline produced (fixtures/pplx_embedding_reference.json), passage by
// passage, rather than asserting shape.
//
// # Model identity travels with every vector
//
// [Embedder.ModelID] is stamped into every stored vector (plan §II.10 row 10).
// Nothing in Rust Remem recorded which model produced a vector, so changing the
// model silently corrupted the space. Here a vector whose model id is not the
// running model's is refused, not scored.
package embedding

import (
	"context"
	"errors"
	"math"

	"github.com/remem-org/remem-go/internal/errs"
)

// Embedder turns text into vectors.
//
// Implementations must return unit-norm vectors, in the same order as their
// input, and exactly [Embedder.Dim] components each. Everything downstream —
// cosine recovery from L2 distance above all — assumes unit vectors.
//
// There are two operations because a memory and a query are embedded
// differently. A memory's content is embedded whole, as passages, each computed
// with the rest of the document in view; a query is one short text whose single
// vector is compared against those passages. A query is a one-passage document,
// so for any text that fits one passage the two produce the same vector.
type Embedder interface {
	// EmbedQueries returns one vector per text, in order.
	EmbedQueries(ctx context.Context, texts []string) ([][]float32, error)
	// EmbedDocuments embeds each text as a memory's content, in order.
	EmbedDocuments(ctx context.Context, texts []string) ([]Document, error)
	// Dim is the width of every vector this embedder produces.
	Dim() int
	// ModelID names the model, and is stamped into every stored vector.
	ModelID() string
}

// WholeDocuments wraps one vector per text as one-passage documents.
//
// It is for an embedder that has no passages to give — a caller's supplied
// vector, a fixed benchmark fixture — so that it can say so in one line rather
// than inventing a passage structure it does not have.
func WholeDocuments(vectors [][]float32) []Document {
	out := make([]Document, len(vectors))
	for i, v := range vectors {
		out[i] = Document{Vector: v}
	}
	return out
}

// Document is one text embedded as a memory's content.
//
// Vector is the canonical document vector: the mean over every content token of
// the first segment, quantised and normalised by the model's rule. Passages is
// empty when the content fits one passage, because that passage's vector *is*
// the document vector, and storing it twice would give a one-passage memory two
// index entries that always agree.
type Document struct {
	Vector   []float32
	Passages []Passage
}

// Passage is one passage of a multi-passage document: its byte range in the
// content, and its vector, computed with the rest of its segment in view.
//
// Start and End are UTF-8 byte offsets, [Start, End), and fall on character
// boundaries. Two adjacent passages can share bytes: a byte-level tokenizer may
// split one character across two tokens, and when the passage boundary falls
// between them each passage's span covers the whole character.
type Passage struct {
	Start, End uint32
	Vector     []float32
}

// Model is the only model whose vectors are comparable with the stored corpus.
// It is a fixed constant of the system (plan §Global Constraints), not a knob.
// The model runs from its fp32 ONNX export; the int8 in its vectors is the
// output rule it was trained with, not the precision of its weights.
const Model = "pplx-embed-context-v1-0.6b"

// Dim is that model's output width.
const Dim = 1024

// How content becomes vectors. These are not settings: they decide what is
// written to disk, and they are part of the passage_index format, so changing
// one is a format change and a re-embedding of every memory.
const (
	// PassageTokens is a passage's length in tokens; the last passage of a
	// document may be shorter. 256 is the chunk length the model's contextual
	// training used.
	PassageTokens = 256
	// SegmentPassages is how many passages one forward pass embeds together,
	// each knowing the others: 16, which is what contextual training saw.
	SegmentPassages = 16
	// SegmentOverlap is how many passages consecutive segments share, so that
	// a passage near a segment's edge can take its vector from the segment in
	// which it has context on both sides.
	SegmentOverlap = 2
	// SegmentTokens is a full segment's length: sixteen passages and the
	// fifteen separators between them.
	SegmentTokens = SegmentPassages*PassageTokens + SegmentPassages - 1
)

// Normalise scales v to unit length, in place, and returns it.
//
// A zero vector is left alone rather than divided by zero. It can only come
// from an input that tokenised to nothing but padding, and NaNs propagating
// into the index would be far worse than one vector that matches nothing.
func Normalise(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return v
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return v
}

// ErrNoTexts is returned for an empty request, so a caller that built an empty
// batch by accident hears about it rather than receiving an empty result it
// then indexes into.
var ErrNoTexts = errors.New("embed was called with no texts")

// checkOutput holds implementations to the contract above. It is cheap, and it
// runs on the way out of every embedder, because the failure it catches is one
// nothing downstream would notice.
func checkOutput(op string, texts []string, out [][]float32, dim int) error {
	if len(out) != len(texts) {
		return errs.E(errs.Storage, op, errors.New("the embedder returned a different number of vectors than it was given texts"))
	}
	for _, v := range out {
		if err := checkVector(op, v, dim); err != nil {
			return err
		}
	}
	return nil
}

// checkDocuments is checkOutput for documents. Beyond each vector, it holds the
// passage contract: none for a one-passage document, otherwise at least two,
// each a range of the text in order.
func checkDocuments(op string, texts []string, out []Document, dim int) error {
	if len(out) != len(texts) {
		return errs.E(errs.Storage, op, errors.New("the embedder returned a different number of documents than it was given texts"))
	}
	for i, d := range out {
		if err := checkVector(op, d.Vector, dim); err != nil {
			return err
		}
		if len(d.Passages) == 1 {
			return errs.E(errs.Storage, op, errors.New(
				"the embedder returned a single passage; a one-passage document is its document vector"))
		}
		var prev uint32
		for _, p := range d.Passages {
			if p.Start > p.End || p.Start < prev || int(p.End) > len(texts[i]) {
				return errs.E(errs.Storage, op, errors.New("the embedder returned a passage span outside its text or out of order"))
			}
			prev = p.Start
			if err := checkVector(op, p.Vector, dim); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkVector(op string, v []float32, dim int) error {
	if len(v) != dim {
		return errs.E(errs.Storage, op, errors.New("the embedder returned a vector of the wrong width"))
	}
	var sum float64
	for _, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return errs.E(errs.Storage, op, errors.New("the embedder returned a vector containing NaN or infinity"))
		}
		sum += float64(x) * float64(x)
	}
	// A zero vector is legal (see Normalise); anything else must be unit.
	if sum != 0 && math.Abs(math.Sqrt(sum)-1) > 1e-4 {
		return errs.E(errs.Storage, op, errors.New("the embedder returned a vector that is not unit-norm"))
	}
	return nil
}
