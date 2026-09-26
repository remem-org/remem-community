// Package embedding turns text into vectors, and is the most dangerous package
// in Remem to get subtly wrong.
//
// A mistake here does not crash and does not look wrong. Mean-pool without the
// attention mask, or normalise before pooling instead of after, and the output
// is still 384 unit-length floats that a dimension check and a norm check both
// accept — but it is a different vector space from the one the stored corpus
// lives in, and the only symptom is search results that are slightly worse than
// they should be, forever. That is why the test that matters here compares
// against 200 vectors the Rust implementation produced (Task 0.6) rather than
// asserting shape.
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
type Embedder interface {
	// Embed returns one vector per text, in order.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	// Dim is the width of every vector this embedder produces.
	Dim() int
	// ModelID names the model, and is stamped into every stored vector.
	ModelID() string
}

// Model is the only model whose vectors are comparable with the stored corpus.
// It is a fixed constant of the system (plan §Global Constraints), not a knob.
const Model = "all-MiniLM-L6-v2"

// Dim is that model's output width.
const Dim = 384

// MaxSequenceTokens is the model's input limit. Longer text is truncated
// rather than rejected: a memory that is too long to embed exactly is still
// worth storing and still worth finding.
const MaxSequenceTokens = 256

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
	}
	return nil
}
