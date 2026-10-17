package embedding

import (
	"context"
	"fmt"
	"math"

	"github.com/remem-org/remem-go/internal/errs"
)

// External is the embedder of a server whose vectors come from the caller.
//
// It computes nothing. It exists so that everything downstream that stamps or
// checks a model's identity — the canonical vector's model id, the index's
// refusal of a corpus written by another model, a snapshot's vector rule —
// keeps working from one declaration, rather than each growing a second path
// for "there is no model". Embed refuses rather than returning zeros, for the
// reason "There is no fallback embedder" gives: a vector that means nothing
// must never be stored under a model's name.
type External struct {
	modelID string
	dim     int
}

// NewExternal declares the model the caller's vectors come from and their
// width. Configuration has validated both.
func NewExternal(modelID string, dim int) *External {
	return &External{modelID: modelID, dim: dim}
}

// EmbedQueries refuses: this server takes every query vector from its caller.
func (e *External) EmbedQueries(context.Context, []string) ([][]float32, error) {
	return nil, errs.E(errs.Invalid, "embedding.External.EmbedQueries", fmt.Errorf(
		"embedding.provider is external: this server computes no vectors, so every semantic or hybrid search must carry `query_vector`"))
}

// EmbedDocuments refuses: this server takes every memory's vector from its
// caller, one per memory, and so has no passages to give either.
func (e *External) EmbedDocuments(context.Context, []string) ([]Document, error) {
	return nil, errs.E(errs.Invalid, "embedding.External.EmbedDocuments", fmt.Errorf(
		"embedding.provider is external: this server computes no vectors, so every write that sets content must carry `embedding`"))
}

func (e *External) Dim() int        { return e.dim }
func (e *External) ModelID() string { return e.modelID }

// callerSupplied is implemented by an embedder whose vectors come from the
// caller, and by a wrapper that forwards the question to what it wraps.
type callerSupplied interface{ callerSupplied() bool }

func (e *External) callerSupplied() bool { return true }

// CallerSupplied reports whether e declares that vectors come from the caller.
// It is the one test of the mode, so the service that must refuse a supplied
// vector and the one that must require it cannot disagree about which server
// they are in.
func CallerSupplied(e Embedder) bool {
	c, ok := e.(callerSupplied)
	return ok && c.callerSupplied()
}

// CheckSupplied validates a caller's vector and returns a copy of it.
//
// It is refused rather than repaired. The width must be the server's, because a
// vector of another width is another model's; every component must be finite,
// because one NaN poisons every distance it enters; and where the metric
// divides by the norm, a zero vector has no direction to compare. It is never
// rescaled: under cosine the distance normalises for itself, and under the
// other metrics the magnitude is part of what the caller meant.
//
// field names the request field in the refusal — `embedding` on a write,
// `query_vector` on a search — so the caller is told which one to fix.
func CheckSupplied(field string, v []float32, dim int, nonZero bool) ([]float32, error) {
	const op = "embedding.CheckSupplied"
	if len(v) != dim {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"%s has %d components; this server's vectors have %d", field, len(v), dim))
	}
	var sum float64
	for i, x := range v {
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, errs.E(errs.Invalid, op, fmt.Errorf(
				"%s component %d is %v; every component must be finite", field, i, x))
		}
		sum += f * f
	}
	if nonZero && sum == 0 {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"%s is the zero vector, which has no direction for a cosine to compare", field))
	}
	return append([]float32(nil), v...), nil
}
