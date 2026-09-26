// Package embeddingtest provides a deterministic embedder for the tests of
// every package above internal/embedding.
//
// It exists because those tests are about storage, ranking and isolation, not
// about the model: loading 90 MB of weights to assert that a search is
// tenant-scoped would make the suite slow, and would make it fail on a machine
// that has not run scripts/fetch-model.sh.
//
// It is a *deterministic* fake rather than a random one, and that distinction
// is what makes it useful: the same text always produces the same vector, and
// texts that share words produce vectors that are close, so a test can assert
// that "cats are mammals" ranks above "quantum chromodynamics" for the query
// "feline biology" and mean it.
package embeddingtest

import (
	"context"
	"hash/fnv"
	"strings"
	"sync"

	"github.com/remem-org/remem-go/internal/embedding"
)

// Fake is a deterministic bag-of-words embedder.
//
// It is safe for concurrent use, because the batching service always calls an
// embedder from a goroutine it owns: a fake with plain int counters produces a
// data race in every test that exercises batching, which is most of them.
type Fake struct {
	dim     int
	modelID string

	mu    sync.Mutex
	calls int
	texts int
	err   error
}

var _ embedding.Embedder = (*Fake)(nil)

// New returns a fake embedder at the real model's width and identity, so that
// a record written under it round-trips through the same model-identity checks
// production data does.
func New() *Fake {
	return &Fake{dim: embedding.Dim, modelID: embedding.Model}
}

// WithModel returns a fresh fake claiming to be a different model, for tests
// that need a vector the running model must refuse. It does not carry over the
// call counters, and cannot: a Fake owns a mutex, so copying one is a vet
// error and a latent race.
func (f *Fake) WithModel(id string) *Fake {
	return &Fake{dim: f.dim, modelID: id}
}

func (f *Fake) Dim() int        { return f.dim }
func (f *Fake) ModelID() string { return f.modelID }

// Calls reports how many times Embed was invoked. Batching assertions read it:
// a CreateBatch of twenty memories that embedded twenty times has lost the only
// thing batching was for.
func (f *Fake) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// Texts reports how many texts were seen across every call.
func (f *Fake) Texts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.texts
}

// Fail makes every later call return err. Nil restores normal behaviour.
func (f *Fake) Fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *Fake) Embed(_ context.Context, texts []string) ([][]float32, error) {
	f.mu.Lock()
	f.calls++
	f.texts += len(texts)
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}

	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i] = f.vector(text)
	}
	return out, nil
}

// vector hashes each word to a small set of dimensions and accumulates. Two
// texts sharing a word therefore share signal, which is the property that lets
// a ranking test be about ranking.
func (f *Fake) vector(text string) []float32 {
	v := make([]float32, f.dim)
	for _, word := range strings.Fields(strings.ToLower(text)) {
		word = strings.Trim(word, ".,;:!?\"'()")
		if word == "" {
			continue
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(word))
		sum := h.Sum64()
		// Three dimensions per word: enough that distinct words rarely
		// collide entirely, few enough that shared words dominate.
		for k := range 3 {
			idx := int((sum >> (k * 17)) % uint64(f.dim))
			v[idx] += 1
		}
	}
	if len(strings.Fields(text)) == 0 {
		// An empty or whitespace-only text still gets a stable vector rather
		// than a zero one, so it does not compare equal to every query.
		v[0] = 1
	}
	return embedding.Normalise(v)
}
