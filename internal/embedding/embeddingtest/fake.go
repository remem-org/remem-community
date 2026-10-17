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
	"unicode"

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

// Calls reports how many times either operation was invoked. Batching assertions read it:
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

// PassageWords is how many words make one of the fake's passages. The model's
// passages are 256 tokens; the fake's are short, so that a test can write a
// multi-passage memory in a line or two and know which passage a word is in:
// word w is in passage w / PassageWords.
const PassageWords = 32

func (f *Fake) begin(n int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.texts += n
	return f.err
}

// EmbedQueries returns each text's bag-of-words vector.
func (f *Fake) EmbedQueries(_ context.Context, texts []string) ([][]float32, error) {
	if err := f.begin(len(texts)); err != nil {
		return nil, err
	}
	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i] = f.vector(text)
	}
	return out, nil
}

// EmbedDocuments splits each text into passages of PassageWords words, the way
// the model splits one into passages of 256 tokens: deterministically, with
// byte spans, none for a text that fits one passage. Each passage's vector is
// the bag of its own words, so a search for a word ranks the memory by the
// passage that holds it. The document vector is the bag of the first segment's
// words, as the model's is the mean over its first segment.
func (f *Fake) EmbedDocuments(_ context.Context, texts []string) ([]embedding.Document, error) {
	if err := f.begin(len(texts)); err != nil {
		return nil, err
	}
	out := make([]embedding.Document, len(texts))
	for i, text := range texts {
		words := wordSpans(text)
		if len(words) <= PassageWords {
			out[i] = embedding.Document{Vector: f.vector(text)}
			continue
		}
		var d embedding.Document
		for w := 0; w < len(words); w += PassageWords {
			last := min(w+PassageWords, len(words)) - 1
			start, end := words[w][0], words[last][1]
			d.Passages = append(d.Passages, embedding.Passage{
				Start: uint32(start), End: uint32(end), Vector: f.vector(text[start:end]),
			})
		}
		segmentEnd := words[min(len(words), PassageWords*embedding.SegmentPassages)-1][1]
		d.Vector = f.vector(text[:segmentEnd])
		out[i] = d
	}
	return out, nil
}

// wordSpans is the byte range of every whitespace-separated word.
func wordSpans(text string) [][2]int {
	var out [][2]int
	start := -1
	for i, r := range text {
		if unicode.IsSpace(r) {
			if start >= 0 {
				out = append(out, [2]int{start, i})
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		out = append(out, [2]int{start, len(text)})
	}
	return out
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
