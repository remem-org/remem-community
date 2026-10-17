//go:build cgo && onnx

package onnx_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/onnx"
)

func openPassageModel(t *testing.T) embedding.Embedder {
	t.Helper()
	path := os.Getenv("REMEM_EMBEDDING_MODEL_PATH")
	if path == "" {
		path = filepath.Join("..", "..", "..", ".models", "pplx-embed-context-v1-0.6b")
	}
	if _, err := os.Stat(filepath.Join(path, "onnx", "model.onnx")); err != nil {
		t.Skipf("no model at %s; run scripts/fetch-model.sh", path)
	}
	e, err := onnx.Open(onnx.Config{
		ModelPath:         path,
		SharedLibraryPath: os.Getenv("REMEM_ONNX_LIBRARY_PATH"),
		BatchSize:         8,
	})
	if err != nil {
		t.Fatalf("opening the model: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := e.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	return e
}

// referenceCase is one document of fixtures/pplx_embedding_reference.json,
// which scripts/pplx_reference.py wrote from the model card's Python pipeline.
type referenceCase struct {
	Name     string `json:"name"`
	Text     string `json:"text"`
	Document []int  `json:"document"`
	Passages []struct {
		Index int   `json:"index"`
		Start int   `json:"start"`
		End   int   `json:"end"`
		Int8  []int `json:"int8"`
	} `json:"passages"`
}

func loadReference(t *testing.T) map[string]referenceCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "fixtures", "pplx_embedding_reference.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Model string          `json:"model"`
		Cases []referenceCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.Model != embedding.Model {
		t.Fatalf("the fixture is of %q and this build embeds with %q", f.Model, embedding.Model)
	}
	out := make(map[string]referenceCase, len(f.Cases))
	for _, c := range f.Cases {
		out[c.Name] = c
	}
	return out
}

func cosineInt8(v []float32, ref []int) float64 {
	r := make([]float32, len(ref))
	for i, x := range ref {
		r[i] = float32(x)
	}
	return cosine(v, r)
}

// The gate this package exists to pass. The reference was computed by a
// different program in a different language from the model card's published
// pipeline, so agreeing with it at every passage holds the tokenizer, the
// separator construction, segment overlap and ownership, span pooling, the
// int8 rule's rounding and the normalisation all at once. A mistake in any of
// them still produces 1,024 unit-length floats.
func TestDocumentsMatchThePythonReference(t *testing.T) {
	m := openPassageModel(t)
	ref := loadReference(t)

	names := []string{"short", "forty-passages", "non-ascii"}
	texts := make([]string, len(names))
	for i, n := range names {
		texts[i] = ref[n].Text
	}
	docs, err := m.EmbedDocuments(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != len(names) {
		t.Fatalf("%d documents for %d texts", len(docs), len(names))
	}
	for i, n := range names {
		c, d := ref[n], docs[i]
		if got := cosineInt8(d.Vector, c.Document); got < 0.9999 {
			t.Errorf("%s: document vector at cosine %.6f against the reference", n, got)
		}
		if len(c.Passages) == 1 {
			if len(d.Passages) != 0 {
				t.Errorf("%s: a one-passage document carried %d passages; its document vector is its passage", n, len(d.Passages))
			}
			continue
		}
		if len(d.Passages) != len(c.Passages) {
			t.Fatalf("%s: %d passages, the reference has %d", n, len(d.Passages), len(c.Passages))
		}
		for j, p := range c.Passages {
			g := d.Passages[j]
			if int(g.Start) != p.Start || int(g.End) != p.End {
				t.Errorf("%s passage %d: span [%d,%d), the reference has [%d,%d)", n, j, g.Start, g.End, p.Start, p.End)
			}
			if got := cosineInt8(g.Vector, p.Int8); got < 0.9999 {
				t.Errorf("%s passage %d: cosine %.6f against the reference", n, j, got)
			}
		}
	}

	// A query is a one-passage document, so the short case's query vector is
	// its document vector.
	qs, err := m.EmbedQueries(context.Background(), []string{ref["short"].Text})
	if err != nil {
		t.Fatal(err)
	}
	if got := cosineInt8(qs[0], ref["short"].Document); got < 0.9999 {
		t.Errorf("query vector at cosine %.6f against the reference document vector", got)
	}
}

func sameDocument(t *testing.T, what string, a, b embedding.Document) {
	t.Helper()
	if got := cosine(a.Vector, b.Vector); got < 0.9999 {
		t.Errorf("%s: document vectors at cosine %.6f", what, got)
	}
	if len(a.Passages) != len(b.Passages) {
		t.Fatalf("%s: %d passages against %d", what, len(a.Passages), len(b.Passages))
	}
	for i := range a.Passages {
		pa, pb := a.Passages[i], b.Passages[i]
		if pa.Start != pb.Start || pa.End != pb.End {
			t.Errorf("%s passage %d: span [%d,%d) against [%d,%d)", what, i, pa.Start, pa.End, pb.Start, pb.End)
		}
		if got := cosine(pa.Vector, pb.Vector); got < 0.9999 {
			t.Errorf("%s passage %d: cosine %.6f", what, i, got)
		}
	}
}

// A short document packed into one pass beside a longer one is padded to the
// longer one's width. The padding carries real numbers — the model computes
// something for every position — so a vector that moved here would mean the
// mask is not doing its job.
func TestPaddingBesideALongerTextDoesNotMoveAVector(t *testing.T) {
	m := openPassageModel(t)
	ref := loadReference(t)
	short, long := ref["short"].Text, ref["non-ascii"].Text

	alone, err := m.EmbedDocuments(context.Background(), []string{short})
	if err != nil {
		t.Fatal(err)
	}
	packed, err := m.EmbedDocuments(context.Background(), []string{short, long})
	if err != nil {
		t.Fatal(err)
	}
	sameDocument(t, "short, padded", alone[0], packed[0])
}

// The same content always produces the same passages: spans exactly, and
// vectors to within what a different batch's arithmetic can move them.
func TestTheSameContentAlwaysGivesTheSameSpans(t *testing.T) {
	m := openPassageModel(t)
	ref := loadReference(t)
	text := ref["non-ascii"].Text

	first, err := m.EmbedDocuments(context.Background(), []string{text})
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.EmbedDocuments(context.Background(), []string{ref["short"].Text, text, "another, unrelated note"})
	if err != nil {
		t.Fatal(err)
	}
	sameDocument(t, "non-ascii, twice", first[0], second[1])
}

// The separator and the pad are the same token, id 151643, so only the
// attention mask tells them apart. A pipeline that finds passage boundaries by
// looking for the separator's id — which is how the model card's own pipeline
// works — would read every pad position of a short row as a boundary. Content
// that itself spells out "<|endoftext|>" puts that id inside a passage, where
// it is content and not a boundary either.
func TestAPadIsNotASeparator(t *testing.T) {
	m := openPassageModel(t)
	ref := loadReference(t)
	base := ref["non-ascii"].Text
	text := base[:200] + " <|endoftext|> " + base[200:]
	longer := strings.Repeat(base, 2)

	alone, err := m.EmbedDocuments(context.Background(), []string{text})
	if err != nil {
		t.Fatal(err)
	}
	packed, err := m.EmbedDocuments(context.Background(), []string{text, longer})
	if err != nil {
		t.Fatal(err)
	}
	sameDocument(t, "literal separator, padded", alone[0], packed[0])

	ps := alone[0].Passages
	if len(ps) < 2 {
		t.Fatalf("%d passages; the text is long enough for at least two", len(ps))
	}
	if ps[0].Start != 0 || int(ps[len(ps)-1].End) != len(text) {
		t.Errorf("passages span [%d,%d) of a %d-byte text", ps[0].Start, ps[len(ps)-1].End, len(text))
	}
	for i := 1; i < len(ps); i++ {
		if ps[i].Start > ps[i-1].End {
			t.Errorf("a gap between passage %d (ends %d) and %d (starts %d)", i-1, ps[i-1].End, i, ps[i].Start)
		}
	}
	for i, p := range ps {
		if math.Abs(float64(norm(p.Vector))-1) > 1e-4 {
			t.Errorf("passage %d has norm %v", i, norm(p.Vector))
		}
	}
}

// Documents of very different lengths embedded in one call are packed into
// passes by length — a full segment alone, shorter sequences together, padded
// to their pass's longest — and every document must come out as it does when
// embedded by itself. Compared by cosine, not equality: a different packing is
// different arithmetic, which is noise, and padding leaking in is not.
func TestAMixedBatchEqualsEachDocumentEmbeddedAlone(t *testing.T) {
	m := openPassageModel(t)
	ref := loadReference(t)
	texts := []string{
		ref["short"].Text,
		ref["non-ascii"].Text,
		strings.Repeat("the quick brown fox jumps over the lazy dog. ", 500), // two segments
		"another short note about the harbour",
	}
	together, err := m.EmbedDocuments(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	for i, text := range texts {
		alone, err := m.EmbedDocuments(context.Background(), []string{text})
		if err != nil {
			t.Fatal(err)
		}
		sameDocument(t, fmt.Sprintf("document %d", i), alone[0], together[i])
	}
}
