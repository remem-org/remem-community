//go:build cgo && onnx

// This file is in package onnx, not onnx_test, because it pools the model's
// raw output two different ways. That is the whole point of it: see
// TestTheModelAndTokenizerReproduceTheRustReferenceUnderCLSPooling.

package onnx

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// referenceFile is the frozen Rust output (Task 0.6). It lives at the
// repository root rather than under testdata/ because it is 1.7 MB and is
// shared with the differential harness; a second copy would be a second thing
// that can drift.
const referenceFile = "../../../fixtures/embedding_reference.json"

type referenceFileT struct {
	ModelID string `json:"model_id"`
	Dim     int    `json:"dim"`
	Cases   []struct {
		Text   string    `json:"text"`
		Vector []float32 `json:"vector"`
	} `json:"cases"`
}

func loadRef(t *testing.T) referenceFileT {
	t.Helper()
	b, err := os.ReadFile(referenceFile)
	if err != nil {
		t.Fatalf("reading the reference vectors: %v", err)
	}
	var ref referenceFileT
	if err := json.Unmarshal(b, &ref); err != nil {
		t.Fatalf("parsing %s: %v", referenceFile, err)
	}
	if len(ref.Cases) != 200 {
		t.Fatalf("the reference holds %d cases, the contract is 200", len(ref.Cases))
	}
	return ref
}

func openModel(t *testing.T) *Model {
	t.Helper()
	path := os.Getenv("REMEM_EMBEDDING_MODEL_PATH")
	if path == "" {
		path = filepath.Join("..", "..", "..", ".models", "all-MiniLM-L6-v2")
	}
	if _, err := os.Stat(filepath.Join(path, "model.onnx")); err != nil {
		t.Skipf("no model at %s; run scripts/fetch-model.sh", path)
	}
	e, err := Open(Config{
		ModelPath:         path,
		SharedLibraryPath: os.Getenv("REMEM_ONNX_LIBRARY_PATH"),
		IntraOpThreads:    2,
	})
	if err != nil {
		t.Fatalf("opening the model: %v", err)
	}
	m := e.(*Model)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// clsPool takes each sequence's first token and normalises. It is what
// fastembed 3.14.1 does — `output_data.slice(s![.., 0, ..])` in
// src/text_embedding.rs — and therefore what produced every vector in the Rust
// corpus. It lives in a test file because Remem does not pool this way.
func clsPool(o forwardOutput) [][]float32 {
	out := make([][]float32, o.batch)
	for b := range o.batch {
		v := append([]float32(nil), o.hidden[b*o.seq*o.dim:b*o.seq*o.dim+o.dim]...)
		out[b] = normaliseCopy(v)
	}
	return out
}

func normaliseCopy(v []float32) []float32 {
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

func cos(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// TestTheModelAndTokenizerReproduceTheRustReferenceUnderCLSPooling is the test
// Task 3.3 exists for, adjusted for a fact the plan got wrong.
//
// The plan says Rust mean-pools with the attention mask, and asks Go to agree
// with the reference vectors to cosine >= 0.9999. Rust does not mean-pool: it
// uses fastembed 3.14.1, which takes token 0 of last_hidden_state. Remem
// deliberately mean-pools instead, which is what all-MiniLM-L6-v2 was trained
// for, so the two implementations cannot agree on the stored vector and the
// original assertion can never pass. That divergence is recorded in the plan's
// Part II.10.
//
// What the fixture can still prove is nearly all of it, and exactly the part
// that is hard to get right: run the same 200 strings through this binary's
// model and tokenizer, pool the output the way Rust pooled it, and require
// agreement. That holds the model file, every tokenizer decision — WordPiece
// vocabulary, lowercasing, accent stripping, special tokens — the tensor
// layout, and the attention mask, all to the byte. The only thing it does not
// hold is the pooling choice, which is the one thing that was decided on
// purpose.
//
// 0.9999 rather than exact equality: ONNX Runtime may select different kernels
// across builds, and float32 accumulation order differs. A cosine of 0.9999
// between two 384-dimensional unit vectors is far tighter than any ranking
// difference could survive.
func TestTheModelAndTokenizerReproduceTheRustReferenceUnderCLSPooling(t *testing.T) {
	ref := loadRef(t)
	m := openModel(t)

	if m.ModelID() != ref.ModelID {
		t.Fatalf("this embedder is %q, the reference is %q", m.ModelID(), ref.ModelID)
	}
	if m.Dim() != ref.Dim {
		t.Fatalf("this embedder produces %d dimensions, the reference has %d", m.Dim(), ref.Dim)
	}

	texts := make([]string, len(ref.Cases))
	for i, c := range ref.Cases {
		texts[i] = c.Text
	}
	out, err := m.forward(texts)
	if err != nil {
		t.Fatal(err)
	}
	got := clsPool(out)

	worst, worstText := 1.0, ""
	for i, c := range ref.Cases {
		d := cos(got[i], c.Vector)
		if d < worst {
			worst, worstText = d, c.Text
		}
		if d < 0.9999 {
			t.Errorf("text %q: cosine to the Rust vector is %f, want >= 0.9999.\n"+
				"The model, the tokenizer or the attention mask does not match Rust's.", trunc(c.Text), d)
		}
	}
	t.Logf("%d reference strings; worst cosine %.7f on %q", len(ref.Cases), worst, trunc(worstText))
}

// Mean pooling is chosen for retrieval quality, so that claim is asserted
// rather than believed. Under CLS pooling two unrelated memories already sit at
// roughly 0.63 cosine, which makes search.similarity_threshold unusable — the
// same defect class the behaviour baseline records at REM-74, where 1/(1+d)
// floored at one third. Mean pooling puts unrelated content near zero.
//
// The thresholds here are loose enough not to be a tuning test and tight
// enough to fail if pooling silently reverted.
func TestMeanPoolingSeparatesUnrelatedContent(t *testing.T) {
	ref := loadRef(t)
	m := openModel(t)

	texts := make([]string, len(ref.Cases))
	index := make(map[string]int, len(ref.Cases))
	for i, c := range ref.Cases {
		texts[i] = c.Text
		index[c.Text] = i
	}
	out, err := m.forward(texts)
	if err != nil {
		t.Fatal(err)
	}
	got := meanPool(out)

	// Near-duplicates that exist in the reference corpus, against the average
	// over sixty unrelated sentences. The assertion is on the gap between the
	// two rather than on either number: an absolute threshold on a pair like
	// "test sentence"/"text sentence" is a tuning test, where the gap is the
	// property that decides whether a relevance threshold can work at all.
	near := [][2]string{
		{"The temperature is 20 degrees.", "The temperature is 21 degrees."},
		{"colour", "color"},
		{"This is a test sentence.", "This is a text sentence."},
	}
	var nearSum float64
	for _, p := range near {
		i, ok1 := index[p[0]]
		j, ok2 := index[p[1]]
		if !ok1 || !ok2 {
			t.Fatalf("the reference no longer contains %q and %q; this test's pairs must be updated", p[0], p[1])
		}
		nearSum += cos(got[i], got[j])
	}
	nearAvg := nearSum / float64(len(near))

	// The first sixty cases are distinct sentences on unrelated subjects.
	var sum float64
	n := 0
	for i := range 60 {
		for j := i + 1; j < 60; j++ {
			sum += cos(got[i], got[j])
			n++
		}
	}
	unrelated := sum / float64(n)

	t.Logf("near-duplicate average %.3f, unrelated average %.3f, separation %.3f",
		nearAvg, unrelated, nearAvg-unrelated)

	if unrelated > 0.35 {
		t.Errorf("unrelated memories average %.3f cosine; under mean pooling this is near 0.1.\n"+
			"A value near 0.6 means the vectors are CLS-pooled, or the attention mask is not being honoured.", unrelated)
	}
	if nearAvg-unrelated < 0.5 {
		t.Errorf("near-duplicates separate from unrelated content by only %.3f.\n"+
			"Mean pooling gives roughly 0.8; CLS pooling gives roughly 0.3, which is what makes "+
			"search.similarity_threshold unusable.", nearAvg-unrelated)
	}
}

func trunc(s string) string {
	if len(s) <= 60 {
		return s
	}
	return s[:57] + "..."
}
