//go:build cgo && onnx

package onnx_test

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/onnx"
)

func mustOpenEmbedder(t *testing.T) embedding.Embedder {
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
		IntraOpThreads:    2,
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

func cosine(a, b []float32) float64 {
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

func norm(v []float32) float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	return float32(math.Sqrt(sum))
}

// A text embedded alone and the same text embedded beside a much longer one
// must produce the same vector. If they differ, the attention mask is not being
// honoured: the batch pads to its longest sequence, and a vector that changes
// with the shape of the batch it happened to travel in is precisely the failure
// this package exists to avoid. It is also the exact bug that was found and
// fixed while writing this task.
func TestBatchShapeDoesNotChangeAVector(t *testing.T) {
	e := mustOpenEmbedder(t)
	ctx := context.Background()

	const short = "the sky is blue"
	long := strings.Repeat("a much longer neighbouring sentence with many more tokens in it. ", 12)

	alone, err := e.EmbedQueries(ctx, []string{short})
	if err != nil {
		t.Fatal(err)
	}
	together, err := e.EmbedQueries(ctx, []string{short, long})
	if err != nil {
		t.Fatal(err)
	}
	if d := cosine(alone[0], together[0]); d < 0.99999 {
		t.Fatalf("the same text embedded alone and in a padded batch differs: cosine %f.\n"+
			"Pooling is averaging over padding positions.", d)
	}
}

func TestVectorsAreL2Normalised(t *testing.T) {
	e := mustOpenEmbedder(t)
	v, err := e.EmbedQueries(context.Background(), []string{"hello world"})
	if err != nil {
		t.Fatal(err)
	}
	if n := norm(v[0]); math.Abs(float64(n)-1.0) > 1e-5 {
		t.Fatalf("norm is %f, want 1.0 — downstream cosine recovery assumes unit vectors", n)
	}
}

func TestDimensionIs1024(t *testing.T) {
	e := mustOpenEmbedder(t)
	if e.Dim() != 1024 {
		t.Fatalf("Dim() = %d", e.Dim())
	}
	v, err := e.EmbedQueries(context.Background(), []string{"anything"})
	if err != nil {
		t.Fatal(err)
	}
	if len(v[0]) != 1024 {
		t.Fatalf("a vector has %d components", len(v[0]))
	}
}

// Model identity is stamped into every stored vector, so it must be the string
// the rest of the system compares against, not a path or a file name.
func TestTheModelIsIdentified(t *testing.T) {
	if got := mustOpenEmbedder(t).ModelID(); got != embedding.Model {
		t.Fatalf("ModelID() = %q, want %q", got, embedding.Model)
	}
}

func TestTheSameTextAlwaysProducesTheSameVector(t *testing.T) {
	e := mustOpenEmbedder(t)
	ctx := context.Background()
	a, err := e.EmbedQueries(ctx, []string{"determinism matters"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.EmbedQueries(ctx, []string{"determinism matters"})
	if err != nil {
		t.Fatal(err)
	}
	if d := cosine(a[0], b[0]); d < 0.9999999 {
		t.Fatalf("two runs of the same text differ: cosine %f", d)
	}
}

// A text longer than one segment is embedded whole, not truncated or refused:
// every passage gets a vector, and together they cover the content.
func TestALongTextIsEmbeddedWhole(t *testing.T) {
	e := mustOpenEmbedder(t)
	long := strings.Repeat("the quick brown fox jumps over the lazy dog. ", 500) // about 5,000 tokens
	docs, err := e.EmbedDocuments(context.Background(), []string{long})
	if err != nil {
		t.Fatalf("a long text was refused: %v", err)
	}
	ps := docs[0].Passages
	if len(ps) < 17 {
		t.Fatalf("%d passages; a text longer than one segment needs at least 17", len(ps))
	}
	if ps[0].Start != 0 || int(ps[len(ps)-1].End) != len(long) {
		t.Fatalf("passages cover [%d,%d) of %d bytes", ps[0].Start, ps[len(ps)-1].End, len(long))
	}
	for i, p := range ps {
		if n := norm(p.Vector); math.Abs(float64(n)-1.0) > 1e-5 {
			t.Fatalf("passage %d has norm %f", i, n)
		}
	}
}

func TestEmptyAndWhitespaceTextsProduceUsableVectors(t *testing.T) {
	e := mustOpenEmbedder(t)
	got, err := e.EmbedQueries(context.Background(), []string{"", "   ", "\n\t"})
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range got {
		if len(v) != 1024 {
			t.Fatalf("case %d has %d components", i, len(v))
		}
		for _, x := range v {
			if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
				t.Fatalf("case %d is not finite: the pooling divisor was not clamped", i)
			}
		}
	}
}

func TestAnEmptyBatchIsRefused(t *testing.T) {
	if _, err := mustOpenEmbedder(t).EmbedQueries(context.Background(), nil); err == nil {
		t.Fatal("an empty batch was accepted")
	}
}

func TestOpeningAModelThatIsNotThereSaysWhatToRun(t *testing.T) {
	_, err := onnx.Open(onnx.Config{ModelPath: filepath.Join(t.TempDir(), "absent")})
	if err == nil {
		t.Fatal("opening a missing model succeeded")
	}
	if !strings.Contains(err.Error(), "fetch-model.sh") {
		t.Fatalf("the error does not say what to run: %v", err)
	}
}

// Close must be idempotent and must make later use fail rather than crash: the
// server's shutdown path closes it, and a request in flight must get an error,
// not a segfault in C.
func TestCloseIsIdempotentAndLaterUseIsRefused(t *testing.T) {
	e := mustOpenEmbedder(t)
	c := e.(interface{ Close() error })
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := e.EmbedQueries(context.Background(), []string{"after close"}); err == nil {
		t.Fatal("a closed model served a request")
	}
}
