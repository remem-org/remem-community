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
		path = filepath.Join("..", "..", "..", ".models", "all-MiniLM-L6-v2")
	}
	if _, err := os.Stat(filepath.Join(path, "model.onnx")); err != nil {
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

	alone, err := e.Embed(ctx, []string{short})
	if err != nil {
		t.Fatal(err)
	}
	together, err := e.Embed(ctx, []string{short, long})
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
	v, err := e.Embed(context.Background(), []string{"hello world"})
	if err != nil {
		t.Fatal(err)
	}
	if n := norm(v[0]); math.Abs(float64(n)-1.0) > 1e-5 {
		t.Fatalf("norm is %f, want 1.0 — downstream cosine recovery assumes unit vectors", n)
	}
}

func TestDimensionIs384(t *testing.T) {
	e := mustOpenEmbedder(t)
	if e.Dim() != 384 {
		t.Fatalf("Dim() = %d", e.Dim())
	}
	v, err := e.Embed(context.Background(), []string{"anything"})
	if err != nil {
		t.Fatal(err)
	}
	if len(v[0]) != 384 {
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
	a, err := e.Embed(ctx, []string{"determinism matters"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.Embed(ctx, []string{"determinism matters"})
	if err != nil {
		t.Fatal(err)
	}
	if d := cosine(a[0], b[0]); d < 0.9999999 {
		t.Fatalf("two runs of the same text differ: cosine %f", d)
	}
}

// Text longer than the sequence limit is truncated, not refused: a memory too
// long to embed exactly is still worth storing and still worth finding.
func TestVeryLongTextIsTruncatedRatherThanRefused(t *testing.T) {
	e := mustOpenEmbedder(t)
	long := strings.Repeat("the quick brown fox jumps over the lazy dog. ", 500)
	v, err := e.Embed(context.Background(), []string{long})
	if err != nil {
		t.Fatalf("a very long text was refused: %v", err)
	}
	if n := norm(v[0]); math.Abs(float64(n)-1.0) > 1e-5 {
		t.Fatalf("norm is %f", n)
	}
}

func TestEmptyAndWhitespaceTextsProduceUsableVectors(t *testing.T) {
	e := mustOpenEmbedder(t)
	got, err := e.Embed(context.Background(), []string{"", "   ", "\n\t"})
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range got {
		if len(v) != 384 {
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
	if _, err := mustOpenEmbedder(t).Embed(context.Background(), nil); err == nil {
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
	if _, err := e.Embed(context.Background(), []string{"after close"}); err == nil {
		t.Fatal("a closed model served a request")
	}
}
