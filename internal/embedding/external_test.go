package embedding_test

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
)

func TestAnExternalEmbedderDeclaresItsModelAndComputesNothing(t *testing.T) {
	e := embedding.NewExternal("cohere-embed-english-v3.0", 768)
	if e.ModelID() != "cohere-embed-english-v3.0" || e.Dim() != 768 {
		t.Fatalf("declared %q/%d", e.ModelID(), e.Dim())
	}
	// Each refusal names the mode and the request field that carries the
	// vector instead, so a caller is told what to send.
	_, err := e.EmbedQueries(context.Background(), []string{"anything"})
	if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "external") || !strings.Contains(err.Error(), "query_vector") {
		t.Fatalf("an external embedder must refuse a query, naming the mode and query_vector; got %v", err)
	}
	_, err = e.EmbedDocuments(context.Background(), []string{"anything"})
	if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "external") || !strings.Contains(err.Error(), "`embedding`") {
		t.Fatalf("an external embedder must refuse a document, naming the mode and embedding; got %v", err)
	}
	if !embedding.CallerSupplied(e) {
		t.Fatal("an external embedder is caller-supplied")
	}
}

func TestOnlyTheExternalEmbedderIsCallerSupplied(t *testing.T) {
	if embedding.CallerSupplied(embeddingtest.New()) {
		t.Fatal("a computing embedder is not caller-supplied")
	}
	if embedding.CallerSupplied(nil) {
		t.Fatal("no embedder is not caller-supplied")
	}
}

func TestASuppliedVectorIsCheckedAndNeverRescaled(t *testing.T) {
	good := []float32{3, 4}
	got, err := embedding.CheckSupplied("embedding", good, 2, true)
	if err != nil {
		t.Fatalf("a finite vector of the right width is accepted: %v", err)
	}
	if got[0] != 3 || got[1] != 4 {
		t.Fatalf("a supplied vector is stored as given, got %v", got)
	}
	got[0] = 99
	if good[0] != 3 {
		t.Fatal("CheckSupplied must return a copy, not alias the caller's slice")
	}

	for _, tc := range []struct {
		name    string
		v       []float32
		nonZero bool
		want    []string
	}{
		{"wrong width", []float32{1, 2, 3}, true, []string{"embedding", "3", "2"}},
		{"empty", nil, true, []string{"embedding", "0", "2"}},
		{"not finite", []float32{float32(math.NaN()), 1}, true, []string{"embedding", "finite"}},
		{"infinite", []float32{float32(math.Inf(1)), 1}, true, []string{"embedding", "finite"}},
		{"zero under cosine", []float32{0, 0}, true, []string{"embedding", "zero"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := embedding.CheckSupplied("embedding", tc.v, 2, tc.nonZero)
			if !errs.Is(err, errs.Invalid) {
				t.Fatalf("want an Invalid refusal, got %v", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("refusal %q does not name %q", err, w)
				}
			}
		})
	}
	if _, err := embedding.CheckSupplied("embedding", []float32{0, 0}, 2, false); err != nil {
		t.Fatalf("a zero vector is legal where the metric defines it: %v", err)
	}
}

func TestClosingNoServiceIsNotAFailure(t *testing.T) {
	var s *embedding.Service
	if err := s.Close(); err != nil {
		t.Fatalf("a server in external mode builds no service; closing it must be a no-op: %v", err)
	}
}
