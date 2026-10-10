package flat_test

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// A corpus embedded by one model is not searched with another's queries. The
// exact index held this only by accident while the model was a fixed constant;
// a server whose vectors come from the caller can change the model by
// configuration, and then only this check stands between two spaces and one
// ranking.
func TestACorpusFromAnotherModelIsRefused(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	store := vector.NewStore(kv)
	ctx := context.Background()
	if err := store.Put(ctx, "acme", tenant.DefaultNamespace, id.New(),
		&vector.Vector{ModelID: "all-MiniLM-L6-v2", Dim: 2, Values: []float32{1, 0}}); err != nil {
		t.Fatal(err)
	}

	same := flat.New(store, distance.Cosine, flat.Serving("all-MiniLM-L6-v2"))
	if _, err := same.Search(ctx, "acme", []float32{1, 0}, 1, nil); err != nil {
		t.Fatalf("the corpus's own model is served: %v", err)
	}

	other := flat.New(store, distance.Cosine, flat.Serving("cohere-embed-english-v3.0"))
	_, err := other.Search(ctx, "acme", []float32{1, 0}, 1, nil)
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("want Invalid, got %v", err)
	}
	for _, w := range []string{"all-MiniLM-L6-v2", "cohere-embed-english-v3.0"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("refusal %q does not name %q", err, w)
		}
	}

	if _, err := flat.New(store, distance.Cosine).Search(ctx, "acme", []float32{1, 0}, 1, nil); err != nil {
		t.Fatalf("an index told no model compares none, as before: %v", err)
	}
}
