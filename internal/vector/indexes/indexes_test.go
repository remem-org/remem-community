package indexes_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexes"
)

func TestOpenBuildsBothIndexes(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"flat", "hnsw"} {
		t.Run(kind, func(t *testing.T) {
			kv := memkv.New()
			t.Cleanup(func() { _ = kv.Close() })

			idx, err := indexes.Open(kv, indexes.Config{Kind: kind, Metric: "cosine"})
			if err != nil {
				t.Fatalf("opening %q: %v", kind, err)
			}
			rid := id.New()
			if err := idx.Insert(ctx, "acme", rid, []float32{1, 0, 0, 0}); err != nil {
				t.Fatal(err)
			}
			hits, err := idx.Search(ctx, "acme", []float32{1, 0, 0, 0}, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(hits) != 1 || hits[0].ID != rid {
				t.Fatalf("%q did not find what it stored: %v", kind, hits)
			}
		})
	}
}

// Which index needs telling about writes is answered by the interface, not by
// the configured name. That is what keeps the write path free of a branch on
// which index is running — and it is why `flat` must *not* satisfy it: telling
// an exact scan of the canonical rows about a canonical row would be a second
// write of the same fact, stamped with whatever model id the caller happened to
// have.
func TestOnlyTheDerivedIndexNeedsMaintaining(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	exact, err := indexes.Open(kv, indexes.Config{Kind: "flat", Metric: "cosine"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := exact.(vector.Maintainer); ok {
		t.Fatal("the exact index asks to be told about writes; it is the writes")
	}

	approx, err := indexes.Open(kv, indexes.Config{Kind: "hnsw", Metric: "cosine"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := approx.(vector.Maintainer); !ok {
		t.Fatal("the approximate index keeps a structure of its own and is never told about writes")
	}
}

func TestOpenRefusesWhatItCannotBuild(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	if _, err := indexes.Open(kv, indexes.Config{Kind: "ivf", Metric: "cosine"}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid for an index this binary does not build", err)
	}
	if _, err := indexes.Open(kv, indexes.Config{Kind: "hnsw", Metric: "manhattan"}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid for a metric this binary does not have", err)
	}
}
