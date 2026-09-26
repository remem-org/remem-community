package hnsw_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// One memory unlike all the others must still be findable.
//
// This is the failure an approximate index has to be held to, because its
// mistakes look like results: a search for the outlier's own vector returned an
// unrelated memory at maximum distance, confidently, with no sign anything was
// wrong.
//
// The mechanism is worth stating. Search follows out-edges, so a node is
// reachable only if something links *to* it. Neighbour lists are pruned by a
// diversity heuristic, and near-duplicates are never pruned in favour of a
// distant node — so every duplicate eventually drops the outlier, the outlier
// keeps its own out-edges into the cluster, and nothing anywhere points back.
func TestAnOutlierAmongDuplicatesStaysReachable(t *testing.T) {
	ctx := context.Background()
	for _, duplicates := range []int{50, 200} {
		t.Run("", func(t *testing.T) {
			kv := memkv.New()
			t.Cleanup(func() { _ = kv.Close() })
			idx := newIndex(t, kv, hnsw.Options{})

			outlier := id.New()
			q := []float32{1, 0, 0, 0}
			if err := idx.Insert(ctx, "acme", outlier, q); err != nil {
				t.Fatal(err)
			}
			for range duplicates {
				if err := idx.Insert(ctx, "acme", id.New(), []float32{0, 1, 0, 0}); err != nil {
					t.Fatal(err)
				}
			}

			hits, err := idx.Search(ctx, "acme", q, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(hits) != 1 || hits[0].ID != outlier {
				t.Fatalf("with %d duplicates, a search for the outlier's own vector returned %v", duplicates, hits)
			}

			// And after a restart, when the graph is materialised from node
			// records rather than built in memory.
			restarted := newIndex(t, kv, hnsw.Options{})
			hits, err = restarted.Search(ctx, "acme", q, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(hits) != 1 || hits[0].ID != outlier {
				t.Fatalf("with %d duplicates, the reloaded index returned %v", duplicates, hits)
			}
		})
	}
}
