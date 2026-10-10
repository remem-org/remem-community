package indexqueue_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexes"
	"github.com/remem-org/remem-go/internal/vector/indexqueue"
	"github.com/remem-org/remem-go/internal/vector/vectortest"
)

// A queue in front of an index is still that index. The suite drives Insert,
// Delete, Search, Stats, Health and Rebuild — every method the queue does not
// defer — and it is the check that wrapping changed none of their answers. Only
// Indexed and Removed are deferred, and the suite does not call them: they are
// the write path's, and it has Flush.
func TestTheWrappedIndexStillSatisfiesTheContract(t *testing.T) {
	vectortest.Run(t, func(t *testing.T) vector.Index {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		idx, err := indexes.Open(kv, indexes.Config{
			Kind: "hnsw", Metric: "l2", Neighbours: 16,
			BuildEffort: 200, SearchEffort: 64, ResidentBudgetBytes: 64 << 20,
		})
		if err != nil {
			t.Fatal(err)
		}
		q := indexqueue.New(idx.(indexqueue.Maintained))
		t.Cleanup(func() { _ = q.Close(context.Background()) })
		return q
	})
}
