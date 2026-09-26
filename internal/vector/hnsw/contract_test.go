package hnsw_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
	"github.com/remem-org/remem-go/internal/vector/vectortest"
)

// The whole point of the shared suite: `flat` is the definition of correct, and
// on cases this small there is no approximation to hide behind. An
// implementation that disagrees here has a bug rather than a recall trade-off.
func TestContract(t *testing.T) {
	vectortest.Run(t, func(t *testing.T) vector.Index {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		idx, err := hnsw.New(kv, hnsw.Options{Metric: distance.L2})
		if err != nil {
			t.Fatalf("hnsw.New: %v", err)
		}
		return idx
	})
}
