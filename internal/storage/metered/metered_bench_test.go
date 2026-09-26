package metered_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/metered"
)

// The decorator's cost is what it adds to a write that already exists: a key
// parse and a labelled counter lookup. It is measured against the in-memory
// store, whose own cost is small enough that the addition is visible rather
// than lost under an fsync.
func BenchmarkSet(b *testing.B) {
	for _, c := range []struct {
		name string
		kv   func() storage.KV
	}{
		{"raw", func() storage.KV { return memkv.New() }},
		{"metered", func() storage.KV { return metered.New(memkv.New(), &obs.NewMetrics().Storage) }},
	} {
		b.Run(c.name, func(b *testing.B) {
			kv := c.kv()
			defer func() { _ = kv.Close() }()
			ctx := context.Background()
			key := keys.Record("acme", ns, keys.RecordMemory, [16]byte{1})
			value := []byte("value")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := kv.Set(ctx, key, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
