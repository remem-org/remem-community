package record

import (
	"context"
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
)

// benchCorpus is how many records the point read picks between. It is large
// enough that the reads miss the block cache's hottest page and small enough
// that the whole corpus is one memtable, which is the state a server's recent
// writes are in.
const benchCorpus = 2000

// BenchmarkBodyDecode splits what repo.GetBodyFrom costs into the point read
// and the decode, and measures a decode projected to the fields a result page
// reads against the full-fidelity one.
//
// The question it answers is plan item 17's: the Phase 13 profile put the full
// decode at 20 percent of a first-page search, and this says how much of that
// is avoidable. point_read and decode_full together are GetBodyFrom; the ratio
// between them is what decides whether a second decoder is worth its upkeep.
//
// Reads go through storage.SeekReader over a pinned snapshot, in a scattered
// order, because that is how a page of ranked hits arrives — ids in relevance
// order, not key order.
func BenchmarkBodyDecode(b *testing.B) {
	const t tenant.ID = "acme"
	ns := tenant.DefaultNamespace

	kv, err := pebble.Open(b.TempDir(), pebble.Options{
		CacheSizeBytes:    config.DefaultCacheSizeBytes,
		MemTableSizeBytes: config.DefaultMemTableSizeBytes,
		BloomBitsPerKey:   config.DefaultBloomBitsPerKey,
	})
	if err != nil {
		b.Fatalf("opening the store: %v", err)
	}
	b.Cleanup(func() { _ = kv.Close() })

	ctx := context.Background()
	ids := make([]id.ID, benchCorpus)
	bodies := make([][]byte, benchCorpus)
	batch := kv.NewBatch()
	for i := range ids {
		ids[i] = id.New()
		rec := fullyPopulated(ids[i])
		// Distinct content per record, so no two bodies are the same length
		// and the decode is not measured over one cached string.
		rec.Content = fmt.Sprintf("%s (note %d)", rec.Content, i)
		body, err := encodeBody(rec)
		if err != nil {
			b.Fatalf("encoding record %d: %v", i, err)
		}
		bodies[i] = body
		batch.Set(BodyKey(t, ns, ids[i]), body)
	}
	if err := batch.Commit(ctx, false); err != nil {
		b.Fatalf("writing the corpus: %v", err)
	}
	b.Logf("corpus of %d bodies, %d bytes each", benchCorpus, len(bodies[0]))

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	lower, upper := keys.TenantRange(t)

	// A fixed scatter rather than math/rand, so every sub-benchmark and every
	// run reads the same sequence.
	order := make([]int, benchCorpus)
	for i := range order {
		order[i] = (i * 1049) % benchCorpus
	}

	b.Run("point_read", func(b *testing.B) {
		reader := storage.NewSeekReader(snap, lower, upper)
		defer func() { _ = reader.Close() }()
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			v, err := reader.Get(ctx, BodyKey(t, ns, ids[order[i%benchCorpus]]))
			if err != nil {
				b.Fatal(err)
			}
			sinkBytes = v
		}
	})

	b.Run("decode_full", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			k := order[i%benchCorpus]
			rec, err := decodeBody(t, ns, ids[k], bodies[k], "bench")
			if err != nil {
				b.Fatal(err)
			}
			sinkRecord = rec
		}
	})

	b.Run("decode_projected", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			k := order[i%benchCorpus]
			rec, err := decodeBodyProjected(t, ns, ids[k], bodies[k], "bench")
			if err != nil {
				b.Fatal(err)
			}
			sinkRecord = rec
		}
	})

	// The two end to end, which is what a result page actually pays. They
	// re-read the body on every iteration, so the decode works over a buffer
	// the reader has just allocated rather than over one the loop has kept
	// warm.
	b.Run("read_and_decode_full", func(b *testing.B) {
		reader := storage.NewSeekReader(snap, lower, upper)
		defer func() { _ = reader.Close() }()
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			k := order[i%benchCorpus]
			v, err := reader.Get(ctx, BodyKey(t, ns, ids[k]))
			if err != nil {
				b.Fatal(err)
			}
			rec, err := decodeBody(t, ns, ids[k], v, "bench")
			if err != nil {
				b.Fatal(err)
			}
			sinkRecord = rec
		}
	})

	b.Run("read_and_decode_projected", func(b *testing.B) {
		reader := storage.NewSeekReader(snap, lower, upper)
		defer func() { _ = reader.Close() }()
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			k := order[i%benchCorpus]
			v, err := reader.Get(ctx, BodyKey(t, ns, ids[k]))
			if err != nil {
				b.Fatal(err)
			}
			rec, err := decodeBodyProjected(t, ns, ids[k], v, "bench")
			if err != nil {
				b.Fatal(err)
			}
			sinkRecord = rec
		}
	})
}

// Sinks, so nothing above is dead code the compiler may delete.
var (
	sinkBytes  []byte
	sinkRecord *Record
)
