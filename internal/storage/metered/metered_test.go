package metered_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/metered"
	"github.com/remem-org/remem-go/internal/storage/storagetest"
	"github.com/remem-org/remem-go/internal/tenant"
)

var ns = tenant.DefaultNamespace

func newMetered(t *testing.T) (storage.KV, *obs.Metrics) {
	t.Helper()
	m := obs.NewMetrics()
	kv := metered.New(memkv.New(), &m.Storage)
	t.Cleanup(func() { _ = kv.Close() })
	return kv, m
}

func record(t tenant.ID) []byte { return keys.Record(t, ns, keys.RecordMemory, id.New()) }

// value reads one counter child. It writes the metric into the protobuf form
// the client library already depends on, rather than pulling in the testutil
// package, whose diff dependency is not otherwise in this module.
func value(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// series counts the children a collector currently holds.
func series(c prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)
	n := 0
	for range ch {
		n++
	}
	return n
}

// The decorator is a second path through the storage contract, so it runs the
// contract. Counting must not change a single answer the store gives.
func TestTheDecoratorKeepsTheStorageContract(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.KV {
		kv, _ := newMetered(t)
		return kv
	})
}

func TestAWriteIsCountedByTenantAndSpace(t *testing.T) {
	kv, m := newMetered(t)
	ctx := context.Background()
	if err := kv.Set(ctx, record("acme"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete(ctx, record("acme")); err != nil {
		t.Fatal(err)
	}
	if got := value(t, m.Storage.WritesTotal.WithLabelValues("acme", "record")); got != 2 {
		t.Fatalf("a Set and a Delete counted %v writes under acme/record, want 2", got)
	}
	if got := value(t, m.Storage.WritesTotal.WithLabelValues("globex", "record")); got != 0 {
		t.Fatalf("globex was charged %v writes it did not make", got)
	}
}

// A batch is counted per key, and only once it has committed: a write that
// never happened is not a write, and counting at staging time would report
// every conflicted retry as work done.
func TestABatchIsCountedPerKeyOnlyWhenItCommits(t *testing.T) {
	kv, m := newMetered(t)
	ctx := context.Background()
	writes := m.Storage.WritesTotal.WithLabelValues("acme", "record")

	b := kv.NewBatch()
	for range 3 {
		b.Set(record("acme"), []byte("x"))
	}
	if got := value(t, writes); got != 0 {
		t.Fatalf("staging counted %v writes before any commit", got)
	}
	if err := b.Commit(ctx, true); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if got := value(t, writes); got != 3 {
		t.Fatalf("a committed batch of three counted %v writes", got)
	}

	failed := kv.NewBatch()
	failed.Expect(record("acme"), []byte("expected"), true) // the key does not exist, so this conflicts
	failed.Set(record("acme"), []byte("x"))
	if err := failed.Commit(ctx, true); !errs.Is(err, errs.Conflict) {
		t.Fatalf("the conditional commit returned %v, want Conflict", err)
	}
	_ = failed.Close()
	if got := value(t, writes); got != 3 {
		t.Fatalf("a batch that failed to commit moved the count to %v", got)
	}
}

func TestAReadIsCountedEvenWhenTheKeyIsAbsent(t *testing.T) {
	kv, m := newMetered(t)
	ctx := context.Background()
	if _, err := kv.Get(ctx, record("acme")); !errs.Is(err, errs.NotFound) {
		t.Fatalf("Get of an absent key: %v", err)
	}
	snap := kv.NewSnapshot()
	if _, err := snap.Get(ctx, record("acme")); !errs.Is(err, errs.NotFound) {
		t.Fatalf("snapshot Get of an absent key: %v", err)
	}
	_ = snap.Close()
	if got := value(t, m.Storage.ReadsTotal.WithLabelValues("acme", "record")); got != 2 {
		t.Fatalf("two reads of an absent key counted %v", got)
	}
}

// A scan is observed when its iterator closes, labelled by its lower bound:
// the duration that matters is how long the range was held open.
func TestAScanIsObservedWhenItsIteratorCloses(t *testing.T) {
	kv, m := newMetered(t)
	lower, upper := keys.SpaceRange("acme", ns, keys.SpaceText)
	it := kv.NewIterator(lower, upper)
	for ok := it.First(); ok; ok = it.Next() {
	}
	if n := series(m.Storage.ScanDuration); n != 0 {
		t.Fatalf("an open iterator already reported %d scan series", n)
	}
	_ = it.Close()
	_ = it.Close() // idempotent, and observed once

	var h dto.Metric
	obs, err := m.Storage.ScanDuration.GetMetricWithLabelValues("acme", "text")
	if err != nil {
		t.Fatal(err)
	}
	if err := obs.(prometheus.Metric).Write(&h); err != nil {
		t.Fatal(err)
	}
	if got := h.GetHistogram().GetSampleCount(); got != 1 {
		t.Fatalf("one closed scan, closed twice, was observed %d times", got)
	}
}

// System rows have no tenant, and a key that does not parse is exactly the one
// an operator needs to see — so neither is dropped.
func TestSystemAndUnparseableKeysAreCountedNotDropped(t *testing.T) {
	kv, m := newMetered(t)
	ctx := context.Background()
	if err := kv.Set(ctx, keys.System("format"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(ctx, []byte{0xFF, 0x01}, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if got := value(t, m.Storage.WritesTotal.WithLabelValues("", "system")); got != 1 {
		t.Fatalf("a system write counted %v under the empty tenant", got)
	}
	if got := value(t, m.Storage.WritesTotal.WithLabelValues("", "unparsed")); got != 1 {
		t.Fatalf("an unparseable key counted %v under unparsed", got)
	}
}

// A store that can back itself up must still be one after it is wrapped:
// schema.BackUp finds the capability by type assertion, and a decorator that
// hid it would turn every pre-migration backup off without a word. And a store
// that cannot must not start claiming to.
func TestTheBackupCapabilityIsForwardedExactlyWhenItExists(t *testing.T) {
	m := obs.NewMetrics()
	// memkv can back itself up (it writes a storage.Dump), so it is the store
	// that has the capability. The first version of this test assumed it did
	// not, and failed against a decorator that was forwarding correctly.
	if _, ok := metered.New(memkv.New(), &m.Storage).(storage.Backupper); !ok {
		t.Fatal("wrapping a store that backs up hid the capability")
	}
	// Embedding only the interface exposes nothing beyond it, which makes this
	// a store with no backup by construction.
	if _, ok := metered.New(plainStore{memkv.New()}, &m.Storage).(storage.Backupper); ok {
		t.Fatal("wrapping a store with no backup made it claim one")
	}
}

type plainStore struct{ storage.KV }
