package hnsw

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// TestCrashDuringRebuildResumes: a rebuild stopped part-way continues from its
// cursor rather than starting again, and finishes with the index an
// uninterrupted rebuild builds.
//
// "Continues" is measured, not inferred: the resumed run's inserts are counted
// and must be fewer than the corpus. And "the index an uninterrupted rebuild
// builds" is compared as bytes — the whole node-record space of both stores.
func TestCrashDuringRebuildResumes(t *testing.T) {
	const (
		n     = 3000
		dim   = 16
		batch = 500
	)
	const tid = tenant.ID("acme")
	ctx := context.Background()

	ids, vecs := resumeCorpus(n, dim)

	// The reference: one uninterrupted rebuild over the same corpus.
	refKV := memkv.New()
	t.Cleanup(func() { _ = refKV.Close() })
	putCorpus(t, refKV, tid, ids, vecs)
	ref := resumeIndex(t, refKV)
	if err := ref.Rebuild(ctx, tid, vector.NewStore(refKV), vector.WithBatchSize(batch)); err != nil {
		t.Fatalf("reference rebuild: %v", err)
	}

	// Interrupted after its second saved cursor.
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	putCorpus(t, kv, tid, ids, vecs)
	first := resumeIndex(t, kv)

	var saved [][]byte
	stop, cancel := context.WithCancel(ctx)
	defer cancel()
	err := first.Rebuild(stop, tid, vector.NewStore(kv),
		vector.WithBatchSize(batch),
		vector.WithProgress(nil, func(_ context.Context, c []byte) error {
			saved = append(saved, append([]byte(nil), c...))
			if len(saved) == 2 {
				cancel()
			}
			return nil
		}))
	if err == nil {
		t.Fatal("the interrupted rebuild reported success")
	}
	if !errors.Is(err, context.Canceled) && stop.Err() == nil {
		t.Fatalf("the interrupted rebuild failed for a reason other than its cancellation: %v", err)
	}
	partial := countSpace(t, kv, tid)
	if partial == 0 || partial >= n {
		t.Fatalf("after the interruption %d node records exist; want some and not all %d", partial, n)
	}

	// Resumed in a fresh index, as a restarted process would be.
	second := resumeIndex(t, kv)
	inserted := 0
	second.onInsert = func() { inserted++ }
	if err := second.Rebuild(ctx, tid, vector.NewStore(kv),
		vector.WithBatchSize(batch),
		vector.WithProgress(saved[len(saved)-1], func(context.Context, []byte) error { return nil })); err != nil {
		t.Fatalf("resuming: %v", err)
	}
	if inserted == 0 || inserted >= n {
		t.Fatalf("the resumed rebuild inserted %d of %d records: it should continue, not restart", inserted, n)
	}
	if inserted+partial != n {
		t.Fatalf("the resume inserted %d onto %d already written, which is not the %d the corpus holds",
			inserted, partial, n)
	}

	got, want := dumpNodes(t, kv, tid), dumpNodes(t, refKV, tid)
	if !bytes.Equal(got, want) {
		t.Fatalf("the resumed index's node records differ from an uninterrupted rebuild's "+
			"(%d bytes against %d)", len(got), len(want))
	}

	// And it answers: every stored vector finds itself first.
	for i := 0; i < n; i += 97 {
		hits, err := second.Search(ctx, tid, vecs[i], 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != 1 || hits[0].ID != ids[i] {
			t.Fatalf("record %d does not find itself after the resumed rebuild", i)
		}
	}
}

// TestAnUnrecognisedCursorRebuildsFromTheStart: a cursor this binary did not
// write is not an error — a whole rebuild is always a correct answer to
// "rebuild this" — and it must not be mistaken for progress.
func TestAnUnrecognisedCursorRebuildsFromTheStart(t *testing.T) {
	const tid = tenant.ID("acme")
	ctx := context.Background()
	ids, vecs := resumeCorpus(200, 8)
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	putCorpus(t, kv, tid, ids, vecs)

	idx := resumeIndex(t, kv)
	inserted := 0
	idx.onInsert = func() { inserted++ }
	if err := idx.Rebuild(ctx, tid, vector.NewStore(kv),
		vector.WithProgress([]byte{0xFF, 0x01}, func(context.Context, []byte) error { return nil })); err != nil {
		t.Fatalf("Rebuild with a foreign cursor: %v", err)
	}
	if inserted != len(ids) {
		t.Fatalf("a foreign cursor inserted %d of %d: it was taken as progress", inserted, len(ids))
	}
}

func resumeCorpus(n, dim int) ([]id.ID, [][]float32) {
	r := newRNG(42)
	ids := make([]id.ID, n)
	vecs := make([][]float32, n)
	for i := range ids {
		ids[i] = id.New()
		vecs[i] = r.unit(dim)
	}
	return ids, vecs
}

func putCorpus(t *testing.T, kv storage.KV, tid tenant.ID, ids []id.ID, vecs [][]float32) {
	t.Helper()
	store := vector.NewStore(kv)
	for i := range ids {
		if err := store.Put(context.Background(), tid, tenant.DefaultNamespace, ids[i], &vector.Vector{
			ModelID: "test", Dim: len(vecs[i]), Values: vecs[i],
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func resumeIndex(t *testing.T, kv storage.KV) *Index {
	t.Helper()
	idx, err := New(kv, Options{Metric: distance.L2})
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func dumpNodes(t *testing.T, kv storage.KV, tid tenant.ID) []byte {
	t.Helper()
	lower, upper := keys.SpaceRange(tid, tenant.DefaultNamespace, keys.SpaceVectorIndex)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	var out []byte
	for ok := it.First(); ok; ok = it.Next() {
		out = append(append(out, it.Key()...), it.Value()...)
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

func countSpace(t *testing.T, kv storage.KV, tid tenant.ID) int {
	t.Helper()
	lower, upper := keys.SpaceRange(tid, tenant.DefaultNamespace, keys.SpaceVectorIndex)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	c := 0
	for ok := it.First(); ok; ok = it.Next() {
		c++
	}
	return c
}
