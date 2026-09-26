// Package storagetest is the shared contract every storage.KV implementation
// must satisfy (spec §46.2).
//
// It is exported rather than internal to the storage package because its whole
// value is that memkv and pebble run the *identical* suite: a behaviour that
// differs between the store used in tests and the store used in production is
// a behaviour that will be discovered in production.
//
// Add a case here, not in one implementation's own test file, whenever a
// requirement is about what a KV *is* rather than about how one is built.
package storagetest

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
)

// Run exercises the behaviour every KV implementation must share.
//
// open returns a fresh, empty store and registers its own cleanup; it is
// called once per subtest so no case can observe another's writes.
func Run(t *testing.T, open func(t *testing.T) storage.KV) {
	t.Helper()
	t.Run("IteratorCloseIsIdempotent", func(t *testing.T) { iteratorClose(t, open) })

	t.Run("ConditionalBatchCommitIsAtomic", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("guard"), []byte("old")))
		must(t, kv.Set(ctx(), []byte("untouched"), []byte("before")))

		b := kv.NewBatch()
		defer func() { _ = b.Close() }()
		b.Expect([]byte("guard"), []byte("old"), true)
		b.Set([]byte("guard"), []byte("ours"))
		b.Set([]byte("untouched"), []byte("ours"))
		must(t, kv.Set(ctx(), []byte("guard"), []byte("theirs")))

		if err := b.Commit(ctx(), true); !errs.Is(err, errs.Conflict) {
			t.Fatalf("stale conditional commit = %v, want Conflict", err)
		}
		assertGet(t, kv, "guard", "theirs")
		assertGet(t, kv, "untouched", "before")
	})

	t.Run("ConditionalBatchDistinguishesMissingAndEmpty", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("empty"), []byte{}))

		missing := kv.NewBatch()
		defer func() { _ = missing.Close() }()
		missing.Expect([]byte("empty"), nil, false)
		missing.Set([]byte("wrong"), []byte("write"))
		if err := missing.Commit(ctx(), true); !errs.Is(err, errs.Conflict) {
			t.Fatalf("empty value treated as missing: %v", err)
		}

		empty := kv.NewBatch()
		defer func() { _ = empty.Close() }()
		empty.Expect([]byte("empty"), []byte{}, true)
		empty.Expect([]byte("absent"), nil, false)
		empty.Set([]byte("ok"), []byte("write"))
		must(t, empty.Commit(ctx(), true))
		assertGet(t, kv, "ok", "write")
	})

	t.Run("GetMissingIsNotFound", func(t *testing.T) {
		kv := open(t)
		_, err := kv.Get(ctx(), []byte("nope"))
		if !errs.Is(err, errs.NotFound) {
			t.Fatalf("want NotFound, got %v", err)
		}
	})

	t.Run("SetThenGetRoundTrips", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("k"), []byte("v")))
		assertGet(t, kv, "k", "v")

		must(t, kv.Set(ctx(), []byte("k"), []byte("v2")))
		assertGet(t, kv, "k", "v2")
	})

	t.Run("GetReturnsACopyTheCallerOwns", func(t *testing.T) {
		// A caller that mutates what Get returned must not corrupt the store.
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("k"), []byte("value")))

		got, err := kv.Get(ctx(), []byte("k"))
		must(t, err)
		for i := range got {
			got[i] = 'X'
		}
		assertGet(t, kv, "k", "value")
	})

	t.Run("SetDoesNotRetainCallerSlices", func(t *testing.T) {
		// Callers build keys in reusable buffers. An implementation that keeps
		// the slice instead of copying it corrupts silently and much later.
		kv := open(t)
		key := []byte("key")
		val := []byte("val")
		must(t, kv.Set(ctx(), key, val))
		copy(key, "zzz")
		copy(val, "zzz")
		assertGet(t, kv, "key", "val")
	})

	t.Run("DeleteThenGetIsNotFound", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("k"), []byte("v")))
		must(t, kv.Delete(ctx(), []byte("k")))
		if _, err := kv.Get(ctx(), []byte("k")); !errs.Is(err, errs.NotFound) {
			t.Fatalf("want NotFound after delete, got %v", err)
		}
	})

	t.Run("DeleteAbsentKeyIsNotAnError", func(t *testing.T) {
		// The post-condition — the key is absent — already holds. Making this
		// an error would force every caller to ignore it.
		kv := open(t)
		must(t, kv.Delete(ctx(), []byte("never-existed")))
	})

	t.Run("EmptyValueIsDistinctFromAbsent", func(t *testing.T) {
		// A tombstone, a presence marker and an empty index entry are all real
		// values. Collapsing them into "absent" loses information.
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("empty"), []byte{}))

		got, err := kv.Get(ctx(), []byte("empty"))
		must(t, err)
		if got == nil || len(got) != 0 {
			t.Fatalf("an empty value must read back as empty and present, got %v (len %d)", got, len(got))
		}
		if _, err := kv.Get(ctx(), []byte("absent")); !errs.Is(err, errs.NotFound) {
			t.Fatalf("want NotFound for a key never written, got %v", err)
		}
	})

	t.Run("BatchIsAtomic", func(t *testing.T) {
		kv := open(t)
		b := kv.NewBatch()
		defer b.Close()
		b.Set([]byte("a"), []byte("1"))
		b.Set([]byte("b"), []byte("2"))

		// Not visible before commit.
		if _, err := kv.Get(ctx(), []byte("a")); !errs.Is(err, errs.NotFound) {
			t.Fatal("batch writes leaked before commit")
		}
		must(t, b.Commit(ctx(), true))
		assertGet(t, kv, "a", "1")
		assertGet(t, kv, "b", "2")
	})

	t.Run("BatchReadsItsOwnWrites", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("stored"), []byte("old")))

		b := kv.NewBatch()
		defer b.Close()
		b.Set([]byte("stored"), []byte("new"))
		b.Set([]byte("fresh"), []byte("v"))

		if got, err := b.Get([]byte("stored")); err != nil || string(got) != "new" {
			t.Fatalf("staged overwrite not visible in the batch: %q %v", got, err)
		}
		if got, err := b.Get([]byte("fresh")); err != nil || string(got) != "v" {
			t.Fatalf("staged write not visible in the batch: %q %v", got, err)
		}
		if got, err := b.Get([]byte("stored-elsewhere")); !errs.Is(err, errs.NotFound) {
			t.Fatalf("want NotFound through the batch, got %q %v", got, err)
		}
	})

	t.Run("BatchDeleteShadowsTheStore", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("k"), []byte("v")))

		b := kv.NewBatch()
		defer b.Close()
		b.Delete([]byte("k"))
		if _, err := b.Get([]byte("k")); !errs.Is(err, errs.NotFound) {
			t.Fatalf("a staged delete must read as NotFound inside the batch, got %v", err)
		}
		// Still present for everyone else until commit.
		assertGet(t, kv, "k", "v")

		must(t, b.Commit(ctx(), true))
		if _, err := kv.Get(ctx(), []byte("k")); !errs.Is(err, errs.NotFound) {
			t.Fatalf("want NotFound after the batch committed, got %v", err)
		}
	})

	t.Run("BatchLenCountsOperations", func(t *testing.T) {
		kv := open(t)
		b := kv.NewBatch()
		defer b.Close()
		if b.Len() != 0 {
			t.Fatalf("a fresh batch has 0 operations, got %d", b.Len())
		}
		b.Set([]byte("a"), []byte("1"))
		b.Set([]byte("a"), []byte("2")) // same key: still two operations
		b.Delete([]byte("b"))
		if b.Len() != 3 {
			t.Fatalf("want 3 staged operations, got %d", b.Len())
		}
	})

	t.Run("BatchAppliesOperationsInOrder", func(t *testing.T) {
		kv := open(t)
		b := kv.NewBatch()
		defer b.Close()
		b.Set([]byte("k"), []byte("first"))
		b.Delete([]byte("k"))
		b.Set([]byte("k"), []byte("last"))
		must(t, b.Commit(ctx(), true))
		assertGet(t, kv, "k", "last")
	})

	t.Run("BatchDoesNotRetainCallerSlices", func(t *testing.T) {
		kv := open(t)
		b := kv.NewBatch()
		defer b.Close()
		key := []byte("key")
		val := []byte("val")
		b.Set(key, val)
		copy(key, "zzz")
		copy(val, "zzz")
		must(t, b.Commit(ctx(), true))
		assertGet(t, kv, "key", "val")
	})

	t.Run("SnapshotIsStable", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("k"), []byte("before")))
		snap := kv.NewSnapshot()
		defer snap.Close()
		must(t, kv.Set(ctx(), []byte("k"), []byte("after")))

		got, err := snap.Get(ctx(), []byte("k"))
		must(t, err)
		if string(got) != "before" {
			t.Fatalf("snapshot must not observe later writes, got %q", got)
		}
	})

	t.Run("SnapshotDoesNotObserveLaterInsertsOrDeletes", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("gone"), []byte("v")))
		snap := kv.NewSnapshot()
		defer snap.Close()

		must(t, kv.Delete(ctx(), []byte("gone")))
		must(t, kv.Set(ctx(), []byte("added"), []byte("v")))

		if got, err := snap.Get(ctx(), []byte("gone")); err != nil || string(got) != "v" {
			t.Fatalf("a key deleted after the snapshot must still be visible in it: %q %v", got, err)
		}
		if _, err := snap.Get(ctx(), []byte("added")); !errs.Is(err, errs.NotFound) {
			t.Fatalf("a key added after the snapshot must be invisible in it, got %v", err)
		}
	})

	t.Run("SnapshotIteratorIsStable", func(t *testing.T) {
		kv := open(t)
		for _, k := range []string{"a", "c"} {
			must(t, kv.Set(ctx(), []byte(k), []byte(k)))
		}
		snap := kv.NewSnapshot()
		defer snap.Close()
		must(t, kv.Set(ctx(), []byte("b"), []byte("b")))

		it := snap.NewIterator(nil, nil)
		defer it.Close()
		if got := collect(t, it); !reflect.DeepEqual(got, []string{"a", "c"}) {
			t.Fatalf("a snapshot iterator observed a later write: %v", got)
		}
	})

	t.Run("IteratorRangeIsHalfOpen", func(t *testing.T) {
		kv := open(t)
		seed(t, kv, "a", "b", "c", "d")

		it := kv.NewIterator([]byte("b"), []byte("d"))
		defer it.Close()
		if got := collect(t, it); !reflect.DeepEqual(got, []string{"b", "c"}) {
			t.Fatalf("[lower, upper) violated: %v", got)
		}
	})

	t.Run("IteratorNilBoundsCoverEverything", func(t *testing.T) {
		kv := open(t)
		seed(t, kv, "a", "b", "c")

		it := kv.NewIterator(nil, nil)
		defer it.Close()
		if got := collect(t, it); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
			t.Fatalf("nil bounds must cover the whole store: %v", got)
		}
	})

	t.Run("IteratorOverAnEmptyRangeYieldsNothing", func(t *testing.T) {
		kv := open(t)
		seed(t, kv, "a", "z")

		it := kv.NewIterator([]byte("m"), []byte("n"))
		defer it.Close()
		if it.First() {
			t.Fatalf("an empty range must not position, got %q", it.Key())
		}
		if it.Last() {
			t.Fatal("an empty range must not position on Last either")
		}
		must(t, it.Error())
	})

	t.Run("IteratorReversesOverTheSameRange", func(t *testing.T) {
		kv := open(t)
		seed(t, kv, "a", "b", "c", "d")

		it := kv.NewIterator([]byte("b"), []byte("d"))
		defer it.Close()
		var got []string
		for ok := it.Last(); ok; ok = it.Prev() {
			got = append(got, string(it.Key()))
		}
		must(t, it.Error())
		if !reflect.DeepEqual(got, []string{"c", "b"}) {
			t.Fatalf("reverse iteration must mirror forward iteration: %v", got)
		}
	})

	t.Run("IteratorSeekGEHonoursTheBounds", func(t *testing.T) {
		kv := open(t)
		seed(t, kv, "a", "c", "e", "g")

		it := kv.NewIterator([]byte("c"), []byte("g"))
		defer it.Close()

		if !it.SeekGE([]byte("d")) || string(it.Key()) != "e" {
			t.Fatalf("SeekGE must land on the smallest key >= the target, got %q", it.Key())
		}
		if !it.SeekGE([]byte("a")) || string(it.Key()) != "c" {
			t.Fatalf("SeekGE below the lower bound must clamp to it, got %q", it.Key())
		}
		if it.SeekGE([]byte("g")) {
			t.Fatalf("SeekGE at the exclusive upper bound must not position, got %q", it.Key())
		}
		must(t, it.Error())
	})

	t.Run("IteratorObservesWritesMadeBeforeItWasCreated", func(t *testing.T) {
		kv := open(t)
		seed(t, kv, "a")
		it := kv.NewIterator(nil, nil)
		defer it.Close()
		if got := collect(t, it); !reflect.DeepEqual(got, []string{"a"}) {
			t.Fatalf("iterator missed a write that preceded it: %v", got)
		}
	})

	t.Run("IteratorValuesMatchTheirKeys", func(t *testing.T) {
		kv := open(t)
		for _, k := range []string{"a", "b", "c"} {
			must(t, kv.Set(ctx(), []byte(k), []byte("v-"+k)))
		}
		it := kv.NewIterator(nil, nil)
		defer it.Close()
		for ok := it.First(); ok; ok = it.Next() {
			if want := "v-" + string(it.Key()); string(it.Value()) != want {
				t.Fatalf("key %q carried value %q, want %q", it.Key(), it.Value(), want)
			}
		}
		must(t, it.Error())
	})

	t.Run("KeyAndValueMustBeCopiedToSurviveNext", func(t *testing.T) {
		// The contract says Key and Value are valid only until the next
		// positioning call. That is a *permission* implementations may take,
		// not an obligation they must honour, and the two implementations here
		// take it differently: memkv poisons both buffers deliberately, while
		// Pebble reuses its key buffer but leaves the old value bytes sitting
		// in a block it has not yet overwritten. Asserting "the old bytes are
		// gone" therefore cannot be a shared requirement — it would fail
		// Pebble for behaving exactly as documented.
		//
		// What every implementation must guarantee is the other direction:
		// a caller that copies gets the right answer, forwards and backwards.
		// The hostile version of this case lives in memkv's own test, because
		// memkv is what higher layers are tested against, and it is there that
		// a missing copy has to fail loudly.
		kv := open(t)
		for _, k := range []string{"aaaa", "bb", "cccccc"} {
			must(t, kv.Set(ctx(), []byte(k), []byte("value-of-"+k)))
		}

		it := kv.NewIterator(nil, nil)
		defer it.Close()

		type pair struct{ key, value string }
		var forward []pair
		for ok := it.First(); ok; ok = it.Next() {
			forward = append(forward, pair{
				key:   string(append([]byte(nil), it.Key()...)),
				value: string(append([]byte(nil), it.Value()...)),
			})
		}
		must(t, it.Error())

		want := []pair{
			{"aaaa", "value-of-aaaa"},
			{"bb", "value-of-bb"},
			{"cccccc", "value-of-cccccc"},
		}
		if !reflect.DeepEqual(forward, want) {
			t.Fatalf("copied entries are wrong going forward: %v", forward)
		}

		var backward []pair
		for ok := it.Last(); ok; ok = it.Prev() {
			backward = append(backward, pair{
				key:   string(append([]byte(nil), it.Key()...)),
				value: string(append([]byte(nil), it.Value()...)),
			})
		}
		must(t, it.Error())
		for i, j := 0, len(backward)-1; i < j; i, j = i+1, j-1 {
			backward[i], backward[j] = backward[j], backward[i]
		}
		if !reflect.DeepEqual(backward, want) {
			t.Fatalf("copied entries are wrong going backward: %v", backward)
		}
	})

	t.Run("ConcurrentBatchesDoNotInterleave", func(t *testing.T) {
		// Run under -race. Each batch writes a marker under two keys; a reader
		// that sees one marker must see the same one under both, or the two
		// batches were applied half-and-half.
		kv := open(t)
		const rounds = 200

		var wg sync.WaitGroup
		for w := 0; w < 2; w++ {
			marker := fmt.Sprintf("writer-%d", w)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < rounds; i++ {
					b := kv.NewBatch()
					b.Set([]byte("left"), []byte(marker))
					b.Set([]byte("right"), []byte(marker))
					if err := b.Commit(ctx(), false); err != nil {
						t.Errorf("commit: %v", err)
					}
					b.Close()
				}
			}()
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < rounds*4; i++ {
				snap := kv.NewSnapshot()
				left, lerr := snap.Get(ctx(), []byte("left"))
				right, rerr := snap.Get(ctx(), []byte("right"))
				snap.Close()
				if lerr != nil || rerr != nil {
					continue // nothing committed yet
				}
				if !bytes.Equal(left, right) {
					t.Errorf("torn batch: left=%q right=%q", left, right)
					return
				}
			}
		}()

		wg.Wait()
		<-done
	})

	t.Run("FlushMakesPrecedingWritesDurable", func(t *testing.T) {
		// Nothing here can prove durability without killing a process, which
		// is test/crash's job. What this holds is the part a caller depends on
		// every time: Flush succeeds, and the data is still there afterwards.
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("k"), []byte("v")))
		must(t, kv.Flush(ctx()))
		assertGet(t, kv, "k", "v")

		// Flushing an untouched store is not an error either; a periodic
		// flusher must not need to know whether anything was written.
		must(t, kv.Flush(ctx()))
	})

	t.Run("SnapshotIteratorAfterCloseYieldsNothingAndReportsWhy", func(t *testing.T) {
		// The failure has to travel: NewIterator cannot return an error, so a
		// construction failure has to surface through Error() rather than as
		// an empty result indistinguishable from an empty range.
		kv := open(t)
		must(t, kv.Set(ctx(), []byte("k"), []byte("v")))

		snap := kv.NewSnapshot()
		must(t, snap.Close())

		it := snap.NewIterator(nil, nil)
		defer it.Close()
		if it.First() {
			t.Fatal("an iterator over a closed snapshot must not position")
		}
		if it.Error() == nil {
			t.Fatal("an iterator that could not be created must report why, not look empty")
		}
		if it.Key() != nil || it.Value() != nil {
			t.Fatal("an invalid iterator must not expose a key or value")
		}
		if it.Last() || it.Next() || it.Prev() || it.SeekGE([]byte("k")) {
			t.Fatal("no positioning call may succeed on a failed iterator")
		}
	})

	t.Run("OperationsAfterCloseAreRefused", func(t *testing.T) {
		kv := open(t)
		must(t, kv.Close())
		if err := kv.Set(ctx(), []byte("k"), []byte("v")); err == nil {
			t.Fatal("a write after Close must fail rather than be silently lost")
		}
		if _, err := kv.Get(ctx(), []byte("k")); err == nil {
			t.Fatal("a read after Close must fail")
		}
	})
}

func ctx() context.Context { return context.Background() }

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertGet(t *testing.T, kv storage.KV, key, want string) {
	t.Helper()
	got, err := kv.Get(ctx(), []byte(key))
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("get %q = %q, want %q", key, got, want)
	}
}

func seed(t *testing.T, kv storage.KV, keys ...string) {
	t.Helper()
	for _, k := range keys {
		must(t, kv.Set(ctx(), []byte(k), []byte(k)))
	}
}

// collect walks the iterator forward, copying each key as the contract
// requires, and fails on an iteration error.
func collect(t *testing.T, it storage.Iterator) []string {
	t.Helper()
	var got []string
	for ok := it.First(); ok; ok = it.Next() {
		got = append(got, string(it.Key()))
	}
	must(t, it.Error())
	return got
}
