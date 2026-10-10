package txn_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/txn"
)

func TestWritesAreInvisibleUntilCommit(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	ctx := context.Background()

	tx := txn.New(kv)
	defer tx.Close()
	tx.Set([]byte("k"), []byte("v"))

	if _, err := kv.Get(ctx, []byte("k")); !errs.Is(err, errs.NotFound) {
		t.Fatalf("the store saw an uncommitted write: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, err := kv.Get(ctx, []byte("k"))
	if err != nil || string(got) != "v" {
		t.Fatalf("after Commit: %q, %v", got, err)
	}
}

// Read-your-writes inside the transaction is what lets a repository check
// "does this record already exist" against work the same transaction staged.
func TestReadsSeeTheTransactionsOwnWrites(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	if err := kv.Set(context.Background(), []byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}

	tx := txn.New(kv)
	defer tx.Close()
	tx.Set([]byte("k"), []byte("new"))
	got, err := tx.Get([]byte("k"))
	if err != nil || string(got) != "new" {
		t.Fatalf("Get = %q, %v; want the staged value", got, err)
	}

	tx.Delete([]byte("k"))
	if _, err := tx.Get([]byte("k")); !errs.Is(err, errs.NotFound) {
		t.Fatalf("a key staged for deletion read as %v, want NotFound", err)
	}
}

func TestClosingWithoutCommitDiscardsEverything(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	ctx := context.Background()

	tx := txn.New(kv)
	tx.Set([]byte("k"), []byte("v"))
	tx.Close()

	if _, err := kv.Get(ctx, []byte("k")); !errs.Is(err, errs.NotFound) {
		t.Fatalf("an abandoned transaction left a write behind: %v", err)
	}
}

// A second Commit must fail rather than replay. Replaying would re-apply
// writes over whatever happened in between — which, for a repository that
// retries on a caller error, is how a stale record overwrites a fresh one.
func TestCommitIsNotRepeatable(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	ctx := context.Background()

	tx := txn.New(kv)
	defer tx.Close()
	tx.Set([]byte("k"), []byte("v"))
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("a committed transaction committed again")
	}
}

func TestUsingAClosedTransactionIsRefused(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()

	tx := txn.New(kv)
	tx.Close()
	tx.Set([]byte("k"), []byte("v")) // must not panic
	if _, err := tx.Get([]byte("k")); err == nil {
		t.Fatal("a closed transaction served a read")
	}
	if err := tx.Commit(context.Background()); err == nil {
		t.Fatal("a closed transaction committed")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	tx := txn.New(kv)
	tx.Close()
	tx.Close()
}

func TestLenReportsStagedOperations(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	tx := txn.New(kv)
	defer tx.Close()

	if tx.Len() != 0 {
		t.Fatalf("a fresh transaction has %d staged operations", tx.Len())
	}
	tx.Set([]byte("a"), []byte("1"))
	tx.Delete([]byte("b"))
	if tx.Len() != 2 {
		t.Fatalf("Len = %d, want 2", tx.Len())
	}
}

// Durability is a decision an operator makes once, in configuration, not one a
// call site makes per write — so it is fixed when the transaction is opened.
func TestSyncIsAPropertyOfTheTransaction(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	tx.Set([]byte("k"), []byte("v"))
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// Exactly one of the two callback sets runs, and Close is what guarantees one of
// them does: something a staged write reserved outside the batch is given back
// whichever way the transaction ends.
func TestExactlyOneOfTheCallbackSetsRuns(t *testing.T) {
	ctx := context.Background()
	for name, finish := range map[string]func(tx txn.Tx) error{
		"committed": func(tx txn.Tx) error { return tx.Commit(ctx) },
		"abandoned": func(tx txn.Tx) error { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			kv := memkv.New()
			defer func() { _ = kv.Close() }()

			var committed, closed int
			tx := txn.New(kv)
			if !tx.OnCommit(func() { committed++ }) {
				t.Fatal("a fresh transaction refused a commit callback")
			}
			if !tx.OnClose(func() { closed++ }) {
				t.Fatal("a fresh transaction refused a close callback")
			}
			tx.Set([]byte("k"), []byte("v"))
			if err := finish(tx); err != nil {
				t.Fatalf("finishing: %v", err)
			}
			// Every transaction is closed, and twice is allowed, which is where a
			// callback that ran too often or at the wrong end would show.
			tx.Close()
			tx.Close()

			wantCommitted, wantClosed := 0, 1
			if name == "committed" {
				wantCommitted, wantClosed = 1, 0
			}
			if committed != wantCommitted || closed != wantClosed {
				t.Errorf("%d commit callbacks and %d close callbacks ran, want %d and %d",
					committed, closed, wantCommitted, wantClosed)
			}
		})
	}
}

// A commit that fails is not a commit, so it is the *close* callbacks that run — the
// ones that give back what the staged write reserved outside the batch.
//
// The conflict is the ordinary way a commit fails: `txn.Do` retries on one, and a
// retry whose predecessor had published its reservation instead of giving it back
// would leave it held for the life of the process. attr.Indexer.Stage's staging mark
// is that reservation, and TestAConflictingStageGivesItsStagingMarkBack
// (internal/attr) is this same claim through it.
func TestAFailedCommitRunsTheCloseCallbacksAndNotTheCommitOnes(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	ctx := context.Background()
	if err := kv.Set(ctx, []byte("k"), []byte("current")); err != nil {
		t.Fatal(err)
	}

	var committed, closed int
	tx := txn.New(kv)
	if !tx.OnCommit(func() { committed++ }) {
		t.Fatal("a fresh transaction refused a commit callback")
	}
	if !tx.OnClose(func() { closed++ }) {
		t.Fatal("a fresh transaction refused a close callback")
	}

	// A condition the store does not satisfy, which is what a writer that read a
	// value another writer has since changed stages.
	tx.Expect([]byte("k"), []byte("stale"), true)
	tx.Set([]byte("k"), []byte("new"))
	if err := tx.Commit(ctx); !errs.Is(err, errs.Conflict) {
		t.Fatalf("Commit returned %v, want a conflict", err)
	}

	// Every transaction is closed, and twice is allowed: that is where a callback
	// that ran at the wrong end or more than once shows.
	tx.Close()
	tx.Close()

	if committed != 0 || closed != 1 {
		t.Errorf("%d commit callbacks and %d close callbacks ran after a failed commit, "+
			"want 0 and 1", committed, closed)
	}
	got, err := kv.Get(ctx, []byte("k"))
	if err != nil || string(got) != "current" {
		t.Errorf("after the failed commit the store holds %q, %v; want the value it had",
			got, err)
	}
}

// A spent transaction reports that it took neither registration rather than
// dropping it silently. A caller whose callback gives back something it has
// already reserved has no other way to find out.
func TestASpentTransactionRefusesCallbacksAndSaysSo(t *testing.T) {
	ctx := context.Background()
	for name, spend := range map[string]func(tx txn.Tx){
		"committed": func(tx txn.Tx) {
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("committing: %v", err)
			}
		},
		"closed": func(tx txn.Tx) { tx.Close() },
	} {
		t.Run(name, func(t *testing.T) {
			kv := memkv.New()
			defer func() { _ = kv.Close() }()

			tx := txn.New(kv)
			defer tx.Close()
			spend(tx)

			ran := 0
			if tx.OnCommit(func() { ran++ }) {
				t.Error("a spent transaction accepted a commit callback")
			}
			if tx.OnClose(func() { ran++ }) {
				t.Error("a spent transaction accepted a close callback")
			}
			tx.Close()
			if ran != 0 {
				t.Errorf("%d callbacks a spent transaction had refused ran anyway", ran)
			}
		})
	}
}
