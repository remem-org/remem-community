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
