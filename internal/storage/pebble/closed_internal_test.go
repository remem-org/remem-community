package pebble

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

// A write that arrives after the store closed is an error, never a panic.
//
// Pebble's own answer to a commit into a closed DB is `panic(ErrClosed)`:
// unrecoverable, and the shutdown's problem rather than the writer's. Store.Set
// and Store.Delete have always guarded against it; a batch did not, so a
// background worker still mid-write when the store closed took the process
// down with it instead of reporting Unavailable and being counted. That is the
// second half of the index queue's shutdown race — the worker outliving the
// drain — and this is the half that makes it survivable wherever else it
// happens too.
func TestABatchCommittedAfterTheStoreClosedIsUnavailable(t *testing.T) {
	s := openStore(t)
	key := keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, id.New())

	b := s.NewBatch()
	defer func() { _ = b.Close() }()
	b.Set(key, []byte("a node record a worker was still writing"))

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(context.Background(), false); !errs.Is(err, errs.Unavailable) {
		t.Fatalf("committing into a closed store returned %v, want Unavailable", err)
	}
}

// The guard belongs inside apply, under the stripes, and not only in Commit.
//
// Commit's check is the early answer a caller gets for a store that was
// already closed. apply's is the one that cannot be raced: Store.Close takes
// every stripe before it sets the flag, so a batch holding its own stripes
// either commits into an open store or sees the flag — the same argument
// Store.Set and Store.Delete rest on. Calling apply directly is what pins it
// there, because a test going through Commit would be satisfied by either.
func TestApplyRefusesAClosedStoreWhileHoldingItsStripes(t *testing.T) {
	s := openStore(t)
	key := keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, id.New())

	b := s.NewBatch().(*batch)
	defer func() { _ = b.Close() }()
	b.Set(key, []byte("a node record a worker was still writing"))

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.apply(); !errs.Is(err, errs.Unavailable) {
		t.Fatalf("applying into a closed store returned %v, want Unavailable", err)
	}
}
