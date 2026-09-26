package txn_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/txn"
)

// A conflict is contention, so a retry that re-reads and re-stages succeeds.
// This is what lets a row every write in a tenant touches be conditionally
// committed at all.
func TestDoRetriesAConflictAndSucceeds(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	ctx := context.Background()

	key := []byte("counter")
	if err := kv.Set(ctx, key, []byte("0")); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	attempts := 0
	err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		attempts++
		value, err := tx.Get(key)
		if err != nil {
			return err
		}
		tx.Expect(key, value, true)
		// Move the row underneath the first attempt only, so the second one
		// reads what it will still find at commit.
		if attempts == 1 {
			if err := kv.Set(ctx, key, []byte("1")); err != nil {
				return err
			}
		}
		tx.Set(key, []byte("2"))
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("the body ran %d times, want 2", attempts)
	}
	got, err := kv.Get(ctx, key)
	if err != nil || string(got) != "2" {
		t.Fatalf("the retried write did not land: %q, %v", got, err)
	}
}

// Sustained contention is reported rather than retried forever. A caller that
// waits indefinitely on a lock it cannot see is worse than one told the write
// met a concurrent one.
func TestDoGivesUpAndReportsTheConflict(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	ctx := context.Background()

	key := []byte("always-moving")
	if err := kv.Set(ctx, key, []byte("0")); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	attempts := 0
	err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		attempts++
		value, err := tx.Get(key)
		if err != nil {
			return err
		}
		tx.Expect(key, value, true)
		if err := kv.Set(ctx, key, []byte{byte(attempts)}); err != nil {
			return err
		}
		tx.Set(key, []byte("never"))
		return nil
	})
	if !errs.Is(err, errs.Conflict) {
		t.Fatalf("Do returned %v, want a conflict", err)
	}
	if attempts != txn.MaxAttempts {
		t.Fatalf("the body ran %d times, want %d", attempts, txn.MaxAttempts)
	}
}

// Only a conflict is retried. Running a body again because it returned
// "content is too long" would turn one bad request into eight.
func TestDoDoesNotRetryAnOrdinaryFailure(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()

	boom := errors.New("boom")
	attempts := 0
	err := txn.Do(context.Background(), kv, func(txn.Tx) error {
		attempts++
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do returned %v, want the body's own error", err)
	}
	if attempts != 1 {
		t.Fatalf("the body ran %d times, want 1", attempts)
	}
}

// The gate is what removes the contention rather than absorbing it: writers on
// one key go one at a time, and writers on different keys do not wait for each
// other. The second half is the property that matters for a multi-tenant
// server — one busy tenant must not slow the rest.
func TestTheGateSerialisesOneKeyAndNotAnother(t *testing.T) {
	gate := txn.NewGate()

	var mu sync.Mutex
	inside, peak := 0, 0
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := gate.Lock("acme")
			defer unlock()

			mu.Lock()
			inside++
			if inside > peak {
				peak = inside
			}
			mu.Unlock()

			mu.Lock()
			inside--
			mu.Unlock()
		}()
	}
	wg.Wait()
	if peak != 1 {
		t.Fatalf("%d writers were inside the gate at once", peak)
	}

	// Two different keys must not block each other. Asserted with a timeout
	// rather than by nesting the locks, so a stripe collision fails the test
	// instead of hanging the suite.
	unlock := gate.Lock("acme")
	defer unlock()

	taken := make(chan func(), 1)
	go func() { taken <- gate.Lock("globex") }()
	select {
	case release := <-taken:
		release()
	case <-time.After(2 * time.Second):
		t.Fatal("a writer for one tenant blocked behind another tenant's write")
	}
}
