package pebble_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// These are the tests that hold the store's conditional-commit semantics while
// the lock behind them is narrowed. Both passed before the write mutex was
// striped by tenant and must keep passing after: what striping changes is who
// waits, never what a condition means.
//
// Run them with -race. The point of a lock this narrow is that it is easy to
// make too narrow, and a condition checked without the guard its write needs
// shows up as a lost update, not as a data race — so the counters below are
// what actually catches it, and -race catches the rest.

// A conditional create is how every Remem write claims a new key: Expect the
// key to be absent, then write it. Exactly one of many concurrent attempts on
// the same key may win, and the losers must be told Conflict — not succeed, and
// not fail some other way.
func TestOneConditionalCreatorWinsAmongManyInOneTenant(t *testing.T) {
	kv := open(t)
	ctx := context.Background()
	key := keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, id.New())

	const attempts = 32
	var (
		start           sync.WaitGroup
		done            sync.WaitGroup
		mu              sync.Mutex
		won             int
		unexpectedError error
	)
	start.Add(1)
	for i := range attempts {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			b := kv.NewBatch()
			defer func() { _ = b.Close() }()
			b.Expect(key, nil, false)
			b.Set(key, []byte(fmt.Sprintf("writer-%d", i)))
			err := b.Commit(ctx, true)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errs.Is(err, errs.Conflict):
			default:
				unexpectedError = err
			}
		}()
	}
	start.Done()
	done.Wait()

	if unexpectedError != nil {
		t.Fatalf("a losing creator failed with %v, want Conflict", unexpectedError)
	}
	if won != 1 {
		t.Fatalf("%d of %d concurrent conditional creates succeeded, want exactly 1", won, attempts)
	}
}

// Eight tenants, each with writers incrementing that tenant's own counter under
// a condition on its current value. A read-modify-write that is not atomic
// against the other writers of the same key loses increments, and the final
// count says so; the fact that the other seven tenants are writing at the same
// time must make no difference at all.
func TestConcurrentConditionalWritersAcrossTenantsLoseNoUpdates(t *testing.T) {
	kv := open(t)
	ctx := context.Background()

	const (
		tenants            = 8
		writersPerTenant   = 4
		incrementsPerWiter = 25
	)
	counters := make([][]byte, tenants)
	for i := range counters {
		counters[i] = keys.AttrRow(tenant.ID(fmt.Sprintf("tenant-%d", i)),
			tenant.DefaultNamespace, id.Zero)
	}

	var wg sync.WaitGroup
	for i := range tenants {
		for range writersPerTenant {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range incrementsPerWiter {
					if err := increment(ctx, kv, counters[i]); err != nil {
						t.Errorf("tenant %d: %v", i, err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()

	want := uint64(writersPerTenant * incrementsPerWiter)
	for i := range tenants {
		got, err := kv.Get(ctx, counters[i])
		if err != nil {
			t.Fatalf("tenant %d counter: %v", i, err)
		}
		if n := binary.BigEndian.Uint64(got); n != want {
			t.Errorf("tenant %d counted %d increments, want %d: a conditional write was not atomic", i, n, want)
		}
	}
}

// increment adds one to the counter at key, retrying while another writer gets
// there first. The retry is what makes a lost update visible as a wrong total
// rather than as an error: a conflict is expected and handled, so the only way
// the total can come out short is if a conflict went undetected.
func increment(ctx context.Context, kv storage.KV, key []byte) error {
	for {
		old, err := kv.Get(ctx, key)
		if err != nil && !errs.Is(err, errs.NotFound) {
			return err
		}
		exists := err == nil
		var n uint64
		if exists {
			n = binary.BigEndian.Uint64(old)
		}

		b := kv.NewBatch()
		b.Expect(key, old, exists)
		b.Set(key, binary.BigEndian.AppendUint64(nil, n+1))
		err = b.Commit(ctx, false)
		_ = b.Close()
		switch {
		case err == nil:
			return nil
		case errs.Is(err, errs.Conflict):
			continue
		default:
			return err
		}
	}
}
