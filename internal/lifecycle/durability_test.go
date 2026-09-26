package lifecycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
)

// syncSpy records whether each commit asked for durability.
type syncSpy struct {
	storage.KV
	syncs []bool
}

func (s *syncSpy) NewBatch() storage.Batch {
	return &syncSpyBatch{Batch: s.KV.NewBatch(), spy: s}
}

type syncSpyBatch struct {
	storage.Batch
	spy *syncSpy
}

func (b *syncSpyBatch) Commit(ctx context.Context, sync bool) error {
	b.spy.syncs = append(b.spy.syncs, sync)
	return b.Batch.Commit(ctx, sync)
}

// A recall waits for the disk, and it does so because the alternative was
// measured and found wanting.
//
// The first version of this code committed the recall event unsynced, on the
// reasoning that an unsynced write still reaches the write-ahead log and
// therefore survives the process dying. Phase 10's end-to-end run disproved it
// directly: a recall recorded, `kill -9`, restart, and the event was gone —
// Pebble applies an unsynced batch to the memtable and buffers the log, and
// nothing guarantees it has reached the operating system when the call returns.
//
// The plan's completion criterion is "recall survives process termination", so
// the fsync is paid. What is not paid is the rest of what Rust measured: one
// appended key, no read-modify-write, no conflict, no global write lock.
//
// This test is plumbing rather than durability — the guarantee is the store's
// and is verified against the real binary — but plumbing that silently reverts
// is exactly how the guarantee would be lost again.
func TestARecallCommitsDurably(t *testing.T) {
	ctx := context.Background()
	scope := events.Scope{Tenant: "acme", Namespace: tenant.DefaultNamespace, Subject: id.New()}
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	for _, sync := range []bool{true, false} {
		t.Run(map[bool]string{true: "durable", false: "not"}[sync], func(t *testing.T) {
			spy := &syncSpy{KV: memkv.New()}
			t.Cleanup(func() { _ = spy.KV.Close() })

			wrote, err := lifecycle.Record(ctx, events.NewStore(spy), spy, scope,
				at, lifecycle.DefaultRecallWindow, sync)
			if err != nil {
				t.Fatalf("Record: %v", err)
			}
			if !wrote {
				t.Fatal("the recall was coalesced away on an empty stream")
			}
			if len(spy.syncs) != 1 {
				t.Fatalf("the recall made %d commits, want 1", len(spy.syncs))
			}
			if spy.syncs[0] != sync {
				t.Fatalf("the recall committed with sync=%v, want %v — a recall is exactly as "+
					"durable as a memory, and this is where that is decided", spy.syncs[0], sync)
			}
		})
	}
}

// And it is one commit, not a read-modify-write. That is the half of Rust's
// measured cost this design removes, so it is asserted rather than described.
func TestARecallIsOneAppendAndNoRecordWrite(t *testing.T) {
	ctx := context.Background()
	spy := &syncSpy{KV: memkv.New()}
	t.Cleanup(func() { _ = spy.KV.Close() })

	scope := events.Scope{Tenant: "acme", Namespace: tenant.DefaultNamespace, Subject: id.New()}
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	store := events.NewStore(spy)

	if _, err := lifecycle.Record(ctx, store, spy, scope, at, lifecycle.DefaultRecallWindow, true); err != nil {
		t.Fatalf("Record: %v", err)
	}
	// A second recall inside the window writes nothing at all — no commit, and
	// therefore no fsync. The window is a durability bound as well as a
	// counting rule.
	if _, err := lifecycle.Record(ctx, store, spy, scope,
		at.Add(time.Second), lifecycle.DefaultRecallWindow, true); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if len(spy.syncs) != 1 {
		t.Fatalf("two recalls inside the window made %d commits, want 1", len(spy.syncs))
	}

	// Exactly one row exists, and it is in the event space.
	rows := 0
	it := spy.NewIterator(nil, nil)
	for ok := it.First(); ok; ok = it.Next() {
		rows++
	}
	_ = it.Close()
	if rows != 1 {
		t.Fatalf("a recall wrote %d rows, want the one appended event", rows)
	}
}
