package lifecycle

import (
	"context"
	"time"

	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/txn"
)

// RecallHealthBoost is the health a recall grants, clamped into [0, MaxHealth].
// services/recall.rs:24, applied at :67.
const RecallHealthBoost = 10.0

// DefaultRecallWindow is how close together two recalls of one memory have to
// be for the second to be free.
//
// It matches Rust's flush interval (config/remem-server.toml:100), which is
// what makes `access_count` count recall *sessions* rather than round trips
// (behaviour baseline §3): a client that fetches a memory and then updates it
// registers one recall, not two.
//
// The difference is where the window lives. Rust's is a process-local flush
// timer, so a restart resets it and two recalls either side of one count twice.
// Here it is decided against the stream — a recall is written only if the
// subject's newest recall is older than this — so it is a function of the data
// and survives a restart. TestTheCoalescingWindowSurvivesARestart is the half
// Rust cannot hold.
const DefaultRecallWindow = 30 * time.Second

// Record notes that a memory was addressed, and reports whether it wrote
// anything.
//
// # What counts as a recall
//
// Addressing a memory by id. Search does not: it discovers memories rather than
// addressing them, which is Rust's rule (behaviour baseline §3) and the one the
// differential harness compares against. Budgeted recall does not either, for
// the same reason — the caller did not name the memory, the ranking did.
//
// # It writes one key, and it does wait for the disk
//
// An earlier version of this did not, on the reasoning that an unsynced commit
// still reaches the write-ahead log and therefore survives the process dying.
// That reasoning was wrong, and Phase 10's end-to-end run disproved it: a recall
// recorded, `kill -9`, restart, and the event was gone. Pebble's unsynced write
// is applied to the memtable and buffered; nothing guarantees it has reached the
// operating system when the call returns.
//
// So the fsync is paid. What is *not* paid is the rest of what Rust measured
// (services/recall.rs:1-12): this is one key appended where nothing else can be
// writing, so it never reads first, never conflicts, and takes no global write
// lock — where Rust's recall was a full record rewrite inside one. The
// coalescing window bounds it to one write per memory per window, and Pebble
// group-commits concurrent syncs, so a busy server pays far fewer fsyncs than
// recalls.
//
// `sync` follows the store's own storage.sync_writes, so a recall is exactly as
// durable as a memory. An operator who has turned durability off has turned it
// off for everything, which is a decision they can reason about; a read path
// that quietly kept its own weaker setting is not.
func Record(ctx context.Context, store *events.Store, kv storage.KV, scope events.Scope,
	now time.Time, window time.Duration, sync bool) (bool, error) {
	if window <= 0 {
		window = DefaultRecallWindow
	}

	last, ok, err := store.NewestRecall(ctx, kv, scope)
	if err != nil {
		return false, err
	}
	if ok && now.Sub(last.At) < window {
		// Inside the window. A flag, not a counter: the recall happened and is
		// already represented by the row that is there.
		return false, nil
	}

	tx := txn.New(kv, txn.Sync(sync))
	defer tx.Close()

	if _, err := store.Append(ctx, tx, events.Event{
		Tenant:    scope.Tenant,
		Namespace: scope.Namespace,
		Subject:   scope.Subject,
		At:        now,
		Kind:      events.Recalled,
		Actor:     "api",
		Reason:    "the memory was fetched by id",
	}); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// Fold brings a record's recall counters up to date from the stream, and
// reports how many recalls it folded.
//
// # The watermark is last_recalled_at
//
// Every recall event strictly newer than it is unfolded. That works precisely
// because of the coalescing window: two recalls of one subject are at least one
// window apart, so no two can share a millisecond and be split by a strict
// comparison. A second durable cursor would be a second thing that can disagree
// with the first.
//
// # It is additive, and the timestamps only move forward
//
// The count is incremented from what the record currently holds rather than
// computed from a value read earlier, so a concurrent update cannot make it go
// backwards — Rust's rule at services/recall.rs:52-56, and the reason it
// survives the sweep losing a race.
//
// # It runs before the pass decides
//
// Expiry reads `access_count` to choose between promoting a memory and
// archiving it as unused, and active forgetting reads health. An unfolded
// recall is a memory somebody used that still looks untouched, so folding after
// the decision would arrive after the choice was made. Rust drains up front for
// the same reason and says so twice.
func Fold(ctx context.Context, store *events.Store, r events.Reader,
	scope events.Scope, rec *record.Record) (int, error) {
	n, newest, err := store.RecallsSince(ctx, r, scope, rec.Fields.LastRecalledAt)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}

	rec.Fields.AccessCount += uint32(n)
	if newest.After(rec.Fields.AccessedAt) {
		rec.Fields.AccessedAt = newest
	}
	if newest.After(rec.Fields.LastRecalledAt) {
		rec.Fields.LastRecalledAt = newest
	}

	health := float64(rec.Fields.Health) + RecallHealthBoost*float64(n)
	if health > float64(MaxHealth) {
		health = float64(MaxHealth)
	}
	if health < 0 {
		health = 0
	}
	rec.Fields.Health = float32(health)
	return n, nil
}
