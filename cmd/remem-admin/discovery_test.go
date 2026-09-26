package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

// A mistyped --data-dir must be refused rather than turned into a new empty
// database that reports "no tenants found" — which reads as a corpus with
// nothing to backfill. The Phase 6 finding, and this command writes, so it
// cannot simply open read-only.
func TestDiscoveryBackfillRefusesADirectoryWithNoDatabase(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "typo")
	if _, err := run([]string{"discovery", "backfill", "--data-dir", missing}); err == nil {
		t.Fatal("a path holding no database was accepted")
	}
	if _, err := pebble.Open(missing, pebble.Options{ReadOnly: true}); err == nil {
		t.Fatal("the refused path now holds a database: the command created one on its way out")
	}
}

func TestDiscoveryBackfillNeedsADataDir(t *testing.T) {
	if _, err := run([]string{"discovery", "backfill"}); err == nil {
		t.Fatal("discovery backfill ran without --data-dir")
	}
}

// The command an operator runs on a corpus written before discovery existed:
// every live memory ends up named by a queued job, and the server drains them.
func TestDiscoveryBackfillQueuesEveryMemory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	ctx := tenant.NewContext(context.Background(), "acme")

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	repo := record.NewRepo(kv)
	if _, err := tenant.Ensure(ctx, tenantkv.New(kv, clock.System()), "acme"); err != nil {
		t.Fatal(err)
	}
	want := map[id.ID]bool{}
	for range 5 {
		want[writeMemory(t, ctx, kv, repo, "a memory written before discovery existed", "old")] = true
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := run([]string{"discovery", "backfill", "--data-dir", dir}); err != nil {
		t.Fatalf("backfilling: %v", err)
	}

	// Reopened afterwards, because the command holds Pebble's exclusive lock
	// while it runs — which is also why this is an offline command.
	kv, err = pebble.Open(dir, pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()

	queued, err := jobs.NewQueue(kv, clock.System()).List(context.Background(), "acme",
		jobs.Filter{Types: []jobs.Type{discoveryJobType}, Limit: jobs.MaxListLimit})
	if err != nil {
		t.Fatal(err)
	}
	got := map[id.ID]bool{}
	for _, j := range queued {
		subjects, err := discovery.DecodePayload(j.Payload)
		if err != nil {
			t.Fatalf("a queued job's payload will not decode: %v", err)
		}
		for _, s := range subjects {
			got[s] = true
		}
	}
	if len(got) != len(want) {
		t.Fatalf("the backfill queued %d memories, want %d", len(got), len(want))
	}
	for rid := range want {
		if !got[rid] {
			t.Fatalf("memory %s was never queued", rid)
		}
	}
}
