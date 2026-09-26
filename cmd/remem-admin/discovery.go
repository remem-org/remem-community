package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
)

// discoveryJobType is the durable name the server registers the handler under.
//
// It is spelled here rather than imported from internal/server, because this
// command must not drag the whole composition root — an embedder, a listener, a
// migration runner — into a binary that opens a data directory and writes job
// rows. The name is durable, so the duplication is a constant rather than a
// coupling, and a mismatch would show up immediately: the server would refuse
// the type as unregistered.
const discoveryJobType jobs.Type = "discovery.similar"

func discoveryCommand(args []string) (int, error) {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "backfill":
		return discoveryBackfill(args)
	default:
		usage()
		return 0, fmt.Errorf("unknown discovery subcommand %q", sub)
	}
}

// discoveryBackfill queues relationship discovery for every live memory.
//
// # What it is for
//
// Discovery is enqueued by the write that creates a memory, so a corpus written
// before Phase 11 — or while `discovery.enabled` was off, or during an outage
// that exhausted a job's attempts — holds memories that were never considered.
// Nothing notices on its own: there is deliberately no "was this discovered"
// flag to sweep on, because that flag would be a second durable fact about
// every memory that discovery would then have to keep true.
//
// # It queues work rather than doing it
//
// The rows it writes are ordinary discovery jobs, so the server drains them at
// worker speed when it next starts, with the same leases, retries and
// checkpoints everything else gets. That is also why this can run against a
// stopped server and is the only thing it can do there: the corpus needs
// embeddings compared, and comparing them is the running server's job.
//
// Running it twice is safe and is still work. The second pass over an unchanged
// memory finds its neighbours already linked and writes no edge — but it does
// pay for the vector search that established that.
func discoveryBackfill(args []string) (int, error) {
	fs := flag.NewFlagSet("discovery backfill", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to backfill")
	only := fs.String("tenant", "", "backfill one tenant rather than every tenant")
	perJob := fs.Int("per-job", discovery.DefaultSubjectsPerJob,
		"how many memories one queued job carries")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" {
		return 0, errors.New("discovery backfill needs --data-dir")
	}

	// Read-only first, then read-write. This command writes, so a mistyped path
	// would otherwise create an empty database and report a clean corpus — the
	// defect Phase 6 found in `graph verify` and every command here has carried
	// the fix for since.
	probe, err := pebble.Open(*dataDir, pebble.Options{ReadOnly: true})
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w\n"+
			"check the path, and note that a data directory in use by a running server may not be "+
			"readable: stop it first", *dataDir, err)
	}
	_ = probe.Close()

	kv, err := pebble.Open(*dataDir, pebble.Options{})
	if err != nil {
		return 0, fmt.Errorf("opening %s for writing: %w", *dataDir, err)
	}
	defer func() { _ = kv.Close() }()

	clk := clock.System()
	dir, scoped, err := scope(kv, clk, *only)
	if err != nil {
		return 0, err
	}
	only = &scoped

	ctx := context.Background()
	repo := record.NewRepo(kv)
	enq := discovery.NewEnqueuer(jobs.NewQueue(kv, clk), discoveryJobType)

	backfill := func(t tenant.ID) error {
		started := time.Now()
		stats, err := discovery.Backfill(ctx, kv, repo, enq,
			discovery.Scope{Tenant: t, Namespace: tenant.DefaultNamespace}, *perJob)
		if err != nil {
			return fmt.Errorf("backfilling tenant %s: %w", t, err)
		}
		fmt.Printf("tenant %s: %d memories queued in %d jobs (%d archived, skipped) in %s\n",
			t, stats.Memories, stats.Jobs, stats.Archived,
			time.Since(started).Round(time.Millisecond))
		return nil
	}

	if *only != "" {
		if err := backfill(tenant.ID(*only)); err != nil {
			return 0, err
		}
		return 0, nil
	}

	// The cross-tenant walk goes through the tenant directory, which is the
	// single audited path across tenants (Invariant 1).
	seen := 0
	if err := dir.ForEach(ctx, func(t tenant.ID) error {
		seen++
		return backfill(t)
	}); err != nil {
		return 0, err
	}
	if seen == 0 {
		fmt.Println("no tenants found")
	}
	return 0, nil
}
