package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexes"
)

func vectorCommand(args []string) (int, error) {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "rebuild":
		return vectorRebuild(args)
	default:
		usage()
		return 0, fmt.Errorf("unknown vector subcommand %q", sub)
	}
}

// vectorRebuild rebuilds the approximate index from the canonical vectors.
//
// It is the offline form of a repair the running server now schedules for
// itself: a damaged index logs "the vector index needs rebuilding" *and*
// enqueues a durable `vector.rebuild` job (Phase 9). The search that discovered
// the damage still does not do the work, because a rebuild started there would
// charge one user for everyone's repair.
//
// This exists because a *stopped* server still needs a repair path, and a job
// queue is not reachable with the process down. With the server running,
// `POST /api/v1/admin/jobs/vector.rebuild/run` is the same thing without the
// data directory — which is the form to reach for, since this command needs
// Pebble's exclusive lock and therefore needs the server stopped.
//
// The rebuild reads the canonical vectors and writes only node records. It
// cannot lose a memory: the canonical rows are its input, and they are not
// among its outputs.
func vectorRebuild(args []string) (int, error) {
	fs := flag.NewFlagSet("vector rebuild", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to rebuild")
	only := fs.String("tenant", "", "rebuild one tenant rather than every tenant")
	metric := fs.String("metric", "cosine", "the distance metric the server is configured with")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" {
		return 0, errors.New("vector rebuild needs --data-dir")
	}

	// Read-write, unlike `graph verify`: this writes. A mistyped path would
	// therefore create an empty database, so the directory is checked for a
	// database first, read-only, and the check is the whole reason for opening
	// twice.
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

	dir, scoped, err := scope(kv, clock.System(), *only)
	if err != nil {
		return 0, err
	}
	only = &scoped

	index, err := indexes.Open(kv, indexes.Config{Kind: "hnsw", Metric: *metric})
	if err != nil {
		return 0, err
	}
	store := vector.NewStore(kv)

	ctx := context.Background()
	rebuild := func(t tenant.ID) error {
		started := time.Now()
		if err := index.Rebuild(ctx, t, store); err != nil {
			return fmt.Errorf("rebuilding tenant %s: %w", t, err)
		}
		s, err := index.Stats(ctx, t)
		if err != nil {
			return err
		}
		fmt.Printf("tenant %s: %d vectors rebuilt in %s\n", t, s.Vectors, time.Since(started).Round(time.Millisecond))
		return nil
	}

	if *only != "" {
		if err := rebuild(tenant.ID(*only)); err != nil {
			return 0, err
		}
		return 0, nil
	}

	// The cross-tenant walk goes through the tenant directory, which is the
	// single audited path across tenants (Invariant 1).
	seen := 0
	if err := dir.ForEach(ctx, func(t tenant.ID) error {
		seen++
		return rebuild(t)
	}); err != nil {
		return 0, err
	}
	if seen == 0 {
		fmt.Println("no tenants found")
	}
	return 0, nil
}
