package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
)

func textCommand(args []string) (int, error) {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "rebuild":
		return textRebuild(args)
	default:
		usage()
		return 0, fmt.Errorf("unknown text subcommand %q", sub)
	}
}

// textRebuild rebuilds the inverted index from the canonical record bodies.
//
// It exists for two situations. A server that finds the index marked
// mid-rebuild both logs it and enqueues a durable `text.rebuild` job (Phase 9),
// so this is the offline form of a repair a running server now performs for
// itself — needed because a *stopped* server still needs a repair path, and a
// job queue is not reachable with the process down. The search that discovered
// the damage still does not do the work: that would charge one user for
// everyone's repair.
//
// The second situation is the one no job covers: an upgrade that changes how
// text is split into terms makes every posting on disk describe the old rules,
// and this is what brings the corpus forward.
//
// It cannot lose a memory. Its input is the record bodies and its only outputs
// are rows in the text key space, so an interrupted run leaves a corpus that is
// harder to search and has lost nothing. The interruption is visible: the
// rebuild marker stays set, so every keyword search reports itself incomplete
// until the command is run again.
func textRebuild(args []string) (int, error) {
	fs := flag.NewFlagSet("text rebuild", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to rebuild")
	only := fs.String("tenant", "", "rebuild one tenant rather than every tenant")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" {
		return 0, errors.New("text rebuild needs --data-dir")
	}

	// Read-only first, then read-write: this command writes, so a mistyped path
	// would otherwise create an empty database and report a clean corpus. The
	// probe is the whole reason for opening twice — the same shape `vector
	// rebuild` uses, and the defect Phase 6 found in `graph verify`.
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

	ctx := context.Background()
	index := text.New()

	rebuild := func(t tenant.ID) error {
		started := time.Now()
		ns := tenant.DefaultNamespace
		if err := index.Rebuild(ctx, kv, t, ns, record.NewContentSource(kv)); err != nil {
			return fmt.Errorf("rebuilding tenant %s: %w", t, err)
		}
		stats, err := text.ReadStats(ctx, kv, t, ns)
		if err != nil {
			return err
		}
		fmt.Printf("tenant %s: %d memories indexed in %s\n",
			t, stats.Documents, time.Since(started).Round(time.Millisecond))
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
