package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
)

// exitInconsistent is what `graph verify` returns when the graph is broken but
// the command itself worked.
const exitInconsistent = 2

func graphCommand(args []string) (int, error) {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "verify":
		return graphVerify(args)
	default:
		usage()
		return 0, fmt.Errorf("unknown graph subcommand %q", sub)
	}
}

func graphVerify(args []string) (int, error) {
	fs := flag.NewFlagSet("graph verify", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to inspect")
	only := fs.String("tenant", "", "check one tenant rather than every tenant")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" {
		return 0, errors.New("graph verify needs --data-dir")
	}

	// Read-only, and that is two decisions rather than one. Verify never writes,
	// so the mode matches what it does — and read-only refuses a directory that
	// does not hold a database instead of creating one, which is what an
	// inspection tool must do with a mistyped path. Opening read-write would
	// answer "no tenants found" for a typo, which reads as a clean corpus.
	kv, err := pebble.Open(*dataDir, pebble.Options{ReadOnly: true})
	if err != nil {
		// The two common failures are a mistyped path and a running server, and
		// the error Pebble produces says neither.
		return 0, fmt.Errorf("opening %s: %w\n"+
			"check the path, and note that a data directory in use by a running server may not be "+
			"readable: stop it, or point --data-dir at a backup checkpoint under .backups/", *dataDir, err)
	}
	defer func() { _ = kv.Close() }()

	dir, scoped, err := scope(kv, clock.System(), *only)
	if err != nil {
		return 0, err
	}
	reports, err := verifyTenants(context.Background(), kv, dir, tenant.ID(scoped))
	if err != nil {
		return 0, err
	}

	broken := 0
	for _, rep := range reports {
		printReport(rep)
		broken += rep.Broken
	}
	if len(reports) == 0 {
		fmt.Println("no tenants found")
		return 0, nil
	}
	if broken == 0 {
		return 0, nil
	}
	fmt.Printf("\n%d problems found. Rebuild the reverse index to clear the repairable ones.\n", broken)
	return exitInconsistent, nil
}

// verifyTenants runs the checker over one tenant or every tenant.
//
// The cross-tenant walk is here rather than in internal/graph on purpose:
// tenant.Directory.ForEach is the single audited cross-tenant path
// (Invariant 1), and this command is one of the few callers the import guard
// allows to make it. graph.Verify itself is tenant-scoped and has no way to
// reach across.
// verifyTenants checks one tenant, or every tenant dir names.
//
// The directory is a parameter rather than built here, because which tenants
// this build may look at is a policy the command settles (see scope.go) and the
// walk below is the mechanism. Handing the mechanism the policy is what keeps
// the two from being re-decided in each of these commands.
func verifyTenants(ctx context.Context, kv storage.KV, dir tenant.Directory, only tenant.ID) ([]graph.Report, error) {
	scoped := func(t tenant.ID) (graph.Report, error) {
		return graph.Verify(ctx, kv, graph.Scope{Tenant: t, Namespace: tenant.DefaultNamespace})
	}
	if only != "" {
		rep, err := scoped(only)
		if err != nil {
			return nil, err
		}
		return []graph.Report{rep}, nil
	}

	var reports []graph.Report
	err := dir.ForEach(ctx, func(t tenant.ID) error {
		rep, err := scoped(t)
		if err != nil {
			return err
		}
		reports = append(reports, rep)
		return nil
	})
	return reports, err
}

func printReport(rep graph.Report) {
	fmt.Fprintf(os.Stdout, "tenant %s: %d edges, %d reverse entries, %d problems\n",
		rep.Tenant, rep.Edges, rep.Reverse, rep.Broken)
	for _, f := range rep.Findings {
		repair := "needs a decision"
		if f.Kind.Repairable() {
			repair = "repairable by rebuild"
		}
		fmt.Fprintf(os.Stdout, "  %s [%s]\n", f, repair)
	}
	if rep.Truncated {
		fmt.Fprintf(os.Stdout, "  ... %d more problems not listed\n", rep.Broken-len(rep.Findings))
	}
}
