package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

// exitMigrationRequired is what `tenants inventory` returns when the directory
// holds tenants a single-tenant build cannot serve. It is the value
// exitInconsistent uses, for the reason run's comment gives: the command worked
// and found something an operator has to act on, which is neither a success nor
// a failure.
const exitMigrationRequired = 2

func tenantsCommand(args []string) (int, error) {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "inventory":
		return tenantsInventory(args)
	default:
		usage()
		return 0, fmt.Errorf("unknown tenants subcommand %q", sub)
	}
}

// tenantsInventory lists the tenants a data directory holds.
//
// It is the step before an upgrade to a build that serves one tenant: that
// build refuses to start over a directory holding more, and this is how an
// operator finds out what it will say and what to export, with the server still
// running the binary they have.
func tenantsInventory(args []string) (int, error) {
	fs := flag.NewFlagSet("tenants inventory", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to inspect")
	implicit := fs.String("implicit-tenant", config.DefaultTenant,
		"the tenant a single-tenant build serves, as tenant.default names it")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" {
		return 0, errors.New("tenants inventory needs --data-dir")
	}

	// Read-only, for the two reasons graph verify gives: it never writes, and a
	// mistyped path must be refused rather than become an empty database that
	// reports one tenant and reads as a clean upgrade.
	kv, err := pebble.Open(*dataDir, pebble.Options{ReadOnly: true})
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w\n"+
			"check the path, and note that a data directory in use by a running server may not be "+
			"readable: stop it, or point --data-dir at a backup checkpoint under .backups/", *dataDir, err)
	}
	defer func() { _ = kv.Close() }()

	return inventoryOver(context.Background(), tenantkv.New(kv, clock.System()),
		*dataDir, tenant.ID(*implicit), os.Stdout, os.Stderr)
}

// inventoryOver is the reporting half, over a directory rather than a path, so
// that the exit code and the message are testable without a Pebble directory.
func inventoryOver(ctx context.Context, dir tenant.Directory, dataDir string,
	implicit tenant.ID, out, errOut io.Writer,
) (int, error) {
	inv, err := tenant.TakeInventory(ctx, dir, implicit)
	if err != nil {
		return 0, err
	}
	metas, err := dir.List(ctx)
	if err != nil {
		return 0, err
	}

	fmt.Fprintf(out, "%s holds %d tenant(s); a single-tenant build serves %q\n",
		dataDir, len(metas), inv.Implicit)
	for _, m := range metas {
		mark := " "
		if m.ID == inv.Implicit {
			mark = "*"
		}
		name := m.DisplayName
		if name == "" {
			name = "(no display name)"
		}
		fmt.Fprintf(out, "  %s %-32s schema %d  created %s  %s\n",
			mark, m.ID, m.SchemaVersion, m.CreatedAt.UTC().Format("2006-01-02"), name)
	}

	if err := inv.MigrationRequired(); err != nil {
		fmt.Fprintln(errOut, "\nmigration required:", err)
		return exitMigrationRequired, nil
	}
	fmt.Fprintln(out, "\nthis directory is ready for a build that serves one tenant.")
	return 0, nil
}
