package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

func inventoryDirectory(t *testing.T, ids ...tenant.ID) tenant.Directory {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	dir := tenantkv.New(kv, clock.System())
	for _, id := range ids {
		if err := dir.Create(context.Background(), id, tenant.Meta{}); err != nil {
			t.Fatalf("creating tenant %s: %v", id, err)
		}
	}
	return dir
}

// A directory a single-tenant build can serve exits zero and says so. "The
// command worked and found something to act on" is exit 2, the value graph
// verify already uses for the same reason.
func TestInventoryExitsZeroOnADirectoryASingleTenantBuildCanServe(t *testing.T) {
	for _, ids := range [][]tenant.ID{nil, {"default"}} {
		var out, errOut bytes.Buffer
		code, err := inventoryOver(context.Background(), inventoryDirectory(t, ids...),
			"./data", "default", &out, &errOut)
		if err != nil {
			t.Fatalf("inventory over %v: %v", ids, err)
		}
		if code != 0 {
			t.Fatalf("a directory holding %v exited %d, want 0: %s", ids, code, errOut.String())
		}
		if !strings.Contains(out.String(), "ready for a build that serves one tenant") {
			t.Fatalf("the report does not say the directory is ready:\n%s", out.String())
		}
	}
}

func TestInventoryExitsTwoAndNamesEveryOtherTenant(t *testing.T) {
	var out, errOut bytes.Buffer
	code, err := inventoryOver(context.Background(),
		inventoryDirectory(t, "default", "acme", "globex"), "./data", "default", &out, &errOut)
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if code != exitMigrationRequired {
		t.Fatalf("exit code %d, want %d on a directory holding three tenants", code, exitMigrationRequired)
	}
	for _, want := range []string{"acme", "globex", "remem-admin export"} {
		if !strings.Contains(errOut.String(), want) {
			t.Fatalf("the refusal does not mention %q:\n%s", want, errOut.String())
		}
	}
	// The listing marks the implicit tenant, so an operator can see which of
	// the rows is the one that stays.
	if !strings.Contains(out.String(), "* default") {
		t.Fatalf("the listing does not mark the implicit tenant:\n%s", out.String())
	}
}

// Read-only, for the reason TestVerifyRefusesADirectoryThatHoldsNoDatabase
// gives: a mistyped path that became an empty database would report one tenant,
// which reads as "this directory is ready".
func TestInventoryRefusesADirectoryThatHoldsNoDatabase(t *testing.T) {
	dir := t.TempDir() + "/not-a-data-directory"
	if _, err := tenantsInventory([]string{"--data-dir", dir}); err == nil {
		t.Fatal("a path with no database was accepted")
	}
	if _, statErr := os.Stat(dir); statErr == nil {
		t.Fatal("the failed open created the directory; an inspection tool must not")
	}
}

func TestInventoryNeedsADataDirectory(t *testing.T) {
	if _, err := tenantsInventory(nil); err == nil {
		t.Fatal("tenants inventory ran without --data-dir")
	}
}

func TestUnknownTenantsSubcommandIsRefusedByName(t *testing.T) {
	if _, err := tenantsCommand([]string{"merge"}); err == nil {
		t.Fatal("an unknown tenants subcommand was accepted")
	}
}
