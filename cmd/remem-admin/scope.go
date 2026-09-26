package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/version"
)

// The tenant scope every command operates in.
//
// remem-admin opens a data directory rather than talking to a server, so the
// server's resolver is not in the picture and nothing here has a request to
// resolve. What it does have is the same build tag and the same obligation:
// under tenant.SingleTenant a command must not rebuild, restore or check a
// tenant this build does not serve. Most of these commands *write*, and a
// rebuild of another tenant's index is the boundary failing quietly rather than
// loudly.
//
// One helper, called instead of tenantkv.New, is what keeps that from being
// eleven separate checks — the disposition the server's composition root has,
// for the same reason.
//
// # Export is deliberately the exception
//
// [migrationScope] lets `export` read a directory holding tenants this build
// does not serve, which [scope] refuses. That is not an oversight and it is not
// a hole in the boundary; it is the boundary's escape route.
//
// A deployment upgrading into a single-tenant build finds its server refusing to
// start, and the supported way out is one export per tenant into a deployment
// that resolves tenants from an identity. If the binary that refuses to start
// were also the binary that cannot export, the operator would be stranded
// mid-upgrade with their service down and the fix not in the box — and no data
// would be any safer for it, because the previous release's binary reads the
// same bytes. The capability being kept is offline, read-only, and available
// only to someone who already has the disk.
//
// What is *not* excepted: an export that names no tenant still writes only the
// implicit tenant, one that names another says out loud what it is for, and
// every command that writes refuses the directory exactly as the server does.

// implicitTenant is the tenant a single-tenant build serves, as this process can
// know it.
//
// It comes from REMEM_TENANT_DEFAULT — the same variable that sets
// tenant.default on the server — and falls back to the built-in default. A flag
// would be more explicit and would have to be threaded through eleven commands;
// the environment variable is the one an operator running against their own
// deployment's directory already has, and `tenants inventory` takes an explicit
// --implicit-tenant for the case where they do not.
func implicitTenant() (tenant.ID, error) {
	name := os.Getenv("REMEM_TENANT_DEFAULT")
	if name == "" {
		name = config.DefaultTenant
	}
	return tenant.Parse(name)
}

// singleTenantBuild reports whether this build serves one tenant, and which.
func singleTenantBuild() (tenant.ID, bool, error) {
	c, err := tenant.ParseCapability(version.TenantCapability)
	if err != nil {
		return "", false, err
	}
	if c != tenant.SingleTenant {
		return "", false, nil
	}
	implicit, err := implicitTenant()
	if err != nil {
		return "", false, err
	}
	return implicit, true, nil
}

// scope returns the tenant directory a command may walk and the --tenant it may
// narrow to, refusing a directory or a tenant this build does not serve.
//
// only is the command's --tenant flag, empty for "every tenant". Under
// tenant.SingleTenant "every tenant" is the implicit one, and any other name is
// refused rather than answered with an empty result: a rebuild that reported
// success over a tenant it never looked at is the worst of the available
// behaviours.
func scope(kv storage.KV, clk clock.Clock, only string) (tenant.Directory, string, error) {
	dir := tenantkv.New(kv, clk)

	implicit, single, err := singleTenantBuild()
	if err != nil || !single {
		return dir, only, err
	}
	if only != "" && only != string(implicit) {
		return nil, "", errs.E(errs.Forbidden, "remem-admin", fmt.Errorf(
			"this build serves one tenant, %q, so it will not operate on tenant %q. "+
				"Set REMEM_TENANT_DEFAULT if this directory's implicit tenant is a different one, or see "+
				"docs/MIGRATION.md for moving a tenant into a deployment that serves several",
			implicit, only))
	}

	// The directory the server refuses to start over is one this command refuses
	// too. Working quietly on a subset of it would hide exactly what the
	// inventory exists to surface.
	inv, err := tenant.TakeInventory(context.Background(), dir, implicit)
	if err != nil {
		return nil, "", err
	}
	if err := inv.MigrationRequired(); err != nil {
		return nil, "", err
	}
	return tenant.Confine(dir, implicit), string(implicit), nil
}

// migrationScope is scope for the two read-only commands that are the migration
// path itself: `export` and `tenants inventory`.
//
// It does not refuse a directory holding other tenants, because refusing it is
// what would strand an operator mid-upgrade. It still narrows an unqualified
// run to the implicit tenant, so an ordinary backup of a single-tenant
// deployment is a single-tenant snapshot, and it says out loud when a run is
// reaching past that — the note is addressed to whoever finds the file later.
func migrationScope(kv storage.KV, clk clock.Clock, only string, notes io.Writer) (tenant.Directory, string, error) {
	dir := tenantkv.New(kv, clk)

	implicit, single, err := singleTenantBuild()
	if err != nil || !single {
		return dir, only, err
	}
	if only == "" {
		return tenant.Confine(dir, implicit), string(implicit), nil
	}
	if only != string(implicit) {
		fmt.Fprintf(notes,
			"note: this build serves one tenant, %q, and this is an export of tenant %q.\n"+
				"      That is supported for one purpose: moving a tenant out of a directory this build\n"+
				"      will not serve, into a deployment that resolves tenants from an authenticated\n"+
				"      identity. docs/MIGRATION.md has the procedure. Nothing here is modified.\n",
			implicit, only)
	}
	return dir, only, nil
}

// refuseForeignTenants refuses a snapshot carrying tenant data this build does
// not serve.
//
// A restore is the one way a tenant can arrive on disk without anything asking
// the resolver, so it is the one place the boundary has to be checked against a
// file rather than a request. The whole file is refused rather than the extra
// tenants filtered out: importing a subset would leave an operator believing
// they had restored their backup, and merging them into the implicit tenant
// would silently join two corpora that were never one.
func refuseForeignTenants(carried []string) error {
	implicit, single, err := singleTenantBuild()
	if err != nil || !single {
		return err
	}
	var foreign []string
	for _, t := range carried {
		if t != string(implicit) {
			foreign = append(foreign, t)
		}
	}
	if len(foreign) == 0 {
		return nil
	}
	return errs.E(errs.Forbidden, "remem-admin.import", fmt.Errorf(
		"this snapshot carries %d tenant(s) this build does not serve (%s), and it serves only %q. "+
			"Nothing has been imported and no database was created. Restore it into a deployment that "+
			"resolves tenants from an authenticated identity, or export the one tenant you want with "+
			"`remem-admin export --tenant %s`; docs/MIGRATION.md has the procedure",
		len(foreign), strings.Join(foreign, ", "), implicit, implicit))
}
