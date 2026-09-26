package tenant

import (
	"context"
	"fmt"
	"strings"

	"github.com/remem-org/remem-go/internal/errs"
)

// Inventory is a data directory's tenant register, split by whether each
// tenant is the implicit one a single-tenant edition serves.
//
// It exists because a single-tenant edition cannot simply start over a
// directory that holds more than one tenant. Resolving every request to the
// implicit tenant would leave the other tenants' records on disk, unreachable
// and unmentioned — a successful start that looks like a successful upgrade.
// The inventory is what turns that into a refusal an operator can act on, and
// it is deliberately read-only: taking it must never be the thing that creates
// the implicit tenant.
type Inventory struct {
	// Implicit is the tenant the inventory was taken against.
	Implicit ID
	// HasImplicit reports whether the implicit tenant is registered. A fresh
	// directory has none, which is not a migration problem.
	HasImplicit bool
	// Others are the registered tenants that are not the implicit one, in id
	// order.
	Others []ID
}

// TakeInventory reads the tenant register and reports what it holds.
//
// It reads and never writes: [Ensure] is the provisioning path and this is not
// it. A caller that wants the implicit tenant to exist provisions it after the
// inventory has been taken and accepted, so that the refusal below describes
// the directory as the operator left it.
func TakeInventory(ctx context.Context, d Directory, implicit ID) (Inventory, error) {
	const op = "tenant.TakeInventory"

	if _, err := Parse(string(implicit)); err != nil {
		return Inventory{}, errs.E(errs.Invalid, op,
			fmt.Errorf("the implicit tenant %q is not a tenant id: %w", implicit, err))
	}
	metas, err := d.List(ctx)
	if err != nil {
		return Inventory{}, err
	}
	inv := Inventory{Implicit: implicit}
	for _, m := range metas {
		if m.ID == implicit {
			inv.HasImplicit = true
			continue
		}
		inv.Others = append(inv.Others, m.ID)
	}
	return inv, nil
}

// SingleTenant reports whether the directory holds nothing a single-tenant
// edition would have to leave behind.
func (inv Inventory) SingleTenant() bool { return len(inv.Others) == 0 }

// MigrationRequired is the refusal a single-tenant edition owes an operator
// whose directory holds more than one tenant, or nil when it holds one.
//
// The message names every tenant it found rather than counting them: the
// operator's next step is one export per tenant, and a count does not tell
// them what to type. It is errs.MigrationRequired rather than Invalid because
// that is exactly what it is — the directory is intact and this binary cannot
// serve all of it — and because the format refusal an operator has already
// seen from a newer directory reads the same way.
func (inv Inventory) MigrationRequired() error {
	if inv.SingleTenant() {
		return nil
	}
	names := make([]string, len(inv.Others))
	for i, t := range inv.Others {
		names[i] = string(t)
	}
	return errs.E(errs.MigrationRequired, "tenant.Inventory", fmt.Errorf(
		"this data directory holds %d tenants beyond the implicit tenant %q (%s), and this build serves one. "+
			"Nothing has been changed. Export each of them with `remem-admin export --data-dir <dir> --tenant <id> "+
			"--out <id>.rsnap`, import each snapshot into a deployment that resolves tenants from an authenticated "+
			"identity, and keep this directory as it is for rollback. docs/MIGRATION.md has the procedure",
		len(inv.Others), inv.Implicit, strings.Join(names, ", ")))
}
