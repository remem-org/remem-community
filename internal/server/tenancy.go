package server

import (
	"context"
	"errors"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

// The tenant policy, as the composition root settles it.
//
// Two things happen here and nowhere else. The capability is chosen — from the
// build's own declaration, or from the option a composition root with an
// identity layer passes — and the tenant directory is narrowed to what that
// capability serves. Everything downstream reads the directory it is given and
// has no edition check of its own, which is the shape design.md chose over an
// `if capability == SingleTenant` beside every job, metric, backup and route:
// one missed call site is a cross-tenant path, and nothing fails when it is
// missed.

// scopeDirectory narrows the tenant directory to what this build's capability
// serves, refusing a directory it cannot serve all of.
//
// The refusal happens here — before the migrations, before the indexes, before
// anything listens or any worker starts — for two reasons. A single-tenant build
// must not migrate rows it is then going to refuse to serve, because a
// successful migration followed by a start that ignores the data is the shape
// the whole inventory exists to prevent. And nothing has been written at this
// point, so the directory is exactly as the operator left it and rollback is
// the prior binary over it.
func scopeDirectory(cfg config.Config, capability tenant.Capability,
	dir *tenantkv.Directory,
) (tenant.Directory, error) {
	switch capability {
	case tenant.SingleTenant:
		// Below.
	case tenant.IdentityScoped:
		return dir, nil
	default:
		// Fail closed, the whole way down. New settles the capability before
		// calling build, so reaching this means a caller composed the
		// dependencies without a policy — and the directory is the one thing
		// that must not be handed out unconfined by default.
		return nil, errs.E(errs.Invalid, "server.build", errors.New(
			"these dependencies were built without a tenant capability; the composition root selects one"))
	}
	implicit, err := tenant.Parse(cfg.Tenant.Default)
	if err != nil {
		return nil, err
	}
	inv, err := tenant.TakeInventory(context.Background(), dir, implicit)
	if err != nil {
		return nil, err
	}
	if err := inv.MigrationRequired(); err != nil {
		return nil, err
	}
	return tenant.Confine(dir, implicit), nil
}
