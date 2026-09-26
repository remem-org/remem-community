package tenant

import (
	"context"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
)

// Confine narrows a directory to one tenant.
//
// It is how [SingleTenant] is enforced everywhere that does not go through
// [Resolver]. The resolver settles which tenant a *request* is for, and that
// covers HTTP and MCP; it covers nothing else. Background jobs are scheduled by
// walking the directory, metrics label every tenant the directory lists, a
// snapshot exports every tenant the directory names, the session sweep visits
// each of them, and a retention-policy route exists for a tenant the directory
// can produce. Each of those is a cross-tenant path with no request behind it,
// and each of them asks the directory.
//
// So the directory is where the confinement goes. The composition root wraps
// the real one once and hands the wrapper to everything, and the alternative —
// an `if capability == SingleTenant` beside every one of those call sites — is
// the shape design.md rejected: one missed job, metric or backup is a
// cross-tenant path, and nothing fails when it is missed.
//
// A tenant other than the confined one reads as absent rather than forbidden.
// That is the disposition a memory in another tenant already has, and the
// reason is the same: whether some other tenant exists is not information this
// deployment has any business confirming. Creating one is [errs.Forbidden]
// instead, because a caller asking for it has asked for something this build
// will not do, and "not found" would read as a bug in the request.
func Confine(d Directory, only ID) Directory {
	return confined{inner: d, only: only}
}

type confined struct {
	inner Directory
	only  ID
}

var _ Directory = confined{}

func (c confined) Create(ctx context.Context, id ID, m Meta) error {
	if id != c.only {
		return errs.E(errs.Forbidden, "tenant.Directory.Create", fmt.Errorf(
			"this build serves one tenant, %q, so tenant %q cannot be created", c.only, id))
	}
	return c.inner.Create(ctx, id, m)
}

func (c confined) Get(ctx context.Context, id ID) (Meta, error) {
	if id != c.only {
		return Meta{}, errs.E(errs.NotFound, "tenant.Directory.Get", fmt.Errorf("no tenant %q", id))
	}
	return c.inner.Get(ctx, id)
}

func (c confined) List(ctx context.Context) ([]Meta, error) {
	metas, err := c.inner.List(ctx)
	if err != nil {
		return nil, err
	}
	// Filtered from the real listing rather than synthesised, so a directory
	// that does not hold the implicit tenant yet lists nothing: "one tenant"
	// and "one tenant that exists" are different answers, and a caller reading
	// a synthesised row would believe the second.
	var out []Meta
	for _, m := range metas {
		if m.ID == c.only {
			out = append(out, m)
		}
	}
	return out, nil
}

func (c confined) ForEach(ctx context.Context, fn func(ID) error) error {
	return c.inner.ForEach(ctx, func(id ID) error {
		if id != c.only {
			return nil
		}
		return fn(id)
	})
}
