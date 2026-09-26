package server

import (
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Option overrides a dependency the server would otherwise build itself.
//
// It is deliberately narrow: a composition root whose every part is injectable
// is not a composition root, it is a second configuration system. Two of the
// three exist for tests, and only for the pieces a test genuinely cannot use in
// their real form — the model, which is 90 MB of weights, and the wall clock.
//
// The third is not a test convenience. [WithTenantCapability] is how a
// composition root that carries an identity and authorization layer states the
// policy it can enforce; the build tag is only the default for a build that
// carries none. Selecting it explicitly is the point: a capability inferred
// from a release target would advertise isolation the binary has no way to
// enforce.
type Option func(*options)

type options struct {
	embedder   embedding.Embedder
	clk        clock.Clock
	capability tenant.Capability
}

// WithEmbedder supplies an embedder instead of loading the model.
func WithEmbedder(e embedding.Embedder) Option { return func(o *options) { o.embedder = e } }

// WithClock supplies a clock instead of the wall clock.
func WithClock(c clock.Clock) Option { return func(o *options) { o.clk = c } }

// WithTenantCapability selects the tenant policy, overriding the one this build
// declares in version.TenantCapability.
//
// A deployment that resolves tenants from an authenticated identity passes
// tenant.IdentityScoped here, and is responsible for having an identity layer
// in front of it. Everything else gets the build's own declaration, which for
// every officially supported build today is tenant.SingleTenant.
func WithTenantCapability(c tenant.Capability) Option {
	return func(o *options) { o.capability = c }
}
