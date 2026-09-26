package tenant

import (
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
)

// Capability is an edition's tenant policy: how many tenants a build serves,
// and where the answer to "which one is this request" comes from.
//
// It is a policy and not a feature flag. The engine below it is tenant-aware in
// every edition — the durable key model, the protocols and the portable
// snapshot all carry a tenant identity whatever this says — so a corpus written
// by one capability is readable by the other and a migration between them moves
// rows rather than rewriting them. What the capability decides is only which
// requests a build will answer.
//
// The zero value is [NoCapability], which resolves nothing. That is deliberate:
// a resolver built without a policy must refuse rather than pick one, because
// the two policies differ precisely in what they refuse and defaulting to
// either would be a silent choice about isolation.
type Capability uint8

const (
	// NoCapability is the zero value: no policy was selected, and nothing
	// resolves. A composition root that reaches it has forgotten to choose.
	NoCapability Capability = iota

	// SingleTenant serves one implicit tenant and refuses every
	// caller-supplied tenant selector.
	//
	// This is what the officially supported Community and Business builds are.
	// It is a statement about the capabilities those builds ship, not a
	// cryptographic boundary: the Community source is Apache-2.0 and a
	// recipient may rebuild it however they like. What it buys the operator is
	// that no request, job, metric, backup or restore in such a build can name
	// a second tenant, so a single-tenant deployment cannot be talked into
	// becoming a leaky multi-tenant one by configuration.
	SingleTenant

	// IdentityScoped derives each request's tenant from the authenticated
	// identity and its authorization, which is the Enterprise/Cloud policy.
	//
	// A credential bound to one tenant stays bound to it. A credential
	// authorized across an explicit set may select within that set and no
	// further. A selector is input to that decision and never authority on its
	// own.
	IdentityScoped
)

var capabilityNames = [...]string{
	NoCapability:   "none",
	SingleTenant:   "single-tenant",
	IdentityScoped: "identity",
}

// String returns the stable name of the capability. It appears in logs, in
// `GET /api/v1/health` and in release metadata, so it does not change once
// shipped.
func (c Capability) String() string {
	if int(c) < len(capabilityNames) && capabilityNames[c] != "" {
		return capabilityNames[c]
	}
	return fmt.Sprintf("capability(%d)", uint8(c))
}

// ParseCapability turns a build's declared capability name into the policy.
//
// The name arrives as a string because the build tag that decides it lives in
// internal/version, which sits below this package and must not import it. The
// composition root joins the two, which is the one place allowed to know how
// the parts fit together.
func ParseCapability(name string) (Capability, error) {
	for c, n := range capabilityNames {
		if n == name && Capability(c) != NoCapability {
			return Capability(c), nil
		}
	}
	return NoCapability, errs.E(errs.Invalid, "tenant.ParseCapability", fmt.Errorf(
		"%q is not a tenant capability: this build declares one of %q or %q",
		name, SingleTenant, IdentityScoped))
}
