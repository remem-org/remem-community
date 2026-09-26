//go:build !enterprise

package version

// TenantCapability is the tenant policy this build supports, decided by the
// `enterprise` build tag and named by internal/tenant's Capability.
//
// It is a separate tag from `business` on purpose. The business edition
// packages observability around the same single-node engine; it does not
// package an identity and authorization layer, and a release target must be
// mapped to a capability only when the build actually contains what the
// capability requires. Equating the two would advertise tenant isolation this
// binary has no way to enforce.
//
// The name is a string rather than a tenant.Capability because internal/tenant
// sits above this package: tenant.ParseCapability turns it into the policy, and
// the composition root is what joins them.
const TenantCapability = "single-tenant"
