// Package tenant is Remem's isolation identity.
//
// A tenant is the boundary that Invariant 1 is about — there is no unscoped
// read path — and the boundary along which the keyspace is split when Phase 14
// distributes it. A namespace is a subdivision inside one tenant: what Rust
// Remem called a partition, and what a product would call a project, workspace
// or environment.
//
// Only the identity types live here in Phase 2, because that is all the key
// encoding needs and the key encoding is a durable format that has to be
// settled first. The directory, the resolver and provisioning arrive in Phase
// 3 (plan Task 3.1) on top of these types.
//
// # Why namespace exists before the feature does
//
// Nothing in the product exposes namespaces yet, and [DefaultNamespace] is the
// only value in use. It is in the key encoding anyway because the alternative
// is worse in a specific way: retrofitting it means rewriting every key every
// customer owns, and the plan (§II.5) records the failure that actually
// follows from that — under delivery pressure the namespace model gets built
// as a post-retrieval filter, which is precisely the mistake tenant
// partitioning exists to prevent.
package tenant

import (
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
)

// ID identifies a tenant. It is a distinct type from [Namespace] so that
// passing one where the other belongs cannot compile.
type ID string

// Namespace identifies a subdivision within a tenant.
type Namespace string

// DefaultNamespace is the namespace every record lives in until a product
// decision introduces more. It is a real value in every key, not a special
// case: there is no such thing as a record outside a namespace.
const DefaultNamespace Namespace = "default"

// MaxIDLen bounds an identifier. The limit exists because a tenant id is a
// length-prefixed component of every key that tenant owns, so an unbounded one
// is unbounded storage overhead on every row.
const MaxIDLen = 128

// Parse validates and returns a tenant id.
func Parse(s string) (ID, error) {
	if err := validate(s, "tenant id"); err != nil {
		return "", errs.E(errs.Invalid, "tenant.Parse", err)
	}
	return ID(s), nil
}

// ParseNamespace validates and returns a namespace id.
func ParseNamespace(s string) (Namespace, error) {
	if err := validate(s, "namespace"); err != nil {
		return "", errs.E(errs.Invalid, "tenant.ParseNamespace", err)
	}
	return Namespace(s), nil
}

func (t ID) String() string        { return string(t) }
func (n Namespace) String() string { return string(n) }

// Valid reports whether the id is well formed. It is the predicate behind
// [Parse], exposed so a key builder can refuse a bad id without allocating an
// error it is about to discard.
func (t ID) Valid() bool { return validate(string(t), "") == nil }

// Valid reports whether the namespace is well formed.
func (n Namespace) Valid() bool { return validate(string(n), "") == nil }

// validate holds the one definition of a well-formed identifier.
//
// The character set is deliberately narrow — lowercase letters, digits,
// underscore and hyphen. An identifier appears in keys, in metric labels, in
// log fields and in URLs, and every character that needs escaping in one of
// those is a character that eventually gets escaped inconsistently in another.
// Case is excluded for the same reason: "Acme" and "acme" as different tenants
// is a support incident waiting to happen.
func validate(s, what string) error {
	if len(s) == 0 {
		return fmt.Errorf("%s must not be empty", what)
	}
	if len(s) > MaxIDLen {
		return fmt.Errorf("%s is %d bytes, the limit is %d", what, len(s), MaxIDLen)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return fmt.Errorf("%s %q contains %q; only [a-z0-9_-] is allowed", what, s, string(c))
		}
	}
	return nil
}
