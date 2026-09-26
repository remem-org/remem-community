package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Credential is one configured API key.
type Credential struct {
	// ID names the credential in logs. It is not the secret.
	ID string
	// Secret is the bearer token the caller presents.
	Secret string `secret:"true"`
	// Tenant binds the credential to one tenant. Empty leaves it unbound.
	Tenant tenant.ID
	// CrossTenant authorises naming a tenant per request. Only valid on an
	// unbound credential: a credential that is both bound and cross-tenant is
	// a contradiction, and guessing which half was meant is how isolation
	// bugs are configured into existence.
	CrossTenant bool
	// Authorized is the explicit set of tenants a cross-tenant credential may
	// act in. Empty leaves its authority unnarrowed, which is the single
	// operator credential.
	//
	// It exists so that a cross-tenant operation acts on a scope configuration
	// stated rather than on one inferred from the absence of a binding. A
	// credential authorised for two tenants is refused on a third, and a
	// cross-tenant listing shows it those two.
	Authorized []tenant.ID
}

// Verifier turns a presented credential into a principal.
type Verifier interface {
	Verify(ctx context.Context, credential string) (Principal, error)
}

// Keyring is the static credential set from configuration.
//
// Plan Task 3.1 names a package-level auth.Verify. This is that function with
// its configuration made explicit rather than held in a package variable: a
// process-wide mutable credential set would be state no configuration file
// describes, which spec §51 rules out, and two servers in one test binary
// would fight over it.
type Keyring struct {
	// byHash is keyed by the SHA-256 of the secret rather than the secret, so
	// a heap dump of a running server does not hand over every API key.
	byHash map[[32]byte]Principal
	// order holds the hashes in configuration order, so verification of an
	// unknown credential still compares against every entry rather than
	// short-circuiting on a map miss.
	order [][32]byte
}

var _ Verifier = (*Keyring)(nil)

// NewKeyring builds a keyring, refusing a credential set it cannot serve
// unambiguously.
func NewKeyring(creds []Credential) (*Keyring, error) {
	const op = "auth.NewKeyring"

	k := &Keyring{byHash: make(map[[32]byte]Principal, len(creds))}
	for i, c := range creds {
		where := c.ID
		if where == "" {
			where = fmt.Sprintf("credential %d", i)
		}
		if c.Secret == "" {
			return nil, errs.E(errs.Invalid, op, fmt.Errorf("%s has no secret", where))
		}
		if c.Tenant != "" {
			if _, err := tenant.Parse(string(c.Tenant)); err != nil {
				return nil, errs.E(errs.Invalid, op, fmt.Errorf("%s is bound to %q: %w", where, c.Tenant, err))
			}
			if c.CrossTenant {
				return nil, errs.E(errs.Invalid, op, fmt.Errorf(
					"%s is bound to tenant %q and also marked cross-tenant; a credential is one or the other",
					where, c.Tenant))
			}
			if len(c.Authorized) > 0 {
				return nil, errs.E(errs.Invalid, op, fmt.Errorf(
					"%s is bound to tenant %q and also authorised for a set of tenants; a binding is already a set of one",
					where, c.Tenant))
			}
		}
		for _, a := range c.Authorized {
			if _, err := tenant.Parse(string(a)); err != nil {
				return nil, errs.E(errs.Invalid, op, fmt.Errorf("%s is authorised for %q: %w", where, a, err))
			}
		}
		if len(c.Authorized) > 0 && !c.CrossTenant {
			// An authorised set on a credential that may not name a tenant
			// describes an authority it cannot exercise. Accepting it would
			// read as "this key can reach these two tenants", which is the
			// opposite of what it would do.
			return nil, errs.E(errs.Invalid, op, fmt.Errorf(
				"%s is authorised for a set of tenants but not marked cross-tenant, so it could never name one of them",
				where))
		}

		h := sha256.Sum256([]byte(c.Secret))
		if _, dup := k.byHash[h]; dup {
			return nil, errs.E(errs.Invalid, op, fmt.Errorf(
				"%s reuses a secret another credential already uses; which tenant a request belongs to would depend on map order", where))
		}
		id := c.ID
		if id == "" {
			id = fmt.Sprintf("credential-%d", i)
		}
		k.byHash[h] = Principal{ID: id, Tenant: c.Tenant, CrossTenant: c.CrossTenant, Authorized: c.Authorized}
		k.order = append(k.order, h)
	}
	return k, nil
}

// Anonymous reports whether the keyring holds no credentials — the
// unauthenticated development case.
func (k *Keyring) Anonymous() bool { return len(k.order) == 0 }

// Verify returns the principal behind a presented credential.
//
// An empty keyring admits [Anonymous]. Configuration is what forbids that in
// production (config refuses a production server with no api_key); this
// package does not second-guess it, because a server that refused every
// request while reporting a valid configuration would be worse.
//
// The comparison is constant-time over every configured credential, and does
// not short-circuit on the first match: response time must not reveal how many
// credentials exist or how far down the list one sits.
func (k *Keyring) Verify(_ context.Context, credential string) (Principal, error) {
	const op = "auth.Verify"

	if k.Anonymous() {
		return Anonymous, nil
	}

	presented := sha256.Sum256([]byte(credential))
	var found Principal
	matched := 0
	for _, h := range k.order {
		if subtle.ConstantTimeCompare(h[:], presented[:]) == 1 {
			found = k.byHash[h]
			matched++
		}
	}
	if matched == 0 || credential == "" {
		return Principal{}, errs.E(errs.Unauthorized, op, errors.New("the presented credential is not one this server issued"))
	}
	return found, nil
}
