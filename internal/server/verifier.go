package server

import (
	"context"
	"strings"

	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/users"
)

// verifier is the one credential check every request passes: stored user
// tokens through the user service, everything else through the configured
// keyring.
//
// The two are composed here rather than in internal/auth so that auth, which
// every surface reads principals from, never depends on the package that
// stores tokens (a boundary internal/arch holds).
//
// The prefix decides the path, and a credential with it never reaches the
// keyring: configuration refuses a configured secret with the prefix, so a
// token that fails here could not have been a configured key, and trying the
// keyring as well would only make a refusal cost two lookups instead of one.
type verifier struct {
	keyring *auth.Keyring
	users   *users.Service
}

var _ auth.Verifier = verifier{}

func (v verifier) Verify(ctx context.Context, credential string) (auth.Principal, error) {
	if !strings.HasPrefix(credential, users.TokenPrefix) {
		return v.keyring.Verify(ctx, credential)
	}
	who, err := v.users.Verify(ctx, credential)
	if err != nil {
		return auth.Principal{}, err
	}
	// A user token is bound to its user's tenant exactly as a tenant-bound key
	// is, and is never cross-tenant: only the operator credential acts across
	// tenants.
	return auth.Principal{
		ID:     "user:" + who.User.String(),
		Tenant: who.Tenant,
		Kind:   auth.KindUser,
		User:   who.User,
	}, nil
}
