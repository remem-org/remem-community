package users

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/users/pb"
	"google.golang.org/protobuf/proto"
)

// Identity is who a verified token says the caller is.
//
// It is this package's answer rather than an auth.Principal, so that the
// package that reads principals never depends on the one that stores tokens:
// the composition root turns one into the other.
type Identity struct {
	Tenant tenant.ID
	User   id.ID
}

// refused is the one refusal every failed verification returns. A caller
// probing tokens learns nothing about which part was wrong: whether the id
// exists, whether the secret was close, whether the token was revoked or
// expired, or whether its user is disabled.
var refused = errs.E(errs.Unauthorized, "users.Verify",
	errors.New("the presented token is not one this server accepts"))

// Verify turns a presented token into the identity it authenticates.
//
// It costs three point reads and one digest comparison whatever the number of
// users and tokens, and keeps no cache: a revocation or a disable committed
// before a request is seen by that request, on one node and on many. The
// tenant comes from the token's index row, never from the request, so nothing
// the caller sends can move a token to another tenant.
//
// A storage failure is reported as itself rather than as a refusal: telling a
// caller its token is wrong when the store is down sends it to fix the wrong
// thing. A row that will not decode is errs.Corruption, for the same reason.
func (s *Service) Verify(ctx context.Context, token string) (Identity, error) {
	const op = "users.Verify"
	tid, secret, err := parseToken(token)
	if err != nil {
		return Identity{}, refused
	}

	raw, err := s.kv.Get(ctx, keys.TokenIndex(tid))
	if err != nil {
		return Identity{}, refusedOr(err)
	}
	var idx pb.TokenIndex
	if err := proto.Unmarshal(raw, &idx); err != nil {
		return Identity{}, corrupt(op, "token index", tid, err)
	}
	t, err := tenant.Parse(idx.GetTenant())
	if err != nil {
		return Identity{}, corrupt(op, "token index", tid, err)
	}

	raw, err = s.kv.Get(ctx, keys.UserToken(t, ns, tid))
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return Identity{}, errs.E(errs.Corruption, op, fmt.Errorf(
				"token %s is indexed to tenant %s, which holds no such token", tid, t))
		}
		return Identity{}, err
	}
	tok, digest, err := decodeToken(tid, raw, op)
	if err != nil {
		return Identity{}, err
	}
	presented := sha256.Sum256(secret)
	if subtle.ConstantTimeCompare(presented[:], digest) != 1 {
		return Identity{}, refused
	}
	if tok.StateAt(s.clk.Now()) != "active" {
		return Identity{}, refused
	}

	raw, err = s.kv.Get(ctx, keys.User(t, ns, tok.User))
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return Identity{}, errs.E(errs.Corruption, op, fmt.Errorf(
				"token %s belongs to user %s, which tenant %s does not hold", tid, tok.User, t))
		}
		return Identity{}, err
	}
	u, err := decodeUser(tok.User, raw, op)
	if err != nil {
		return Identity{}, err
	}
	if u.State != Active {
		return Identity{}, refused
	}
	return Identity{Tenant: t, User: u.ID}, nil
}

// refusedOr is the refusal for a token id nothing indexed, and the error itself
// for anything else.
func refusedOr(err error) error {
	if errs.Is(err, errs.NotFound) {
		return refused
	}
	return err
}
