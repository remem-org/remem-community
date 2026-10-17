package users

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/users/pb"
	"google.golang.org/protobuf/proto"
)

// TokenPrefix begins every user token. It tells verification which path a
// presented credential takes, and makes a leaked token findable by a secret
// scanner. Configuration refuses a configured key with this prefix, which
// could otherwise never authenticate.
const TokenPrefix = "rmm_"

// secretLen is the secret's width: 256 bits of randomness. It is not a
// password, so it is digested with plain SHA-256; a slow hash would add latency
// and no security.
const secretLen = 32

// encoding is RFC 4648 base32 in lower case, unpadded: the token stays one word
// a shell, a URL and a header all pass through unquoted.
var encoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// Token is one token, as every surface but issuance sees it. It has no field a
// secret or its digest could travel in, so nothing built on it can leak one.
type Token struct {
	ID        id.ID
	User      id.ID
	CreatedAt time.Time
	// ExpiresAt is when the token stops authenticating; nil means never.
	ExpiresAt *time.Time
	// RevokedAt is when the token was revoked; nil means it was not.
	RevokedAt *time.Time
}

// StateAt reports the token's state at now: revoked, expired or active. It is
// the token's own state, and says nothing about whether its user is disabled.
func (t Token) StateAt(now time.Time) string {
	switch {
	case t.RevokedAt != nil:
		return "revoked"
	case t.ExpiresAt != nil && !now.Before(*t.ExpiresAt):
		return "expired"
	}
	return "active"
}

// Issued is a token at the one moment its secret exists outside its holder.
type Issued struct {
	Token Token
	// Secret is the whole token string, prefix included: what the holder
	// presents. It is returned here and never again.
	Secret string
}

// IssueToken issues a token to a user. expiresAt, when given, must be in the
// future.
func (s *Service) IssueToken(ctx context.Context, uid id.ID, expiresAt *time.Time) (Issued, error) {
	const op = "users.IssueToken"
	t, err := tenant.Require(ctx)
	if err != nil {
		return Issued{}, err
	}
	now := s.now()
	var expiry *time.Time
	if expiresAt != nil {
		e := expiresAt.UTC().Truncate(time.Millisecond)
		if !e.After(now) {
			return Issued{}, errs.E(errs.Invalid, op, fmt.Errorf(
				"the expiry %s is not in the future", e.Format(time.RFC3339)))
		}
		expiry = &e
	}

	// Everything random is drawn before the transaction (Invariant 9): a retried
	// body must write the token it will hand back, not a second one.
	var tid id.ID
	secret := make([]byte, secretLen)
	if _, err := rand.Read(tid[:]); err != nil {
		return Issued{}, errs.E(errs.Unavailable, op, fmt.Errorf("drawing a token id: %w", err))
	}
	if _, err := rand.Read(secret); err != nil {
		return Issued{}, errs.E(errs.Unavailable, op, fmt.Errorf("drawing a token secret: %w", err))
	}
	digest := sha256.Sum256(secret)
	tok := Token{ID: tid, User: uid, CreatedAt: now, ExpiresAt: expiry}

	err = txn.Do(ctx, s.kv, func(tx txn.Tx) error {
		// The user must exist when the token does, so its row is read and
		// expected unchanged: a token cannot be issued against a read that a
		// concurrent writer has since overtaken.
		_, raw, err := readUser(tx, t, uid, op)
		if err != nil {
			return err
		}
		tx.Expect(keys.User(t, ns, uid), raw, true)

		tkey, ikey := keys.UserToken(t, ns, tid), keys.TokenIndex(tid)
		tx.Expect(tkey, nil, false)
		tx.Expect(ikey, nil, false)
		tx.Set(tkey, encodeToken(tok, digest[:]))
		tx.Set(ikey, mustMarshal(&pb.TokenIndex{Tenant: string(t)}))
		return nil
	})
	if err != nil {
		return Issued{}, err
	}
	return Issued{Token: tok, Secret: formatToken(tid, secret)}, nil
}

// ListTokens returns a user's tokens, oldest first, in every state.
func (s *Service) ListTokens(ctx context.Context, uid id.ID) ([]Token, error) {
	const op = "users.ListTokens"
	if _, err := s.Get(ctx, uid); err != nil {
		return nil, err
	}
	t, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	// A tenant's tokens are one range, scanned whole and filtered by user.
	// Listing is an administrator's occasional read; keying tokens under their
	// user instead would make every verification name a user it does not yet
	// know.
	lo, hi := keys.UserKindRange(t, ns, keys.UserTok)
	it := s.kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()

	var out []Token
	for ok := it.First(); ok; ok = it.Next() {
		tid, err := tailID(it.Key(), op)
		if err != nil {
			return nil, err
		}
		tok, _, err := decodeToken(tid, it.Value(), op)
		if err != nil {
			return nil, err
		}
		if tok.User == uid {
			out = append(out, tok)
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b Token) int {
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.ID.String(), b.ID.String())
	})
	return out, nil
}

// RevokeToken revokes one of a user's tokens, permanently. Revoking a revoked
// token succeeds and keeps the first revocation's time.
func (s *Service) RevokeToken(ctx context.Context, uid, tid id.ID) (Token, error) {
	const op = "users.RevokeToken"
	t, err := tenant.Require(ctx)
	if err != nil {
		return Token{}, err
	}
	now := s.now()

	var out Token
	err = txn.Do(ctx, s.kv, func(tx txn.Tx) error {
		tkey := keys.UserToken(t, ns, tid)
		raw, err := tx.Get(tkey)
		if errs.Is(err, errs.NotFound) {
			return errs.E(errs.NotFound, op, fmt.Errorf("user %s holds no token %s", uid, tid))
		}
		if err != nil {
			return err
		}
		tok, digest, err := decodeToken(tid, raw, op)
		if err != nil {
			return err
		}
		// Another user's token is reported exactly as a missing one: an
		// operator who names the wrong user learns nothing about the token.
		if tok.User != uid {
			return errs.E(errs.NotFound, op, fmt.Errorf("user %s holds no token %s", uid, tid))
		}
		if tok.RevokedAt == nil {
			tok.RevokedAt = &now
			tx.Expect(tkey, raw, true)
			tx.Set(tkey, encodeToken(tok, digest))
		}
		out = tok
		return nil
	})
	return out, err
}

func formatToken(tid id.ID, secret []byte) string {
	return TokenPrefix + encoding.EncodeToString(tid[:]) + "_" + encoding.EncodeToString(secret)
}

// errMalformed is the one answer for a token that does not parse. It carries no
// detail, because every refusal of a token reads the same (see Verify).
var errMalformed = errors.New("malformed token")

// parseToken splits a presented token into its id and its secret.
func parseToken(s string) (id.ID, []byte, error) {
	rest, ok := strings.CutPrefix(s, TokenPrefix)
	if !ok {
		return id.Zero, nil, errMalformed
	}
	idPart, secretPart, ok := strings.Cut(rest, "_")
	if !ok {
		return id.Zero, nil, errMalformed
	}
	raw, err := decodeCanonical(idPart, 16)
	if err != nil {
		return id.Zero, nil, err
	}
	secret, err := decodeCanonical(secretPart, secretLen)
	if err != nil {
		return id.Zero, nil, err
	}
	var tid id.ID
	copy(tid[:], raw)
	return tid, secret, nil
}

// decodeCanonical decodes one part of a token and refuses any spelling but the
// one formatToken writes.
//
// Neither width is a multiple of five bytes, so the last character of each part
// carries bits the decoder ignores: thirty-two bytes is 256 bits in 52
// characters, and the last character holds one bit and four of padding. Without
// this check, sixteen different strings would be the same token, and a token
// compared as a string anywhere — a log scrubber, a revocation list a client
// keeps — would miss fifteen of them.
func decodeCanonical(part string, width int) ([]byte, error) {
	b, err := encoding.DecodeString(part)
	if err != nil || len(b) != width || encoding.EncodeToString(b) != part {
		return nil, errMalformed
	}
	return b, nil
}

func encodeToken(t Token, digest []byte) []byte {
	m := &pb.Token{
		UserId:          t.User.Bytes(),
		Digest:          digest,
		CreatedAtUnixMs: uint64(t.CreatedAt.UnixMilli()),
	}
	if t.ExpiresAt != nil {
		m.ExpiresAtUnixMs = proto.Uint64(uint64(t.ExpiresAt.UnixMilli()))
	}
	if t.RevokedAt != nil {
		m.RevokedAtUnixMs = proto.Uint64(uint64(t.RevokedAt.UnixMilli()))
	}
	return mustMarshal(m)
}

// decodeToken returns the token and the digest it stores, which only
// verification and revocation's rewrite ever read.
func decodeToken(tid id.ID, raw []byte, op string) (Token, []byte, error) {
	var m pb.Token
	if err := proto.Unmarshal(raw, &m); err != nil {
		return Token{}, nil, corrupt(op, "token", tid, err)
	}
	uid, err := id.FromBytes(m.GetUserId())
	if err != nil {
		return Token{}, nil, corrupt(op, "token", tid, err)
	}
	if len(m.GetDigest()) != sha256.Size {
		return Token{}, nil, corrupt(op, "token", tid, fmt.Errorf("digest is %d bytes", len(m.GetDigest())))
	}
	tok := Token{ID: tid, User: uid, CreatedAt: fromMillis(m.GetCreatedAtUnixMs())}
	if m.ExpiresAtUnixMs != nil {
		e := fromMillis(m.GetExpiresAtUnixMs())
		tok.ExpiresAt = &e
	}
	if m.RevokedAtUnixMs != nil {
		r := fromMillis(m.GetRevokedAtUnixMs())
		tok.RevokedAt = &r
	}
	return tok, m.GetDigest(), nil
}
