package codec

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TokenVersion is the version byte every page token carries.
//
// A token is a durable-ish format: it survives in a client's state, in a
// paginating script, in a saved URL. Bumping this refuses the old shape rather
// than misreading it, which matters because everything after the version byte
// is positional -- a token read at the wrong version resolves to a real
// position in a real index, just the wrong one.
//
// It starts at 3 rather than 1 because query.Cursor issued versions 1 and 2 in
// its own layout before this package owned the framing. Continuing the sequence
// keeps a client's refusal message monotonic instead of telling them the server
// has gone backwards.
const TokenVersion uint8 = 3

// TokenKind says which walk a token resumes.
//
// Without it, a search cursor presented to the history endpoint decodes
// structurally -- right version, right tenant -- and names a position in a
// stream it was never about. The byte costs nothing and turns that into a
// refusal.
type TokenKind uint8

const (
	// TokenListing resumes an ordered attribute-index walk.
	TokenListing TokenKind = 1
	// TokenSearch resumes a ranked result set materialised in a paging session.
	TokenSearch TokenKind = 2
	// TokenHistory resumes an audit stream by position.
	TokenHistory TokenKind = 3
	// TokenEdges resumes a walk over one record's relationships.
	TokenEdges TokenKind = 4
	// TokenRelated resumes a materialised graph traversal. It is distinct from
	// TokenSearch even though both name a ranked session, so that a cursor from
	// one endpoint presented to the other is refused by kind rather than only by
	// the session's query fingerprint.
	TokenRelated TokenKind = 5
)

func (k TokenKind) String() string {
	switch k {
	case TokenListing:
		return "listing"
	case TokenSearch:
		return "search"
	case TokenHistory:
		return "history"
	case TokenEdges:
		return "connections"
	case TokenRelated:
		return "related"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// EncodeToken renders an opaque page token.
//
// base64url without padding, because a token ends up in a query string and in
// a JSON body, and both are places where '+' and '=' cost an escaping bug
// eventually.
func EncodeToken(kind TokenKind, t tenant.ID, payload []byte) string {
	b := make([]byte, 0, 8+len(t)+len(payload))
	b = append(b, TokenVersion, uint8(kind))
	b = binary.AppendUvarint(b, uint64(len(t)))
	b = append(b, t...)
	b = append(b, payload...)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeToken reads a token, checking that it belongs to this tenant and names
// the walk the caller is resuming.
//
// Returning Invalid rather than NotFound is deliberate: the token is malformed
// *for this request*, and "not found" would suggest that presenting it
// somewhere else might work.
func DecodeToken(token string, want TokenKind, t tenant.ID) ([]byte, error) {
	const op = "codec.DecodeToken"

	kind, name, payload, err := splitToken(token, op)
	if err != nil {
		return nil, err
	}
	if kind != want {
		return nil, badToken(op, "this page token resumes a %s and this request is a %s; "+
			"start the %s again", kind, want, want)
	}
	if name != t {
		return nil, badToken(op, "this page token was issued for a different tenant")
	}
	return payload, nil
}

// TokenTenant reads the tenant a token was minted for without checking it
// against a request. It exists for JSON unmarshalling, which has no request
// scope to check against and must still refuse a structurally broken token.
func TokenTenant(token string) (tenant.ID, error) {
	const op = "codec.TokenTenant"
	_, name, _, err := splitToken(token, op)
	return name, err
}

func splitToken(token, op string) (kind TokenKind, name tenant.ID, payload []byte, err error) {
	if token == "" {
		return 0, "", nil, badToken(op, "this page token is empty")
	}
	raw, decErr := base64.RawURLEncoding.DecodeString(token)
	if decErr != nil {
		return 0, "", nil, badToken(op, "this page token is not a token this server issued")
	}
	if len(raw) < 2 {
		return 0, "", nil, badToken(op, "this page token ends before its header")
	}
	if v := raw[0]; v != TokenVersion {
		return 0, "", nil, badToken(op,
			"this page token is version %d and this server issues version %d; "+
				"start the listing again rather than resuming it", v, TokenVersion)
	}
	kind = TokenKind(raw[1])
	nameBytes, rest, readErr := readLengthPrefixed(raw[2:])
	if readErr != nil {
		return 0, "", nil, badToken(op, "this page token ends inside its tenant")
	}
	return kind, tenant.ID(nameBytes), rest, nil
}

func badToken(op, format string, args ...any) error {
	return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
}

// readLengthPrefixed reads a uvarint-prefixed byte string, returning it and
// whatever follows.
func readLengthPrefixed(b []byte) ([]byte, []byte, error) {
	n, w := binary.Uvarint(b)
	if w <= 0 {
		return nil, nil, fmt.Errorf("truncated length prefix")
	}
	b = b[w:]
	if uint64(len(b)) < n {
		return nil, nil, fmt.Errorf("truncated body")
	}
	return b[:n], b[n:], nil
}

// AppendLengthPrefixed writes a uvarint-prefixed byte string into a token
// payload. It is exported so that the packages building payloads -- query,
// events, graph -- frame their variable-length fields the same way.
func AppendLengthPrefixed(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// ReadLengthPrefixed is the inverse of AppendLengthPrefixed.
func ReadLengthPrefixed(b []byte) ([]byte, []byte, error) { return readLengthPrefixed(b) }
