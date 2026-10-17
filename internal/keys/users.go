package keys

import (
	"encoding/hex"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
)

// UserKind is the leading byte of a key in the user space, so that users,
// tokens and the name index are each one range: listing users, listing a user's
// tokens and rebuilding the name index each scan exactly one kind.
type UserKind byte

const (
	// UserRow is a user: its name, state and timestamps. Canonical.
	UserRow UserKind = 0x01
	// UserTok is a token: its user, the digest of its secret, its expiry and
	// whether it is revoked. Canonical.
	UserTok UserKind = 0x02
	// UserNames maps a folded user name to the user holding it. It is computed
	// from the user rows and rebuilt from them on import, and it is what makes a
	// name unique within a tenant: two writers claiming one name contend on one
	// key.
	UserNames UserKind = 0x03
)

var userKindNames = map[UserKind]string{
	UserRow:   "user",
	UserTok:   "token",
	UserNames: "name",
}

// String returns the stable name of the kind, used in `remem-admin inspect`.
func (k UserKind) String() string {
	if n, ok := userKindNames[k]; ok {
		return n
	}
	return "unknown"
}

// User addresses one user of a tenant.
func User(t tenant.ID, ns tenant.Namespace, uid id.ID) []byte {
	k := begin(t, ns, SpaceUser, 1+idLen)
	k = append(k, byte(UserRow))
	return append(k, uid[:]...)
}

// UserToken addresses one token. The id is the token's own, not its user's, so
// that verification reads the token it was handed without knowing whose it is.
func UserToken(t tenant.ID, ns tenant.Namespace, tokenID id.ID) []byte {
	k := begin(t, ns, SpaceUser, 1+idLen)
	k = append(k, byte(UserTok))
	return append(k, tokenID[:]...)
}

// UserName addresses the name-index row for one folded name.
//
// The name is the whole remainder, so it needs no length prefix: a name row is
// read by exact key or scanned as a whole kind, never as the prefix of another.
func UserName(t tenant.ID, ns tenant.Namespace, folded string) []byte {
	k := begin(t, ns, SpaceUser, 1+len(folded))
	k = append(k, byte(UserNames))
	return append(k, folded...)
}

// UserKindRange is every row of one kind in a tenant's user space.
func UserKindRange(t tenant.ID, ns tenant.Namespace, kind UserKind) (lower, upper []byte) {
	return PrefixRange(append(scopePrefix(t, ns, 2), byte(SpaceUser), byte(kind)))
}

// tokenIndexPrefix is the system path every token-index row begins with.
const tokenIndexPrefix = "/tokens/"

// TokenIndex addresses the system row that names a token's tenant.
//
// Authentication happens before tenant resolution, so a presented token must be
// found without a tenant, while every key a user owns is tenant-first
// (Invariant 1). This row is the one untenanted fact about a token: it holds
// only the tenant, and reveals nothing without the token row it points to. The
// id is lowercase hex so the path stays readable in `remem-admin inspect`.
func TokenIndex(tokenID id.ID) []byte {
	// Spelled out rather than through [System], which takes a string and would
	// cost a second allocation for the path.
	k := begin("", "", SpaceSystem, len(tokenIndexPrefix)+hex.EncodedLen(idLen))
	k = append(k, tokenIndexPrefix...)
	return hex.AppendEncode(k, tokenID[:])
}

// TokenIndexRange is every token-index row: what a verifier walks to reconcile
// the index against the token rows it points into.
func TokenIndexRange() (lower, upper []byte) {
	return PrefixRange(System(tokenIndexPrefix))
}
