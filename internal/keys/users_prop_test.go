package keys_test

import (
	"bytes"
	"testing"

	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"pgregory.net/rapid"
)

// inside reports whether k falls in the half-open range [lo, hi).
func inside(k, lo, hi []byte) bool {
	return bytes.Compare(k, lo) >= 0 && (hi == nil || bytes.Compare(k, hi) < 0)
}

// TestEachUserRowKindIsOneRangeInsideItsSpace: listing users, listing tokens and
// rebuilding the name index each scan one kind, so each kind must be one range,
// inside the user space and so inside the tenant's own range — which is what
// makes a tenant export or erasure reach every user row without remembering to.
func TestEachUserRowKindIsOneRangeInsideItsSpace(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		ns := tenant.Namespace(idGen().Draw(rt, "namespace"))
		uid, tok := drawID(rt, "user"), drawID(rt, "token")
		name := rapid.StringN(1, 64, -1).Draw(rt, "name")

		rows := map[keys.UserKind][]byte{
			keys.UserRow:   keys.User(tid, ns, uid),
			keys.UserTok:   keys.UserToken(tid, ns, tok),
			keys.UserNames: keys.UserName(tid, ns, name),
		}
		tlo, thi := keys.TenantRange(tid)
		slo, shi := keys.SpaceRange(tid, ns, keys.SpaceUser)
		for kind, k := range rows {
			if !inside(k, tlo, thi) || !inside(k, slo, shi) {
				rt.Fatalf("a %s row fell outside its tenant or space: %x", kind, k)
			}
			for other := range rows {
				if lo, hi := keys.UserKindRange(tid, ns, other); other != kind && inside(k, lo, hi) {
					rt.Fatalf("a %s row fell inside the %s range: %x", kind, other, k)
				}
			}
			if lo, hi := keys.UserKindRange(tid, ns, kind); !inside(k, lo, hi) {
				rt.Fatalf("a %s row is outside its own kind's range: %x", kind, k)
			}
		}
	})
}

// TestTokenIndexIsUntenantedAndDistinct: authentication reads the index before
// any tenant is known, so it must be a system row, and two token ids must never
// share one — a collision would hand one token's tenant to another.
func TestTokenIndexIsUntenantedAndDistinct(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a, b := drawID(rt, "a"), drawID(rt, "b")
		ka := keys.TokenIndex(a)

		lo, hi := keys.SystemRange()
		if !inside(ka, lo, hi) {
			rt.Fatalf("the token index is outside the system space: %x", ka)
		}
		if ilo, ihi := keys.TokenIndexRange(); !inside(ka, ilo, ihi) {
			rt.Fatalf("the token index is outside its own range: %x", ka)
		}
		if a != b && bytes.Equal(ka, keys.TokenIndex(b)) {
			rt.Fatalf("two token ids share an index key: %x", ka)
		}
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		if tlo, thi := keys.TenantRange(tid); inside(ka, tlo, thi) {
			rt.Fatalf("the token index fell inside tenant %q's range", tid)
		}
		if got, ok := keys.TenantOf(ka); !ok || got != "" {
			rt.Fatalf("the token index reads as tenant %q, want untenanted", got)
		}
	})
}
