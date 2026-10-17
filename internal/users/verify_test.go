package users_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/users"
)

func TestAValidTokenNamesItsUserAndItsTenant(t *testing.T) {
	s, _ := newService(t)
	u, _ := s.Create(in("acme"), "planner")
	issued, _ := s.IssueToken(in("acme"), u.ID, nil)

	// No tenant on the context: authentication runs before one is known, and
	// the tenant comes from the token, never from the request.
	who, err := s.Verify(context.Background(), issued.Secret)
	if err != nil {
		t.Fatal(err)
	}
	if who.Tenant != "acme" || who.User != u.ID {
		t.Fatalf("verified as %+v, want acme/%s", who, u.ID)
	}
	// Even a request carrying another tenant cannot move it.
	who, err = s.Verify(in("beta"), issued.Secret)
	if err != nil || who.Tenant != "acme" {
		t.Fatalf("verified as %+v, %v under a beta context", who, err)
	}
}

// Every refusal reads the same, so a caller probing tokens learns nothing about
// which part was wrong: whether the id exists, whether the secret was close,
// whether the token was revoked or the user disabled.
func TestEveryRefusalIsTheSameUnauthorized(t *testing.T) {
	s, clk := newService(t)
	u, _ := s.Create(in("acme"), "planner")
	good, _ := s.IssueToken(in("acme"), u.ID, nil)
	revoked, _ := s.IssueToken(in("acme"), u.ID, nil)
	if _, err := s.RevokeToken(in("acme"), u.ID, revoked.Token.ID); err != nil {
		t.Fatal(err)
	}
	soon := clk.Now().Add(time.Minute)
	expiring, _ := s.IssueToken(in("acme"), u.ID, &soon)
	disabledUser, _ := s.Create(in("acme"), "researcher")
	disabled, _ := s.IssueToken(in("acme"), disabledUser.ID, nil)
	if _, err := s.SetState(in("acme"), disabledUser.ID, users.Disabled); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)

	// A known id with one character in the middle of the secret changed, and
	// one with the last character changed: that character carries one bit and
	// four of padding, so a decoder that ignored padding would accept it.
	mid := len(good.Secret) - 20
	wrong := good.Secret[:mid] + flip(good.Secret[mid]) + good.Secret[mid+1:]
	noncanonical := good.Secret[:len(good.Secret)-1] + flip(good.Secret[len(good.Secret)-1])
	// A well-formed token whose id this store never issued: minted by another.
	elsewhere, _ := newService(t)
	stranger, _ := elsewhere.Create(in("acme"), "planner")
	other, err := elsewhere.IssueToken(in("acme"), stranger.ID, nil)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]string{
		"wrong secret":  wrong,
		"non-canonical": noncanonical,
		"unknown id":    other.Secret,
		"revoked":       revoked.Secret,
		"expired":       expiring.Secret,
		"disabled":      disabled.Secret,
		"malformed":     "rmm_not-a-token",
		"no prefix":     "planner",
		"empty":         "",
	}
	var first string
	for name, tok := range cases {
		_, err := s.Verify(context.Background(), tok)
		if !errs.Is(err, errs.Unauthorized) {
			t.Fatalf("%s: got %v, want Unauthorized", name, err)
		}
		if first == "" {
			first = err.Error()
		}
		if err.Error() != first {
			t.Fatalf("%s: refused as %q, another as %q; every refusal must read the same", name, err, first)
		}
	}
	if _, err := s.Verify(context.Background(), good.Secret); err != nil {
		t.Fatalf("the good token stopped working: %v", err)
	}
}

func TestReEnablingRestoresLiveTokensAndNotRevokedOnes(t *testing.T) {
	s, _ := newService(t)
	u, _ := s.Create(in("acme"), "planner")
	live, _ := s.IssueToken(in("acme"), u.ID, nil)
	dead, _ := s.IssueToken(in("acme"), u.ID, nil)
	_, _ = s.RevokeToken(in("acme"), u.ID, dead.Token.ID)
	_, _ = s.SetState(in("acme"), u.ID, users.Disabled)
	if _, err := s.Verify(context.Background(), live.Secret); !errs.Is(err, errs.Unauthorized) {
		t.Fatalf("a disabled user's token: %v", err)
	}
	_, _ = s.SetState(in("acme"), u.ID, users.Active)
	if _, err := s.Verify(context.Background(), live.Secret); err != nil {
		t.Fatalf("the live token after re-enabling: %v", err)
	}
	if _, err := s.Verify(context.Background(), dead.Secret); !errs.Is(err, errs.Unauthorized) {
		t.Fatalf("the revoked token after re-enabling: %v", err)
	}
}

func TestTheRefusalNeverEchoesTheToken(t *testing.T) {
	s, _ := newService(t)
	u, _ := s.Create(in("acme"), "planner")
	good, _ := s.IssueToken(in("acme"), u.ID, nil)
	_, _ = s.RevokeToken(in("acme"), u.ID, good.Token.ID)
	_, err := s.Verify(context.Background(), good.Secret)
	if err == nil || strings.Contains(err.Error(), good.Secret[4:20]) || strings.Contains(err.Error(), good.Token.ID.String()) {
		t.Fatalf("the refusal %q names the token", err)
	}
}

func flip(c byte) string {
	if c == 'a' {
		return "b"
	}
	return "a"
}
