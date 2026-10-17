package users_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/users"
)

func TestAnIssuedTokenIsShownOnceAndListedWithoutItsSecret(t *testing.T) {
	s, _ := newService(t)
	u, _ := s.Create(in("acme"), "planner")
	issued, err := s.IssueToken(in("acme"), u.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(issued.Secret, "rmm_") || issued.Token.User != u.ID {
		t.Fatalf("issued %+v", issued)
	}
	list, err := s.ListTokens(in("acme"), u.ID)
	if err != nil || len(list) != 1 || list[0].ID != issued.Token.ID {
		t.Fatalf("listed %+v, %v", list, err)
	}
	// Structural rather than behavioural: a listed token has no field a secret
	// or its digest could travel in, so no surface built on it can leak one.
	for i := range reflect.TypeOf(users.Token{}).NumField() {
		f := reflect.TypeOf(users.Token{}).Field(i).Name
		if strings.Contains(strings.ToLower(f), "secret") || strings.Contains(strings.ToLower(f), "digest") {
			t.Fatalf("users.Token has a field %s", f)
		}
	}
}

func TestTwoTokensAreDistinctAndBothBelongToTheUser(t *testing.T) {
	s, _ := newService(t)
	u, _ := s.Create(in("acme"), "planner")
	a, _ := s.IssueToken(in("acme"), u.ID, nil)
	b, _ := s.IssueToken(in("acme"), u.ID, nil)
	if a.Token.ID == b.Token.ID || a.Secret == b.Secret {
		t.Fatal("two issued tokens are not distinct")
	}
	if list, _ := s.ListTokens(in("acme"), u.ID); len(list) != 2 {
		t.Fatalf("the user holds %d tokens, want 2", len(list))
	}
	other, _ := s.Create(in("acme"), "researcher")
	if list, _ := s.ListTokens(in("acme"), other.ID); len(list) != 0 {
		t.Fatalf("another user lists %d tokens", len(list))
	}
}

func TestTokensCannotBeIssuedToAMissingUser(t *testing.T) {
	s, _ := newService(t)
	if _, err := s.IssueToken(in("acme"), id.New(), nil); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
	u, _ := s.Create(in("acme"), "planner")
	if _, err := s.IssueToken(in("beta"), u.ID, nil); !errs.Is(err, errs.NotFound) {
		t.Fatalf("another tenant's user: got %v, want NotFound", err)
	}
}

func TestAnExpiryMustBeInTheFuture(t *testing.T) {
	s, clk := newService(t)
	u, _ := s.Create(in("acme"), "planner")
	past := clk.Now().Add(-time.Second)
	if _, err := s.IssueToken(in("acme"), u.ID, &past); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
	future := clk.Now().Add(time.Hour)
	issued, err := s.IssueToken(in("acme"), u.ID, &future)
	if err != nil || issued.Token.ExpiresAt == nil || !issued.Token.ExpiresAt.Equal(future.UTC().Truncate(time.Millisecond)) {
		t.Fatalf("issued %+v, %v", issued.Token, err)
	}
	if got := issued.Token.StateAt(clk.Now()); got != "active" {
		t.Fatalf("state %q before expiry", got)
	}
	clk.Advance(2 * time.Hour)
	list, _ := s.ListTokens(in("acme"), u.ID)
	if got := list[0].StateAt(clk.Now()); got != "expired" {
		t.Fatalf("state %q after expiry", got)
	}
}

func TestRevocationIsPermanentAndIdempotent(t *testing.T) {
	s, clk := newService(t)
	u, _ := s.Create(in("acme"), "planner")
	issued, _ := s.IssueToken(in("acme"), u.ID, nil)
	r, err := s.RevokeToken(in("acme"), u.ID, issued.Token.ID)
	if err != nil || r.RevokedAt == nil || r.StateAt(clk.Now()) != "revoked" {
		t.Fatalf("revoked %+v, %v", r, err)
	}
	first := *r.RevokedAt
	clk.Advance(time.Minute)
	again, err := s.RevokeToken(in("acme"), u.ID, issued.Token.ID)
	if err != nil || !again.RevokedAt.Equal(first) {
		t.Fatalf("a second revocation moved the time or failed: %+v, %v", again, err)
	}
}

func TestRevokingAnotherUsersTokenIsNotFound(t *testing.T) {
	s, _ := newService(t)
	a, _ := s.Create(in("acme"), "planner")
	b, _ := s.Create(in("acme"), "researcher")
	issued, _ := s.IssueToken(in("acme"), a.ID, nil)
	if _, err := s.RevokeToken(in("acme"), b.ID, issued.Token.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
	if _, err := s.RevokeToken(in("acme"), a.ID, id.New()); !errs.Is(err, errs.NotFound) {
		t.Fatalf("an unknown token: got %v, want NotFound", err)
	}
}
