package users_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/users"
)

func newService(t *testing.T) (*users.Service, *clock.Fake) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)
	return users.New(kv, clk), clk
}

func in(t tenant.ID) context.Context { return tenant.NewContext(context.Background(), t) }

func TestACreatedUserIsReadBackInItsTenantOnly(t *testing.T) {
	s, _ := newService(t)
	u, err := s.Create(in("acme"), "Planner")
	if err != nil {
		t.Fatal(err)
	}
	if u.ID.IsZero() || u.Name != "Planner" || u.State != users.Active {
		t.Fatalf("created %+v", u)
	}
	got, err := s.Get(in("acme"), u.ID)
	if err != nil || got.Name != "Planner" {
		t.Fatalf("read back %+v, %v", got, err)
	}
	if _, err := s.Get(in("beta"), u.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("another tenant's user read as %v, want NotFound", err)
	}
	list, err := s.List(in("beta"))
	if err != nil || len(list) != 0 {
		t.Fatalf("tenant beta lists %v, %v", list, err)
	}
}

func TestAContextWithoutATenantIsRefused(t *testing.T) {
	s, _ := newService(t)
	if _, err := s.Create(context.Background(), "planner"); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestADuplicateNameIsAConflictNamingTheHolder(t *testing.T) {
	s, _ := newService(t)
	first, err := s.Create(in("acme"), "Planner")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Create(in("acme"), "planner")
	if !errs.Is(err, errs.Conflict) || !strings.Contains(err.Error(), first.ID.String()) {
		t.Fatalf("got %v, want Conflict naming %s", err, first.ID)
	}
	if _, err := s.Create(in("beta"), "planner"); err != nil {
		t.Fatalf("the same name in another tenant was refused: %v", err)
	}
}

// Two writers claiming one name contend on one key. Exactly one wins; the other
// is told the name is taken, once, rather than having the refusal retried as if
// it were a lost race.
func TestConcurrentCreatesOfOneNameHaveOneWinner(t *testing.T) {
	for range 20 {
		s, _ := newService(t)
		var wg sync.WaitGroup
		results := make([]error, 8)
		for i := range results {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, results[i] = s.Create(in("acme"), "planner")
			}()
		}
		wg.Wait()
		won := 0
		for _, err := range results {
			switch {
			case err == nil:
				won++
			case !errs.Is(err, errs.Conflict):
				t.Fatalf("a losing create failed with %v, want Conflict", err)
			}
		}
		if won != 1 {
			t.Fatalf("%d creates of one name succeeded, want exactly 1", won)
		}
		if list, _ := s.List(in("acme")); len(list) != 1 {
			t.Fatalf("%d users exist, want 1", len(list))
		}
	}
}

func TestARenameKeepsTheIDAndFreesTheOldName(t *testing.T) {
	s, _ := newService(t)
	a, _ := s.Create(in("acme"), "agent-a")
	renamed, err := s.Rename(in("acme"), a.ID, "Planner")
	if err != nil {
		t.Fatal(err)
	}
	if renamed.ID != a.ID || renamed.Name != "Planner" {
		t.Fatalf("renamed to %+v", renamed)
	}
	if _, err := s.Create(in("acme"), "agent-a"); err != nil {
		t.Fatalf("the old name is still held: %v", err)
	}
	if _, err := s.Create(in("acme"), "planner"); !errs.Is(err, errs.Conflict) {
		t.Fatalf("the new name is not held: %v", err)
	}
}

// Renaming to the name already held, in another case, is a change of how it is
// written and must not conflict with itself.
func TestARenameToTheSameFoldedNameSucceeds(t *testing.T) {
	s, _ := newService(t)
	a, _ := s.Create(in("acme"), "planner")
	if _, err := s.Rename(in("acme"), a.ID, "Planner"); err != nil {
		t.Fatal(err)
	}
}

func TestARenameRacingACreateHasOneWinner(t *testing.T) {
	for range 20 {
		s, _ := newService(t)
		a, _ := s.Create(in("acme"), "agent-a")
		var wg sync.WaitGroup
		var renameErr, createErr error
		wg.Add(2)
		go func() { defer wg.Done(); _, renameErr = s.Rename(in("acme"), a.ID, "planner") }()
		go func() { defer wg.Done(); _, createErr = s.Create(in("acme"), "planner") }()
		wg.Wait()
		if (renameErr == nil) == (createErr == nil) {
			t.Fatalf("rename %v and create %v: want exactly one to win", renameErr, createErr)
		}
		for _, err := range []error{renameErr, createErr} {
			if err != nil && !errs.Is(err, errs.Conflict) {
				t.Fatalf("the loser failed with %v, want Conflict", err)
			}
		}
	}
}

func TestDisableAndEnable(t *testing.T) {
	s, clk := newService(t)
	a, _ := s.Create(in("acme"), "planner")
	clk.Advance(time.Millisecond)
	d, err := s.SetState(in("acme"), a.ID, users.Disabled)
	if err != nil || d.State != users.Disabled || !d.UpdatedAt.After(a.UpdatedAt) {
		t.Fatalf("disabled %+v, %v", d, err)
	}
	if _, err := s.Create(in("acme"), "Planner"); !errs.Is(err, errs.Conflict) {
		t.Fatalf("a disabled user's name was released: %v", err)
	}
	e, err := s.SetState(in("acme"), a.ID, users.Active)
	if err != nil || e.State != users.Active {
		t.Fatalf("enabled %+v, %v", e, err)
	}
}

func TestChangingAMissingUserIsNotFound(t *testing.T) {
	s, _ := newService(t)
	if _, err := s.Rename(in("acme"), id.New(), "x"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("rename: %v", err)
	}
	if _, err := s.SetState(in("acme"), id.New(), users.Disabled); !errs.Is(err, errs.NotFound) {
		t.Fatalf("disable: %v", err)
	}
}

func TestListIsInCreationOrder(t *testing.T) {
	s, clk := newService(t)
	var want []string
	for _, n := range []string{"c", "a", "b"} {
		clk.Advance(time.Millisecond)
		if _, err := s.Create(in("acme"), n); err != nil {
			t.Fatal(err)
		}
		want = append(want, n)
	}
	list, err := s.List(in("acme"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, u := range list {
		got = append(got, u.Name)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("listed %v, want creation order %v", got, want)
	}
}

// A row stores milliseconds; a write must return what a read will, or a caller
// comparing the two sees a change nobody made.
func TestWhatAWriteReturnsIsWhatAReadReturns(t *testing.T) {
	s, clk := newService(t)
	clk.Advance(1234567) // nanoseconds: off any millisecond boundary
	u, _ := s.Create(in("acme"), "planner")
	got, _ := s.Get(in("acme"), u.ID)
	if !got.CreatedAt.Equal(u.CreatedAt) || !got.UpdatedAt.Equal(u.UpdatedAt) {
		t.Fatalf("created %v, read back %v", u.CreatedAt, got.CreatedAt)
	}
	issued, _ := s.IssueToken(in("acme"), u.ID, nil)
	list, _ := s.ListTokens(in("acme"), u.ID)
	if !list[0].CreatedAt.Equal(issued.Token.CreatedAt) {
		t.Fatalf("issued at %v, listed at %v", issued.Token.CreatedAt, list[0].CreatedAt)
	}
}

