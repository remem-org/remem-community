package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/session"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
)

func newRegistry(t *testing.T) (*session.Registry, *clock.Fake, *memkv.Store) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)
	return session.New(kv, clk, time.Hour), clk, kv
}

func acme() context.Context { return tenant.NewContext(context.Background(), "acme") }

func TestCreateThenGet(t *testing.T) {
	r, _, _ := newRegistry(t)
	s, err := r.Create(acme(), session.Session{
		Principal: "acme-agent", ClientName: "claude", ClientVersion: "1.2",
		ProtocolVersion: "2025-06-18",
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.ID.IsZero() || s.Tenant != "acme" || s.CreatedAt.IsZero() {
		t.Fatalf("session = %+v", s)
	}

	got, err := r.Get(acme(), s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClientName != "claude" || got.ProtocolVersion != "2025-06-18" || got.Principal != "acme-agent" {
		t.Fatalf("session = %+v", got)
	}
}

func TestAnUnknownSessionIsNotFound(t *testing.T) {
	r, _, _ := newRegistry(t)
	if _, err := r.Get(acme(), id.New()); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

// Sessions belong to a tenant like everything else. A session id leaked from
// one tenant must not open a door in another.
func TestSessionsAreTenantScoped(t *testing.T) {
	r, _, _ := newRegistry(t)
	s, err := r.Create(acme(), session.Session{Principal: "a"})
	if err != nil {
		t.Fatal(err)
	}
	other := tenant.NewContext(context.Background(), "other")
	if _, err := r.Get(other, s.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("a session id from acme resolved in another tenant: %v", err)
	}
}

func TestAnExpiredSessionIsNotFound(t *testing.T) {
	r, clk, _ := newRegistry(t)
	s, err := r.Create(acme(), session.Session{Principal: "a"})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Hour + time.Minute)
	if _, err := r.Get(acme(), s.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("an expired session resolved: %v", err)
	}
}

// Touch is what keeps a working agent's session alive. Without it a client that
// is mid-conversation loses its session on the hour.
func TestTouchExtendsTheLifeOfASession(t *testing.T) {
	r, clk, _ := newRegistry(t)
	s, err := r.Create(acme(), session.Session{Principal: "a"})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(50 * time.Minute)
	if err := r.Touch(acme(), s.ID); err != nil {
		t.Fatal(err)
	}
	clk.Advance(50 * time.Minute)
	if _, err := r.Get(acme(), s.ID); err != nil {
		t.Fatalf("a session used 50 minutes ago expired: %v", err)
	}
}

func TestTouchingAnUnknownSessionIsNotFound(t *testing.T) {
	r, _, _ := newRegistry(t)
	if err := r.Touch(acme(), id.New()); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

func TestDeleteEndsASessionAndIsIdempotent(t *testing.T) {
	r, _, _ := newRegistry(t)
	s, err := r.Create(acme(), session.Session{Principal: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(acme(), s.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(acme(), s.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
	if err := r.Delete(acme(), s.ID); err != nil {
		t.Fatalf("deleting an ended session: %v", err)
	}
}

func TestSweepRemovesOnlyExpiredSessions(t *testing.T) {
	r, clk, _ := newRegistry(t)
	old, err := r.Create(acme(), session.Session{Principal: "old"})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(90 * time.Minute)
	fresh, err := r.Create(acme(), session.Session{Principal: "fresh"})
	if err != nil {
		t.Fatal(err)
	}

	n, err := r.SweepExpired(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the sweep removed %d sessions, want 1", n)
	}
	if _, err := r.Get(acme(), fresh.ID); err != nil {
		t.Fatalf("the sweep removed a live session: %v", err)
	}
	if _, err := r.Get(acme(), old.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("the expired session survived: %v", err)
	}
}

func TestSweepIsTenantScoped(t *testing.T) {
	r, clk, _ := newRegistry(t)
	other := tenant.NewContext(context.Background(), "other")
	s, err := r.Create(other, session.Session{Principal: "a"})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Hour)

	if _, err := r.SweepExpired(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	// Still there: acme's sweep must not touch another tenant's rows, however
	// expired they are.
	if _, err := r.Get(other, s.ID); !errs.Is(err, errs.NotFound) {
		t.Fatal("expected the row to still be present but expired")
	}
	n, err := r.SweepExpired(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the other tenant's sweep removed %d, want 1", n)
	}
}

func TestAnUnscopedSweepIsRefused(t *testing.T) {
	r, _, _ := newRegistry(t)
	if _, err := r.SweepExpired(context.Background(), ""); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestUnscopedOperationsAreRefused(t *testing.T) {
	r, _, _ := newRegistry(t)
	ctx := context.Background()
	if _, err := r.Create(ctx, session.Session{}); err == nil {
		t.Fatal("Invariant 1: Create without a tenant")
	}
	if _, err := r.Get(ctx, id.New()); err == nil {
		t.Fatal("Invariant 1: Get without a tenant")
	}
	if err := r.Delete(ctx, id.New()); err == nil {
		t.Fatal("Invariant 1: Delete without a tenant")
	}
}

// A session row that will not decode is reported as absent, not as corruption
// — the opposite of every other decode in Remem. That is what "expendable"
// means here: nothing is lost by telling the client to re-initialise, where
// refusing the request would strand it.
func TestAnUnreadableSessionRowIsAbsentRatherThanCorrupt(t *testing.T) {
	r, _, kv := newRegistry(t)
	s, err := r.Create(acme(), session.Session{Principal: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(context.Background(), session.Key("acme", s.ID), []byte("\xff\xff not a session")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(acme(), s.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
	// And the sweep reclaims it rather than leaving it forever.
	n, err := r.SweepExpired(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the sweep removed %d unreadable rows, want 1", n)
	}
}
