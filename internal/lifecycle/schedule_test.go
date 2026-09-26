package lifecycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
)

var epoch = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func rec(policy string, fn func(*record.Record)) *record.Record {
	r := &record.Record{
		ID:        id.New(),
		Tenant:    "acme",
		Namespace: tenant.DefaultNamespace,
		Type:      record.TypeMemory,
		Content:   "a memory",
		Fields: record.Fields{
			Policy:     policy,
			Importance: 0.5,
			Health:     100,
		},
		CreatedAt: epoch,
		UpdatedAt: epoch,
	}
	if fn != nil {
		fn(r)
	}
	return r
}

func policies(t *testing.T) *lifecycle.Policies {
	t.Helper()
	p, err := lifecycle.NewPolicies(nil)
	if err != nil {
		t.Fatalf("NewPolicies: %v", err)
	}
	return p
}

// The schedule is the earliest instant any transition could fire, arm for arm
// with Rust's services/attrs.rs:57.
func TestNextAttentionIsTheEarliestTransition(t *testing.T) {
	p := policies(t)
	day := 24 * time.Hour

	tests := []struct {
		name string
		rec  *record.Record
		want time.Time
	}{{
		// A brand-new long-term memory: importance decay is due a day after
		// creation, and so is active forgetting. Both land on the same instant.
		name: "long term, nothing recorded",
		rec:  rec(lifecycle.LongTerm, nil),
		want: epoch.Add(day),
	}, {
		// A TTL beats both, because it fires first.
		name: "short term with an hour to live",
		rec: rec(lifecycle.ShortTerm, func(r *record.Record) {
			r.Fields.TTL = time.Hour
		}),
		want: epoch.Add(time.Hour),
	}, {
		// A recall pushes active forgetting out by a day from the recall, not
		// from creation. Rust's `last_reinforced`.
		name: "recalled recently",
		rec: rec(lifecycle.ShortTerm, func(r *record.Record) {
			r.Fields.LastRecalledAt = epoch.Add(6 * time.Hour)
		}),
		want: epoch.Add(6*time.Hour + day),
	}, {
		// A health check pushes it out too, and the later of the two wins.
		name: "checked after the recall",
		rec: rec(lifecycle.ShortTerm, func(r *record.Record) {
			r.Fields.LastRecalledAt = epoch.Add(2 * time.Hour)
			r.Fields.LastHealthCheckAt = epoch.Add(9 * time.Hour)
		}),
		want: epoch.Add(9*time.Hour + day),
	}, {
		// Flashbulb protection defers decay and forgetting wholesale, so
		// nothing they would do can come due before it lapses.
		name: "protected",
		rec: rec(lifecycle.LongTerm, func(r *record.Record) {
			r.Fields.ProtectedUntil = epoch.Add(30 * day)
		}),
		want: epoch.Add(30 * day),
	}, {
		// An archived memory has exactly one thing left to happen to it, and
		// it is scheduled from archived_at — not from updated_at, which is
		// what lets the lifecycle leave updated_at alone.
		name: "archived",
		rec: rec(lifecycle.LongTerm, func(r *record.Record) {
			r.Fields.Archived = true
			r.Fields.ArchivedAt = epoch.Add(4 * time.Hour)
			r.UpdatedAt = epoch.Add(99 * day)
		}),
		want: epoch.Add(4*time.Hour + lifecycle.CleanupAfter),
	}, {
		// Pinned: nothing decays, nothing expires, nothing archives. It is
		// still visited, because a policy table that changes must not strand
		// a record forever — see MaxAttentionInterval.
		name: "pinned",
		rec:  rec(lifecycle.Pinned, nil),
		want: epoch.Add(lifecycle.MaxAttentionInterval),
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.NextAttention(tc.rec)
			if !got.Equal(tc.want) {
				t.Errorf("next attention is %v, want %v", got, tc.want)
			}
		})
	}
}

// Invariant 8: the projection reads no clock, so the same record always
// schedules to the same instant.
func TestNextAttentionIsAPureFunction(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.LongTerm, func(r *record.Record) {
		r.Fields.LastRecalledAt = epoch.Add(3 * time.Hour)
	})

	first := p.NextAttention(r)
	time.Sleep(2 * time.Millisecond)
	second := p.NextAttention(r)
	if !first.Equal(second) {
		t.Fatalf("the same record scheduled to %v and then %v; the projection reads a clock",
			first, second)
	}
}

// A live memory is never scheduled beyond the cap, whatever its policy says.
// Without it, a pinned memory would drop out of maintenance permanently and a
// later policy change could never reach it.
func TestNoRecordIsScheduledForever(t *testing.T) {
	p := policies(t)
	for _, pol := range lifecycle.Builtins() {
		for _, archived := range []bool{false, true} {
			r := rec(pol.Name, func(r *record.Record) {
				r.Fields.Archived = archived
				if archived {
					r.Fields.ArchivedAt = epoch
				}
			})
			got := p.NextAttention(r)
			if limit := epoch.Add(lifecycle.MaxAttentionInterval); got.After(limit) {
				t.Errorf("%s (archived=%v) is scheduled for %v, past the %v cap",
					pol.Name, archived, got, lifecycle.MaxAttentionInterval)
			}
		}
	}
}

// The record repository is where the schedule is written, so that no write path
// can produce a record the sweep will never visit.
func TestTheRepositoryComputesTheScheduleOnEveryWrite(t *testing.T) {
	p := policies(t)
	sched := lifecycle.NewScheduler(nil)
	_ = sched

	r := rec(lifecycle.LongTerm, nil)
	if !r.Fields.NextAttentionAt.IsZero() {
		t.Fatalf("the fixture already carries a schedule")
	}
	want := p.NextAttention(r)

	// The scheduler resolves a tenant's table and answers for a record; the
	// repository calls it. Here the interface is exercised directly, and the
	// wiring test lives in internal/record.
	got := lifecycle.NewScheduler(fixedPolicies{p}).NextAttention(context.Background(), "acme", r)
	if !got.Equal(want) {
		t.Fatalf("the scheduler answered %v, the policy table says %v", got, want)
	}
}

type fixedPolicies struct{ p *lifecycle.Policies }

func (f fixedPolicies) Policies(context.Context, tenant.ID) *lifecycle.Policies { return f.p }
