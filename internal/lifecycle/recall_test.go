package lifecycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
)

func (h *harness) recall(t *testing.T, rid id.ID, at time.Time) bool {
	t.Helper()
	wrote, err := lifecycle.Record(context.Background(), h.store, h.kv,
		events.Scope{Tenant: acme, Namespace: tenant.DefaultNamespace, Subject: rid},
		at, lifecycle.DefaultRecallWindow, false)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	return wrote
}

// A flag, not a counter: ten fetches inside the window register one recall
// (behaviour baseline §3).
func TestAccessCountCountsSessionsNotRoundTrips(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.LongTerm, nil))

	for i := 0; i < 10; i++ {
		h.recall(t, r.ID, epoch.Add(time.Duration(i)*time.Second))
	}
	h.sweep(t, epoch.Add(2*day))

	got, err := h.get(t, r.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Fields.AccessCount != 1 {
		t.Fatalf("ten fetches inside a %v window registered %d recalls, want 1",
			lifecycle.DefaultRecallWindow, got.Fields.AccessCount)
	}
}

func TestRecallsOutsideTheWindowEachCount(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.LongTerm, nil))

	for i := 0; i < 3; i++ {
		at := epoch.Add(time.Duration(i) * 2 * lifecycle.DefaultRecallWindow)
		if !h.recall(t, r.ID, at) {
			t.Fatalf("the recall at %v was coalesced away", at)
		}
	}
	h.sweep(t, epoch.Add(2*day))

	got, _ := h.get(t, r.ID)
	if got.Fields.AccessCount != 3 {
		t.Fatalf("access count is %d, want 3", got.Fields.AccessCount)
	}
}

// Rust's window is a process-local flush timer, so a restart resets it and two
// recalls either side of one count twice. Here it is decided against the
// stream, so nothing about this process is involved.
func TestTheCoalescingWindowSurvivesARestart(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.LongTerm, nil))

	h.recall(t, r.ID, epoch)

	// A second store over the same data — every process-local state gone.
	restarted := events.NewStore(h.kv)
	wrote, err := lifecycle.Record(context.Background(), restarted, h.kv,
		events.Scope{Tenant: acme, Namespace: tenant.DefaultNamespace, Subject: r.ID},
		epoch.Add(time.Second), lifecycle.DefaultRecallWindow, false)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if wrote {
		t.Fatalf("a recall one second after another counted, across a restart; " +
			"the window is meant to be a fact about the data")
	}
}

// The watermark is last_recalled_at, so a second fold sees nothing new. Without
// that, every sweep would re-count every recall the memory ever had.
func TestTheFoldIsExactlyOnce(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.LongTerm, nil))

	for i := 0; i < 3; i++ {
		h.recall(t, r.ID, epoch.Add(time.Duration(i)*2*lifecycle.DefaultRecallWindow))
	}

	h.sweep(t, epoch.Add(2*day))
	first, _ := h.get(t, r.ID)
	h.sweep(t, epoch.Add(4*day))
	second, _ := h.get(t, r.ID)

	if first.Fields.AccessCount != 3 {
		t.Fatalf("the first fold counted %d recalls, want 3", first.Fields.AccessCount)
	}
	if second.Fields.AccessCount != 3 {
		t.Fatalf("the second sweep re-counted, reaching %d", second.Fields.AccessCount)
	}
}

// A recall reinforces health by ten points, clamped to the top of the scale
// (services/recall.rs:24,67).
func TestARecallReinforcesHealth(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.LongTerm, func(r *record.Record) { r.Fields.Health = 50 }))

	h.recall(t, r.ID, epoch)
	h.sweep(t, epoch.Add(day))

	got, _ := h.get(t, r.ID)
	// Fifty, plus ten for the recall, minus one day of long-term decay. The
	// fold runs first, which is what lets the decision read a memory somebody
	// used as used rather than as untouched.
	want := float32(50 + lifecycle.RecallHealthBoost - lifecycle.HealthDecayLongTerm)
	if got.Fields.Health != want {
		t.Fatalf("health is %v, want %v", got.Fields.Health, want)
	}
}

// The event is durable the instant the recall happens, which is what the
// completion criterion asks for and what Rust gives up. Nothing here needs the
// sweep to have run.
func TestRecallIsDurableBeforeAnythingFoldsIt(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.LongTerm, nil))
	h.recall(t, r.ID, epoch)

	// A fresh reader over the same store, as a restarted process would have.
	got := h.history(t, r.ID)
	if len(got) != 1 || got[0].Kind != events.Recalled {
		t.Fatalf("the recall is not in the stream: %+v", got)
	}

	// And the record has not been touched, which is the other half of the
	// trade: one appended key instead of a record rewrite.
	rec, _ := h.get(t, r.ID)
	if rec.Fields.AccessCount != 0 {
		t.Fatalf("the recall rewrote the record: access count %d", rec.Fields.AccessCount)
	}
}

// The fold happens before the pass decides. An unfolded recall is a memory
// somebody used that still looks untouched, so a decision taken first would
// archive a memory that had just been read.
func TestTheFoldRunsBeforeTheDecision(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.ShortTerm, func(r *record.Record) {
		r.Fields.TTL = time.Hour
	}))

	// Three recalls, each outside the window, so the memory earns promotion.
	for i := 0; i < 3; i++ {
		h.recall(t, r.ID, epoch.Add(time.Duration(i)*2*lifecycle.DefaultRecallWindow))
	}
	h.sweep(t, epoch.Add(2*time.Hour))

	got, err := h.get(t, r.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Fields.Archived {
		t.Fatalf("a memory recalled three times was archived as unused; the fold " +
			"arrived after the decision")
	}
	if got.Fields.Policy != lifecycle.LongTerm {
		t.Fatalf("policy is %q, want promotion to %q", got.Fields.Policy, lifecycle.LongTerm)
	}
}
