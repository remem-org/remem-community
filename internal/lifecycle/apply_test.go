package lifecycle_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/record"
)

const day = 24 * time.Hour

func kinds(r lifecycle.Result) []events.Kind {
	out := make([]events.Kind, 0, len(r.Notes))
	for _, n := range r.Notes {
		out = append(out, n.Kind)
	}
	return out
}

func TestShortTermExpiresAtItsTTL(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.ShortTerm, func(r *record.Record) { r.Fields.TTL = time.Hour })

	// A minute before, nothing is owed.
	if got := lifecycle.Apply(p, r.Clone(), epoch.Add(59*time.Minute)); got.Archived() {
		t.Fatalf("archived before the ttl elapsed: %+v", got.Notes)
	}

	got := lifecycle.Apply(p, r, epoch.Add(time.Hour))
	if !got.Changed {
		t.Fatalf("nothing happened at the ttl")
	}
	if !r.Fields.Archived {
		t.Errorf("the memory is not archived")
	}
	if !r.Fields.ArchivedAt.Equal(epoch.Add(time.Hour)) {
		t.Errorf("archived_at is %v, want the pass's instant", r.Fields.ArchivedAt)
	}
	if k := kinds(got); len(k) != 1 || k[0] != events.Expired {
		t.Errorf("wrote %v, want exactly one expired event", k)
	}
}

func TestShortTermWithoutATTLNeverExpires(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.ShortTerm, nil)

	// Ten years on, with health reset each pass so only expiry could act.
	for i := 1; i <= 10; i++ {
		r.Fields.Health = 100
		r.Fields.LastHealthCheckAt = epoch.Add(time.Duration(i) * 365 * day)
		lifecycle.Apply(p, r, epoch.Add(time.Duration(i)*365*day))
	}
	if r.Fields.Archived {
		t.Fatalf("a short-term memory with no ttl was archived. short_term is the default " +
			"policy, so this would archive every memory in the product. Go decides this " +
			"deliberately: Rust gives a short-term memory created without a TTL one of an " +
			"hour (memory_manager.rs:149), and Go does not — docs/PARITY.md")
	}
}

// A TTL is a short-term memory's and nothing else's. Rust expires only
// short-term memories (types.rs:1135-1145), and a record written before Phase 13
// — or a policy changed by PATCH — can carry a TTL under another policy. Under
// long_term it is inert: the expiry pass does not act on it, and the schedule
// does not wake the record for it.
func TestATTLOnARecordThatIsNotShortTermIsInert(t *testing.T) {
	p := policies(t)
	for _, policy := range []string{lifecycle.LongTerm, lifecycle.Pinned} {
		r := rec(policy, func(r *record.Record) { r.Fields.TTL = time.Hour })

		pol, _ := lifecycle.Builtin(policy)
		if due := lifecycle.EarliestTransition(pol, r); due.Equal(r.CreatedAt.Add(time.Hour)) {
			t.Fatalf("a %s memory is scheduled for its ttl at %v, which the expiry pass will not act on", policy, due)
		}

		got := lifecycle.Apply(p, r, epoch.Add(2*time.Hour))
		if r.Fields.Archived {
			t.Fatalf("a %s memory was archived by a ttl: %v", policy, kinds(got))
		}
		for _, k := range kinds(got) {
			if k == events.Expired || k == events.Promoted {
				t.Fatalf("a %s memory's ttl wrote a %s event", policy, k)
			}
		}
	}
}

func TestExpiredMemoryWithRecallsIsPromoted(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.ShortTerm, func(r *record.Record) {
		r.Fields.TTL = time.Hour
		r.Fields.AccessCount = 3 // lifecycle_manager.rs:57
	})

	got := lifecycle.Apply(p, r, epoch.Add(time.Hour))
	if r.Fields.Archived {
		t.Fatalf("a memory recalled three times was archived instead of promoted")
	}
	if r.Fields.Policy != lifecycle.LongTerm {
		t.Errorf("policy is %q, want %q", r.Fields.Policy, lifecycle.LongTerm)
	}
	if r.Fields.TTL != 0 {
		t.Errorf("the ttl survived the promotion: %v", r.Fields.TTL)
	}
	if k := kinds(got); len(k) != 1 || k[0] != events.Promoted {
		t.Errorf("wrote %v, want exactly one promoted event", k)
	}
}

func TestTwoRecallsAreNotEnoughToPromote(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.ShortTerm, func(r *record.Record) {
		r.Fields.TTL = time.Hour
		r.Fields.AccessCount = 2
	})
	lifecycle.Apply(p, r, epoch.Add(time.Hour))
	if !r.Fields.Archived {
		t.Fatalf("two recalls promoted a memory; the threshold is three and it is >=")
	}
}

// The plan's headline number: 0.995^days from last_decay_at, to 1e-6, over 365
// simulated days. Compared against the closed form rather than against a
// re-implementation of the loop.
func TestImportanceDecayMatchesRust(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.LongTerm, func(r *record.Record) { r.Fields.Importance = 0.8 })

	for d := 1; d <= 365; d++ {
		now := epoch.Add(time.Duration(d) * day)
		// Health is held up so that only importance decay is under test.
		// Without this the memory is archived on day 50 — 100 health at 2
		// points a day — and importance stops moving, which is correct
		// behaviour and is asserted in TestHealthDecayMatchesRust and
		// TestAnArchivedMemoryDoesNotDecay rather than here.
		r.Fields.Health = 100
		r.Fields.LastHealthCheckAt = now

		lifecycle.Apply(p, r, now)

		want := 0.8 * math.Pow(lifecycle.ImportanceDecayPerDay, float64(d))
		if diff := math.Abs(float64(r.Fields.Importance) - want); diff > 1e-6 {
			t.Fatalf("after %d days importance is %v, want %v (off by %v)",
				d, r.Fields.Importance, want, diff)
		}
	}
}

// Rust truncates to whole days and writes nothing for a pass under one
// (lifecycle_manager.rs:279-282). Without it a sweep rewrites the whole corpus
// on every run, and every comparison with Rust is off by part of a day's factor.
func TestDecayReasonsInWholeDays(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.LongTerm, func(r *record.Record) { r.Fields.Importance = 0.8 })

	got := lifecycle.Apply(p, r, epoch.Add(23*time.Hour+59*time.Minute))
	if got.Changed {
		t.Fatalf("a pass 23h59m after creation changed the record: %+v", got.Notes)
	}
	if r.Fields.Importance != 0.8 {
		t.Errorf("importance moved to %v inside the first day", r.Fields.Importance)
	}

	// And a pass at 47 hours applies one day's decay, not two.
	lifecycle.Apply(p, r, epoch.Add(47*time.Hour))
	want := 0.8 * lifecycle.ImportanceDecayPerDay
	if diff := math.Abs(float64(r.Fields.Importance) - want); diff > 1e-6 {
		t.Errorf("after 47 hours importance is %v, want one day's decay %v", r.Fields.Importance, want)
	}
}

func TestHealthDecayMatchesRust(t *testing.T) {
	p := policies(t)
	for _, tc := range []struct {
		policy string
		perDay float64
	}{
		{lifecycle.LongTerm, lifecycle.HealthDecayLongTerm},
		{lifecycle.ShortTerm, lifecycle.HealthDecayShortTerm},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			r := rec(tc.policy, nil)
			lifecycle.Apply(p, r, epoch.Add(3*day))
			want := 100 - tc.perDay*3
			if diff := math.Abs(float64(r.Fields.Health) - want); diff > 1e-6 {
				t.Fatalf("after three days health is %v, want %v", r.Fields.Health, want)
			}
		})
	}
}

// Rust's `importance_decay_does_not_reset_active_forgetting_clock`: the two
// passes keep separate clocks, so decaying importance must not push forgetting
// out by a day.
func TestImportanceDecayDoesNotResetTheForgettingClock(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.LongTerm, nil)

	lifecycle.Apply(p, r, epoch.Add(day))
	if !r.Fields.LastDecayAt.Equal(epoch.Add(day)) {
		t.Fatalf("last_decay_at is %v", r.Fields.LastDecayAt)
	}
	if !r.Fields.LastHealthCheckAt.Equal(epoch.Add(day)) {
		t.Fatalf("last_health_check_at is %v", r.Fields.LastHealthCheckAt)
	}
	if want := 100 - lifecycle.HealthDecayLongTerm; math.Abs(float64(r.Fields.Health)-want) > 1e-6 {
		t.Fatalf("health is %v, want %v — forgetting did not run alongside decay", r.Fields.Health, want)
	}
}

func TestDecayDoesNotTouchUpdatedAt(t *testing.T) {
	p := policies(t)

	t.Run("decay", func(t *testing.T) {
		r := rec(lifecycle.LongTerm, nil)
		lifecycle.Apply(p, r, epoch.Add(5*day))
		if !r.UpdatedAt.Equal(epoch) {
			t.Fatalf("updated_at moved to %v; a decay pass is not a content edit, and "+
				"ORDER BY updated_at would report every memory as recently edited", r.UpdatedAt)
		}
	})

	// And nor does archiving. Rust has to move it because cleanup selects on
	// it; Go schedules cleanup from archived_at, so the rule holds everywhere.
	t.Run("archive", func(t *testing.T) {
		r := rec(lifecycle.LongTerm, func(r *record.Record) { r.Fields.Health = 1 })
		lifecycle.Apply(p, r, epoch.Add(day))
		if !r.Fields.Archived {
			t.Fatalf("the fixture did not archive")
		}
		if !r.UpdatedAt.Equal(epoch) {
			t.Fatalf("archiving moved updated_at to %v", r.UpdatedAt)
		}
	})
}

func TestFlashbulbIsImmuneUntilItLapses(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.LongTerm, func(r *record.Record) {
		r.Fields.Importance = 0.8
		r.Fields.ProtectedUntil = epoch.Add(lifecycle.FlashbulbProtection)
	})

	// Twenty-nine days in, nothing has moved.
	lifecycle.Apply(p, r, epoch.Add(29*day))
	if r.Fields.Importance != 0.8 || r.Fields.Health != 100 {
		t.Fatalf("a protected memory decayed: importance %v, health %v",
			r.Fields.Importance, r.Fields.Health)
	}

	// Thirty-one days in, it decays again — and from creation, because the
	// protection deferred the pass rather than resetting the clock.
	lifecycle.Apply(p, r, epoch.Add(31*day))
	if r.Fields.Importance == 0.8 {
		t.Fatalf("the memory is still immune after the window lapsed")
	}
}

func TestHealthZeroArchivesNeverDeletes(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.LongTerm, func(r *record.Record) { r.Fields.Health = 1 })

	got := lifecycle.Apply(p, r, epoch.Add(day))
	if got.Delete {
		t.Fatalf("health reaching zero destroyed the record; no heuristic may (plan §II.10 row 8)")
	}
	if !r.Fields.Archived {
		t.Fatalf("health reached zero and the memory was not archived: health %v", r.Fields.Health)
	}
	if k := kinds(got); len(k) != 1 || k[0] != events.Archived {
		t.Errorf("wrote %v, want exactly one archived event", k)
	}
	if got.Notes[0].Actor != "active_forgetting" {
		t.Errorf("actor is %q", got.Notes[0].Actor)
	}
}

func TestNoConfigurationMakesAHeuristicHardDelete(t *testing.T) {
	// Every built-in, every health, every age short of the retention: nothing
	// deletes. The only Delete in the system follows an archive that is already
	// older than a stated retention.
	p := policies(t)
	for _, pol := range lifecycle.Builtins() {
		for _, health := range []float32{100, 50, 1, 0, -5} {
			r := rec(pol.Name, func(r *record.Record) { r.Fields.Health = health })
			for d := 1; d <= 60; d++ {
				if got := lifecycle.Apply(p, r, epoch.Add(time.Duration(d)*day)); got.Delete {
					if !r.Fields.Archived {
						t.Fatalf("%s deleted a live memory on day %d", pol.Name, d)
					}
				}
			}
		}
	}
}

func TestCleanupHardDeletesAfterRetention(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.LongTerm, func(r *record.Record) {
		r.Fields.Archived = true
		r.Fields.ArchivedAt = epoch
	})

	if got := lifecycle.Apply(p, r, epoch.Add(29*day)); got.Delete {
		t.Fatalf("deleted 29 days after archiving; the retention is %v", lifecycle.CleanupAfter)
	}

	got := lifecycle.Apply(p, r, epoch.Add(lifecycle.CleanupAfter))
	if !got.Delete {
		t.Fatalf("not deleted at the retention")
	}
	if k := kinds(got); len(k) != 1 || k[0] != events.HardDeleted {
		t.Errorf("wrote %v, want exactly one hard_deleted event", k)
	}
	if got.Notes[0].Actor != "cleanup" {
		t.Errorf("actor is %q, want cleanup", got.Notes[0].Actor)
	}
}

func TestPinnedIsNeverCleaned(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.Pinned, func(r *record.Record) {
		r.Fields.Archived = true
		r.Fields.ArchivedAt = epoch
	})
	for _, d := range []int{30, 365, 3650} {
		if got := lifecycle.Apply(p, r, epoch.Add(time.Duration(d)*day)); got.Delete {
			t.Fatalf("a pinned memory was deleted %d days after archiving; "+
				"CleanupAfter is nil, which means never", d)
		}
	}
}

func TestAnArchivedMemoryDoesNotDecay(t *testing.T) {
	p := policies(t)
	r := rec(lifecycle.LongTerm, func(r *record.Record) {
		r.Fields.Archived = true
		r.Fields.ArchivedAt = epoch
		r.Fields.Importance = 0.8
	})
	lifecycle.Apply(p, r, epoch.Add(5*day))
	if r.Fields.Importance != 0.8 {
		t.Fatalf("an archived memory's importance decayed to %v; every transition but "+
			"cleanup skips an archived record", r.Fields.Importance)
	}
}

func TestAnUnknownPolicyChangesNothing(t *testing.T) {
	p := policies(t)
	r := rec("written_by_a_newer_binary", func(r *record.Record) {
		r.Fields.Importance = 0.8
		r.Fields.TTL = time.Minute
	})
	for _, d := range []int{1, 30, 365} {
		got := lifecycle.Apply(p, r, epoch.Add(time.Duration(d)*day))
		if got.Changed || got.Delete {
			t.Fatalf("a policy this binary cannot read acted on day %d: %+v", d, got)
		}
	}
}

// Rust's `every_sweeps_due_time_is_covered_by_the_projection`, and the reason
// it matters: a transition that can fire before the schedule says so is a
// memory that gets it late, or never.
//
// This asks the transitions themselves — by running Apply forward over a fine
// grid and noting the first instant anything happens — rather than asking the
// projection twice.
func TestEverySweepConditionIsCoveredByTheSchedule(t *testing.T) {
	p := policies(t)
	shapes := []struct {
		name string
		mut  func(*record.Record)
	}{
		{"fresh", nil},
		{"with a ttl", func(r *record.Record) { r.Fields.TTL = 90 * time.Minute }},
		{"with a ttl and recalls", func(r *record.Record) {
			r.Fields.TTL, r.Fields.AccessCount = 90*time.Minute, 5
		}},
		{"recalled", func(r *record.Record) { r.Fields.LastRecalledAt = epoch.Add(time.Hour) }},
		{"decayed", func(r *record.Record) { r.Fields.LastDecayAt = epoch.Add(time.Hour) }},
		{"checked", func(r *record.Record) { r.Fields.LastHealthCheckAt = epoch.Add(time.Hour) }},
		{"protected", func(r *record.Record) { r.Fields.ProtectedUntil = epoch.Add(2 * day) }},
		{"nearly forgotten", func(r *record.Record) { r.Fields.Health = 1 }},
		{"archived", func(r *record.Record) {
			r.Fields.Archived, r.Fields.ArchivedAt = true, epoch
		}},
	}

	const step = 15 * time.Minute
	for _, pol := range lifecycle.Builtins() {
		for _, shape := range shapes {
			t.Run(pol.Name+"/"+shape.name, func(t *testing.T) {
				scheduled := p.NextAttention(rec(pol.Name, shape.mut))

				var fired time.Time
				for at := epoch; at.Before(epoch.Add(40 * day)); at = at.Add(step) {
					probe := rec(pol.Name, shape.mut)
					if got := lifecycle.Apply(p, probe, at); got.Changed || got.Delete {
						fired = at
						break
					}
				}
				if fired.IsZero() {
					return // nothing is ever owed for this shape
				}
				if scheduled.After(fired) {
					t.Fatalf("%s/%s fires by %v but is scheduled for %v — it would be visited "+
						"%v late", pol.Name, shape.name, fired, scheduled, scheduled.Sub(fired))
				}
			})
		}
	}
}

// A short retention must not report itself as no retention at all.
//
// Phase 10's end-to-end run set a five-second cleanup on a tenant and read back
// "archived for 0s, past this policy's 5s retention" — which reads as a memory
// deleted the instant it was retired. The duration was rounded to the hour,
// which is invisible against the thirty-day default and useless against
// anything an operator would set to watch the behaviour.
func TestTheCleanupReasonSurvivesAShortRetention(t *testing.T) {
	short := 5 * time.Second
	over, _ := lifecycle.Builtin(lifecycle.ShortTerm)
	over.CleanupAfter = &short
	p, err := lifecycle.NewPolicies(map[string]lifecycle.Policy{lifecycle.ShortTerm: over})
	if err != nil {
		t.Fatalf("NewPolicies: %v", err)
	}

	r := rec(lifecycle.ShortTerm, func(r *record.Record) {
		r.Fields.Archived, r.Fields.ArchivedAt = true, epoch
	})
	got := lifecycle.Apply(p, r, epoch.Add(9*time.Second))
	if !got.Delete {
		t.Fatalf("not deleted nine seconds into a five-second retention")
	}
	if reason := got.Notes[0].Reason; !strings.Contains(reason, "9s") {
		t.Fatalf("the reason is %q; it should say how long the memory was archived for", reason)
	}
}
