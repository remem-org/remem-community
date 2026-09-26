package lifecycle_test

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/record"
)

// decayReference is fixtures/decay_reference.json.
type decayReference struct {
	Source    string  `json:"source"`
	Generator string  `json:"generator"`
	Tolerance float64 `json:"tolerance"`
	Constants struct {
		ImportanceDecayPerDay float64 `json:"importance_decay_per_day"`
		HealthDecayShortTerm  float64 `json:"health_decay_short_term"`
		HealthDecayLongTerm   float64 `json:"health_decay_long_term"`
		DayMS                 int64   `json:"day_ms"`
	} `json:"constants"`
	Scenarios []struct {
		Name              string  `json:"name"`
		Policy            string  `json:"policy"`
		StepHours         int64   `json:"step_hours"`
		Reinforced        bool    `json:"reinforced"`
		InitialImportance float64 `json:"initial_importance"`
		InitialHealth     float64 `json:"initial_health"`
		Passes            []struct {
			AtMS       int64   `json:"at_ms"`
			Importance float64 `json:"importance"`
			Health     float64 `json:"health"`
		} `json:"passes"`
	} `json:"scenarios"`
}

func loadReference(t *testing.T) decayReference {
	t.Helper()
	path := filepath.Join("..", "..", "fixtures", "decay_reference.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var ref decayReference
	if err := json.Unmarshal(b, &ref); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	if len(ref.Scenarios) == 0 {
		t.Fatalf("%s holds no scenarios; a fixture that asserts nothing is worse than none", path)
	}
	return ref
}

// The plan's headline completion criterion: decay arithmetic matches Rust to
// 1e-6 over 365 simulated days.
//
// # What this compares, and what it does not
//
// The fixture is Rust's arithmetic — the four expressions from
// services/lifecycle_manager.rs, transcribed with their line numbers and
// compiled by rustc with the same f32 semantics and the same integer
// truncation. It is not a run of the Rust server, because remem-development is
// frozen at pre-go-freeze and a comparison made against a modified tree is
// worth less than one made against the tag.
//
// So this holds the arithmetic and not the *selection*: which memories each
// sweep chooses to act on is not compared here. That is the differential
// harness's lifecycle axis, it is not built, and Phase 13 owns it —
// test/differential/COVERAGE.md says so rather than leaving it to be assumed.
//
// The irregular-interval scenarios are the ones worth having. Rust records each
// pass at `now` rather than at the last whole-day boundary, so the sub-day
// remainder is discarded and decay is slightly slower than continuous. A Go
// implementation that carried the remainder would agree on every daily scenario
// and diverge on every other one.
func TestImportanceAndHealthDecayMatchTheRustReference(t *testing.T) {
	ref := loadReference(t)

	// The fixture's own constants have to match this binary's, or the
	// comparison is between two systems that were never asked the same
	// question.
	if ref.Constants.ImportanceDecayPerDay != lifecycle.ImportanceDecayPerDay {
		t.Fatalf("the reference decays importance at %v, this binary at %v",
			ref.Constants.ImportanceDecayPerDay, lifecycle.ImportanceDecayPerDay)
	}
	if ref.Constants.HealthDecayLongTerm != lifecycle.HealthDecayLongTerm {
		t.Fatalf("the reference decays long-term health at %v, this binary at %v",
			ref.Constants.HealthDecayLongTerm, lifecycle.HealthDecayLongTerm)
	}
	if ref.Constants.HealthDecayShortTerm != lifecycle.HealthDecayShortTerm {
		t.Fatalf("the reference decays short-term health at %v, this binary at %v",
			ref.Constants.HealthDecayShortTerm, lifecycle.HealthDecayShortTerm)
	}

	table := policies(t)
	tolerance := ref.Tolerance
	if tolerance <= 0 {
		tolerance = 1e-6
	}

	for _, sc := range ref.Scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			created := time.UnixMilli(0).UTC()
			r := &record.Record{
				ID:      idFor(sc.Name),
				Tenant:  "acme",
				Type:    record.TypeMemory,
				Content: "a memory",
				Fields: record.Fields{
					Policy:     sc.Policy,
					Importance: float32(sc.InitialImportance),
					Health:     float32(sc.InitialHealth),
				},
				CreatedAt: created,
				UpdatedAt: created,
			}

			compared := 0
			for i, want := range sc.Passes {
				now := time.UnixMilli(want.AtMS).UTC()

				// A reinforced scenario models a memory somebody keeps using.
				// The reference restores full health at each pass; here that
				// stands in for the fold, whose own arithmetic — ten points per
				// recall, clamped to the top of the scale — is asserted by
				// TestARecallReinforcesHealth rather than duplicated into this
				// comparison.
				if sc.Reinforced {
					r.Fields.Health = float32(lifecycle.MaxHealth)
					r.Fields.LastHealthCheckAt = now
				}

				lifecycle.Apply(table, r, now)
				compared++

				if d := math.Abs(float64(r.Fields.Importance) - want.Importance); d > tolerance {
					t.Fatalf("pass %d (%v): importance is %v, Rust has %v (off by %v)",
						i+1, now, r.Fields.Importance, want.Importance, d)
				}
				if d := math.Abs(float64(r.Fields.Health) - want.Health); d > tolerance {
					t.Fatalf("pass %d (%v): health is %v, Rust has %v (off by %v)",
						i+1, now, r.Fields.Health, want.Health, d)
				}
				if r.Fields.Archived {
					// Health reached zero and the memory was retired, which
					// stops both decays. The two exhaustion scenarios exist to
					// reach exactly this point; the reinforced ones must not.
					if sc.Reinforced {
						t.Fatalf("a reinforced memory was archived at pass %d", i+1)
					}
					break
				}
			}

			// A comparison that stopped early compares less than it claims to.
			// Without this, a change that archived memories sooner would make
			// the 365-day scenario silently assert fifty days.
			if want := len(sc.Passes); sc.Reinforced && compared != want {
				t.Fatalf("compared %d of %d passes", compared, want)
			}
			if compared < 10 {
				t.Fatalf("compared only %d passes; this scenario asserts almost nothing", compared)
			}
		})
	}
}

// The 365-day scenario is the one the completion criterion names, so its length
// is asserted rather than assumed: a fixture regenerated with fewer passes would
// otherwise quietly stop testing what was promised.
func TestTheReferenceCoversAFullYear(t *testing.T) {
	ref := loadReference(t)
	for _, sc := range ref.Scenarios {
		if sc.Name == "long_term_daily_365" {
			if len(sc.Passes) != 365 {
				t.Fatalf("the 365-day scenario holds %d passes", len(sc.Passes))
			}
			return
		}
	}
	t.Fatal("the reference has no long_term_daily_365 scenario")
}

// idFor derives a stable id from a name, so a failure names a scenario rather
// than a fresh uuid.
func idFor(name string) (out [16]byte) {
	copy(out[:], name)
	// Any non-zero id will do; the byte forces one for a short name.
	out[15] = 1
	return out
}
