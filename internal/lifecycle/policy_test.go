package lifecycle_test

import (
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/lifecycle"
)

// The built-ins reproduce Rust's constants. Every number here was read from
// crates/remem-server/src/services/lifecycle_manager.rs at pre-go-freeze, not
// from plan §II.6 — whose table gets two of them wrong.
func TestBuiltinsMatchTheRustConstants(t *testing.T) {
	tests := []struct {
		name            string
		ttl             *time.Duration
		promoteAt       *uint32
		promoteTo       string
		importanceDecay float64
		healthDecay     float64
		archiveAt       *float32
		cleanupAfter    *time.Duration
	}{{
		// lifecycle_manager.rs:57 (promote_threshold), :265 (importance decay
		// is long-term only), :420-423 (health decay 8.0 short-term),
		// attrs.rs:38 (cleanup after 30 days). The TTL comes from the record
		// and Go gives it no default. That is a decision, not a copy of Rust:
		// Rust applies an hour at create (memory_manager.rs:149), and Go does
		// not, because short_term is the default policy and a default would
		// archive every unrecalled memory an hour after it was written.
		// docs/PARITY.md records it.
		name: "short_term", ttl: nil,
		promoteAt: u32(3), promoteTo: "long_term",
		importanceDecay: 1.0, healthDecay: 8.0,
		archiveAt: f32(0), cleanupAfter: dur(30 * 24 * time.Hour),
	}, {
		name: "long_term", ttl: nil,
		promoteAt: nil, promoteTo: "",
		importanceDecay: 0.995, healthDecay: 2.0,
		archiveAt: f32(0), cleanupAfter: dur(30 * 24 * time.Hour),
	}, {
		// pinned is new and costs nothing: nothing about it moves.
		name: "pinned", ttl: nil,
		promoteAt: nil, promoteTo: "",
		importanceDecay: 1.0, healthDecay: 0,
		archiveAt: nil, cleanupAfter: nil,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := lifecycle.Builtin(tc.name)
			if !ok {
				t.Fatalf("%s is not a built-in policy", tc.name)
			}
			if !sameDur(p.TTL, tc.ttl) {
				t.Errorf("TTL = %v, want %v", p.TTL, tc.ttl)
			}
			if !sameU32(p.PromoteAtRecalls, tc.promoteAt) {
				t.Errorf("PromoteAtRecalls = %v, want %v", p.PromoteAtRecalls, tc.promoteAt)
			}
			if p.PromoteTo != tc.promoteTo {
				t.Errorf("PromoteTo = %q, want %q", p.PromoteTo, tc.promoteTo)
			}
			if p.ImportanceDecay != tc.importanceDecay {
				t.Errorf("ImportanceDecay = %v, want %v", p.ImportanceDecay, tc.importanceDecay)
			}
			if p.HealthDecay != tc.healthDecay {
				t.Errorf("HealthDecay = %v, want %v", p.HealthDecay, tc.healthDecay)
			}
			if !sameF32(p.ArchiveAtHealth, tc.archiveAt) {
				t.Errorf("ArchiveAtHealth = %v, want %v", p.ArchiveAtHealth, tc.archiveAt)
			}
			if !sameDur(p.CleanupAfter, tc.cleanupAfter) {
				t.Errorf("CleanupAfter = %v, want %v", p.CleanupAfter, tc.cleanupAfter)
			}
		})
	}
}

// The one number plan §II.6 gets most wrong, pinned by name so that a future
// edit toward the plan's table fails here with the reason attached.
func TestLongTermHealthDecaysAtTwoPointsADayNotOne(t *testing.T) {
	p, _ := lifecycle.Builtin("long_term")
	if p.HealthDecay != 2.0 {
		t.Fatalf("long_term health decay is %v; Rust decays 2.0/day "+
			"(lifecycle_manager.rs:422). Plan §II.6's table says 1.0, which would let an "+
			"untouched memory survive 100 days instead of 50", p.HealthDecay)
	}
	s, _ := lifecycle.Builtin("short_term")
	if s.HealthDecay != 8.0 {
		t.Fatalf("short_term health decay is %v; Rust decays 8.0/day "+
			"(lifecycle_manager.rs:421). Plan §II.6's table says 0, which would make a "+
			"short-term memory without a TTL immortal", s.HealthDecay)
	}
}

func TestShortTermHasNoDefaultTTL(t *testing.T) {
	p, _ := lifecycle.Builtin("short_term")
	if p.TTL != nil {
		t.Fatalf("short_term carries a default TTL of %v. short_term is the default policy "+
			"and nothing in the product sets a TTL, so a default would archive every memory "+
			"%v after it was written. That is what Rust does — one hour, applied at create "+
			"(memory_manager.rs:149) — and Go's not doing it is a decision recorded in "+
			"docs/PARITY.md, not a copy of Rust", *p.TTL, *p.TTL)
	}
}

func TestPolicyOverridePerTenant(t *testing.T) {
	base, _ := lifecycle.Builtin("long_term")
	slower := base
	slower.ImportanceDecay = 0.999

	a, err := lifecycle.NewPolicies(nil)
	if err != nil {
		t.Fatalf("NewPolicies: %v", err)
	}
	b, err := lifecycle.NewPolicies(map[string]lifecycle.Policy{"long_term": slower})
	if err != nil {
		t.Fatalf("NewPolicies with an override: %v", err)
	}

	pa, err := a.Get("long_term")
	if err != nil {
		t.Fatalf("tenant A: %v", err)
	}
	pb, err := b.Get("long_term")
	if err != nil {
		t.Fatalf("tenant B: %v", err)
	}
	if pa.ImportanceDecay != 0.995 {
		t.Errorf("tenant A's long_term decays at %v, want the built-in 0.995", pa.ImportanceDecay)
	}
	if pb.ImportanceDecay != 0.999 {
		t.Errorf("tenant B's long_term decays at %v, want its override 0.999", pb.ImportanceDecay)
	}

	// An override replaces one policy, not the table. The others stay built-in.
	if p, err := b.Get("short_term"); err != nil || p.HealthDecay != 8.0 {
		t.Errorf("tenant B's short_term is %+v (%v), want the built-in", p, err)
	}
}

func TestAPolicyOverrideNamingNothingIsRefused(t *testing.T) {
	base, _ := lifecycle.Builtin("short_term")
	broken := base
	broken.PromoteTo = "medium_term"

	_, err := lifecycle.NewPolicies(map[string]lifecycle.Policy{"short_term": broken})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("an override promoting into an unknown policy is %v, want Invalid", err)
	}
	if err == nil || !strings.Contains(err.Error(), "medium_term") {
		t.Errorf("the refusal does not name the missing policy: %v", err)
	}
}

func TestAnOverrideIsRefusedAtWriteTimeNotAtSweepTime(t *testing.T) {
	tests := []struct {
		name   string
		break_ func(*lifecycle.Policy)
		want   string
	}{
		{"decay above one", func(p *lifecycle.Policy) { p.ImportanceDecay = 1.5 }, "importance"},
		{"decay at zero", func(p *lifecycle.Policy) { p.ImportanceDecay = 0 }, "importance"},
		{"negative health decay", func(p *lifecycle.Policy) { p.HealthDecay = -1 }, "health"},
		{"negative ttl", func(p *lifecycle.Policy) { p.TTL = dur(-time.Hour) }, "ttl"},
		{"archive above the scale", func(p *lifecycle.Policy) { p.ArchiveAtHealth = f32(500) }, "archive"},
		{"cleanup at zero", func(p *lifecycle.Policy) { p.CleanupAfter = dur(0) }, "cleans up"},
		{"promotion with nowhere to go", func(p *lifecycle.Policy) {
			p.PromoteAtRecalls, p.PromoteTo = u32(3), ""
		}, "promote"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := lifecycle.Builtin("long_term")
			tc.break_(&p)
			_, err := lifecycle.NewPolicies(map[string]lifecycle.Policy{"long_term": p})
			if !errs.Is(err, errs.Invalid) {
				t.Fatalf("got %v, want Invalid — a typo that surfaces as \"nothing decays\" "+
					"is a typo nobody finds", err)
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Errorf("the refusal does not mention %q: %v", tc.want, err)
			}
		})
	}
}

// A policy name this binary does not know is a rolling upgrade, not a bug, and
// the only safe response to "I do not know what this memory's rules are" is to
// leave it alone.
func TestAnUnknownPolicyIsInert(t *testing.T) {
	p, err := lifecycle.NewPolicies(nil)
	if err != nil {
		t.Fatalf("NewPolicies: %v", err)
	}

	if _, err := p.Get("written_by_a_newer_binary"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("Get on an unknown policy is %v, want NotFound", err)
	}

	inert := p.Resolve("written_by_a_newer_binary")
	switch {
	case inert.TTL != nil:
		t.Error("an unknown policy expires memories")
	case inert.ImportanceDecay != 1.0:
		t.Errorf("an unknown policy decays importance at %v", inert.ImportanceDecay)
	case inert.HealthDecay != 0:
		t.Errorf("an unknown policy decays health at %v", inert.HealthDecay)
	case inert.ArchiveAtHealth != nil:
		t.Error("an unknown policy archives memories")
	case inert.CleanupAfter != nil:
		t.Error("an unknown policy deletes memories")
	case inert.PromoteAtRecalls != nil:
		t.Error("an unknown policy promotes memories")
	}
}

// An empty policy on a record is the default, not an unknown one: records
// written before Phase 5 carry no policy at all.
func TestAnEmptyPolicyResolvesToTheDefault(t *testing.T) {
	p, _ := lifecycle.NewPolicies(nil)
	got := p.Resolve("")
	want := p.Resolve("short_term")
	if got.Name != want.Name {
		t.Fatalf("an empty policy resolves to %q, want %q", got.Name, want.Name)
	}
}

func u32(n uint32) *uint32               { return &n }
func f32(f float32) *float32             { return &f }
func dur(d time.Duration) *time.Duration { return &d }

func sameDur(a, b *time.Duration) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
func sameU32(a, b *uint32) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
func sameF32(a, b *float32) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
