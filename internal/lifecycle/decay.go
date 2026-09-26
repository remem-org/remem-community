package lifecycle

import (
	"math"
	"time"

	"github.com/remem-org/remem-go/internal/record"
)

// decayImportance applies the per-day multiplier for every whole day since the
// last pass.
//
// # Two clocks, not one
//
// It advances `last_decay_at` and nothing else. Active forgetting keeps its own
// `last_health_check_at`, and Rust has a test named for exactly this —
// `importance_decay_does_not_reset_active_forgetting_clock` — because one
// shared clock would let a decay pass push forgetting out by a day every time
// it ran, and a memory nobody touched would never be forgotten.
//
// # It is set to now, not to the last whole boundary
//
// A pass 47 hours after the last one applies one day's decay and records the
// pass at 47 hours, discarding the 23-hour remainder. That makes decay slightly
// slower than continuous, and it is what Rust does
// (lifecycle_manager.rs:287) — so it is what the differential comparison
// expects. Carrying the remainder would be more accurate and would not match.
//
// # No event
//
// Decay is a continuous function of time, not a transition. `importance`,
// `health`, `last_decay_at` and `last_health_check_at` say exactly what it has
// done and when, deterministically, so a row per memory per pass would restate
// the record — 365 million rows a year at a million memories. What decay
// *causes* is an archive, and that is an event.
func decayImportance(pol Policy, rec *record.Record, now time.Time, out *Result) {
	if pol.ImportanceDecay >= 1 {
		return
	}
	from := rec.Fields.LastDecayAt
	if from.IsZero() {
		from = rec.CreatedAt
	}
	days := wholeDays(from, now)
	if days == 0 {
		return
	}

	// Computed in float64 and stored in float32. Rust multiplies in f32; over
	// 365 days the two differ by about 1e-8 on an importance of 0.5, which is
	// two orders of magnitude inside the 1e-6 the comparison asks for, and
	// doing the arithmetic in the wider type is the half that is defensible on
	// its own terms.
	decayed := float64(rec.Fields.Importance) * math.Pow(pol.ImportanceDecay, float64(days))
	rec.Fields.Importance = float32(math.Max(decayed, 0))
	rec.Fields.LastDecayAt = now
	out.Changed = true
}

// decayHealth removes points of health for every whole day since the memory was
// last reinforced or last checked, whichever is later.
//
// The clock deliberately excludes `updated_at`. Rust says why at
// lifecycle_manager.rs:409-415: updated_at moves on the lifecycle's own
// schedule rather than because the memory was recalled or edited, so counting
// it as reinforcement would mean a memory nobody has touched keeps healing
// itself every time a sweep looks at it.
//
// `accessed_at` stands in when there has been no genuine recall, which is
// Rust's `last_recalled_at.unwrap_or(accessed_at)`.
func decayHealth(pol Policy, rec *record.Record, now time.Time, out *Result) {
	if pol.HealthDecay <= 0 {
		return
	}
	reinforced := rec.Fields.LastRecalledAt
	if reinforced.IsZero() {
		reinforced = rec.Fields.AccessedAt
	}
	checked := rec.Fields.LastHealthCheckAt
	if checked.IsZero() {
		checked = rec.CreatedAt
	}
	days := wholeDays(later(reinforced, checked), now)
	if days == 0 {
		return
	}

	health := float64(rec.Fields.Health) - pol.HealthDecay*float64(days)
	rec.Fields.Health = float32(math.Min(math.Max(health, 0), float64(MaxHealth)))
	rec.Fields.LastHealthCheckAt = now
	out.Changed = true
}
