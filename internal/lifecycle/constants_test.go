package lifecycle_test

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/lifecycle"
)

// Rust's lifecycle numbers, pinned as literals.
//
// The behaviour tests use the constants by name, which is right for them and
// means a changed constant changes the test's expectation along with the code,
// so nothing fails. This test is where a changed number fails. Every value was
// read from remem-development at pre-go-freeze, and docs/PARITY.md names this
// test for each (BEHAVIOUR_BASELINE.md §8).
func TestLifecycleConstantsAreRustsNumbers(t *testing.T) {
	for _, c := range []struct {
		name      string
		got, want any
		where     string
	}{
		{"daily importance decay", lifecycle.ImportanceDecayPerDay, 0.995, "lifecycle_manager.rs:251"},
		{"recall health boost", float64(lifecycle.RecallHealthBoost), 10.0, "recall.rs:24"},
		{"cleanup after", lifecycle.CleanupAfter, 30 * 24 * time.Hour, "attrs.rs:38"},
		{"flashbulb arousal", lifecycle.FlashbulbArousal, float32(0.8), "memory_manager.rs:114"},
		{"flashbulb protection", lifecycle.FlashbulbProtection, 30 * 24 * time.Hour, "memory_manager.rs:115"},
		{"default run budget", lifecycle.DefaultRunBudget, 10_000, "lifecycle_manager.rs:38"},
		{"sweep effort factor", lifecycle.SweepEffortFactor, 8, "lifecycle_manager.rs:45"},
		{"promote at recalls", lifecycle.PromoteAtRecalls, uint32(3), "lifecycle_manager.rs:58"},
	} {
		if c.got != c.want {
			t.Errorf("%s is %v, want Rust's %v (%s)", c.name, c.got, c.want, c.where)
		}
	}
}
