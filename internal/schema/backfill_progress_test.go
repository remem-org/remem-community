package schema_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/version"
)

// recorded reads a gauge series through the registry, and reports whether
// anything wrote it at all.
//
// The first version read it with WithLabelValues, which creates the series at
// zero when nothing has — so "records remaining is 0" held for a completed
// migration and for one that never reported, and that assertion passed against
// the code it was written to catch. Gathering sees only series that were set.
func recorded(t *testing.T, m *obs.Metrics, family, migration string) (float64, bool) {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != family {
			continue
		}
		for _, series := range f.GetMetric() {
			for _, l := range series.GetLabel() {
				if l.GetName() == "migration" && l.GetValue() == migration {
					return series.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// TestTheBackfillReportsItsProgress: the one migration this binary ships blocks
// start-up for a time proportional to the corpus, which is exactly when an
// operator wants to know how much is left. The runner reports progress only for
// a step that declares its size, and until Phase 13 no shipped step did — so
// remem_migration_progress_ratio and remem_migration_records_remaining were
// declared, registered, and could never be recorded by this binary.
func TestTheBackfillReportsItsProgress(t *testing.T) {
	const corpus = 2500 // three checkpoints at a batch of 1,000
	h := newBackfillHarness(t, corpus)
	ctx := context.Background()

	from := version.Current()
	from.AttrSchema = 1
	if _, err := schema.Open(ctx, h.kv, from); err != nil {
		t.Fatalf("stamping the directory at attr_schema 1: %v", err)
	}

	reg, err := schema.Builtin(version.Current(), h.indexer, nil)
	if err != nil {
		t.Fatal(err)
	}
	var step schema.Migration
	for _, m := range reg.All() {
		if m.ID == schema.AttrBackfillID {
			step = m
		}
	}
	if step.ID == "" {
		t.Fatalf("the shipped registry has no %q step", schema.AttrBackfillID)
	}

	metrics := obs.NewMetrics()
	runner, err := schema.NewRunner(schema.RunnerConfig{
		KV: h.kv, Tenants: h.dir, Clock: clock.NewFake(clock.FakeStart), Metrics: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Run(ctx, []schema.Migration{step}); err != nil {
		t.Fatalf("running the backfill: %v", err)
	}

	if got, ok := recorded(t, metrics, "remem_migration_progress_ratio", schema.AttrBackfillID); !ok || got != 1 {
		t.Errorf("a completed backfill: progress recorded=%v value=%v, want recorded and 1", ok, got)
	}
	if got, ok := recorded(t, metrics, "remem_migration_records_remaining", schema.AttrBackfillID); !ok || got != 0 {
		t.Errorf("a completed backfill: records remaining recorded=%v value=%v, want recorded and 0", ok, got)
	}
	if got := h.indexedRecords(t); got != corpus {
		t.Fatalf("the backfill indexed %d of %d records", got, corpus)
	}
}
