package schema_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/version"
)

// recordingReindexer records which tenants a rebuild was queued for, and can
// refuse one tenant once, to interrupt a run.
type recordingReindexer struct {
	queued []tenant.ID
	failOn tenant.ID
}

func (r *recordingReindexer) StageTextRebuild(_ context.Context, _ txn.Tx, t tenant.ID) error {
	if t == r.failOn {
		r.failOn = ""
		return errors.New("interrupted")
	}
	r.queued = append(r.queued, t)
	return nil
}

// A directory written before the text index was versioned queues one keyword
// index rebuild per tenant, once, and then carries text_index 1.
//
// Phase 13 moved golang.org/x/text to v0.39.0 for GO-2026-5970. On Go 1.27 that
// version normalises with Unicode 17 tables where v0.25.0 used Unicode 15, and
// the tokeniser's normalisation decides what is written into the postings key
// space. So a posting written before the upgrade can disagree with the term a
// query normalises to after it. A rebuild re-derives every posting under the
// new rules, and the format version is what makes it happen once rather than
// never or at every start.
func TestAnUpgradeQueuesOneKeywordRebuildPerTenant(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)
	dir := tenantkv.New(kv, clk)
	tenants := []tenant.ID{"acme", "globex", "initech"}
	for _, id := range tenants {
		if _, err := tenant.Ensure(tenant.NewContext(ctx, id), dir, id); err != nil {
			t.Fatal(err)
		}
	}

	// Every format current except the text index, which such a directory never
	// recorded.
	f := schema.CurrentFormat(version.Current())
	delete(f.Subsystems, "text_index")
	if err := schema.WriteFormat(ctx, kv, f); err != nil {
		t.Fatal(err)
	}

	reindexer := &recordingReindexer{failOn: "globex"}
	run := func() error {
		t.Helper()
		format, err := schema.ReadFormat(ctx, kv)
		if err != nil {
			t.Fatal(err)
		}
		reg, err := schema.Builtin(version.Current(), nil, reindexer)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := reg.Plan(format, version.Current())
		if err != nil {
			t.Fatal(err)
		}
		runner, err := schema.NewRunner(schema.RunnerConfig{KV: kv, Tenants: dir, Clock: clk, DataDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		return runner.Run(ctx, plan)
	}

	// The first run is interrupted at the second tenant, after the first
	// tenant's rebuild was queued.
	if err := run(); err == nil {
		t.Fatal("the interrupted run reported success")
	}
	// The second resumes: the first tenant is not queued twice.
	if err := run(); err != nil {
		t.Fatalf("the resumed run: %v", err)
	}
	if !slices.Equal(reindexer.queued, tenants) {
		t.Fatalf("rebuilds were queued for %v, want each of %v exactly once", reindexer.queued, tenants)
	}

	format, err := schema.ReadFormat(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if got := format.Subsystems["text_index"].Current; got != version.TextIndex {
		t.Fatalf("text_index is %d after the migration, want %d", got, version.TextIndex)
	}
	// And a third start has nothing to do.
	reindexer.queued = nil
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(reindexer.queued) != 0 {
		t.Fatalf("a directory already at text_index %d queued %v again", version.TextIndex, reindexer.queued)
	}
}
