package main

import (
	"context"
	"fmt"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/version"
)

// upgradePassageFormat runs the server's checkpointed migration chain offline.
// The manifest is directory-wide, so --tenant cannot select only part of an
// upgrade. Once current, an ordinary rebuild may still be scoped to one tenant.
func upgradePassageFormat(ctx context.Context, kv storage.KV, tenants tenant.Directory, dataDir, only string, e embedding.Embedder) error {
	want := version.Current()
	format, err := schema.Open(ctx, kv, want)
	if err != nil {
		return err
	}
	if !format.NeedsMigration(want) {
		return nil
	}
	if only != "" {
		return fmt.Errorf("the passage_index upgrade covers the whole directory; omit --tenant, then rebuild one tenant after the migration completes")
	}
	clk := clock.System()
	reindexer := offlineReindexer{queue: jobs.NewQueue(kv, clk)}
	registry, err := schema.Builtin(want, attr.NewIndexer(attr.MustTable()), reindexer, schema.WithPassages(e, reindexer))
	if err != nil {
		return err
	}
	plan, err := registry.Plan(format, want)
	if err != nil {
		return err
	}
	runner, err := schema.NewRunner(schema.RunnerConfig{KV: kv, Tenants: tenants, Clock: clk, DataDir: dataDir})
	if err != nil {
		return err
	}
	return runner.Run(ctx, plan)
}

// Rebuild jobs commit alongside the migration's cursor, just as on startup.
// The server drains them when it next starts; --index all also rebuilds inline.
type offlineReindexer struct{ queue *jobs.Queue }

func (r offlineReindexer) stage(ctx context.Context, tx txn.Tx, t tenant.ID, kind jobs.Type) error {
	outstanding, err := r.queue.HasOutstanding(ctx, t, kind)
	if err != nil || outstanding {
		return err
	}
	return r.queue.Enqueue(ctx, tx, &jobs.Job{Tenant: t, Namespace: tenant.DefaultNamespace, Type: kind, MaxAttempts: 2})
}
func (r offlineReindexer) StageVectorRebuild(ctx context.Context, tx txn.Tx, t tenant.ID) error {
	return r.stage(ctx, tx, t, "vector.rebuild")
}
func (r offlineReindexer) StageTextRebuild(ctx context.Context, tx txn.Tx, t tenant.ID) error {
	return r.stage(ctx, tx, t, "text.rebuild")
}
