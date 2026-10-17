package schema_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/version"
)

// A directory written before users existed has no user_format, and opening it
// must not be refused for want of a migration path. It is moved forward by one
// instant step that rewrites nothing, because such a directory holds no user
// rows to convert: absent and "version 1 with no users" describe the same bytes.
func TestADirectoryFromBeforeUsersIsStampedWithoutRewriting(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)
	dir := tenantkv.New(kv, clk)
	if _, err := tenant.Ensure(tenant.NewContext(ctx, "acme"), dir, "acme"); err != nil {
		t.Fatal(err)
	}

	f := schema.CurrentFormat(version.Current())
	delete(f.Subsystems, "user_format")
	if err := schema.WriteFormat(ctx, kv, f); err != nil {
		t.Fatal(err)
	}

	reg, err := schema.Builtin(version.Current(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reg.Plan(f, version.Current())
	if err != nil {
		t.Fatalf("a directory from before users has no migration path: %v", err)
	}
	if len(plan) != 1 || plan[0].ID != schema.UserFormatID {
		t.Fatalf("the plan is %v, want exactly %s", plan, schema.UserFormatID)
	}
	if plan[0].RewritesData || plan[0].Strategy != schema.StrategyInstant {
		t.Fatal("stamping user_format must be instant and rewrite nothing, so it takes no backup")
	}

	runner, err := schema.NewRunner(schema.RunnerConfig{KV: kv, Tenants: dir, Clock: clk, DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Run(ctx, plan); err != nil {
		t.Fatal(err)
	}

	format, err := schema.ReadFormat(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if got := format.Subsystems["user_format"].Current; got != version.UserFormat {
		t.Fatalf("user_format is %d after the step, want %d", got, version.UserFormat)
	}
	lo, hi := keys.SpaceRange("acme", tenant.DefaultNamespace, keys.SpaceUser)
	it := kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	if it.First() {
		t.Fatalf("stamping user_format wrote a user row: %x", it.Key())
	}
}
