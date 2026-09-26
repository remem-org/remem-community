package server

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// fixtures says what a job type needs in the store before it is meaningful to
// run, and there must be one for every registered type.
//
// The map is the guard. Adding a job type without adding a fixture fails
// TestEveryRegisteredHandlerIsIdempotent by name, which is the only way an
// at-least-once framework can hold its own contract — a handler nobody checked
// is a handler whose second run is a guess.
//
// A fixture returns the payload the job carries, or nil for a type whose work
// is "this whole tenant". Discovery is the first type that names its subjects,
// and a payload it cannot decode is a job it refuses rather than a job that
// silently does nothing.
var fixtures = map[jobs.Type]func(*testing.T, *deps, tenant.ID) []byte{
	TypeVectorRebuild:     seedMemories,
	TypeTextRebuild:       seedMemories,
	TypeGraphRebuild:      seedConnectedMemories,
	TypeAttrRebuild:       seedMemories,
	TypeJobsReap:          seedFinishedJobs,
	TypeLifecycleMaintain: seedDueMemories,
	TypeDiscoverySimilar:  seedNearDuplicates,
}

func TestEveryRegisteredHandlerIsIdempotent(t *testing.T) {
	// Delivery is at-least-once, so every handler runs twice in ordinary
	// operation: a lease lapses under a slow one, a process dies between the
	// handler returning and the completion committing, a retry re-runs one that
	// failed halfway. The contract is asserted rather than asked for — the
	// whole keyspace after two runs must equal the keyspace after one.
	for _, typ := range mustDeps(t).registry.Types() {
		t.Run(typ.String(), func(t *testing.T) {
			seed, ok := fixtures[typ]
			if !ok {
				t.Fatalf("job type %s has no idempotence fixture.\n"+
					"Every registered type needs one: at-least-once delivery means this handler "+
					"will run twice on one payload in ordinary operation, and nothing else checks "+
					"that it may.", typ)
			}

			d := mustDeps(t)
			const tid = tenant.ID("acme")
			payload := seed(t, d, tid)

			ctx := context.Background()
			run := func() {
				j := &jobs.Job{Tenant: tid, Namespace: tenant.DefaultNamespace,
					Type: typ, Payload: payload}
				if err := d.queue.Submit(ctx, j); err != nil {
					t.Fatalf("Submit: %v", err)
				}
				handler, err := d.registry.Handler(typ)
				if err != nil {
					t.Fatal(err)
				}
				if err := handler.Handle(ctx, j, noCheckpoint{}); err != nil {
					t.Fatalf("the first run of %s failed: %v", typ, err)
				}
			}

			run()
			once := dumpExcludingJobs(t, d.kv)
			run()
			twice := dumpExcludingJobs(t, d.kv)

			if !bytes.Equal(once, twice) {
				t.Fatalf("running %s twice left a different keyspace than running it once; "+
					"at-least-once delivery makes that a defect, not a nuance", typ)
			}
		})
	}
}

// dumpExcludingJobs is every row except the job space's own.
//
// The job rows differ between the two runs by construction — each run submits
// its own job and leaves its own row — and comparing them would make every
// handler fail for a reason that has nothing to do with the handler. What is
// being asserted is that the *work* is idempotent.
func dumpExcludingJobs(t *testing.T, kv storage.KV) []byte {
	t.Helper()
	var buf bytes.Buffer
	it := kv.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		if isJobRow(t, it.Key()) {
			continue
		}
		buf.Write(it.Key())
		buf.WriteByte(0)
		buf.Write(it.Value())
		buf.WriteByte(0)
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func isJobRow(t *testing.T, key []byte) bool {
	t.Helper()
	_, _, space, err := keys.ParseSpace(key)
	if err != nil {
		return false // a row this test cannot classify is compared, not skipped
	}
	return space == keys.SpaceJob
}

func seedMemories(t *testing.T, d *deps, tid tenant.ID) []byte {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), tid)
	if _, err := tenant.Ensure(ctx, d.tenants, tid); err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{
		"the deployment pipeline runs on a self-hosted runner",
		"invoice INV-2024-8871 was paid on the third of March",
		"marzipan recipes from a Bavarian bakery",
	} {
		if _, err := d.memories.Create(ctx, memory.CreateReq{Content: content}); err != nil {
			t.Fatalf("seeding a memory: %v", err)
		}
	}
	return nil
}

func seedConnectedMemories(t *testing.T, d *deps, tid tenant.ID) []byte {
	t.Helper()
	seedMemories(t, d, tid)
	ctx := tenant.NewContext(context.Background(), tid)

	page, err := d.memories.List(ctx, memory.ListReq{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Memories) < 2 {
		t.Fatalf("the fixture produced %d memories", len(page.Memories))
	}
	tx := txn.New(d.kv)
	defer tx.Close()
	if err := d.edges.Add(ctx, tx, graph.Edge{
		From: page.Memories[0].ID, To: page.Memories[1].ID,
		Type: graph.RelatedTo, Strength: 0.8,
	}); err != nil {
		t.Fatalf("connecting the fixture: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return nil
}

// seedNearDuplicates stores memories close enough to link and returns the one
// discovery is asked about.
//
// It checks that premise rather than assuming it. The comparison this fixture
// feeds is "the keyspace after two runs equals the keyspace after one", which a
// handler that wrote nothing at all satisfies perfectly — so a fixture whose
// memories fall below the threshold would turn the assertion vacuous without
// saying so. A test that cannot fail is worse than no test.
//
// The check is on the embeddings rather than on a probe run, because a probe
// would write the very edges the two runs are meant to produce and leave the
// comparison just as vacuous by a different route.
func seedNearDuplicates(t *testing.T, d *deps, tid tenant.ID) []byte {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), tid)
	if _, err := tenant.Ensure(ctx, d.tenants, tid); err != nil {
		t.Fatal(err)
	}
	contents := []string{
		"the deployment pipeline runs on a self-hosted runner in eu-west",
		"the deployment pipeline runs on a self-hosted runner in us-east",
		"the deployment pipeline runs on a self-hosted runner nightly",
	}

	vectors, err := d.embedding.Embed(ctx, contents)
	if err != nil {
		t.Fatal(err)
	}
	cos := 1 - distance.CosineDistance(vectors[0], vectors[1])
	if cos < d.cfg.Discovery.Threshold {
		t.Fatalf("the fixture's two closest memories sit at cosine %v, below the %v threshold; "+
			"discovery would write nothing and comparing two runs of it would prove nothing",
			cos, d.cfg.Discovery.Threshold)
	}

	var subject id.ID
	for i, content := range contents {
		m, err := d.memories.Create(ctx, memory.CreateReq{Content: content})
		if err != nil {
			t.Fatalf("seeding a memory: %v", err)
		}
		if i == 0 {
			subject = m.ID
		}
	}
	payload, err := discovery.EncodePayload([]id.ID{subject})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func seedFinishedJobs(t *testing.T, d *deps, tid tenant.ID) []byte {
	t.Helper()
	ctx := context.Background()
	if _, err := tenant.Ensure(tenant.NewContext(ctx, tid), d.tenants, tid); err != nil {
		t.Fatal(err)
	}
	fake, ok := d.clk.(*clock.Fake)
	if !ok {
		t.Fatal("this fixture needs the fake clock, so that a finished job can be older than the retention")
	}
	for range 3 {
		j := &jobs.Job{Tenant: tid, Type: TypeGraphRebuild}
		if err := d.queue.Submit(ctx, j); err != nil {
			t.Fatal(err)
		}
		claimed, err := d.queue.Claim(ctx, tid, "seed", []jobs.Type{TypeGraphRebuild}, 1)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("Claim: %d, %v", len(claimed), err)
		}
		if err := d.queue.Complete(ctx, claimed[0]); err != nil {
			t.Fatal(err)
		}
	}
	fake.Advance(2 * d.cfg.Jobs.Retention)
	return nil
}

// seedDueMemories stores memories and then moves the clock far enough forward
// that every one of them is owed a decay pass.
//
// The idempotence question here is a real one rather than a formality. The pass
// mutates the records it visits — importance down, health down, the two decay
// clocks forward — so "running it twice leaves what running it once leaves"
// holds only because each decay advances its own clock to the instant of the
// pass, and the next pass measures whole elapsed days from there. Removing
// either `LastDecayAt = now` or `LastHealthCheckAt = now` makes the second run
// decay a second time and this fixture reports it.
func seedDueMemories(t *testing.T, d *deps, tid tenant.ID) []byte {
	t.Helper()
	seedMemories(t, d, tid)

	ctx := tenant.NewContext(context.Background(), tid)
	// One of each policy, because they exercise different halves of the pass:
	// only long_term decays importance, only short_term expires, and pinned
	// must come through untouched. A fixture of default memories alone would
	// leave importance decay unrun and the assertion would hold vacuously.
	for _, seed := range []memory.CreateReq{
		{Content: "a long-lived note about the release process", Policy: "long_term"},
		{Content: "a note that should outlive everything", Policy: "pinned"},
		{Content: "a note with an hour to live", Policy: "short_term", TTL: time.Hour},
	} {
		if _, err := d.memories.Create(ctx, seed); err != nil {
			t.Fatalf("seeding a %s memory: %v", seed.Policy, err)
		}
	}

	fake, ok := d.clk.(*clock.Fake)
	if !ok {
		t.Fatal("this fixture needs the fake clock, so that a memory can become due")
	}
	fake.Advance(3 * 24 * time.Hour)
	return nil
}

type noCheckpoint struct{}

func (noCheckpoint) Save(context.Context, []byte) error { return nil }
func (noCheckpoint) Processed(uint64)                   {}
func (noCheckpoint) Changed(uint64)                     {}

func mustDeps(t *testing.T) *deps {
	t.Helper()
	return mustDepsWith(t, func(*config.Config) {})
}

// mustDepsWith is mustDeps with the configuration adjusted first — for a test
// that needs the approximate vector index, whose node records the default flat
// index never writes.
func mustDepsWith(t *testing.T, mutate func(*config.Config)) *deps {
	t.Helper()
	cfg := config.Default()
	cfg.Storage.Engine = "memory"
	cfg.Storage.SyncWrites = false
	cfg.Server.Env = "development"
	cfg.Jobs.Retention = time.Hour
	mutate(&cfg)

	d, err := build(cfg, options{
		embedder: embeddingtest.New(),
		clk:      clock.NewFake(clock.FakeStart),
		// These tests drive the dependencies directly and name tenants other
		// than the implicit one, which is the identity-scoped contract. A
		// single-tenant build's dependencies are exercised in
		// single_tenant_test.go.
		capability: tenant.IdentityScoped,
	})
	if err != nil {
		t.Fatalf("building the server's dependencies: %v", err)
	}
	t.Cleanup(func() { _ = d.close() })
	return d
}

func TestADamagedIndexSchedulesItsOwnRebuild(t *testing.T) {
	// The payoff of the phase, and the reason the two standing warnings existed
	// at all: a tenant whose index cannot answer completely now says so *and*
	// gets a durable repair queued, instead of leaving a log line for somebody
	// to notice.
	//
	// Neither trigger fires unless the tenant is already degraded. This one
	// makes it so honestly — by interrupting a rebuild, which is exactly what
	// leaves the marker set in production.
	d := mustDeps(t)
	const tid = tenant.ID("acme")
	seedMemories(t, d, tid)

	if err := d.texts.Rebuild(context.Background(), d.kv, tid, tenant.DefaultNamespace,
		failingSource{}); err == nil {
		t.Fatal("the interrupted rebuild reported success")
	}

	ctx := tenant.NewContext(context.Background(), tid)
	if _, err := d.memories.Search(ctx, memory.SearchReq{
		Query: "pipeline", Type: memory.SearchKeyword, Limit: 5,
	}); err != nil {
		t.Fatalf("searching a degraded index must still answer: %v", err)
	}
	// A second search must not queue a second rebuild of the same index.
	if _, err := d.memories.Search(ctx, memory.SearchReq{
		Query: "invoice", Type: memory.SearchKeyword, Limit: 5,
	}); err != nil {
		t.Fatal(err)
	}
	d.repairs.close()

	queued, err := d.queue.List(context.Background(), tid,
		jobs.Filter{Types: []jobs.Type{TypeTextRebuild}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 {
		t.Fatalf("a degraded keyword index produced %d rebuild jobs, want exactly 1", len(queued))
	}
	if queued[0].Tenant != tid || queued[0].State != jobs.Pending {
		t.Fatalf("the scheduled repair is %+v", queued[0])
	}
}

func TestTheVectorRebuildHookQueuesTheJobItNames(t *testing.T) {
	// This is the object handed to indexes.Open, so what it does here is what
	// the approximate index's damage report does in production.
	d := mustDeps(t)
	const tid = tenant.ID("acme")
	seedMemories(t, d, tid)

	hook := d.repairs.vectorRebuilder()
	hook.ScheduleRebuild(tid, "half the node records did not decode")
	hook.ScheduleRebuild(tid, "half the node records did not decode")
	d.repairs.close()

	queued, err := d.queue.List(context.Background(), tid,
		jobs.Filter{Types: []jobs.Type{TypeVectorRebuild}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 {
		t.Fatalf("two damage reports produced %d rebuild jobs, want 1", len(queued))
	}
}

// failingSource interrupts a rebuild after it has cleared the index, which is
// what leaves the tenant degraded.
type failingSource struct{}

func (failingSource) Scan(context.Context, tenant.ID, tenant.Namespace,
	func(id.ID, string, []string) error,
) error {
	return errors.New("the walk was interrupted")
}

// The three discovery numbers live in two places — internal/config, so an
// operator can read and set them, and internal/discovery, so the package has a
// default without importing configuration into every build. This is what stops
// the two copies drifting.
//
// internal/config cannot simply import the constants: it is imported by
// everything, and internal/discovery pulls in the graph, the record repository
// and the job framework. The duplication is deliberate and this is its price.
func TestConfigDefaultsMatchTheDiscoveryPackage(t *testing.T) {
	if config.DefaultDiscoveryThreshold != discovery.DefaultThreshold {
		t.Errorf("discovery.threshold defaults to %v in configuration and %v in the package",
			config.DefaultDiscoveryThreshold, discovery.DefaultThreshold)
	}
	if config.DefaultDiscoveryTopK != discovery.DefaultTopK {
		t.Errorf("discovery.top_k defaults to %d in configuration and %d in the package",
			config.DefaultDiscoveryTopK, discovery.DefaultTopK)
	}
	if config.DefaultDiscoveryMaxCandidates != discovery.MaxCandidates {
		t.Errorf("discovery.max_candidates defaults to %d in configuration and %d in the package",
			config.DefaultDiscoveryMaxCandidates, discovery.MaxCandidates)
	}
}

// TestTheReaperReportsWhatItRemoved holds the reaper's half of the attempt
// history's counts. Everything this handler touches it deletes, so processed
// and changed are the same number — and both are reported rather than one being
// left for a reader to infer from the other.
func TestTheReaperReportsWhatItRemoved(t *testing.T) {
	d := mustDeps(t)
	ctx := context.Background()
	const tid = tenant.ID("acme")

	// Two finished jobs, aged past the retention.
	for range 2 {
		j := &jobs.Job{Tenant: tid, Namespace: tenant.DefaultNamespace, Type: TypeVectorRebuild}
		if err := d.queue.Submit(ctx, j); err != nil {
			t.Fatal(err)
		}
		claimed, err := d.queue.Claim(ctx, tid, "node-test", nil, 1)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("Claim: %d, %v", len(claimed), err)
		}
		if err := d.queue.Complete(ctx, claimed[0]); err != nil {
			t.Fatal(err)
		}
	}
	d.clk.(*clock.Fake).Advance(d.cfg.Jobs.Retention + time.Hour)

	cp := &countingCheckpointer{}
	if err := d.reapJobs(ctx, &jobs.Job{Tenant: tid, Namespace: tenant.DefaultNamespace}, cp); err != nil {
		t.Fatalf("reapJobs: %v", err)
	}
	if cp.changed == nil || *cp.changed != 2 {
		t.Fatalf("the reaper removed two rows and reported %v changed", cp.changed)
	}
	if cp.processed == nil || *cp.processed != 2 {
		t.Fatalf("the reaper reported %v processed, want 2", cp.processed)
	}

	// A pass with nothing due reports zero rather than leaving it unavailable:
	// "the reaper ran and found nothing" is a real answer.
	empty := &countingCheckpointer{}
	if err := d.reapJobs(ctx, &jobs.Job{Tenant: tid, Namespace: tenant.DefaultNamespace}, empty); err != nil {
		t.Fatalf("reapJobs: %v", err)
	}
	if empty.changed == nil || *empty.changed != 0 {
		t.Fatalf("an empty pass reported %v changed, want 0", empty.changed)
	}
}

// countingCheckpointer records what a handler reported, as pointers so that
// "nothing was reported" stays distinguishable from "zero".
type countingCheckpointer struct {
	processed *uint64
	changed   *uint64
}

func (*countingCheckpointer) Save(context.Context, []byte) error { return nil }
func (c *countingCheckpointer) Processed(n uint64)               { c.processed = &n }
func (c *countingCheckpointer) Changed(n uint64)                 { c.changed = &n }
