package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/onnx"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/lifecycle/policykv"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/session"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/metered"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/indexes"
	"github.com/remem-org/remem-go/internal/version"
)

// deps is everything one server owns, in the order it must be torn down: last
// built, first closed.
type deps struct {
	// capability is the tenant policy this server was composed with. It is
	// settled in New, before anything is built.
	capability tenant.Capability
	cfg        config.Config
	clk        clock.Clock
	metrics    *obs.Metrics

	// background is the tail of the migration plan: the steps that run
	// alongside serving rather than before it. Server.Run owns the goroutine.
	background []schema.Migration
	migrations *schema.Runner

	kv      storage.KV
	slots   *schema.Slots
	indexer *attr.Indexer
	texts   *text.Index
	vectors vector.Index
	edges   graph.Service
	repo    record.Repo

	// The background job framework. The queue is durable, the registry maps a
	// type to its handler, the pool runs them and the scheduler enqueues the
	// recurring ones. Server.Run owns the two goroutines.
	registry  *jobs.Registry
	queue     *jobs.Queue
	pool      *jobs.Pool
	scheduler *jobs.Scheduler
	repairs   *repairs

	// The lifecycle. `policies` is the durable override store, `policyCache`
	// the scheduler's cached view of it, and `events` the audit stream every
	// recall and transition is written to.
	policies    *policykv.Store
	policyCache *lifecycle.Registry
	events      *events.Store
	maintainer  *lifecycle.Maintainer
	discoverer  *discovery.Discoverer
	embedder    embedding.Embedder
	embedding   *embedding.Service
	memories    *memory.Service
	// tenants is the directory everything in the process uses. Under
	// tenant.SingleTenant it is allTenants confined to the implicit tenant,
	// which is what makes the job scheduler's fan-out, the metrics labels, the
	// session sweep and the policy routes single-tenant without an edition
	// check at each of them.
	tenants tenant.Directory
	// allTenants is the unconfined directory. Exactly two things use it: the
	// start-up inventory, which has to see what it is about to refuse, and the
	// migration runner, which must not skip a tenant's rows. Both are audited
	// cross-tenant work (Invariant 1).
	allTenants *tenantkv.Directory
	sessions   *session.Registry
	keyring    *auth.Keyring
}

// maintainerOf asks an index whether it keeps a structure of its own beside the
// canonical vectors, the way memory.Service does. An exact index does not: it
// *is* a scan of those vectors, so there is nothing for a deletion to tidy.
func maintainerOf(index vector.Index) vector.Maintainer {
	if m, ok := index.(vector.Maintainer); ok {
		return m
	}
	return nil
}

// build assembles the server's dependencies.
//
// The order is the dependency order and the failure order both: everything that
// can refuse — an unreadable config, a directory written by a newer Remem, a
// missing model — refuses here, before anything listens. Spec §51 asks for a
// fast failure, and the property that makes it useful is that a server which
// has bound a port is a server an orchestrator believes is working.
func build(cfg config.Config, opts options) (*deps, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	d := &deps{cfg: cfg, clk: opts.clk, metrics: obs.NewMetrics()}
	if d.clk == nil {
		d.clk = clock.System()
	}

	kv, err := openStore(cfg)
	if err != nil {
		return nil, err
	}
	// Engine facts are sampled from the store as opened, before it is wrapped:
	// they belong to the engine, and the in-memory store has none to report.
	if r, ok := kv.(storage.StatsReporter); ok {
		d.metrics.ObserveEngine(cfg.Storage.Engine, func() obs.EngineSample {
			s := r.EngineStats()
			return obs.EngineSample{
				DiskBytes: s.DiskBytes, Compactions: s.Compactions,
				CacheHits: s.CacheHits, CacheMisses: s.CacheMisses,
			}
		})
	}
	// Wrapped once, here, so that every read and write in the process is
	// counted — including those of code written after this line was.
	kv = metered.New(kv, &d.metrics.Storage)
	d.kv = kv

	// Before the migrations, because a step may need to walk every tenant, and
	// it needs only the store and a clock.
	d.allTenants = tenantkv.New(kv, d.clk)
	d.capability = opts.capability
	if d.tenants, err = scopeDirectory(cfg, opts.capability, d.allTenants); err != nil {
		_ = kv.Close()
		return nil, err
	}

	// The registered attribute slot table, built before the migrations because
	// a migration that backfills attribute rows needs it to write them with.
	slots, err := attr.Table()
	if err != nil {
		_ = kv.Close()
		return nil, err
	}
	d.slots = slots
	d.indexer = attr.NewIndexer(slots)

	// The job queue, built before anything that reports damage to it. Nothing
	// runs yet: Server.Run starts the pool, so a `remem-admin` command or a
	// failed start-up never leaves work half-claimed.
	d.queue = jobs.NewQueue(kv, d.clk,
		jobs.WithSyncWrites(cfg.Storage.SyncWrites),
		jobs.WithLease(cfg.Jobs.LeaseDuration))
	d.registry = jobs.NewRegistry()
	d.repairs = newRepairs(d.queue, d.clk)

	// The inverted index, built before the migrations for the same reason the
	// slot table is: a migration that backfills postings would need it to write
	// them with. It holds no state and nothing to close.
	if cfg.Text.Enabled {
		d.texts = text.New()
	}

	// The format gate and the migrations, before a byte of user data is read or
	// written. A directory this binary cannot interpret is refused here rather
	// than discovered by a read that returns something plausible.
	if err := d.migrate(context.Background()); err != nil {
		_ = kv.Close()
		return nil, err
	}

	// The slot table is validated against the one on disk and persisted, after
	// the migrations rather than before: a migration may be what makes the
	// registered table legal to install, and stamping it first would record a
	// table the data has not been brought up to.
	if err := schema.OpenSlots(context.Background(), kv, slots); err != nil {
		_ = kv.Close()
		return nil, err
	}

	d.embedder = opts.embedder
	if d.embedder == nil {
		e, err := openEmbedder(cfg)
		if err != nil {
			_ = kv.Close()
			return nil, err
		}
		d.embedder = e
	}
	d.embedding = embedding.NewService(d.embedder, embedding.ServiceConfig{
		BatchSize:  cfg.Embedding.BatchSize,
		FillWindow: cfg.Embedding.FillWindow,
		Workers:    cfg.Embedding.Workers,
		CacheSize:  cfg.Embedding.CacheSize,
	})

	metric, err := distance.Parse(cfg.Vector.Metric)
	if err != nil {
		_ = d.embedding.Close()
		_ = kv.Close()
		return nil, err
	}

	// The index is chosen by name and built behind vector.Index. Nothing here
	// names a layer, a neighbour bound or an ef, which is spec §40.3 and the
	// rule internal/arch enforces: this package may not import the approximate
	// index, and it does not need to.
	index, err := indexes.Open(kv, indexes.Config{
		Kind:                cfg.Vector.Index,
		Metric:              cfg.Vector.Metric,
		Neighbours:          cfg.Vector.HNSWM,
		BuildEffort:         cfg.Vector.HNSWEfConstruction,
		SearchEffort:        cfg.Vector.HNSWEfSearch,
		ResidentBudgetBytes: int64(cfg.Vector.ResidentBudgetMB) << 20,
		ModelID:             d.embedder.ModelID(),
		Rebuilds:            d.repairs.vectorRebuilder(),
	})
	if err != nil {
		_ = d.embedding.Close()
		_ = kv.Close()
		return nil, err
	}
	d.vectors = index

	d.sessions = session.New(kv, d.clk, 0)

	// The lazy migration hook, and only when there is something to upgrade: a
	// hook that decides to do nothing on every read is a cost every deployment
	// pays for a migration none of them are running.
	upgrader, err := schema.NewRecordUpgrader()
	if err != nil {
		_ = d.embedding.Close()
		_ = kv.Close()
		return nil, err
	}
	var repoOpts []record.RepoOption
	if upgrader != nil {
		repoOpts = append(repoOpts, record.WithUpgrader(upgrader))
	}
	// The attribute indexer is a repository option rather than a call the write
	// path makes, so that adding a write path cannot add a way to forget it: a
	// record whose row was never written exists and cannot be listed.
	repoOpts = append(repoOpts, record.WithIndexer(d.indexer))
	// The text index is a second indexer on the same hook, not a call the write
	// path makes. Both stage into the record's own transaction, so a memory and
	// everything that makes it findable land together or not at all (spec §12).
	if d.texts != nil {
		repoOpts = append(repoOpts, record.WithIndexer(d.texts))
	}

	d.edges = graph.NewService(kv, d.clk)

	// The lifecycle, before the repository, because the repository computes a
	// record's next-attention time through it on every write. Wiring it after
	// would leave a window in which a record could be stored with no schedule —
	// which is safe (zero reads as "due now") and is still a window there is no
	// reason to have.
	d.events = events.NewStore(kv)
	d.policies = policykv.New(kv, d.clk)
	d.policyCache = lifecycle.NewRegistry(d.tenants, d.policies.Loader())
	repoOpts = append(repoOpts, record.WithScheduler(lifecycle.NewScheduler(d.policyCache)))

	// Discovery, before the memory service, because the write path is given the
	// enqueuer. The handler itself is built after the repository below, since
	// it reads records through the same one the write path writes to.
	memoryOpts := []memory.Option{memory.WithSearchMetrics(&d.metrics.Search)}
	if cfg.Discovery.Enabled {
		memoryOpts = append(memoryOpts,
			memory.WithDiscovery(discovery.NewEnqueuer(d.queue, TypeDiscoverySimilar)))
	}
	if cfg.Lifecycle.Enabled {
		memoryOpts = append(memoryOpts,
			memory.WithRecall(d.events, cfg.Lifecycle.RecallWindow))
	}
	if d.texts != nil {
		memoryOpts = append(memoryOpts, memory.WithTextIndex(d.texts),
			memory.WithTextRebuilder(d.repairs.textRebuilder()))
	}
	// One repository, shared by the write path and the lifecycle sweep. Two
	// would be two sets of derived indexes over the same records, and the sweep
	// would maintain a different world than the one a search reads.
	d.repo = record.NewRepo(kv, repoOpts...)
	d.memories = memory.New(kv, d.repo,
		index, d.embedding, d.clk, d.slots, d.edges, memory.Config{
			DefaultLimit:   cfg.Search.DefaultLimit,
			MaxLimit:       cfg.Search.MaxLimit,
			MaxPageDepth:   cfg.Search.MaxPageDepth,
			RankedTTL:      cfg.Paging.RankedTTL,
			WidenMaxFactor: cfg.Search.WidenMaxFactor,
			ListMaxFactor:  cfg.Search.ListMaxFactor,
			Metric:         metric,
			BatchSize:      cfg.Embedding.BatchSize,
			SyncWrites:     cfg.Storage.SyncWrites,
		}, memoryOpts...)

	creds := cfg.Credentials()
	converted := make([]auth.Credential, len(creds))
	for i, c := range creds {
		authorized := make([]tenant.ID, len(c.Authorized))
		for j, a := range c.Authorized {
			authorized[j] = tenant.ID(a)
		}
		converted[i] = auth.Credential{
			ID: c.ID, Secret: c.Secret,
			Tenant: tenant.ID(c.Tenant), CrossTenant: c.CrossTenant,
			Authorized: authorized,
		}
	}
	keyring, err := auth.NewKeyring(converted)
	if err != nil {
		_ = d.embedding.Close()
		_ = kv.Close()
		return nil, err
	}
	d.keyring = keyring

	// The lifecycle handler, built here rather than in registerBuiltinJobs
	// because it closes over the repository the memory service was given — and
	// building a second repository for it would give the sweep a different set
	// of derived indexes than the write path has.
	maintainer, err := lifecycle.NewMaintainer(lifecycle.Deps{
		KV:       kv,
		Repo:     d.repo,
		Slots:    d.slots,
		Events:   d.events,
		Policies: d.policyCache,
		Clock:    d.clk,
		Edges:    d.edges,
		Vectors:  maintainerOf(index),
		Submit:   d.queue.Submit,
		Type:     TypeLifecycleMaintain,
		Metrics:  &d.metrics.Lifecycle,

		RunBudget:      cfg.Lifecycle.RunBudget,
		EventRetention: cfg.Lifecycle.EventRetention,
		RecallWindow:   cfg.Lifecycle.RecallWindow,
	})
	if err != nil {
		_ = d.embedding.Close()
		_ = kv.Close()
		return nil, err
	}
	d.maintainer = maintainer

	// The discovery handler, built here for the same reason: it reads records
	// through the repository the write path writes to, and a second one would
	// give it a different set of derived indexes than a search reads.
	//
	// It is built whether or not discovery.enabled is set, so that jobs already
	// on disk when an operator turns discovery off still have a handler to run
	// them. What the setting controls is whether new work is enqueued.
	discoverer, err := discovery.NewDiscoverer(discovery.Deps{
		KV:      kv,
		Repo:    d.repo,
		Index:   index,
		Edges:   d.edges,
		Clock:   d.clk,
		Metric:  metric,
		Metrics: &d.metrics.Discovery,

		Threshold:     cfg.Discovery.Threshold,
		TopK:          cfg.Discovery.TopK,
		MaxCandidates: cfg.Discovery.MaxCandidates,
	})
	if err != nil {
		_ = d.embedding.Close()
		_ = kv.Close()
		return nil, err
	}
	d.discoverer = discoverer

	// The handlers, and the pool and scheduler that will run them. Registering
	// after everything they close over exists is what lets each handler be a
	// closure rather than a struct with its own copy of the dependency graph.
	if err := d.registerBuiltinJobs(); err != nil {
		_ = d.embedding.Close()
		_ = kv.Close()
		return nil, err
	}
	if err := d.buildJobRunners(); err != nil {
		_ = d.embedding.Close()
		_ = kv.Close()
		return nil, err
	}

	return d, nil
}

// buildJobRunners constructs the pool and the scheduler.
func (d *deps) buildJobRunners() error {
	pool, err := jobs.NewPool(jobs.PoolConfig{
		Queue: d.queue, Registry: d.registry, Tenants: d.tenants, Clock: d.clk,
		Owner:        d.nodeID(),
		Workers:      d.cfg.Jobs.Workers,
		PollInterval: d.cfg.Jobs.PollInterval,
		DrainTimeout: d.cfg.Jobs.DrainTimeout,
		Metrics:      d.metrics,
	})
	if err != nil {
		return err
	}
	d.pool = pool

	interval := schedulerTick(d.registry.Recurring(), d.cfg.Jobs.PollInterval)
	sched, err := jobs.NewScheduler(jobs.SchedulerConfig{
		Queue: d.queue, Registry: d.registry, Tenants: d.tenants, Clock: d.clk,
		Interval: interval,
	})
	if err != nil {
		return err
	}
	d.scheduler = sched
	return nil
}

// schedulerTick is how often the scheduler looks at its registrations.
//
// # It comes from the registrations, not from a setting
//
// It used to be derived from jobs.retention — a Phase 9 shortcut that held only
// because the reaper was the one recurring type and its interval came from the
// same setting. Phase 10 registers lifecycle.maintain at lifecycle.sweep_interval,
// and the end-to-end run found what that produced: a tick of three hours over a
// sweep asked to run hourly. An operator setting a dial and getting something
// else is the class of defect Phase 9's configuration work exists to prevent, so
// the tick is derived from the shortest interval anything actually registered.
//
// # And it is bounded on both sides
//
// A quarter of the shortest interval, so a run is at most 25% of a period late —
// and with the drift fix in jobs.Scheduler.advance, late once rather than later
// every time. Never finer than the pool polls, because a tick that enqueues work
// nothing will look at for another second is a directory walk for nothing. Never
// coarser than a minute, so a long interval does not make start-up recovery slow.
func schedulerTick(recurring []jobs.Entry, poll time.Duration) time.Duration {
	shortest := time.Duration(0)
	for _, e := range recurring {
		if shortest == 0 || e.Every < shortest {
			shortest = e.Every
		}
	}
	if shortest == 0 {
		// Nothing recurring is registered. The scheduler still runs, because a
		// type may be registered by a later phase, and a minute is cheap.
		return time.Minute
	}

	tick := shortest / 4
	if floor := poll; tick < floor {
		tick = floor
	}
	if tick < time.Second {
		tick = time.Second
	}
	if tick > time.Minute {
		tick = time.Minute
	}
	return tick
}

// nodeID is what a lease is granted to.
//
// It is the configured node id, falling back to the hostname, falling back to a
// fixed name. A lease has to name somebody — that is what makes it fenceable —
// and an empty owner would make every log line about a stuck job say nothing.
func (d *deps) nodeID() string {
	if id := d.cfg.Node.ID; id != "" {
		return id
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "remem"
}

// migrate opens the format gate and brings the directory forward.
//
// The plan is split by position rather than by strategy (see [schema.Split]):
// everything up to the first background step runs here, and the tail is handed
// to Server.Run to execute once the listener is up. Blocking start-up on a
// background step would make upgrade downtime proportional to corpus size,
// which is the thing that strategy exists to avoid.
//
// Both refusals live here, before anything listens: a directory written by a
// newer Remem, and a directory older than this binary with no path forward.
func (d *deps) migrate(ctx context.Context) error {
	want := version.Current()

	format, err := schema.Open(ctx, d.kv, want)
	if err != nil {
		return err
	}

	registry, err := schema.Builtin(want, d.indexer, textReindexer{queue: d.queue})
	if err != nil {
		return err
	}
	plan, err := registry.Plan(format, want)
	if err != nil {
		return err
	}

	runner, err := schema.NewRunner(schema.RunnerConfig{
		KV:      d.kv,
		Tenants: d.allTenants,
		Clock:   d.clk,
		DataDir: d.cfg.Storage.Path,
		Metrics: d.metrics,
	})
	if err != nil {
		return err
	}
	d.migrations = runner

	startup, background := schema.Split(plan)
	d.background = background
	return runner.Run(ctx, startup)
}

func openStore(cfg config.Config) (storage.KV, error) {
	switch cfg.Storage.Engine {
	case "memory":
		return memkv.New(), nil
	case "pebble":
		return pebble.Open(cfg.Storage.Path, pebble.Options{
			CacheSizeBytes: cfg.Storage.CacheSizeBytes,
		})
	default:
		return nil, errs.E(errs.Invalid, "server.openStore",
			fmt.Errorf("storage.engine %q is not one this binary knows", cfg.Storage.Engine))
	}
}

// openEmbedder loads the model.
//
// A build without the onnx tag fails here, and that is on purpose: the
// alternative is a server that starts, produces meaningless vectors, and stamps
// the real model's name on every one of them. A durable corpus filled that way
// cannot be told from a good one afterwards.
func openEmbedder(cfg config.Config) (embedding.Embedder, error) {
	if cfg.Embedding.Model != embedding.Model {
		return nil, errs.E(errs.Invalid, "server.openEmbedder", fmt.Errorf(
			"embedding.model is %q; this binary produces vectors comparable only with %q",
			cfg.Embedding.Model, embedding.Model))
	}
	return onnx.Open(onnx.Config{
		ModelPath:         cfg.Embedding.ModelPath,
		SharedLibraryPath: cfg.Embedding.ONNXLibraryPath,
		MaxSequenceTokens: cfg.Embedding.MaxSequenceTokens,
		IntraOpThreads:    cfg.Embedding.Workers,
	})
}

// close releases everything, in reverse order of construction, and reports the
// first failure without skipping the rest. A shutdown that stopped at the first
// error would leave a data directory locked.
func (d *deps) close() error {
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	// The repair scheduler first: it holds goroutines that write through the
	// queue, and closing the store under them would turn a shutdown into a
	// handful of errors nobody asked for.
	d.repairs.close()
	if d.memories != nil {
		keep(d.memories.Close())
	}
	if d.embedding != nil {
		keep(d.embedding.Close())
	}
	if d.kv != nil {
		keep(d.kv.Close())
	}
	return first
}

var errNotReady = errors.New("the server is still starting")
