package memory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// MaxContentBytes bounds one memory's content.
//
// The limit exists because content is embedded, indexed and replicated, and an
// unbounded one turns a single request into unbounded work in three places. It
// is generous: a megabyte is far more prose than a memory ever is.
const MaxContentBytes = 1 << 20

// MaxTags bounds one memory's tags, and a search's or a recall's tag filter.
//
// Rust's number (routes/memories.rs MAX_TAGS). A memory's tags are postings
// written in its own transaction, and a filter's are required terms each
// candidate is checked against, so an unbounded list is unbounded work in one
// request. Found in Phase 13's security review, where the only bound was the
// 8 MiB request body.
const MaxTags = 50

// MaxBatch bounds how many memories one batch store holds. Each is a model run
// inside one request and a write inside one transaction. Decided with the user
// in Phase 13's security review.
const MaxBatch = 1000

// MaxQueryBytes bounds a search's query and a recall's context. The embedder
// reads only the first 128 tokens of it, but a keyword search looks up every
// term, so the bound is on the text. It is generous for a recall context, which
// is an agent's working context. Decided with the user in Phase 13's security
// review.
const MaxQueryBytes = 64 << 10

// Config is what the service needs from configuration.
type Config struct {
	// DefaultLimit is the number of results a search returns when it asks for
	// none.
	DefaultLimit int
	// MaxLimit caps what a caller may ask for.
	MaxLimit int
	// MaxPageDepth bounds the ranking a paged search materialises. Reaching
	// it reports Truncated on every page of the session. A larger requested
	// page size takes precedence so the first page can still be filled.
	MaxPageDepth int
	// RankedTTL bounds idle time for search and traversal sessions.
	RankedTTL time.Duration
	// WidenMaxFactor bounds how far a search widens its candidate set to make
	// room for filtered-out results. Fixed at 32 (plan §Global Constraints).
	WidenMaxFactor int
	// ListMaxFactor is the same bound for a listing, larger on purpose: a
	// listing candidate is one attribute-row read (Rust's list_max_factor,
	// config.rs:290-300).
	ListMaxFactor int
	// Metric is how the index measures distance. It decides whether a score
	// can be a recovered cosine.
	Metric distance.Metric
	// BatchSize caps one embedding call.
	BatchSize int
	// SyncWrites makes each commit durable before it returns.
	SyncWrites bool
}

func (c Config) withDefaults() Config {
	if c.DefaultLimit <= 0 {
		c.DefaultLimit = 10
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = 200
	}
	if c.MaxPageDepth <= 0 {
		// The corpus/cost tradeoff is recorded in docs/architecture/query.md.
		c.MaxPageDepth = 2000
	}
	if c.RankedTTL <= 0 {
		// The idle-arrival experiment is recorded in docs/architecture/query.md.
		c.RankedTTL = time.Minute
	}
	if c.WidenMaxFactor < 1 {
		c.WidenMaxFactor = 32
	}
	if c.ListMaxFactor < 1 {
		c.ListMaxFactor = 128
	}
	if c.Metric == 0 {
		c.Metric = distance.L2
	}
	if c.BatchSize <= 0 {
		c.BatchSize = embedding.DefaultBatchSize
	}
	return c
}

// Service implements memory semantics over records, vectors and an index.
type Service struct {
	kv       storage.KV
	repo     record.Repo
	index    vector.Index
	embedder embedding.Embedder
	clk      clock.Clock
	cfg      Config

	// planner and executor are the query layer. They are nil in a build with
	// no attribute index, and List then refuses by name rather than returning
	// an empty page — an empty page from a corpus that is not empty is the
	// failure this whole phase exists to avoid.
	planner  *query.Planner
	executor *query.Executor
	paging   *pagingRegistry

	// texts is the inverted index. It is nil when text.enabled is off, and a
	// keyword or hybrid search is then refused by name rather than silently
	// answered as a semantic one — a caller who asked for an exact-term search
	// and got a meaning-based one has no way to tell.
	texts *text.Index

	// textRebuilds is handed to the executor, which is the only thing that
	// asks text.Health and therefore the only thing that learns an index is
	// incomplete.
	textRebuilds text.Rebuilder

	// searchMetrics is where a search reports what it cost. Nil records nothing.
	searchMetrics *obs.SearchMetrics

	// events is the audit stream. It is nil in a build with recall recording
	// off, and Get then records nothing.
	events       *events.Store
	recallWindow time.Duration

	// discovery stages the follow-up relationship work into a write's own
	// transaction. Nil in a build with discovery off, and a write then stores
	// the memory and queues nothing.
	discovery Enqueuer

	// edges is the graph. It is nil in a build with no graph, and every method
	// that needs one then refuses by name rather than answering emptily: an
	// empty neighbourhood over a connected corpus is the failure this codebase
	// keeps refusing to ship.
	edges graph.Service

	// writes serialises the transactions of one tenant.
	//
	// The text index keeps a per-tenant row that every write touches — the
	// corpus statistics BM25 scores against — and that row is conditionally
	// committed, because a lost increment is a ranking that is silently wrong
	// from then on. Without this, ordinary concurrent ingestion into one tenant
	// produces conflicts that have nothing to do with the caller: measured on
	// the in-memory store, thirty-two writers failed one write in five hundred
	// and sixty-four failed eight in a thousand. See txn.Gate.
	writes *txn.Gate

	// maintain is set when the configured index keeps a structure of its own
	// beside the canonical vectors and has to be told when they change. An
	// exact index does not: it *is* a scan of those vectors.
	//
	// It comes from asking the index whether it implements vector.Maintainer
	// rather than from configuration, so that "which index is running" is
	// never a branch in the write path — the interface answers, and an index
	// added later is wired by satisfying it.
	maintain vector.Maintainer
}

// Option supplies a dependency the service can run without.
type Option func(*Service)

// WithTextIndex turns on keyword and hybrid search. Without it those modes are
// refused by name rather than answered emptily.
//
// It is an option rather than another positional parameter because New already
// takes eight, and the ninth that is nil at half the call sites is the one that
// makes a constructor unreadable.
func WithTextIndex(ix *text.Index) Option {
	return func(s *Service) { s.texts = ix }
}

// WithSearchMetrics records each search's latency, the candidates it examined,
// and whether it reported itself truncated. Without it nothing is recorded,
// which is what a test that is not about metrics wants.
func WithSearchMetrics(m *obs.SearchMetrics) Option {
	return func(s *Service) { s.searchMetrics = m }
}

// WithTextRebuilder is where a degraded keyword index reports itself, so that
// the discovery becomes a durable repair job rather than a log line.
func WithTextRebuilder(r text.Rebuilder) Option {
	return func(s *Service) { s.textRebuilds = r }
}

// Enqueuer stages a memory write's follow-up discovery work into that write's
// own transaction.
//
// It is an interface here, with the implementation in internal/discovery,
// because the alternative is a cycle: internal/discovery reads records and
// walks the graph the way internal/lifecycle does, and internal/memory is a
// service over both. It is the shape lifecycle.Deps.Type established — the
// durable job type name belongs to the composition root, and a domain package
// naming its own would be the framework-imports-its-work cycle by another route.
//
// The subjects are the memories one transaction wrote, in write order. A batch
// is therefore one job carrying its subjects rather than one job per memory:
// the queue's cost is per row and the work is per subject either way.
type Enqueuer interface {
	EnqueueDiscovery(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace, subjects []id.ID) error
}

// WithDiscovery makes a memory write enqueue relationship discovery, in the
// same transaction.
//
// Without it nothing is enqueued and nothing fails: the graph does not fill
// itself, which is a build a composition root may legitimately want and is not
// a degradation a write should refuse over.
func WithDiscovery(e Enqueuer) Option {
	return func(s *Service) { s.discovery = e }
}

// WithRecall turns on durable recall recording: fetching a memory by id appends
// one event to its stream.
//
// Without it, Get records nothing and the lifecycle sees a memory nobody ever
// used — which is not a degradation to leave on by accident, so it is wired by
// the composition root rather than defaulted.
func WithRecall(store *events.Store, window time.Duration) Option {
	return func(s *Service) {
		s.events = store
		s.recallWindow = window
	}
}

// New builds the service. Every dependency is explicit: there is no package
// state, so two services in one process — or one test binary — do not share
// anything.
func New(kv storage.KV, repo record.Repo, index vector.Index, embedder embedding.Embedder,
	clk clock.Clock, slots *schema.Slots, edges graph.Service, cfg Config, opts ...Option) *Service {
	c := cfg.withDefaults()
	s := &Service{
		kv:       kv,
		repo:     repo,
		index:    index,
		embedder: embedder,
		clk:      clk,
		cfg:      c,
		edges:    edges,
		writes:   txn.NewGate(),
	}
	for _, o := range opts {
		o(s)
	}
	if slots != nil {
		s.paging = newPagingRegistry(kv, clk, c.RankedTTL)
		s.planner = query.NewPlanner(slots, c.WidenMaxFactor, c.ListMaxFactor)
		var executorOpts []query.ExecutorOption
		if s.texts != nil {
			executorOpts = append(executorOpts, query.WithTextIndex(s.texts))
		}
		if s.textRebuilds != nil {
			executorOpts = append(executorOpts, query.WithTextRebuilder(s.textRebuilds))
		}
		s.executor = query.NewExecutor(slots, index, c.Metric, executorOpts...)
	}
	if m, ok := index.(vector.Maintainer); ok {
		s.maintain = m
	}
	return s
}

// indexed tells a derived vector index that a canonical vector has committed.
//
// It runs after the commit and its error is deliberately not returned. Plan
// §II.4 makes the vector index the one asynchronously maintained index for
// exactly this reason: a memory the user asked to store is stored, and an index
// that failed to record it must not turn a successful write into a failed one.
// Nothing is lost by that — an unindexed canonical vector is inserted when the
// tenant is next materialised — and the index marks itself degraded, so a
// search that cannot see the memory says its answer may be incomplete rather
// than implying the memory is not there.
func (s *Service) indexed(ctx context.Context, t tenant.ID, rid id.ID, v []float32) {
	if s.maintain == nil {
		return
	}
	_ = s.maintain.Indexed(ctx, t, rid, v)
}

// removed tells a derived vector index that a canonical vector has been
// deleted. Its error is dropped for the same reason, and with a stronger
// guarantee behind it: the canonical vector is already gone, so the memory is
// unfindable whether or not this call succeeds.
func (s *Service) removed(ctx context.Context, t tenant.ID, rid id.ID) {
	if s.maintain == nil {
		return
	}
	_ = s.maintain.Removed(ctx, t, rid)
}

// Get returns one memory.
//
// An archived memory is reported as absent unless the caller asks for it. That
// is a deliberate choice of NotFound over a distinct "archived" error: the two
// would have to be distinguished by every caller, and the one caller that
// forgot would show retired memories in an ordinary listing.
func (s *Service) Get(ctx context.Context, rid id.ID, opts GetOpts) (*Memory, error) {
	const op = "memory.Get"

	rec, err := s.repo.Get(ctx, rid)
	if err != nil {
		return nil, err
	}
	if rec.Fields.Archived && !opts.IncludeArchived {
		return nil, errs.E(errs.NotFound, op, fmt.Errorf("memory %s is archived", rid))
	}
	s.recalled(ctx, rec)
	return fromRecord(s.peek(ctx, rec)), nil
}

// peek returns the record as it would be once its outstanding recalls are
// folded, without writing anything.
//
// # Why a read has to do this at all
//
// A recall appends an event and does not touch the record; the counters catch
// up when the lifecycle sweep next visits the memory. That visit is scheduled by
// what the memory is *owed* — for anything that decays, a day away — so the
// stored `access_count` lags a recall by up to a day, not by a sweep interval.
// A caller who fetched a memory ten times and saw a count of zero would be right
// to call that broken.
//
// Rust has the same problem and the same answer (services/search_engine.rs:165):
// "a read must never consume a recall no write has applied", so it reports the
// pending delta without folding it. Its pending set is a map in memory; ours is
// the durable stream, and the read is a seek into a range that is empty for
// every memory nobody has recalled since the last sweep — which is nearly all
// of them.
//
// # What it deliberately does not fix
//
// Listing by `last_recalled_at` walks the attribute index, and the index holds
// the stored value. Correcting the returned records would produce a page ordered
// by one number and displaying another, which is worse than a page that is
// consistently one visit behind. That bound is real and is written down in
// docs/architecture/lifecycle.md rather than papered over here.
func (s *Service) peek(ctx context.Context, rec *record.Record) *record.Record {
	if s.events == nil {
		return rec
	}
	// On a copy: this is a read, and a caller holding the record must not find
	// counters on it that nothing has committed.
	out := rec.Clone()
	scope := events.Scope{Tenant: rec.Tenant, Namespace: rec.Namespace, Subject: rec.ID}
	if _, err := lifecycle.Fold(ctx, s.events, s.kv, scope, out); err != nil {
		// The stored view is still a true answer, just an older one. Failing a
		// read because its telemetry could not be topped up would trade the
		// thing the caller wanted for the thing that measures it.
		obs.Logger(ctx).Warn("outstanding recalls could not be folded into a read",
			"tenant", rec.Tenant.String(), "memory", rec.ID.String(), "error", err)
		return rec
	}
	return out
}

// recalled notes that a memory was addressed by id.
//
// # Why this is a recall and a search is not
//
// The caller named this memory. A search discovers memories rather than
// addressing them, so it records nothing — Rust's rule (behaviour baseline §3),
// and the one the differential harness compares against. Budgeted recall does
// not record either, for the same reason: the ranking chose, not the caller.
//
// # Why its failure is not the caller's problem
//
// The read succeeded. A memory the user asked for is in hand, and failing the
// request because a telemetry row could not be appended would trade the thing
// they wanted for the thing that measures it. It is logged instead, and the
// visible cost is a promotion one recall later.
func (s *Service) recalled(ctx context.Context, rec *record.Record) {
	if s.events == nil || rec.Fields.Archived {
		return
	}
	scope := events.Scope{Tenant: rec.Tenant, Namespace: rec.Namespace, Subject: rec.ID}
	if _, err := lifecycle.Record(ctx, s.events, s.kv, scope, s.clk.Now().UTC(),
		s.recallWindow, s.cfg.SyncWrites); err != nil {
		obs.Logger(ctx).Warn("a recall could not be recorded",
			"tenant", rec.Tenant.String(), "memory", rec.ID.String(), "error", err)
	}
}

// Delete retires a memory, or removes it.
//
// Soft is the default and archives: the record, its vector and its export entry
// all survive, and only retrieval changes. Hard removes the record and its
// vector in one transaction, and is the only path that destroys user data —
// which is why it is a parameter a caller has to pass rather than a threshold
// something can cross.
//
// Deleting an already-archived memory again is not an error: the post-condition
// holds.
func (s *Service) Delete(ctx context.Context, rid id.ID, hard bool) error {
	const op = "memory.Delete"

	rec, err := s.repo.Get(ctx, rid)
	if err != nil {
		return err
	}

	t, err := s.requireTenant(ctx)
	if err != nil {
		return err
	}

	if hard {
		if err := s.write(ctx, t, func(tx txn.Tx) error {
			if err := s.repo.Delete(ctx, tx, rid); err != nil {
				return err
			}
			// The memory's relationships go with it, in the same transaction.
			// Leaving them is not merely untidy: traversal would spend its node
			// budget reaching a record that no longer exists, so "memories
			// related to this one" would silently return fewer than it was
			// asked for, and nothing would say why.
			if s.edges != nil {
				if _, err := s.edges.RemoveEvery(ctx, tx, rid); err != nil {
					return errs.E(errs.KindOf(err), op, err)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		// The canonical vector went with the record inside that transaction, so
		// the memory is already unfindable. This tidies the index's own
		// structure; nothing depends on it landing.
		s.removed(ctx, t, rid)
		return nil
	}

	if rec.Fields.Archived {
		return nil
	}
	// Stamped before the transaction, because a retry must write the same
	// memory the first attempt did rather than one dated a moment later
	// (Invariant 9, and see txn.Do).
	now := s.clk.Now().UTC()
	return s.write(ctx, t, func(tx txn.Tx) error {
		// The read is inside the retried region, for the reason Update gives at
		// length: Put conditions its commit on the bytes the record was read as,
		// and txn.Do re-runs only this body. Archiving the record read above —
		// before the transaction existed — made every retry re-issue the same
		// stale expectation, so a loser to any concurrent write of this memory
		// (a second archive, an edit, a sweep) conflicted until its attempt
		// budget ran out rather than converging. Measured: sixteen concurrent
		// archives of one memory, and archives failed on every one of five runs.
		cur, err := s.repo.Get(ctx, rid)
		if err != nil {
			return err
		}
		if cur.Fields.Archived {
			// Archived by someone else first. The post-condition holds, so this
			// is success, and the body writes nothing — an empty commit, which
			// the storage contract suite covers on both adapters.
			return nil
		}
		cur.Fields.Archived = true
		cur.Fields.ArchivedAt = now
		cur.UpdatedAt = now
		if err := s.repo.Put(ctx, tx, cur); err != nil {
			return errs.E(errs.KindOf(err), op, err)
		}
		return nil
	})
}

// write runs one transaction for a tenant: serialised against that tenant's
// other writes, and retried if it still meets a conflict.
//
// Both halves are needed and they do different jobs. The gate removes the
// contention this process creates on rows every write touches; the retry covers
// a conflict the gate cannot see — a genuine concurrent update of the same
// memory, or a writer outside this service.
//
// body may run more than once, so everything a retry must not redraw — the
// record id, the timestamp, the embedding — is computed by the caller before
// this is entered.
//
// The fsync is paid after the gate is released, and write still does not return
// until it has. Held under the gate, a tenant's writes fsynced strictly one at a
// time, and Phase 13 profiled that as 77% of the write path's lock wait: fifty
// concurrent creates queued behind each other's disk sync. Committed unsynced
// under the gate and synced after it, concurrent writes share one fsync, and the
// gate still does the one job it is for — keeping this process's writes off the
// rows every write touches. Nothing is acknowledged before it is durable: the
// write-ahead log is sequential, so a sync issued after the commit covers it.
func (s *Service) write(ctx context.Context, t tenant.ID, body func(tx txn.Tx) error) error {
	unlock := s.writes.Lock(string(t))
	err := txn.Do(ctx, s.kv, body, txn.Sync(false))
	unlock()
	if err != nil || !s.cfg.SyncWrites {
		return err
	}
	return s.kv.Flush(ctx)
}

// requireTenant is the one place below the API layer that turns a missing
// scope into an error, so that adding a method here cannot add a way to forget
// it (Invariant 1).
func (s *Service) requireTenant(ctx context.Context) (tenant.ID, error) {
	return tenant.Require(ctx)
}

var errNoContent = errors.New("a memory must have content")

// boundedQuery refuses query text or a tag filter past its bound. what names
// the text as the caller knows it: a search's query, a recall's context.
func boundedQuery(op, what, text string, tags []string) error {
	if len(text) > MaxQueryBytes {
		return errs.E(errs.Invalid, op, fmt.Errorf("the %s is %d bytes, the limit is %d", what, len(text), MaxQueryBytes))
	}
	if len(tags) > MaxTags {
		return errs.E(errs.Invalid, op, fmt.Errorf("%d tags to filter by, the limit is %d", len(tags), MaxTags))
	}
	return nil
}
