package lifecycle

import (
	"context"
	"sync"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
)

// MaxAttentionInterval caps how far ahead any record is scheduled.
//
// Without it, a `pinned` memory — nothing decays, nothing expires, nothing
// archives — would be scheduled for never and drop out of maintenance
// permanently. That is fine until the tenant's policy table changes, at which
// point there is no walk that reaches the records the old table exempted, and
// no way to find them short of a full scan.
//
// A month is chosen so the cost is one attribute-row read per memory per month
// for records that genuinely have nothing owed, which is the cheapest read in
// the system, and so that a policy change takes effect within a billing period
// rather than within a decade.
const MaxAttentionInterval = 30 * Day

// NextAttention is when a lifecycle sweep next needs to look at this record.
//
// One value shared by every transition — the earliest instant at which any of
// them could have work to do — rather than one index per transition. A memory
// may be woken and turn out to be owed nothing; that costs one row read and no
// payload read, which is much cheaper than maintaining four mutable indexed
// attributes on the write path. Rust reaches the same conclusion at
// services/attrs.rs:44-51 and then walks the index five times anyway.
//
// # It reads no clock
//
// Every arm is a stored timestamp plus a constant, so the answer is a pure
// function of the record and the policy table. That is what puts it outside
// Invariant 8 by construction rather than by care, and what makes a rebuilt
// attribute row comparable with an incrementally maintained one.
//
// The plan's signature takes a `now`; Rust's does not, and it does not need to.
//
// # Each arm mirrors a transition, and the mirroring is the fragile part
//
// A transition whose condition changes without this changing with it stops
// being scheduled for the memories it should act on — silently. Rust holds the
// two together with every_sweeps_due_time_is_covered_by_the_projection, and
// TestEverySweepConditionIsCoveredByTheSchedule does the same here: it asks
// each transition, through its own code, when it would next fire, and fails if
// this scheduled the record any later. It lives beside the transitions rather
// than here, because a test that asked *this* function both questions would be
// comparing a value with itself.
func (p *Policies) NextAttention(rec *record.Record) time.Time {
	pol := p.For(rec)
	cap := latest(rec).Add(MaxAttentionInterval)

	if due := EarliestTransition(pol, rec); !due.IsZero() && due.Before(cap) {
		return due
	}
	return cap
}

// EarliestTransition is the first instant anything is owed to this record under
// this policy, or the zero time if nothing ever is.
//
// It is exported and separate from [Policies.NextAttention] so the schedule can
// be checked against it: NextAttention adds the cap and the policy lookup, and
// this is the arithmetic the sweep's own conditions have to agree with.
func EarliestTransition(pol Policy, rec *record.Record) time.Time {
	f := rec.Fields

	// An archived memory has exactly one thing left to happen to it, and every
	// other transition skips it. Scheduling it for its cleanup date is what
	// keeps archived volume out of the walk entirely.
	//
	// It is scheduled from archived_at rather than from updated_at, which is
	// where Rust puts it — and that difference is why archiving here does not
	// have to move updated_at. See the package comment.
	if f.Archived {
		if pol.CleanupAfter == nil {
			return time.Time{}
		}
		return archivedAt(rec).Add(*pol.CleanupAfter)
	}

	// Flashbulb protection defers decay and forgetting wholesale, so nothing
	// they would do can come due before it lapses (memory_manager.rs:115).
	protected := f.ProtectedUntil

	var due time.Time

	// Expiry: due when the TTL runs out, counted from creation. The record's
	// own TTL wins over the policy's default, which is where Rust keeps it and
	// where the create surface sets it.
	if ttl := effectiveTTL(pol, rec); ttl > 0 {
		due = earlier(due, rec.CreatedAt.Add(ttl))
	}

	// Importance decay: a whole day after the last decay pass, or after
	// creation if none has run.
	if pol.ImportanceDecay < 1 {
		last := f.LastDecayAt
		if last.IsZero() {
			last = rec.CreatedAt
		}
		due = earlier(due, notBefore(last.Add(Day), protected))
	}

	// Active forgetting: a whole day after the last genuine reinforcement or
	// the last health check, whichever is later. Deliberately excludes
	// updated_at, which the lifecycle touches on its own schedule rather than
	// because the memory was recalled or edited (lifecycle_manager.rs:409-415).
	if pol.HealthDecay > 0 {
		reinforced := f.LastRecalledAt
		if reinforced.IsZero() {
			reinforced = f.AccessedAt
		}
		checked := f.LastHealthCheckAt
		if checked.IsZero() {
			checked = rec.CreatedAt
		}
		due = earlier(due, notBefore(later(reinforced, checked).Add(Day), protected))
	}

	return due
}

// effectiveTTL is the record's own TTL, or the policy's default.
//
// The record's own TTL counts only under short_term. Rust expires only
// short-term memories (types.rs:1135-1145) and drops the TTL from anything that
// ends up long-term, so a TTL under another policy — on a record written before
// Phase 13, or one whose policy a PATCH changed — is inert: neither the expiry
// pass nor the schedule acts on it. A policy's own default TTL is an operator's
// explicit setting and still applies whatever the policy is named.
func effectiveTTL(pol Policy, rec *record.Record) time.Duration {
	if rec.Fields.TTL > 0 && rec.Fields.Policy == ShortTerm {
		return rec.Fields.TTL
	}
	if pol.TTL != nil {
		return *pol.TTL
	}
	return 0
}

// archivedAt is when the record was retired, falling back to its last write.
//
// A record marked archived with no timestamp is one Phase 5 or an import wrote;
// treating it as archived at its last update is the reading that eventually
// cleans it up, where treating it as archived at the epoch would delete it on
// the first sweep.
func archivedAt(rec *record.Record) time.Time {
	if !rec.Fields.ArchivedAt.IsZero() {
		return rec.Fields.ArchivedAt
	}
	return rec.UpdatedAt
}

// latest is the newest instant the record itself carries, which is what the cap
// is measured from so that the whole projection stays a pure function.
func latest(rec *record.Record) time.Time {
	out := rec.CreatedAt
	for _, t := range []time.Time{
		rec.UpdatedAt, rec.Fields.ArchivedAt, rec.Fields.AccessedAt,
		rec.Fields.LastRecalledAt, rec.Fields.LastDecayAt, rec.Fields.LastHealthCheckAt,
		rec.Fields.ProtectedUntil,
	} {
		out = later(out, t)
	}
	return out
}

func earlier(a, b time.Time) time.Time {
	if a.IsZero() || b.Before(a) {
		return b
	}
	return a
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// notBefore holds a due time back to a protection window.
func notBefore(due, floor time.Time) time.Time {
	if floor.After(due) {
		return floor
	}
	return due
}

// Source resolves a tenant's policy table.
//
// It is an interface so that [Scheduler] can be given a fixed table in a test
// and the durable directory in a server, and so this package does not have to
// decide where overrides are stored.
type Source interface {
	Policies(ctx context.Context, t tenant.ID) *Policies
}

// Scheduler answers "when does this record next need attention" for a tenant.
//
// It satisfies record.Scheduler, and the repository calls it on every write —
// which is the point. A write path that computed the schedule itself is a write
// path that can omit it, and the symptom is a memory that never decays, never
// expires and never appears in any sweep. The same argument record.WithIndexer
// makes one level up.
type Scheduler struct{ src Source }

// NewScheduler returns the scheduler over a policy source. A nil source uses
// the built-in table, which is what a build with no tenant overrides wants.
func NewScheduler(src Source) *Scheduler { return &Scheduler{src: src} }

// NextAttention implements record.Scheduler.
func (s *Scheduler) NextAttention(ctx context.Context, t tenant.ID, rec *record.Record) time.Time {
	return s.policies(ctx, t).NextAttention(rec)
}

func (s *Scheduler) policies(ctx context.Context, t tenant.ID) *Policies {
	if s == nil || s.src == nil {
		p, _ := NewPolicies(nil)
		return p
	}
	return s.src.Policies(ctx, t)
}

// Registry is the durable [Source]: a tenant's overrides, read from the
// directory row and cached.
//
// # Why it caches
//
// It is consulted on every record write, and a directory read per write would
// put a store round trip on the hottest path in the system to serve a feature
// most tenants never use. The cache is invalidated by the one surface that
// changes an override, which makes it exact on a single node.
//
// Phase 14 makes it stale instead: an override written on one node reaches
// another's cache when that node next misses. The bound is stated here rather
// than discovered, and the fix is a directory watch, not a smaller cache.
//
// # Why a failure does not fail the write
//
// A directory row that cannot be read falls back to the built-in table with one
// warning. The alternative is that a transient store error makes storing a
// memory fail, to protect a policy override; the sweep re-reads the table on
// every run and corrects whatever this got wrong.
type Registry struct {
	dir tenant.Directory
	// load reads one tenant's overrides. It is a field rather than a direct
	// call so the storage of overrides can change without this type changing.
	load func(ctx context.Context, t tenant.ID) (map[string]Policy, error)

	mu    sync.RWMutex
	cache map[tenant.ID]*Policies
}

// NewRegistry returns the registry over a loader.
func NewRegistry(dir tenant.Directory, load func(context.Context, tenant.ID) (map[string]Policy, error)) *Registry {
	return &Registry{dir: dir, load: load, cache: map[tenant.ID]*Policies{}}
}

// Policies returns a tenant's resolved table, reading it once and remembering.
func (r *Registry) Policies(ctx context.Context, t tenant.ID) *Policies {
	r.mu.RLock()
	p, ok := r.cache[t]
	r.mu.RUnlock()
	if ok {
		return p
	}

	built, err := r.resolve(ctx, t)
	if err != nil {
		obs.Logger(ctx).Warn("a tenant's retention policies could not be read; "+
			"the built-in policies are in force for this write",
			"tenant", t.String(), "error", err)
		built, _ = NewPolicies(nil)
		// Deliberately not cached: a transient failure must not pin the
		// built-ins in memory for the life of the process.
		return built
	}

	r.mu.Lock()
	r.cache[t] = built
	r.mu.Unlock()
	return built
}

// Invalidate drops a tenant's cached table, and is called by the surface that
// changes one.
func (r *Registry) Invalidate(t tenant.ID) {
	r.mu.Lock()
	delete(r.cache, t)
	r.mu.Unlock()
}

func (r *Registry) resolve(ctx context.Context, t tenant.ID) (*Policies, error) {
	if r.load == nil {
		return NewPolicies(nil)
	}
	over, err := r.load(ctx, t)
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			// A tenant with no directory row yet — auto-provisioning is in
			// flight — has no overrides, which is not a failure.
			return NewPolicies(nil)
		}
		return nil, err
	}
	return NewPolicies(over)
}
