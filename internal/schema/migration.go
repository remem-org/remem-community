package schema

import (
	"context"
	"fmt"
	"sort"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/version"
)

// Strategy is how a migration does its work (spec §20). The three are not
// interchangeable, and the difference is where the cost lands.
type Strategy uint8

const (
	// StrategyUnspecified is the zero value. It is refused at construction, so
	// a step never runs with an unconsidered strategy.
	StrategyUnspecified Strategy = iota

	// StrategyInstant rewrites no data: a new optional field, a new key
	// namespace, a feature flag. It completes before the server listens.
	StrategyInstant

	// StrategyLazy upgrades records as they are read. The step itself does no
	// bulk work — it declares that mixed-format operation is safe, and the
	// upgrading happens in record.Repo.Get. Use it only where a record written
	// in the old format and one written in the new can coexist.
	StrategyLazy

	// StrategyBackground walks the corpus incrementally, checkpointing a
	// cursor. It runs alongside serving, because blocking start-up on it would
	// make upgrade downtime proportional to corpus size — which is the thing
	// this strategy exists to avoid.
	StrategyBackground

	// StrategyRebuild also walks the corpus incrementally, and must finish
	// before the server serves.
	//
	// The difference from StrategyBackground is what the walk produces. A
	// background step transforms data that is already readable, so a request
	// arriving mid-migration reads either the old shape or the new one and both
	// are answerable. A rebuild step produces an *access path* — an index a
	// query walks to find candidates — and a half-built access path does not
	// return old answers, it returns fewer answers. A listing served against a
	// third-built attribute index reports two hundred memories out of a
	// thousand and reports it as the whole corpus.
	//
	// There is no honest flag for that. `Truncated` means the widening budget
	// was spent and must not be overloaded, and a second flag would put the
	// burden on every client to check it — with the ones that forget showing
	// users a wrong answer that looks right. So the cost is paid where it is
	// visible instead: the server refuses traffic until the index exists, and
	// upgrade downtime is proportional to corpus size.
	StrategyRebuild
)

var strategyNames = [...]string{
	StrategyUnspecified: "unspecified",
	StrategyInstant:     "instant",
	StrategyLazy:        "lazy",
	StrategyBackground:  "background",
	StrategyRebuild:     "rebuild",
}

func (s Strategy) String() string {
	if int(s) < len(strategyNames) && strategyNames[s] != "" {
		return strategyNames[s]
	}
	return fmt.Sprintf("strategy(%d)", uint8(s))
}

// Migration is one step that moves a directory's durable format forward.
//
// A step declares what must be true before it runs and what is true after, and
// the registry derives execution order from those declarations. That is
// deliberate: a list ordered by position means inserting a step in the middle
// silently reorders the ones already shipped, and the failure surfaces as data
// migrated by the wrong step rather than as an error.
type Migration struct {
	// ID is stable and never reused. It is how a run interrupted halfway finds
	// its own durable state again, so renaming one loses a resume point.
	ID string

	// Requires is subsystem → the exact version that must be on disk. Exact,
	// not "at least": a step written against version 2 has not been thought
	// about against version 3, and running it there is a guess.
	Requires map[string]uint32

	// Advances is subsystem → the version in force once the step succeeds.
	Advances map[string]uint32

	// RewritesData is true when the step changes bytes that already exist, and
	// it is what triggers the pre-migration backup. A step that only adds rows
	// leaves the originals intact and needs none.
	RewritesData bool

	Strategy Strategy

	// Run does the work. It must be safe to rerun: a crash between the work and
	// the manifest write brings the runner back here with the originals
	// wherever the last checkpoint left them.
	Run func(ctx context.Context, mc *Context) error
}

// Context is what a migration is handed.
//
// It carries no logger and no metrics: those belong to the runner, which
// reports on the step rather than letting each step invent its own account of
// itself.
type Context struct {
	KV      storage.KV
	Tenants tenant.Directory
	Clock   clock.Clock

	// Resume is the cursor the previous attempt last checkpointed, nil on the
	// first run. A step that walks a range seeks to it.
	Resume []byte

	// Done is the processed count carried forward from the previous attempt,
	// so progress reporting is about the migration rather than about this
	// process's share of it.
	Done uint64

	// Total is how much work the step has, when it knows. A step that sets it
	// gets migration_progress_ratio and migration_records_remaining reported;
	// one that leaves it zero gets neither, because a ratio invented from a
	// step that does not know its own size is a number a dashboard treats as
	// real. It may be set at any point before the first checkpoint.
	Total uint64

	// Checkpoint stages this migration's cursor into tx and commits it.
	//
	// The work and the cursor land together or not at all, which is what makes
	// resume exact rather than approximate: there is no window in which the
	// cursor says a record is done and the record is not. A migration commits
	// through this and never calls tx.Commit itself — which is why Checkpoint
	// takes the transaction rather than being told about it afterwards.
	Checkpoint func(ctx context.Context, tx txn.Tx, cursor []byte, processed uint64) error
}

// Registry is the ordered set of migrations a binary ships.
type Registry struct {
	migrations []Migration
}

// NewRegistry validates the binary's migration list and returns it.
//
// supported is the set of versions this binary writes — version.Current() in
// the composition root, injected in tests, exactly as [Open] takes it. It is a
// parameter rather than a package lookup because a step advancing a format past
// what the binary writes is checked against it, and a test cannot exercise that
// check against a constant.
//
// Every refusal here is a programming error in that list. Caught at
// construction it is a start-up failure naming the step; caught at run time it
// is a half-migrated directory, which is the more expensive of the two by a
// wide margin.
func NewRegistry(supported version.Versions, ms ...Migration) (*Registry, error) {
	const op = "schema.NewRegistry"

	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
	}

	seenID := make(map[string]bool, len(ms))
	// seenAdvance keys on subsystem and target version: two steps claiming to
	// produce the same version of the same format is an ambiguity the planner
	// would resolve by coin toss.
	type advance struct {
		subsystem string
		to        uint32
	}
	seenAdvance := map[advance]string{}

	for _, m := range ms {
		switch {
		case m.ID == "":
			return nil, bad("a migration has no id; the id is how an interrupted run finds its own state")
		case m.Run == nil:
			return nil, bad("migration %q has no Run", m.ID)
		case m.Strategy == StrategyUnspecified:
			return nil, bad("migration %q has no strategy; instant, lazy and background put the cost in different places "+
				"and the choice is not a default", m.ID)
		case len(m.Advances) == 0:
			return nil, bad("migration %q advances nothing, so nothing would ever mark it done", m.ID)
		case seenID[m.ID]:
			return nil, bad("migration id %q is used twice; ids are never reused", m.ID)
		}
		seenID[m.ID] = true

		for name := range m.Requires {
			if _, known := version.SupportedVersion(name); !known {
				return nil, bad("migration %q requires %q, which is not a durable format this binary versions", m.ID, name)
			}
		}
		for name, to := range m.Advances {
			if _, known := version.SupportedVersion(name); !known {
				return nil, bad("migration %q advances %q, which is not a durable format this binary versions", m.ID, name)
			}
			if writes, _ := supported.Get(name); to > writes {
				return nil, bad("migration %q advances %s to version %d, but this binary writes version %d; "+
					"the step would leave a directory this same binary then refuses to open",
					m.ID, version.Describe(name), to, writes)
			}
			if from := m.Requires[name]; to <= from {
				return nil, bad("migration %q advances %s from %d to %d; a step must move a version forward",
					m.ID, version.Describe(name), from, to)
			}
			if other, dup := seenAdvance[advance{name, to}]; dup {
				return nil, bad("migrations %q and %q both advance %s to version %d; "+
					"only one step may produce a given version of a format", other, m.ID, name, to)
			}
			seenAdvance[advance{name, to}] = m.ID
		}
	}

	out := make([]Migration, len(ms))
	copy(out, ms)
	return &Registry{migrations: out}, nil
}

// All returns the registered migrations, in declaration order. Declaration
// order is not execution order — see [Registry.Plan] — and this exists for
// `remem-admin inspect` rather than for the runner.
func (r *Registry) All() []Migration {
	out := make([]Migration, len(r.migrations))
	copy(out, r.migrations)
	return out
}

// Plan returns the steps that move a directory at `from` to the versions in
// `to`, in the order they must run.
//
// Order comes from the declarations, never from registration order: a step runs
// once every version it requires holds exactly, and the result is a total order
// because ties are broken by id. That determinism is not cosmetic — a plan that
// varied between runs would resume a crashed migration into a different
// sequence than the one that crashed.
//
// A plan that cannot reach the target fails, naming the format it got stuck on.
// The alternative is opening a directory nothing has actually migrated.
func (r *Registry) Plan(from Format, to version.Versions) ([]Migration, error) {
	const op = "schema.Registry.Plan"

	// Absent reads as zero, which is older than anything: a directory written
	// before a format existed migrates forward rather than being refused.
	state := make(map[string]uint32, len(version.FormatNames()))
	for _, name := range version.FormatNames() {
		state[name] = from.Subsystems[name].Current
	}

	remaining := make([]Migration, len(r.migrations))
	copy(remaining, r.migrations)
	sort.Slice(remaining, func(i, j int) bool { return remaining[i].ID < remaining[j].ID })

	target := make(map[string]uint32, len(version.FormatNames()))
	for _, name := range version.FormatNames() {
		target[name], _ = to.Get(name)
	}

	var plan []Migration
	for {
		next := -1
		for i, m := range remaining {
			if applies(m, state, target) {
				next = i
				break
			}
		}
		if next < 0 {
			break
		}
		m := remaining[next]
		for name, v := range m.Advances {
			state[name] = v
		}
		plan = append(plan, m)
		remaining = append(remaining[:next], remaining[next+1:]...)
	}

	for _, name := range version.FormatNames() {
		want, _ := to.Get(name)
		if got := state[name]; got < want {
			return nil, errs.E(errs.MigrationRequired, op, fmt.Errorf(
				"no migration path for %s: this directory reaches version %d and this binary needs version %d. "+
					"The step that closes that gap is missing from this build — upgrade to one that ships it "+
					"(binary version %s)",
				version.Describe(name), got, want, version.Binary))
		}
	}
	return plan, nil
}

// applies reports whether a step's preconditions hold exactly, whether it still
// has something to contribute, and whether it stays inside the plan's target.
//
// The second condition is what stops a completed step from being replanned on
// every open. The third is what makes "plan from here to there" mean it: a step
// producing a version beyond the target is a step for a later binary, and
// running it would leave a directory this one refuses to reopen.
func applies(m Migration, state, target map[string]uint32) bool {
	for name, want := range m.Requires {
		if state[name] != want {
			return false
		}
	}
	for name, to := range m.Advances {
		if to > target[name] {
			return false
		}
	}
	for name, to := range m.Advances {
		if state[name] < to {
			return true
		}
	}
	return false
}

// Split divides a plan at the first background step: what runs before the
// server listens, and what runs alongside it.
//
// The split is by position, not by strategy, and that is the whole point. An
// instant step registered after a background one requires the version the
// background step produces, so promoting it to start-up would run it against a
// directory that has not been migrated yet. Everything from the first
// background step onward keeps the plan's order and waits its turn.
//
// StrategyRebuild is on the start-up side, and a rebuild step registered *after*
// a background one therefore still waits — which is the correct reading of "by
// position": it needs the version that background step produces, and running it
// early would rebuild an index over data that has not been migrated.
func Split(plan []Migration) (startup, background []Migration) {
	for i, m := range plan {
		if m.Strategy == StrategyBackground {
			return plan[:i:i], plan[i:]
		}
	}
	return plan, nil
}
