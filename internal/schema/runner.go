package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// RunnerConfig is what a [Runner] needs.
type RunnerConfig struct {
	KV storage.KV

	// Tenants is handed to each step. It is optional: a step that touches only
	// untenanted rows does not need it, and requiring it here would mean the
	// composition root could not run a migration before the directory exists.
	Tenants tenant.Directory

	Clock clock.Clock

	// DataDir is where the pre-migration backup is written, as
	// <DataDir>/.backups/pre-<unix>/. Empty means no backup can be taken, which
	// is a refusal rather than a skip once a step declares RewritesData.
	DataDir string

	// Metrics is optional. Nil means a runner that reports through the log and
	// the durable state row only, which is what a test wants.
	Metrics *obs.Metrics
}

// Runner executes a plan.
//
// One step at a time, in the plan's order, each ending with the manifest
// advanced and the step marked done — in a single transaction, so a directory
// is never left claiming a version whose step did not finish.
type Runner struct {
	cfg RunnerConfig
}

// NewRunner validates the configuration and returns a runner.
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	const op = "schema.NewRunner"
	switch {
	case cfg.KV == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("a migration runner needs a store"))
	case cfg.Clock == nil:
		// Not a defaultable dependency: a migration stamps times into a durable
		// row, and durable business logic never reads the wall clock directly.
		return nil, errs.E(errs.Invalid, op, errors.New("a migration runner needs a clock"))
	}
	return &Runner{cfg: cfg}, nil
}

// Run executes plan against the directory.
//
// The sequence per step, and the order is the whole design:
//
//  1. A step already marked done is skipped, so an ordinary restart rewrites
//     nothing.
//  2. Before the *first* step that rewrites data, a backup is taken. A backup
//     that fails aborts the plan with the data untouched — proceeding would
//     rewrite a corpus having told an operator it was protected.
//  3. The step runs, resuming from whatever cursor the last attempt committed.
//  4. On success, the manifest advance and the done marker commit together. A
//     crash between them would leave a directory claiming a version whose step
//     did not finish, or a finished step the next open would replan.
//  5. On failure, the state row records why, and the plan stops. Later steps
//     declare preconditions the failed one was supposed to establish.
//
// A step must be safe to rerun: a crash anywhere in 3 brings the runner back to
// it with the originals wherever the last checkpoint left them.
func (r *Runner) Run(ctx context.Context, plan []Migration) error {
	const op = "schema.Runner.Run"

	if len(plan) == 0 {
		return nil
	}
	log := obs.Logger(ctx)

	format, err := ReadFormat(ctx, r.cfg.KV)
	if err != nil {
		return err
	}
	if format.Subsystems == nil {
		format.Subsystems = map[string]Subsystem{}
	}

	backedUp := false
	for _, m := range plan {
		state, found, err := ReadState(ctx, r.cfg.KV, m.ID)
		if err != nil {
			return err
		}
		if found && state.State == StateDone {
			log.Debug("migration already applied", "migration", m.ID)
			continue
		}

		if m.RewritesData && !backedUp {
			dest, err := BackUp(ctx, r.cfg.KV, r.cfg.DataDir, r.cfg.Clock.Now())
			if err != nil {
				r.failed(m.ID)
				return errs.E(errs.KindOf(err), op, fmt.Errorf(
					"migration %q rewrites data and its backup could not be taken, so it did not run: %w", m.ID, err))
			}
			// Once per plan, not once per step: one copy of the pre-migration
			// state is the point, and the second step's "before" is the first
			// step's "after".
			backedUp = true
			if dest != "" {
				log.Info("backed up the data directory before migrating", "destination", dest, "migration", m.ID)
			}
		}

		state = r.begin(state, found, m, format)
		if err := WriteState(ctx, r.cfg.KV, state); err != nil {
			return err
		}

		log.Info("running migration", "migration", m.ID, "strategy", m.Strategy.String(),
			"resuming_at", state.Processed, "advances", m.Advances)
		r.running(m.ID, 1)

		if err := r.step(ctx, m, &state); err != nil {
			r.running(m.ID, 0)
			r.failed(m.ID)
			state.State = StateFailed
			state.Error = err.Error()
			state.LastProgressAt = r.cfg.Clock.Now().UTC()
			if werr := WriteState(ctx, r.cfg.KV, state); werr != nil {
				log.Error("the failure of a migration could not be recorded", "migration", m.ID, "error", werr)
			}
			return errs.E(errs.KindOf(err), op, fmt.Errorf("migration %q: %w", m.ID, err))
		}
		r.running(m.ID, 0)

		if err := r.finish(ctx, m, &state, &format); err != nil {
			return err
		}
		log.Info("migration complete", "migration", m.ID, "processed", state.Processed, "format", format.String())
	}
	return nil
}

// begin returns the state a step starts from: a fresh row, or the one a crashed
// attempt left behind.
//
// A row left running is what a crash looks like, and resuming from it is the
// whole reason the row exists. A row left failed is retried from its cursor —
// a step that gave up permanently after one bad disk day would need an operator
// to know it existed before they could ask for it again.
func (r *Runner) begin(state MigrationState, found bool, m Migration, format Format) MigrationState {
	now := r.cfg.Clock.Now().UTC()
	if found && (state.State == StateRunning || state.State == StateFailed) {
		state.State = StateRunning
		state.Error = ""
		state.LastProgressAt = now
		return state
	}
	return MigrationState{
		ID:             m.ID,
		Source:         currentVersions(format),
		Target:         advanced(currentVersions(format), m.Advances),
		State:          StateRunning,
		StartedAt:      now,
		LastProgressAt: now,
	}
}

// step runs the migration body with a checkpoint bound to its state row.
func (r *Runner) step(ctx context.Context, m Migration, state *MigrationState) error {
	mc := &Context{
		KV:      r.cfg.KV,
		Tenants: r.cfg.Tenants,
		Clock:   r.cfg.Clock,
		Resume:  state.Cursor,
		Done:    state.Processed,
	}
	mc.Checkpoint = r.checkpoint(m.ID, state, mc)
	return m.Run(ctx, mc)
}

// checkpoint returns the Checkpoint a step is handed.
//
// The staged cursor and the caller's work commit together, and the in-memory
// state is advanced *only after* that commit succeeds. Advancing it first would
// mean a failed commit still left a cursor claiming the work was done — which
// is the exact bug the whole arrangement exists to prevent, and it would be
// invisible until a resume skipped the records.
func (r *Runner) checkpoint(id string, state *MigrationState, mc *Context) func(context.Context, txn.Tx, []byte, uint64) error {
	return func(ctx context.Context, tx txn.Tx, cursor []byte, processed uint64) error {
		next := *state
		next.Cursor = append([]byte(nil), cursor...)
		next.Processed = processed
		next.State = StateRunning
		next.LastProgressAt = r.cfg.Clock.Now().UTC()

		if err := StageState(tx, next); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		*state = next
		r.progress(id, next.Processed, mc.Total)
		return nil
	}
}

// finish advances the manifest and marks the step done, in one transaction.
//
// Atomicity here is not tidiness. Advancing the manifest first and crashing
// would leave a state row running forever, with nothing that would ever clear
// it; marking the step done first and crashing would leave the manifest behind
// and the step skipped, so the version could never be reached and the next open
// would refuse the directory naming a gap nothing can close.
func (r *Runner) finish(ctx context.Context, m Migration, state *MigrationState, format *Format) error {
	next := *format
	next.Subsystems = make(map[string]Subsystem, len(format.Subsystems))
	for name, s := range format.Subsystems {
		next.Subsystems[name] = s
	}
	for name, v := range m.Advances {
		// Current, MinReader and MinWriter all move. That is the conservative
		// default CurrentFormat already documents: a bump made without thinking
		// about compatibility locks older binaries out rather than letting them
		// in, and letting them in wrongly is the expensive direction.
		next.Subsystems[name] = Subsystem{Current: v, MinReader: v, MinWriter: v}
	}

	done := *state
	done.State = StateDone
	done.Error = ""
	done.LastProgressAt = r.cfg.Clock.Now().UTC()

	tx := txn.New(r.cfg.KV)
	defer tx.Close()

	stageFormat(tx, *format, next)
	if err := StageState(tx, done); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	*state = done
	*format = next
	return nil
}

// currentVersions flattens a manifest to subsystem → version in force.
func currentVersions(f Format) map[string]uint32 {
	out := make(map[string]uint32, len(f.Subsystems))
	for name, s := range f.Subsystems {
		out[name] = s.Current
	}
	return out
}

func advanced(base map[string]uint32, by map[string]uint32) map[string]uint32 {
	out := make(map[string]uint32, len(base))
	for name, v := range base {
		out[name] = v
	}
	for name, v := range by {
		out[name] = v
	}
	return out
}

// --- observability ----------------------------------------------------------
//
// Spec §20 requires migrations to be observable. Three things carry that: the
// structured log lines above, the durable state row (which an operator reads
// with `remem-admin inspect` without needing the log that scrolled past), and
// the metrics below.
//
// migration_progress_ratio and migration_records_remaining are only set by a
// step that declares how much work it has — see [Context.Total]. Most cannot,
// and a ratio invented from a step that does not know its own size would be a
// number a dashboard treats as real.

func (r *Runner) running(id string, v float64) {
	if r.cfg.Metrics == nil {
		return
	}
	r.cfg.Metrics.Migrations.Running.WithLabelValues(id).Set(v)
}

func (r *Runner) failed(id string) {
	if r.cfg.Metrics == nil {
		return
	}
	r.cfg.Metrics.Migrations.FailuresTotal.WithLabelValues(id).Inc()
}

func (r *Runner) progress(id string, processed, total uint64) {
	if r.cfg.Metrics == nil || total == 0 {
		return
	}
	remaining := float64(0)
	if total > processed {
		remaining = float64(total - processed)
	}
	r.cfg.Metrics.Migrations.RecordsRemaining.WithLabelValues(id).Set(remaining)
	r.cfg.Metrics.Migrations.Progress.WithLabelValues(id).Set(float64(processed) / float64(total))
}
