package discovery

import (
	"context"
	"errors"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// DefaultSubjectsPerJob is how many memories one backfilled job carries.
//
// The bound is not the row's size — [MaxSubjects] would allow ten times this —
// it is how much work one claim represents. A worker holding a lease over a
// thousand vector searches is a worker whose lease lapses halfway, and the job
// is then reclaimed and redone from its last checkpoint by somebody else.
const DefaultSubjectsPerJob = 100

// scanPage is how many records one page of the walk holds. It bounds the
// iterator's working set, not the backfill.
const scanPage = 256

// BackfillStats is what one tenant's backfill enqueued.
type BackfillStats struct {
	// Memories is how many were queued for discovery.
	Memories int
	// Archived is how many were passed over. An archived memory neither
	// discovers nor is discovered, so queuing one would be a job that reads a
	// record to decide to do nothing.
	Archived int
	// Jobs is how many rows were written.
	Jobs int
}

// Backfill enqueues discovery for every live memory in one tenant.
//
// # Why it exists
//
// Discovery is enqueued by the write that creates a memory, so a corpus written
// before this phase — or while `discovery.enabled` was off, or during an
// outage that exhausted a job's attempts — has memories that were never
// considered. Nothing else notices: there is no "was this memory discovered"
// flag to sweep on, deliberately, because that flag would be a second durable
// fact about every memory that discovery would then have to keep true.
//
// # Why it is not a recurring job
//
// The second pass over an unchanged memory finds the same neighbours already
// linked and writes nothing, so a periodic backfill would redo a corpus's worth
// of vector searches for no result. It is a command an operator runs when they
// know why.
//
// # One snapshot
//
// The whole walk reads one pinned view, so a memory written while it runs is
// either wholly in the backfill or wholly absent — and a memory absent from it
// was enqueued by its own write, which is the case this is here to cover the
// absence of.
func Backfill(ctx context.Context, kv storage.KV, repo record.Repo, enq *Enqueuer,
	sc Scope, perJob int,
) (BackfillStats, error) {
	const op = "discovery.Backfill"

	var stats BackfillStats
	switch {
	case kv == nil || repo == nil:
		return stats, errs.E(errs.Invalid, op, errors.New("a backfill needs a store and a repository"))
	case enq == nil:
		return stats, errs.E(errs.Invalid, op, errors.New(
			"a backfill needs somewhere to enqueue: without a queue it would read the whole corpus and do nothing"))
	case sc.Tenant == "":
		return stats, errs.E(errs.Invalid, op, errors.New(
			"a backfill requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	if perJob <= 0 || perJob > MaxSubjects {
		perJob = DefaultSubjectsPerJob
	}

	ctx = tenant.NewContext(ctx, sc.Tenant)
	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	batch := make([]id.ID, 0, perJob)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := enq.Submit(ctx, sc.Tenant, sc.namespace(), batch); err != nil {
			return err
		}
		stats.Jobs++
		batch = batch[:0]
		return nil
	}

	var from *id.ID
	for {
		page, err := repo.Scan(ctx, snap, from, scanPage)
		if err != nil {
			return stats, err
		}
		if len(page) == 0 {
			break
		}
		for _, rec := range page {
			if rec.Fields.Archived {
				stats.Archived++
				continue
			}
			batch = append(batch, rec.ID)
			stats.Memories++
			if len(batch) == perJob {
				if err := flush(); err != nil {
					return stats, err
				}
			}
		}
		last := page[len(page)-1].ID
		from = &last
		if len(page) < scanPage {
			break
		}
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}
