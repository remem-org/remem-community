package schema

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/version"
)

// RowIndexer writes a record's derived attribute rows.
//
// It is an interface here and implemented in internal/attr for one reason: attr
// depends on this package for its slot table, so this package cannot depend on
// attr. The alternative — putting the migration in attr — would mean the list
// of steps a binary ships is no longer in one place, which is the property that
// lets `remem-admin` print what a build knows how to do.
type RowIndexer interface {
	Stage(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace,
		rid id.ID, rec *record.Record) error
}

// backfillBatch is how many records one checkpointed transaction covers.
//
// It trades restart cost against commit cost. Too small and the migration is
// one fsync per record; too large and a crash re-does more work than it saved.
// A thousand records is a few hundred kilobytes of index entries, which is a
// batch a store commits without noticing.
const backfillBatch = 1000

// AttrBackfillID is the backfill step's stable id. It is exported so a test can
// name the step it is asserting about, and so an operator can find its state
// row.
const AttrBackfillID = "0001-attribute-rows"

// Builtin is the migration list this binary ships, in declaration order.
//
// A migration is added here and nowhere else. The list lives in this package
// rather than in the composition root so that a step's declarations are
// reviewed beside the rules that validate them, and so `remem-admin` can print
// what a binary knows how to do without starting a server.
//
// The function takes the versions this binary writes rather than reading them
// from the version package, for the reason [NewRegistry] does: the check that
// no step advances a format past what the binary writes cannot be exercised
// against a constant.
func Builtin(supported version.Versions, indexer RowIndexer, reindexer TextReindexer) (*Registry, error) {
	return NewRegistry(supported, attrBackfill(indexer), textIndexRebuild(reindexer))
}

// TextReindexer queues a rebuild of one tenant's keyword index inside tx.
//
// It is an interface here for the reason [RowIndexer] is: the job framework
// that runs the rebuild is not something this package may depend on. The
// composition root implements it by staging a text.rebuild job.
type TextReindexer interface {
	StageTextRebuild(ctx context.Context, tx txn.Tx, t tenant.ID) error
}

// TextIndexRebuildID is the text-index step's stable id.
const TextIndexRebuildID = "0002-text-index-unicode-17"

// textIndexRebuild queues one keyword-index rebuild per tenant, and advances
// text_index from absent to 1.
//
// # What it is for
//
// Phase 13 moved golang.org/x/text from v0.25.0 to v0.39.0 for GO-2026-5970.
// On Go 1.27 the newer version normalises with Unicode 17 tables where the
// older used Unicode 15. Normalisation decides the terms written into the
// postings key space, so a posting written before the upgrade can disagree
// with the term a query normalises to after it. The characters affected are
// those Unicode 16 and 17 assigned; normalisation is stable for everything
// earlier, so the rebuild is expected to change little. It is paid anyway,
// because "expected to change little" is not a property anyone checks.
//
// # Why it queues rather than rebuilds
//
// The rebuild is the text.rebuild job, which already exists, already runs one
// at a time per tenant, and already makes a keyword search report itself
// incomplete while it runs. A migration that rebuilt inline would be a second
// rebuild path racing the first. Queuing is instant, so the step is: it runs
// before the listener binds and costs one job row per tenant. The job and the
// cursor commit together, so a resumed run does not queue a tenant twice.
func textIndexRebuild(reindexer TextReindexer) Migration {
	return Migration{
		ID:           TextIndexRebuildID,
		Requires:     map[string]uint32{"text_index": 0},
		Advances:     map[string]uint32{"text_index": 1},
		RewritesData: false,
		Strategy:     StrategyInstant,
		Run: func(ctx context.Context, mc *Context) error {
			const op = "schema.textIndexRebuild"
			if reindexer == nil {
				return errs.E(errs.Invalid, op, errors.New(
					"the text-index step has no way to queue a rebuild; this build changed how postings "+
						"are derived and registered nothing to re-derive them"))
			}
			if mc.Tenants == nil {
				return errs.E(errs.Invalid, op, errors.New(
					"the text-index step needs the tenant directory: indexes are per tenant"))
			}
			resume := tenant.ID(mc.Resume)
			reached := resume == ""
			done := mc.Done
			return mc.Tenants.ForEach(ctx, func(t tenant.ID) error {
				// Tenants come back in id order, and the cursor is the last
				// tenant whose rebuild was queued.
				if !reached {
					reached = t == resume
					return nil
				}
				tx := txn.New(mc.KV)
				defer tx.Close()
				if err := reindexer.StageTextRebuild(tenant.NewContext(ctx, t), tx, t); err != nil {
					return errs.E(errs.KindOf(err), op, fmt.Errorf("queuing tenant %s's rebuild: %w", t, err))
				}
				done++
				return mc.Checkpoint(ctx, tx, []byte(t), done)
			})
		},
	}
}

// attrBackfill builds attribute rows and slot indexes for every record.
//
// # What it is for
//
// A directory written before this build has record bodies and no attribute
// rows. Listing and filtering are answered by walking a slot index, so on such
// a directory every listing would return nothing at all — not an error, not a
// warning, just an empty page from a corpus of thousands. The step exists to
// close that gap before anything can observe it.
//
// # Why it blocks start-up
//
// StrategyRebuild, not StrategyBackground, and the reasoning is on the strategy
// itself: a half-built access path returns fewer answers rather than older
// ones, and there is no honest way to say so in a response. The price is
// upgrade downtime proportional to corpus size, paid once, visibly.
//
// # Why it takes no backup
//
// RewritesData is false, and that is a claim about what the step touches: it
// writes into the attribute row and attribute index spaces only, and reads
// record bodies without modifying them. Nothing that exists is overwritten, so
// there is nothing a backup would preserve. If this step ever starts rewriting
// bodies, the flag moves with it.
func attrBackfill(indexer RowIndexer) Migration {
	return Migration{
		ID:           AttrBackfillID,
		Requires:     map[string]uint32{"attr_schema": 1},
		Advances:     map[string]uint32{"attr_schema": 2},
		RewritesData: false,
		Strategy:     StrategyRebuild,
		Run: func(ctx context.Context, mc *Context) error {
			return runAttrBackfill(ctx, mc, indexer)
		},
	}
}

func runAttrBackfill(ctx context.Context, mc *Context, indexer RowIndexer) error {
	const op = "schema.attrBackfill"

	if indexer == nil {
		return errs.E(errs.Invalid, op, errors.New(
			"the attribute backfill has no indexer to write rows with; this is a build that "+
				"registered a slot table and no way to populate it"))
	}
	if mc.Tenants == nil {
		return errs.E(errs.Invalid, op, errors.New(
			"the attribute backfill needs the tenant directory: rows are per tenant and there is "+
				"no unscoped write path (Invariant 1)"))
	}

	resumeTenant, resumeAfter, err := decodeBackfillCursor(mc.Resume)
	if err != nil {
		return err
	}

	// Declare the size before the first checkpoint, so the runner reports
	// progress and records remaining. This step blocks start-up for a time
	// proportional to the corpus, which is exactly when an operator most wants
	// to know how much is left — and until Phase 13 no shipped step declared a
	// size, so those two metrics could never be recorded by this binary.
	//
	// The count is of the whole corpus even on a resumed run: Done carries what
	// earlier attempts finished, so the ratio stays right across a restart.
	total, err := countRecords(ctx, mc)
	if err != nil {
		return err
	}
	mc.Total = total

	repo := record.NewRepo(mc.KV)
	done := mc.Done
	reached := resumeTenant == ""

	return mc.Tenants.ForEach(ctx, func(t tenant.ID) error {
		// Tenants come back in id order, so a resumed run skips every tenant
		// that finished before the crash without re-reading it.
		if !reached {
			if t != resumeTenant {
				return nil
			}
			reached = true
		}
		after := resumeAfter
		resumeAfter = nil

		// The tenant travels in the context because every read below is a
		// scoped read; ForEach is the one place a cross-tenant loop is legal,
		// and this is where it turns back into a scoped one.
		tctx := tenant.NewContext(ctx, t)

		for {
			snap := mc.KV.NewSnapshot()
			recs, err := repo.Scan(tctx, snap, after, backfillBatch)
			_ = snap.Close()
			if err != nil {
				return errs.E(errs.KindOf(err), op, err)
			}
			if len(recs) == 0 {
				return nil
			}

			tx := txn.New(mc.KV)
			for _, rec := range recs {
				if err := indexer.Stage(tctx, tx, t, rec.Namespace, rec.ID, rec); err != nil {
					tx.Close()
					return errs.E(errs.KindOf(err), op, err)
				}
			}
			last := recs[len(recs)-1].ID
			done += uint64(len(recs))

			// The rows and the cursor commit together, so resume is exact
			// rather than approximate: there is no window in which the cursor
			// says a record is indexed and it is not.
			if err := mc.Checkpoint(ctx, tx, encodeBackfillCursor(t, last), done); err != nil {
				tx.Close()
				return err
			}
			tx.Close()

			after = &last
			if len(recs) < backfillBatch {
				return nil
			}
		}
	})
}

// countRecords counts every tenant's records by walking the record keys.
//
// It pays one pass over the record space before the backfill's own pass. That
// is the price of an honest progress figure, and it is small beside the
// backfill, which decodes every body and writes rows for it.
func countRecords(ctx context.Context, mc *Context) (uint64, error) {
	const op = "schema.attrBackfill"
	var n uint64
	err := mc.Tenants.ForEach(ctx, func(t tenant.ID) error {
		lower, upper := keys.SpaceRange(t, tenant.DefaultNamespace, keys.SpaceRecord)
		it := mc.KV.NewIterator(lower, upper)
		for ok := it.First(); ok; ok = it.Next() {
			n++
		}
		err := it.Error()
		_ = it.Close()
		if err != nil {
			return errs.E(errs.KindOf(err), op, err)
		}
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
		return nil
	})
	return n, err
}

// encodeBackfillCursor is <tenant length><tenant><record id>.
//
// It is not a storage key. A storage key would be shorter to produce and would
// tie the resume point to the key encoding's version, so a key-encoding
// migration running beside this one would leave a cursor neither step could
// read.
func encodeBackfillCursor(t tenant.ID, rid id.ID) []byte {
	out := make([]byte, 0, 2+len(t)+16)
	out = binary.AppendUvarint(out, uint64(len(t)))
	out = append(out, t...)
	return append(out, rid[:]...)
}

func decodeBackfillCursor(b []byte) (tenant.ID, *id.ID, error) {
	const op = "schema.decodeBackfillCursor"

	if len(b) == 0 {
		return "", nil, nil
	}
	n, w := binary.Uvarint(b)
	if w <= 0 || uint64(len(b[w:])) < n+16 {
		return "", nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the attribute backfill's saved position will not parse; it cannot be resumed from"))
	}
	t := tenant.ID(b[w : w+int(n)])
	rid, err := id.FromBytes(b[w+int(n):])
	if err != nil {
		return "", nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the attribute backfill's saved position holds a malformed record id"))
	}
	return t, &rid, nil
}
