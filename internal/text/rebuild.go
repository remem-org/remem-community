package text

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// Source yields one tenant's memories, for [Index.Rebuild].
//
// The method is Scan rather than ForEach, for the reason vector.Source gives:
// ForEach is reserved for tenant.Directory.ForEach, the single audited
// cross-tenant path, and the guard in internal/arch polices it by name.
//
// It yields content and tags rather than records, so the rebuild depends on
// what it actually indexes rather than on the record model. What supplies it —
// a scan of canonical bodies — is the caller's business.
type Source interface {
	Scan(ctx context.Context, t tenant.ID, ns tenant.Namespace,
		fn func(rid id.ID, content string, tags []string) error) error
}

// Health says whether this tenant's keyword searches can currently be complete.
//
// It is a separate call rather than a field on every [Hit], and the reason is
// what the state is about: an index is incomplete for a tenant, not for a
// query. Every search that tenant makes is affected identically until the
// rebuild finishes.
//
// This is the interface Phase 7 had to widen vector.Index for, added here from
// the start because the lesson was already paid for: an index that can be
// incomplete must be able to say so, and the caller that asks must be pinned by
// a test. TestKeywordSearchOverARebuildingIndexSaysSo pins the one caller.
type Health struct {
	// Degraded reports that this tenant's keyword searches may be incomplete.
	Degraded bool
	// Reason says what is wrong, in a sentence an operator can act on. It is
	// empty when Degraded is false.
	Reason string
}

// Rebuilder is told that a tenant's keyword index should be rebuilt.
//
// It mirrors vector.Rebuilder deliberately: both indexes can be incomplete for
// a tenant, both discover it on a read, and neither may repair itself there —
// a rebuild started inside the search that found the damage charges one user
// for everyone's repair. The implementation enqueues a durable job (Phase 9);
// before that existed it logged, and a log line is what an operator had to
// notice.
//
// It takes no context and returns no error because it is called from a read
// path that must not wait on it and cannot act on a failure. Whatever
// implements it is responsible for not blocking.
type Rebuilder interface {
	ScheduleRebuild(t tenant.ID, reason string)
}

// RebuilderFunc adapts a function to [Rebuilder].
type RebuilderFunc func(t tenant.ID, reason string)

// ScheduleRebuild calls f.
func (f RebuilderFunc) ScheduleRebuild(t tenant.ID, reason string) { f(t, reason) }

// Health reports whether a tenant's index is complete.
//
// It is one read of the statistics row, which is cheap enough to call on every
// search — unlike counting the postings, which is the corpus.
func (ix *Index) Health(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace) (Health, error) {
	stats, err := ReadStats(ctx, r, t, ns)
	if err != nil {
		return Health{}, err
	}
	if !stats.Rebuilding {
		return Health{}, nil
	}
	return Health{
		Degraded: true,
		Reason: "the keyword index is being rebuilt, or a rebuild was interrupted; " +
			"run `remem-admin text rebuild` and searches will be complete again",
	}, nil
}

// rebuildBatch is how many rows one clearing transaction removes.
//
// Bounded because a tenant's whole text space does not fit in one batch and
// should not have to: the marker is what makes a partial clear safe, so the
// batch size is a memory decision rather than a correctness one.
const rebuildBatch = 1000

// Rebuild replaces one tenant's index from src.
//
// # It is restartable, not resumable
//
// The order is: mark, clear, repopulate, unmark. An interrupted rebuild
// therefore leaves the marker set, [Health] reports the tenant degraded, and
// every search says its answer may be incomplete until somebody runs this
// again. It does not resume from where it stopped.
//
// That is the same disposition Phase 7 settled on for the vector rebuild, and
// the same reason: checkpoint-resumable belongs to the Checkpointer that Phase
// 9 introduces, and inventing a second progress format here would be a durable
// decision taken to save one operator one re-run.
//
// # It cannot lose a memory
//
// Its input is the canonical record bodies and its outputs are only rows in the
// text space. A rebuild that fails half way leaves a corpus that is harder to
// search and has lost nothing — which is the whole meaning of a derived index
// (Invariant 3).
func (ix *Index) Rebuild(ctx context.Context, kv storage.KV, t tenant.ID, ns tenant.Namespace,
	src Source) error {
	const op = "text.Index.Rebuild"

	if t == "" {
		return errs.E(errs.Invalid, op, errNoTenant)
	}
	if src == nil {
		return errs.E(errs.Invalid, op, errors.New("a rebuild needs a source of memories"))
	}

	if err := ix.mark(ctx, kv, t, ns, true, Stats{}); err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}
	if err := ix.clear(ctx, kv, t, ns); err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}

	var built Stats
	tx := txn.New(kv)
	defer tx.Close()
	staged := 0

	flush := func() error {
		if staged == 0 {
			return nil
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		tx.Close()
		tx = txn.New(kv)
		staged = 0
		return nil
	}

	err := src.Scan(ctx, t, ns, func(rid id.ID, content string, tags []string) error {
		if rid.IsZero() {
			return errs.E(errs.Invalid, op, errNoRecord)
		}
		terms := analyse(content, tags)
		row := docRow{DocLen: uint32(terms.length), Terms: make([]string, 0, len(terms.freq))}
		for term := range terms.freq {
			row.Terms = append(row.Terms, term)
		}

		// Written directly rather than through Add: the space has just been
		// cleared, so there is nothing to withdraw, and the totals are summed
		// here instead of being read back and incremented a document at a time.
		ix.stagePostings(tx, t, ns, rid, nil, terms)
		tx.Set(docKey(t, ns, rid), encodeDocRow(row))
		built.Documents++
		built.TotalLength += uint64(row.DocLen)

		staged++
		if staged >= rebuildBatch {
			return flush()
		}
		return ctx.Err()
	})
	if err != nil {
		return errs.E(errs.KindOf(err), op, fmt.Errorf("reading the memories of tenant %s: %w", t, err))
	}
	if err := flush(); err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}

	// The marker is cleared in the same write that installs the totals, so a
	// search can never see a complete index reporting an empty corpus.
	if err := ix.mark(ctx, kv, t, ns, false, built); err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}
	return nil
}

// mark writes the statistics row with the rebuild marker set or cleared.
//
// It is unconditional, unlike every other write to this row. A rebuild owns the
// tenant's index for its duration — the data directory is held under Pebble's
// exclusive lock by the command running it — so there is no concurrent writer
// whose increment it could lose, and a conditional write would only be able to
// fail.
func (ix *Index) mark(ctx context.Context, kv storage.KV, t tenant.ID, ns tenant.Namespace,
	rebuilding bool, stats Stats) error {
	stats.Rebuilding = rebuilding
	return kv.Set(ctx, statsKey(t, ns), encodeStats(stats))
}

// clear removes every row of a tenant's text space except the statistics row,
// which carries the marker that says a clear is in progress.
func (ix *Index) clear(ctx context.Context, kv storage.KV, t tenant.ID, ns tenant.Namespace) error {
	stats := statsKey(t, ns)
	lower, upper := keys.SpaceRange(t, ns, keys.SpaceText)

	for {
		batch, err := ix.collect(ctx, kv, lower, upper, stats)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		tx := txn.New(kv)
		for _, key := range batch {
			tx.Delete(key)
		}
		if err := tx.Commit(ctx); err != nil {
			tx.Close()
			return err
		}
		tx.Close()
	}
}

// collect reads up to one batch of keys to delete. The keys are copied because
// an iterator's are valid only until the next positioning call, and these
// outlive the iterator by design — deleting while iterating a range is how a
// walk starts skipping rows.
func (ix *Index) collect(ctx context.Context, kv storage.KV, lower, upper, skip []byte) ([][]byte, error) {
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	var batch [][]byte
	for ok := it.First(); ok && len(batch) < rebuildBatch; ok = it.Next() {
		if bytes.Equal(it.Key(), skip) {
			continue
		}
		batch = append(batch, bytes.Clone(it.Key()))
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return batch, ctx.Err()
}
