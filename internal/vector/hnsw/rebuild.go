package hnsw

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// rebuildBatch is how many records one committed step inserts. It bounds the
// size of one commit and, with a checkpoint saved after each, how much work an
// interrupted rebuild repeats.
const rebuildBatch = 2048

// The rebuild cursor. It is private to this package — a job row carries it as
// opaque bytes — and versioned, because a row can outlive the binary that wrote
// it.
//
//	[u8 version] [u8 phase] [uvarint records inserted so far]
//
// There is one phase, and that is the design rather than a placeholder. Once
// the canonical vectors are replaced and the node records cleared, what is left
// to do is a function of the store — every canonical vector with no node record
// — so the cursor needs to say only that the destructive first half committed.
// The count is for an operator reading the job, not for the resume: a crash
// between a batch's commit and the save of its count repeats nothing, because
// the records that batch inserted already have their node records.
const (
	cursorVersion byte = 1
	phaseReplaced byte = 1
)

func encodeCursor(inserted uint64) []byte {
	return binary.AppendUvarint([]byte{cursorVersion, phaseReplaced}, inserted)
}

// resumable reports whether cursor is one this binary wrote after the replace
// committed. Anything else — nil, a foreign version, a truncated row — starts
// from the beginning, which is always a correct answer to "rebuild this".
func resumable(cursor []byte) bool {
	return len(cursor) >= 2 && cursor[0] == cursorVersion && cursor[1] == phaseReplaced
}

// Rebuild replaces the tenant's vectors and index from src.
//
// It is the recovery path Invariant 3 requires, and it is also how a corpus is
// imported. Both halves are replaced, because for this index they are one
// thing: the canonical vectors decide what is in the graph, so rebuilding the
// graph from a source that disagreed with them would produce an index that the
// next materialisation would immediately undo.
//
// # How it resumes
//
// The first half is destructive and atomic: drain src, replace the canonical
// vectors, clear the node records, save the cursor. Every refusal the source
// can produce happens during the drain, before a byte of the tenant's data is
// touched.
//
// The second half is the materialisation every cold tenant already goes
// through — place the node records that exist, insert the canonical vectors
// that have none, in key order — done in committed batches rather than in one
// transaction. So an interrupted rebuild leaves exactly the state a
// materialisation finishes, and a resumed one skips the first half and finishes
// it. Layers come from record ids and insertion is in id order, so the graph a
// resumed rebuild ends with is the graph an uninterrupted one builds;
// TestCrashDuringRebuildResumes compares the node records as bytes.
//
// # Why it works through the resident graph
//
// The batches are inserted into the tenant's resident graph under its lock,
// rather than into a private one published at the end. A live write arriving
// mid-rebuild then inserts into the same graph with the same node numbering, and
// a search reads it and reports `truncated` through Health. A private graph
// would leave live writes landing in a stale one whose node numbers collide
// with the rebuild's.
//
// A rebuild that fails part-way drops the tenant from memory rather than leave
// a partial graph resident and reporting itself healthy: the next reader
// materialises it, which completes it.
func (x *Index) Rebuild(ctx context.Context, t tenant.ID, src vector.Source, opts ...vector.RebuildOption) error {
	const op = "hnsw.Rebuild"

	if err := checkScope(t, op); err != nil {
		return err
	}
	if src == nil {
		return errs.E(errs.Invalid, op, errors.New("a rebuild needs a source"))
	}
	o := vector.BuildRebuildOptions(opts...)
	batch := o.BatchSize
	if batch <= 0 {
		batch = rebuildBatch
	}
	save := o.Save
	if save == nil {
		save = func(context.Context, []byte) error { return nil }
	}

	if !resumable(o.Cursor) {
		if err := x.replace(ctx, t, src); err != nil {
			return err
		}
		if err := save(ctx, encodeCursor(0)); err != nil {
			return err
		}
	}

	r, work, res, err := x.residentForRebuild(ctx, t)
	if err != nil {
		return err
	}
	finished := false
	defer func() {
		x.release(r)
		if finished {
			// What the materialisation could not place is what this rebuild
			// could not repair, so the tenant keeps saying so. res.Corrupt is
			// deliberately not carried over: the unreadable node records were
			// deleted below and their vectors re-inserted, which is precisely
			// the repair a rebuild exists to perform, so clearing that part of
			// the reason is the truth. Reporting a healthy tenant because a
			// rebuild ran is how a dropped record became invisible.
			x.finishRebuilding(t, degradedReason(loadResult{
				Unplaceable: res.Unplaceable,
				Width:       res.Width,
			}))
		} else {
			x.forget(t)
		}
	}()

	if err := x.dropUnreadable(ctx, work.unreadable); err != nil {
		return err
	}

	var inserted uint64
	for start := 0; start < len(work.pending); start += batch {
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
		end := min(start+batch, len(work.pending))
		n, sz, err := x.insertBatch(ctx, t, r, work.pending[start:end], work.vectors)
		// Charged per batch, failed or not, so the budget is honest for the
		// whole of a long rebuild rather than only at its end.
		x.charge(r, sz)
		if err != nil {
			return err
		}
		inserted += uint64(n)
		if err := save(ctx, encodeCursor(inserted)); err != nil {
			return err
		}
	}

	finished = true
	x.evict()
	return nil
}

// replace is the destructive first half: the canonical vectors become exactly
// what src holds, and every node record is removed.
func (x *Index) replace(ctx context.Context, t tenant.ID, src vector.Source) error {
	const op = "hnsw.Rebuild"

	// Drain first. Every refusal the source can produce happens before a byte
	// of the tenant's data is touched, which is what makes a failed rebuild a
	// no-op rather than a partial one.
	vectors := make(map[id.ID]*vector.Vector)
	if err := src.Scan(ctx, t, func(rid id.ID, v []float32) error {
		if len(v) == 0 {
			return errs.E(errs.Invalid, op, errors.New("an empty vector cannot be indexed"))
		}
		vectors[rid] = &vector.Vector{
			// x.modelID and not stampedModel: when this server was not told
			// which model it runs, the model id is left empty so that
			// vector.Store.Replace keeps the one already recorded. A rebuild
			// reads coordinates through vector.Source and cannot know what
			// produced them; writing "unknown" over a corpus that did know is
			// how `remem-admin vector rebuild` made a tenant unsearchable.
			ModelID: x.modelID,
			Dim:     len(v),
			Values:  append([]float32(nil), v...),
		}
		return nil
	}); err != nil {
		return err
	}

	x.markRebuilding(t)
	if err := x.store.Replace(ctx, t, tenant.DefaultNamespace, vectors); err != nil {
		return err
	}
	if err := x.clearNodes(ctx, t); err != nil {
		return err
	}
	// The resident graph, if any, describes node records that no longer exist.
	x.forget(t)
	return nil
}

// residentForRebuild installs a freshly materialised graph as the tenant's
// resident one, marked as rebuilding, and pins it.
//
// It replaces whatever was resident rather than reusing it: a graph loaded
// before the node records were cleared describes rows that are gone.
//
// It returns what the materialisation learned rather than discarding it. A
// rebuild repairs what it can and must report what it could not, and the
// materialisation is the only thing that saw the damage: swallowing the result
// here is how a rebuild that dropped a canonical vector came to report a
// healthy tenant.
func (x *Index) residentForRebuild(ctx context.Context, t tenant.ID) (*resident, pendingWork, loadResult, error) {
	x.mu.Lock()
	r, ok := x.tenants[t]
	if !ok {
		r = &resident{t: t}
		x.tenants[t] = r
	}
	r.pin++
	x.touch(t)
	x.mu.Unlock()

	var work pendingWork
	r.mu.Lock()
	g, res, err := x.materialise(ctx, t, &work)
	if err != nil {
		r.mu.Unlock()
		x.release(r)
		return nil, work, res, err
	}
	r.g = g
	r.loaded = true
	r.rebuilding = true
	// Honest for the duration: the rebuild has not repaired anything yet, and
	// Health reports "being rebuilt" ahead of this either way. The reason the
	// rebuild settles on is written when it finishes.
	r.degraded = degradedReason(res)
	sz := r.measure()
	r.mu.Unlock()
	x.charge(r, sz)
	return r, work, res, nil
}

// dropUnreadable deletes node records a materialisation could not decode. A
// fresh rebuild has none, since it cleared them; a resumed one may.
func (x *Index) dropUnreadable(ctx context.Context, doomed [][]byte) error {
	if len(doomed) == 0 {
		return nil
	}
	tx := txn.New(x.kv)
	defer tx.Close()
	for _, k := range doomed {
		tx.Delete(k)
	}
	return tx.Commit(ctx)
}

// insertBatch inserts one batch into the resident graph and commits every node
// record the batch changed, returning how many records it inserted and the
// graph's size afterwards.
//
// The lock is held through the commit, so a live write cannot stage a neighbour
// list between this batch's staging and its commit and then be overwritten by
// the older one.
func (x *Index) insertBatch(ctx context.Context, t tenant.ID, r *resident,
	ids []id.ID, vectors map[id.ID]*vector.Vector,
) (_ int, sz size, _ error) {
	const op = "hnsw.Rebuild"

	r.mu.Lock()
	defer r.mu.Unlock()
	// The batch's size is measured under the lock it inserted under, on every
	// return, and charged by the caller once that lock is released.
	defer func() { sz = r.measure() }()

	dirty := map[uint32]struct{}{}
	inserted := 0
	for _, rid := range ids {
		if _, present := r.g.byID[rid]; present {
			continue // a live write indexed it while the rebuild was running
		}
		v, ok := vectors[rid]
		if !ok {
			continue
		}
		nums, err := r.g.insert(rid, v.Values)
		if err != nil {
			return inserted, sz, errs.E(errs.KindOf(err), op, err)
		}
		if x.onInsert != nil {
			x.onInsert()
		}
		for _, n := range nums {
			dirty[n] = struct{}{}
		}
		inserted++
	}
	if len(dirty) == 0 {
		return inserted, sz, nil
	}

	nums := make([]uint32, 0, len(dirty))
	for n := range dirty {
		nums = append(nums, n)
	}
	tx := txn.New(x.kv)
	defer tx.Close()
	x.stage(tx, t, r.g, nums)
	return inserted, sz, tx.Commit(ctx)
}

// markRebuilding raises the flag Stats and Health report for the duration of a
// rebuild.
func (x *Index) markRebuilding(t tenant.ID) {
	if r, ok := x.residentFor(t); ok {
		r.mu.Lock()
		r.rebuilding = true
		r.mu.Unlock()
	}
}

// finishRebuilding lowers that flag and sets what the finished rebuild leaves
// the tenant saying — empty when it repaired everything it found.
//
// The reason is a parameter rather than an unconditional clear because a
// rebuild does not repair everything: a canonical vector of another width is
// still of another width afterwards, and a tenant that silently lost a record
// must not report itself healthy merely because a repair ran over it.
func (x *Index) finishRebuilding(t tenant.ID, degraded string) {
	if r, ok := x.residentFor(t); ok {
		r.mu.Lock()
		r.rebuilding = false
		r.degraded = degraded
		r.mu.Unlock()
	}
}

// residentFor returns the tenant's resident entry if it is in memory. A tenant
// that is not is not an error for either caller: nothing is resident to mark,
// and the next materialisation decides the state afresh.
func (x *Index) residentFor(t tenant.ID) (*resident, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	r, ok := x.tenants[t]
	return r, ok
}
