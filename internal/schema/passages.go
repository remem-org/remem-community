package schema

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// PassageReembedID is the re-embedding step's stable id.
const PassageReembedID = "0003-reembed-passages"

// VectorReindexer queues a rebuild of one tenant's approximate vector index
// inside tx. The composition root implements it by staging a vector.rebuild job;
// it is an interface for the reason [TextReindexer] is.
type VectorReindexer interface {
	StageVectorRebuild(ctx context.Context, tx txn.Tx, t tenant.ID) error
}

// BuiltinOption gives [Builtin] what a step needs from the composition root.
type BuiltinOption func(*builtinDeps)

type builtinDeps struct {
	passagesSet bool
	embedder    embedding.Embedder
	vectors     VectorReindexer
}

// WithPassages supplies the re-embedding step's embedder, nil on a build
// without the model, and the way it queues a vector index rebuild.
func WithPassages(e embedding.Embedder, vectors VectorReindexer) BuiltinOption {
	return func(d *builtinDeps) { d.passagesSet, d.embedder, d.vectors = true, e, vectors }
}

// reembedPage is how many memories one checkpointed transaction re-embeds.
// Small, because the embedding is the cost: a page of long memories is minutes
// of model time, and it is the work an interrupted run repeats.
const reembedPage = 16

// passageReembed re-embeds every memory under the passage model and writes its
// passages, and advances passage_index from absent to 1
// (openspec/changes/passage-vectors, design decision 9).
//
// # Why it blocks start-up
//
// A directory written before passages exists holds 384-wide vectors of
// all-MiniLM-L6-v2, each of a memory's first 128 tokens. This build compares
// 1,024-wide vectors of another model. The two are not one space, and a server
// that answered from a mix of them would rank memories by numbers that mean
// nothing. So nothing is served until every memory has been re-embedded, which
// is what StrategyRebuild means. A background backfill was rejected for the
// reason the attribute backfill gives: a half-built access path returns fewer
// answers rather than older ones, and nothing can say so honestly.
//
// It rewrites canonical vectors, so it declares RewritesData and the runner
// takes the pre-migration backup first. The way back is the old binary on that
// checkpoint, and the old binary refuses this directory by name, because it
// does not know passage_index.
//
// # What it writes
//
// Per memory, in one transaction per page: the canonical vector — the document
// vector, stamped with this model — and the passage rows, with the cursor. The
// record body is not touched. When a tenant is finished, its HNSW node records
// are deleted and a vector.rebuild is queued: their neighbour lists were built
// in the old space, and materialised over the new vectors they would be a graph
// of the wrong shape that nothing reports as damaged. With them gone, the next
// materialisation builds the graph from the entries, and the job does that work
// before a user's search would have to.
//
// An external-mode server has no model to derive anything from: its vectors
// came from its callers, and they are what they were. The step advances it
// with no work.
func passageReembed(d builtinDeps) Migration {
	return Migration{
		ID:           PassageReembedID,
		Requires:     map[string]uint32{"passage_index": 0},
		Advances:     map[string]uint32{"passage_index": 1},
		RewritesData: true,
		Strategy:     StrategyRebuild,
		Run: func(ctx context.Context, mc *Context) error {
			return runPassageReembed(ctx, mc, d)
		},
	}
}

func runPassageReembed(ctx context.Context, mc *Context, d builtinDeps) error {
	const op = "schema.passageReembed"

	if d.embedder == nil {
		return errs.E(errs.Unavailable, op, errors.New(
			"this data directory was written under an earlier embedding model, and every memory must be "+
				"re-embedded with "+embedding.Model+" before it can be served. This binary has no model: "+
				"build it with -tags onnx (CGO_ENABLED=1) after running scripts/fetch-model.sh, and set "+
				"embedding.model_path"))
	}
	if embedding.CallerSupplied(d.embedder) {
		return nil
	}
	if d.vectors == nil {
		return errs.E(errs.Invalid, op, errors.New(
			"the re-embedding step has no way to queue a vector index rebuild; this build changed the "+
				"vector space and registered nothing to rebuild the index in it"))
	}
	if mc.Tenants == nil {
		return errs.E(errs.Invalid, op, errors.New(
			"the re-embedding step needs the tenant directory: vectors are per tenant"))
	}

	cur, err := decodeReembedCursor(mc.Resume)
	if err != nil {
		return err
	}
	total, err := countRecords(ctx, mc)
	if err != nil {
		return err
	}
	mc.Total = total

	repo := record.NewRepo(mc.KV)
	store := vector.NewStore(mc.KV)
	modelID := d.embedder.ModelID()
	done := mc.Done
	reached := cur.tenant == ""

	return mc.Tenants.ForEach(ctx, func(t tenant.ID) error {
		after := (*id.ID)(nil)
		if !reached {
			if t != cur.tenant {
				return nil
			}
			reached = true
			if cur.finished {
				return nil // its index was cleared and its rebuild queued
			}
			after = cur.after
		}
		tctx := tenant.NewContext(ctx, t)

		for {
			snap := mc.KV.NewSnapshot()
			recs, err := repo.Scan(tctx, snap, after, reembedPage)
			_ = snap.Close()
			if err != nil {
				return errs.E(errs.KindOf(err), op, err)
			}
			if len(recs) == 0 {
				return finishTenant(tctx, mc, d.vectors, t, after, done)
			}

			// Outside the transaction: an embedding is the one thing a commit
			// retry must not redo, and it is slow.
			texts := make([]string, len(recs))
			for i, rec := range recs {
				texts[i] = rec.Content
			}
			docs, err := d.embedder.EmbedDocuments(tctx, texts)
			if err != nil {
				return errs.E(errs.KindOf(err), op, fmt.Errorf("re-embedding tenant %s: %w", t, err))
			}
			if len(docs) != len(recs) {
				return errs.E(errs.Storage, op, fmt.Errorf("the embedder returned %d documents for %d memories", len(docs), len(recs)))
			}

			tx := txn.New(mc.KV)
			for i, rec := range recs {
				v := docs[i].Vector
				if err := store.Stage(tx, t, rec.Namespace, rec.ID, &vector.Vector{ModelID: modelID, Dim: len(v), Values: v}); err != nil {
					tx.Close()
					return err
				}
				if err := vector.StagePassages(tx, t, rec.Namespace, rec.ID, record.PassageSetOf(modelID, docs[i]).Rows); err != nil {
					tx.Close()
					return err
				}
			}
			last := recs[len(recs)-1].ID
			done += uint64(len(recs))
			// The vectors and the cursor commit together, so a resumed run
			// re-embeds no memory twice and skips none.
			if err := mc.Checkpoint(ctx, tx, encodeReembedCursor(reembedCursor{tenant: t, after: &last}), done); err != nil {
				tx.Close()
				return err
			}
			tx.Close()
			after = &last
			if len(recs) < reembedPage {
				return finishTenant(tctx, mc, d.vectors, t, after, done)
			}
		}
	})
}

// finishTenant deletes the tenant's node records, queues its vector rebuild and
// records the tenant finished, in one transaction.
func finishTenant(ctx context.Context, mc *Context, vectors VectorReindexer, t tenant.ID, last *id.ID, done uint64) error {
	const op = "schema.passageReembed"
	tx := txn.New(mc.KV)
	defer tx.Close()
	lo, hi := keys.SpaceRange(t, tenant.DefaultNamespace, keys.SpaceVectorIndex)
	it := mc.KV.NewIterator(lo, hi)
	for ok := it.First(); ok; ok = it.Next() {
		tx.Delete(append([]byte(nil), it.Key()...))
	}
	err := it.Error()
	_ = it.Close()
	if err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}
	if err := vectors.StageVectorRebuild(ctx, tx, t); err != nil {
		return errs.E(errs.KindOf(err), op, fmt.Errorf("queuing tenant %s's vector rebuild: %w", t, err))
	}
	return mc.Checkpoint(ctx, tx, encodeReembedCursor(reembedCursor{tenant: t, after: last, finished: true}), done)
}

// reembedCursor is where the step got to: the tenant, the last memory
// re-embedded in it, and whether the tenant's index has been dealt with.
type reembedCursor struct {
	tenant   tenant.ID
	after    *id.ID
	finished bool
}

// encodeReembedCursor is <tenant length><tenant><flags><record id, when set>.
// Not a storage key, for the reason encodeBackfillCursor gives.
func encodeReembedCursor(c reembedCursor) []byte {
	out := binary.AppendUvarint(nil, uint64(len(c.tenant)))
	out = append(out, c.tenant...)
	var flags byte
	if c.finished {
		flags |= 1
	}
	if c.after != nil {
		flags |= 2
	}
	out = append(out, flags)
	if c.after != nil {
		out = append(out, c.after[:]...)
	}
	return out
}

func decodeReembedCursor(b []byte) (reembedCursor, error) {
	const op = "schema.decodeReembedCursor"
	if len(b) == 0 {
		return reembedCursor{}, nil
	}
	bad := errs.E(errs.Corruption, op, errors.New("the re-embedding step's saved position will not parse; it cannot be resumed from"))
	n, w := binary.Uvarint(b)
	if w <= 0 || uint64(len(b[w:])) < n+1 {
		return reembedCursor{}, bad
	}
	c := reembedCursor{tenant: tenant.ID(b[w : w+int(n)])}
	rest := b[w+int(n):]
	flags := rest[0]
	rest = rest[1:]
	c.finished = flags&1 != 0
	if flags&2 != 0 {
		rid, err := id.FromBytes(rest)
		if err != nil {
			return reembedCursor{}, bad
		}
		c.after = &rid
	} else if len(rest) != 0 {
		return reembedCursor{}, bad
	}
	return c, nil
}
