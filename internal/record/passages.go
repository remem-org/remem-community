package record

import (
	"bytes"
	"context"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// PassageSet is a memory's passage vectors, as a write that changed its content
// computed them. It travels on [Record.Passages] and is never encoded into the
// body: passages are derived rows, rebuilt from content, model and passage
// format, and a body carrying them would be a second copy that can disagree.
//
// An empty set is meaningful: a memory whose content fits one passage has no
// passage rows, because its canonical vector is its one passage.
type PassageSet struct {
	Rows []vector.PassageRow
}

// Clone returns a deep copy.
func (p *PassageSet) Clone() *PassageSet {
	if p == nil {
		return nil
	}
	out := &PassageSet{Rows: make([]vector.PassageRow, len(p.Rows))}
	for i, r := range p.Rows {
		out.Rows[i] = vector.PassageRow{Start: r.Start, End: r.End, Vector: r.Vector.Clone()}
	}
	return out
}

// PassageSetOf is what a content write stages for the passage indexer: the
// document's passages stamped with the model that produced them, or an empty
// set for content that fits one passage. Never nil, because a write that sets
// content must say what its passages are; nil means "content unchanged".
//
// It is the one place an embedded document becomes passage rows, shared by the
// write path and the rebuild so the two cannot disagree about a row.
func PassageSetOf(modelID string, d embedding.Document) *PassageSet {
	set := &PassageSet{Rows: make([]vector.PassageRow, len(d.Passages))}
	for i, p := range d.Passages {
		set.Rows[i] = vector.PassageRow{Start: p.Start, End: p.End,
			Vector: &vector.Vector{ModelID: modelID, Dim: len(p.Vector), Values: p.Vector}}
	}
	return set
}

// passageIndexer maintains a memory's passage rows in the record's own
// transaction, so a memory's passages commit with its content and cannot
// describe content it no longer has.
//
// A memory's rows are always the whole set, numbered 0…n-1 with no gaps: a
// write replaces them all at once. That is what lets a transaction with no
// range delete remove the old ones, by deleting from the first number the new
// set does not use until the first that is not stored. Two writes racing to
// replace one memory's set cannot interleave here, because both condition
// their commit on the record body they read.
type passageIndexer struct{}

// NewPassageIndexer returns the derived index of passage rows.
func NewPassageIndexer() Indexer { return passageIndexer{} }

// Stage reads rec.Passages: nil leaves the stored rows alone, a set replaces
// them, and a removed record takes them with it.
func (passageIndexer) Stage(_ context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID, rec *Record) error {
	if rec == nil {
		return vector.StageDeletePassages(tx, t, ns, rid)
	}
	if rec.Passages == nil {
		return nil
	}
	return stagePassages(tx, t, ns, rid, rec.Passages)
}

// stagePassages replaces rid's passage rows with set, through the vector
// package, which owns the passage key space as it owns the canonical one.
func stagePassages(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID, set *PassageSet) error {
	return vector.StagePassages(tx, t, ns, rid, set.Rows)
}

// passageRebuildPage is how many records one rebuild step embeds and commits.
// Small, because embedding is the cost: a page of long memories is minutes of
// model time, and the checkpoint after it is what an interrupted rebuild
// resumes from.
const passageRebuildPage = 16

// RebuildPassages recomputes every record's passage rows from its content, and
// reports how many records it restaged.
//
// It is the rebuild path Invariant 3 requires of the passage space, and unlike
// the other rebuilds it runs the model: passage rows are a function of the
// content and the model, so recomputing them is embedding the corpus again.
// External mode has no passages, so it embeds nothing and stages an empty set
// for every record, which removes any rows that should not be there.
//
// resume is a checkpoint a previous run saved, or nil to start at the
// beginning; save is called with the next checkpoint after each committed
// page, so an interrupted rebuild continues rather than starting again.
//
// The embedding runs outside the transaction, so content can change between
// reading a record and staging its rows. A record whose body is no longer the
// one that was embedded is skipped: the write that changed it staged its own
// passages in its own transaction, and overwriting them with the old content's
// would be a stale index the rebuild created. The body is re-read inside the
// transaction body, so a retry converges rather than re-issuing a stale
// expectation (see txn.Do).
func RebuildPassages(ctx context.Context, kv storage.KV, t tenant.ID, ns tenant.Namespace,
	e embedding.Embedder, resume []byte, save func(checkpoint []byte) error) (int, error) {
	const op = "record.RebuildPassages"

	repo := NewRepo(kv)
	tctx := tenant.NewContext(ctx, t)
	var from *id.ID
	if len(resume) > 0 {
		rid, err := id.FromBytes(resume)
		if err != nil {
			return 0, errs.E(errs.Invalid, op, err)
		}
		from = &rid
	}

	written := 0
	for {
		// A snapshot per page, not one for the whole run: a rebuild of a large
		// corpus is hours of embedding, and a snapshot held that long stops the
		// store reclaiming everything written meanwhile.
		snap := kv.NewSnapshot()
		page, err := repo.Scan(tctx, snap, from, passageRebuildPage)
		_ = snap.Close()
		if err != nil {
			return written, err
		}
		if len(page) == 0 {
			return written, clearPassagesPending(tctx, kv, t)
		}

		sets := make([]*PassageSet, len(page))
		if embedding.CallerSupplied(e) {
			for i := range sets {
				sets[i] = &PassageSet{}
			}
		} else {
			texts := make([]string, len(page))
			for i, rec := range page {
				texts[i] = rec.Content
			}
			docs, err := e.EmbedDocuments(tctx, texts)
			if err != nil {
				return written, err
			}
			for i, d := range docs {
				sets[i] = PassageSetOf(e.ModelID(), d)
			}
		}

		staged := 0
		if err := txn.Do(tctx, kv, func(tx txn.Tx) error {
			staged = 0
			for i, rec := range page {
				key := BodyKey(t, ns, rec.ID)
				cur, err := tx.Get(key)
				if errs.Is(err, errs.NotFound) {
					continue // deleted meanwhile, and its rows with it
				}
				if err != nil {
					return err
				}
				if !bytes.Equal(cur, rec.originalBody) {
					continue // rewritten meanwhile, with its own passages
				}
				tx.Expect(key, cur, true)
				if err := stagePassages(tx, t, ns, rec.ID, sets[i]); err != nil {
					return err
				}
				staged++
			}
			return nil
		}); err != nil {
			return written, err
		}
		written += staged

		last := page[len(page)-1].ID
		from = &last
		if save != nil {
			if err := save(append([]byte(nil), last[:]...)); err != nil {
				return written, err
			}
		}
		if len(page) < passageRebuildPage {
			return written, clearPassagesPending(tctx, kv, t)
		}
	}
}

// clearPassagesPending removes t's pending marker once a rebuild has computed
// every one of its memories' passages.
func clearPassagesPending(ctx context.Context, kv storage.KV, t tenant.ID) error {
	return kv.Delete(ctx, passagesPendingKey(t))
}

// passagesPendingPrefix names the system rows that mark a tenant whose passages
// an import could not compute. They are system rows, as the tenant directory
// and migration state are, because what they say is about the directory's
// readiness to serve rather than about any memory.
const passagesPendingPrefix = "passages_pending/"

func passagesPendingKey(t tenant.ID) []byte { return keys.System(passagesPendingPrefix + string(t)) }

// MarkPassagesPending records in tx that t has memories whose passage rows were
// not computed: an import of a snapshot of this model, run without the model.
// The server computes them before it serves (see [RebuildPassages]).
func MarkPassagesPending(tx txn.Tx, t tenant.ID) { tx.Set(passagesPendingKey(t), []byte{1}) }

// PassagesPending lists the tenants marked by [MarkPassagesPending], in order.
func PassagesPending(ctx context.Context, r interface {
	NewIterator(lower, upper []byte) storage.Iterator
}) ([]tenant.ID, error) {
	prefix := keys.System(passagesPendingPrefix)
	lo, hi := keys.PrefixRange(prefix)
	it := r.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	var out []tenant.ID
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, tenant.ID(it.Key()[len(prefix):]))
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return out, it.Error()
}
