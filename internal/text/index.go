package text

import (
	"context"
	"errors"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// Reader is the read surface a search needs. Both [storage.KV] and
// [storage.Snapshot] satisfy it, which is the point: a hybrid query reads its
// text step through the same pinned snapshot as its vector step, its graph step
// and its attribute filter, so all four describe one state.
//
// This is the correction Phase 6 made for graph.Traverse, for the same reason.
// A text step reading through the store would rank a page against a corpus the
// rest of the query cannot see.
type Reader interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	NewIterator(lower, upper []byte) storage.Iterator
}

// Index is the inverted index over a deployment's records.
//
// It is a concrete type rather than an interface, unlike vector.Index, and the
// reason is that there is one implementation. vector.Index is an interface
// because `flat` and `hnsw` both exist and `flat` is the definition of correct
// that `hnsw` is verified against; an interface here would be a contract with
// one implementor, a shared suite that duplicates this package's unit tests,
// and an abstraction chosen before there is a second thing to abstract over.
// Extracting one when a second implementation arrives is mechanical.
//
// It holds no state. Every method is given the tenant it works on and the
// transaction or reader it works through, which is what lets a rebuild run
// across tenants without forging a context per tenant.
type Index struct{}

// New returns the index.
func New() *Index { return &Index{} }

// Ensure the write path's hook and this implementation cannot drift: if
// record.Indexer gains a parameter, this fails to compile here rather than
// silently leaving the repository with no text index.
var _ record.Indexer = (*Index)(nil)

// Stage brings the postings for rid into line with rec, inside tx. A nil rec
// removes them.
//
// It satisfies [record.Indexer], which is a correction to the plan's
// Add(ctx, tx, t, id, text, tags): a write path that has to remember to call
// the text index is a write path that will forget, and internal/record already
// declares the hook that makes forgetting impossible. A record also already
// carries its content and its tags, so the plan's extra parameters were two
// copies of what is in rec.
func (ix *Index) Stage(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace,
	rid id.ID, rec *record.Record) error {
	if rec == nil {
		return ix.Remove(ctx, tx, t, ns, rid)
	}
	return ix.Add(ctx, tx, t, ns, rid, rec.Content, rec.Fields.Tags)
}

// Add indexes a record's content and tags, withdrawing whatever the previous
// version of that record contributed.
//
// The withdrawal is the whole of the update path and the expensive thing to get
// wrong. Writing the new postings without deleting the old ones leaves a record
// findable by every word it has ever contained — not detectably wrong, because
// the record exists, so nothing in the system ever notices.
func (ix *Index) Add(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace,
	rid id.ID, content string, tags []string) error {
	const op = "text.Index.Add"

	if err := checkScope(t, rid, op); err != nil {
		return err
	}

	terms := analyse(content, tags)
	old, err := ix.currentDoc(tx, t, ns, rid)
	if err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}

	next := docRow{DocLen: uint32(terms.length), Terms: make([]string, 0, len(terms.freq))}
	for term := range terms.freq {
		next.Terms = append(next.Terms, term)
	}
	ix.stagePostings(tx, t, ns, rid, old, terms)
	tx.Set(docKey(t, ns, rid), encodeDocRow(next))

	return ix.stageStats(ctx, tx, t, ns, statsDelta(old, &next), op)
}

// Remove withdraws every posting a record contributed.
func (ix *Index) Remove(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID) error {
	const op = "text.Index.Remove"

	if err := checkScope(t, rid, op); err != nil {
		return err
	}

	old, err := ix.currentDoc(tx, t, ns, rid)
	if err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}
	if old == nil {
		// Nothing indexed. Removing what is not there is not an error: the
		// post-condition already holds, and a delete of a memory written
		// before the text index existed must not fail.
		return nil
	}

	ix.stagePostings(tx, t, ns, rid, old, analysis{})
	tx.Delete(docKey(t, ns, rid))

	return ix.stageStats(ctx, tx, t, ns, statsDelta(old, nil), op)
}

// currentDoc reads the record's term row as the transaction sees it — its own
// staged writes first, then the store — and registers the expectation that
// makes the update conditional.
//
// A row that will not decode is not an error. It is derived data, and refusing
// the write would mean one damaged row makes the record it describes
// permanently unwritable; the stale postings survive instead, until a rebuild
// clears them. This is the same judgement attr.Indexer makes about a damaged
// attribute row, for the same reason.
func (ix *Index) currentDoc(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID) (*docRow, error) {
	key := docKey(t, ns, rid)
	value, err := tx.Get(key)
	if errs.Is(err, errs.NotFound) {
		tx.Expect(key, nil, false)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tx.Expect(key, value, true)
	row, err := decodeDocRow(value)
	if err != nil {
		return nil, nil
	}
	return &row, nil
}

// stagePostings reconciles the postings between two states of a record.
func (ix *Index) stagePostings(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID,
	old *docRow, next analysis) {
	if old != nil {
		for _, term := range old.Terms {
			if _, kept := next.freq[term]; !kept {
				tx.Delete(postingKey(t, ns, term, rid))
			}
		}
	}
	for term, freq := range next.freq {
		tx.Set(postingKey(t, ns, term, rid),
			encodePosting(posting{TermFreq: freq, DocLen: uint32(next.length)}))
	}
}

// stageStats moves the tenant's running totals by delta, conditionally.
//
// The expectation is what makes the count exact. Two writes into one tenant
// that commit at the same instant would otherwise both read the same total and
// both write the same successor, losing an increment — and a document count
// that drifts low is an inverse document frequency that is wrong for every
// query from then on, with nothing reporting it. The cost is that the second
// writer gets errs.Conflict, which the write path retries; see
// docs/architecture/text.md.
//
// storage.Batch ignores an expectation registered after the key is staged, so a
// batch writing twenty memories registers one expectation against the value the
// batch started from and accumulates the other nineteen deltas in its own
// staged row.
func (ix *Index) stageStats(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace,
	delta statsChange, op string) error {
	if delta.zero() {
		return nil
	}

	key := statsKey(t, ns)
	value, err := tx.Get(key)
	switch {
	case errs.Is(err, errs.NotFound):
		tx.Expect(key, nil, false)
		value = nil
	case err != nil:
		return errs.E(errs.KindOf(err), op, err)
	default:
		tx.Expect(key, value, true)
	}

	stats := Stats{}
	if value != nil {
		decoded, err := decodeStats(value)
		if err != nil {
			// A damaged statistics row is derived data and a rebuild's job. It
			// must not make the tenant unwritable, so the totals restart from
			// this write rather than the write failing. Scores are wrong until
			// the rebuild; memories are not lost.
			decoded = Stats{}
		}
		stats = decoded
	}

	stats.Documents = addDelta(stats.Documents, delta.documents)
	stats.TotalLength = addDelta(stats.TotalLength, delta.totalLength)
	tx.Set(key, encodeStats(stats))
	return nil
}

// statsChange is how one write moves the tenant's totals.
type statsChange struct {
	documents   int64
	totalLength int64
}

func (c statsChange) zero() bool { return c.documents == 0 && c.totalLength == 0 }

func statsDelta(old *docRow, next *docRow) statsChange {
	var c statsChange
	if old != nil {
		c.documents--
		c.totalLength -= int64(old.DocLen)
	}
	if next != nil {
		c.documents++
		c.totalLength += int64(next.DocLen)
	}
	return c
}

// addDelta applies a signed change to an unsigned total, with a floor at zero.
//
// The floor is a safety net, not a design: the totals are exact, so it should
// never fire. If it does, the corpus is being repaired from a damaged row, and
// clamping is better than the wrap-around that would put avgdl at 1.8e19 and
// every score at zero.
func addDelta(total uint64, delta int64) uint64 {
	if delta < 0 {
		drop := uint64(-delta)
		if drop > total {
			return 0
		}
		return total - drop
	}
	return total + uint64(delta)
}

// analysis is one record's terms with their frequencies, and its length.
type analysis struct {
	freq   map[string]uint32
	length int
}

// analyse turns content and tags into the terms indexed for a record.
//
// Length counts tags as well as content words. A tag is a term that a memory
// carries, and excluding it would make a heavily tagged memory look shorter
// than it is to the length normalisation — which is the one place BM25 lets a
// document buy rank by omitting information.
func analyse(content string, tags []string) analysis {
	out := analysis{freq: map[string]uint32{}}
	for _, term := range Tokenize(content) {
		out.freq[term]++
		out.length++
	}
	for _, tag := range tags {
		term := TagTerm(tag)
		if term == tagPrefix {
			continue
		}
		out.freq[term]++
		out.length++
	}
	return out
}

// RecordTerms is every term one record contributes, sorted.
//
// It reads the record's own term row rather than scanning the postings, which
// is what makes "what does this memory index as" answerable in one read — by a
// rebuild comparing itself with the incremental index, and by an operator
// asking why a memory is not being found.
func RecordTerms(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace, rid id.ID) ([]string, error) {
	const op = "text.RecordTerms"

	value, err := r.Get(ctx, docKey(t, ns, rid))
	if errs.Is(err, errs.NotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errs.E(errs.KindOf(err), op, err)
	}
	row, err := decodeDocRow(value)
	if err != nil {
		return nil, errs.E(errs.KindOf(err), op, err)
	}
	return row.Terms, nil
}

// ReadStats returns a tenant's corpus statistics, or the zero value when the
// tenant has never been written to.
func ReadStats(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace) (Stats, error) {
	const op = "text.ReadStats"

	value, err := r.Get(ctx, statsKey(t, ns))
	if errs.Is(err, errs.NotFound) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, errs.E(errs.KindOf(err), op, err)
	}
	stats, err := decodeStats(value)
	if err != nil {
		return Stats{}, errs.E(errs.KindOf(err), op, err)
	}
	return stats, nil
}

// checkScope refuses a row that belongs to nobody. Invariant 1 made
// structural: there is no way to spell a posting with no tenant.
func checkScope(t tenant.ID, rid id.ID, op string) error {
	switch {
	case t == "":
		return errs.E(errs.Invalid, op, errNoTenant)
	case rid.IsZero():
		return errs.E(errs.Invalid, op, errNoRecord)
	}
	return nil
}

var (
	errNoTenant = errors.New("a posting requires a tenant: there is no unscoped write path (Invariant 1)")
	errNoRecord = errors.New("a posting requires a record")
)

// HasTerms reports whether a record carries every one of terms.
//
// It is the filter form of the index, and it is a point read per term rather
// than a scan: the posting key is built from the term and the record, so
// "does this memory carry this tag" is a lookup of a key holding no value at
// all. That is what lets a tag filter narrow a step the inverted index did not
// produce — see query.Query.tagTerms.
func (ix *Index) HasTerms(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace,
	rid id.ID, terms []string) (bool, error) {
	const op = "text.Index.HasTerms"

	for _, term := range terms {
		_, err := r.Get(ctx, postingKey(t, ns, term, rid))
		if errs.Is(err, errs.NotFound) {
			return false, nil
		}
		if err != nil {
			return false, errs.E(errs.KindOf(err), op, err)
		}
	}
	return true, nil
}
