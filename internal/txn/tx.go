// Package txn is Remem's transaction: one atomic unit of work over a
// storage.Batch.
//
// Spec §12 requires a record and its indexes to be written atomically. Writing
// a memory is therefore one transaction containing the record body, the
// canonical vector, the attribute row and its slot index entries, the text
// postings, the out-edges and in-edges, and any job rows the write enqueues.
// A reader never observes half of it.
//
// The HNSW insert is deliberately outside it (plan §II.4): the graph is the one
// asynchronously maintained index, so an HNSW failure never fails a write.
//
// # Why this exists at all, given storage.Batch
//
// Three reasons, and none of them is abstraction for its own sake. A Batch has
// no error on Set, so a caller cannot tell a staged write from a rejected one;
// a Batch can be committed twice, which for a caller that retries means
// re-applying stale writes over fresh ones; and durability on a Batch is a
// per-Commit argument, which makes it a decision every call site takes rather
// than one an operator configures. Tx closes all three.
package txn

import (
	"context"
	"errors"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
)

// Tx is one atomic unit of work.
//
// It is owned by the goroutine that created it and is not safe for concurrent
// use. Close must be called; Commit does not release it.
type Tx interface {
	Expect(key, value []byte, exists bool)
	// OnCommit registers a callback run once, after a successful commit, and
	// reports whether the registration took.
	//
	// A transaction that has already committed or been closed accepts none, and
	// used to drop them silently. A caller whose callback *releases* something
	// the staged write reserved cannot afford that — the reservation would be
	// held for the life of the process — so the answer is reported and such a
	// caller undoes its reservation itself. See attr.Indexer.Stage.
	OnCommit(func()) bool
	// OnClose registers a callback run once, when the transaction is closed
	// without having committed, and reports whether the registration took.
	//
	// It is the other half of OnCommit for anything a staged write reserves
	// outside the batch: the commit callback publishes it, this one gives it
	// back. Commit and Close are exclusive, so exactly one of the two runs for
	// a transaction that is closed — which Close always is, being deferred.
	OnClose(func()) bool

	// Get reads through the transaction: its own staged writes first, then the
	// store. A key staged for deletion reads as errs.NotFound.
	Get(key []byte) ([]byte, error)
	// Set stages a write. The slices are copied.
	Set(key, value []byte)
	// Delete stages a deletion.
	Delete(key []byte)
	// Len reports how many operations are staged.
	Len() int
	// Commit applies every staged operation atomically. It may be called once.
	Commit(ctx context.Context) error
	// Close discards the transaction if it has not committed. It is idempotent
	// and safe to defer immediately after New.
	Close()
}

// Option configures a transaction.
type Option func(*tx)

// Sync sets whether the commit is durable before Commit returns. It comes from
// storage.sync_writes, so that durability is one configured decision rather
// than a choice re-taken at every write site.
func Sync(sync bool) Option {
	return func(t *tx) { t.sync = sync }
}

// New opens a transaction over kv. Durability defaults to on: a store that
// loses the last writes after reporting them written is a worse default than a
// slow one, and turning it off is a decision an operator makes explicitly.
func New(kv storage.KV, opts ...Option) Tx {
	t := &tx{batch: kv.NewBatch(), sync: true}
	for _, o := range opts {
		o(t)
	}
	return t
}

type tx struct {
	onCommit  []func()
	onClose   []func()
	batch     storage.Batch
	sync      bool
	committed bool
	closed    bool
}

var (
	errClosed    = errors.New("this transaction has been closed")
	errCommitted = errors.New("this transaction has already committed; open a new one rather than replaying it")
)

func (t *tx) Get(key []byte) ([]byte, error) {
	if err := t.usable("txn.Get"); err != nil {
		return nil, err
	}
	return t.batch.Get(key)
}

// Set and Delete stage silently on a spent transaction rather than panicking.
// A caller that has already had Commit or Get fail is unwinding, and a panic
// during that unwinding replaces a clear error with a stack trace.
func (t *tx) Set(key, value []byte) {
	if t.spent() {
		return
	}
	t.batch.Set(key, value)
}

func (t *tx) Delete(key []byte) {
	if t.spent() {
		return
	}
	t.batch.Delete(key)
}

func (t *tx) Len() int {
	if t.closed {
		return 0
	}
	return t.batch.Len()
}

func (t *tx) Commit(ctx context.Context) error {
	const op = "txn.Commit"
	if err := t.usable(op); err != nil {
		return err
	}
	if err := t.batch.Commit(ctx, t.sync); err != nil {
		return err
	}
	t.committed = true
	// The close callbacks are the undo of the commit callbacks, so a commit
	// retires them before running theirs: the deferred Close that follows every
	// transaction must not then undo what this commit just published.
	t.onClose = nil
	for _, f := range t.onCommit {
		f()
	}
	return nil
}

func (t *tx) Close() {
	if t.closed {
		return
	}
	t.closed = true
	_ = t.batch.Close()
	for _, f := range t.onClose {
		f()
	}
	t.onClose = nil
}

func (t *tx) spent() bool { return t.closed || t.committed }

func (t *tx) usable(op string) error {
	switch {
	case t.closed:
		return errs.E(errs.Invalid, op, errClosed)
	case t.committed:
		return errs.E(errs.Invalid, op, errCommitted)
	}
	return nil
}

func (t *tx) Expect(key, value []byte, exists bool) {
	if !t.spent() {
		t.batch.Expect(key, value, exists)
	}
}
func (t *tx) OnCommit(f func()) bool {
	if t.spent() {
		return false
	}
	t.onCommit = append(t.onCommit, f)
	return true
}

func (t *tx) OnClose(f func()) bool {
	if t.spent() {
		return false
	}
	t.onClose = append(t.onClose, f)
	return true
}
