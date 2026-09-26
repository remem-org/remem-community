package record

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// Repo reads and writes canonical records.
//
// Every method takes a context from which the tenant is already resolved.
// There is no unscoped variant of any read (Invariant 1), and there is no
// method that takes a tenant as a parameter either — a parameter is something
// a caller can pass wrongly, where a context is something the request boundary
// sets once.
type Repo interface {
	// Get returns a record with its canonical vectors, or errs.NotFound.
	Get(ctx context.Context, rid id.ID) (*Record, error)

	// GetFrom is Get as of a pinned snapshot, so that a page of results and
	// the walk that chose it read one state.
	//
	// It does not lazily upgrade. An upgrade is a write, and a read through a
	// snapshot is a read of the past: persisting one derived from a stale view
	// would write back over whatever has happened since.
	GetFrom(ctx context.Context, snap storage.Snapshot, rid id.ID) (*Record, error)

	// GetBodyFrom is GetFrom without the canonical vectors, for a caller that
	// returns a record to a user. A result never carries an embedding, so
	// reading one is a point read per result for nothing. from is read like a
	// snapshot; a storage.SeekReader over one answers it faster.
	GetBodyFrom(ctx context.Context, from BodyReader, rid id.ID) (*Record, error)

	// Put stages a record and its vectors into tx. Nothing is visible until
	// tx commits (spec §12).
	Put(ctx context.Context, tx txn.Tx, r *Record) error

	// Delete stages the removal of a record and its vectors.
	Delete(ctx context.Context, tx txn.Tx, rid id.ID) error

	// Scan returns up to limit records in id order, starting after from.
	//
	// It reads from snap rather than from the store, so every page of one
	// listing sees the same state: a record written between page one and page
	// ten can neither be skipped nor returned twice (plan §II.9).
	Scan(ctx context.Context, snap storage.Snapshot, from *id.ID, limit int) ([]*Record, error)
}

// Upgrader brings a record decoded from an older user schema up to date
// (spec §20.2, the lazy migration strategy).
//
// It is declared here rather than in internal/schema so the domain does not
// depend on the migration machinery: a record knows it may be behind, and it
// does not need to know who decides what to do about it.
type Upgrader interface {
	// Upgrade rewrites rec in place, reporting the version it now carries and
	// whether anything changed. A record already current reports changed=false
	// and must not be touched.
	Upgrade(ctx context.Context, rec *Record, from uint32) (to uint32, changed bool, err error)
}

// Scheduler decides when a record next needs a lifecycle sweep's attention.
//
// It is declared here and implemented in internal/lifecycle, the way round that
// keeps the dependency pointing inward: a record knows it has a schedule, and
// it does not need to know what a retention policy is.
//
// [Repo.Put] calls it on every write, and that is the whole point. A write path
// that computed the schedule itself is a write path that can omit it, and a
// record with no schedule is a memory that never decays, never expires and
// appears in no sweep. The same argument [WithIndexer] makes one level down: a
// call site that can be forgotten eventually is.
//
// It returns a time rather than an error. A policy table that cannot be read
// must not make storing a memory fail — the sweep re-reads the table on every
// run and corrects whatever a degraded read got wrong.
type Scheduler interface {
	NextAttention(ctx context.Context, t tenant.ID, rec *Record) time.Time
}

// Indexer maintains the derived rows over a record, inside the caller's
// transaction.
//
// It is declared here and implemented in internal/attr, which is the way round
// that keeps the dependency pointing inward: internal/attr knows what a record
// is, because a slot table is a projection of one, and internal/record must not
// know what a slot is. The alternative — the write path calling the indexer
// itself — is a call site that can be forgotten, and a forgotten one leaves a
// record that exists and cannot be listed.
//
// Every implementation stages into tx and never commits: the record body, its
// vector and every derived row land together or not at all (spec §12).
type Indexer interface {
	// Stage brings the derived rows for rid into line with rec. A nil rec means
	// the record is being removed, and the rows go with it.
	//
	// It is given the record's scope explicitly rather than reading it from the
	// context because a rebuild legitimately runs across tenants, exactly as
	// vector.Store.Stage is.
	Stage(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID, rec *Record) error
}

// RepoOption configures a repository.
type RepoOption func(*repo)

// WithUpgrader turns on lazy schema upgrading in [Repo.Get]. Without it a
// record is returned exactly as it was stored, which is what every path that
// has no migration registered wants — and it costs nothing.
func WithUpgrader(u Upgrader) RepoOption { return func(r *repo) { r.upgrader = u } }

// WithScheduler makes [Repo.Put] compute each record's next-attention time.
//
// Without one, Put stores whatever the caller left in the field — which for
// every path in a build with no lifecycle is the zero value, read as "due now".
// That is the safe direction: a record with no schedule is visited immediately
// rather than never.
func WithScheduler(s Scheduler) RepoOption { return func(r *repo) { r.scheduler = s } }

// WithIndexer turns on derived-index maintenance in [Repo.Put] and
// [Repo.Delete]. A repository without one writes canonical rows only, which is
// what a rebuild that is itself producing the indexes wants.
//
// It accumulates rather than replaces: there is more than one derived index
// over a record — the attribute row and its slot indexes, and the text
// postings — and each is maintained by the package that owns its key space.
// Every one of them stages into the same transaction as the record body, so a
// memory and everything that makes it findable land together or not at all
// (spec §12). A second call replacing the first would silently disable an
// index, and the symptom is a memory that exists and cannot be found.
func WithIndexer(ix Indexer) RepoOption {
	return func(r *repo) { r.indexers = append(r.indexers, ix) }
}

// NewRepo returns the repository over kv.
func NewRepo(kv storage.KV, opts ...RepoOption) Repo {
	r := &repo{kv: kv, vectors: vector.NewStore(kv)}
	for _, o := range opts {
		o(r)
	}
	return r
}

type repo struct {
	kv        storage.KV
	vectors   *vector.Store
	upgrader  Upgrader
	indexers  []Indexer
	scheduler Scheduler
}

func (r *repo) Get(ctx context.Context, rid id.ID) (*Record, error) {
	const op = "record.Get"

	t, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	ns := tenant.DefaultNamespace

	body, err := r.kv.Get(ctx, BodyKey(t, ns, rid))
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			// A record in another tenant is reported as absent, never as
			// forbidden: "you may not see this" confirms it exists, which is
			// itself a cross-tenant disclosure.
			return nil, errs.E(errs.NotFound, op, fmt.Errorf("no record %s", rid))
		}
		return nil, err
	}

	rec, err := decodeBody(t, ns, rid, body, op)
	if err != nil {
		return nil, err
	}

	vec, err := r.kv.Get(ctx, VectorKey(t, ns, rid))
	switch {
	case err == nil:
		v, err := decodeVector(rid, vec, op)
		if err != nil {
			return nil, err
		}
		rec.Vectors[VectorContent] = v
	case errs.Is(err, errs.NotFound):
		// A record without a vector is legal: it is what an import that
		// carried no embeddings, or a write whose embedder failed, leaves
		// behind. Search will not find it; Get must still return it.
	default:
		return nil, err
	}

	if err := r.upgrade(ctx, t, ns, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// upgrade applies the lazy migration hook (spec §20.2): a record behind the
// current user schema is brought up to date, persisted, and returned.
//
// The upgrade itself is not optional — returning a record in a shape the caller
// cannot interpret would defeat the point. The *write-back* is: it happens in
// its own transaction, and a failure is logged rather than propagated.
//
// That asymmetry is the decision worth stating. A read that fails because a
// cache-fill failed is a read that fails on a read-only store, which is exactly
// how `remem-admin inspect` opens a directory, and during shutdown, and while a
// disk is full. The upgraded record is correct either way; the only thing lost
// is having to do it again next time.
//
// It is also a non-deterministic side effect on a read path, which Invariant 9
// forbids inside a replicated apply path. Nothing replicates in Phase 4; Phase
// 14 must move the write-back off the apply path, and
// docs/architecture/schema-and-migrations.md records that debt.
//
// Only Get upgrades. Scan does not: a listing that rewrote every record it
// walked would turn a page of results into a page of writes, and a background
// migration is the right tool for a whole corpus.
func (r *repo) upgrade(ctx context.Context, t tenant.ID, ns tenant.Namespace, rec *Record) error {
	if r.upgrader == nil {
		return nil
	}
	to, changed, err := r.upgrader.Upgrade(ctx, rec, rec.SchemaVersion)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	rec.SchemaVersion = to

	// Best effort write-back uses the same atomic index maintenance as Put.
	tx := txn.New(r.kv, txn.Sync(false))
	defer tx.Close()
	err = r.Put(ctx, tx, rec)
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		obs.Logger(ctx).Warn("a record was upgraded on read and the upgrade could not be persisted", "record", rec.ID.String(), "schema_version", to, "error", err)
	}

	return nil
}

func (r *repo) GetFrom(ctx context.Context, snap storage.Snapshot, rid id.ID) (*Record, error) {
	const op = "record.GetFrom"

	t, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	ns := tenant.DefaultNamespace

	rec, err := readBody(ctx, snap, t, ns, rid, op)
	if err != nil {
		return nil, err
	}

	vec, err := snap.Get(ctx, VectorKey(t, ns, rid))
	switch {
	case err == nil:
		v, err := decodeVector(rid, vec, op)
		if err != nil {
			return nil, err
		}
		rec.Vectors[VectorContent] = v
	case errs.Is(err, errs.NotFound):
	default:
		return nil, err
	}
	return rec, nil
}

// BodyReader is what a record body is read from: a snapshot, or a reader over
// one.
type BodyReader interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
}

func (r *repo) GetBodyFrom(ctx context.Context, from BodyReader, rid id.ID) (*Record, error) {
	const op = "record.GetBodyFrom"

	t, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	return readBody(ctx, from, t, tenant.DefaultNamespace, rid, op)
}

// readBody reads and decodes a record body, reporting an absent one as
// errs.NotFound naming the record.
func readBody(ctx context.Context, from BodyReader, t tenant.ID, ns tenant.Namespace, rid id.ID, op string) (*Record, error) {
	body, err := from.Get(ctx, BodyKey(t, ns, rid))
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return nil, errs.E(errs.NotFound, op, fmt.Errorf("no record %s", rid))
		}
		return nil, err
	}
	return decodeBody(t, ns, rid, body, op)
}

func (r *repo) Put(ctx context.Context, tx txn.Tx, rec *Record) error {
	const op = "record.Put"

	t, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	if err := validate(t, rec, op); err != nil {
		return err
	}
	ns := rec.namespace()

	// The schedule is computed here, before the body is encoded, so the record
	// body and the attribute row projected from it carry the same instant. It
	// mutates the caller's record for the reason the commit callback below
	// does: after a Put, the caller holds what was stored.
	if r.scheduler != nil {
		rec.Fields.NextAttentionAt = r.scheduler.NextAttention(ctx, t, rec)
	}

	body, err := encodeBody(rec)
	if err != nil {
		return errs.E(errs.Invalid, op, err)
	}
	key := BodyKey(t, ns, rec.ID)
	if rec.originalBody != nil {
		tx.Expect(key, rec.originalBody, true)
	} else {
		old, err := tx.Get(key)
		if err != nil && !errs.Is(err, errs.NotFound) {
			return err
		}
		tx.Expect(key, old, err == nil)
	}
	tx.Set(key, body)
	tx.OnCommit(func() { rec.originalBody = bytes.Clone(body) })

	if v := rec.Vectors[VectorContent]; v != nil {
		if err := r.vectors.Stage(tx, t, ns, rec.ID, v); err != nil {
			return err
		}
	}
	return r.index(ctx, tx, t, ns, rec.ID, rec)
}

// index stages every derived index over one record, in registration order.
// A failure stops there and leaves the transaction uncommitted, so a partly
// indexed record is never visible.
func (r *repo) index(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace,
	rid id.ID, rec *Record) error {
	for _, ix := range r.indexers {
		if err := ix.Stage(ctx, tx, t, ns, rid, rec); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo) Delete(ctx context.Context, tx txn.Tx, rid id.ID) error {
	t, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	ns := tenant.DefaultNamespace
	// The vector goes with the record. A canonical vector whose record is gone
	// is an orphan the flat index still scans and still ranks.
	key := BodyKey(t, ns, rid)
	old, err := tx.Get(key)
	if err != nil && !errs.Is(err, errs.NotFound) {
		return err
	}
	tx.Expect(key, old, err == nil)
	tx.Delete(key)
	r.vectors.StageDelete(tx, t, ns, rid)
	return r.index(ctx, tx, t, ns, rid, nil)
}

func (r *repo) Scan(ctx context.Context, snap storage.Snapshot, from *id.ID, limit int) ([]*Record, error) {
	const op = "record.Scan"

	t, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("a scan limit must be greater than zero"))
	}
	ns := tenant.DefaultNamespace

	lower, upper := keys.PrefixRange(bodyPrefix(t, ns))
	if from != nil {
		// Half-open on the low end: SeekGE would return the cursor row again,
		// so the cursor is advanced past it by seeking to its successor.
		lower = successor(BodyKey(t, ns, *from))
	}

	it := snap.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	out := make([]*Record, 0, limit)
	for ok := it.First(); ok && len(out) < limit; ok = it.Next() {
		_, _, _, rid, err := keys.ParseRecord(it.Key())
		if err != nil {
			return nil, err
		}
		rec, err := decodeBody(t, ns, rid, it.Value(), op)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}

	// The vectors are read after the body scan rather than during it, because
	// the iterator's key and value buffers are only valid until the next
	// positioning call and a Get in the middle of a walk is a second iterator
	// over the same snapshot.
	for _, rec := range out {
		vec, err := snap.Get(ctx, VectorKey(t, ns, rec.ID))
		if errs.Is(err, errs.NotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		v, err := decodeVector(rec.ID, vec, op)
		if err != nil {
			return nil, err
		}
		rec.Vectors[VectorContent] = v
	}
	return out, nil
}

// bodyPrefix is every record body of one namespace: the space prefix plus the
// record type byte, so a scan over memories does not walk another type's rows
// once one exists.
func bodyPrefix(t tenant.ID, ns tenant.Namespace) []byte {
	k := BodyKey(t, ns, id.Zero)
	return k[:len(k)-len(id.Zero)]
}

// successor is the smallest key greater than k: k with a zero byte appended.
// Nothing sorts between the two, so it is exactly "the row after this one".
func successor(k []byte) []byte {
	return append(append([]byte(nil), k...), 0x00)
}

func validate(t tenant.ID, rec *Record, op string) error {
	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
	}
	switch {
	case rec == nil:
		return bad("record is nil")
	case rec.ID.IsZero():
		return bad("a record must have an id")
	case rec.Tenant != "" && rec.Tenant != t:
		return bad("record belongs to tenant %q but is being written into %q", rec.Tenant, t)
	case rec.Type == 0:
		return bad("a record must have a type")
	case rec.Content == "":
		return bad("a record must have content")
	case rec.CreatedAt.IsZero():
		return bad("a record must carry its creation time")
	}
	for name, v := range rec.Vectors {
		switch {
		case v == nil:
			return bad("vector %q is nil", name)
		case v.ModelID == "":
			return bad("vector %q carries no model id: a later model change would corrupt the space silently", name)
		case v.Dim != len(v.Values):
			return bad("vector %q declares %d dimensions and carries %d values", name, v.Dim, len(v.Values))
		}
	}
	return nil
}
