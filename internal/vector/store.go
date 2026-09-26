package vector

import (
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/codec/pb"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// Vector is a canonical embedding.
//
// It travels with the model that produced it. Nothing in Rust Remem recorded
// that (plan §II.10 row 10), so changing the model left every stored vector the
// right width and the wrong meaning, and search degraded without ever failing.
type Vector struct {
	ModelID string
	Dim     int
	Values  []float32
}

// Clone returns a deep copy. A Vector handed to a caller must not alias what
// the store decoded, or a caller that normalises in place mutates what the next
// read returns.
func (v *Vector) Clone() *Vector {
	if v == nil {
		return nil
	}
	out := *v
	out.Values = append([]float32(nil), v.Values...)
	return &out
}

// Store owns the canonical vector key space: its keys, its encoding, and every
// read and write of it.
//
// One owner, because there are two writers — the record repository, which
// stages a vector into the same transaction as its record, and a rebuild, which
// writes outside one. Two copies of the key layout and the framing would be two
// things that can disagree about a durable format.
type Store struct{ kv storage.KV }

// NewStore returns the canonical vector store over kv.
func NewStore(kv storage.KV) *Store { return &Store{kv: kv} }

var _ Source = (*Store)(nil)

// Key is the storage key of one canonical vector.
func Key(t tenant.ID, ns tenant.Namespace, rid id.ID) []byte { return keys.Vector(t, ns, rid) }

// Encode frames a vector for storage.
func Encode(v *Vector) ([]byte, error) {
	if v == nil {
		return nil, errs.E(errs.Invalid, "vector.Encode", errors.New("vector is nil"))
	}
	return codec.MarshalVector(&pb.Vector{
		ModelId: v.ModelID,
		Dim:     uint32(v.Dim),
		Values:  v.Values,
	})
}

// Decode reads a stored vector.
func Decode(b []byte) (*Vector, error) {
	v, err := codec.UnmarshalVector(b)
	if err != nil {
		return nil, err
	}
	return &Vector{ModelID: v.GetModelId(), Dim: int(v.GetDim()), Values: v.GetValues()}, nil
}

// Stage writes a vector into tx, so that it commits with the record it belongs
// to and not separately (spec §12).
func (s *Store) Stage(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID, v *Vector) error {
	value, err := Encode(v)
	if err != nil {
		return err
	}
	tx.Set(Key(t, ns, rid), value)
	return nil
}

// StageDelete removes a vector as part of tx.
func (s *Store) StageDelete(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID) {
	tx.Delete(Key(t, ns, rid))
}

// Put writes a vector outside any transaction. It is the rebuild path; a write
// that must be atomic with its record uses [Store.Stage].
func (s *Store) Put(ctx context.Context, t tenant.ID, ns tenant.Namespace, rid id.ID, v *Vector) error {
	value, err := Encode(v)
	if err != nil {
		return err
	}
	return s.kv.Set(ctx, Key(t, ns, rid), value)
}

// Delete removes a vector outside any transaction.
func (s *Store) Delete(ctx context.Context, t tenant.ID, ns tenant.Namespace, rid id.ID) error {
	return s.kv.Delete(ctx, Key(t, ns, rid))
}

// Replace atomically makes vectors the complete canonical vector set for one
// tenant and namespace. Encoding and staging may fail, but no reader observes
// any replacement or removal until the whole transaction commits.
//
// # An empty ModelID inherits, it does not erase
//
// A vector arriving with no ModelID takes the one already stored for that
// record, and only falls back to "unknown" when there is nothing to inherit.
//
// This is not politeness, it is the fix for a defect that reached a shipped
// command. Replace is the choke point of every rebuild, and a rebuild reads its
// vectors through [Source], which yields coordinates and nothing else — so a
// rebuild cannot know what produced them. `remem-admin vector rebuild` therefore
// wrote "unknown" over the model id of every vector in the corpus it was
// repairing, and the next search refused the tenant outright because the server
// runs a model the corpus no longer claimed. §II.10 row 10 exists precisely
// because a vector that does not say what made it cannot be told afterwards
// from one that does; a repair path that erases it is worse than no repair path.
//
// A caller that genuinely knows better — an import carrying vectors from a
// named model — sets ModelID and overwrites, which is what it should do.
func (s *Store) Replace(ctx context.Context, t tenant.ID, ns tenant.Namespace, vectors map[id.ID]*Vector) error {
	existing := make(map[id.ID]string)
	var order []id.ID
	if err := s.scan(ctx, t, ns, func(rid id.ID, v *Vector) error {
		existing[rid] = v.ModelID
		order = append(order, rid)
		return nil
	}); err != nil {
		return err
	}

	tx := txn.New(s.kv)
	defer tx.Close()
	for _, rid := range order {
		if _, replaced := vectors[rid]; !replaced {
			s.StageDelete(tx, t, ns, rid)
		}
	}
	for rid, v := range vectors {
		if v != nil && v.ModelID == "" {
			inherited := existing[rid]
			if inherited == "" {
				// Nothing to inherit. "unknown" rather than empty because the
				// codec refuses a vector carrying no model id at all, and
				// "unknown" is a value a later audit can find and a later
				// migration can act on.
				inherited = UnknownModel
			}
			clone := *v
			clone.ModelID = inherited
			v = &clone
		}
		if err := s.Stage(tx, t, ns, rid, v); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// UnknownModel is the model id given to a vector whose provenance nothing
// records. It is deliberately a value rather than an empty string: an audit can
// search for it, and a migration can act on it.
const UnknownModel = "unknown"

// Get returns one canonical vector, or errs.NotFound.
func (s *Store) Get(ctx context.Context, t tenant.ID, ns tenant.Namespace, rid id.ID) (*Vector, error) {
	const op = "vector.Store.Get"
	b, err := s.kv.Get(ctx, Key(t, ns, rid))
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return nil, errs.E(errs.NotFound, op, fmt.Errorf("no vector for record %s", rid))
		}
		return nil, err
	}
	v, err := Decode(b)
	if err != nil {
		return nil, errs.E(errs.KindOf(err), op, fmt.Errorf("the vector of record %s: %w", rid, err))
	}
	return v, nil
}

// Scan walks every canonical vector of a tenant's default namespace, in key
// order. It is deliberately not called ForEach — see [Source].
//
// The values handed to fn are borrowed from the iterator and are valid only for
// the duration of the call, which is what lets a scan of a hundred thousand
// vectors allocate nothing per row. A callback that keeps one copies it.
func (s *Store) Scan(ctx context.Context, t tenant.ID, fn func(id.ID, []float32) error) error {
	return s.scan(ctx, t, tenant.DefaultNamespace, func(rid id.ID, v *Vector) error {
		return fn(rid, v.Values)
	})
}

// ScanVectors walks every canonical vector of a tenant's default namespace with
// the model identity attached, in key order.
//
// [Store.Scan] drops the model id because vector.Source exists to feed an index
// plain coordinates. An index that materialises a whole tenant needs more than
// that: a corpus holding vectors from two different models is not a space, and
// the only place that can be noticed is where every vector is read at once.
//
// The Vector handed to fn is borrowed from the iterator. A callback that keeps
// one calls Clone.
func (s *Store) ScanVectors(ctx context.Context, t tenant.ID, fn func(id.ID, *Vector) error) error {
	return s.scan(ctx, t, tenant.DefaultNamespace, fn)
}

func (s *Store) scan(ctx context.Context, t tenant.ID, ns tenant.Namespace, fn func(id.ID, *Vector) error) error {
	const op = "vector.Store.Scan"

	if t == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"a vector scan requires a tenant: there is no unscoped read path (Invariant 1)"))
	}

	lower, upper := keys.SpaceRange(t, ns, keys.SpaceVector)
	it := s.kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	for ok := it.First(); ok; ok = it.Next() {
		rid, err := ridFromKey(it.Key(), op)
		if err != nil {
			return err
		}
		v, err := Decode(it.Value())
		if err != nil {
			return errs.E(errs.KindOf(err), op, fmt.Errorf("the vector of record %s: %w", rid, err))
		}
		if err := fn(rid, v); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
	}
	return it.Error()
}

// ridFromKey recovers the record id trailing a vector key.
func ridFromKey(k []byte, op string) (id.ID, error) {
	if len(k) < 16 {
		return id.Zero, errs.E(errs.Corruption, op, errors.New("a key in the vector range is too short to hold a record id"))
	}
	rid, err := id.FromBytes(k[len(k)-16:])
	if err != nil {
		return id.Zero, errs.E(errs.Corruption, op, errors.New("a vector key holds a malformed record id"))
	}
	return rid, nil
}
