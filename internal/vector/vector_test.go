package vector_test

import (
	"context"
	"errors"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

const ns = tenant.DefaultNamespace

func newStore(t *testing.T) (*vector.Store, *memkv.Store) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return vector.NewStore(kv), kv
}

func sample() *vector.Vector {
	return &vector.Vector{ModelID: "all-MiniLM-L6-v2", Dim: 4, Values: []float32{0.5, 0.5, 0.5, 0.5}}
}

func TestPutGetRoundTripsWithModelIdentity(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	rid := id.New()
	if err := s.Put(ctx, "acme", ns, rid, sample()); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "acme", ns, rid)
	if err != nil {
		t.Fatal(err)
	}
	if got.ModelID != "all-MiniLM-L6-v2" || got.Dim != 4 || len(got.Values) != 4 {
		t.Fatalf("got %+v", got)
	}
}

func TestGetOfAnAbsentVectorIsNotFound(t *testing.T) {
	s, _ := newStore(t)
	if _, err := s.Get(context.Background(), "acme", ns, id.New()); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

// A vector is canonical data. A value that will not decode is Corruption
// naming the record, never an absent vector: reporting it missing would let a
// search quietly return one fewer result forever.
func TestACorruptVectorIsCorruption(t *testing.T) {
	s, kv := newStore(t)
	ctx := context.Background()
	rid := id.New()
	if err := s.Put(ctx, "acme", ns, rid, sample()); err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(ctx, vector.Key("acme", ns, rid), []byte("not a vector")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "acme", ns, rid); !errs.Is(err, errs.Corruption) {
		t.Fatalf("Get = %v, want Corruption", err)
	}
	err := s.Scan(ctx, "acme", func(id.ID, []float32) error { return nil })
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("Scan = %v, want Corruption", err)
	}
}

func TestStageCommitsWithItsTransaction(t *testing.T) {
	s, kv := newStore(t)
	ctx := context.Background()
	rid := id.New()

	tx := txn.New(kv)
	defer tx.Close()
	if err := s.Stage(tx, "acme", ns, rid, sample()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "acme", ns, rid); !errs.Is(err, errs.NotFound) {
		t.Fatal("a staged vector was visible before its transaction committed")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "acme", ns, rid); err != nil {
		t.Fatalf("after commit: %v", err)
	}
}

func TestStageDeleteCommitsWithItsTransaction(t *testing.T) {
	s, kv := newStore(t)
	ctx := context.Background()
	rid := id.New()
	if err := s.Put(ctx, "acme", ns, rid, sample()); err != nil {
		t.Fatal(err)
	}

	tx := txn.New(kv)
	defer tx.Close()
	s.StageDelete(tx, "acme", ns, rid)
	if _, err := s.Get(ctx, "acme", ns, rid); err != nil {
		t.Fatal("the vector vanished before the transaction committed")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "acme", ns, rid); !errs.Is(err, errs.NotFound) {
		t.Fatalf("after commit: %v", err)
	}
}

func TestScanIsTenantScopedAndOrdered(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	for range 3 {
		if err := s.Put(ctx, "acme", ns, id.New(), sample()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put(ctx, "other", ns, id.New(), sample()); err != nil {
		t.Fatal(err)
	}

	var seen []id.ID
	if err := s.Scan(ctx, "acme", func(rid id.ID, _ []float32) error {
		seen = append(seen, rid)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 {
		t.Fatalf("acme's scan saw %d vectors; the other tenant leaked", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if string(seen[i-1][:]) >= string(seen[i][:]) {
			t.Fatal("the scan is not in key order")
		}
	}
}

func TestScanStopsAtTheCallbacksError(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	for range 5 {
		if err := s.Put(ctx, "acme", ns, id.New(), sample()); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("stop")
	n := 0
	err := s.Scan(ctx, "acme", func(id.ID, []float32) error {
		n++
		if n == 2 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Scan = %v", err)
	}
	if n != 2 {
		t.Fatalf("the scan continued past the error: %d rows", n)
	}
}

func TestAnUnscopedScanIsRefused(t *testing.T) {
	s, _ := newStore(t)
	err := s.Scan(context.Background(), "", func(id.ID, []float32) error { return nil })
	if !errs.Is(err, errs.Invalid) {
		t.Fatal("Invariant 1: there is no unscoped read path")
	}
}

// Encode refuses a vector that cannot be stored safely, rather than letting the
// error surface later as a search result nobody can explain.
func TestEncodeRefusesAnIncoherentVector(t *testing.T) {
	for name, v := range map[string]*vector.Vector{
		"nil":          nil,
		"no model":     {Dim: 1, Values: []float32{1}},
		"dim is a lie": {ModelID: "m", Dim: 9, Values: []float32{1}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := vector.Encode(v); !errs.Is(err, errs.Invalid) {
				t.Fatalf("got %v, want Invalid", err)
			}
		})
	}
}

// A Vector handed to a caller must not alias what the store decoded: a caller
// that normalises in place would otherwise mutate what the next read returns.
func TestCloneDoesNotAlias(t *testing.T) {
	v := sample()
	c := v.Clone()
	c.Values[0] = 99
	if v.Values[0] == 99 {
		t.Fatal("Clone returned a shallow copy")
	}
	if (*vector.Vector)(nil).Clone() != nil {
		t.Fatal("cloning nil must give nil")
	}
}
