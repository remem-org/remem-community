package record_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

const model = "all-MiniLM-L6-v2"

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ctx carries a tenant, because every read path below the API layer needs one:
// Invariant 1 has no unscoped variant, so an "empty" context in these tests is
// still a scoped one.
func ctx() context.Context { return withTenant(context.Background(), "acme") }

func withTenant(parent context.Context, t tenant.ID) context.Context {
	return tenant.NewContext(parent, t)
}

func sampleRecord(t tenant.ID) *record.Record {
	values := make([]float32, 384)
	for i := range values {
		values[i] = float32(i%7) / 10
	}
	now := clock.FakeStart
	return &record.Record{
		ID:      id.New(),
		Tenant:  t,
		Type:    record.TypeMemory,
		Content: "the sky is blue",
		Fields:  record.Fields{Tags: []string{"weather"}, Source: "test"},
		Vectors: map[string]*record.Vector{
			record.VectorContent: {ModelID: model, Dim: 384, Values: values},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func newRepo(t *testing.T) (record.Repo, storage.KV) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return record.NewRepo(kv), kv
}

func commitPut(t *testing.T, repo record.Repo, kv storage.KV, r *record.Record) {
	t.Helper()
	tx := txn.New(kv)
	defer tx.Close()
	must(t, repo.Put(withTenant(context.Background(), r.Tenant), tx, r))
	must(t, tx.Commit(context.Background()))
}

func TestPutWritesRecordAndVectorInOneTransaction(t *testing.T) {
	repo, kv := newRepo(t)
	tx := txn.New(kv)
	defer tx.Close()
	r := sampleRecord("acme")
	must(t, repo.Put(ctx(), tx, r))

	// Nothing visible before commit.
	if _, err := repo.Get(ctx(), r.ID); !errs.Is(err, errs.NotFound) {
		t.Fatal("the record was visible before its transaction committed")
	}
	must(t, tx.Commit(ctx()))

	got, err := repo.Get(withTenant(ctx(), "acme"), r.ID)
	must(t, err)
	if got.Content != r.Content {
		t.Fatalf("content lost: %q", got.Content)
	}
	if len(got.Vectors[record.VectorContent].Values) != 384 {
		t.Fatalf("vector lost: %d dims", len(got.Vectors[record.VectorContent].Values))
	}
}

func TestGetIsTenantScoped(t *testing.T) {
	repo, kv := newRepo(t)
	r := sampleRecord("acme")
	commitPut(t, repo, kv, r)

	_, err := repo.Get(withTenant(ctx(), "other"), r.ID)
	if !errs.Is(err, errs.NotFound) {
		t.Fatalf("a record must be invisible outside its tenant, got %v", err)
	}
}

func TestGetWithoutATenantIsRefused(t *testing.T) {
	repo, _ := newRepo(t)
	_, err := repo.Get(context.Background(), id.New())
	if err == nil {
		t.Fatal("Invariant 1: no unscoped read path")
	}
}

func TestVectorCarriesModelIdentity(t *testing.T) {
	repo, kv := newRepo(t)
	r := sampleRecord("acme")
	r.Vectors[record.VectorContent].ModelID = model
	// A record whose vector was produced by a different model must be
	// detectable, or a model change silently corrupts the space.
	commitPut(t, repo, kv, r)
	got, err := repo.Get(ctx(), r.ID)
	must(t, err)
	if got.Vectors[record.VectorContent].ModelID != model {
		t.Fatal("model identity lost")
	}
}

func TestPutRefusesARecordWhoseTenantIsNotTheContexts(t *testing.T) {
	repo, kv := newRepo(t)
	tx := txn.New(kv)
	defer tx.Close()
	r := sampleRecord("other")
	if err := repo.Put(ctx(), tx, r); !errs.Is(err, errs.Invalid) {
		t.Fatalf("Put = %v; a record must not be written into a tenant other than the request's", err)
	}
}

func TestPutRefusesAnIncoherentRecord(t *testing.T) {
	repo, kv := newRepo(t)
	for name, mangle := range map[string]func(*record.Record){
		"no id":           func(r *record.Record) { r.ID = id.Zero },
		"no content":      func(r *record.Record) { r.Content = "" },
		"no type":         func(r *record.Record) { r.Type = 0 },
		"vector dim lies": func(r *record.Record) { r.Vectors[record.VectorContent].Dim = 7 },
		"vector no model": func(r *record.Record) { r.Vectors[record.VectorContent].ModelID = "" },
		"no created time": func(r *record.Record) { r.CreatedAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			tx := txn.New(kv)
			defer tx.Close()
			r := sampleRecord("acme")
			mangle(r)
			if err := repo.Put(ctx(), tx, r); !errs.Is(err, errs.Invalid) {
				t.Fatalf("Put = %v, want Invalid", err)
			}
		})
	}
}

func TestDeleteRemovesTheRecordAndItsVector(t *testing.T) {
	repo, kv := newRepo(t)
	r := sampleRecord("acme")
	commitPut(t, repo, kv, r)

	tx := txn.New(kv)
	defer tx.Close()
	must(t, repo.Delete(ctx(), tx, r.ID))
	must(t, tx.Commit(ctx()))

	if _, err := repo.Get(ctx(), r.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("Get after Delete = %v", err)
	}
	// The vector must go with it. A canonical vector whose record is gone is
	// an orphan the flat index would still scan and still rank.
	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	got, err := repo.Scan(ctx(), snap, nil, 10)
	must(t, err)
	if len(got) != 0 {
		t.Fatalf("Scan returned %d records after Delete", len(got))
	}
}

func TestScanIsOrderedTenantScopedAndPageable(t *testing.T) {
	repo, kv := newRepo(t)
	var acme []id.ID
	for range 5 {
		r := sampleRecord("acme")
		commitPut(t, repo, kv, r)
		acme = append(acme, r.ID)
	}
	commitPut(t, repo, kv, sampleRecord("other"))

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	all, err := repo.Scan(ctx(), snap, nil, 100)
	must(t, err)
	if len(all) != 5 {
		t.Fatalf("Scan returned %d records for acme, want 5 — the other tenant leaked", len(all))
	}
	for i := 1; i < len(all); i++ {
		if string(all[i-1].ID[:]) >= string(all[i].ID[:]) {
			t.Fatalf("Scan is not in id order at %d", i)
		}
	}

	first, err := repo.Scan(ctx(), snap, nil, 2)
	must(t, err)
	if len(first) != 2 {
		t.Fatalf("limit ignored: %d records", len(first))
	}
	from := first[len(first)-1].ID
	next, err := repo.Scan(ctx(), snap, &from, 2)
	must(t, err)
	if len(next) != 2 || next[0].ID == from {
		t.Fatalf("the second page repeated its cursor: %v", next[0].ID)
	}
	_ = acme
}

// A snapshot is what makes a paged listing safe (plan §II.9): page two must
// see the state page one saw, not the state at the moment it was asked for.
func TestScanSeesTheSnapshotNotThePresent(t *testing.T) {
	repo, kv := newRepo(t)
	commitPut(t, repo, kv, sampleRecord("acme"))

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	commitPut(t, repo, kv, sampleRecord("acme"))

	got, err := repo.Scan(ctx(), snap, nil, 100)
	must(t, err)
	if len(got) != 1 {
		t.Fatalf("the snapshot saw %d records; a write made after it was taken is invisible", len(got))
	}
}

// Canonical data that will not decode is never replaced by a zero value:
// returning an empty record would silently swap a memory for nothing.
func TestACorruptRecordBodyIsCorruption(t *testing.T) {
	repo, kv := newRepo(t)
	r := sampleRecord("acme")
	commitPut(t, repo, kv, r)

	must(t, kv.Set(context.Background(), record.BodyKey("acme", tenant.DefaultNamespace, r.ID), []byte("junk")))
	if _, err := repo.Get(ctx(), r.ID); !errs.Is(err, errs.Corruption) {
		t.Fatalf("Get = %v, want Corruption", err)
	}
}

// The id inside the body must match the key it was read from. If it does not,
// something wrote a record under the wrong key, and answering with it would
// return one memory when another was asked for.
func TestARecordUnderTheWrongKeyIsCorruption(t *testing.T) {
	repo, kv := newRepo(t)
	r := sampleRecord("acme")
	commitPut(t, repo, kv, r)

	body, err := kv.Get(context.Background(), record.BodyKey("acme", tenant.DefaultNamespace, r.ID))
	must(t, err)
	other := id.New()
	must(t, kv.Set(context.Background(), record.BodyKey("acme", tenant.DefaultNamespace, other), body))

	if _, err := repo.Get(ctx(), other); !errs.Is(err, errs.Corruption) {
		t.Fatalf("Get = %v, want Corruption", err)
	}
}

func TestArchivalSurvivesTheRoundTrip(t *testing.T) {
	repo, kv := newRepo(t)
	r := sampleRecord("acme")
	r.Fields.Archived = true
	r.Fields.ArchivedAt = clock.FakeStart.Add(time.Hour)
	commitPut(t, repo, kv, r)

	got, err := repo.Get(ctx(), r.ID)
	must(t, err)
	if !got.Fields.Archived || !got.Fields.ArchivedAt.Equal(r.Fields.ArchivedAt) {
		t.Fatalf("archival lost: %+v", got.Fields)
	}
}

func TestTagsAndSourceSurviveTheRoundTrip(t *testing.T) {
	repo, kv := newRepo(t)
	r := sampleRecord("acme")
	r.Fields.Tags = []string{"weather", "sky"}
	commitPut(t, repo, kv, r)

	got, err := repo.Get(ctx(), r.ID)
	must(t, err)
	if len(got.Fields.Tags) != 2 || got.Fields.Tags[0] != "weather" {
		t.Fatalf("tags lost: %v", got.Fields.Tags)
	}
	if got.Fields.Source != "test" {
		t.Fatalf("source lost: %q", got.Fields.Source)
	}
}
