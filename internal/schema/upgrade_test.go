package schema_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/version"
)

func tagging(from, to uint32, tag string) schema.RecordUpgrade {
	return schema.RecordUpgrade{From: from, To: to, Apply: func(r *record.Record) error {
		r.Fields.Tags = append(r.Fields.Tags, tag)
		return nil
	}}
}

func TestNoUpgradesMeansNoHook(t *testing.T) {
	// Nil, not an upgrader that decides to do nothing on every read: the read
	// path should carry no hook at all when nothing is registered.
	u, err := schema.NewRecordUpgrader()
	if err != nil {
		t.Fatal(err)
	}
	if u != nil {
		t.Fatalf("got %v", u)
	}
}

func TestTheChainRunsEveryStepInOrder(t *testing.T) {
	u, err := schema.NewRecordUpgrader(tagging(2, 3, "second"), tagging(1, 2, "first"))
	if err != nil {
		t.Fatal(err)
	}

	rec := &record.Record{ID: id.New(), Content: "x", SchemaVersion: 1}
	to, changed, err := u.Upgrade(ctx(), rec, rec.SchemaVersion)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || to != 3 {
		t.Fatalf("to=%d changed=%v", to, changed)
	}
	if strings.Join(rec.Fields.Tags, ",") != "first,second" {
		t.Fatalf("the chain ran in %v", rec.Fields.Tags)
	}
}

func TestARecordAlreadyCurrentIsUntouched(t *testing.T) {
	u, err := schema.NewRecordUpgrader(tagging(1, 2, "first"))
	if err != nil {
		t.Fatal(err)
	}

	rec := &record.Record{ID: id.New(), Content: "x", SchemaVersion: 2}
	to, changed, err := u.Upgrade(ctx(), rec, rec.SchemaVersion)
	if err != nil {
		t.Fatal(err)
	}
	if changed || to != 2 || len(rec.Fields.Tags) != 0 {
		t.Fatalf("to=%d changed=%v tags=%v", to, changed, rec.Fields.Tags)
	}
}

// The rolling-upgrade case. A record written by a newer binary, at a schema
// this one has never heard of, is readable — protobuf keeps the fields this
// binary does not understand — and refusing it would take a mixed-version
// deployment down on the read path.
func TestARecordFromTheFutureIsLeftAlone(t *testing.T) {
	u, err := schema.NewRecordUpgrader(tagging(1, 2, "first"))
	if err != nil {
		t.Fatal(err)
	}

	rec := &record.Record{ID: id.New(), Content: "x", SchemaVersion: 9}
	to, changed, err := u.Upgrade(ctx(), rec, rec.SchemaVersion)
	if err != nil {
		t.Fatal(err)
	}
	if changed || to != 9 {
		t.Fatalf("to=%d changed=%v", to, changed)
	}
}

func TestAFailedUpgradeIsNotPartiallyApplied(t *testing.T) {
	boom := schema.RecordUpgrade{From: 2, To: 3, Apply: func(*record.Record) error {
		return errors.New("this upgrade cannot be done")
	}}
	u, err := schema.NewRecordUpgrader(tagging(1, 2, "first"), boom)
	if err != nil {
		t.Fatal(err)
	}

	rec := &record.Record{ID: id.New(), Content: "x", SchemaVersion: 1}
	to, changed, err := u.Upgrade(ctx(), rec, rec.SchemaVersion)
	if err == nil {
		t.Fatal("a failing step reported success")
	}
	// The version must not advance: a record the caller reads as current and is
	// not is worse than one that plainly failed.
	if changed || to != 1 {
		t.Fatalf("to=%d changed=%v", to, changed)
	}
}

func TestTheChainRefusesAGap(t *testing.T) {
	_, err := schema.NewRecordUpgrader(tagging(1, 2, "a"), tagging(3, 4, "b"))
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "every read") {
		t.Fatalf("the error must say what a gap costs: %v", err)
	}
}

func TestTheChainRefusesABranch(t *testing.T) {
	if _, err := schema.NewRecordUpgrader(tagging(1, 2, "a"), tagging(1, 3, "b")); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}
}

func TestTheChainRefusesAStepThatDoesNotAdvance(t *testing.T) {
	if _, err := schema.NewRecordUpgrader(tagging(2, 1, "backwards")); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}
}

// --- through the repository -------------------------------------------------

func writeRecord(t *testing.T, kv storage.KV, c context.Context, repo record.Repo, rec *record.Record) {
	t.Helper()
	tx := txn.New(kv)
	defer tx.Close()
	if err := repo.Put(c, tx, rec); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(c); err != nil {
		t.Fatal(err)
	}
}

// The plan's TestLazyUpgradeRewritesOnRead: reading an old-format record
// persists the upgraded form, so the next reader — including one with no
// upgrader — sees the new shape.
func TestLazyUpgradeRewritesOnRead(t *testing.T) {
	kv := newKV(t)
	c := tenant.NewContext(ctx(), "acme")

	plain := record.NewRepo(kv)
	rec := &record.Record{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: "an old memory", SchemaVersion: 1,
		CreatedAt: at2026(),
	}
	writeRecord(t, kv, c, plain, rec)

	up, err := schema.NewRecordUpgrader(tagging(1, 2, "upgraded"))
	if err != nil {
		t.Fatal(err)
	}
	upgrading := record.NewRepo(kv, record.WithUpgrader(up))

	got, err := upgrading.Get(c, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 2 || len(got.Fields.Tags) != 1 {
		t.Fatalf("the read did not upgrade: version=%d tags=%v", got.SchemaVersion, got.Fields.Tags)
	}

	// Persisted, not merely returned: a reader with no upgrader must see it.
	stored, err := plain.Get(c, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SchemaVersion != 2 {
		t.Fatalf("the upgrade was returned and not written back: stored at version %d", stored.SchemaVersion)
	}
	if len(stored.Fields.Tags) != 1 || stored.Fields.Tags[0] != "upgraded" {
		t.Fatalf("the stored record does not carry the upgrade: %v", stored.Fields.Tags)
	}
}

// A read must not fail because a cache-fill did. `remem-admin inspect` opens a
// directory read-only, and a read path that failed there would make an upgraded
// corpus unreadable by the tool an operator reaches for when it is in trouble.
func TestLazyUpgradeSurvivesAFailedWriteBack(t *testing.T) {
	kv := newKV(t)
	c := tenant.NewContext(ctx(), "acme")

	rec := &record.Record{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: "an old memory", SchemaVersion: 1,
		CreatedAt: at2026(),
	}
	writeRecord(t, kv, c, record.NewRepo(kv), rec)

	up, err := schema.NewRecordUpgrader(tagging(1, 2, "upgraded"))
	if err != nil {
		t.Fatal(err)
	}
	readOnly := record.NewRepo(&failingCommits{KV: kv}, record.WithUpgrader(up))

	got, err := readOnly.Get(c, rec.ID)
	if err != nil {
		t.Fatalf("a read failed because its write-back failed: %v", err)
	}
	if got.SchemaVersion != 2 || len(got.Fields.Tags) != 1 {
		t.Fatalf("the record was not upgraded in memory: version=%d tags=%v", got.SchemaVersion, got.Fields.Tags)
	}
	// And nothing was written, so the next read redoes it rather than seeing
	// half of it.
	stored, err := record.NewRepo(kv).Get(c, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SchemaVersion != 1 {
		t.Fatalf("a write that failed to commit changed the stored record to version %d", stored.SchemaVersion)
	}
}

// An upgrade that cannot be applied fails the read. Returning a half-upgraded
// record would hand the caller something it reads as current and is not.
func TestAFailedUpgradeFailsTheRead(t *testing.T) {
	kv := newKV(t)
	c := tenant.NewContext(ctx(), "acme")

	rec := &record.Record{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: "x", SchemaVersion: 1, CreatedAt: at2026(),
	}
	writeRecord(t, kv, c, record.NewRepo(kv), rec)

	up, err := schema.NewRecordUpgrader(schema.RecordUpgrade{From: 1, To: 2, Apply: func(*record.Record) error {
		return errs.E(errs.Corruption, "test", errors.New("this record cannot be upgraded"))
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := record.NewRepo(kv, record.WithUpgrader(up)).Get(c, rec.ID); err == nil {
		t.Fatal("a record that could not be upgraded was returned as if it were current")
	}
}

// A listing does not upgrade. A page of results that rewrote every record it
// walked would turn a read into a write proportional to the corpus, and a
// background migration is the tool for a whole corpus.
func TestScanDoesNotUpgrade(t *testing.T) {
	kv := newKV(t)
	c := tenant.NewContext(ctx(), "acme")

	rec := &record.Record{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: "x", SchemaVersion: 1, CreatedAt: at2026(),
	}
	writeRecord(t, kv, c, record.NewRepo(kv), rec)

	up, err := schema.NewRecordUpgrader(tagging(1, 2, "upgraded"))
	if err != nil {
		t.Fatal(err)
	}
	repo := record.NewRepo(kv, record.WithUpgrader(up))

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	got, err := repo.Scan(c, snap, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records", len(got))
	}
	if got[0].SchemaVersion != 1 {
		t.Fatalf("Scan upgraded a record to version %d", got[0].SchemaVersion)
	}
}

// --- the two version scales stay apart --------------------------------------

// The test proto/tenant/v1/tenant.proto promises Phase 4 would write: the
// tenant's user-schema version and Remem's storage format versions are separate
// numbers, and moving one leaves the other unmoved.
func TestUserSchemaVersionIsIndependentOfStorageFormat(t *testing.T) {
	kv := newKV(t)
	dir := t.TempDir()
	c := tenant.NewContext(ctx(), "acme")

	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}
	dirs := tenantkv.New(kv, clock.NewFake(at2026()))
	if err := dirs.Create(ctx(), "acme", tenant.Meta{SchemaVersion: 7}); err != nil {
		t.Fatal(err)
	}
	rec := &record.Record{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: "x", SchemaVersion: 7, CreatedAt: at2026(),
	}
	writeRecord(t, kv, c, record.NewRepo(kv), rec)

	// Move the storage format. Nothing about the tenant or the record is a
	// storage format, so nothing about either may move with it.
	if err := newRunner(t, kv, dir).Run(ctx(), []schema.Migration{step("attr-1-to-2", "attr_schema", 1, 2)}); err != nil {
		t.Fatal(err)
	}

	f, err := schema.ReadFormat(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Subsystems["attr_schema"].Current; got != 2 {
		t.Fatalf("the storage format did not move: %d", got)
	}

	meta, err := dirs.Get(ctx(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if meta.SchemaVersion != 7 {
		t.Fatalf("a storage format migration moved the tenant's user schema to %d", meta.SchemaVersion)
	}
	got, err := record.NewRepo(kv).Get(c, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 7 {
		t.Fatalf("a storage format migration moved the record's user schema to %d", got.SchemaVersion)
	}

	// And the converse: a record at a user-schema version the binary's storage
	// versions know nothing about is stored and read back unchanged.
	if v := version.Current(); v.RecordEnvelope == 7 || v.AttrSchema == 7 {
		t.Fatal("this test relies on 7 not being one of the storage format versions")
	}
}
