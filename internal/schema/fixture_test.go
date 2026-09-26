package schema_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
)

// The fixture is a whole directory at an older format, held as a portable
// key-value stream rather than as a checked-in Pebble directory.
//
// A Pebble directory would be a fixture whose format is *Pebble's* version, not
// Remem's: a Pebble upgrade would break it, and reviewing a change to it would
// mean reading a binary blob. The stream is the same bytes storage.Dump
// produces, so it is regenerable, diffable, and readable by both stores.
const (
	fixtureDir  = "testdata/legacy-format"
	fixtureFile = "store.kvdump"

	// legacyRecords is how many memories the fixture holds. Small enough to
	// review, large enough that a migration that stops early is visible.
	legacyRecords = 25
)

// updateFixtures regenerates the checked-in fixture:
//
//	go test ./internal/schema -run TestGenerateTheLegacyFixture -update-fixtures
//
// It is a flag rather than a script so the generator and the test that consumes
// it cannot drift: whatever writes the fixture is compiled against the same
// types that read it.
var updateFixtures = flag.Bool("update-fixtures", false, "regenerate internal/schema/testdata")

const legacyTenant tenant.ID = "acme"

// legacyStore builds a directory as an older Remem left it: the manifest at
// attr_schema 1, one tenant, and a corpus of records at user schema 1.
func legacyStore(t *testing.T) *memkv.Store {
	t.Helper()
	kv := memkv.New()

	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}
	dirs := tenantkv.New(kv, clock.NewFake(at2026()))
	if err := dirs.Create(ctx(), legacyTenant, tenant.Meta{DisplayName: "Acme", SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}

	c := tenant.NewContext(ctx(), legacyTenant)
	repo := record.NewRepo(kv)
	for i := 0; i < legacyRecords; i++ {
		// Deterministic ids: a fixture regenerated with fresh UUIDs would be a
		// fixture whose diff is always the whole file.
		var rid id.ID
		rid[15] = byte(i + 1)
		rid[6] = 0x40 // a v4 id, which is what an import from Rust Remem carries

		writeRecord(t, kv, c, repo, &record.Record{
			ID: rid, Tenant: legacyTenant, Namespace: tenant.DefaultNamespace,
			Type: record.TypeMemory, Content: legacyContent(i), SchemaVersion: 1,
			CreatedAt: at2026(), UpdatedAt: at2026(),
		})
	}
	return kv
}

func legacyContent(i int) string {
	return "a memory written before the migration, number " + string(rune('a'+i%26))
}

func TestGenerateTheLegacyFixture(t *testing.T) {
	if !*updateFixtures {
		t.Skip("pass -update-fixtures to regenerate")
	}
	kv := legacyStore(t)
	defer func() { _ = kv.Close() }()

	if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(fixtureDir, fixtureFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	if err := storage.Dump(ctx(), kv, f); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", filepath.Join(fixtureDir, fixtureFile))
}

func loadFixture(t *testing.T) storage.KV {
	t.Helper()
	f, err := os.Open(filepath.Join(fixtureDir, fixtureFile))
	if err != nil {
		t.Fatalf("the legacy fixture is missing; regenerate it with -update-fixtures: %v", err)
	}
	defer func() { _ = f.Close() }()

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	if err := storage.Restore(ctx(), kv, f); err != nil {
		t.Fatal(err)
	}
	return kv
}

// The fixture is what it claims to be. Without this, a regenerated fixture
// could quietly become a current-format one, and the migration test below would
// pass by migrating nothing.
func TestTheLegacyFixtureIsBehind(t *testing.T) {
	kv := loadFixture(t)

	f, err := schema.ReadFormat(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Subsystems["attr_schema"].Current; got != 1 {
		t.Fatalf("the fixture is at attr_schema %d; it is supposed to be behind", got)
	}
	if !f.NeedsMigration(migratedTo(2)) {
		t.Fatal("the fixture does not need migrating, so the migration test would prove nothing")
	}
}

// The plan's completion criterion: a legacy-format directory migrates forward
// and every record is readable afterwards.
func TestLegacyFixtureMigratesForward(t *testing.T) {
	kv := loadFixture(t)
	dir := t.TempDir()
	c := tenant.NewContext(ctx(), legacyTenant)

	// The fixture predates the text index's format version, and this test is
	// about one step on attr_schema. Holding text_index where the fixture is
	// keeps the plan to that step; TestAnUpgradeQueuesOneKeywordRebuildPerTenant
	// is the text index's.
	target := migratedTo(2)
	target.TextIndex = 0

	// It opens before it is migrated — older is accepted, and moving it forward
	// is the runner's job.
	format, err := schema.Open(ctx(), kv, target)
	if err != nil {
		t.Fatalf("an older directory must open: %v", err)
	}

	// A step that rewrites every record body, which is the shape of a real
	// format migration and the shape that needs a backup.
	stamp := schema.Migration{
		ID:           "stamp-every-record",
		Requires:     map[string]uint32{"attr_schema": 1},
		Advances:     map[string]uint32{"attr_schema": 2},
		RewritesData: true,
		Strategy:     schema.StrategyBackground,
		Run:          stampEveryRecord,
	}
	registry, err := schema.NewRegistry(target, stamp)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Plan(format, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 1 {
		t.Fatalf("the plan has %d steps", len(plan))
	}
	if err := newRunner(t, kv, dir).Run(ctx(), plan); err != nil {
		t.Fatal(err)
	}

	// Every record is readable, and carries what the migration did to it.
	repo := record.NewRepo(kv)
	for i := 0; i < legacyRecords; i++ {
		var rid id.ID
		rid[15] = byte(i + 1)
		rid[6] = 0x40

		got, err := repo.Get(c, rid)
		if err != nil {
			t.Fatalf("record %s is not readable after the migration: %v", rid, err)
		}
		if got.Content != legacyContent(i) {
			t.Fatalf("record %s: the migration changed its content to %q", rid, got.Content)
		}
		if len(got.Fields.Tags) != 1 || got.Fields.Tags[0] != "migrated" {
			t.Fatalf("record %s was not migrated: tags %v", rid, got.Fields.Tags)
		}
	}

	// The tenant came through untouched: a storage format migration is not a
	// user-schema migration.
	meta, err := tenantkv.New(kv, clock.NewFake(at2026())).Get(ctx(), legacyTenant)
	if err != nil {
		t.Fatal(err)
	}
	if meta.DisplayName != "Acme" || meta.SchemaVersion != 1 {
		t.Fatalf("the tenant row changed: %+v", meta)
	}

	after, err := schema.ReadFormat(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if after.NeedsMigration(target) {
		t.Fatalf("the directory still needs migrating: %s", after)
	}
	if !backupExists(t, dir) {
		t.Fatal("a step that rewrites every record ran without a backup")
	}
}

// stampEveryRecord walks the corpus, tags each record, and checkpoints — the
// shape every background migration has.
func stampEveryRecord(c context.Context, mc *schema.Context) error {
	scoped := tenant.NewContext(c, legacyTenant)
	repo := record.NewRepo(mc.KV)

	snap := mc.KV.NewSnapshot()
	defer func() { _ = snap.Close() }()

	processed := mc.Done
	var from *id.ID
	if mc.Resume != nil {
		var rid id.ID
		copy(rid[:], mc.Resume)
		from = &rid
	}

	for {
		batch, err := repo.Scan(scoped, snap, from, 8)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, rec := range batch {
			rec.Fields.Tags = append(rec.Fields.Tags, "migrated")

			tx := txn.New(mc.KV)
			if err := repo.Put(scoped, tx, rec); err != nil {
				tx.Close()
				return err
			}
			processed++
			cursor := rec.ID
			if err := mc.Checkpoint(c, tx, cursor[:], processed); err != nil {
				tx.Close()
				return err
			}
			tx.Close()
			from = &cursor
		}
	}
}
