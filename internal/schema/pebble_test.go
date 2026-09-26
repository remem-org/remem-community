package schema_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	pebblekv "github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/version"
)

// The manifest is the first thing written to a real directory and the first
// thing read back from one, so it is worth exercising against the store that
// actually persists rather than only against memkv. A manifest that survives a
// close and reopen is the property the whole gate rests on.
func TestManifestSurvivesACloseAndReopen(t *testing.T) {
	dir := t.TempDir()

	kv, err := pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := schema.Open(ctx(), kv, version.Current())
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	second, err := schema.Open(ctx(), reopened, version.Current())
	if err != nil {
		t.Fatalf("reopening a directory this binary just stamped failed: %v", err)
	}
	for _, name := range version.FormatNames() {
		if first.Subsystems[name] != second.Subsystems[name] {
			t.Errorf("%s changed across a restart: %v then %v",
				name, first.Subsystems[name], second.Subsystems[name])
		}
	}
	if second.NeedsMigration(version.Current()) {
		t.Fatal("a directory this binary stamped must not immediately need migrating")
	}
}

// A pre-migration backup exists to be restored, so the test that matters is the
// one that reopens it. This is also where the plan's "port Rust's recursive
// copy verbatim" would have failed silently: a file-by-file copy of an open
// Pebble directory captures the LOCK file and a WAL mid-write, and the copy may
// refuse to open — which nothing short of reopening it would notice.
func TestAPebbleBackupReopensAndHoldsThePreMigrationRecords(t *testing.T) {
	dir := t.TempDir()

	kv, err := pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := schema.Open(ctx(), kv, version.Current()); err != nil {
		t.Fatal(err)
	}
	key := keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, [16]byte{7})
	if err := kv.Set(ctx(), key, []byte("a memory")); err != nil {
		t.Fatal(err)
	}

	dest, err := schema.BackUp(ctx(), kv, dir, at2026())
	if err != nil {
		t.Fatal(err)
	}
	if dest == "" {
		t.Fatal("a directory holding a record must be backed up")
	}

	// Destroy the original the way a migration that went wrong would, while the
	// store is still open — the state the backup has to survive.
	if err := kv.Delete(ctx(), key); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	restored, err := pebblekv.Open(dest, pebblekv.Options{})
	if err != nil {
		t.Fatalf("the backup will not open, which is the one failure a pre-migration backup cannot have: %v", err)
	}
	defer func() { _ = restored.Close() }()

	got, err := restored.Get(ctx(), key)
	if err != nil {
		t.Fatalf("the record is not in the backup: %v", err)
	}
	if string(got) != "a memory" {
		t.Fatalf("the backup holds %q", got)
	}
	// And the manifest came with it, so the restored directory passes the gate.
	if _, err := schema.Open(ctx(), restored, version.Current()); err != nil {
		t.Fatalf("the restored directory does not pass the format gate: %v", err)
	}
}

// The backup lives inside the data directory, and Pebble must not mistake it
// for its own. A directory that will not reopen after a backup was taken into
// it is a directory an upgrade has bricked.
func TestPebbleReopensTheDirectoryABackupWasTakenInto(t *testing.T) {
	dir := t.TempDir()

	kv, err := pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := schema.Open(ctx(), kv, version.Current()); err != nil {
		t.Fatal(err)
	}
	key := keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, [16]byte{7})
	if err := kv.Set(ctx(), key, []byte("a memory")); err != nil {
		t.Fatal(err)
	}
	if _, err := schema.BackUp(ctx(), kv, dir, at2026()); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		t.Fatalf("the data directory will not reopen after a backup was taken into it: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.Get(ctx(), key); err != nil {
		t.Fatal(err)
	}
}
