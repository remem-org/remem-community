package schema_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/version"
)

func at2026() time.Time { return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) }

// userRow is a key outside the system space — what "this directory holds data
// worth backing up" means. The manifest and the tenant directory are not it: a
// fresh directory has both and has nothing to lose.
func userRow(t *testing.T, kv storage.KV) {
	t.Helper()
	if err := kv.Set(ctx(), keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, [16]byte{1}), []byte("a memory")); err != nil {
		t.Fatal(err)
	}
}

func TestBackupWritesIntoTheDataDirectory(t *testing.T) {
	dir := t.TempDir()
	kv := newKV(t)
	userRow(t, kv)

	dest, err := schema.BackUp(ctx(), kv, dir, at2026())
	if err != nil {
		t.Fatal(err)
	}
	if dest == "" {
		t.Fatal("a directory holding data must be backed up")
	}

	// Inside data_dir, not beside it: a sibling path is not guaranteed to land
	// on the same volume when data_dir is a bind mount, and a backup on the
	// container's writable overlay is gone with the container.
	if !strings.HasPrefix(dest, filepath.Join(dir, ".backups")+string(filepath.Separator)) {
		t.Fatalf("the backup went to %s, which is not inside %s/.backups", dest, dir)
	}
	if _, err := os.Stat(filepath.Join(dest, memkv.DumpFile)); err != nil {
		t.Fatalf("the backup is empty: %v", err)
	}
}

// The name carries the moment it was taken, from the injected clock rather than
// from time.Now: a backup nobody can date is a backup nobody will trust enough
// to delete.
func TestBackupIsNamedForTheMomentItWasTaken(t *testing.T) {
	dir := t.TempDir()
	kv := newKV(t)
	userRow(t, kv)

	dest, err := schema.BackUp(ctx(), kv, dir, at2026())
	if err != nil {
		t.Fatal(err)
	}
	want := "pre-" + strconv.FormatInt(at2026().Unix(), 10)
	if filepath.Base(dest) != want {
		t.Fatalf("got %s, want %s", filepath.Base(dest), want)
	}
}

func TestBackupIsSkippedWhenOptedOut(t *testing.T) {
	t.Setenv(schema.SkipBackupEnv, "1")

	dir := t.TempDir()
	kv := newKV(t)
	userRow(t, kv)

	dest, err := schema.BackUp(ctx(), kv, dir, at2026())
	if err != nil {
		t.Fatal(err)
	}
	if dest != "" {
		t.Fatalf("the opt-out was set and a backup was taken anyway: %s", dest)
	}
	if _, err := os.Stat(filepath.Join(dir, ".backups")); !os.IsNotExist(err) {
		t.Fatal("the backups directory was created despite the opt-out")
	}
}

// A directory with a manifest and a tenant row and nothing else has nothing to
// lose. Backing it up on every fresh install would leave a pile of empty
// directories nobody ever deletes, since backups are never auto-deleted.
func TestNothingIsBackedUpWhenThereIsNoUserData(t *testing.T) {
	dir := t.TempDir()
	kv := newKV(t)
	if _, err := schema.Open(ctx(), kv, version.Current()); err != nil {
		t.Fatal(err)
	}

	dest, err := schema.BackUp(ctx(), kv, dir, at2026())
	if err != nil {
		t.Fatal(err)
	}
	if dest != "" {
		t.Fatalf("an empty directory was backed up to %s", dest)
	}
}

// The whole point of a backup is that it can be restored. A copy nobody has
// reopened is a copy nobody has verified.
func TestABackupCanBeRestored(t *testing.T) {
	dir := t.TempDir()
	kv := newKV(t)
	userRow(t, kv)
	key := keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, [16]byte{1})

	dest, err := schema.BackUp(ctx(), kv, dir, at2026())
	if err != nil {
		t.Fatal(err)
	}

	// Wreck the original the way a migration would.
	if err := kv.Delete(ctx(), key); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(filepath.Join(dest, memkv.DumpFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	restored := memkv.New()
	defer func() { _ = restored.Close() }()
	if err := storage.Restore(ctx(), restored, f); err != nil {
		t.Fatal(err)
	}
	got, err := restored.Get(ctx(), key)
	if err != nil {
		t.Fatalf("the record is not in the backup: %v", err)
	}
	if string(got) != "a memory" {
		t.Fatalf("the backup holds %q", got)
	}
}

// A store that cannot copy itself consistently is a refusal, not a silent skip.
// Proceeding would rewrite data having told an operator it was protected.
func TestAStoreThatCannotBackUpIsRefused(t *testing.T) {
	dir := t.TempDir()
	kv := newKV(t)
	userRow(t, kv)

	_, err := schema.BackUp(ctx(), noBackup{kv}, dir, at2026())
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), schema.SkipBackupEnv) {
		t.Fatalf("the error must name the way out of it: %v", err)
	}
}

func TestBackupFailureLeavesTheDataUntouched(t *testing.T) {
	dir := t.TempDir()
	kv := newKV(t)
	userRow(t, kv)
	key := keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, [16]byte{1})

	// A file where the backups directory has to go, so the copy cannot start.
	if err := os.WriteFile(filepath.Join(dir, ".backups"), []byte("in the way"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := schema.BackUp(ctx(), kv, dir, at2026()); err == nil {
		t.Fatal("an unwritable backup destination reported success")
	}
	got, err := kv.Get(ctx(), key)
	if err != nil || string(got) != "a memory" {
		t.Fatalf("the data did not survive a failed backup: %q %v", got, err)
	}
}

// noBackup is a KV that is otherwise ordinary and cannot copy itself.
type noBackup struct{ storage.KV }
