package main

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
)

// secretContent is what a memory says. inspect must never print it.
const secretContent = "the vault combination is 31-7-44"

// seedInspectable writes n records for a tenant through a repository that
// maintains attribute rows, which is the consistent state the checker compares
// damage against.
func seedInspectable(t *testing.T, kv storage.KV, name tenant.ID, n int) []id.ID {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), name)
	dir := tenantkv.New(kv, clock.NewFake(clock.FakeStart))
	if _, err := tenant.Ensure(ctx, dir, name); err != nil {
		t.Fatalf("creating tenant %s: %v", name, err)
	}
	repo := record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(attr.MustTable())))
	tx := txn.New(kv)
	defer tx.Close()
	var ids []id.ID
	for range n {
		rid := id.New()
		rec := &record.Record{ID: rid, Tenant: name, Namespace: tenant.DefaultNamespace,
			Type: record.TypeMemory, Content: secretContent, CreatedAt: time.UnixMilli(1_725_000_000_000).UTC()}
		if err := repo.Put(ctx, tx, rec); err != nil {
			t.Fatalf("writing a record: %v", err)
		}
		ids = append(ids, rid)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestInspectCheckIsCleanOnAConsistentCorpus(t *testing.T) {
	kv := memkv.New()
	seedInspectable(t, kv, "acme", 3)
	seedInspectable(t, kv, "globex", 2)

	// The raw directory, because this test is about the walk rather than about
	// which tenants a build may walk — that policy is settled in the command,
	// and single_tenant_test.go covers it.
	dir := tenantkv.New(kv, clock.System())
	reports, err := checkTenants(context.Background(), kv, dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 2 {
		t.Fatalf("checked %d tenants, want both", len(reports))
	}
	for _, r := range reports {
		if !r.Clean() {
			t.Errorf("tenant %s has findings on a consistent corpus: %v", r.Tenant, r.Findings)
		}
	}
}

// The command exits 2 on a damaged directory — neither success, which would
// make it useless as a check, nor failure, which would be indistinguishable from
// one that could not open the directory — and names the row that is wrong.
func TestInspectCheckExitsTwoAndNamesTheDamage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ids := seedInspectable(t, kv, "acme", 3)
	if err := kv.Delete(context.Background(), keys.AttrRow("acme", tenant.DefaultNamespace, ids[1])); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	out, code, err := captureRun(t, "inspect", "check", "--data-dir", dir)
	if err != nil {
		t.Fatalf("inspect check: %v", err)
	}
	if code != exitInconsistent {
		t.Fatalf("inspect check on a damaged directory exited %d, want %d:\n%s", code, exitInconsistent, out)
	}
	if !strings.Contains(out, ids[1].String()) || !strings.Contains(out, "attr_row") {
		t.Fatalf("the report does not name the record and the space:\n%s", out)
	}
	if strings.Contains(out, secretContent) {
		t.Fatalf("inspect check printed memory content:\n%s", out)
	}
}

// Read-only first, like every command here: a mistyped path must be refused,
// not become an empty database that reports a clean corpus.
func TestInspectCheckRefusesADirectoryThatHoldsNoDatabase(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "not-a-data-dir")
	if _, _, err := captureRun(t, "inspect", "check", "--data-dir", missing); err == nil {
		t.Fatal("inspect check accepted a path holding no database")
	}
	if _, err := os.Stat(missing); err == nil {
		t.Fatal("inspect check created a directory at a mistyped path")
	}
}

func TestInspectSpacesCountsEachTenantsKeysBySpace(t *testing.T) {
	kv := memkv.New()
	seedInspectable(t, kv, "acme", 3)

	rows, err := spaceUsage(context.Background(), kv, "acme")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, r := range rows {
		counts[r.Space] = r.Keys
		if r.Keys > 0 && r.Bytes <= 0 {
			t.Errorf("space %s holds %d keys and %d bytes", r.Space, r.Keys, r.Bytes)
		}
	}
	if counts["record"] != 3 || counts["attr_row"] != 3 {
		t.Fatalf("usage counts %v, want 3 records and 3 attribute rows", counts)
	}
}

// A key is described by what its layout says and by how long its value is —
// never by the value, which for a record is somebody's memory.
func TestInspectKeyDescribesARecordWithoutItsContent(t *testing.T) {
	kv := memkv.New()
	ids := seedInspectable(t, kv, "acme", 1)
	key := keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, ids[0])

	desc, err := describeKey(context.Background(), kv, hex.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"acme", "record", ids[0].String(), "value"} {
		if !strings.Contains(desc, want) {
			t.Errorf("the description lacks %q:\n%s", want, desc)
		}
	}
	if strings.Contains(desc, secretContent) {
		t.Fatalf("inspect key printed memory content:\n%s", desc)
	}
}

func TestInspectKeyRefusesInputThatIsNotHex(t *testing.T) {
	if _, err := describeKey(context.Background(), memkv.New(), "not hex at all"); err == nil {
		t.Fatal("a key that is not hex was accepted")
	}
}

// captureRun runs remem-admin with args and returns what it printed to stdout.
func captureRun(t *testing.T, args ...string) (string, int, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	code, runErr := run(args)
	os.Stdout = saved
	_ = w.Close()
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return b.String(), code, runErr
}
