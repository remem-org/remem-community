package main

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"google.golang.org/protobuf/proto"
)

// seedCorpus builds a small data directory the way a server would have written
// it, and returns its path.
func seedCorpus(t *testing.T) (dir string, ids []id.ID) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "data")
	ctx := tenant.NewContext(context.Background(), "acme")

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(text.New()))
	if _, err := tenant.Ensure(ctx, tenantkv.New(kv, clock.System()), "acme"); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		ids = append(ids, writeEmbeddedMemory(t, ctx, kv, repo,
			"a memory about invoices and payments number "+string(rune('a'+i))))
	}
	edges := graph.NewService(kv, clock.System())
	if err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		return edges.Add(ctx, tx, graph.Edge{
			From: ids[0], To: ids[1], Type: graph.RelatedTo, Strength: 0.9,
			Meta: map[string]string{"why": "by hand"},
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, ids
}

func writeEmbeddedMemory(t *testing.T, ctx context.Context, kv storage.KV, repo record.Repo,
	content string,
) id.ID {
	t.Helper()
	return writeEmbeddedMemoryAs(t, ctx, kv, repo, "acme", content)
}

// writeEmbeddedMemoryAs is the same write for a named tenant, which the
// multi-tenant migration rehearsal needs.
func writeEmbeddedMemoryAs(t *testing.T, ctx context.Context, kv storage.KV, repo record.Repo,
	tid tenant.ID, content string,
) id.ID {
	t.Helper()
	rid := id.New()
	values := make([]float32, embedding.Dim)
	for i := range values {
		values[i] = float32(rid[i%len(rid)]) / 255
	}
	rec := &record.Record{
		ID: rid, Tenant: tid, Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: content,
		Fields: record.Fields{Tags: []string{"invoices"}}.WithDefaults(),
		Vectors: map[string]*vector.Vector{record.VectorContent: {
			ModelID: embedding.Model, Dim: embedding.Dim,
			Values: embedding.Normalise(values),
		}},
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		UpdatedAt: time.Unix(1700000000, 0).UTC(),
	}
	if err := txn.Do(ctx, kv, func(tx txn.Tx) error { return repo.Put(ctx, tx, rec) }); err != nil {
		t.Fatalf("writing a memory: %v", err)
	}
	return rid
}

// The whole operator loop, in the order docs/MIGRATION.md gives it: export from
// one directory, import into another, verify the second against the file.
func TestExportImportVerifyRoundTrip(t *testing.T) {
	src, ids := seedCorpus(t)
	out := filepath.Join(t.TempDir(), "corpus.rsnap")
	dst := filepath.Join(t.TempDir(), "restored")

	if _, err := run([]string{"export", "--data-dir", src, "--out", out}); err != nil {
		t.Fatalf("export: %v", err)
	}
	if fi, err := os.Stat(out); err != nil || fi.Size() == 0 {
		t.Fatalf("the export wrote nothing: %v", err)
	}

	// --vectors=verbatim, because this corpus's embeddings already claim the
	// running model and remem-admin in a plain build has no model to recompute
	// with. A Rust snapshot would refuse this and say why.
	if _, err := run([]string{"import", "--data-dir", dst, "--in", out,
		"--vectors", "verbatim"}); err != nil {
		t.Fatalf("import: %v", err)
	}

	code, err := run([]string{"verify", "--data-dir", dst, "--in", out})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if code != 0 {
		t.Fatalf("verify exited %d on a faithful import", code)
	}

	// The memories are there, and so are the derived rows the import rebuilt.
	kv, err := pebble.Open(dst, pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	ctx := tenant.NewContext(context.Background(), "acme")
	repo := record.NewRepo(kv)
	for _, rid := range ids {
		if _, err := repo.Get(ctx, rid); err != nil {
			t.Fatalf("memory %s did not survive: %v", rid, err)
		}
	}
	for _, space := range []keys.Space{keys.SpaceEdgeIn, keys.SpaceAttrRow, keys.SpaceText} {
		lower, upper := keys.SpaceRange("acme", tenant.DefaultNamespace, space)
		it := kv.NewIterator(lower, upper)
		empty := !it.First()
		_ = it.Close()
		if empty {
			t.Fatalf("the import built no %s rows", space)
		}
	}
}

// verify exits 2 when the store and the snapshot disagree: zero on a corpus
// that does not match would make it useless as a check, and a non-zero error
// would be indistinguishable from a command that could not run.
func TestVerifyExitsTwoOnDisagreement(t *testing.T) {
	src, ids := seedCorpus(t)
	out := filepath.Join(t.TempDir(), "corpus.rsnap")
	if _, err := run([]string{"export", "--data-dir", src, "--out", out}); err != nil {
		t.Fatalf("export: %v", err)
	}

	// Delete one memory from the store the snapshot was taken from.
	kv, err := pebble.Open(src, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := tenant.NewContext(context.Background(), "acme")
	repo := record.NewRepo(kv)
	if err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		return repo.Delete(ctx, tx, ids[0])
	}); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	code, err := run([]string{"verify", "--data-dir", src, "--in", out})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if code != 2 {
		t.Fatalf("verify exited %d on a store missing a memory, want 2", code)
	}
}

// A shared-only export is what any implementation compiling proto/snapshot/v1
// can read, and it says what that cost.
func TestSharedOnlyExportReportsWhatItDropped(t *testing.T) {
	src, _ := seedCorpus(t)
	out := filepath.Join(t.TempDir(), "shared.rsnap")

	stdout := captureStdout(t, func() {
		if _, err := run([]string{"export", "--data-dir", src, "--out", out,
			"--shared-only"}); err != nil {
			t.Fatalf("export: %v", err)
		}
	})
	if !strings.Contains(stdout, "metadata on 1 connection(s)") {
		t.Fatalf("the export did not say what it dropped:\n%s", stdout)
	}
}

// A mistyped --data-dir on a read-only command must be refused rather than
// turned into a new empty database that exports cleanly and reports no tenants.
func TestExportRefusesADirectoryWithNoDatabase(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "typo")
	out := filepath.Join(t.TempDir(), "x.rsnap")
	if _, err := run([]string{"export", "--data-dir", missing, "--out", out}); err == nil {
		t.Fatal("a path holding no database was accepted")
	}
	if _, err := pebble.Open(missing, pebble.Options{ReadOnly: true}); err == nil {
		t.Fatal("the refused path now holds a database")
	}
}

// An import into a path that exists and is not a Remem data directory is
// refused. Importing into a *new* path is the ordinary case and is allowed,
// which is what makes this different from every other command here.
func TestImportRefusesAPathThatIsNotADataDirectory(t *testing.T) {
	src, _ := seedCorpus(t)
	out := filepath.Join(t.TempDir(), "corpus.rsnap")
	if _, err := run([]string{"export", "--data-dir", src, "--out", out}); err != nil {
		t.Fatalf("export: %v", err)
	}

	occupied := t.TempDir()
	if err := os.WriteFile(filepath.Join(occupied, "notes.txt"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := run([]string{"import", "--data-dir", occupied, "--in", out, "--vectors", "verbatim"})
	if err == nil {
		t.Fatal("an import into an occupied directory was accepted")
	}
	if !strings.Contains(err.Error(), "does not hold a Remem data directory") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}

func TestSnapshotCommandsNeedTheirArguments(t *testing.T) {
	for _, args := range [][]string{
		{"export", "--data-dir", "x"},
		{"export", "--out", "x"},
		{"import", "--data-dir", "x"},
		{"import", "--in", "x"},
		{"verify", "--data-dir", "x"},
		{"rebuild", "--data-dir", "x"},
	} {
		if _, err := run(args); err == nil {
			t.Fatalf("%v ran without its arguments", args)
		}
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			sb.Write(buf[:n])
			if err != nil {
				break
			}
		}
		done <- sb.String()
	}()
	fn()
	_ = w.Close()
	os.Stdout = old
	return <-done
}

// A command that refuses should leave no trace of having tried. Importing a
// foreign snapshot verbatim is refused before the destination is opened, so a
// mistyped flag does not leave an empty database behind — found by running the
// command, where the library-level refusal fired after the directory was open.
func TestARefusedImportLeavesNoDatabase(t *testing.T) {
	src, _ := seedCorpus(t)
	out := filepath.Join(t.TempDir(), "corpus.rsnap")
	if _, err := run([]string{"export", "--data-dir", src, "--out", out}); err != nil {
		t.Fatalf("export: %v", err)
	}
	rustLike := rewriteSourceImpl(t, out, "rust")

	dst := filepath.Join(t.TempDir(), "restored")
	_, err := run([]string{"import", "--data-dir", dst, "--in", rustLike, "--vectors", "verbatim"})
	if err == nil {
		t.Fatal("a foreign snapshot was imported verbatim")
	}
	if !strings.Contains(err.Error(), "wrong space") {
		t.Fatalf("the refusal does not say why: %v", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("the refused import created %s", dst)
	}
}

// rewriteSourceImpl rewrites a snapshot's header to claim another
// implementation wrote it, leaving every block untouched.
func rewriteSourceImpl(t *testing.T, path, impl string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	hlen := int(binary.LittleEndian.Uint32(raw[12:]))
	var h pb.Header
	if err := proto.Unmarshal(raw[16:16+hlen], &h); err != nil {
		t.Fatal(err)
	}
	h.SourceImpl = impl
	rewritten, err := proto.Marshal(&h)
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(nil), raw[:12]...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(rewritten)))
	out = append(out, rewritten...)
	out = append(out, raw[16+hlen:]...)

	dst := path + ".rust"
	if err := os.WriteFile(dst, out, 0o600); err != nil {
		t.Fatal(err)
	}
	return dst
}

// An import queues the index rebuild rather than doing it, the way a degraded
// index has since Phase 9. Building it here would make an import pay for work
// the server can do in the background, on a binary that may not even be the one
// that will serve.
func TestImportQueuesAVectorRebuild(t *testing.T) {
	src, _ := seedCorpus(t)
	out := filepath.Join(t.TempDir(), "corpus.rsnap")
	dst := filepath.Join(t.TempDir(), "restored")

	if _, err := run([]string{"export", "--data-dir", src, "--out", out}); err != nil {
		t.Fatalf("export: %v", err)
	}
	if _, err := run([]string{"import", "--data-dir", dst, "--in", out,
		"--vectors", "verbatim"}); err != nil {
		t.Fatalf("import: %v", err)
	}

	kv, err := pebble.Open(dst, pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()

	queued, err := jobs.NewQueue(kv, clock.System()).
		List(context.Background(), "acme", jobs.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, j := range queued {
		if j.Type == "vector.rebuild" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no vector.rebuild job was queued; the queue holds %d job(s)", len(queued))
	}
}
