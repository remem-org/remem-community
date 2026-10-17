package schema_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/version"
)

// The re-embedding migration (openspec/changes/passage-vectors, design decision
// 9): a directory written under all-MiniLM-L6-v2 holds 384-wide vectors of the
// first 128 tokens of each memory. This build serves 1,024-wide contextual
// passages, so every memory is embedded again, whole, before the listener binds.

// recordingVectorReindexer records which tenants a vector rebuild was queued for.
type recordingVectorReindexer struct{ queued []tenant.ID }

func (r *recordingVectorReindexer) StageVectorRebuild(_ context.Context, _ txn.Tx, t tenant.ID) error {
	r.queued = append(r.queued, t)
	return nil
}

// failingAfter is an embedder that fails after n successful calls, to interrupt
// a migration part-way.
type failingAfter struct {
	*embeddingtest.Fake
	left int
}

func (f *failingAfter) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	if f.left == 0 {
		return nil, errors.New("killed")
	}
	f.left--
	return f.Fake.EmbedDocuments(ctx, texts)
}

var legacyTenants = []tenant.ID{"acme", "globex"}

// legacyDirectory writes what a MiniLM-era build left behind: per tenant, n
// memories with 384-wide canonical vectors stamped all-MiniLM-L6-v2 (every
// third long enough for several passages under the new embedder), node records
// of an HNSW graph over them, and a manifest that has never heard of passages.
func legacyDirectory(t *testing.T, kv storage.KV, n int) (tenant.Directory, *clock.Fake) {
	t.Helper()
	ctx := context.Background()
	clk := clock.NewFake(clock.FakeStart)
	dir := tenantkv.New(kv, clk)
	repo := record.NewRepo(kv)
	for ti, tn := range legacyTenants {
		tctx := tenant.NewContext(ctx, tn)
		if _, err := tenant.Ensure(tctx, dir, tn); err != nil {
			t.Fatal(err)
		}
		for i := range n {
			content := fmt.Sprintf("memory %d of tenant %s about the harbour", i, tn)
			if i%3 == 0 {
				content = strings.Repeat(content+" and the tide tables ", 12)
			}
			old := make([]float32, 384)
			old[(i+ti)%384] = 1
			rec := &record.Record{
				ID: id.New(), Tenant: tn, Type: record.TypeMemory, Content: content,
				Fields:    record.Fields{Policy: "short_term", Health: 100, Importance: 0.5},
				Vectors:   map[string]*record.Vector{record.VectorContent: {ModelID: "all-MiniLM-L6-v2", Dim: 384, Values: old}},
				CreatedAt: time.Unix(1, 0), UpdatedAt: time.Unix(1, 0),
			}
			if err := txn.Do(tctx, kv, func(tx txn.Tx) error { return repo.Put(tctx, tx, rec) }); err != nil {
				t.Fatal(err)
			}
			// A node record of the old graph. Its neighbour lists were built in
			// MiniLM's space and mean nothing in the new one.
			if err := kv.Set(ctx, keys.VectorIndex(tn, tenant.DefaultNamespace, uint64(i+1)), []byte{1}); err != nil {
				t.Fatal(err)
			}
		}
	}
	f := schema.CurrentFormat(version.Current())
	delete(f.Subsystems, "passage_index")
	if err := schema.WriteFormat(ctx, kv, f); err != nil {
		t.Fatal(err)
	}
	return dir, clk
}

func runMigrations(t *testing.T, kv storage.KV, dir tenant.Directory, clk clock.Clock, dataDir string,
	metrics *obs.Metrics, opts ...schema.BuiltinOption) error {
	t.Helper()
	ctx := context.Background()
	format, err := schema.ReadFormat(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := schema.Builtin(version.Current(), nil, nil, opts...)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reg.Plan(format, version.Current())
	if err != nil {
		t.Fatal(err)
	}
	runner, err := schema.NewRunner(schema.RunnerConfig{KV: kv, Tenants: dir, Clock: clk, DataDir: dataDir, Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	return runner.Run(ctx, plan)
}

// vectorSpaces is every canonical vector and passage row of every tenant, as
// bytes, for comparing two directories.
func vectorSpaces(t *testing.T, kv storage.KV) [][]byte {
	t.Helper()
	var out [][]byte
	for _, tn := range legacyTenants {
		for _, s := range []keys.Space{keys.SpaceVector, keys.SpacePassage} {
			lo, hi := keys.SpaceRange(tn, tenant.DefaultNamespace, s)
			it := kv.NewIterator(lo, hi)
			for ok := it.First(); ok; ok = it.Next() {
				out = append(out, append(bytes.Clone(it.Key()), it.Value()...))
			}
			_ = it.Close()
		}
	}
	return out
}

func TestTheReembeddingMigrationRewritesEveryMemory(t *testing.T) {
	t.Setenv(schema.SkipBackupEnv, "1")
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	const perTenant = 9
	dir, clk := legacyDirectory(t, kv, perTenant)
	vectors := &recordingVectorReindexer{}
	metrics := obs.NewMetrics()

	if err := runMigrations(t, kv, dir, clk, t.TempDir(), metrics,
		schema.WithPassages(embeddingtest.New(), vectors)); err != nil {
		t.Fatalf("the migration: %v", err)
	}

	store := vector.NewStore(kv)
	for _, tn := range legacyTenants {
		canonical, passages := 0, map[id.ID]int{}
		if err := store.ScanEntries(ctx, tn, func(k vector.Key, v *vector.Vector) error {
			if v.ModelID != embedding.Model || v.Dim != embedding.Dim {
				t.Fatalf("tenant %s: entry %v is %q/%d after the migration", tn, k, v.ModelID, v.Dim)
			}
			passages[k.ID]++
			canonical++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		long := 0
		for _, n := range passages {
			if n > 1 {
				long++
			}
		}
		if len(passages) != perTenant || long != perTenant/3 {
			t.Fatalf("tenant %s: %d memories indexed, %d by passages; want %d and %d",
				tn, len(passages), long, perTenant, perTenant/3)
		}
		lo, hi := keys.SpaceRange(tn, tenant.DefaultNamespace, keys.SpaceVectorIndex)
		it := kv.NewIterator(lo, hi)
		if it.First() {
			t.Fatalf("tenant %s still holds node records built in the old space", tn)
		}
		_ = it.Close()
	}
	if !slices.Equal(vectors.queued, legacyTenants) {
		t.Fatalf("vector rebuilds queued for %v, want each tenant once", vectors.queued)
	}

	format, err := schema.ReadFormat(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if got := format.Subsystems["passage_index"].Current; got != version.PassageIndex {
		t.Fatalf("passage_index is %d after the migration, want %d", got, version.PassageIndex)
	}
	st, found, err := schema.ReadState(ctx, kv, schema.PassageReembedID)
	if err != nil || !found {
		t.Fatalf("no state for the migration: %v", err)
	}
	if st.Processed != uint64(perTenant*len(legacyTenants)) {
		t.Fatalf("the migration processed %d, want %d", st.Processed, perTenant*len(legacyTenants))
	}
	// A step reports progress only when it declared how much there was to do,
	// and a step that blocks start-up for hours had better report it.
	if ratio, ok := recorded(t, metrics, "remem_migration_progress_ratio", schema.PassageReembedID); !ok || ratio != 1 {
		t.Fatalf("progress ratio %v (recorded %v): the step did not declare its total", ratio, ok)
	}
}

// Killed part-way, it resumes, and ends with exactly the rows an uninterrupted
// run writes.
func TestAnInterruptedReembeddingResumesToTheSameResult(t *testing.T) {
	t.Setenv(schema.SkipBackupEnv, "1")
	const perTenant = 40
	whole := memkv.New()
	t.Cleanup(func() { _ = whole.Close() })
	wdir, wclk := legacyDirectory(t, whole, perTenant)

	// The same directory twice: ids are drawn once and copied, so the two runs
	// meet the same records.
	broken := memkv.New()
	t.Cleanup(func() { _ = broken.Close() })
	copyAll(t, whole, broken)
	bdir := tenantkv.New(broken, wclk)

	if err := runMigrations(t, whole, wdir, wclk, t.TempDir(), nil,
		schema.WithPassages(embeddingtest.New(), &recordingVectorReindexer{})); err != nil {
		t.Fatal(err)
	}

	killer := &failingAfter{Fake: embeddingtest.New(), left: 2}
	if err := runMigrations(t, broken, bdir, wclk, t.TempDir(), nil,
		schema.WithPassages(killer, &recordingVectorReindexer{})); err == nil {
		t.Fatal("the interrupted run reported success")
	}
	resumed := embeddingtest.New()
	if err := runMigrations(t, broken, bdir, wclk, t.TempDir(), nil,
		schema.WithPassages(resumed, &recordingVectorReindexer{})); err != nil {
		t.Fatalf("the resumed run: %v", err)
	}
	if resumed.Texts() >= 2*perTenant {
		t.Fatalf("the resumed run embedded %d of %d memories: it started again rather than resuming",
			resumed.Texts(), 2*perTenant)
	}
	want, got := vectorSpaces(t, whole), vectorSpaces(t, broken)
	if len(got) != len(want) {
		t.Fatalf("%d vector rows after resuming, %d after an uninterrupted run", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("vector row %d differs between the resumed and the uninterrupted run", i)
		}
	}
}

// A build without the model cannot re-embed, and says so by name rather than
// serving a corpus of another model's vectors.
func TestABuildWithoutTheModelRefusesTheDirectoryByName(t *testing.T) {
	t.Setenv(schema.SkipBackupEnv, "1")
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	dir, clk := legacyDirectory(t, kv, 3)
	err := runMigrations(t, kv, dir, clk, t.TempDir(), nil,
		schema.WithPassages(nil, &recordingVectorReindexer{}))
	if err == nil {
		t.Fatal("a build without the model ran the re-embedding")
	}
	for _, want := range []string{"re-embed", "onnx"} {
		if !strings.Contains(strings.ToLower(err.Error()), want) {
			t.Fatalf("the refusal does not say %q: %v", want, err)
		}
	}
}

// A server whose vectors come from its callers has no model to derive anything
// from: the step advances and its vectors are left exactly as they are.
func TestAnExternalModeDirectoryAdvancesWithNoWork(t *testing.T) {
	t.Setenv(schema.SkipBackupEnv, "1")
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	dir, clk := legacyDirectory(t, kv, 3)
	before := vectorSpaces(t, kv)
	vectors := &recordingVectorReindexer{}
	if err := runMigrations(t, kv, dir, clk, t.TempDir(), nil,
		schema.WithPassages(embedding.NewExternal("all-MiniLM-L6-v2", 384), vectors)); err != nil {
		t.Fatal(err)
	}
	after := vectorSpaces(t, kv)
	if len(after) != len(before) {
		t.Fatalf("an external-mode directory's vectors changed: %d rows, was %d", len(after), len(before))
	}
	for i := range before {
		if !bytes.Equal(after[i], before[i]) {
			t.Fatal("an external-mode directory's vectors changed")
		}
	}
	if len(vectors.queued) != 0 {
		t.Fatalf("an external-mode directory queued rebuilds for %v", vectors.queued)
	}
	format, err := schema.ReadFormat(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if got := format.Subsystems["passage_index"].Current; got != version.PassageIndex {
		t.Fatalf("passage_index is %d, want %d", got, version.PassageIndex)
	}
}

// The step rewrites canonical vectors, so the runner takes the pre-migration
// backup first: the way back is the old binary on that checkpoint.
func TestTheReembeddingTakesTheBackupFirst(t *testing.T) {
	dataDir := t.TempDir()
	kv, err := pebble.Open(filepath.Join(dataDir, "db"), pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	dir, clk := legacyDirectory(t, kv, 3)
	if err := runMigrations(t, kv, dir, clk, dataDir, nil,
		schema.WithPassages(embeddingtest.New(), &recordingVectorReindexer{})); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(dataDir, ".backups"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("no pre-migration backup was taken: %v %v", entries, err)
	}

	reg, err := schema.Builtin(version.Current(), nil, nil,
		schema.WithPassages(embeddingtest.New(), &recordingVectorReindexer{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range reg.All() {
		if m.ID == schema.PassageReembedID && (!m.RewritesData || m.Strategy != schema.StrategyRebuild) {
			t.Fatalf("the step rewrites data %v with strategy %s; it must take the backup and block start-up",
				m.RewritesData, m.Strategy)
		}
	}
}

func copyAll(t *testing.T, from, to storage.KV) {
	t.Helper()
	it := from.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		if err := to.Set(context.Background(), bytes.Clone(it.Key()), bytes.Clone(it.Value())); err != nil {
			t.Fatal(err)
		}
	}
}
