// Package migration imports the Rust reference corpora into a Go store.
//
// It is the one test in the tree that reads what the other implementation
// actually wrote, over a whole corpus. Everything else about the format is
// checked against snapshots Go itself produced, plus the 1.4 KB wire vector in
// internal/snapshot/rustgolden_test.go — which pins the framing but says
// nothing about five thousand records with unicode content, 120-byte tags,
// orphan edges, self-edges and extreme lifecycle values.
//
// # Why it skips
//
// The corpora are not committed. They are 22 MB of derived bytes that
// `make fixtures` reproduces byte for byte from the frozen tree, and the pinned
// digests in scripts/generate-fixtures.sh are what makes that reproduction a
// check. So this suite skips, naming the command, on a machine that has not run
// it — which is most CI runs, and is a limitation stated in
// test/differential/COVERAGE.md rather than left to be discovered.
package migration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
)

// fixture describes one reference corpus and what remem-development's
// docs/FIXTURES.md says it holds at seed 42.
type fixture struct {
	name     string
	records  uint64
	vectors  uint64
	missing  uint64
	edges    uint64
	fileSize int64
}

var fixtures = []fixture{
	{name: "tiny", records: 50, vectors: 50, missing: 0, edges: 120, fileSize: 73380},
	{name: "pathological", records: 5000, vectors: 4998, missing: 2, edges: 8000, fileSize: 7259212},
	{name: "typical", records: 10000, vectors: 10000, missing: 0, edges: 35000, fileSize: 14598093},
}

func pathOf(f fixture) string {
	return filepath.Join("..", "..", "fixtures", "rust", f.name+".rsnap")
}

func requireFixture(t *testing.T, f fixture) string {
	t.Helper()
	path := pathOf(f)
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		t.Skipf("%s is not present; run `make fixtures` to generate the reference corpora", path)
	}
	if err != nil {
		t.Fatal(err)
	}
	// The size is pinned as well as the digest, because a corpus that has
	// quietly drifted would still import cleanly — against the wrong corpus.
	if fi.Size() != f.fileSize {
		t.Fatalf("%s is %d bytes and docs/FIXTURES.md pins %d: the corpus has drifted, or it "+
			"was generated at a seed other than 42", path, fi.Size(), f.fileSize)
	}
	return path
}

// destination is a fresh store wired the way the composition root wires the
// server, so an imported memory gets the derived rows a stored one would.
type destination struct {
	kv   storage.KV
	dst  snapshot.Destination
	repo record.Repo
}

func newDestination(t *testing.T) *destination {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.System()

	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(text.New()))
	return &destination{
		kv:   kv,
		repo: repo,
		dst: snapshot.Destination{
			KV: kv, Tenants: tenantkv.New(kv, clk), Records: repo,
			Edges: graph.NewStore(kv), Events: events.NewStore(kv),
			// The deterministic fake, not the real model: this suite is about
			// whether Rust's bytes become Go's rows, and loading ninety
			// megabytes of weights to find that out would make it a test of
			// the embedder. That the re-embedding produces the *right* vectors
			// is internal/embedding's own agreement test against the 200
			// reference vectors.
			Embedder: embeddingtest.New(),
		},
	}
}

func TestImportAllFixtures(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			path := requireFixture(t, f)
			in, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = in.Close() }()

			d := newDestination(t)
			rep, err := snapshot.Import(context.Background(), d.dst, in, snapshot.ImportOpts{})
			if err != nil {
				t.Fatalf("importing %s: %v", f.name, err)
			}

			if rep.Stats.Records != f.records {
				t.Errorf("read %d records, want %d", rep.Stats.Records, f.records)
			}
			if rep.Stats.Vectors != f.vectors {
				t.Errorf("read %d vectors, want %d", rep.Stats.Vectors, f.vectors)
			}
			if rep.Stats.Edges != f.edges {
				t.Errorf("read %d edges, want %d", rep.Stats.Edges, f.edges)
			}

			// Every record is re-embedded, including the ones the export
			// carried no vector for: Rust retires an archived memory's vector
			// from its index and Go keeps one, so recomputing is the faithful
			// import (§II.10 row 16 for why none of Rust's is usable).
			if rep.ReEmbedded != f.records {
				t.Errorf("re-embedded %d records, want all %d", rep.ReEmbedded, f.records)
			}
			if rep.VectorsVerbatim != 0 {
				t.Errorf("kept %d of Rust's vectors; none is comparable with this model's",
					rep.VectorsVerbatim)
			}
			if rep.Stats.MissingVectors != f.missing {
				t.Errorf("counted %d records with no vector in the file, want %d",
					rep.Stats.MissingVectors, f.missing)
			}

			// And the store holds them.
			if got := rows(t, d.kv, "default", keys.SpaceRecord); got != int(f.records) {
				t.Errorf("the store holds %d records, want %d", got, f.records)
			}
			if got := rows(t, d.kv, "default", keys.SpaceVector); got != int(f.records) {
				t.Errorf("the store holds %d vectors, want %d (one per record)", got, f.records)
			}
		})
	}
}

// The pathological corpus is the one that carries every case a long-lived
// deployment accumulates and the write path cannot produce: unicode with a
// combining mark, a 120-byte tag past the old index's limit, importance at
// exactly 0 and 1, health at 0, flashbulb windows in the past and the future,
// archived records inside and past the cleanup age, a self-edge, and an edge
// whose target was hard-deleted.
func TestPathologicalFixtureImports(t *testing.T) {
	f := fixtures[1]
	path := requireFixture(t, f)
	in, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()

	d := newDestination(t)
	rep, err := snapshot.Import(context.Background(), d.dst, in, snapshot.ImportOpts{})
	if err != nil {
		t.Fatalf("importing the pathological corpus: %v", err)
	}

	if len(rep.SelfEdges) == 0 {
		t.Error("the corpus carries a self-edge and none was rejected")
	}
	if len(rep.OrphanEdges) == 0 {
		t.Error("the corpus carries an edge whose target was deleted and none was reported")
	}
	// Rejected, not merely counted: neither reached the graph.
	for _, r := range rep.SelfEdges {
		if r.From != r.To {
			t.Errorf("a rejected self-edge is not one: %v", r)
		}
	}
	total := rows(t, d.kv, "default", keys.SpaceEdgeOut)
	if want := int(f.edges) - len(rep.SelfEdges) - len(rep.OrphanEdges); total != want {
		t.Errorf("the store holds %d edges; %d carried, %d self, %d orphan", total, f.edges,
			len(rep.SelfEdges), len(rep.OrphanEdges))
	}

	// The extreme values survive as themselves. A health of 0 in particular:
	// reading it as "unset, therefore 100" would silently resurrect a
	// fully-decayed memory (§II.10 row 8).
	var sawZeroHealth, sawUnicode, sawLongTag, sawArchived bool
	ctx := tenant.NewContext(context.Background(), "default")
	snap := d.kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	scanAll(t, ctx, d.repo, snap, func(rec *record.Record) {
		if rec.Fields.Health == 0 {
			sawZeroHealth = true
		}
		if rec.Fields.Archived {
			sawArchived = true
		}
		for _, r := range rec.Content {
			if r > 0x7f {
				sawUnicode = true
				break
			}
		}
		for _, tag := range rec.Fields.Tags {
			if len(tag) >= 120 {
				sawLongTag = true
			}
		}
	})
	if !sawZeroHealth {
		t.Error("no memory came back with health 0; a zero-check default would have hidden it")
	}
	if !sawUnicode {
		t.Error("no memory came back with non-ASCII content")
	}
	if !sawLongTag {
		t.Error("no memory came back with a 120-byte tag")
	}
	if !sawArchived {
		t.Error("no archived memory came back")
	}
}

// Rust exports, Go imports, Go exports: every record, vector and edge survives,
// and the second import over the same file writes nothing new.
func TestRoundTripIsLossless(t *testing.T) {
	f := fixtures[0]
	path := requireFixture(t, f)

	d := newDestination(t)
	first := importFile(t, d, path)

	// Go's own export of the imported corpus, then a fresh import of that.
	out := filepath.Join(t.TempDir(), "go.rsnap")
	exportTo(t, d, out)

	e := newDestination(t)
	second := importFile(t, e, out)

	if second.Stats.Records != first.Stats.Records {
		t.Fatalf("Go's own snapshot holds %d records where Rust's held %d",
			second.Stats.Records, first.Stats.Records)
	}
	if second.Stats.Edges != first.Stats.Edges-uint64(len(first.SelfEdges)+len(first.OrphanEdges)) {
		t.Fatalf("Go's own snapshot holds %d edges; Rust's held %d, of which %d were rejected",
			second.Stats.Edges, first.Stats.Edges,
			len(first.SelfEdges)+len(first.OrphanEdges))
	}
	// Every vector is kept this time: Go wrote them, with this model's id.
	if second.VectorsVerbatim != second.Stats.Vectors || second.ReEmbedded != 0 {
		t.Fatalf("Go's own snapshot was re-embedded: %d kept, %d recomputed of %d",
			second.VectorsVerbatim, second.ReEmbedded, second.Stats.Vectors)
	}

	// And a verification of the second store against the file it came from.
	in, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	snap := e.kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	rep, err := snapshot.Verify(context.Background(), snapshot.Sources{
		Snap: snap, Tenants: e.dst.Tenants, Records: e.repo, Events: events.NewStore(e.kv),
	}, in)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Clean() {
		t.Fatalf("the round trip does not verify: %v", rep.Findings)
	}
}

// A second import of the same file writes nothing new, which is what makes
// restarting a crashed migration safe.
func TestImportingTwiceChangesNothing(t *testing.T) {
	f := fixtures[0]
	path := requireFixture(t, f)

	d := newDestination(t)
	importFile(t, d, path)
	before := rows(t, d.kv, "default", keys.SpaceRecord)
	beforeEdges := rows(t, d.kv, "default", keys.SpaceEdgeOut)

	second := importFile(t, d, path)
	if second.Unchanged != f.records {
		t.Errorf("the second import found %d unchanged records, want %d",
			second.Unchanged, f.records)
	}
	if got := rows(t, d.kv, "default", keys.SpaceRecord); got != before {
		t.Errorf("the second import changed the record count from %d to %d", before, got)
	}
	if got := rows(t, d.kv, "default", keys.SpaceEdgeOut); got != beforeEdges {
		t.Errorf("the second import changed the edge count from %d to %d", beforeEdges, got)
	}
}

func importFile(t *testing.T, d *destination, path string) snapshot.Report {
	t.Helper()
	in, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	rep, err := snapshot.Import(context.Background(), d.dst, in, snapshot.ImportOpts{})
	if err != nil {
		t.Fatalf("importing %s: %v", path, err)
	}
	return rep
}

func exportTo(t *testing.T, d *destination, path string) {
	t.Helper()
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	snap := d.kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	if _, err := snapshot.Export(context.Background(), snapshot.Sources{
		Snap: snap, Tenants: d.dst.Tenants, Records: d.repo, Events: events.NewStore(d.kv),
	}, out, snapshot.ExportOpts{}); err != nil {
		t.Fatalf("exporting: %v", err)
	}
}

func rows(t *testing.T, kv storage.KV, tid tenant.ID, space keys.Space) int {
	t.Helper()
	lower, upper := keys.SpaceRange(tid, tenant.DefaultNamespace, space)
	it := kv.NewIterator(lower, upper)
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	_ = it.Close()
	return n
}

func scanAll(t *testing.T, ctx context.Context, repo record.Repo, snap storage.Snapshot,
	fn func(*record.Record),
) {
	t.Helper()
	var from *id.ID
	for {
		page, err := repo.Scan(ctx, snap, from, 500)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			return
		}
		for _, rec := range page {
			fn(rec)
		}
		last := page[len(page)-1].ID
		from = &last
		if len(page) < 500 {
			return
		}
	}
}

// The parity surface plan §II.10 names as "export/import round-trip fidelity",
// checked where it is actually checkable.
//
// The differential driver cannot do this one. It compares two running servers
// through their APIs, and the question here is whether a *file* Rust wrote
// became the rows Go holds — which needs the file and the store, not two
// servers. So it is snapshot.Verify over a real Rust export, row by row: every
// record hashed over the fields the shared schema carries, every edge matched
// by its whole identity, and the header's own counts checked against its own
// blocks.
//
// What it deliberately does not compare is vector values, because §II.10 row 16
// puts Rust's and Go's in different spaces. Norms are compared, and that is not
// nothing: a re-embedded vector that is not unit-length ranks against a corpus
// it is not on the same scale as.
func TestGoHoldsWhatRustExported(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			path := requireFixture(t, f)
			d := newDestination(t)
			rep := importFile(t, d, path)

			in, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = in.Close() }()

			snap := d.kv.NewSnapshot()
			defer func() { _ = snap.Close() }()
			got, err := snapshot.Verify(context.Background(), snapshot.Sources{
				Snap: snap, Tenants: d.dst.Tenants, Records: d.repo,
				Events: events.NewStore(d.kv),
			}, in)
			if err != nil {
				t.Fatal(err)
			}

			// The edges the import refused are the only expected divergence,
			// and they are expected precisely because they are named: a
			// self-edge Go's write path cannot produce, and an edge whose
			// target record the corpus does not hold.
			rejected := len(rep.SelfEdges) + len(rep.OrphanEdges)
			var unexpected []snapshot.Finding
			for _, finding := range got.Findings {
				if finding.Kind == snapshot.FindingMissing && finding.What == "edge" {
					continue
				}
				unexpected = append(unexpected, finding)
			}
			if len(unexpected) > 0 {
				t.Fatalf("Go does not hold what Rust exported: %v", unexpected)
			}
			if missing := got.Broken; missing != rejected {
				t.Fatalf("%d edges are in the file and not the store; %d were rejected by name",
					missing, rejected)
			}
			if got.Snapshot.Records != got.Store.Records {
				t.Fatalf("the file holds %d records and the store %d",
					got.Snapshot.Records, got.Store.Records)
			}
		})
	}
}
