package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/onnx"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/lifecycle/policykv"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
)

// The portable snapshot commands.
//
// A snapshot is the only backup format Remem has (Invariant 12: a copy of
// Pebble's files is not one, because a backup only Pebble can read is not an
// escape route from Pebble), and it is the one surface the Rust implementation
// and this one share. So there are three verbs and they are deliberately
// separate: export writes a file, import reads one, and verify compares one
// against a live store without writing anything — which is the step between an
// import and decommissioning the system the file came from.

// exportCommand writes a snapshot of a data directory.
func exportCommand(args []string) (int, error) {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to export")
	out := fs.String("out", "", "the snapshot file to write")
	only := fs.String("tenant", "", "export one tenant rather than every tenant")
	noCompress := fs.Bool("no-compress", false, "store blocks uncompressed")
	sharedOnly := fs.Bool("shared-only", false,
		"write only the sections proto/snapshot/v1 defines, dropping what it has no field for")
	pageSize := fs.Int("page-size", snapshot.DefaultPageSize, "records per block")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" || *out == "" {
		return 0, errors.New("export needs --data-dir and --out")
	}

	// Read-only, and that is not merely caution: an export must not be able to
	// modify the corpus it is preserving, and a mistyped path must not become a
	// new empty database that exports cleanly and says "no tenants".
	kv, err := openReadOnly(*dataDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = kv.Close() }()

	// Export is the one command that may read a directory holding tenants this
	// build does not serve, because it is the way out of one. scope.go says why.
	dir, scoped, err := migrationScope(kv, clock.System(), *only, os.Stdout)
	if err != nil {
		return 0, err
	}

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	f, err := os.Create(*out)
	if err != nil {
		return 0, fmt.Errorf("creating %s: %w", *out, err)
	}
	defer func() { _ = f.Close() }()

	compression := snapshot.CompressionZstd
	if *noCompress {
		compression = snapshot.CompressionNone
	}

	started := time.Now()
	stats, err := snapshot.Export(context.Background(), snapshot.Sources{
		Snap:    snap,
		Tenants: dir,
		Records: record.NewRepo(kv),
		Events:  events.NewStore(kv),
	}, f, snapshot.ExportOpts{
		Compression: compression,
		PageSize:    *pageSize,
		Tenant:      tenant.ID(scoped),
		SharedOnly:  *sharedOnly,
	})
	if err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("flushing %s: %w", *out, err)
	}

	fmt.Printf("exported %d tenant(s), %d record(s), %d vector(s) (%d without one), "+
		"%d edge(s), %d event(s) to %s in %s\n",
		stats.Tenants, stats.Records, stats.Vectors, stats.MissingVectors,
		stats.Edges, stats.Events, *out, time.Since(started).Round(time.Millisecond))

	// What a shared-only export cost, said out loud. Losing user-supplied
	// annotation quietly is the failure this format exists not to have.
	if stats.EdgesWithMetaDropped > 0 || stats.EventsDropped > 0 {
		fmt.Printf("dropped, because proto/snapshot/v1 has no field for them: "+
			"metadata on %d connection(s), %d lifecycle audit event(s). "+
			"Export without --shared-only to keep them.\n",
			stats.EdgesWithMetaDropped, stats.EventsDropped)
	}
	return 0, nil
}

// importCommand reads a snapshot into a data directory.
func importCommand(args []string) (int, error) {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to import into")
	in := fs.String("in", "", "the snapshot file to read")
	onConflict := fs.String("on-conflict", "fail",
		"what to do about a memory that already exists with different content: fail, skip or overwrite")
	vectors := fs.String("vectors", "auto",
		"auto, re-embed or verbatim; see the note below on why verbatim is usually wrong")
	resume := fs.Bool("resume", false,
		"continue from the cursor a previous run left beside the snapshot")
	batch := fs.Int("batch", snapshot.DefaultBatchSize, "rows per transaction")
	modelPath := fs.String("model-path", os.Getenv("REMEM_EMBEDDING_MODEL_PATH"),
		"the all-MiniLM-L6-v2 directory, for the embeddings an import recomputes")
	onnxPath := fs.String("onnx-library-path", os.Getenv("REMEM_EMBEDDING_ONNX_LIBRARY_PATH"),
		"the ONNX Runtime shared library")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" || *in == "" {
		return 0, errors.New("import needs --data-dir and --in")
	}
	mode, err := snapshot.ParseConflictMode(*onConflict)
	if err != nil {
		return 0, err
	}
	policy, err := parseVectorPolicy(*vectors)
	if err != nil {
		return 0, err
	}

	f, err := os.Open(*in)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", *in, err)
	}
	defer func() { _ = f.Close() }()

	// The header first, before the destination is opened for writing: it says
	// whether this import needs a model at all, and refusing before a database
	// is touched is better than refusing halfway through one.
	header, err := snapshot.ReadHeader(f)
	if err != nil {
		return 0, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return 0, err
	}
	fmt.Printf("importing a snapshot written by %s %s at %s: %d tenant(s), %d record(s), "+
		"%d vector(s), %d edge(s), %d event(s)\n",
		header.GetSourceImpl(), header.GetSourceVersion(),
		time.UnixMilli(int64(header.GetCreatedAtUnixMs())).UTC().Format(time.RFC3339),
		len(header.GetTenants()), header.GetRecordCount(), header.GetVectorCount(),
		header.GetEdgeCount(), header.GetEventCount())

	// And the tenants the file carries, before the destination is opened, for
	// the reason Phase 12 learned the hard way: a refused import must leave no
	// trace of having tried, and the vector-policy refusal that fired from
	// inside the library left an empty Pebble database behind.
	if err := refuseForeignTenants(header.GetTenants()); err != nil {
		return 0, err
	}

	embedder, err := importEmbedder(header, policy, *modelPath, *onnxPath)
	if err != nil {
		return 0, err
	}
	if c, ok := embedder.(interface{ Close() error }); ok && embedder != nil {
		defer func() { _ = c.Close() }()
	}

	kv, err := openReadWrite(*dataDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = kv.Close() }()

	clk := clock.System()
	dir := tenantkv.New(kv, clk)
	// The repository is built the way the server builds it, so an imported
	// memory gets the same attribute rows, the same postings and the same
	// next-attention time a stored one would — computed by the code that owns
	// them rather than by a second copy inside the importer.
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(text.New()),
		record.WithScheduler(lifecycle.NewScheduler(
			lifecycle.NewRegistry(dir, policykv.New(kv, clk).Loader()))))

	cursorPath := *in + ".resume"
	var cursor []byte
	if *resume {
		cursor, err = os.ReadFile(cursorPath)
		if err != nil && !os.IsNotExist(err) {
			return 0, fmt.Errorf("reading %s: %w", cursorPath, err)
		}
		if len(cursor) > 0 {
			fmt.Printf("resuming from %s\n", cursorPath)
		}
	}

	ctx := context.Background()
	started := time.Now()
	lastSaved := time.Now()
	rep, err := snapshot.Import(ctx, snapshot.Destination{
		KV: kv, Tenants: dir, Records: repo,
		Edges: graph.NewStore(kv), Events: events.NewStore(kv), Embedder: embedder,
	}, f, snapshot.ImportOpts{
		OnConflict: mode,
		Vectors:    policy,
		BatchSize:  *batch,
		Resume:     cursor,
		Progress: func(p snapshot.Progress) {
			// The cursor is written beside the snapshot rather than into the
			// store, because Invariant 1 gives every key a tenant and an
			// import cursor spans them.
			if len(p.Cursor) == 0 || time.Since(lastSaved) < time.Second {
				return
			}
			lastSaved = time.Now()
			_ = os.WriteFile(cursorPath, p.Cursor, 0o600)
		},
	})
	if err != nil {
		if len(rep.Cursor) > 0 {
			_ = os.WriteFile(cursorPath, rep.Cursor, 0o600)
			fmt.Fprintf(os.Stderr, "progress saved to %s; re-run with --resume\n", cursorPath)
		}
		return 0, err
	}
	_ = os.Remove(cursorPath)

	fmt.Printf("imported %d record(s), %d edge(s), %d event(s) in %s\n",
		rep.Stats.Records, rep.Stats.Edges, rep.Stats.Events,
		time.Since(started).Round(time.Millisecond))
	fmt.Printf("embeddings: %d recomputed, %d taken from the snapshot\n",
		rep.ReEmbedded, rep.VectorsVerbatim)
	if rep.Unchanged > 0 || rep.Skipped > 0 {
		fmt.Printf("already present: %d identical, %d kept because they differed\n",
			rep.Unchanged, rep.Skipped)
	}
	reportRejected("self-edges rejected", rep.SelfEdges, rep.Truncated)
	reportRejected("orphan edges removed", rep.OrphanEdges, rep.Truncated)

	if err := queueVectorRebuilds(ctx, kv, dir, clk); err != nil {
		// The corpus is imported and correct; only the head start on the index
		// was lost. Saying so beats failing an import that succeeded.
		fmt.Fprintf(os.Stderr,
			"the import finished, but queueing the vector index rebuild failed: %v\n"+
				"run `remem-admin rebuild --index vector` before relying on semantic search\n", err)
		return 0, nil
	}
	fmt.Println("a vector index rebuild is queued for each tenant; the server runs it when it " +
		"next starts. Until it does, searches answer correctly and report truncated: true")
	return 0, nil
}

// vectorRebuildJobType is the durable name the server registers the handler
// under.
//
// Spelled here rather than imported from internal/server, for the reason
// discovery.go gives: this command must not drag the composition root — an
// embedder it has already loaded itself, a listener, a migration runner — into a
// binary that opens a data directory and writes rows. The name is durable, so
// the duplication is a constant rather than a coupling, and a mismatch shows up
// immediately as a type the server refuses as unregistered.
const vectorRebuildJobType jobs.Type = "vector.rebuild"

// queueVectorRebuilds asks each imported tenant's approximate index to be
// rebuilt.
//
// An import writes canonical vectors and no node records, deliberately: the
// HNSW graph is derived, it is materialised lazily, and vector.Index.Health
// reports the tenant as truncated until it is built — so a search over a freshly
// imported corpus answers correctly and says it is degraded. That is safe, and
// it is still a first search that is slower than it needs to be for a corpus
// whose size is known in advance.
//
// So the import queues the repair the same way a degraded index does since
// Phase 9, rather than doing it: building the index here would make an import
// pay for work the server is about to be able to do in the background, on a
// binary that may not even be the one that will serve.
func queueVectorRebuilds(ctx context.Context, kv storage.KV, dir tenant.Directory,
	clk clock.Clock,
) error {
	queue := jobs.NewQueue(kv, clk)
	// The cross-tenant walk goes through the tenant directory, the single
	// audited path across tenants (Invariant 1).
	return dir.ForEach(ctx, func(t tenant.ID) error {
		return txn.Do(ctx, kv, func(tx txn.Tx) error {
			return queue.Enqueue(ctx, tx, &jobs.Job{
				Tenant: t, Namespace: tenant.DefaultNamespace,
				Type: vectorRebuildJobType, MaxAttempts: 2,
			})
		})
	})
}

// verifyCommand compares a snapshot against a live store.
func verifyCommand(args []string) (int, error) {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to compare")
	in := fs.String("in", "", "the snapshot file to compare it against")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" || *in == "" {
		return 0, errors.New("verify needs --data-dir and --in")
	}

	kv, err := openReadOnly(*dataDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = kv.Close() }()

	f, err := os.Open(*in)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", *in, err)
	}
	defer func() { _ = f.Close() }()

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	dir, _, err := scope(kv, clock.System(), "")
	if err != nil {
		return 0, err
	}
	rep, err := snapshot.Verify(context.Background(), snapshot.Sources{
		Snap:    snap,
		Tenants: dir,
		Records: record.NewRepo(kv),
		Events:  events.NewStore(kv),
	}, f)
	if err != nil {
		return 0, err
	}

	fmt.Printf("snapshot: %d record(s), %d vector(s), %d edge(s), %d event(s)\n",
		rep.Snapshot.Records, rep.Snapshot.Vectors, rep.Snapshot.Edges, rep.Snapshot.Events)
	fmt.Printf("store:    %d record(s), %d vector(s), %d edge(s)\n",
		rep.Store.Records, rep.Store.Vectors, rep.Store.Edges)
	if rep.Clean() {
		fmt.Println("the store holds what the snapshot describes")
		return 0, nil
	}
	fmt.Printf("%d disagreement(s):\n", rep.Broken)
	for _, f := range rep.Findings {
		fmt.Println(" ", f)
	}
	if rep.Truncated {
		fmt.Printf("  ... and %d more, not listed\n", rep.Broken-len(rep.Findings))
	}
	// Exit 2, the disposition `graph verify` already has: "the command worked
	// and found problems" is neither success nor failure, and zero on a corpus
	// that does not match would make this useless as a check.
	return 2, nil
}

func reportRejected(what string, rows []snapshot.Rejected, truncated bool) {
	if len(rows) == 0 {
		return
	}
	fmt.Printf("%s: %d\n", what, len(rows))
	for i, r := range rows {
		if i == 10 {
			fmt.Printf("  ... and %d more\n", len(rows)-10)
			break
		}
		fmt.Println(" ", r)
	}
	if truncated {
		fmt.Println("  (the list was capped; the counts above are exact)")
	}
}

func parseVectorPolicy(s string) (snapshot.VectorPolicy, error) {
	switch s {
	case "auto":
		return snapshot.VectorsAuto, nil
	case "re-embed":
		return snapshot.VectorsReEmbed, nil
	case "verbatim":
		return snapshot.VectorsVerbatim, nil
	default:
		return 0, fmt.Errorf("%q is not a vector policy; the policies are auto, re-embed and "+
			"verbatim", s)
	}
}

// importEmbedder loads the model, if this import needs one.
//
// It is loaded only when it will be used, so a Go-to-Go restore with
// --vectors=verbatim works on a machine that has never fetched a model — and an
// import that *does* need one fails here, before a database has been opened for
// writing, rather than partway through it.
func importEmbedder(header *snapshot.Header, policy snapshot.VectorPolicy,
	modelPath, onnxPath string,
) (embedding.Embedder, error) {
	foreign := header.GetSourceImpl() != "go"

	// The refusal that has to happen here rather than inside Import, even
	// though Import refuses it too. Found by running the command: importing a
	// Rust snapshot with --vectors=verbatim was refused *after* the data
	// directory had been opened for writing, so a refused import left an empty
	// database behind — where the no-embedder refusal, which happens on this
	// side, left nothing. A command that refuses should leave no trace of
	// having tried.
	if policy == snapshot.VectorsVerbatim && foreign {
		return nil, fmt.Errorf(
			"this snapshot was written by %q, and its vectors are not comparable with the ones "+
				"this binary produces: Rust pools the model's output differently "+
				"(implementation plan §II.10 row 16), so importing them verbatim would give every "+
				"memory a vector of the right width in the wrong space and every search a "+
				"plausible wrong answer. Import without --vectors=verbatim, on a binary built "+
				"with the embedder", header.GetSourceImpl())
	}

	needs := policy == snapshot.VectorsReEmbed || (policy == snapshot.VectorsAuto && foreign)
	if !needs {
		return nil, nil
	}
	if !onnx.Available {
		return nil, fmt.Errorf(
			"this snapshot's embeddings have to be recomputed and this remem-admin was built " +
				"without the model. Rebuild it with\n" +
				"    CGO_ENABLED=1 CGO_LDFLAGS=\"-L$(pwd)/.tools/lib\" go build -tags onnx -o remem-admin ./cmd/remem-admin\n" +
				"after running scripts/fetch-model.sh")
	}
	if modelPath == "" {
		return nil, errors.New(
			"import needs --model-path (or REMEM_EMBEDDING_MODEL_PATH) to recompute this " +
				"snapshot's embeddings")
	}
	e, err := onnx.Open(onnx.Config{ModelPath: modelPath, SharedLibraryPath: onnxPath})
	if err != nil {
		return nil, err
	}
	fmt.Printf("recomputing embeddings with %s\n", e.ModelID())
	return e, nil
}

// openReadOnly opens a data directory for reading, refusing a path that holds
// no database rather than creating one.
func openReadOnly(dataDir string) (storage.KV, error) {
	kv, err := pebble.Open(dataDir, pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w\n"+
			"check the path, and note that a data directory in use by a running server may not be "+
			"readable: stop it first", filepath.Clean(dataDir), err)
	}
	return kv, nil
}

// openReadWrite probes read-only first, so a mistyped path is refused rather
// than becoming a new empty database — the defect Phase 6 found in `graph
// verify` and every command here has carried the fix for since.
//
// An import is the one command where that reasoning does not hold on its own,
// because importing into a *fresh* directory is the ordinary case. So it creates
// one only when the path does not exist at all, and refuses a path that exists
// and is not a Remem data directory.
func openReadWrite(dataDir string) (storage.KV, error) {
	if _, err := os.Stat(dataDir); err == nil {
		probe, err := pebble.Open(dataDir, pebble.Options{ReadOnly: true})
		if err != nil {
			return nil, fmt.Errorf("opening %s: %w\n"+
				"the path exists but does not hold a Remem data directory. Import into a new "+
				"path, or stop the server that is using this one", filepath.Clean(dataDir), err)
		}
		_ = probe.Close()
	}
	kv, err := pebble.Open(dataDir, pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening %s for writing: %w", filepath.Clean(dataDir), err)
	}
	return kv, nil
}
