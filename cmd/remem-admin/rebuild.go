package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexes"
	"github.com/remem-org/remem-go/internal/version"
)

// rebuildIndexes are the derived indexes an operator can name, in the order
// spec §43 lists the scopes: whole database, one tenant, one index.
//
// Every one of them is derived and therefore rebuildable, which is Invariant 3
// stated as a command. The canonical rows — record bodies, canonical vectors,
// out-edges — are not here and never will be: nothing rebuilds them, which is
// what makes them canonical.
var rebuildIndexes = []string{"vector", "text", "attr", "graph-in", "passages"}

// spaceIndexName is the `--index` name that rebuilds a derived key space.
//
// Both attribute spaces share one name because one pass reprojects both: a slot
// entry is written by the same Stage call that writes the row it indexes.
// TestEveryDerivedSpaceHasARebuildCommand walks keys.AllSpaces against this, so
// a derived space added without a command fails by name.
func spaceIndexName(s keys.Space) (string, bool) {
	switch s {
	case keys.SpaceVectorIndex:
		return "vector", true
	case keys.SpaceText:
		return "text", true
	case keys.SpaceAttrRow, keys.SpaceAttrIndex:
		return "attr", true
	case keys.SpaceEdgeIn:
		return "graph-in", true
	case keys.SpacePassage:
		return "passages", true
	}
	return "", false
}

// rebuildCommand rebuilds derived indexes.
//
// It is the unified spelling spec §43 asks for, and it *joins* `vector rebuild`
// and `text rebuild` rather than replacing them: those two are named in
// CLAUDE.md, in docs/architecture/, and — the reason that settles it — in the
// server's own warning messages. A command a log line points at does not get
// renamed.
//
// What it adds is the two that had no command at all. The attribute rows and
// their slot indexes were rebuilt only by a migration, and the in-edge index
// only by a job; both are derived, and Invariant 3 says a derived index has a
// tested rebuild path rather than a repair.
func rebuildCommand(args []string) (int, error) {
	fs := flag.NewFlagSet("rebuild", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to rebuild")
	which := fs.String("index", "",
		"which derived index to rebuild: vector, text, attr, graph-in, passages, or all")
	only := fs.String("tenant", "", "rebuild one tenant rather than every tenant")
	metric := fs.String("metric", "cosine", "the distance metric the server is configured with")
	modelPath := fs.String("model-path", os.Getenv("REMEM_EMBEDDING_MODEL_PATH"),
		"with --index passages or all: the pplx-embed-context-v1-0.6b directory")
	onnxPath := fs.String("onnx-library-path", os.Getenv("REMEM_EMBEDDING_ONNX_LIBRARY_PATH"),
		"the ONNX Runtime shared library")
	provider := fs.String("embedding-provider", envOr("REMEM_EMBEDDING_PROVIDER", config.ProviderONNX),
		"onnx, or external for a data directory whose server takes vectors from its callers")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if *dataDir == "" {
		return 0, errors.New("rebuild needs --data-dir")
	}
	wanted, err := chooseIndexes(*which)
	if err != nil {
		return 0, err
	}
	// Passages are the one rebuild that runs the model, so the model is opened
	// before the database: a build without it is refused by name and leaves
	// nothing behind. An external-mode directory has no passages and needs no
	// model; its rebuild clears any stray rows.
	var embedder embedding.Embedder
	if slices.Contains(wanted, "passages") {
		if *provider == config.ProviderExternal {
			embedder = embedding.NewExternal("external", config.EmbeddingDimension)
		} else {
			embedder, err = openModel(*modelPath, *onnxPath,
				"rebuilding the passage index recomputes every memory's passage embeddings",
				"rebuild --index passages needs --model-path (or REMEM_EMBEDDING_MODEL_PATH): "+
					"passage rows are recomputed from each memory's content by the model")
			if err != nil {
				return 0, err
			}
		}
		if c, ok := embedder.(io.Closer); ok {
			defer func() { _ = c.Close() }()
		}
	}

	kv, err := openReadWriteExisting(*dataDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = kv.Close() }()

	dir, scoped, err := scope(kv, clock.System(), *only)
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	upgraded := false
	if embedder != nil {
		format, err := schema.Open(ctx, kv, version.Current())
		if err != nil {
			return 0, err
		}
		upgraded = format.Subsystems["passage_index"].Current < version.PassageIndex
		if err := upgradePassageFormat(ctx, kv, dir, *dataDir, *only, embedder); err != nil {
			return 0, err
		}
	}
	only = &scoped
	run := func(t tenant.ID) error {
		for _, name := range wanted {
			if upgraded && name == "passages" {
				fmt.Printf("tenant %s: passages built by the passage_index migration\n", t)
				continue
			}
			started := time.Now()
			n, err := rebuildOne(ctx, kv, name, t, *metric, embedder)
			if err != nil {
				return fmt.Errorf("rebuilding %s for tenant %s: %w", name, t, err)
			}
			fmt.Printf("tenant %s: %s rebuilt (%d row(s)) in %s\n",
				t, name, n, time.Since(started).Round(time.Millisecond))
		}
		return nil
	}

	if *only != "" {
		return 0, run(tenant.ID(*only))
	}
	seen := 0
	if err := dir.ForEach(ctx, func(t tenant.ID) error {
		seen++
		return run(t)
	}); err != nil {
		return 0, err
	}
	if seen == 0 {
		fmt.Println("no tenants found")
	}
	return 0, nil
}

func chooseIndexes(which string) ([]string, error) {
	switch which {
	case "":
		return nil, fmt.Errorf("rebuild needs --index: one of %v, or all", rebuildIndexes)
	case "all":
		return rebuildIndexes, nil
	}
	for _, name := range rebuildIndexes {
		if name == which {
			return []string{which}, nil
		}
	}
	return nil, fmt.Errorf("%q is not a derived index; the indexes are %v, or all",
		which, rebuildIndexes)
}

// rebuildOne rebuilds a single index for one tenant and reports how many rows
// it wrote.
//
// Each case delegates to the package that owns the key space, rather than
// reimplementing a rebuild here. A second implementation of a derived index is
// a second thing that can disagree with the canonical rows.
func rebuildOne(ctx context.Context, kv storage.KV, name string, t tenant.ID,
	metric string, embedder embedding.Embedder,
) (int, error) {
	ns := tenant.DefaultNamespace
	switch name {
	case "vector":
		index, err := indexes.Open(kv, indexes.Config{Kind: "hnsw", Metric: metric})
		if err != nil {
			return 0, err
		}
		if err := index.Rebuild(ctx, t, vector.NewStore(kv)); err != nil {
			return 0, err
		}
		s, err := index.Stats(ctx, t)
		if err != nil {
			return 0, err
		}
		return int(s.Vectors), nil

	case "text":
		ix := text.New()
		if err := ix.Rebuild(ctx, kv, t, ns, record.NewContentSource(kv)); err != nil {
			return 0, err
		}
		stats, err := text.ReadStats(ctx, kv, t, ns)
		if err != nil {
			return 0, err
		}
		return int(stats.Documents), nil

	case "attr":
		return attr.RebuildRows(ctx, kv, t, ns)

	case "graph-in":
		return graph.RebuildIn(ctx, kv, graph.Scope{Tenant: t, Namespace: ns})

	case "passages":
		// Not resumable here, as no remem-admin rebuild is: an interrupted run
		// starts again. The passages.rebuild job is the resumable form.
		return record.RebuildPassages(ctx, kv, t, ns, embedder, nil, nil)

	default:
		return 0, fmt.Errorf("%q is not a derived index", name)
	}
}

// openReadWriteExisting opens a data directory that must already exist,
// probing read-only first so a mistyped path is refused rather than becoming a
// new empty database that reports a clean rebuild over no data. This is the
// defect Phase 6 found in `graph verify`, and every command here has carried
// the fix for it since.
func openReadWriteExisting(dataDir string) (storage.KV, error) {
	probe, err := pebble.Open(dataDir, pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w\n"+
			"check the path, and note that a data directory in use by a running server may not be "+
			"readable: stop it first", dataDir, err)
	}
	_ = probe.Close()

	kv, err := pebble.Open(dataDir, pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening %s for writing: %w", dataDir, err)
	}
	return kv, nil
}
