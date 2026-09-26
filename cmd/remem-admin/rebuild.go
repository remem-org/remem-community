package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexes"
)

// rebuildIndexes are the derived indexes an operator can name, in the order
// spec §43 lists the scopes: whole database, one tenant, one index.
//
// Every one of them is derived and therefore rebuildable, which is Invariant 3
// stated as a command. The canonical rows — record bodies, canonical vectors,
// out-edges — are not here and never will be: nothing rebuilds them, which is
// what makes them canonical.
var rebuildIndexes = []string{"vector", "text", "attr", "graph-in"}

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
		"which derived index to rebuild: vector, text, attr, graph-in, or all")
	only := fs.String("tenant", "", "rebuild one tenant rather than every tenant")
	metric := fs.String("metric", "cosine", "the distance metric the server is configured with")
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

	kv, err := openReadWriteExisting(*dataDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = kv.Close() }()

	dir, scoped, err := scope(kv, clock.System(), *only)
	if err != nil {
		return 0, err
	}
	only = &scoped

	ctx := context.Background()
	run := func(t tenant.ID) error {
		for _, name := range wanted {
			started := time.Now()
			n, err := rebuildOne(ctx, kv, name, t, *metric)
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
	metric string,
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
