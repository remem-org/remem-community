// Package indexes builds the vector index a deployment configured.
//
// It exists so that the composition root does not have to name one. Spec §40.3
// and internal/arch's rule say no caller outside internal/vector may import
// internal/vector/hnsw; a server that selected its own index would have to,
// and would then be one import away from tuning it. Here the choice is a
// string from configuration and the parameters are numbers, and the server sees
// a vector.Index.
//
// The package cannot live in internal/vector itself: hnsw imports vector for
// the interface it satisfies, so vector importing hnsw would be a cycle. A
// sibling under internal/vector is what the guard permits and what the
// dependency direction allows.
package indexes

import (
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// Config is a deployment's index settings, in neutral terms.
//
// The tuning fields are named for what they do rather than for the algorithm
// that has them, so that adding a second approximate index later does not mean
// a second set of fields through every layer. The configuration file names them
// after the algorithm on purpose — an operator tuning an HNSW graph is entitled
// to see the word — and the translation happens once, at the composition root.
type Config struct {
	// Kind is "flat" or "hnsw".
	Kind string
	// Metric is "cosine", "dot" or "l2".
	Metric string

	// Neighbours bounds how many edges each entry keeps.
	Neighbours int
	// BuildEffort is how many candidates an insertion considers.
	BuildEffort int
	// SearchEffort is how many candidates a query considers.
	SearchEffort int
	// ResidentBudgetBytes bounds the memory approximate indexes may hold.
	ResidentBudgetBytes int64

	// ModelID is the embedding model this server runs. An index that can tell
	// refuses a corpus embedded by another one.
	ModelID string
	// Rebuilds is told when an index was found damaged.
	Rebuilds vector.Rebuilder
}

// Open builds the configured index over kv.
//
// It also reports whether the index needs to be told about writes: an exact
// index does not, because it is a scan of the canonical rows and there is
// nothing to tell it. The caller wires maintenance from the returned
// vector.Maintainer rather than from the configured name, so that "which index
// is running" never becomes a branch in the write path.
func Open(kv storage.KV, cfg Config) (vector.Index, error) {
	const op = "indexes.Open"

	metric, err := distance.Parse(cfg.Metric)
	if err != nil {
		return nil, err
	}

	switch cfg.Kind {
	case "flat":
		return flat.New(vector.NewStore(kv), metric), nil
	case "hnsw":
		return hnsw.New(kv, hnsw.Options{
			Metric: metric,
			Params: hnsw.Params{
				M:              cfg.Neighbours,
				EfConstruction: cfg.BuildEffort,
				EfSearch:       cfg.SearchEffort,
			},
			ResidentBudgetBytes: cfg.ResidentBudgetBytes,
			ModelID:             cfg.ModelID,
			Rebuilds:            cfg.Rebuilds,
		})
	default:
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"vector.index is %q; this binary builds flat and hnsw", cfg.Kind))
	}
}
