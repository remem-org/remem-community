// Package hnsw is the approximate vector index Remem searches at scale.
//
// It is derived data, and the whole design follows from one decision recorded
// in docs/architecture/vector.md: **the canonical vector row is what says a
// memory is in the index.** A node record holds neighbour lists and nothing
// else. Nothing here duplicates an embedding on disk.
//
// That is not a storage micro-optimisation, though it is worth roughly 6x on
// the index (75 MB against 450 MB for a quarter-million memories). It is what
// makes deletion exact. Rust Remem kept its deleted set in a memory-mapped
// region that every checkpoint erased, so after a restart each hard-deleted
// vector came back as a phantom that consumed index memory forever and let
// get_vector serve a stale embedding (PROJECT_REVIEW §2.1 #4). Here a deleted
// memory has no canonical vector, so there is nothing for a node record to be
// about: the node cannot materialise, whether or not the process survived long
// enough to tidy up. The failure class is gone rather than mitigated, which is
// why this package has no tombstone key and no repair sweep.
//
// # Nothing outside this package names HNSW
//
// Callers hold a vector.Index (spec §40.3, enforced by internal/arch). The
// composition root selects an implementation through internal/vector/indexes
// and never sees an M, an ef or a layer.
package hnsw

import (
	"errors"
	"hash/fnv"
	"math"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
)

// Params are the four numbers that decide the graph's shape and the search's
// effort.
//
// They are configuration rather than constants because the trade they make is
// a deployment's to take: every one of them buys recall with memory or with
// latency. The defaults are the values Phase 7's recall tests are measured at.
type Params struct {
	// M is the neighbour bound above layer zero.
	M int
	// M0 is the neighbour bound at layer zero, where most of the graph's
	// connectivity lives. Twice M is the usual choice and the default.
	M0 int
	// EfConstruction is how many candidates an insert considers. Higher builds
	// a better graph, once, at build cost.
	EfConstruction int
	// EfSearch is how many candidates a query considers. Higher costs latency
	// per query and buys recall; TestRecallDegradesGracefully pins that this
	// number is doing what it claims rather than being decorative.
	EfSearch int
}

// Defaults are the parameters the recall tests are measured against:
// recall@10 >= 0.95 at EfSearch 64, and still above 0.80 at 16.
func Defaults() Params {
	return Params{M: 16, M0: 32, EfConstruction: 200, EfSearch: 64}
}

// withDefaults fills in anything a caller left at zero.
func (p Params) withDefaults() Params {
	d := Defaults()
	if p.M <= 0 {
		p.M = d.M
	}
	if p.M0 <= 0 {
		p.M0 = 2 * p.M
	}
	if p.EfConstruction <= 0 {
		p.EfConstruction = d.EfConstruction
	}
	if p.EfSearch <= 0 {
		p.EfSearch = d.EfSearch
	}
	return p
}

// Validate reports whether the parameters can build a graph.
func (p Params) Validate() error {
	const op = "hnsw.Params.Validate"
	switch {
	case p.M <= 0:
		return errs.E(errs.Invalid, op, errors.New("m must be greater than zero"))
	case p.M0 < p.M:
		return errs.E(errs.Invalid, op, errors.New("m0 cannot be below m: layer zero carries the connectivity"))
	case p.EfConstruction <= 0:
		return errs.E(errs.Invalid, op, errors.New("ef_construction must be greater than zero"))
	case p.EfSearch <= 0:
		return errs.E(errs.Invalid, op, errors.New("ef_search must be greater than zero"))
	}
	return nil
}

// maxLevel bounds how tall the graph may grow.
//
// The level distribution is exponential, so a level above this is astronomically
// unlikely and reaching one would mean the hash below is broken. Bounding it
// turns "the level function went wrong" from unbounded memory into a number a
// test can assert.
const maxLevel = 24

// levelOf returns the top layer a record occupies.
//
// It is derived from the record id rather than drawn from a random source, and
// that is deliberate twice over. Invariant 9 forbids a random draw inside a
// deterministic apply path, and Phase 14 will replicate index maintenance.
// More immediately, it is what lets TestRebuildMatchesIncrementalConstruction
// mean something: a rebuilt graph assigns every record the layer the
// incrementally built one did, so any difference between the two comes from
// insertion order alone, which is the thing being measured.
//
// The distribution is the standard floor(-ln(u) / ln(M)) with u uniform in
// (0, 1]. The id is hashed rather than used directly because a UUIDv7's leading
// 48 bits are a timestamp: records created in the same millisecond would
// otherwise land on the same layer together.
func levelOf(rid id.ID, m int) int {
	if m < 2 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write(rid[:])
	// The top 53 bits give a uniform value in [0, 1); shifting away from zero
	// keeps -ln(u) finite.
	u := float64(h.Sum64()>>11+1) / float64(uint64(1)<<53)
	l := int(-math.Log(u) / math.Log(float64(m)))
	if l > maxLevel {
		return maxLevel
	}
	return l
}
