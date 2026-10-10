// Package distance holds Remem's metric kernels and the conversions from a
// distance to a number a user is shown.
//
// The conversions matter more than the kernels. A distance orders results; a
// *score* is what a caller thresholds on, and the behaviour baseline records
// what happens when the two are confused (REM-74): under 1/(1+d), two
// orthogonal unit vectors score 1/3 rather than 0, so "reject anything below
// 0.4" rejects almost nothing and a relevance threshold is decoration.
//
// Recovered cosine is exact for L2 and cosine because embeddings are unit-norm
// on the way in. It is not available for dot product, and this package says so
// rather than returning a plausible number — see [CosineFromOK].
//
// Most kernels here are plain loops, because spec §55 forbids vector
// optimisation that has not been measured. The dot product is the exception and
// it earned it: profiled over a whole server it was half of an HNSW insert and
// the inner loop of every search walk, at 158ns for 384 dimensions. It has an
// architecture kernel — dot_arm64.s, with dot_generic.go for everything else
// and for the `purego` tag — and [CosineWithNorms] records both what that
// bought and what it costs in the last bits of a distance.
package distance

import (
	"fmt"
	"math"

	"github.com/remem-org/remem-go/internal/errs"
)

// Metric selects how far apart two vectors are.
type Metric uint8

const (
	// L2 is the *squared* euclidean distance. Squared, because the square root
	// is monotonic and therefore changes no ordering, while costing a call per
	// comparison — and because the cosine recovery below is exact in squared
	// form: for unit vectors, d = 2 - 2·cos.
	L2 Metric = iota + 1
	// Cosine is 1 - cos, so that smaller is closer like every other metric here.
	Cosine
	// DotProduct is the negated inner product. Negated for the same reason:
	// every metric in this package orders smallest-first, and a metric that
	// ordered the other way would be a special case at every call site.
	DotProduct
)

var metricNames = map[Metric]string{L2: "l2", Cosine: "cosine", DotProduct: "dot"}

// String returns the stable name used in configuration and metric labels.
func (m Metric) String() string {
	if n, ok := metricNames[m]; ok {
		return n
	}
	return "unknown"
}

// Parse reads a metric name from configuration.
func Parse(s string) (Metric, error) {
	for m, name := range metricNames {
		if name == s {
			return m, nil
		}
	}
	return 0, errs.E(errs.Invalid, "distance.Parse",
		fmt.Errorf("%q is not a metric; use l2, cosine or dot", s))
}

// Compute returns the distance between a and b under m.
//
// Vectors of different widths are an error rather than a truncated comparison:
// it means a vector from another model reached the index, and quietly comparing
// the first 384 of 768 components would produce a ranking nobody could explain.
func Compute(m Metric, a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errs.E(errs.Invalid, "distance.Compute",
			fmt.Errorf("cannot compare a %d-dimensional vector with a %d-dimensional one", len(a), len(b)))
	}
	switch m {
	case L2:
		return L2Squared(a, b), nil
	case Cosine:
		return CosineDistance(a, b), nil
	case DotProduct:
		return -Dot(a, b), nil
	default:
		return 0, errs.E(errs.Invalid, "distance.Compute", fmt.Errorf("unknown metric %d", m))
	}
}

// L2Squared is the squared euclidean distance.
func L2Squared(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// Dot is the inner product.
func Dot(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// CosineDistance is 1 - cos(a, b).
//
// It normalises rather than assuming unit vectors, because this is the metric a
// caller chooses when the vectors might not be normalised — if they always were,
// L2 would answer the same question more cheaply.
func CosineDistance(a, b []float32) float32 {
	return CosineWithNorms(a, b, Norm(a), Norm(b))
}

// Norm is v's Euclidean length, in the precision CosineWithNorms uses.
func Norm(v []float32) float64 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	return math.Sqrt(sum)
}

// CosineWithNorms is CosineDistance for a caller that already knows both
// vectors' lengths, and returns exactly the same number.
//
// An index computes thousands of distances against vectors whose lengths do
// not change, so recomputing both norms on every call is waste. CosineDistance
// is this function over freshly computed norms, so the two can never disagree
// in a last bit and reorder a tie between an index and the flat oracle
// (TestCosineWithNormsMatchesCosineDistance).
//
// # Why the dot product is single precision, unrolled, and in assembly on arm64
//
// Profiled in Phase 13, over 90% of an HNSW insert was this dot product, and an
// insert holds its tenant's graph lock, so its cost capped concurrent writes.
// Measured at 384 dimensions (BenchmarkDot384*): the float64 loop took 355ns, an
// unrolled float64 loop 283ns, and an unrolled float32 loop 145ns. Four
// independent accumulators let the compiler keep each on its own register
// rather than serialise every add on one.
//
// The price is precision in the last decimals of a distance, about the seventh,
// and it is paid uniformly: every cosine distance in the system comes from here,
// so HNSW, the flat oracle, semantic and hybrid search all still agree with each
// other exactly. Vectors themselves are float32 on disk and in memory, so no
// stored value moves.
//
// §55's condition was "until it is measured to be" faster, and the server-wide
// profile behind the server speed plan measured it: the scalar loop was
// still half of an insert. [dot32] is therefore a NEON kernel on arm64 and that
// same scalar loop everywhere else. Measured on an M1 Pro at 384 dimensions,
// this function went from 161.6ns to 45.2ns, and an HNSW insert and search
// each fell by more than 40%.
//
// The price is a second kind of last-bit difference, this one between
// architectures rather than between precisions. Both kernels stay inside the
// bound dot_internal_test.go states, and because the whole system calls this
// one function, every index on one machine still agrees with every other one
// there exactly. Reproducibility *across* architectures was never on offer —
// float32 arithmetic in a different order is a different sum, and the Go
// compiler already contracted this loop's multiply-add on arm64 and not on
// amd64 — so what holds everywhere is the bound, not the bits. hnsw's search
// digest records the measured effect on actual answers.
func CosineWithNorms(a, b []float32, na, nb float64) float32 {
	if na == 0 || nb == 0 {
		// A zero vector has no direction. Reporting maximum distance is the
		// only answer that cannot mislead: it matches nothing rather than
		// matching everything.
		return 1
	}
	return float32(1 - float64(dot32(a, b))/(na*nb))
}

// CosineFromOK recovers the cosine similarity from a distance, and reports
// whether the metric permits it.
//
// L2 and cosine both do, exactly, because embeddings are L2-normalised on the
// way in: for unit vectors squared_l2 = 2 - 2·cos, so cos = 1 - d/2.
//
// Dot product does not, and this returns false rather than a number. The index
// does not guarantee normalisation under that metric, so any "cosine" derived
// from a dot-product distance would be a similarity for vectors that might not
// be unit-length — a number that looks calibrated and is not.
func CosineFromOK(m Metric, d float32) (float32, bool) {
	switch m {
	case L2:
		return clampCosine(1 - d/2), true
	case Cosine:
		return clampCosine(1 - d), true
	default:
		return 0, false
	}
}

// CosineFrom recovers the cosine similarity, returning 0 where the metric does
// not permit recovery. Callers that must distinguish "orthogonal" from "cannot
// say" use [CosineFromOK].
func CosineFrom(m Metric, d float32) float32 {
	c, _ := CosineFromOK(m, d)
	return c
}

// Score is the monotonic ordering value 1/(1+d).
//
// It orders correctly under every metric, which is all it is for. It is *not*
// the reported relevance: it floors at 1/3 for orthogonal unit vectors under
// L2 and never approaches zero, which is what made a relevance threshold
// useless in Rust Remem (REM-74). Use [Relevance].
func Score(d float32) float32 {
	if d < 0 {
		// Only reachable under dot product, where a negated inner product may
		// exceed 1 in magnitude. Clamping keeps the value in (0, 1] instead of
		// producing a negative "score" or dividing by zero at d = -1.
		d = 0
	}
	return 1 / (1 + d)
}

// Relevance is the number a caller sees: recovered cosine where the metric
// permits it, and the ordering value otherwise.
//
// This is the Rust contract, kept deliberately (behaviour baseline §1.3): two
// unrelated memories score near 0.0, not 0.33, which is what makes a threshold
// usable at all.
func Relevance(m Metric, d float32) float32 {
	if c, ok := CosineFromOK(m, d); ok {
		return c
	}
	return Score(d)
}

// clampCosine holds a recovered cosine in [0, 1].
//
// Float error can put an identical pair a hair above 1, and a genuinely
// opposite pair below 0. Both are reported to users, and a similarity of
// 1.0000001 is a support question nobody should have to answer.
func clampCosine(c float32) float32 {
	switch {
	case c > 1:
		return 1
	case c < 0:
		return 0
	default:
		return c
	}
}
