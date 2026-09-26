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
// The kernels are plain loops. Spec §55 forbids premature vector optimisation,
// and Go's compiler auto-vectorises this shape well; a hand-unrolled version
// would be harder to read and no faster until it is measured to be.
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
// # Why the dot product is single precision and unrolled
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
func CosineWithNorms(a, b []float32, na, nb float64) float32 {
	if na == 0 || nb == 0 {
		// A zero vector has no direction. Reporting maximum distance is the
		// only answer that cannot mislead: it matches nothing rather than
		// matching everything.
		return 1
	}
	return float32(1 - float64(dot32(a, b))/(na*nb))
}

// dot32 is the dot product in single precision, four accumulators wide.
func dot32(a, b []float32) float32 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
	}
	for ; i < len(a); i++ {
		s0 += a[i] * b[i]
	}
	return s0 + s1 + s2 + s3
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
