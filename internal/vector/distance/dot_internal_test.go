package distance

import (
	"math"
	"math/rand"
	"testing"

	"pgregory.net/rapid"
)

// The dot product has a hand-written kernel per architecture (plan item 15), so
// it needs a test per width rather than a test per metric: what goes wrong in a
// vector kernel goes wrong in the tail, at the width where the unrolled body
// stops and the remainder starts.
//
// The reference is the same sum in double precision. The tolerance is relative
// to the sum of the terms' magnitudes, not to the result: a dot product of
// random vectors cancels, so the result can be arbitrarily close to zero while
// the rounding error stays what the summands made it, and a tolerance relative
// to the result would be unmeetable at some seed. dotTolerance is 1e-6 of that
// magnitude — about sixteen float32 unit roundoffs (2^-24 ≈ 6e-8), which bounds
// any summation order over the widths a model produces.
//
// A relative tolerance alone is still not enough, which rapid found on the
// thirtieth case: a product small enough to be a float32 denormal has no
// relative accuracy left at all, only the absolute granularity of the denormals
// themselves. So the bound is the standard one for a float32 sum, relative term
// plus underflow term — dim+1 roundings, each of at most one smallest denormal.
const dotTolerance = 1e-6

// dotToleranceAt is the error a correct float32 dot product of dim terms may
// have against the double-precision sum of the same terms.
func dotToleranceAt(dim int, magnitude float64) float64 {
	return dotTolerance*magnitude + float64(dim+1)*math.SmallestNonzeroFloat32
}

// dotReference is the dot product in double precision, and the sum of the
// terms' magnitudes that bounds the error of computing it in single.
func dotReference(a, b []float32) (sum, magnitude float64) {
	for i := range a {
		p := float64(a[i]) * float64(b[i])
		sum += p
		magnitude += math.Abs(p)
	}
	return sum, magnitude
}

// dotScalar is the portable kernel, four accumulators wide: what an
// architecture without an assembly kernel runs, and what `purego` selects. It
// is here as well as in dot_generic.go so that a test can compare the two on an
// architecture where only one of them is compiled.
func dotScalar(a, b []float32) float32 {
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

// TestDotMatchesADoubleReferenceAtEveryWidth covers every width from nothing to
// 520: every remainder of every unroll any kernel here uses, twice over, with
// the production width 384 among them.
func TestDotMatchesADoubleReferenceAtEveryWidth(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	worst, worstDim := 0.0, 0
	for dim := 0; dim <= 520; dim++ {
		a, b := randomVectors(rng, dim)
		want, magnitude := dotReference(a, b)
		got := float64(dot32(a, b))
		if tol := dotToleranceAt(dim, magnitude); math.Abs(got-want) > tol {
			t.Fatalf("dim %d: dot32 = %v, double reference = %v, off by %v against a tolerance of %v",
				dim, got, want, got-want, tol)
		}
		if magnitude > 0 {
			if rel := math.Abs(got-want) / magnitude; rel > worst {
				worst, worstDim = rel, dim
			}
		}
	}
	t.Logf("worst relative error %.3g of the summands' magnitude, at dim %d (tolerance %g)",
		worst, worstDim, dotTolerance)
}

// TestDotHandlesAliasedInput passes one vector as both arguments, which is what
// a self-distance does and what a kernel assuming two disjoint cursors gets
// wrong.
func TestDotHandlesAliasedInput(t *testing.T) {
	rng := rand.New(rand.NewSource(13))
	for _, dim := range []int{0, 1, 3, 4, 15, 16, 17, 31, 33, 63, 384, 519} {
		a, _ := randomVectors(rng, dim)
		want, magnitude := dotReference(a, a)
		got := float64(dot32(a, a))
		if tol := dotToleranceAt(dim, magnitude); math.Abs(got-want) > tol {
			t.Fatalf("dim %d aliased: dot32(a, a) = %v, want %v within %v", dim, got, want, tol)
		}
	}
}

// TestDotIsTheSumOfProductsWhateverTheShape lets rapid choose the width and the
// magnitudes, including the mixed scales that make a reordered sum diverge
// furthest from a sequential one.
func TestDotIsTheSumOfProductsWhateverTheShape(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dim := rapid.IntRange(0, 600).Draw(rt, "dim")
		scale := rapid.SampledFrom([]float64{1e-6, 1e-3, 1, 1e3, 1e6}).Draw(rt, "scale")
		a := make([]float32, dim)
		b := make([]float32, dim)
		for i := range a {
			a[i] = float32(rapid.Float64Range(-1, 1).Draw(rt, "a") * scale)
			b[i] = float32(rapid.Float64Range(-1, 1).Draw(rt, "b"))
		}
		want, magnitude := dotReference(a, b)
		got := float64(dot32(a, b))
		if tol := dotToleranceAt(dim, magnitude); math.Abs(got-want) > tol {
			rt.Fatalf("dim %d scale %g: dot32 = %v, want %v within %v", dim, scale, got, want, tol)
		}
	})
}

// TestDotAgreesWithTheScalarKernel is the evidence behind the search digest: a
// kernel that sums in a different order gives a different last bit, and this
// says how different. The bound is what says a cosine distance can only move
// in its last places, so the only orderings a new kernel can change are the
// ones that were already a tie.
func TestDotAgreesWithTheScalarKernel(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	var worst float64
	for range 5000 {
		a, b := randomUnitVectors(rng, 384)
		d := math.Abs(float64(dot32(a, b)) - float64(dotScalar(a, b)))
		if d > worst {
			worst = d
		}
	}
	t.Logf("over 5000 unit-length 384-dimensional pairs, the kernel and the scalar loop differ by at most %.3g", worst)
	if worst > 1e-6 {
		t.Fatalf("the kernel differs from the scalar loop by %v, far more than float32 rounding explains", worst)
	}
}

func randomVectors(rng *rand.Rand, dim int) ([]float32, []float32) {
	a, b := make([]float32, dim), make([]float32, dim)
	for i := range a {
		a[i] = float32(rng.NormFloat64())
		b[i] = float32(rng.NormFloat64())
	}
	return a, b
}

func randomUnitVectors(rng *rand.Rand, dim int) ([]float32, []float32) {
	a, b := randomVectors(rng, dim)
	for _, v := range [][]float32{a, b} {
		var n float64
		for _, x := range v {
			n += float64(x) * float64(x)
		}
		s := float32(1 / math.Sqrt(n))
		for i := range v {
			v[i] *= s
		}
	}
	return a, b
}

// BenchmarkDot384Kernel measures the kernel the index actually calls, at the
// production width. BenchmarkDot384Float32Unrolled in dot_bench_test.go is the
// scalar loop it replaced, kept as the control.
func BenchmarkDot384Kernel(b *testing.B) {
	rng := rand.New(rand.NewSource(3))
	x, y := randomVectors(rng, 384)
	var sink float32
	for range b.N {
		sink = dot32(x, y)
	}
	kernelSink = sink
}

var kernelSink float32
