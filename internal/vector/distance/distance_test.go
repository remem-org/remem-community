package distance_test

import (
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// TestCosineRecoveryMatchesTheRustContract is plan Task 3.4's test, and it is
// the whole reason this package exists separately from the index.
func TestCosineRecoveryMatchesTheRustContract(t *testing.T) {
	if got := distance.CosineFrom(distance.L2, 0.0); math.Abs(float64(got)-1.0) > 1e-6 {
		t.Fatalf("identical vectors must recover cosine 1.0, got %f", got)
	}
	if got := distance.CosineFrom(distance.L2, 2.0); math.Abs(float64(got)) > 1e-6 {
		t.Fatalf("orthogonal unit vectors must recover cosine 0.0, got %f", got)
	}
	if _, ok := distance.CosineFromOK(distance.DotProduct, 0.5); ok {
		t.Fatal("dot product must not report a cosine — the index does not guarantee normalisation")
	}
}

func TestCosineRecoveryUnderTheCosineMetric(t *testing.T) {
	for d, want := range map[float32]float32{0: 1, 0.5: 0.5, 1: 0} {
		if got := distance.CosineFrom(distance.Cosine, d); math.Abs(float64(got-want)) > 1e-6 {
			t.Errorf("CosineFrom(Cosine, %v) = %v, want %v", d, got, want)
		}
	}
}

// A recovered cosine is shown to users, so it must stay inside [0, 1] however
// the float arithmetic lands. A similarity of 1.0000001 is a support question.
func TestRecoveredCosineIsClamped(t *testing.T) {
	if got := distance.CosineFrom(distance.L2, -1e-7); got > 1 {
		t.Fatalf("got %v, want at most 1", got)
	}
	if got := distance.CosineFrom(distance.L2, 4.5); got < 0 {
		t.Fatalf("got %v, want at least 0", got)
	}
}

// This is the property the baseline records at REM-74: the ordering value
// floors at one third, which is why it is not the reported relevance.
func TestScoreFloorsWhereRelevanceDoesNot(t *testing.T) {
	orthogonal := float32(2.0) // squared L2 between orthogonal unit vectors
	if s := distance.Score(orthogonal); math.Abs(float64(s)-1.0/3.0) > 1e-6 {
		t.Fatalf("Score(2.0) = %v, want 1/3 — the floor that makes it unusable as a threshold", s)
	}
	if r := distance.Relevance(distance.L2, orthogonal); r > 1e-6 {
		t.Fatalf("Relevance(L2, 2.0) = %v, want ~0", r)
	}
}

func TestRelevanceFallsBackWhereCosineCannotBeRecovered(t *testing.T) {
	got := distance.Relevance(distance.DotProduct, 1.0)
	if math.Abs(float64(got)-0.5) > 1e-6 {
		t.Fatalf("Relevance(DotProduct, 1.0) = %v, want the ordering value 0.5", got)
	}
}

func TestEveryMetricOrdersSmallestFirst(t *testing.T) {
	a := unit([]float32{1, 0, 0})
	near := unit([]float32{0.9, 0.1, 0})
	far := unit([]float32{0, 1, 0})

	for _, m := range []distance.Metric{distance.L2, distance.Cosine, distance.DotProduct} {
		dNear, err := distance.Compute(m, a, near)
		if err != nil {
			t.Fatal(err)
		}
		dFar, err := distance.Compute(m, a, far)
		if err != nil {
			t.Fatal(err)
		}
		if dNear >= dFar {
			t.Errorf("%s: the nearer vector is %v and the further one %v; smaller must mean closer", m, dNear, dFar)
		}
	}
}

func TestIdenticalUnitVectorsAreZeroApartUnderL2AndCosine(t *testing.T) {
	a := unit([]float32{0.3, -0.4, 0.5})
	for _, m := range []distance.Metric{distance.L2, distance.Cosine} {
		d, err := distance.Compute(m, a, a)
		if err != nil {
			t.Fatal(err)
		}
		if math.Abs(float64(d)) > 1e-6 {
			t.Errorf("%s: a vector is %v from itself", m, d)
		}
	}
}

// A vector of the wrong width means a vector from another model reached the
// index. Comparing the first 384 of 768 components would produce a ranking
// nobody could explain.
func TestMismatchedWidthsAreRefused(t *testing.T) {
	_, err := distance.Compute(distance.L2, []float32{1, 0}, []float32{1, 0, 0})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestAnUnknownMetricIsRefused(t *testing.T) {
	if _, err := distance.Compute(distance.Metric(99), []float32{1}, []float32{1}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

// A zero vector has no direction. Maximum distance is the only answer that
// cannot mislead: it matches nothing rather than matching everything.
func TestAZeroVectorIsMaximallyDistantUnderCosine(t *testing.T) {
	if got := distance.CosineDistance([]float32{0, 0}, []float32{1, 0}); got != 1 {
		t.Fatalf("got %v, want 1", got)
	}
}

func TestMetricNamesRoundTrip(t *testing.T) {
	for _, name := range []string{"l2", "cosine", "dot"} {
		m, err := distance.Parse(name)
		if err != nil {
			t.Fatalf("Parse(%q): %v", name, err)
		}
		if m.String() != name {
			t.Fatalf("Parse(%q).String() = %q", name, m.String())
		}
	}
	if _, err := distance.Parse("euclidean"); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

// Score must stay a usable ordering value under dot product, where a negated
// inner product can be negative.
func TestScoreStaysBoundedForNegativeDistances(t *testing.T) {
	if got := distance.Score(-1); got != 1 {
		t.Fatalf("Score(-1) = %v; a negative distance must not produce a division by zero", got)
	}
}

func unit(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return v
}
