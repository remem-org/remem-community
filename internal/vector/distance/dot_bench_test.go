package distance_test

import (
	"math/rand"
	"testing"
)

// Candidate dot-product loops at the production width, to choose between them
// by measurement. An HNSW insert spends over 90% of its time in the dot
// product (Phase 13 profile), so its loop is the write path's biggest lever.

var dotA, dotB = func() ([]float32, []float32) {
	rng := rand.New(rand.NewSource(3))
	a, b := make([]float32, 384), make([]float32, 384)
	for i := range a {
		a[i], b[i] = float32(rng.NormFloat64()), float32(rng.NormFloat64())
	}
	return a, b
}()

var sinkF64 float64
var sinkF32 float32

func dotF64(a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return dot
}

func dotF64Unrolled(a, b []float32) float64 {
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+4 <= len(a); i += 4 {
		s0 += float64(a[i]) * float64(b[i])
		s1 += float64(a[i+1]) * float64(b[i+1])
		s2 += float64(a[i+2]) * float64(b[i+2])
		s3 += float64(a[i+3]) * float64(b[i+3])
	}
	for ; i < len(a); i++ {
		s0 += float64(a[i]) * float64(b[i])
	}
	return s0 + s1 + s2 + s3
}

func dotF32Unrolled(a, b []float32) float32 {
	var s0, s1, s2, s3 float32
	i := 0
	b = b[:len(a)]
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

func BenchmarkDot384Float64(b *testing.B) {
	for range b.N {
		sinkF64 = dotF64(dotA, dotB)
	}
}

func BenchmarkDot384Float64Unrolled(b *testing.B) {
	for range b.N {
		sinkF64 = dotF64Unrolled(dotA, dotB)
	}
}

func BenchmarkDot384Float32Unrolled(b *testing.B) {
	for range b.N {
		sinkF32 = dotF32Unrolled(dotA, dotB)
	}
}

func TestDotCandidatesAgreeToSinglePrecision(t *testing.T) {
	want := dotF64(dotA, dotB)
	if got := dotF64Unrolled(dotA, dotB); abs64(got-want) > 1e-9 {
		t.Fatalf("unrolled float64 dot differs by %v", got-want)
	}
	if got := float64(dotF32Unrolled(dotA, dotB)); abs64(got-want) > 1e-3 {
		t.Fatalf("unrolled float32 dot differs by %v", got-want)
	}
}

func abs64(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
