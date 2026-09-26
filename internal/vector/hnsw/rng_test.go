package hnsw_test

import "math"

// testRNG is a linear congruential generator: a failure reproduces from the
// seed printed in the test rather than from whatever math/rand was doing.
type testRNG struct{ s uint64 }

func newTestRNG(seed uint64) *testRNG {
	return &testRNG{s: seed*6364136223846793005 + 1442695040888963407}
}

func (r *testRNG) next() float32 {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	return float32(int64(r.s>>33))/float32(1<<30) - 1
}

func (r *testRNG) unit(dim int) []float32 {
	v := make([]float32, dim)
	var sum float64
	for i := range v {
		v[i] = r.next()
		sum += float64(v[i]) * float64(v[i])
	}
	if sum == 0 {
		v[0] = 1
		return v
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return v
}
