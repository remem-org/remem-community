//go:build !arm64 || purego

package distance

// dot32 is the dot product in single precision, four accumulators wide.
//
// This is the portable kernel: every architecture without an assembly one, and
// any build that asks for no assembly with the `purego` tag. Four independent
// accumulators are what a plain `sum += a[i]*b[i]` cannot have — every add
// would wait on the one before it — and they are the reason this loop measured
// 145ns at 384 dimensions against 355ns for the obvious float64 version. See
// [CosineWithNorms] for why it is single precision at all.
//
// The slicing is a bounds check hoisted out of the loop: with b's length pinned
// to a's the compiler proves b[i] is in range from a[i] being in range.
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
