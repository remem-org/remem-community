//go:build arm64 && !purego

package distance

// dot32 is the dot product in single precision, in NEON — see dot_arm64.s.
//
// The assembly takes pointers and a count rather than two slices, so that the
// one bounds check and the one empty case stay here in Go where they can be
// read. Pinning b's length to a's keeps the panic a caller already gets for a
// short b, and makes a[0] and b[0] both safe once the length is known positive.
func dot32(a, b []float32) float32 {
	b = b[:len(a)]
	if len(a) == 0 {
		return 0
	}
	return dotNEON(&a[0], &b[0], len(a))
}

// dotNEON sums n products of the floats at a and b.
//
//go:noescape
func dotNEON(a, b *float32, n int) float32
