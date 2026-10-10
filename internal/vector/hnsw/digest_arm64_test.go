//go:build arm64 && !purego

package hnsw

// arm64 with the NEON dot product (distance/dot_arm64.s), where Go also fuses
// multiply-add in the scalar code. Recorded by the Tier 3 author on Apple
// silicon. The L2 value is also what amd64 produces at GOAMD64=v3, which
// confirms it from a second architecture; the cosine value differs from the
// purego one by the last-bit tolerance search_digest_test.go measures.
var recordedDigests = walkDigests{
	l2:     "ee50f764d879162a6aaf9773c3ca193931dccaafe0d1fbb3e3bf2d370390cec3",
	cosine: "c88db3832f600559bf02d4bac9090baec4ee94922852d41dbbb5169d5ba10577",
	covers: "arm64, NEON dot product, fused multiply-add",
}
