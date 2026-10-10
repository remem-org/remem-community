//go:build amd64 && !amd64.v3

package hnsw

// amd64 at GOAMD64=v1 or v2, Go's default: no fused multiply-add anywhere, so
// every distance rounds after the multiply and again after the add. Measured to
// be the digest the pre-Tier-2 walk produces too, which is what shows the pooled
// working set and the hand-sifted heaps changed nothing on this target.
var recordedDigests = walkDigests{
	l2:     "470ae38a41ef4dd4f0c1e9e884336aaffbbd7f4cb21a66d0c619651941ee08b1",
	cosine: "419a011034a65cdb92e571659c008cda3599b906f1c265b104250bf7c0c5ffc6",
	covers: "amd64 v1-v2, no fused multiply-add",
}
