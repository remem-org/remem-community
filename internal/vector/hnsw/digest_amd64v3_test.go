//go:build amd64.v3

package hnsw

// amd64 at GOAMD64=v3 or v4 (the amd64.v3 tag is set for both), where the
// compiler may emit FMA. Measured, not derived: the L2 digest becomes arm64's,
// so the L2 kernel is fused, while the cosine digest stays the v1 one, so the
// cosine kernel is not. This pair is neither of the other two, which is why it
// has a file of its own rather than sharing one with them.
var recordedDigests = walkDigests{
	l2:     "ee50f764d879162a6aaf9773c3ca193931dccaafe0d1fbb3e3bf2d370390cec3",
	cosine: "419a011034a65cdb92e571659c008cda3599b906f1c265b104250bf7c0c5ffc6",
	covers: "amd64 v3+, L2 fused, cosine not",
}
