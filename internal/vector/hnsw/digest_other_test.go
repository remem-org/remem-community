//go:build !amd64 && !arm64

package hnsw

// Nothing is recorded here, so TestSearchDigestIsUnchanged skips and prints the
// digest it computed rather than passing without a comparison.
var recordedDigests = walkDigests{covers: "no digest recorded"}
