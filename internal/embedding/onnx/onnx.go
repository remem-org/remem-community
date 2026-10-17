// Package onnx runs pplx-embed-context-v1-0.6b through ONNX Runtime.
//
// It is the only package that may import onnxruntime_go or the tokenizer
// bindings; internal/arch fails the build otherwise. Everything above it holds
// an embedding.Embedder and cannot tell what is behind it.
//
// # Build tags
//
// The implementation is behind `cgo && onnx` because it links against a shared
// library and a static archive that a plain `go build` has no way to find. The
// tagless build gets [Open] returning an error that says what to run, which
// keeps `go build ./...` working on a machine that has never fetched a model.
// The passage arithmetic (plan.go) is untagged, so its tests run everywhere.
//
// # What is easy to get wrong
//
//  1. The separator and the pad are the same token. Passage boundaries are
//     positions recorded when a sequence is built, never found by looking for
//     the separator's id, and padding is told apart only by the mask.
//  2. Token ids are sliced into passages; text never is. Re-tokenising a
//     substring can change the tokens at its edges, and a passage's vector
//     would then depend on where it was cut out.
//  3. Each passage is pooled over its own span of its segment's forward pass,
//     and the document vector over every content token of the first segment —
//     separators excluded in both.
//  4. The output rule is round(tanh(x)·127) with rounding half to even, then
//     L2 normalisation. The raw pooled floats are not what the model was
//     trained to compare.
//
// Every one of these still produces 1,024 unit-length floats when it is wrong.
// The test that holds them all is the agreement test against
// fixtures/pplx_embedding_reference.json, written by an independent Python
// implementation, at cosine >= 0.9999 per document and per passage.
package onnx

// Config describes where the model lives and how it is run.
type Config struct {
	// ModelPath is the directory scripts/fetch-model.sh fills: onnx/model.onnx
	// with its two data files, and tokenizer.json.
	ModelPath string
	// SharedLibraryPath is the ONNX Runtime shared library. Empty means the
	// platform default, which is what a Docker image with ldconfig run gets.
	SharedLibraryPath string
	// BatchSize bounds how many sequences one forward pass holds. A pass is
	// also bounded by tokens, which is what limits a pass of long segments
	// (see packPasses). Zero takes embedding.DefaultBatchSize.
	BatchSize int
	// IntraOpThreads bounds ONNX Runtime's own parallelism. Zero leaves the
	// runtime's default, which counts the host's cores and ignores a container
	// CPU quota; the server never passes zero, it resolves the count first
	// (embedding.IntraOpThreads).
	IntraOpThreads int
}
