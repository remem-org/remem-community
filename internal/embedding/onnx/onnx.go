// Package onnx runs all-MiniLM-L6-v2 through ONNX Runtime.
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
//
// # The three things that are easy to get wrong
//
//  1. Mean pooling must use the attention mask. Averaging over padding
//     positions produces a vector that is the right width, the right norm, and
//     a different point in space — one that no dimension or norm check
//     catches.
//  2. Normalisation happens after pooling, never before. Normalising each
//     token then averaging is a different operation with a plausible result.
//  3. Every sequence in a batch is padded to the batch's longest, and the mask
//     must reflect that padding, not the model's maximum length.
//
// The test that holds all three is the agreement test against the 200 vectors
// the Rust implementation produced, at cosine >= 0.9999. Nothing weaker would
// catch any of them.
package onnx

// Config describes where the model lives and how it is run.
type Config struct {
	// ModelPath is the directory holding model.onnx and tokenizer.json.
	ModelPath string
	// SharedLibraryPath is the ONNX Runtime shared library. Empty means the
	// platform default, which is what a Docker image with ldconfig run gets.
	SharedLibraryPath string
	// MaxSequenceTokens truncates longer input. Zero takes the model default.
	MaxSequenceTokens int
	// IntraOpThreads bounds ONNX Runtime's own parallelism. Zero leaves the
	// runtime's default, which is one thread per core — too many when several
	// batches run at once.
	IntraOpThreads int
}
