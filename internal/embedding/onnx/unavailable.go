//go:build !cgo || !onnx

package onnx

import (
	"errors"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
)

// Available reports whether this build can run the model.
const Available = false

// Open reports that this binary was built without the embedder.
//
// It is an error rather than a silent fallback to a stub embedder. A server
// that quietly produced meaningless vectors would fill a durable corpus with
// them, and every one would carry a model id claiming otherwise.
func Open(Config) (embedding.Embedder, error) {
	return nil, errs.E(errs.Unavailable, "onnx.Open", errors.New(
		"this binary was built without the ONNX embedder. Build with -tags onnx and CGO_ENABLED=1, "+
			"after running scripts/fetch-model.sh to fetch the model and the tokenizer library"))
}
