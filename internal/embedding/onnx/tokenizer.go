//go:build cgo && onnx

package onnx

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/daulet/tokenizers"
	"github.com/remem-org/remem-go/internal/errs"
)

// tokenizer wraps the model's Qwen3 byte-level BPE tokenizer.
//
// The binding is used rather than a Go reimplementation because the tokenizer
// decides what the model sees, and byte-level BPE with Qwen's pre-tokenisation
// rules has enough corners that a reimplementation would agree on every test
// string and diverge on the next. Sharing the implementation leaves only the
// passage construction and the pooling, which the reference test holds.
//
// This tokenizer.json configures no truncation, no padding and no special
// tokens, which is what Remem needs: content is tokenised whole, once, and the
// passage construction inserts the separators itself. MiniLM's tokenizer
// truncated to 128 tokens inside the encode call, before Remem's own limit ever
// applied, and nothing reported it; the encode here asks for no special tokens
// and the reference test would notice a truncation as a missing passage.
type tokenizer struct {
	mu    sync.Mutex
	inner *tokenizers.Tokenizer
}

func openTokenizer(modelPath string) (*tokenizer, error) {
	const op = "onnx.openTokenizer"

	path := filepath.Join(modelPath, "tokenizer.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"no tokenizer at %s: run scripts/fetch-model.sh %s", path, modelPath))
	}
	inner, err := tokenizers.FromBytes(data)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("reading %s: %w", path, err))
	}
	return &tokenizer{inner: inner}, nil
}

func (t *tokenizer) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inner != nil {
		t.inner.Close()
		t.inner = nil
	}
}

// tokens is one text's token ids and, for each, the UTF-8 byte range of the
// text it came from.
//
// The offsets are bytes because the Rust tokenizer reports bytes and this
// binding passes them through — verified against the non-ASCII reference case,
// 2,079 bytes and 1,626 characters. Python's binding converts them to character
// indices, which is why the reference script converts back. A byte-level BPE
// token can be part of a character, so two adjacent tokens can report the same
// character's whole range; a span built from a first and a last token's offsets
// still falls on character boundaries.
type tokens struct {
	ids     []int64
	offsets [][2]uint32
}

func (t *tokenizer) encode(text string) (tokens, error) {
	const op = "onnx.encode"

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inner == nil {
		return tokens{}, errs.E(errs.Unavailable, op, errors.New("the tokenizer has been closed"))
	}
	enc, err := t.inner.EncodeWithOptionsErr(text, false, tokenizers.WithReturnOffsets())
	if err != nil {
		return tokens{}, errs.E(errs.Storage, op, fmt.Errorf("tokenising: %w", err))
	}
	if len(enc.Offsets) != len(enc.IDs) {
		return tokens{}, errs.E(errs.Storage, op, errors.New("the tokenizer returned offsets for a different number of tokens"))
	}
	out := tokens{ids: make([]int64, len(enc.IDs)), offsets: make([][2]uint32, len(enc.IDs))}
	for i, id := range enc.IDs {
		out.ids[i] = int64(id)
		out.offsets[i] = [2]uint32{uint32(enc.Offsets[i][0]), uint32(enc.Offsets[i][1])}
	}
	return out, nil
}
