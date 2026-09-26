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

// tokenizer wraps the Hugging Face WordPiece tokenizer that produced the Rust
// corpus.
//
// The binding is used rather than a Go reimplementation for one reason: the
// tokenizer decides what the model sees, and BERT's normalisation — lowercase,
// accent stripping, CJK character splitting, punctuation handling — has enough
// corners that a reimplementation would agree on the 200 reference strings and
// diverge on the 201st. Sharing the implementation removes that whole class of
// difference and leaves only pooling, which is testable.
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

// encoding is one tokenised text, padded to the batch width.
type encoding struct {
	ids     []int64
	mask    []int64
	typeIDs []int64
}

type batchEncoding struct {
	rows  []encoding
	width int
}

// encodeBatch tokenises every text and pads them to the longest.
//
// The attention mask comes from the tokenizer, never from the length of the id
// slice. That distinction is the whole task: tokenizer.json for this model
// configures fixed padding to 128 tokens, so a five-word memory arrives as 128
// ids of which 123 are [PAD]. Deriving the mask from len(ids) marks all 128 as
// real, the model then attends to the padding, and every vector comes out
// wrong — plausibly wrong, at the right width and the right norm.
//
// The padding the tokenizer applies is also stripped here, and the batch is
// padded to its own longest sequence instead. The cost of a run is quadratic in
// sequence length and most memories are short, so a batch of one-line memories
// held at 128 columns does many times the work for the same answer. It is also
// what the Rust pipeline does, and a difference in sequence width is a
// difference in the vector.
func (t *tokenizer) encodeBatch(texts []string, maxLen int) (batchEncoding, error) {
	const op = "onnx.encodeBatch"

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inner == nil {
		return batchEncoding{}, errs.E(errs.Unavailable, op, errors.New("the tokenizer has been closed"))
	}

	type row struct{ ids, mask, types []uint32 }
	raw := make([]row, len(texts))
	width := 1 // never zero: a batch of empty strings still needs one column
	for i, text := range texts {
		enc := t.inner.EncodeWithOptions(text, true, // true: add [CLS] and [SEP]
			tokenizers.WithReturnAttentionMask(), tokenizers.WithReturnTypeIDs())

		n := trueLength(enc.AttentionMask, len(enc.IDs))
		if n > maxLen {
			n = maxLen
		}
		raw[i] = row{ids: enc.IDs[:n], mask: enc.AttentionMask[:n], types: clip(enc.TypeIDs, n)}
		if n > width {
			width = n
		}
	}

	out := batchEncoding{rows: make([]encoding, len(texts)), width: width}
	for i, r := range raw {
		e := encoding{
			ids:     make([]int64, width),
			mask:    make([]int64, width),
			typeIDs: make([]int64, width),
		}
		for j := range r.ids {
			e.ids[j] = int64(r.ids[j])
			e.mask[j] = int64(r.mask[j])
			if j < len(r.types) {
				e.typeIDs[j] = int64(r.types[j])
			}
		}
		// Positions beyond the text keep id 0 ([PAD]), mask 0 and type 0.
		out.rows[i] = e
	}
	return out, nil
}

// trueLength is how many leading positions the attention mask marks as real.
//
// A tokenizer with no mask configured returns none, and every position is then
// real — which is correct for a tokenizer that also applied no padding. The
// fallback is deliberate rather than an assumption: it is only reached when
// there is nothing to strip.
func trueLength(mask []uint32, ids int) int {
	if len(mask) == 0 {
		return ids
	}
	n := 0
	for _, m := range mask {
		if m == 0 {
			break
		}
		n++
	}
	return n
}

func clip(v []uint32, n int) []uint32 {
	if len(v) < n {
		return v
	}
	return v[:n]
}
