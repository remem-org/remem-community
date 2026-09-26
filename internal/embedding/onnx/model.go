//go:build cgo && onnx

package onnx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
	ort "github.com/yalue/onnxruntime_go"
)

// Available reports whether this build can run the model.
const Available = true

// The graph's input and output names. They are constants rather than whatever
// the file happens to declare, because a model whose graph does not match is a
// different model, and binding to its names would produce vectors nobody could
// compare with the stored corpus.
const (
	inputIDs      = "input_ids"
	attentionMask = "attention_mask"
	tokenTypeIDs  = "token_type_ids"
	lastHidden    = "last_hidden_state"
)

// runtimeOnce guards ONNX Runtime's process-global environment. The library has
// exactly one, initialising it twice is an error, and destroying it while
// another session is open is a crash — so Remem initialises it once and never
// tears it down. A process that has loaded a model keeps it until it exits.
var (
	runtimeOnce sync.Once
	runtimeErr  error
)

// Model is an ONNX Runtime session over all-MiniLM-L6-v2.
type Model struct {
	tok    *tokenizer
	maxLen int

	// mu serialises session use. ONNX Runtime sessions are not documented as
	// safe for concurrent Run, and the batching service above already bounds
	// concurrency; a mutex here is the cheap way to be certain.
	mu      sync.Mutex
	session *ort.DynamicAdvancedSession
	closed  bool
}

var _ embedding.Embedder = (*Model)(nil)

// Open loads the model and its tokenizer.
func Open(cfg Config) (embedding.Embedder, error) {
	const op = "onnx.Open"

	if cfg.ModelPath == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"embedding.model_path is not set; run scripts/fetch-model.sh and point it at the directory"))
	}
	modelFile := filepath.Join(cfg.ModelPath, "model.onnx")
	if _, err := os.Stat(modelFile); err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"no model at %s: run scripts/fetch-model.sh %s", modelFile, cfg.ModelPath))
	}

	runtimeOnce.Do(func() {
		if cfg.SharedLibraryPath != "" {
			ort.SetSharedLibraryPath(cfg.SharedLibraryPath)
		}
		runtimeErr = ort.InitializeEnvironment()
	})
	if runtimeErr != nil {
		return nil, errs.E(errs.Unavailable, op, fmt.Errorf(
			"ONNX Runtime would not initialise: %w. The shared library must be on the loader path, "+
				"or embedding.onnx_library_path must name it", runtimeErr))
	}

	tok, err := openTokenizer(cfg.ModelPath)
	if err != nil {
		return nil, err
	}

	opts, err := ort.NewSessionOptions()
	if err != nil {
		tok.Close()
		return nil, errs.E(errs.Unavailable, op, err)
	}
	defer func() { _ = opts.Destroy() }()
	if cfg.IntraOpThreads > 0 {
		if err := opts.SetIntraOpNumThreads(cfg.IntraOpThreads); err != nil {
			tok.Close()
			return nil, errs.E(errs.Unavailable, op, err)
		}
	}

	session, err := ort.NewDynamicAdvancedSession(modelFile,
		[]string{inputIDs, attentionMask, tokenTypeIDs}, []string{lastHidden}, opts)
	if err != nil {
		tok.Close()
		return nil, errs.E(errs.Unavailable, op, fmt.Errorf("opening %s: %w", modelFile, err))
	}

	maxLen := cfg.MaxSequenceTokens
	if maxLen <= 0 {
		maxLen = embedding.MaxSequenceTokens
	}
	return &Model{tok: tok, maxLen: maxLen, session: session}, nil
}

func (m *Model) Dim() int        { return embedding.Dim }
func (m *Model) ModelID() string { return embedding.Model }

// Close releases the session and the tokenizer. It is idempotent.
func (m *Model) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	m.tok.Close()
	return m.session.Destroy()
}

// Embed runs one batch through the model and mean-pools the result.
func (m *Model) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out, err := m.forward(texts)
	if err != nil {
		return nil, err
	}
	return meanPool(out), nil
}

// forwardOutput is one batch's raw last hidden state, with the mask that says
// which positions are real.
type forwardOutput struct {
	hidden []float32 // batch * seq * dim, row-major
	mask   []int64   // batch * seq
	batch  int
	seq    int
	dim    int
}

// forward tokenises and runs the model, returning the raw last hidden state.
//
// Pooling is deliberately not part of it. That is what lets the reference test
// pool the same output two ways: mean, which is what Remem stores, and CLS,
// which is what Rust Remem stored — so the fixture still proves the model, the
// tokenizer and the mask are all exactly right, and pins the difference between
// the two implementations to the one decision that was actually taken.
func (m *Model) forward(texts []string) (forwardOutput, error) {
	const op = "onnx.Embed"

	if len(texts) == 0 {
		return forwardOutput{}, errs.E(errs.Invalid, op, embedding.ErrNoTexts)
	}

	encoded, err := m.tok.encodeBatch(texts, m.maxLen)
	if err != nil {
		return forwardOutput{}, err
	}

	batch, seq, dim := len(texts), encoded.width, embedding.Dim
	ids := make([]int64, batch*seq)
	mask := make([]int64, batch*seq)
	types := make([]int64, batch*seq)
	for i, e := range encoded.rows {
		copy(ids[i*seq:], e.ids)
		copy(mask[i*seq:], e.mask)
		copy(types[i*seq:], e.typeIDs)
	}

	shape := ort.NewShape(int64(batch), int64(seq))
	idsT, err := ort.NewTensor(shape, ids)
	if err != nil {
		return forwardOutput{}, errs.E(errs.Storage, op, err)
	}
	defer func() { _ = idsT.Destroy() }()
	maskT, err := ort.NewTensor(shape, mask)
	if err != nil {
		return forwardOutput{}, errs.E(errs.Storage, op, err)
	}
	defer func() { _ = maskT.Destroy() }()
	typesT, err := ort.NewTensor(shape, types)
	if err != nil {
		return forwardOutput{}, errs.E(errs.Storage, op, err)
	}
	defer func() { _ = typesT.Destroy() }()

	hidden, err := ort.NewEmptyTensor[float32](ort.NewShape(int64(batch), int64(seq), int64(dim)))
	if err != nil {
		return forwardOutput{}, errs.E(errs.Storage, op, err)
	}
	defer func() { _ = hidden.Destroy() }()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return forwardOutput{}, errs.E(errs.Unavailable, op, errors.New("the model has been closed"))
	}
	err = m.session.Run([]ort.Value{idsT, maskT, typesT}, []ort.Value{hidden})
	m.mu.Unlock()
	if err != nil {
		return forwardOutput{}, errs.E(errs.Storage, op, fmt.Errorf("running the model: %w", err))
	}

	return forwardOutput{
		hidden: append([]float32(nil), hidden.GetData()...),
		mask:   mask,
		batch:  batch,
		seq:    seq,
		dim:    dim,
	}, nil
}

// meanPool averages each sequence's token vectors, weighted by the attention
// mask, then normalises.
//
// The mask is what makes this correct, and getting it wrong is the failure this
// package's doc comment warns about. Padding positions carry real numbers — the
// model computes something for them — so averaging them in shifts every vector
// by an amount that depends on how much padding the batch happened to need.
// This model's tokenizer pads to a fixed 128 tokens, so a five-word memory is
// 123 parts padding and 5 parts content: a mask built from the length of the id
// slice does not produce a slightly wrong vector, it produces a wrong one, at
// the right width and the right norm.
//
// Normalisation happens after pooling, never before. Normalising each token and
// then averaging is a different operation with an equally plausible result.
//
// The divisor is clamped away from zero for the reason Rust's is: a text that
// tokenised to nothing but padding would otherwise divide by zero and put NaNs
// into the index.
func meanPool(o forwardOutput) [][]float32 {
	out := make([][]float32, o.batch)
	for b := range o.batch {
		v := make([]float32, o.dim)
		var kept float32
		for s := range o.seq {
			if o.mask[b*o.seq+s] == 0 {
				continue
			}
			kept++
			row := o.hidden[(b*o.seq+s)*o.dim:]
			for d := range o.dim {
				v[d] += row[d]
			}
		}
		if kept < 1e-9 {
			kept = 1e-9
		}
		for d := range o.dim {
			v[d] /= kept
		}
		out[b] = embedding.Normalise(v)
	}
	return out
}
