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
	lastHidden    = "last_hidden_state"
)

// graphFile is the fp32 export, relative to the model directory. Its weights are
// in two data files beside it, which ONNX Runtime resolves relative to the
// graph. The int8-weight export (onnx/model_quantized.onnx) is a different
// vector producer and was slower on every input measured; see
// docs/architecture/retrieval.md.
const graphFile = "onnx/model.onnx"

// runtimeOnce guards ONNX Runtime's process-global environment. The library has
// exactly one, initialising it twice is an error, and destroying it while
// another session is open is a crash — so Remem initialises it once and never
// tears it down. A process that has loaded a model keeps it until it exits.
var (
	runtimeOnce sync.Once
	runtimeErr  error
)

// Model is an ONNX Runtime session over pplx-embed-context-v1-0.6b.
type Model struct {
	tok     *tokenizer
	maxRows int

	// gate admits one pass at a time — ONNX Runtime sessions are not documented
	// as safe for concurrent Run — and admits a waiting query before a waiting
	// document pass, so a search never waits behind the rest of a long write.
	// closed is read and written only while holding it.
	gate    *passGate
	session *ort.DynamicAdvancedSession
	closed  bool

	// shrink asks ONNX Runtime's CPU arena to return what a pass no longer
	// needs, and is passed to document passes only. Measured on a full
	// segment: with the arena alone the process kept 5.2 GiB after the pass
	// (1.8 GiB before it); with the arena off the pass was 43% slower and the
	// next queries slower too; shrinking after the pass kept the arena's speed
	// (19.4 s against 18.3 s) and held 3.5 GiB. Query passes run without it,
	// because they are short and latency is what they are for.
	shrink *ort.RunOptions
}

var _ embedding.Embedder = (*Model)(nil)

// Open loads the model and its tokenizer.
func Open(cfg Config) (embedding.Embedder, error) {
	const op = "onnx.Open"

	if cfg.ModelPath == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"embedding.model_path is not set; run scripts/fetch-model.sh and point it at the directory"))
	}
	modelFile := filepath.Join(cfg.ModelPath, graphFile)
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
		[]string{inputIDs, attentionMask}, []string{lastHidden}, opts)
	if err != nil {
		tok.Close()
		return nil, errs.E(errs.Unavailable, op, fmt.Errorf("opening %s: %w", modelFile, err))
	}

	shrink, err := ort.NewRunOptions()
	if err == nil {
		err = shrink.AddRunConfigEntry("memory.enable_memory_arena_shrinkage", "cpu:0")
	}
	if err != nil {
		_ = session.Destroy()
		tok.Close()
		return nil, errs.E(errs.Unavailable, op, fmt.Errorf("configuring arena shrinkage: %w", err))
	}

	maxRows := cfg.BatchSize
	if maxRows <= 0 {
		maxRows = embedding.DefaultBatchSize
	}
	return &Model{tok: tok, maxRows: maxRows, gate: newPassGate(), session: session, shrink: shrink}, nil
}

func (m *Model) Dim() int        { return embedding.Dim }
func (m *Model) ModelID() string { return embedding.Model }

// Close releases the session and the tokenizer. It is idempotent, and it waits
// for a running pass rather than destroying the session under it.
func (m *Model) Close() error {
	m.gate.acquire(true)
	defer m.gate.release()
	if m.closed {
		return nil
	}
	m.closed = true
	m.tok.Close()
	_ = m.shrink.Destroy()
	return m.session.Destroy()
}

// EmbedDocuments embeds each text whole, as passages.
//
// Every passage of every text is embedded; nothing is truncated below the
// content size limit. A text that fits one passage gets its document vector and
// no passages, because that passage's vector is the document vector.
func (m *Model) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	return m.embed(ctx, "onnx.EmbedDocuments", texts, true)
}

// EmbedQueries returns one vector per text: its document vector.
//
// A query is a one-passage document, so for any query up to 256 tokens this is
// exactly the vector the same text would get as a memory's content. A longer
// query is pooled over its first segment, as a long memory's document vector
// is; nothing beyond the first segment is computed.
func (m *Model) EmbedQueries(ctx context.Context, texts []string) ([][]float32, error) {
	docs, err := m.embed(ctx, "onnx.EmbedQueries", texts, false)
	if err != nil {
		return nil, err
	}
	out := make([][]float32, len(docs))
	for i, d := range docs {
		out[i] = d.Vector
	}
	return out, nil
}

// pending is one document being embedded: its tokens, its plan, and the vectors
// as passes complete.
type pending struct {
	tokens   tokens
	plan     plan
	document []float64   // pooled over segment 0's content tokens
	passages [][]float64 // pooled in the owning segment
}

func (m *Model) embed(ctx context.Context, op string, texts []string, wantPassages bool) ([]embedding.Document, error) {
	if len(texts) == 0 {
		return nil, errs.E(errs.Invalid, op, embedding.ErrNoTexts)
	}

	docs := make([]pending, len(texts))
	var seqs []sequence
	for i, text := range texts {
		tk, err := m.tok.encode(text)
		if err != nil {
			return nil, err
		}
		if len(tk.ids) == 0 {
			// An empty text has no token to pool. It gets the separator's
			// state instead: a stable vector rather than a zero one, so it does
			// not compare equal to everything, and never NaN.
			tk = tokens{ids: []int64{separatorID}, offsets: [][2]uint32{{0, 0}}}
		}
		d := pending{tokens: tk, plan: planDocument(len(tk.ids))}
		segments := len(d.plan.segments)
		if !wantPassages || len(d.plan.passages) == 1 {
			segments = 1 // the document vector needs segment 0 only
		}
		if wantPassages && len(d.plan.passages) > 1 {
			d.passages = make([][]float64, len(d.plan.passages))
		}
		for s := range segments {
			seqs = append(seqs, buildSequence(i, s, tk.ids, d.plan))
		}
		docs[i] = d
	}

	lengths := make([]int, len(seqs))
	for i, s := range seqs {
		lengths[i] = len(s.ids)
	}
	for _, pass := range packPasses(lengths, m.maxRows, embedding.SegmentTokens) {
		if err := ctx.Err(); err != nil {
			return nil, errs.E(errs.Unavailable, op, err)
		}
		rows := make([]sequence, len(pass))
		for r, i := range pass {
			rows[r] = seqs[i]
		}
		if err := m.run(op, rows, docs, !wantPassages); err != nil {
			return nil, err
		}
	}

	out := make([]embedding.Document, len(docs))
	for i, d := range docs {
		out[i].Vector = quantise(d.document)
		for p, v := range d.passages {
			start := d.tokens.offsets[d.plan.passages[p].start][0]
			end := d.tokens.offsets[d.plan.passages[p].end-1][1]
			out[i].Passages = append(out[i].Passages, embedding.Passage{Start: start, End: end, Vector: quantise(v)})
		}
	}
	return out, nil
}

// run executes one forward pass and pools each row into the documents it came
// from. Rows are padded on the right to the pass's longest with the pad id and
// mask 0; the spans being pooled never reach a padded position, and the mask
// keeps the padding out of every content token's attention.
func (m *Model) run(op string, rows []sequence, docs []pending, query bool) error {
	width := 0
	for _, r := range rows {
		width = max(width, len(r.ids))
	}
	n := len(rows)
	ids := make([]int64, n*width)
	mask := make([]int64, n*width)
	for r, row := range rows {
		copy(ids[r*width:], row.ids)
		for c := len(row.ids); c < width; c++ {
			ids[r*width+c] = separatorID
		}
		for c := range row.ids {
			mask[r*width+c] = 1
		}
	}

	shape := ort.NewShape(int64(n), int64(width))
	idsT, err := ort.NewTensor(shape, ids)
	if err != nil {
		return errs.E(errs.Storage, op, err)
	}
	defer func() { _ = idsT.Destroy() }()
	maskT, err := ort.NewTensor(shape, mask)
	if err != nil {
		return errs.E(errs.Storage, op, err)
	}
	defer func() { _ = maskT.Destroy() }()
	hiddenT, err := ort.NewEmptyTensor[float32](ort.NewShape(int64(n), int64(width), embedding.Dim))
	if err != nil {
		return errs.E(errs.Storage, op, err)
	}
	defer func() { _ = hiddenT.Destroy() }()

	m.gate.acquire(query)
	if m.closed {
		m.gate.release()
		return errs.E(errs.Unavailable, op, errors.New("the model has been closed"))
	}
	if query {
		err = m.session.Run([]ort.Value{idsT, maskT}, []ort.Value{hiddenT})
	} else {
		err = m.session.RunWithOptions([]ort.Value{idsT, maskT}, []ort.Value{hiddenT}, m.shrink)
	}
	m.gate.release()
	if err != nil {
		return errs.E(errs.Storage, op, fmt.Errorf("running the model: %w", err))
	}

	hidden := hiddenT.GetData()
	for r, row := range rows {
		d := &docs[row.doc]
		if row.segment == 0 {
			d.document = meanOver(hidden, r, width, embedding.Dim, row.spans...)
		}
		if d.passages == nil {
			continue
		}
		first := d.plan.segments[row.segment].first
		for k, sp := range row.spans {
			p := first + k
			if d.plan.owner(p) == row.segment {
				d.passages[p] = meanOver(hidden, r, width, embedding.Dim, sp)
			}
		}
	}
	return nil
}
