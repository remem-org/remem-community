package embedding_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
)

func newService(t *testing.T, inner embedding.Embedder, cfg embedding.ServiceConfig) *embedding.Service {
	t.Helper()
	s := embedding.NewService(inner, cfg)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestABatchOfTwentyIsOneCallToTheModel(t *testing.T) {
	fake := embeddingtest.New()
	s := newService(t, fake, embedding.ServiceConfig{})

	reqs := make([]string, 20)
	for i := range reqs {
		reqs[i] = fmt.Sprintf("memory %d", i)
	}
	got, err := s.EmbedQueries(context.Background(), reqs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 20 {
		t.Fatalf("got %d vectors", len(got))
	}
	if fake.Calls() != 1 {
		t.Fatalf("embedded in %d calls, want 1 batch", fake.Calls())
	}
}

// More texts than one batch holds must still be embedded, and in the right
// order. The order is the part worth asserting: results arrive per batch, and
// reassembling them wrongly would attach every memory to the wrong vector.
func TestMoreTextsThanOneBatchAreEmbeddedInOrder(t *testing.T) {
	fake := embeddingtest.New()
	s := newService(t, fake, embedding.ServiceConfig{BatchSize: 4})

	texts := make([]string, 21)
	for i := range texts {
		texts[i] = fmt.Sprintf("memory %d", i)
	}
	got, err := s.EmbedQueries(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := embeddingtest.New().EmbedQueries(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range texts {
		if cosine(got[i], direct[i]) < 0.9999999 {
			t.Fatalf("vector %d is not the vector for %q", i, texts[i])
		}
	}
	if fake.Calls() < 2 {
		t.Fatalf("21 texts with a batch size of 4 took %d calls", fake.Calls())
	}
}

func TestTheCacheServesARepeatedText(t *testing.T) {
	fake := embeddingtest.New()
	s := newService(t, fake, embedding.ServiceConfig{})
	ctx := context.Background()

	if _, err := s.EmbedQueries(ctx, []string{"the sky is blue"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EmbedQueries(ctx, []string{"the sky is blue"}); err != nil {
		t.Fatal(err)
	}
	if fake.Calls() != 1 {
		t.Fatalf("the same text was embedded %d times", fake.Calls())
	}
	hits, misses, size := s.CacheStats()
	if hits != 1 || misses != 1 || size != 1 {
		t.Fatalf("cache stats: hits=%d misses=%d size=%d", hits, misses, size)
	}
}

// A caller that mutates a vector must not corrupt the cache. The bug this
// prevents does not fail loudly: it degrades every later search that hits the
// same entry.
func TestTheCacheReturnsACopy(t *testing.T) {
	s := newService(t, embeddingtest.New(), embedding.ServiceConfig{})
	ctx := context.Background()

	first, err := s.EmbedQueries(ctx, []string{"mutate me"})
	if err != nil {
		t.Fatal(err)
	}
	original := first[0][0]
	for i := range first[0] {
		first[0][i] = 999
	}

	second, err := s.EmbedQueries(ctx, []string{"mutate me"})
	if err != nil {
		t.Fatal(err)
	}
	if second[0][0] != original {
		t.Fatalf("the cached vector was mutated by a caller: %v", second[0][0])
	}
}

// Duplicates within one request cost one embedding, not several. An agent
// storing the same fact twice in a batch is common.
func TestDuplicatesWithinOneRequestAreEmbeddedOnce(t *testing.T) {
	fake := embeddingtest.New()
	s := newService(t, fake, embedding.ServiceConfig{})

	got, err := s.EmbedQueries(context.Background(), []string{"same", "other", "same", "same"})
	if err != nil {
		t.Fatal(err)
	}
	if fake.Texts() != 2 {
		t.Fatalf("the model saw %d texts, want 2 distinct ones", fake.Texts())
	}
	if cosine(got[0], got[2]) < 0.9999999 || cosine(got[0], got[3]) < 0.9999999 {
		t.Fatal("duplicate texts did not receive the same vector")
	}
}

// Concurrent single-text callers are what batching is for: fifty sessions each
// storing one memory must not be fifty model runs.
func TestConcurrentCallersAreCoalescedIntoBatches(t *testing.T) {
	fake := &countingEmbedder{inner: embeddingtest.New()}
	s := newService(t, fake, embedding.ServiceConfig{FillWindow: 25 * time.Millisecond})

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.EmbedQueries(context.Background(), []string{fmt.Sprintf("memory %d", i)}); err != nil {
				t.Errorf("Embed: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := fake.calls(); got > 25 {
		t.Fatalf("50 concurrent single-text callers produced %d model runs; coalescing is not happening", got)
	}
}

// An isolated request is the common case behind a search or a single create,
// and the fill window is all of its added latency: there is no company coming
// for it to wait for.
func TestAnIsolatedRequestIsNotHeldForTheFillWindow(t *testing.T) {
	const window = 500 * time.Millisecond
	s := newService(t, embeddingtest.New(), embedding.ServiceConfig{FillWindow: window})

	start := time.Now()
	if _, err := s.EmbedQueries(context.Background(), []string{"a lone memory"}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Fatalf("an isolated Embed took %v with a fill window of %v; a batch of one should not wait for company", took, window)
	}
}

// Dispatching a lone request at once must not cost the coalescing a burst
// depends on, however long the window is.
func TestABurstStillCoalescesWithALongFillWindow(t *testing.T) {
	const (
		window  = 500 * time.Millisecond
		callers = 20
	)
	fake := &countingEmbedder{inner: embeddingtest.New()}
	s := newService(t, fake, embedding.ServiceConfig{FillWindow: window})

	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.EmbedQueries(context.Background(), []string{fmt.Sprintf("memory %d", i)}); err != nil {
				t.Errorf("Embed: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := fake.calls(); got >= callers {
		t.Fatalf("%d concurrent single-text callers produced %d model runs; coalescing is not happening", callers, got)
	}
}

func TestAnEmptyRequestIsRefused(t *testing.T) {
	s := newService(t, embeddingtest.New(), embedding.ServiceConfig{})
	if _, err := s.EmbedQueries(context.Background(), nil); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestAModelFailureReachesEveryCallerInTheBatch(t *testing.T) {
	boom := errors.New("the model fell over")
	fake := embeddingtest.New()
	fake.Fail(boom)
	s := newService(t, fake, embedding.ServiceConfig{})

	if _, err := s.EmbedQueries(context.Background(), []string{"a", "b"}); !errors.Is(err, boom) {
		t.Fatalf("Embed = %v, want the model's error", err)
	}
}

func TestACancelledContextStopsWaiting(t *testing.T) {
	inner := newBlockingEmbedder()
	// Released in cleanup rather than left blocked: a batch may or may not
	// reach the model before the cancellation is noticed — the select in
	// submit has two ready cases — and leaving the model wedged would make
	// Close spend its whole shutdown grace on some runs and none on others.
	t.Cleanup(inner.release)

	s := newService(t, inner, embedding.ServiceConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.EmbedQueries(ctx, []string{"never finishes"}); err == nil {
		t.Fatal("a cancelled request waited anyway")
	}
}

// Shutdown must complete even when the model does not. Spec §57 gives the
// server ten seconds to stop, and a Close that waited forever on a wedged
// inference would spend all of it and then be killed.
func TestCloseDoesNotHangOnAWedgedModel(t *testing.T) {
	inner := newBlockingEmbedder()
	t.Cleanup(inner.release)

	s := embedding.NewService(inner, embedding.ServiceConfig{})
	go func() { _, _ = s.EmbedQueries(context.Background(), []string{"wedged"}) }()
	inner.waitUntilCalled(t)

	done := make(chan error, 1)
	go func() { done <- s.Close() }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Close reported success while a batch was still running")
		}
	case <-time.After(embedding.ShutdownGrace + 5*time.Second):
		t.Fatal("Close did not return within its own shutdown grace period")
	}
}

// Spec §57: every goroutine has an owner, and shutdown propagates. A Service
// that leaked its collector would make the server's leak test flaky rather
// than failing.
func TestCloseStopsEveryGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()
	for range 5 {
		s := embedding.NewService(embeddingtest.New(), embedding.ServiceConfig{})
		if _, err := s.EmbedQueries(context.Background(), []string{"work"}); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close is not idempotent: %v", err)
		}
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines leaked: %d -> %d", before, after)
	}
}

// Close closes the inner embedder if it holds resources — the ONNX session does.
func TestCloseReleasesTheInnerEmbedder(t *testing.T) {
	inner := &closableEmbedder{Fake: embeddingtest.New()}
	s := embedding.NewService(inner, embedding.ServiceConfig{})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !inner.closed {
		t.Fatal("the inner embedder was not closed")
	}
}

// The contract check is not decoration: an embedder that returns the wrong
// number of vectors would otherwise attach every memory to its neighbour's.
func TestAMisbehavingEmbedderIsCaught(t *testing.T) {
	for name, inner := range map[string]embedding.Embedder{
		"wrong count": &brokenEmbedder{mode: "count"},
		"wrong width": &brokenEmbedder{mode: "width"},
		"not unit":    &brokenEmbedder{mode: "norm"},
		"nan":         &brokenEmbedder{mode: "nan"},
	} {
		t.Run(name, func(t *testing.T) {
			s := embedding.NewService(inner, embedding.ServiceConfig{})
			defer func() { _ = s.Close() }()
			if _, err := s.EmbedQueries(context.Background(), []string{"a", "b"}); err == nil {
				t.Fatal("a misbehaving embedder's queries were accepted")
			}
			if _, err := s.EmbedDocuments(context.Background(), []string{"a", "b"}); err == nil {
				t.Fatal("a misbehaving embedder's documents were accepted")
			}
		})
	}
}

// A document's passages have a contract of their own, and a violation is as
// silent downstream as a wrong vector: a span past the end of the content is a
// slice that panics when a result is rendered, and a lone passage would give a
// memory two index entries that always agree.
func TestAMisbehavingEmbeddersPassagesAreCaught(t *testing.T) {
	for _, mode := range []string{"one passage", "span outside"} {
		t.Run(mode, func(t *testing.T) {
			s := embedding.NewService(&brokenEmbedder{mode: mode}, embedding.ServiceConfig{})
			defer func() { _ = s.Close() }()
			if _, err := s.EmbedDocuments(context.Background(), []string{"ab", "cd"}); err == nil {
				t.Fatal("a misbehaving embedder's passages were accepted")
			}
		})
	}
}

// Documents travel through the same batching as queries: twenty memories
// written together are collected into batches, repeated content is embedded
// once per request, and every caller gets its own document back in order.
func TestDocumentsAreBatchedDeduplicatedAndReturnedInOrder(t *testing.T) {
	for _, batchSize := range []int{1, 32} {
		t.Run(fmt.Sprintf("batch_size_%d", batchSize), func(t *testing.T) {
			inner := &countingEmbedder{inner: embeddingtest.New()}
			s := embedding.NewService(inner, embedding.ServiceConfig{BatchSize: batchSize})
			defer func() { _ = s.Close() }()
			long := strings.Repeat("word ", 3*embeddingtest.PassageWords)
			texts := []string{"first", long, "first", "third"}
			for request := 1; request <= 2; request++ {
				got, err := s.EmbedDocuments(context.Background(), texts)
				if err != nil {
					t.Fatal(err)
				}
				direct, err := embeddingtest.New().EmbedDocuments(context.Background(), texts)
				if err != nil {
					t.Fatal(err)
				}
				for i := range texts {
					if cosine(got[i].Vector, direct[i].Vector) < 0.999999 || len(got[i].Passages) != len(direct[i].Passages) {
						t.Fatalf("document %d is not the one its text produces", i)
					}
				}
				if len(got[1].Passages) != 3 {
					t.Fatalf("long document has %d passages, want 3", len(got[1].Passages))
				}
				// The fill window can split a request into several calls. The invariant
				// is the texts submitted: duplicates once, and every new request again.
				seen := inner.documents()
				if len(seen) != 3*request {
					t.Fatalf("request %d submitted %d texts, want %d", request, len(seen), 3*request)
				}
				for _, text := range []string{"first", long, "third"} {
					count := 0
					for _, submitted := range seen {
						if submitted == text {
							count++
						}
					}
					if count != request {
						t.Fatalf("one text submitted %d times after %d requests", count, request)
					}
				}
			}
		})
	}
}

func TestNormaliseLeavesAZeroVectorAlone(t *testing.T) {
	v := embedding.Normalise(make([]float32, 8))
	for _, x := range v {
		if math.IsNaN(float64(x)) {
			t.Fatal("Normalise divided by zero")
		}
	}
}

func TestNormaliseProducesAUnitVector(t *testing.T) {
	v := embedding.Normalise([]float32{3, 4})
	if got := math.Hypot(float64(v[0]), float64(v[1])); math.Abs(got-1) > 1e-6 {
		t.Fatalf("norm is %f", got)
	}
}

// --- helpers ---------------------------------------------------------------

type countingEmbedder struct {
	inner    embedding.Embedder
	mu       sync.Mutex
	n        int
	docTexts []string
}

func (c *countingEmbedder) EmbedQueries(ctx context.Context, texts []string) ([][]float32, error) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.inner.EmbedQueries(ctx, texts)
}
func (c *countingEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	c.mu.Lock()
	c.n++
	c.docTexts = append(c.docTexts, texts...)
	c.mu.Unlock()
	return c.inner.EmbedDocuments(ctx, texts)
}
func (c *countingEmbedder) Dim() int        { return c.inner.Dim() }
func (c *countingEmbedder) ModelID() string { return c.inner.ModelID() }
func (c *countingEmbedder) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func (c *countingEmbedder) documents() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.docTexts...)
}

// blockingEmbedder stands in for a model that has wedged. It is releasable so
// a test can end without leaving a goroutine parked forever.
type blockingEmbedder struct {
	called    chan struct{}
	released  chan struct{}
	closeOnce sync.Once
	callOnce  sync.Once
}

func newBlockingEmbedder() *blockingEmbedder {
	return &blockingEmbedder{called: make(chan struct{}), released: make(chan struct{})}
}

func (b *blockingEmbedder) EmbedQueries(context.Context, []string) ([][]float32, error) {
	b.callOnce.Do(func() { close(b.called) })
	<-b.released
	return nil, errors.New("released")
}
func (b *blockingEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	_, err := b.EmbedQueries(ctx, texts)
	return nil, err
}
func (b *blockingEmbedder) Dim() int        { return embedding.Dim }
func (b *blockingEmbedder) ModelID() string { return embedding.Model }
func (b *blockingEmbedder) release()        { b.closeOnce.Do(func() { close(b.released) }) }

func (b *blockingEmbedder) waitUntilCalled(t *testing.T) {
	t.Helper()
	select {
	case <-b.called:
	case <-time.After(5 * time.Second):
		t.Fatal("the embedder was never called")
	}
}

type closableEmbedder struct {
	*embeddingtest.Fake
	closed bool
}

func (c *closableEmbedder) Close() error { c.closed = true; return nil }

type brokenEmbedder struct{ mode string }

func (b *brokenEmbedder) Dim() int        { return embedding.Dim }
func (b *brokenEmbedder) ModelID() string { return embedding.Model }
func (b *brokenEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	unit := make([]float32, embedding.Dim)
	unit[0] = 1
	switch b.mode {
	case "one passage":
		out := make([]embedding.Document, len(texts))
		for i := range out {
			out[i] = embedding.Document{Vector: unit, Passages: []embedding.Passage{{Start: 0, End: 1, Vector: unit}}}
		}
		return out, nil
	case "span outside":
		out := make([]embedding.Document, len(texts))
		for i := range out {
			out[i] = embedding.Document{Vector: unit, Passages: []embedding.Passage{
				{Start: 0, End: 1, Vector: unit}, {Start: 1, End: 999, Vector: unit}}}
		}
		return out, nil
	}
	vs, err := b.EmbedQueries(ctx, texts)
	return embedding.WholeDocuments(vs), err
}
func (b *brokenEmbedder) EmbedQueries(_ context.Context, texts []string) ([][]float32, error) {
	unit := make([]float32, embedding.Dim)
	unit[0] = 1
	switch b.mode {
	case "count":
		return [][]float32{unit}, nil
	case "width":
		out := make([][]float32, len(texts))
		for i := range out {
			out[i] = []float32{1}
		}
		return out, nil
	case "norm":
		out := make([][]float32, len(texts))
		for i := range out {
			v := make([]float32, embedding.Dim)
			v[0] = 5
			out[i] = v
		}
		return out, nil
	default:
		out := make([][]float32, len(texts))
		for i := range out {
			v := make([]float32, embedding.Dim)
			v[0] = float32(math.NaN())
			out[i] = v
		}
		return out, nil
	}
}

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// laneEmbedder answers queries at once and holds every document until
// released: a stand-in for a model busy with long writes.
type laneEmbedder struct {
	*embeddingtest.Fake
	held     chan struct{}
	released chan struct{}
}

func (l *laneEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	l.held <- struct{}{}
	<-l.released
	return l.Fake.EmbedDocuments(ctx, texts)
}

// Long writes must not take the slots a search needs. With one shared pool,
// writes enough to fill every worker slot left a query queued behind them for
// as long as they ran — minutes, for a megabyte of content.
func TestAQueryIsNotQueuedBehindLongWrites(t *testing.T) {
	const workers = 2
	inner := &laneEmbedder{Fake: embeddingtest.New(), held: make(chan struct{}, 16), released: make(chan struct{})}
	s := embedding.NewService(inner, embedding.ServiceConfig{Workers: workers, BatchSize: 1})
	defer func() { _ = s.Close() }()
	defer close(inner.released)

	for i := range workers + 2 {
		go func() { _, _ = s.EmbedDocuments(context.Background(), []string{fmt.Sprintf("a long memory %d", i)}) }()
	}
	for range workers {
		select {
		case <-inner.held:
		case <-time.After(5 * time.Second):
			t.Fatal("the writes never reached the model")
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.EmbedQueries(context.Background(), []string{"a search"})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a query waited behind writes that held every worker slot")
	}
}
