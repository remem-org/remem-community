package embedding

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
)

// Default batching parameters, matching the Rust service, which is well tuned.
const (
	// DefaultBatchSize caps one model run. Beyond this the tensor gets large
	// enough that the padding wasted on short texts costs more than the batch
	// saves.
	DefaultBatchSize = 32
	// DefaultFillWindow is how long a partly-filled batch waits for company,
	// and how recently another item must have arrived for a batch to wait at
	// all: a request that arrives alone, with nothing queued behind it and no
	// neighbour this recent, is dispatched at once. Five milliseconds is short
	// enough to be invisible in a request that is already doing a model run,
	// and long enough to collect the burst an agent fleet produces when
	// several sessions write at once.
	DefaultFillWindow = 5 * time.Millisecond
	// DefaultWorkers bounds how many batches wait on the model at once. The
	// model runs one inference at a time, so this is a queue depth and not a
	// degree of parallelism: the parallelism lives inside one run, in ONNX
	// Runtime's own thread count, and more Go-level concurrency here mostly
	// buys contention.
	DefaultWorkers = 2
	// DefaultCacheSize is how many vectors are remembered. Agents re-embed the
	// same query text constantly — the same question asked twice is the normal
	// case, not the exception.
	DefaultCacheSize = 4096
)

// Service adds caching and cross-caller batching to an [Embedder].
//
// Batching matters because of how agents actually write: many sessions each
// storing one memory, rather than one session storing many. Without
// coalescing, fifty concurrent single-memory writes are fifty model runs of
// one text each. With it they are two runs of thirty-two, on the same
// hardware, for the same work.
//
// The Service is itself an Embedder, so nothing downstream knows it is there.
type Service struct {
	inner Embedder
	cache *cache

	batchSize  int
	fillWindow time.Duration

	// Queries and documents travel in separate lanes, each with its own
	// collector and its own worker slots. A memory can be a megabyte of content
	// and many minutes of model time; in one shared pool, two such writes would
	// hold every worker slot and every search would wait behind them. With a
	// lane each, writes can only ever occupy their own slots, and the model's
	// pass gate admits a waiting query before the next document pass.
	queries, documents *lane

	wg        sync.WaitGroup
	closeOnce sync.Once
	closed    chan struct{}
}

var _ Embedder = (*Service)(nil)

// ServiceConfig configures a [Service]. A zero field takes the default above.
type ServiceConfig struct {
	BatchSize  int
	FillWindow time.Duration
	Workers    int
	CacheSize  int
}

// NewService wraps inner. The returned Service owns one collector goroutine and
// must be closed.
func NewService(inner Embedder, cfg ServiceConfig) *Service {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.FillWindow <= 0 {
		cfg.FillWindow = DefaultFillWindow
	}
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers
	}
	if cfg.CacheSize < 0 {
		cfg.CacheSize = 0
	} else if cfg.CacheSize == 0 {
		cfg.CacheSize = DefaultCacheSize
	}

	s := &Service{
		inner:      inner,
		cache:      newCache(cfg.CacheSize),
		batchSize:  cfg.BatchSize,
		fillWindow: cfg.FillWindow,
		queries:    newLane(cfg.Workers),
		documents:  newLane(cfg.Workers),
		closed:     make(chan struct{}),
	}
	s.wg.Add(2)
	go s.collect(s.queries)
	go s.collect(s.documents)
	return s
}

func (s *Service) Dim() int        { return s.inner.Dim() }
func (s *Service) ModelID() string { return s.inner.ModelID() }

// callerSupplied forwards the mode question to the wrapped embedder.
func (s *Service) callerSupplied() bool { return CallerSupplied(s.inner) }

// CacheStats reports hits, misses and the number of vectors held.
func (s *Service) CacheStats() (hits, misses uint64, size int) { return s.cache.stats() }

// ShutdownGrace bounds how long Close waits for an in-flight model run.
//
// It is a bound rather than an unlimited wait because shutdown must complete:
// spec §57 gives the server ten seconds to stop, and a Close that waited
// forever on a wedged inference would spend all of it and then be killed.
// Five seconds is comfortably longer than any real batch and comfortably
// shorter than the budget.
const ShutdownGrace = 5 * time.Second

// Close stops the collector and releases the inner embedder if it holds
// resources. It is idempotent.
//
// It waits for the goroutines it owns, so that the server's leak test is
// meaningful rather than flaky — but only up to [ShutdownGrace]. Past that it
// reports the failure instead of hanging: a leaked goroutine in a process that
// is exiting is a smaller problem than a process that will not exit.
func (s *Service) Close() error {
	// A server whose vectors come from the caller builds no service, and its
	// shutdown and error paths close it like any other dependency.
	if s == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)

		stopped := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(stopped)
		}()

		select {
		case <-stopped:
		case <-time.After(ShutdownGrace):
			err = errs.E(errs.Unavailable, "embedding.Service.Close", errors.New(
				"an embedding batch did not finish within the shutdown grace period"))
		}

		if c, ok := s.inner.(io.Closer); ok {
			if cerr := c.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	})
	return err
}

// EmbedQueries returns one vector per text, in order.
//
// Cached texts are served without touching the model. Duplicates within one
// request are embedded once — an agent asking the same question twice in a
// batch is common, and it should not cost two model runs.
func (s *Service) EmbedQueries(ctx context.Context, texts []string) ([][]float32, error) {
	const op = "embedding.Service.EmbedQueries"

	if len(texts) == 0 {
		return nil, errs.E(errs.Invalid, op, ErrNoTexts)
	}

	out := make([][]float32, len(texts))
	// first maps a text to the index whose result every other occurrence of
	// that text copies.
	first := make(map[string]int, len(texts))
	var wanted []int

	for i, text := range texts {
		if v, ok := s.cache.get(text); ok {
			out[i] = v
			continue
		}
		if _, seen := first[text]; seen {
			continue
		}
		first[text] = i
		wanted = append(wanted, i)
	}

	if len(wanted) > 0 {
		results, err := s.submit(ctx, op, texts, wanted, false)
		if err != nil {
			return nil, err
		}
		for n, i := range wanted {
			out[i] = results[n].vec
			s.cache.put(texts[i], results[n].vec)
		}
		for i, text := range texts {
			if out[i] == nil {
				out[i] = append([]float32(nil), out[first[text]]...)
			}
		}
	}

	if err := checkOutput(op, texts, out, s.Dim()); err != nil {
		return nil, err
	}
	return out, nil
}

// EmbedDocuments embeds each text as a memory's content, in order.
//
// Documents are batched with everyone else's, as queries are, but not cached.
// A memory's content is embedded when it is written and rarely again, and a
// cache of passage sets would hold a second copy of the corpus's vectors for
// the benefit of a write that repeats itself. Duplicates within one request
// are still embedded once.
func (s *Service) EmbedDocuments(ctx context.Context, texts []string) ([]Document, error) {
	const op = "embedding.Service.EmbedDocuments"

	if len(texts) == 0 {
		return nil, errs.E(errs.Invalid, op, ErrNoTexts)
	}
	first := make(map[string]int, len(texts))
	var wanted []int
	for i, text := range texts {
		if _, seen := first[text]; !seen {
			first[text] = i
			wanted = append(wanted, i)
		}
	}
	results, err := s.submit(ctx, op, texts, wanted, true)
	if err != nil {
		return nil, err
	}
	byFirst := make(map[int]Document, len(wanted))
	for n, i := range wanted {
		byFirst[i] = results[n].doc
	}
	out := make([]Document, len(texts))
	for i, text := range texts {
		out[i] = byFirst[first[text]]
	}
	if err := checkDocuments(op, texts, out, s.Dim()); err != nil {
		return nil, err
	}
	return out, nil
}

// item is one text awaiting a vector, or a document's vectors.
type item struct {
	text     string
	document bool
	done     chan itemResult
	// more says that the same request has further texts still to send, so the
	// collector does not mistake the first text of a twenty-text request for
	// a request that arrived alone.
	more bool
}

type itemResult struct {
	vec []float32
	doc Document
	err error
}

// submit hands the wanted texts to the collector and waits for all of them,
// returning their results in the order of wanted.
func (s *Service) submit(ctx context.Context, op string, texts []string, wanted []int, document bool) ([]itemResult, error) {
	l := s.queries
	if document {
		l = s.documents
	}
	items := make([]*item, len(wanted))
	for n, i := range wanted {
		items[n] = &item{
			text:     texts[i],
			document: document,
			done:     make(chan itemResult, 1),
			more:     n < len(wanted)-1,
		}
	}

	for _, it := range items {
		select {
		case l.reqs <- it:
		case <-ctx.Done():
			return nil, errs.E(errs.Unavailable, op, ctx.Err())
		case <-s.closed:
			return nil, errs.E(errs.Unavailable, op, errClosedService)
		}
	}

	results := make([]itemResult, len(items))
	for n, it := range items {
		select {
		case r := <-it.done:
			if r.err != nil {
				return nil, r.err
			}
			results[n] = r
		case <-ctx.Done():
			return nil, errs.E(errs.Unavailable, op, ctx.Err())
		}
	}
	return results, nil
}

var errClosedService = errors.New("the embedding service is shutting down")

// collect gathers items into batches and hands each to a worker.
//
// A batch closes when it is full or when the fill window expires, whichever
// comes first. The window is started by the first item of a batch rather than
// reset by each arrival, so a steady stream of requests cannot starve the
// batch that is already waiting.
//
// Waiting is conditional, because the window is a bet that company is coming
// and a lone request loses it: the whole window is then added to the latency
// of a single search or create for nothing. So the collector first takes
// whatever is already queued without blocking, and waits only when that found
// company or when another item arrived within the last window — the evidence
// that traffic is bursty right now. An isolated request goes straight to the
// model.
func (s *Service) collect(l *lane) {
	defer s.wg.Done()

	for {
		var (
			pending []*item
			bursty  bool
		)
		select {
		case it := <-l.reqs:
			bursty = time.Since(l.lastArrival) < s.fillWindow
			pending = l.take(pending, it)
		case <-s.closed:
			return
		}

	drain:
		for len(pending) < s.batchSize {
			select {
			case it := <-l.reqs:
				pending = l.take(pending, it)
			default:
				break drain
			}
		}

		if len(pending) == 1 && !pending[0].more && !bursty {
			s.dispatch(l, pending)
			continue
		}

		timer := time.NewTimer(s.fillWindow)
	fill:
		for len(pending) < s.batchSize {
			select {
			case it := <-l.reqs:
				pending = l.take(pending, it)
			case <-timer.C:
				break fill
			case <-s.closed:
				break fill
			}
		}
		timer.Stop()
		s.dispatch(l, pending)
	}
}

// lane is one kind of work's path to the model: its own queue, collector and
// worker slots.
type lane struct {
	reqs    chan *item
	workers chan struct{}

	// lastArrival is when the collector last took an item off reqs; its zero
	// value is long enough ago to count as idle. It is touched only by the
	// lane's collector goroutine, so it needs no lock.
	lastArrival time.Time
}

func newLane(workers int) *lane {
	return &lane{reqs: make(chan *item), workers: make(chan struct{}, workers)}
}

// take adds it to the batch and notes when it arrived.
func (l *lane) take(pending []*item, it *item) []*item {
	l.lastArrival = time.Now()
	return append(pending, it)
}

// dispatch runs one batch, bounded by the worker semaphore.
func (s *Service) dispatch(l *lane, batch []*item) {
	held := false
	select {
	case l.workers <- struct{}{}:
		held = true
	case <-s.closed:
		// Still serve them: a request already accepted is answered, not
		// dropped, even during shutdown.
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			if held {
				<-l.workers
			}
		}()

		var queries, documents []*item
		for _, it := range batch {
			if it.document {
				documents = append(documents, it)
			} else {
				queries = append(queries, it)
			}
		}
		if len(queries) > 0 {
			vecs, err := s.inner.EmbedQueries(context.Background(), textsOf(queries))
			if err == nil && len(vecs) != len(queries) {
				err = errCount
			}
			for i, it := range queries {
				if err != nil {
					it.done <- itemResult{err: err}
					continue
				}
				it.done <- itemResult{vec: vecs[i]}
			}
		}
		if len(documents) > 0 {
			docs, err := s.inner.EmbedDocuments(context.Background(), textsOf(documents))
			if err == nil && len(docs) != len(documents) {
				err = errCount
			}
			for i, it := range documents {
				if err != nil {
					it.done <- itemResult{err: err}
					continue
				}
				it.done <- itemResult{doc: docs[i]}
			}
		}
	}()
}

func textsOf(items []*item) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.text
	}
	return out
}

// errCount is checked here as well as in checkOutput, because the dispatch loop
// indexes the result: an embedder that returned fewer results than it was given
// texts would panic in a goroutine that has no recovery, taking the process
// down instead of failing a request.
var errCount = errs.E(errs.Storage, "embedding.Service", errors.New(
	"the embedder returned a different number of results than it was given texts"))
