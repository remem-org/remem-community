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
	// DefaultFillWindow is how long a partly-filled batch waits for company.
	// Five milliseconds is short enough to be invisible in a request that is
	// already doing a model run, and long enough to collect the burst an agent
	// fleet produces when several sessions write at once.
	DefaultFillWindow = 5 * time.Millisecond
	// DefaultWorkers bounds concurrent model runs. ONNX Runtime parallelises
	// inside one run, so more Go-level concurrency mostly buys contention.
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

	reqs    chan *item
	workers chan struct{}

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
		reqs:       make(chan *item),
		workers:    make(chan struct{}, cfg.Workers),
		closed:     make(chan struct{}),
	}
	s.wg.Add(1)
	go s.collect()
	return s
}

func (s *Service) Dim() int        { return s.inner.Dim() }
func (s *Service) ModelID() string { return s.inner.ModelID() }

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

// Embed returns one vector per text, in order.
//
// Cached texts are served without touching the model. Duplicates within one
// request are embedded once — an agent storing the same fact twice in a batch
// is common, and it should not cost two model runs.
func (s *Service) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	const op = "embedding.Service.Embed"

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
		if err := s.submit(ctx, texts, wanted, out); err != nil {
			return nil, err
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

// item is one text awaiting a vector.
type item struct {
	text string
	done chan itemResult
}

type itemResult struct {
	vec []float32
	err error
}

// submit hands the wanted texts to the collector and waits for all of them.
func (s *Service) submit(ctx context.Context, texts []string, wanted []int, out [][]float32) error {
	const op = "embedding.Service.Embed"

	items := make([]*item, len(wanted))
	for n, i := range wanted {
		items[n] = &item{text: texts[i], done: make(chan itemResult, 1)}
	}

	for _, it := range items {
		select {
		case s.reqs <- it:
		case <-ctx.Done():
			return errs.E(errs.Unavailable, op, ctx.Err())
		case <-s.closed:
			return errs.E(errs.Unavailable, op, errClosedService)
		}
	}

	var firstErr error
	for n, it := range items {
		select {
		case r := <-it.done:
			if r.err != nil {
				if firstErr == nil {
					firstErr = r.err
				}
				continue
			}
			out[wanted[n]] = r.vec
			s.cache.put(it.text, r.vec)
		case <-ctx.Done():
			return errs.E(errs.Unavailable, op, ctx.Err())
		}
	}
	return firstErr
}

var errClosedService = errors.New("the embedding service is shutting down")

// collect gathers items into batches and hands each to a worker.
//
// A batch closes when it is full or when the fill window expires, whichever
// comes first. The window is started by the first item of a batch rather than
// reset by each arrival, so a steady stream of requests cannot starve the
// batch that is already waiting.
func (s *Service) collect() {
	defer s.wg.Done()

	for {
		var pending []*item
		select {
		case it := <-s.reqs:
			pending = append(pending, it)
		case <-s.closed:
			return
		}

		timer := time.NewTimer(s.fillWindow)
	fill:
		for len(pending) < s.batchSize {
			select {
			case it := <-s.reqs:
				pending = append(pending, it)
			case <-timer.C:
				break fill
			case <-s.closed:
				break fill
			}
		}
		timer.Stop()
		s.dispatch(pending)
	}
}

// dispatch runs one batch, bounded by the worker semaphore.
func (s *Service) dispatch(batch []*item) {
	held := false
	select {
	case s.workers <- struct{}{}:
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
				<-s.workers
			}
		}()

		texts := make([]string, len(batch))
		for i, it := range batch {
			texts[i] = it.text
		}

		vecs, err := s.inner.Embed(context.Background(), texts)
		if err == nil && len(vecs) != len(batch) {
			// Checked here as well as in checkOutput, because this loop
			// indexes the result: an embedder that returned fewer vectors than
			// it was given texts would panic in a goroutine that has no
			// recovery, taking the process down instead of failing a request.
			err = errs.E(errs.Storage, "embedding.Service.Embed", errors.New(
				"the embedder returned a different number of vectors than it was given texts"))
		}
		for i, it := range batch {
			if err != nil {
				it.done <- itemResult{err: err}
				continue
			}
			it.done <- itemResult{vec: vecs[i]}
		}
	}()
}
