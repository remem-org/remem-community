package obs

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// EngineSample is what a storage engine reports about itself: cumulative since
// it was opened. It mirrors storage.EngineStats field for field, and exists so
// that this package — which every other package imports — does not import the
// storage abstraction.
type EngineSample struct {
	DiskBytes   uint64
	Compactions int64
	CacheHits   int64
	CacheMisses int64
}

// ObserveEngine attaches the storage engine, named by component, so every
// scrape reports its disk usage, compactions and block-cache behaviour.
//
// Without it the three collectors report only what was recorded on them
// directly, and a store with no engine — the in-memory one — reports nothing
// rather than a row of zeros that would read as a measured idle engine.
func (m *Metrics) ObserveEngine(component string, sample func() EngineSample) {
	m.engine.mu.Lock()
	defer m.engine.mu.Unlock()
	m.engine.component = component
	m.engine.sample = sample
	m.engine.last = EngineSample{}
}

// engineCollector samples the engine and emits the three collectors it feeds,
// in one Collect.
//
// Sampling inside Collect rather than on a timer is what keeps a scrape
// consistent: the numbers a scrape reports are the numbers read for that
// scrape, not ones a background loop happened to leave behind.
type engineCollector struct {
	storage *StorageMetrics

	mu        sync.Mutex
	component string
	sample    func() EngineSample
	// last is the previous sample, so a cumulative engine count becomes counter
	// increments rather than a counter set to an absolute value.
	last EngineSample
}

func (c *engineCollector) Describe(ch chan<- *prometheus.Desc) {
	c.storage.SizeBytes.Describe(ch)
	c.storage.CompactionsTotal.Describe(ch)
	c.storage.CacheAccessesTotal.Describe(ch)
}

func (c *engineCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	if c.sample != nil {
		s := c.sample()
		c.storage.SizeBytes.WithLabelValues(c.component).Set(float64(s.DiskBytes))
		addDelta(c.storage.CompactionsTotal.WithLabelValues(c.component), s.Compactions, c.last.Compactions)
		addDelta(c.storage.CacheAccessesTotal.WithLabelValues(c.component, "hit"), s.CacheHits, c.last.CacheHits)
		addDelta(c.storage.CacheAccessesTotal.WithLabelValues(c.component, "miss"), s.CacheMisses, c.last.CacheMisses)
		c.last = s
	}
	c.mu.Unlock()

	c.storage.SizeBytes.Collect(ch)
	c.storage.CompactionsTotal.Collect(ch)
	c.storage.CacheAccessesTotal.Collect(ch)
}

// addDelta adds what an engine count moved since the last sample. A count that
// went backwards — an engine reopened under the same process — is not
// subtracted: a Prometheus counter that decreases reads as a reset and corrupts
// every rate computed across it, so the new baseline is simply taken.
func addDelta(c prometheus.Counter, now, before int64) {
	if now > before {
		c.Add(float64(now - before))
	}
}
