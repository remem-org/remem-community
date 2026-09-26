package obs_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/obs"
)

// TestAnAttachedEngineIsSampledOnEveryScrape: the engine's cumulative counts
// reach the counters as increments, so a second scrape reports the first
// scrape's numbers plus only what moved since — and a count that went
// backwards is never subtracted, because a decreasing counter reads as a reset.
func TestAnAttachedEngineIsSampledOnEveryScrape(t *testing.T) {
	m := obs.NewMetrics()
	now := obs.EngineSample{DiskBytes: 4096, Compactions: 3, CacheHits: 10, CacheMisses: 2}
	m.ObserveEngine("pebble", func() obs.EngineSample { return now })

	first := scrape(t, m.Handler())
	for _, want := range []string{
		`remem_storage_size_bytes{component="pebble"} 4096`,
		`remem_storage_compactions_total{component="pebble"} 3`,
		`remem_storage_cache_accesses_total{component="pebble",result="hit"} 10`,
		`remem_storage_cache_accesses_total{component="pebble",result="miss"} 2`,
	} {
		if !strings.Contains(first, want) {
			t.Errorf("the first scrape lacks %s", want)
		}
	}

	now = obs.EngineSample{DiskBytes: 8192, Compactions: 5, CacheHits: 25, CacheMisses: 2}
	second := scrape(t, m.Handler())
	for _, want := range []string{
		`remem_storage_size_bytes{component="pebble"} 8192`,
		`remem_storage_compactions_total{component="pebble"} 5`,
		`remem_storage_cache_accesses_total{component="pebble",result="hit"} 25`,
	} {
		if !strings.Contains(second, want) {
			t.Errorf("the second scrape lacks %s", want)
		}
	}

	now = obs.EngineSample{DiskBytes: 1024, Compactions: 1, CacheHits: 4, CacheMisses: 1}
	third := scrape(t, m.Handler())
	if !strings.Contains(third, `remem_storage_compactions_total{component="pebble"} 5`) {
		t.Error("an engine count that went backwards was subtracted from its counter")
	}
}

// TestNoEngineReportsNoEngineNumbers: the in-memory store has no disk, no
// compactions and no cache, and a scrape must not report zeros for them as if
// it had measured an idle engine.
func TestNoEngineReportsNoEngineNumbers(t *testing.T) {
	body := scrape(t, obs.NewMetrics().Handler())
	for _, absent := range []string{
		`remem_storage_size_bytes{`,
		`remem_storage_compactions_total{`,
		`remem_storage_cache_accesses_total{`,
	} {
		if strings.Contains(body, absent) {
			t.Errorf("a scrape with no engine attached reports %s…", absent)
		}
	}
}
