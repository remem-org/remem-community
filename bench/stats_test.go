package bench

import (
	"sort"
	"sync"
	"testing"
	"time"
)

// latencies collects per-request durations from concurrent goroutines.
//
// Why record them at all: plan Phase 13 compares p99 search latency, and a p99
// is a statement about a distribution. ns/op is a mean, and a mean cannot be
// turned into a p99 after the fact. Rust's Criterion keeps per-sample means
// rather than per-request times, so docs/BENCHMARKS.md compares means across
// the two and reports Go's p99 beside Rust's worst sample mean, labelled.
type latencies struct {
	mu sync.Mutex
	d  []time.Duration
}

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	l.d = append(l.d, d)
	l.mu.Unlock()
}

// report attaches p50 and p99, in milliseconds, to the benchmark result.
func (l *latencies) report(b *testing.B, unit string) {
	b.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.d) == 0 {
		return
	}
	b.ReportMetric(percentileMS(l.d, 50), "p50-"+unit)
	b.ReportMetric(percentileMS(l.d, 99), "p99-"+unit)
}

// percentileMS is the nearest-rank percentile of ds, in milliseconds. It sorts
// a copy, so the caller's order is left alone.
func percentileMS(ds []time.Duration, p float64) float64 {
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(p/100*float64(len(sorted))+0.5) - 1
	rank = max(0, min(rank, len(sorted)-1))
	return float64(sorted[rank]) / float64(time.Millisecond)
}

func TestPercentileIsNearestRank(t *testing.T) {
	var ds []time.Duration
	for i := 1; i <= 100; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	if got := percentileMS(ds, 50); got != 50 {
		t.Errorf("p50 of 1..100ms = %v, want 50", got)
	}
	if got := percentileMS(ds, 99); got != 99 {
		t.Errorf("p99 of 1..100ms = %v, want 99", got)
	}
	if got := percentileMS([]time.Duration{7 * time.Millisecond}, 99); got != 7 {
		t.Errorf("p99 of a single sample = %v, want that sample", got)
	}
}
