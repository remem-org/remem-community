package memory_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/obs"
)

func samples(t *testing.T, h *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()
	o, err := h.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatal(err)
	}
	var m dto.Metric
	if err := o.(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

func count(t *testing.T, c *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.WithLabelValues(labels...).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// TestEverySearchIsMeasuredOnceByItsType: a search reports its latency and the
// candidates it examined under its own search_type, a continuation page — which
// slices a ranking already computed — is not a second search, a search stopped
// by its depth bound counts as truncated, and a recall is a search too.
func TestEverySearchIsMeasuredOnceByItsType(t *testing.T) {
	m := obs.NewMetrics()
	// A depth bound of three over eight matching memories: every first page
	// materialises three hits, reaches the bound, and reports truncated.
	svc := newService(t,
		withServiceOption(memory.WithSearchMetrics(&m.Search)),
		withConfig(memory.Config{MaxPageDepth: 3}))
	seed(t, svc,
		"raft elects a leader", "raft replicates a log", "raft needs a majority",
		"raft snapshots its state", "raft steps down on a higher term", "raft commits an entry",
		"raft restarts from its log", "raft sends heartbeats")
	ctx := acmeCtx()

	var firstSemantic memory.SearchResult
	for _, typ := range []memory.SearchType{memory.SearchSemantic, memory.SearchKeyword, memory.SearchHybrid} {
		res, err := svc.Search(ctx, memory.SearchReq{Query: "raft", Type: typ, Limit: 1})
		if err != nil {
			t.Fatalf("%s search: %v", typ, err)
		}
		if typ == memory.SearchSemantic {
			firstSemantic = res
		}
		if got := samples(t, m.Search.Duration, "acme", string(typ)); got != 1 {
			t.Errorf("one %s search recorded %d duration samples", typ, got)
		}
		if got := samples(t, m.Search.CandidatesConsidered, "acme", string(typ)); got != 1 {
			t.Errorf("one %s search recorded %d candidate samples", typ, got)
		}
		if !res.Truncated {
			t.Fatalf("the %s search did not reach its depth bound, so it cannot show truncation is counted", typ)
		}
		if got := count(t, m.Search.TruncatedTotal, "acme", string(typ)); got != 1 {
			t.Errorf("a truncated %s search counted %v truncations", typ, got)
		}
	}

	if firstSemantic.NextCursor == "" {
		t.Fatal("the semantic search offered no second page to continue into")
	}
	if _, err := svc.Search(ctx, memory.SearchReq{
		Query: "raft", Type: memory.SearchSemantic, Limit: 1, Cursor: firstSemantic.NextCursor,
	}); err != nil {
		t.Fatalf("the continuation page: %v", err)
	}
	if got := samples(t, m.Search.Duration, "acme", "semantic"); got != 1 {
		t.Errorf("a continuation page was measured as a search: %d semantic samples, want 1", got)
	}

	if _, err := svc.Recall(ctx, memory.RecallReq{Context: "raft", Type: memory.SearchHybrid, TokenBudget: 1000}); err != nil {
		t.Fatalf("recall: %v", err)
	}
	if got := samples(t, m.Search.Duration, "acme", "hybrid"); got != 2 {
		t.Errorf("a hybrid search and a hybrid recall recorded %d samples, want 2", got)
	}

	// A refused request is not a search, and must not be timed as one.
	if _, err := svc.Search(ctx, memory.SearchReq{Query: "raft", Type: memory.SearchKeyword, Limit: 1_000_000}); err == nil {
		t.Fatal("a search over the result limit was accepted")
	}
	if got := samples(t, m.Search.Duration, "acme", "keyword"); got != 1 {
		t.Errorf("a refused keyword search was measured: %d samples, want 1", got)
	}
}
