package memory

import (
	"time"

	"github.com/remem-org/remem-go/internal/tenant"
)

// observeSearch records one search: how long it took, how many candidates the
// execution examined, and whether it told its caller it was truncated.
//
// It is called once per search, by the request that materialised the ranking.
// A continuation page is not observed — it slices a ranking already computed,
// and timing it as a search would dilute the latency histogram with reads — and
// neither is a refused request, which is not a search at all. Recall reaches
// this through the same path and is recorded under its own search_type.
func (s *Service) observeSearch(t tenant.ID, typ SearchType, started time.Time, examined int, truncated bool) {
	m := s.searchMetrics
	if m == nil {
		return
	}
	tn, mode := string(t), string(typ)
	m.Duration.WithLabelValues(tn, mode).Observe(s.clk.Since(started).Seconds())
	m.CandidatesConsidered.WithLabelValues(tn, mode).Observe(float64(examined))
	if truncated {
		m.TruncatedTotal.WithLabelValues(tn, mode).Inc()
	}
}
