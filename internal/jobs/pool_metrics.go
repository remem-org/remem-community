package jobs

import (
	"context"
)

// The pool's metrics: failures and retries as they happen, and the queue-depth
// gauges on a refresh of their own. Split from worker.go in Phase 13 to keep
// that file under the project's size limit; nothing here changed.

func (p *Pool) countFailure(j *Job) {
	if p.metrics == nil {
		return
	}
	labels := []string{j.Tenant.String(), j.Type.String()}
	if j.Attempts >= j.MaxAttempts {
		p.metrics.Jobs.FailuresTotal.WithLabelValues(labels...).Inc()
		return
	}
	p.metrics.Jobs.RetriesTotal.WithLabelValues(labels...).Inc()
}

// RefreshGauges publishes the queue-depth gauges for the tenants it visits.
//
// It rotates through the tenant list on its own cursor rather than the
// dispatcher's, so a deployment with more tenants than one pass covers still
// sees every one of them eventually, and a large directory never makes one
// refresh expensive.
//
// A label set that has emptied is set back to zero rather than left alone. A
// Prometheus gauge nothing writes keeps its last reading for ever, so a backlog
// that cleared would go on being reported as a backlog — which is exactly the
// alert an operator would then stop trusting.
func (p *Pool) RefreshGauges(ctx context.Context) error {
	if p.metrics == nil {
		return nil
	}
	tenants, err := p.walkFrom(ctx, &p.gaugeCursor)
	if err != nil {
		return err
	}

	live := map[[3]string]float64{}
	for _, t := range tenants {
		counts, err := p.queue.Counts(ctx, t)
		if err != nil {
			return err
		}
		for typ, n := range counts.Pending {
			live[[3]string{"pending", t.String(), typ.String()}] = float64(n)
		}
		for typ, n := range counts.Running {
			live[[3]string{"running", t.String(), typ.String()}] = float64(n)
		}
	}

	visited := map[string]bool{}
	for _, t := range tenants {
		visited[t.String()] = true
	}
	for key := range p.gauges {
		if _, still := live[key]; still || !visited[key[1]] {
			continue
		}
		// Emptied, and this pass looked at its tenant, so the zero is a fact
		// rather than an absence of information.
		p.setGauge(key, 0)
		delete(p.gauges, key)
	}
	for key, n := range live {
		p.setGauge(key, n)
		p.gauges[key] = struct{}{}
	}
	return nil
}

func (p *Pool) setGauge(key [3]string, n float64) {
	if key[0] == "pending" {
		p.metrics.Jobs.Pending.WithLabelValues(key[1], key[2]).Set(n)
		return
	}
	p.metrics.Jobs.Running.WithLabelValues(key[1], key[2]).Set(n)
}
