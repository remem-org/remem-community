package discovery_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
)

// flakyStrategy fails the first n evaluations and then behaves.
type flakyStrategy struct {
	fails int
	inner discovery.Strategy
	err   error
}

func (f *flakyStrategy) Name() string { return "flaky" }

func (f *flakyStrategy) Evaluate(ctx context.Context, subject *record.Record,
	cands []discovery.Candidate) ([]graph.Edge, error) {
	if f.fails > 0 {
		f.fails--
		return nil, f.err
	}
	return f.inner.Evaluate(ctx, subject, cands)
}

// A transient failure retries and eventually succeeds; a permanent one reaches
// Failed carrying the reason.
//
// It is driven through the real queue rather than a fake, because what is being
// asserted is the handshake between the two: the handler returns an error, the
// queue decides whether that is a retry or the end, and the job's LastError is
// what an operator reads.
func TestFailedDiscoveryRetriesThenFails(t *testing.T) {
	t.Run("a transient failure retries and then succeeds", func(t *testing.T) {
		strategy := &flakyStrategy{
			fails: 1,
			inner: discovery.NewSimilarity(0, 0),
			err:   errors.New("the vector index was mid-rebuild"),
		}
		h := newHarness(t, func(d *discovery.Deps) { d.Strategy = strategy })
		subject := h.store(t, "acme", "the subject", unit(0))
		h.store(t, "acme", "its near-duplicate", unit(angleFor(0.99)))

		q, j := queued(t, h, subject.rid)
		ctx := context.Background()

		claimed := claimOne(t, q, j)
		err := h.disc.Handle(ctx, claimed, noCheckpoint{})
		if err == nil {
			t.Fatal("the first run succeeded; the strategy was told to fail")
		}
		must(t, q.Fail(ctx, claimed, err))

		after, err := q.Get(ctx, "acme", j.ID)
		must(t, err)
		if after.State != jobs.Retry {
			t.Fatalf("a failed first attempt left the job %s, want retry", after.State)
		}
		if !strings.Contains(after.LastError, "mid-rebuild") {
			t.Fatalf("the job's LastError is %q; an operator reads that", after.LastError)
		}

		// The retry's due time is in the future, so the clock has to move.
		h.clk.Advance(time.Hour)
		claimed = claimOne(t, q, after)
		must(t, h.disc.Handle(ctx, claimed, noCheckpoint{}))
		must(t, q.Complete(ctx, claimed))

		if edges := h.outEdges(t, "acme", subject.rid); len(edges) != 1 {
			t.Fatalf("the succeeding attempt wrote %d edges, want 1", len(edges))
		}
	})

	t.Run("a permanent failure exhausts its attempts", func(t *testing.T) {
		boom := errors.New("this strategy is broken")
		strategy := &flakyStrategy{fails: 1 << 20, inner: discovery.NewSimilarity(0, 0), err: boom}
		h := newHarness(t, func(d *discovery.Deps) { d.Strategy = strategy })
		subject := h.store(t, "acme", "the subject", unit(0))
		h.store(t, "acme", "its near-duplicate", unit(angleFor(0.99)))

		q, j := queued(t, h, subject.rid)
		ctx := context.Background()

		for attempt := 1; ; attempt++ {
			if attempt > int(jobs.DefaultMaxAttempts)+1 {
				t.Fatal("the job never reached a terminal state")
			}
			current, err := q.Get(ctx, "acme", j.ID)
			must(t, err)
			if current.State.Terminal() {
				if current.State != jobs.Failed {
					t.Fatalf("a permanently broken handler left the job %s, want failed", current.State)
				}
				if !strings.Contains(current.LastError, boom.Error()) {
					t.Fatalf("the failed job's LastError is %q, and does not say why", current.LastError)
				}
				break
			}
			claimed := claimOne(t, q, current)
			must(t, q.Fail(ctx, claimed, h.disc.Handle(ctx, claimed, noCheckpoint{})))
			h.clk.Advance(time.Hour)
		}

		if edges := h.outEdges(t, "acme", subject.rid); len(edges) != 0 {
			t.Fatalf("a job that never succeeded wrote %d edges", len(edges))
		}
	})
}

func queued(t *testing.T, h *harness, subjects ...id.ID) (*jobs.Queue, *jobs.Job) {
	t.Helper()
	q := jobs.NewQueue(h.kv, h.clk)
	payload, err := discovery.EncodePayload(subjects)
	must(t, err)
	j := &jobs.Job{Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: "discovery.similar", Payload: payload}
	must(t, q.Submit(context.Background(), j))
	return q, j
}

func claimOne(t *testing.T, q *jobs.Queue, want *jobs.Job) *jobs.Job {
	t.Helper()
	got, err := q.Claim(context.Background(), "acme", "test", []jobs.Type{"discovery.similar"}, 1)
	must(t, err)
	if len(got) != 1 {
		t.Fatalf("claimed %d jobs, want the one that is due", len(got))
	}
	if got[0].ID != want.ID {
		t.Fatalf("claimed job %s, want %s", got[0].ID, want.ID)
	}
	return got[0]
}

type noCheckpoint struct{}

func (noCheckpoint) Save(context.Context, []byte) error { return nil }
func (noCheckpoint) Processed(uint64)                   {}
func (noCheckpoint) Changed(uint64)                     {}
