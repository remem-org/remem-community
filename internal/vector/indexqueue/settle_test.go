package indexqueue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexqueue"
)

// Settle is the barrier for a reader that cannot live with "eventually".
//
// It promises for one tenant what Flush promises for all of them — every write
// accepted before the call has been applied — so this is the shape of
// TestFlushReturnsOnlyAfterEverythingEnqueuedIsApplied with the real index
// behind it, which is what lets "applied" be read as "searchable" rather than
// as a counter the queue keeps about itself.
func TestSettleWaitsForTheTenantsQueuedWrites(t *testing.T) {
	t.Parallel()
	g := &gatedIndex{Maintained: realIndex(t), gate: make(chan struct{})}
	q := indexqueue.New(g)
	// The gate is opened by the cleanup as well as by the body: a failure
	// leaves a worker at the gate, and a Close that waited for it would turn an
	// informative failure into a hung test.
	t.Cleanup(func() { g.open(); _ = q.Close(context.Background()) })
	ctx := context.Background()

	rid := id.New()
	v := []float32{1, 0, 0, 0}
	if err := q.Indexed(ctx, acme, rid, vector.Single(v)); err != nil {
		t.Fatal(err)
	}

	settled := make(chan error, 1)
	go func() { settled <- q.Settle(ctx, acme) }()
	select {
	case err := <-settled:
		t.Fatalf("Settle returned %v with the tenant's insert not yet applied", err)
	case <-time.After(100 * time.Millisecond):
	}

	g.open()
	select {
	case err := <-settled:
		if err != nil {
			t.Fatalf("Settle: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Settle never returned")
	}

	hits, err := q.Search(ctx, acme, v, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != rid {
		t.Fatalf("searching after a settle found %v, want the record enqueued before it", hits)
	}
}

// A settle on a stopped queue says so rather than answering as though the
// writes it was asked about had been applied. The workers are gone, so nothing
// can apply them, and a caller told "settled" would then read a lagging index
// believing it current — which is the failure this barrier exists to prevent.
// Flush answers the same way.
func TestSettleOnAClosedQueueIsUnavailable(t *testing.T) {
	t.Parallel()
	q := indexqueue.New(&recorder{}, indexqueue.WithWorkers(1))
	ctx := context.Background()
	if err := q.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := q.Settle(ctx, acme); !errs.Is(err, errs.Unavailable) {
		t.Fatalf("settling a closed queue returned %v, want Unavailable", err)
	}
}

// gatedIndex is the real maintained index with a gate in front of its applies.
//
// The recorder has a gate already and answers Search with nothing, which is
// exactly the answer a test about this barrier must not be able to accept, so
// this wraps the index the server actually runs instead.
type gatedIndex struct {
	indexqueue.Maintained
	gate     chan struct{}
	openOnce sync.Once
}

func (g *gatedIndex) Indexed(ctx context.Context, t tenant.ID, rid id.ID, v []vector.Entry) error {
	<-g.gate
	return g.Maintained.Indexed(ctx, t, rid, v)
}

// open releases everything waiting at the gate, and is safe to call twice.
func (g *gatedIndex) open() { g.openOnce.Do(func() { close(g.gate) }) }
