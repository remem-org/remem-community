package text_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
)

// The corpus statistics are the one row every write in a tenant touches, so
// they are the one place a lost update is invisible. A document count that
// drifts low is an inverse document frequency that is wrong for every query
// from then on, and nothing anywhere reports it.
//
// This is the test that pins the decision recorded in docs/architecture/text.md:
// exactness, bought with a conditional commit and a per-tenant gate.
//
// It takes the gate, because the write path does. That is a correction to how
// this test was first written: it drove txn.Do alone and asserted that no write
// is ever refused, which is a property the retry does not have and the gate
// exists to provide. §"Corpus statistics" of text.md records the measurement —
// ungated, sixteen writers cost 1.59 attempts each and thirty-two refuse one
// write in five hundred — so the original assertion was over-claiming, and it
// held only because the ungated window is narrow on an idle machine. Under
// coverage instrumentation everything slows down, the window widens, and it
// failed about one run in four.
//
// The property the retry has on its own is the next test's.
func TestConcurrentWritesKeepTheDocumentCountExact(t *testing.T) {
	ix, kv := newIndex(t)
	ctx := context.Background()
	gate := txn.NewGate()

	const writers, each = 16, 8

	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				rid := id.New()
				content := fmt.Sprintf("memory %d from writer %d about kubernetes", i, w)
				err := func() error {
					// Exactly what memory.Service.write does: the gate removes
					// the contention this process creates on a row every write
					// touches, and the retry covers what the gate cannot see.
					unlock := gate.Lock(string(acme))
					defer unlock()
					return txn.Do(ctx, kv, func(tx txn.Tx) error {
						return ix.Stage(ctx, tx, acme, ns, rid, memoryRecord(rid, acme, content))
					})
				}()
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("a gated write was refused: %v", err)
	}

	stats, err := text.ReadStats(ctx, kv, acme, ns)
	if err != nil {
		t.Fatalf("reading statistics: %v", err)
	}
	if want := uint64(writers * each); stats.Documents != want {
		t.Fatalf("the document count is %d after %d writes: increments were lost, "+
			"so every score this tenant sees from now on is computed against the wrong corpus",
			stats.Documents, want)
	}
	if stats.TotalLength == 0 {
		t.Fatal("the total length is zero, so avgdl is a floor rather than a measurement")
	}
}

// What the retry has on its own: it may refuse a write, and it never loses one.
//
// This is the weaker claim and the important one. Contention is absorbed by the
// gate in production, but the gate is process-local, so the day a second writer
// exists — a `remem-admin` command, or Phase 14's second node before consensus
// orders the write — this is the guarantee that remains. A refused write is an
// error the caller sees. A *lost increment* would be an inverse document
// frequency silently wrong for every query from then on, and that must not
// happen at any concurrency.
func TestAnUngatedWriteIsRefusedRatherThanLost(t *testing.T) {
	ix, kv := newIndex(t)
	ctx := context.Background()

	const writers, each = 16, 8

	var (
		mu        sync.Mutex
		committed uint64
	)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				rid := id.New()
				content := fmt.Sprintf("memory %d from writer %d about kubernetes", i, w)
				err := txn.Do(ctx, kv, func(tx txn.Tx) error {
					return ix.Stage(ctx, tx, acme, ns, rid, memoryRecord(rid, acme, content))
				})
				switch {
				case err == nil:
					mu.Lock()
					committed++
					mu.Unlock()
				case errs.Is(err, errs.Conflict):
					// Contention the retry budget could not absorb. Reported,
					// which is the whole point.
				default:
					t.Errorf("a concurrent write failed for a reason that is not contention: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	stats, err := text.ReadStats(ctx, kv, acme, ns)
	if err != nil {
		t.Fatalf("reading statistics: %v", err)
	}
	if stats.Documents != committed {
		t.Fatalf("the document count is %d after %d committed writes: an increment was lost, "+
			"which is the one outcome the conditional commit exists to make impossible",
			stats.Documents, committed)
	}
}

// Without the retry, a collision must be *reported* rather than silently
// applied. That is what makes the retry a correctness decision instead of a
// convenience: the guard below the retry is what turns a lost update into an
// error somebody can handle.
func TestAnUnretriedCollisionIsRefusedRatherThanLost(t *testing.T) {
	ix, kv := newIndex(t)
	ctx := context.Background()

	first, second := id.New(), id.New()

	// Two transactions opened against the same statistics row, interleaved by
	// hand: both stage, then both commit.
	a := txn.New(kv)
	defer a.Close()
	b := txn.New(kv)
	defer b.Close()

	if err := ix.Stage(ctx, a, acme, ns, first, memoryRecord(first, acme, "alpha memory")); err != nil {
		t.Fatalf("staging the first: %v", err)
	}
	if err := ix.Stage(ctx, b, acme, ns, second, memoryRecord(second, acme, "beta memory")); err != nil {
		t.Fatalf("staging the second: %v", err)
	}
	if err := a.Commit(ctx); err != nil {
		t.Fatalf("committing the first: %v", err)
	}

	err := b.Commit(ctx)
	if !errs.Is(err, errs.Conflict) {
		t.Fatalf("the second commit returned %v; a lost increment must be refused, not applied", err)
	}

	stats, err := text.ReadStats(ctx, kv, acme, ns)
	if err != nil {
		t.Fatalf("reading statistics: %v", err)
	}
	if stats.Documents != 1 {
		t.Fatalf("the document count is %d; the refused transaction applied part of itself", stats.Documents)
	}
	for term, ids := range postingsFor(t, kv, acme) {
		for _, got := range ids {
			if got == second {
				t.Fatalf("the refused transaction left a posting under %q", term)
			}
		}
	}
}

// A tenant that has never been written to has no statistics row, and reading
// one must be an empty corpus rather than an error. avgdl floors at 1 there,
// because a zero would divide BM25's length normalisation into an infinity.
func TestAnUntouchedTenantHasAnEmptyCorpus(t *testing.T) {
	_, kv := newIndex(t)

	stats, err := text.ReadStats(context.Background(), kv, "never-written", ns)
	if err != nil {
		t.Fatalf("reading statistics: %v", err)
	}
	if stats.Documents != 0 || stats.TotalLength != 0 {
		t.Fatalf("an untouched tenant reports %+v", stats)
	}
	if got := stats.AvgDocLen(); got != 1 {
		t.Fatalf("avgdl on an empty corpus is %v, want the floor of 1", got)
	}
}

// One tenant's totals must move only for that tenant's writes. This is the
// plan's TestBM25StatisticsArePerTenant at the level where it is decided: a
// global corpus statistic would leak one tenant's distribution into another's
// scores, and the leak would be invisible in the results.
func TestStatisticsAreScopedToATenant(t *testing.T) {
	ix, kv := newIndex(t)
	ctx := context.Background()

	mine := id.New()
	if err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		return ix.Stage(ctx, tx, acme, ns, mine, memoryRecord(mine, acme, "a single memory"))
	}); err != nil {
		t.Fatalf("writing: %v", err)
	}

	for range 200 {
		rid := id.New()
		if err := txn.Do(ctx, kv, func(tx txn.Tx) error {
			return ix.Stage(ctx, tx, "globex", ns, rid, memoryRecord(rid, "globex", "many memories here"))
		}); err != nil {
			t.Fatalf("writing the other tenant: %v", err)
		}
	}

	mineStats, err := text.ReadStats(ctx, kv, acme, ns)
	if err != nil {
		t.Fatalf("reading statistics: %v", err)
	}
	if mineStats.Documents != 1 {
		t.Fatalf("this tenant's document count is %d after one write and 200 of somebody else's",
			mineStats.Documents)
	}
}
