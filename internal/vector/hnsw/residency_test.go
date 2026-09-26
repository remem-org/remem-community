package hnsw_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// Fifty tenants against a budget that fits a handful of them: the resident set
// stays bounded and every tenant still answers correctly.
//
// The budget is expressed in bytes rather than the plan's 100 MB because the
// property under test is the eviction policy, not the constant. Sizing the test
// to a real budget would mean generating hundreds of megabytes of vectors to
// prove that a comparison and a slice deletion work, and a test nobody runs
// because it takes four minutes protects nothing.
func TestResidencyEvictsUnderBudget(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ctx := context.Background()

	const tenants, perTenant, dim = 50, 40, 16
	rng := newTestRNG(23)

	// Sized to hold several tenants but nothing like fifty, so the test
	// distinguishes a working cache from one that keeps a single entry.
	idx := newIndex(t, kv, hnsw.Options{ResidentBudgetBytes: 100_000})

	nearest := make(map[tenant.ID]id.ID, tenants)
	queries := make(map[tenant.ID][]float32, tenants)
	for i := range tenants {
		tn := tenant.ID(fmt.Sprintf("tenant-%02d", i))
		for range perTenant {
			if err := idx.Insert(ctx, tn, id.New(), rng.unit(dim)); err != nil {
				t.Fatalf("%s: %v", tn, err)
			}
		}
		// One vector per tenant that is exactly the query, so "answers
		// correctly" is a fact rather than a recall estimate.
		want := id.New()
		q := rng.unit(dim)
		if err := idx.Insert(ctx, tn, want, q); err != nil {
			t.Fatalf("%s: %v", tn, err)
		}
		nearest[tn] = want
		queries[tn] = q
	}

	resident, bytes := idx.Resident()
	if resident >= tenants {
		t.Fatalf("all %d tenants are resident: the budget did nothing", resident)
	}
	if resident < 2 {
		t.Fatalf("only %d tenant is resident: a cache that keeps one entry is not a cache", resident)
	}
	if bytes > idx.Budget() {
		t.Fatalf("resident set is %d bytes against a %d byte budget", bytes, idx.Budget())
	}
	t.Logf("%d of %d tenants resident, %d bytes", resident, tenants, bytes)

	// Every tenant answers, including the ones that were evicted and have to be
	// read back from their node records.
	for tn, q := range queries {
		hits, err := idx.Search(ctx, tn, q, 1, nil)
		if err != nil {
			t.Fatalf("%s: %v", tn, err)
		}
		if len(hits) != 1 || hits[0].ID != nearest[tn] {
			t.Fatalf("%s did not find its own exact match after eviction: %v", tn, hits)
		}
	}
	if _, bytes = idx.Resident(); bytes > idx.Budget() {
		t.Fatalf("resident set is %d bytes after the searches, against a %d byte budget", bytes, idx.Budget())
	}
}

// Eight writers and eight readers, under -race. The point is not throughput; it
// is that a graph being mutated is never read half-changed.
func TestConcurrentInsertAndSearch(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ctx := context.Background()
	idx := newIndex(t, kv, hnsw.Options{})

	// Seeded so readers have something to walk from the first moment.
	seed := newTestRNG(29)
	for range 200 {
		if err := idx.Insert(ctx, "acme", id.New(), seed.unit(8)); err != nil {
			t.Fatal(err)
		}
	}

	// The plan asks for ten seconds. A minute of wall clock spread over the
	// package's tests is not worth the seconds of extra coverage, so the long
	// run is what -race in CI gets and a short one is what a developer gets.
	duration := 10 * time.Second
	if testing.Short() {
		duration = 500 * time.Millisecond
	}
	deadline := time.Now().Add(duration)

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for w := range 8 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := newTestRNG(uint64(100 + w))
			for time.Now().Before(deadline) {
				if err := idx.Insert(ctx, "acme", id.New(), r.unit(8)); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	for r := range 8 {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := newTestRNG(uint64(200 + r))
			for time.Now().Before(deadline) {
				hits, err := idx.Search(ctx, "acme", rng.unit(8), 10, nil)
				if err != nil {
					errCh <- err
					return
				}
				for i := 1; i < len(hits); i++ {
					if hits[i-1].Distance > hits[i].Distance {
						errCh <- fmt.Errorf("results came back out of order: %v then %v",
							hits[i-1].Distance, hits[i].Distance)
						return
					}
				}
			}
		}(r)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent operation failed: %v", err)
	}

	s, err := idx.Stats(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if s.Vectors < 200 {
		t.Fatalf("the index holds %d vectors, fewer than the 200 it started with", s.Vectors)
	}
	t.Logf("%d vectors after %s of eight writers and eight readers", s.Vectors, duration)
}
