package attr_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// TestACachedReaderAgreesWithItsOwnSnapshot is plan item 18's whole claim, and
// the only one that matters: whatever the cache does, a reader must see exactly
// what the snapshot it was opened over holds.
//
// The shape is the one the server has. Writers update rows through the indexer in
// real transactions, so they conflict with each other and retry — which also
// exercises the staging mark an abandoned attempt leaves behind. Readers open a
// session the way the paging registry does, generation first and snapshot second,
// then read every record twice through the cache and check each answer against a
// decode of the same record straight from their own snapshot. That oracle cannot
// be wrong: it is the bytes the reader is pinned to.
//
// Run it with -race. The failure this is here to catch is two readers holding one
// generation whose snapshots straddle a commit, and it is a data race in the
// logical sense even when it is not one in the detector's.
//
// The writers run until the readers have finished rather than for a fixed count,
// so every session is read against a store that is being written — a fixed count
// let the writers finish early and left most of the sessions reading a quiet
// store, which is the case the cache is least interesting in. They pause between
// writes, which is what leaves a session a generation that holds long enough to
// get a hit; without hits the run proves nothing about the cache.
//
// That pause is counted in *reader sessions*, not in time, and that is deliberate.
// A row write retires everything the tenant has cached when it finishes, abandoned
// as much as committed — the cache cannot tell a batch that failed its conditions
// from one that failed its sync after becoming visible — so what the hit rate
// depends on is the number of writes per session, and a writer sleeping a fixed
// 400µs does not hold that ratio still. Under `-race` the readers slow by
// about sevenfold while the writers keep their pace, which took the memory arm from
// 48% of its reads served to 2%: still green, still millions of comparisons, and no
// longer a test of the cache at all. Pacing the writers against the readers' own
// progress makes the arm measure the same thing however fast the machine is.
//
// Two arms, because the window the staging mark covers is as wide as the commit:
// the `memory` arm commits in microseconds over memkv, and the `synced` arm pays
// an fsync over a real Pebble directory, which is the deployment where a reader
// has time to land inside one.
//
// Each arm carries its own floor on the share of reads the cache answered, because
// the two buy their hits very differently and a run that stops getting them stops
// testing the cache. See [agreementArm].
func TestACachedReaderAgreesWithItsOwnSnapshot(t *testing.T) {
	for _, arm := range []agreementArm{
		{name: "memory", open: openMemKV, sessions: 8000, comparisons: 1_500_000,
			sessionsPerWrite: 8, hits: 30},
		{name: "synced", open: openPebbleKV, durable: true, sessions: 3500,
			comparisons: 700_000, sessionsPerWrite: 60, hits: 60},
	} {
		t.Run(arm.name, func(t *testing.T) {
			agreeUnderConcurrency(t, arm)
		})
	}
}

// agreementArm is one deployment shape the agreement is checked in.
type agreementArm struct {
	name string
	open func(t *testing.T) storage.KV
	// durable makes every commit an fsync, which is what widens the window between a
	// batch becoming visible and its writer being told it landed.
	durable bool
	// sessions is how many paging sessions each reader opens. The writers run until
	// the readers are done, so this is what sets the length of the run.
	sessions int
	// comparisons is the floor on the rows compared against their own snapshot. It
	// guards against a run that finished early and proved nothing.
	comparisons int64
	// sessionsPerWrite is how many reader sessions must finish between one committed
	// row write and the next. It is the arm's write pressure, and it is counted in
	// sessions rather than in microseconds so that the race detector — which slows
	// the readers and not the writers — cannot change it. See the test's comment.
	sessionsPerWrite int64
	// hits is the floor on the percentage of reads the cache answered.
	//
	// It is the assertion that keeps this a test *of the cache*. Every other check
	// here passes trivially when nothing is ever served — the oracle is the reader's
	// own snapshot, which a miss reads directly — so a run whose hit rate had drifted
	// to nothing would still compare millions of rows and still be green while
	// exercising none of the paths the marks and the generation exist for.
	//
	// Measured on an M1 Pro, the memory arm serves 47% of its reads with `-race` and
	// 57% without, and the synced arm 92% and 89%; the floors are set well under both
	// so that they catch a change of shape rather than the run-to-run spread.
	hits int
}

func agreeUnderConcurrency(t *testing.T, arm agreementArm) {
	t.Helper()
	const (
		records = 24
		writers = 4
		readers = 6
	)
	kv := arm.open(t)

	slots := attr.MustTable()
	cache := attr.NewRowCache(512)
	ix := attr.NewIndexer(slots, attr.WithRowCache(cache))
	ctx := tenant.NewContext(context.Background(), testTenant)

	ids := make([]id.ID, records)
	for i := range ids {
		ids[i] = id.New()
	}
	// A starting state, so a reader's first session has rows to find.
	for i, rid := range ids {
		rec := sampleRecord(rid)
		rec.Fields.Importance = float32(i) / float32(records)
		stage(t, ctx, kv, ix, rid, rec)
	}

	var compared, mismatched, failures, written, finished, due atomic.Int64
	stop := make(chan struct{})
	var writing, reading sync.WaitGroup

	for w := range writers {
		writing.Add(1)
		go func() {
			defer writing.Done()
			rng := rand.New(rand.NewPCG(uint64(w)+1, 0x2545f4914f6cdd1d))
			for {
				// This write's share of the reading, claimed before it is waited for so
				// that the writers divide the sessions between them rather than all
				// waking on the same one.
				if !waitForSessions(stop, &finished, due.Add(arm.sessionsPerWrite)) {
					return
				}
				rid := ids[rng.IntN(records)]
				rec := sampleRecord(rid)
				rec.Fields.Importance = rng.Float32()
				err := txn.Do(ctx, kv, func(tx txn.Tx) error {
					return ix.Stage(ctx, tx, testTenant, testNS, rid, rec)
				}, txn.Sync(arm.durable))
				switch {
				case err == nil:
					written.Add(1)
				case errs.Is(err, errs.Conflict):
					failures.Add(1)
				default:
					t.Errorf("writing: %v", err)
					return
				}
			}
		}()
	}

	for r := range readers {
		reading.Add(1)
		go func() {
			defer reading.Done()
			rng := rand.New(rand.NewPCG(uint64(r)+100, 0x9e3779b97f4a7c15))
			for range arm.sessions {
				n, bad := readSessionOnce(ctx, kv, cache, slots, ids, rng)
				compared.Add(int64(n))
				finished.Add(1)
				if bad != "" {
					mismatched.Add(1)
					t.Error(bad)
					return
				}
			}
		}()
	}
	reading.Wait()
	close(stop)
	writing.Wait()

	if n := compared.Load(); n < arm.comparisons {
		t.Fatalf("only %d comparisons; the test is meant to run thousands", n)
	}
	if n := mismatched.Load(); n != 0 {
		t.Fatalf("%d sessions saw a row their own snapshot does not hold", n)
	}
	if written.Load() == 0 {
		t.Fatal("no write committed while the readers ran, so no reader raced one")
	}
	hits, misses, _ := cache.Stats()
	served := int(100 * hits / (hits + misses))
	t.Logf("%d comparisons, %d hits and %d misses (%d%% served), %d writes committed, "+
		"%d that exhausted their retries",
		compared.Load(), hits, misses, served, written.Load(), failures.Load())
	if served < arm.hits {
		t.Fatalf("the cache answered %d%% of %d reads, want at least %d%%: this run exercised "+
			"the miss path, which every check here passes trivially", served, hits+misses, arm.hits)
	}
}

// waitForSessions blocks until the readers have finished n sessions between them,
// and reports false if they stopped before they got there.
func waitForSessions(stop <-chan struct{}, finished *atomic.Int64, n int64) bool {
	for finished.Load() < n {
		select {
		case <-stop:
			return false
		default:
		}
		time.Sleep(100 * time.Microsecond)
	}
	return true
}

func openMemKV(t *testing.T) storage.KV {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}

// openPebbleKV is the arm where a commit is an fsync, so the interval between a
// batch becoming visible and its writer being told it landed is wide enough for a
// reader to be inside it on purpose rather than by luck.
func openPebbleKV(t *testing.T) storage.KV {
	t.Helper()
	kv, err := pebble.Open(t.TempDir(), pebble.Options{})
	if err != nil {
		t.Fatalf("opening pebble: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}

// readSessionOnce is one paging session: a generation, then a snapshot, then two
// passes over the records through the cache, each answer checked against the
// session's own snapshot. It returns how many comparisons it made and the first
// disagreement it found.
//
// The two passes matter. The first can only miss, so it is the second that reads
// what the cache retained — including anything another session put there in
// between.
func readSessionOnce(ctx context.Context, kv storage.KV, cache *attr.RowCache,
	slots *schema.Slots, ids []id.ID, rng *rand.Rand) (int, string) {
	// The registry's order: the generation first, so a write that lands before the
	// snapshot is taken leaves this reader with one that has moved on.
	g0 := cache.Generation(testTenant)
	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	lower, upper := keys.TenantRange(testTenant)
	seek := storage.NewSeekReader(snap, lower, upper)
	defer func() { _ = seek.Close() }()
	r := attr.NewCachedReader(seek, cache, slots, testTenant, g0)
	defer r.Report()

	compared := 0
	for pass := range 2 {
		for _, i := range rng.Perm(len(ids)) {
			rid := ids[i]
			got, err := r.Row(ctx, testTenant, testNS, rid)
			if err != nil {
				return compared, fmt.Sprintf("the cached reader failed on %s: %v", rid, err)
			}
			want, err := snapshotRow(ctx, snap, slots, rid)
			if err != nil {
				return compared, fmt.Sprintf("reading %s from the snapshot itself: %v", rid, err)
			}
			compared++
			if diff := rowDiff(got, want); diff != "" {
				return compared, fmt.Sprintf("pass %d, record %s: %s", pass, rid, diff)
			}
		}
	}
	return compared, ""
}

// snapshotRow is the oracle: the row as the pinned snapshot holds it, read and
// decoded without the cache anywhere near it.
func snapshotRow(ctx context.Context, snap storage.Snapshot, slots *schema.Slots,
	rid id.ID) (*attr.Row, error) {
	value, err := snap.Get(ctx, attr.RowKey(testTenant, testNS, rid))
	if errs.Is(err, errs.NotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	row, err := attr.DecodeRow(value, slots)
	if err != nil {
		// Undecodable is absent everywhere else on this path, so it is here too.
		return nil, nil
	}
	return row, nil
}

// rowDiff describes the first difference between two rows, or returns "".
func rowDiff(got, want *attr.Row) string {
	switch {
	case got == nil && want == nil:
		return ""
	case got == nil:
		return "the cache reported no row where the snapshot holds one"
	case want == nil:
		return "the cache returned a row the snapshot does not hold"
	}
	if got.Len() != want.Len() {
		return fmt.Sprintf("the cached row carries %d slots, the snapshot's %d", got.Len(), want.Len())
	}
	for _, slot := range want.Slots() {
		w, _ := want.Get(slot)
		g, ok := got.Get(slot)
		if !ok {
			return fmt.Sprintf("slot %d is missing from the cached row", slot)
		}
		if g != w {
			return fmt.Sprintf("slot %d is %s in the cached row and %s in the snapshot", slot, g, w)
		}
	}
	return ""
}
