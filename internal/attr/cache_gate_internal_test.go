package attr

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
)

// The staging gate's tests: what happens when too many writes are in flight to
// track, how the cache comes back, and that turning it off mid-flight serves
// and stores nothing. The generation and mark tests are in cache_internal_test.go.

// The staging marks are bounded, and past the bound the cache turns itself off
// rather than hold a mark it cannot record. An unrecorded mark would be a commit
// window nothing guards, so serving nothing is the only safe answer; it takes a
// transaction dropped without being closed, many times over, to get here.
//
// It says so when it happens. A cache that has silently stopped caching is a
// latency regression an operator can only find by subtracting two counters.
func TestTooManyStagedWritesTurnTheCacheOffAndSaySo(t *testing.T) {
	c := NewRowCache(8)
	logged := captureLogs(t)

	rid := idOf(0)
	g := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); !ok {
		t.Fatal("the cache did not hold the row it was given")
	}

	// One mark per row, past the bound, none of them given back.
	for i := range maxStagedRows + 1 {
		c.beginWrite(cacheTenant, cacheNS, idOf(i+1))
	}

	if n := c.Len(); n != 0 {
		t.Errorf("a cache that turned itself off still holds %d entries", n)
	}
	c.store(cacheTenant, cacheNS, rid, c.Generation(cacheTenant), NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, c.Generation(cacheTenant)); ok {
		t.Error("a cache that turned itself off served a row")
	}
	if out := logged(); !strings.Contains(out, `"level":"WARN"`) ||
		!strings.Contains(out, "turned itself off") ||
		!strings.Contains(out, fmt.Sprintf(`"outstanding":%d`, maxStagedRows+1)) {
		t.Errorf("the cache turned itself off without logging a warning naming the count; got:\n%s", out)
	}

	// It still asks for its marks back while it is off, because its generations
	// must keep moving and a write it has not counted is one that could straddle
	// the instant it comes back on.
	if _, ok := c.beginWrite(cacheTenant, cacheNS, idOf(maxStagedRows+2)); !ok {
		t.Fatal("a cache that is off stopped asking for its staged writes back")
	}
}

// And it comes back once they drain. Nothing is in flight at that moment, so no
// write is between its stage and its end, and an entry stored from
// then on cannot straddle one.
func TestTheCacheComesBackWhenItsStagedWritesDrain(t *testing.T) {
	c := NewRowCache(8)
	logged := captureLogs(t)

	marks := make([]uint64, 0, maxStagedRows+1)
	for i := range maxStagedRows + 1 {
		seq, _ := c.beginWrite(cacheTenant, cacheNS, idOf(i))
		marks = append(marks, seq)
	}
	rid := idOf(maxStagedRows + 5)
	c.store(cacheTenant, cacheNS, rid, c.Generation(cacheTenant), NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, c.Generation(cacheTenant)); ok {
		t.Fatal("a cache that turned itself off served a row")
	}

	// Every one of them is closed, as every transaction is.
	for i, seq := range marks {
		c.finishWrite(cacheTenant, cacheNS, idOf(i), seq)
	}

	g := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); !ok {
		t.Error("the cache was still off after every staged write had been given back")
	}
	if out := logged(); !strings.Contains(out, "is on again") {
		t.Errorf("the cache came back without saying so; got:\n%s", out)
	}
}

// A mark given back more times than it was handed out is a bug in the pairing, and
// the cache treats it as one: it says so at error, clamps the count, and stays off
// for good rather than carry on with a bound that is no longer a count of anything.
//
// Coming back would be the dangerous answer. The gate re-enables on the strength of
// "nothing is in flight", and a count that has undercounted its marks reaches zero
// while writes are still inside their commit windows — so the cache would serve the
// straddle the marks exist to prevent. Staying off costs latency and nothing else.
//
// The extra leave is driven through the gate directly, which is the only way in:
// every path that gives a mark back took one first.
func TestAGateThatLosesCountStaysOffForGood(t *testing.T) {
	c := NewRowCache(8)
	logged := captureLogs(t)
	rid := idOf(0)

	g := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, served := c.lookup(cacheTenant, cacheNS, rid, g); !served {
		t.Fatal("the cache did not hold the row it was given")
	}

	// One write, finished properly, and then one mark too many back.
	seq, _ := c.beginWrite(cacheTenant, cacheNS, rid)
	c.finishWrite(cacheTenant, cacheNS, rid, seq)
	c.reportGate(c.staging.leave())

	if !c.staging.off() {
		t.Fatal("a gate that had given back more marks than it handed out kept serving")
	}
	if n := c.Len(); n != 0 {
		t.Errorf("a cache that stopped for good still holds %d entries", n)
	}
	g = c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, served := c.lookup(cacheTenant, cacheNS, rid, g); served {
		t.Error("a cache that stopped for good served a row")
	}

	// A clean write and its finish is exactly what would turn an ordinary disabled
	// cache back on. This one does not come back.
	seq, _ = c.beginWrite(cacheTenant, cacheNS, rid)
	c.finishWrite(cacheTenant, cacheNS, rid, seq)
	if !c.staging.off() {
		t.Error("a gate that had lost count came back when its marks drained; the count " +
			"it drained to zero is not a count of the writes in flight")
	}

	if out := logged(); !strings.Contains(out, `"level":"ERROR"`) ||
		!strings.Contains(out, "more times than it was handed out") {
		t.Errorf("the cache stopped for good without logging an error saying why; got:\n%s", out)
	}
}

// The clear that follows a trip cannot wipe the mark of a write that staged after
// it, because the gate is held for the whole of the transition.
//
// This is a window that was open. The switch was thrown under the gate's lock, the
// lock was released, and the clear ran after it. In between the marks could drain
// to zero, the gate could turn the cache back on, and a new writer could stage a row
// in it — and the clear then wiped that live writer's mark, leaving its commit
// window unguarded and the row cacheable inside it.
//
// The test drives exactly that interleaving, from inside the clear: a goroutine
// drains every mark and stages a new write while the clear is running. Under the
// fix it cannot get in front — its first leave wants the gate the clear is holding —
// so the sleep is slack for a race that can no longer be won, not a wait for one.
func TestTheClearThatFollowsATripCannotWipeALaterWritersMark(t *testing.T) {
	c := NewRowCache(8)
	captureLogs(t) // the drain below says it turned the cache back on
	rid := idOf(0)

	// Fill the gate to its bound, so that the next mark is the one that trips it.
	for i := range maxStagedRows {
		if _, tripped := c.staging.enter(); tripped != 0 {
			t.Fatalf("the gate tripped on mark %d of a bound of %d", i+1, maxStagedRows)
		}
	}

	var (
		raced  = make(chan struct{})
		seq    uint64
		staged bool
	)
	clear := c.staging.clear
	c.staging.clear = func() {
		go func() {
			defer close(raced)
			// Every outstanding write finishes, which is what turns the cache back
			// on, and a new write then stages a row in it.
			for range maxStagedRows + 1 {
				c.reportGate(c.staging.leave())
			}
			if c.staging.off() {
				return
			}
			seq, staged = c.beginWrite(cacheTenant, cacheNS, rid)
		}()
		time.Sleep(50 * time.Millisecond)
		clear()
	}

	if _, tripped := c.staging.enter(); tripped == 0 {
		t.Fatal("the mark past the bound did not trip the gate")
	}
	<-raced
	if !staged {
		t.Fatal("the cache did not come back on after its marks drained")
	}

	// The new write's mark is still on its row, so nothing may serve it until that
	// write finishes — which is the whole of what a mark is for.
	g := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, served := c.lookup(cacheTenant, cacheNS, rid, g); served {
		t.Error("a clear left over from a trip wiped the mark of a write that staged " +
			"after it: the row is cacheable while that write's commit window is open")
	}

	// And once it finishes, the row is cacheable again.
	c.finishWrite(cacheTenant, cacheNS, rid, seq)
	g = c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, served := c.lookup(cacheTenant, cacheNS, rid, g); !served {
		t.Error("the row stayed quiesced after the write that marked it had committed")
	}
}

// A cache that turns itself off while a lookup or a store is waiting for its shard
// serves nothing and stores nothing: the switch is read inside the shard lock, so
// "off" means it at the instant the answer is given rather than at the instant the
// call started.
//
// Read before the lock, the check was a decision taken before the wait: a store that
// found the cache on and then queued behind the clear landed its entry in a shard the
// clear had already emptied, and a lookup that found it on served a row from a cache
// that was off by the time it answered. Holding the shard here is what the clear
// does, so this is that wait with the race taken out of it — the call cannot get past
// the lock until the switch has been thrown.
func TestACacheThatTurnsOffWhileAShardIsHeldServesAndStoresNothing(t *testing.T) {
	for name, arm := range map[string]struct {
		warm bool
		// act does the thing the switch must stop, and reports whether it happened.
		act func(c *RowCache, rid id.ID, gen uint64) bool
	}{
		"store": {false, func(c *RowCache, rid id.ID, gen uint64) bool {
			c.store(cacheTenant, cacheNS, rid, gen, NewRow(SchemaVersion))
			return c.Len() > 0
		}},
		"lookup": {true, func(c *RowCache, rid id.ID, gen uint64) bool {
			_, served := c.lookup(cacheTenant, cacheNS, rid, gen)
			return served
		}},
	} {
		t.Run(name, func(t *testing.T) {
			c := NewRowCache(8)
			rid := idOf(0)
			gen := c.Generation(cacheTenant)
			if arm.warm {
				c.store(cacheTenant, cacheNS, rid, gen, NewRow(SchemaVersion))
			}

			sh := c.shard(c.key(cacheTenant, cacheNS, rid))
			sh.mu.Lock()
			var happened bool
			reached, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				close(reached)
				happened = arm.act(c, rid, gen)
			}()
			<-reached
			// It is at the shard lock this test holds, with nothing it can do until
			// the lock comes free. The switch is thrown in the meantime.
			time.Sleep(20 * time.Millisecond)
			c.staging.disabled.Store(true)
			sh.mu.Unlock()
			<-done

			if happened {
				t.Errorf("a %s that was waiting for its shard went ahead in a cache that "+
					"had turned itself off while it waited", name)
			}
		})
	}
}

// captureLogs sends the process logger to a buffer for the test, and returns what it
// has collected. The cache logs outside a request, so obs.Logger reaches it through
// the process default exactly as it does for the job framework.
//
// The buffer is guarded, because the cache says what it does from whichever goroutine
// did it — a drain running in one while the test reads what it logged from another is
// the shape of TestTheClearThatFollowsATripCannotWipeALaterWritersMark, and
// bytes.Buffer is not safe for that.
//
// It swaps a process-wide default, so a test that calls this one must not call
// t.Parallel: two of them would each see the other's lines and the loser's Cleanup
// would restore a logger the winner had already replaced.
func captureLogs(t *testing.T) func() string {
	t.Helper()
	previous := obs.Logger(context.Background())
	buf := &guardedBuffer{}
	obs.SetDefault(obs.LoggerTo(buf))
	t.Cleanup(func() { obs.SetDefault(previous) })
	return buf.String
}

// guardedBuffer is a bytes.Buffer that can be written from one goroutine while it is
// read from another.
type guardedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *guardedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *guardedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
