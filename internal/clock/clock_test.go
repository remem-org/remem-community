package clock_test

import (
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
)

var epoch = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

// drain reports how many ticks are waiting on c without blocking.
func drain(c <-chan time.Time) int {
	n := 0
	for {
		select {
		case <-c:
			n++
		default:
			return n
		}
	}
}

func TestFakeTickerFiresOncePerInterval(t *testing.T) {
	f := clock.NewFake(epoch)
	const interval = 100 * time.Millisecond
	tk := f.NewTicker(interval)
	defer tk.Stop()

	if n := drain(tk.C); n != 0 {
		t.Fatalf("a new ticker has not fired yet, got %d ticks", n)
	}
	f.Advance(interval - time.Nanosecond)
	if n := drain(tk.C); n != 0 {
		t.Fatalf("ticker fired before its deadline: %d ticks", n)
	}
	for i := range 3 {
		f.Advance(interval)
		if n := drain(tk.C); n != 1 {
			t.Fatalf("advance %d: want exactly 1 tick, got %d", i, n)
		}
	}
	if got, want := f.Now(), epoch.Add(4*interval-time.Nanosecond); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v", got, want)
	}
}

func TestFakeTickerCoalescesLikeTimeTicker(t *testing.T) {
	// time.Ticker drops ticks a slow receiver did not take. The fake must do
	// the same, or a job loop that falls behind under a fake clock behaves
	// differently from the same loop in production.
	f := clock.NewFake(epoch)
	tk := f.NewTicker(time.Second)
	defer tk.Stop()

	f.Advance(10 * time.Second)
	if n := drain(tk.C); n != 1 {
		t.Fatalf("want 1 coalesced tick, got %d", n)
	}
}

func TestFakeStoppedTickerNeverFires(t *testing.T) {
	f := clock.NewFake(epoch)
	tk := f.NewTicker(time.Second)
	tk.Stop()
	f.Advance(5 * time.Second)
	if n := drain(tk.C); n != 0 {
		t.Fatalf("a stopped ticker fired %d times", n)
	}
	tk.Stop() // idempotent
}

func TestFakeAfterFiresOnce(t *testing.T) {
	f := clock.NewFake(epoch)
	c := f.After(time.Minute)

	f.Advance(59 * time.Second)
	if n := drain(c); n != 0 {
		t.Fatal("After fired early")
	}
	f.Advance(time.Second)
	fired := 0
	select {
	case at := <-c:
		fired++
		if !at.Equal(epoch.Add(time.Minute)) {
			t.Fatalf("After delivered %v, want the deadline %v", at, epoch.Add(time.Minute))
		}
	default:
	}
	if fired != 1 {
		t.Fatal("After did not fire at its deadline")
	}
	f.Advance(time.Hour)
	if n := drain(c); n != 0 {
		t.Fatalf("After fired %d extra times", n)
	}
}

func TestFakeDeliversTheDeadlineNotTheDestination(t *testing.T) {
	// A tick carries the time the tick was due, so lifecycle arithmetic under a
	// fake clock sees the same instants it would in production.
	f := clock.NewFake(epoch)
	tk := f.NewTicker(time.Second)
	defer tk.Stop()

	f.Advance(90 * time.Second)
	at := <-tk.C
	if !at.Equal(epoch.Add(time.Second)) {
		t.Fatalf("tick carried %v, want the first deadline %v", at, epoch.Add(time.Second))
	}
}

func TestFakeNowAndSince(t *testing.T) {
	f := clock.NewFake(epoch)
	if !f.Now().Equal(epoch) {
		t.Fatalf("Now() = %v, want %v", f.Now(), epoch)
	}
	f.Advance(90 * time.Second)
	if got := f.Since(epoch); got != 90*time.Second {
		t.Fatalf("Since() = %v, want 90s", got)
	}
	f.Set(epoch.Add(time.Hour))
	if !f.Now().Equal(epoch.Add(time.Hour)) {
		t.Fatalf("Set did not move the clock: %v", f.Now())
	}
}

func TestFakeSetForwardFiresPendingDeadlines(t *testing.T) {
	f := clock.NewFake(epoch)
	c := f.After(time.Minute)
	f.Set(epoch.Add(2 * time.Minute))
	if n := drain(c); n != 1 {
		t.Fatalf("Set past a deadline must fire it, got %d", n)
	}
}

func TestFakeAdvanceIsSafeFromMultipleGoroutines(t *testing.T) {
	// Lifecycle tests advance the clock from a worker while the test body reads
	// it. Under -race this is the assertion that matters.
	f := clock.NewFake(epoch)
	tk := f.NewTicker(time.Millisecond)
	defer tk.Stop()

	const goroutines, steps = 8, 100
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range steps {
				f.Advance(time.Millisecond)
				_ = f.Now()
				drain(tk.C)
			}
		}()
	}
	wg.Wait()

	if got, want := f.Now(), epoch.Add(goroutines*steps*time.Millisecond); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v — Advance lost an increment", got, want)
	}
}

func TestFakeAdvanceRejectsGoingBackwards(t *testing.T) {
	f := clock.NewFake(epoch)
	defer func() {
		if recover() == nil {
			t.Fatal("Advance with a negative duration must panic, not rewind")
		}
	}()
	f.Advance(-time.Second)
}

func TestSystemClock(t *testing.T) {
	c := clock.System()
	start := c.Now()
	if start.IsZero() {
		t.Fatal("the system clock returned the zero time")
	}
	select {
	case at := <-c.After(time.Millisecond):
		if at.IsZero() {
			t.Fatal("After delivered the zero time")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("system After never fired")
	}
	if c.Since(start) <= 0 {
		t.Fatal("Since must measure forward progress")
	}

	tk := c.NewTicker(time.Millisecond)
	defer tk.Stop()
	select {
	case <-tk.C:
	case <-time.After(2 * time.Second):
		t.Fatal("system ticker never fired")
	}
}

func TestBothImplementationsSatisfyClock(t *testing.T) {
	var _ clock.Clock = clock.System()
	var _ clock.Clock = clock.NewFake(epoch)
}
