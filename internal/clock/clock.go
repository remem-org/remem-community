// Package clock is the only source of time in Remem.
//
// Spec §48 forbids arbitrary time.Now() calls in durable business logic: a
// timestamp that ends up on disk or in a replicated command must come from a
// clock the caller chose, so that lifecycle decay, TTL expiry and job leases
// are testable without sleeping and reproducible without a real elapsed hour.
//
// Business logic takes a [Clock]. Production passes [System]; tests pass
// [NewFake], whose time moves only when the test moves it.
//
// The Rust implementation needed care here for a reason worth recording:
// std::time::Instant cannot be paused, while tokio::time::Instant can, so the
// same lifecycle code was deterministic under one and not the other. In Go the
// distinction is this interface, and the guard is that no durable code path may
// call time.Now() directly.
package clock

import "time"

// Clock is the time source. Every method is safe for concurrent use.
type Clock interface {
	// Now returns the current instant.
	Now() time.Time
	// Since returns the duration elapsed since t, measured on this clock.
	Since(t time.Time) time.Duration
	// NewTicker returns a ticker delivering ticks every d. The caller must
	// Stop it. d must be positive.
	NewTicker(d time.Duration) *Ticker
	// After returns a channel that receives once, d from now.
	After(d time.Duration) <-chan time.Time
}

// Ticker delivers ticks on C. It is the one ticker type both implementations
// return, so code under test never learns which clock it holds.
//
// Like time.Ticker, C is buffered by one and a tick a slow receiver did not
// take is dropped rather than queued: a loop that falls behind sees one late
// tick, not a burst of stale ones.
type Ticker struct {
	C <-chan time.Time

	stop func()
}

// Stop stops the ticker. It is idempotent, and does not close C.
func (t *Ticker) Stop() { t.stop() }

// System returns the wall clock.
func System() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time                  { return time.Now() }
func (systemClock) Since(t time.Time) time.Duration { return time.Since(t) }
func (systemClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (systemClock) NewTicker(d time.Duration) *Ticker {
	t := time.NewTicker(d)
	return &Ticker{C: t.C, stop: t.Stop}
}
