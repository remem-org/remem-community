package clock

import (
	"sync"
	"time"
)

// Fake is a [Clock] whose time moves only when a test moves it, with
// [Fake.Advance] or [Fake.Set].
//
// Every waiting ticker and timer whose deadline the movement passes fires
// during the call, in chronological order, before the call returns — so a test
// may advance the clock and immediately assert on what the tick produced,
// without a sleep and without a race.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	deadline time.Time
	period   time.Duration // zero for a one-shot timer
	ch       chan time.Time
	stopped  bool
}

// NewFake returns a fake clock reading t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// FakeStart is the instant a test clock starts at unless it says otherwise:
// 2026-01-01T00:00:00Z.
//
// It is a fixed, readable, UTC value rather than time.Now() so that a failure
// message quotes the same timestamp on every run and in every timezone, and so
// that a test asserting "created before updated" cannot pass by accident on a
// machine whose clock happens to tick between two calls.
var FakeStart = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// Now returns the fake clock's current instant.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the duration from t to the fake clock's current instant.
func (f *Fake) Since(t time.Time) time.Duration { return f.Now().Sub(t) }

// NewTicker returns a ticker that fires every d of fake time.
func (f *Fake) NewTicker(d time.Duration) *Ticker {
	if d <= 0 {
		panic("clock: NewTicker requires a positive interval")
	}
	return f.newTicker(d, d)
}

// After returns a channel that receives once, d of fake time from now.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	return f.newTicker(d, 0).C
}

func (f *Fake) newTicker(first, period time.Duration) *Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()

	w := &waiter{
		deadline: f.now.Add(first),
		period:   period,
		ch:       make(chan time.Time, 1),
	}
	f.waiters = append(f.waiters, w)
	return &Ticker{C: w.ch, stop: func() { f.stop(w) }}
}

func (f *Fake) stop(w *waiter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.stopped = true
	f.forget(w)
}

// forget removes w from the waiter list. The caller holds f.mu.
func (f *Fake) forget(w *waiter) {
	for i, other := range f.waiters {
		if other == w {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
			return
		}
	}
}

// Advance moves the clock forward by d, firing every deadline it passes.
//
// It panics on a negative d: time does not run backwards, and a test that
// wanted it to meant [Fake.Set].
func (f *Fake) Advance(d time.Duration) {
	if d < 0 {
		panic("clock: Fake.Advance cannot move time backwards; use Set")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advanceTo(f.now.Add(d))
}

// Set moves the clock to t.
//
// Moving forward fires every deadline passed, exactly as Advance does. Moving
// backwards is allowed — it is how a test reproduces clock skew or an operator
// correction — and fires nothing: the pending deadlines simply become further
// away.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.After(f.now) {
		f.advanceTo(t)
		return
	}
	f.now = t
}

// advanceTo walks the clock to target, firing waiters in deadline order. The
// caller holds f.mu.
func (f *Fake) advanceTo(target time.Time) {
	for {
		w := f.earliestDue(target)
		if w == nil {
			break
		}
		// The clock reads the deadline while the tick is delivered, so anything
		// the receiver computes from Now() sees the instant it was scheduled
		// for rather than where this Advance is heading.
		f.now = w.deadline
		select {
		case w.ch <- w.deadline:
		default: // a tick the receiver has not taken yet; drop it, as time.Ticker does
		}
		if w.period <= 0 {
			w.stopped = true
			f.forget(w)
			continue
		}
		w.deadline = w.deadline.Add(w.period)
	}
	f.now = target
}

// earliestDue returns the waiter with the earliest deadline at or before
// target, or nil. The caller holds f.mu.
func (f *Fake) earliestDue(target time.Time) *waiter {
	var found *waiter
	for _, w := range f.waiters {
		if w.stopped || w.deadline.After(target) {
			continue
		}
		if found == nil || w.deadline.Before(found.deadline) {
			found = w
		}
	}
	return found
}
