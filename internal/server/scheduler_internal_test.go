package server

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/jobs"
)

// The scheduler's tick has to come from what is actually registered.
//
// It used to come from jobs.retention, which held only while the reaper was the
// one recurring type and took its interval from the same setting. Phase 10's
// end-to-end run found the consequence: lifecycle.sweep_interval set to five
// seconds, and a scheduler that looked once a minute — an operator setting a
// dial and getting something else.
func TestTheSchedulerTickFollowsTheShortestRegisteredInterval(t *testing.T) {
	entry := func(typ string, every time.Duration) jobs.Entry {
		return jobs.Entry{Type: jobs.Type(typ), Every: every}
	}

	tests := []struct {
		name      string
		recurring []jobs.Entry
		poll      time.Duration
		want      time.Duration
	}{{
		name:      "a five-second sweep is not looked at once a minute",
		recurring: []jobs.Entry{entry("lifecycle.maintain", 5*time.Second), entry("jobs.reap", 6*time.Hour)},
		poll:      time.Second,
		want:      1250 * time.Millisecond, // a quarter of the sweep interval
	}, {
		name:      "an hourly sweep ticks well inside the hour",
		recurring: []jobs.Entry{entry("lifecycle.maintain", time.Hour), entry("jobs.reap", 6*time.Hour)},
		poll:      time.Second,
		want:      time.Minute, // an hour quartered is 15m, capped at a minute
	}, {
		name:      "never finer than the pool polls",
		recurring: []jobs.Entry{entry("lifecycle.maintain", 8*time.Second)},
		poll:      10 * time.Second,
		want:      10 * time.Second,
	}, {
		name:      "nothing recurring still ticks",
		recurring: nil,
		poll:      time.Second,
		want:      time.Minute,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := schedulerTick(tc.recurring, tc.poll); got != tc.want {
				t.Fatalf("tick is %v, want %v", got, tc.want)
			}
		})
	}
}

// The property that matters, stated directly: the scheduler must look at least
// as often as the fastest thing registered wants to run, or that interval is a
// number the operator wrote and the server ignored.
func TestTheTickIsNeverCoarserThanTheShortestInterval(t *testing.T) {
	for _, every := range []time.Duration{
		time.Second, 5 * time.Second, 30 * time.Second,
		time.Minute, 10 * time.Minute, time.Hour, 24 * time.Hour,
	} {
		got := schedulerTick([]jobs.Entry{{Type: "x", Every: every}}, time.Second)
		if got > every {
			t.Errorf("a type registered every %v is looked at every %v", every, got)
		}
	}
}
