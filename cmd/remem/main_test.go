package main

import (
	"testing"

	"github.com/remem-org/remem-go/internal/config"
)

// The environment wins over the [runtime] section, and which value is in force
// has to be decidable without calling into the runtime: debug.SetGCPercent
// returns the value it replaced, not the one now in effect, so the decision is
// the only part of this that can be asserted.
func TestTheEnvironmentWinsOverTheRuntimeSection(t *testing.T) {
	// One configured pair for every case, so each case differs only in its
	// environment.
	configured := config.RuntimeConfig{GCPercent: 200, MemoryLimitBytes: 1 << 30}

	for _, tc := range []struct {
		name string
		rt   config.RuntimeConfig
		env  []string
		want runtimeDecision
	}{
		{
			name: "neither in the environment applies both configured values",
			rt:   configured,
			env:  []string{"PATH=/usr/bin", "REMEM_SERVER_HTTP_ADDR=127.0.0.1:4545"},
			want: runtimeDecision{
				gcPercent: 200, gcSource: fromConfig,
				memoryLimit: 1 << 30, memoryLimitSource: fromConfig,
			},
		},
		{
			name: "GOGC in the environment wins and is reported",
			rt:   configured,
			env:  []string{"GOGC=50"},
			want: runtimeDecision{
				gcPercent: 200, gcSource: fromEnvironment, envGOGC: "50",
				memoryLimit: 1 << 30, memoryLimitSource: fromConfig,
			},
		},
		{
			name: "GOMEMLIMIT in the environment wins and is reported",
			rt:   configured,
			env:  []string{"GOMEMLIMIT=512MiB"},
			want: runtimeDecision{
				gcPercent: 200, gcSource: fromConfig,
				memoryLimit: 1 << 30, memoryLimitSource: fromEnvironment, envGOMEMLIMIT: "512MiB",
			},
		},
		{
			// Zero is the default, so this is what an unconfigured server does:
			// pace the collector, and leave the heap otherwise unbounded.
			name: "a memory limit of zero sets no limit at all",
			rt:   config.RuntimeConfig{GCPercent: 200, MemoryLimitBytes: 0},
			env:  nil,
			want: runtimeDecision{
				gcPercent: 200, gcSource: fromConfig,
				memoryLimit: 0, memoryLimitSource: unset,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := decideRuntimeSettings(tc.rt, tc.env); got != tc.want {
				t.Errorf("decideRuntimeSettings(%+v, %v)\n got %+v\nwant %+v", tc.rt, tc.env, got, tc.want)
			}
		})
	}
}
