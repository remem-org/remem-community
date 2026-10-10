//go:build !pprof

package main

import (
	"runtime"
	"testing"
)

// A shipped build profiles nothing: the diagnostic profiler is compiled in only
// under the pprof build tag, and no release is built with it.
func TestAShippedBuildHasNoProfiler(t *testing.T) {
	if startProfiler != nil {
		t.Fatal("a build without the pprof tag has a profiler to start")
	}
	if got := runtime.SetMutexProfileFraction(-1); got != 0 {
		t.Fatalf("mutex profiling is on (fraction %d) in a build without the pprof tag", got)
	}
}
