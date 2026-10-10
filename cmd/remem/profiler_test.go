//go:build pprof

package main

import (
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"testing"
)

// A diagnostic build answers the two profiles a lock investigation reads, on
// a listener of its own, and has switched both of them on: a mutex or block
// profile served with its rate at zero is an empty profile, which reads as
// "nothing waited".
func TestADiagnosticBuildServesMutexAndBlockProfiles(t *testing.T) {
	addr, err := serveProfiles("127.0.0.1:0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if got := runtime.SetMutexProfileFraction(-1); got != profilerMutexFraction {
		t.Fatalf("mutex profile fraction is %d, want %d", got, profilerMutexFraction)
	}
	for _, p := range []string{"mutex", "block", "goroutine"} {
		resp, err := http.Get("http://" + addr.String() + "/debug/pprof/" + p + "?debug=1")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || len(body) == 0 {
			t.Fatalf("/debug/pprof/%s answered %d with %d bytes", p, resp.StatusCode, len(body))
		}
	}
}
