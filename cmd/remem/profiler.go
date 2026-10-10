//go:build pprof

// The diagnostic profiler: a build made with `-tags pprof` serves Go's runtime
// profiles on a listener of its own, with mutex and block profiling switched
// on. No release is built with the tag, so no shipped binary carries any of
// this.
//
// It has no setting. main.go's rule is that every setting is a flag, an
// environment variable or a TOML line with one name in all three, and a
// diagnostic that needed one would either break that rule or add a setting to
// every edition for the sake of a build none of them ship. The build tag is
// the switch; the address and the two rates are constants.

package main

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
)

const (
	// profilerAddr is every interface, because the diagnostic build runs in a
	// container and is reached through a port published to the host's loopback.
	// It is not the API listener, so nothing it serves is behind authentication.
	profilerAddr = ":6060"
	// profilerMutexFraction samples one in five contention events on a held
	// mutex, reported at the site that *released* it.
	profilerMutexFraction = 5
	// profilerBlockRateNs samples on average one blocking event per 10µs spent
	// blocked, reported at the site that *waited*. Waits on the scale this
	// investigates are milliseconds, so the sampling is effectively complete
	// for them and costs little for the microsecond waits it thins out.
	profilerBlockRateNs = 10_000
)

func init() {
	startProfiler = func(log *slog.Logger) error {
		addr, err := serveProfiles(profilerAddr, log)
		if err != nil {
			return err
		}
		log.Warn("diagnostic build: runtime profiles are served without authentication",
			"addr", addr.String(), "mutex_fraction", profilerMutexFraction,
			"block_rate_ns", profilerBlockRateNs)
		return nil
	}
}

// serveProfiles switches on mutex and block profiling and serves every runtime
// profile under /debug/pprof/ on addr, on a mux of its own rather than
// http.DefaultServeMux.
func serveProfiles(addr string, log *slog.Logger) (net.Addr, error) {
	runtime.SetMutexProfileFraction(profilerMutexFraction)
	runtime.SetBlockProfileRate(profilerBlockRateNs)

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := http.Serve(ln, mux); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Error("the diagnostic profiler stopped", "err", err)
		}
	}()
	return ln.Addr(), nil
}
