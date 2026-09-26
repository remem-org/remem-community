package server

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SignalContext returns a context cancelled by SIGINT or SIGTERM, and a
// function that releases the handler.
//
// A second signal is deliberately *not* trapped: it restores the default
// behaviour, so an operator who has waited through the shutdown timeout and
// pressed Ctrl-C again gets the process killed rather than a second polite
// request. signal.NotifyContext gives exactly that — it stops handling after
// the first — and this wrapper exists to say so where a reader will find it.
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}
