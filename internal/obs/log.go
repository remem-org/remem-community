// Package obs is Remem's observability: the one place a logger is constructed,
// the registry every metric is registered on, and the context plumbing that
// carries tenant, node, shard, request, job and migration identity.
//
// Spec §50 makes observability core infrastructure rather than a decoration.
// Two rules follow, and both are enforced by tests in this package:
//
//   - No package outside internal/obs constructs a logger. Format, level and
//     redaction are decided once.
//   - Memory content is never logged. A memory is the user's data; a log line
//     is not the place it lives.
//
// Identity travels in the context, not in call signatures: a handler puts the
// request id and tenant into the context once, and every line logged under it
// carries them, including from code several layers down that knows nothing
// about requests.
package obs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
)

// Format is a log output encoding.
type Format string

const (
	// FormatJSON is one JSON object per line: the production format.
	FormatJSON Format = "json"
	// FormatText is slog's key=value form, for a developer's terminal.
	FormatText Format = "text"
)

// LogConfig describes a logger. internal/config owns the user-facing settings
// and converts them into this; obs does not import config, so that config may
// log while it validates.
type LogConfig struct {
	Level  slog.Level
	Format Format
	// Output defaults to os.Stderr. Logs go to stderr, not stdout, so a
	// stdio MCP client's protocol stream stays clean.
	Output io.Writer
	// AddSource attaches file:line. Off by default: it is not free.
	AddSource bool
}

// NewLogger builds the logger for a process.
//
// It panics on an unknown format. Configuration is validated at startup
// (spec §51), so reaching here with a format nobody defined is a programming
// error, and quietly picking a default would hide it.
func NewLogger(cfg LogConfig) *slog.Logger {
	out := cfg.Output
	if out == nil {
		out = os.Stderr
	}
	opts := &slog.HandlerOptions{Level: cfg.Level, AddSource: cfg.AddSource}

	var h slog.Handler
	switch cfg.Format {
	case FormatJSON, "":
		h = slog.NewJSONHandler(out, opts)
	case FormatText:
		h = slog.NewTextHandler(out, opts)
	default:
		panic(fmt.Sprintf("obs: unknown log format %q; want %q or %q", cfg.Format, FormatJSON, FormatText))
	}
	return slog.New(&contextHandler{inner: h})
}

// LoggerTo returns a JSON logger at info level writing to w. It is the form
// tests and one-shot commands want.
func LoggerTo(w io.Writer) *slog.Logger {
	return NewLogger(LogConfig{Level: slog.LevelInfo, Format: FormatJSON, Output: w})
}

// contextHandler copies the identifiers spec §50 requires out of the context
// and onto every record, so no call site has to remember to pass them.
type contextHandler struct{ inner slog.Handler }

func (h *contextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs := contextAttrs(ctx); len(attrs) > 0 {
		r.AddAttrs(attrs...)
	}
	return h.inner.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{inner: h.inner.WithGroup(name)}
}

// defaultLogger is what Logger returns for a context carrying none. It is a
// pointer swap rather than a mutable global so that setting it during startup
// races with nothing.
var defaultLogger atomic.Pointer[slog.Logger]

// SetDefault installs l as the process logger, for both [Logger] and the
// standard library's slog.Default.
func SetDefault(l *slog.Logger) {
	defaultLogger.Store(l)
	slog.SetDefault(l)
}

type loggerKey struct{}

// WithLogger attaches a logger to ctx — used to bind per-request or per-job
// attributes once, rather than at every call site below.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// Logger returns the logger for ctx: the one the context carries, else the
// process default, else a stderr JSON logger. It never returns nil, because a
// nil check at every logging call site is a nil check nobody writes.
func Logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	if l := defaultLogger.Load(); l != nil {
		return l
	}
	l := LoggerTo(os.Stderr)
	if defaultLogger.CompareAndSwap(nil, l) {
		return l
	}
	return defaultLogger.Load()
}
