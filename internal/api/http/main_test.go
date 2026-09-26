package http_test

import (
	"io"
	"os"
	"testing"

	"github.com/remem-org/remem-go/internal/obs"
)

// TestMain silences the request log.
//
// The middleware logs every request, which is correct in production and turns
// this package's output into several hundred JSON lines that bury the one
// assertion that failed. The logger is still exercised — it is installed, not
// removed — so a handler that panicked while logging would still fail here.
func TestMain(m *testing.M) {
	obs.SetDefault(obs.LoggerTo(io.Discard))
	os.Exit(m.Run())
}
