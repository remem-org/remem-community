package server_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/obs"
)

// lockedBuffer is a log sink the server's goroutines may write concurrently.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// server.api_keys keeps working but carries no user, so every key in it is one
// unattributed caller. An operator is told once, at start-up, what replaces it.
func TestConfiguredKeysAreAnnouncedAsDeprecatedOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		keys []string
		want int
	}{
		{"with api_keys", []string{"an-agent-key-long-enough@default"}, 1},
		{"without", nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Server.APIKey = "an-operator-key-long-enough-for-this-test"
			cfg.Server.APIKeys = tc.keys
			srv := mustNew(t, cfg)

			logs := &lockedBuffer{}
			ctx, cancel := context.WithCancel(obs.WithLogger(context.Background(), obs.LoggerTo(logs)))
			done := make(chan error, 1)
			go func() { done <- srv.Run(ctx) }()
			waitReady(t, srv)
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}

			got := strings.Count(logs.String(), "remem-admin users create")
			if got != tc.want {
				t.Fatalf("the deprecation notice appeared %d times, want %d:\n%s", got, tc.want, logs)
			}
		})
	}
}
