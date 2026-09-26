//go:build crash

package crash

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/inspect"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/server"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
)

// operatorKey is the child server's credential. It is long enough for the
// production validation the server applies to every configuration.
const operatorKey = "crash-test-operator-key-0000000000"

// unsyncedEnv turns SyncWrites off in the child. It exists for one purpose: to
// show that TestCrashDuringWriteLeavesNoPartialRecord can fail, by running it
// against a server that does not pay for durability and watching an
// acknowledged write disappear.
const unsyncedEnv = "REMEM_CRASH_UNSYNCED"

func init() { scenarios["serve"] = childServe }

// childServe runs a real server over a real Pebble directory until it is killed.
func childServe(t *testing.T, dir string) {
	cfg := config.Default()
	cfg.Storage.Path = dir
	cfg.Storage.SyncWrites = os.Getenv(unsyncedEnv) == ""
	cfg.Server.HTTPAddr = "127.0.0.1:0"
	cfg.Server.APIKey = operatorKey
	cfg.Server.RateLimitRPS = 0
	cfg.Log.Level = "error"

	srv, err := server.New(cfg, server.WithEmbedder(embeddingtest.New()))
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	go func() { _ = srv.Run(context.Background()) }()

	deadline := time.Now().Add(60 * time.Second)
	for !srv.Ready() || srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("the server never became ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	fmt.Printf("ready %s\n", srv.Addr())
	select {} // until the parent kills this process
}

// TestCrashDuringWriteLeavesNoPartialRecord: kill a server twenty times, at a
// seeded moment, while sixteen clients create, rewrite and archive memories
// over one data directory. After every kill:
//
//   - the directory is consistent: no derived row names a record that does not
//     exist, and no record lacks its attribute row (the plan's "no record with a
//     vector but no attribute row", and everything else inspect checks);
//   - every create that was acknowledged with 201 is on disk. An acknowledgement
//     is a promise, and a promise the process could break by dying is not one.
func TestCrashDuringWriteLeavesNoPartialRecord(t *testing.T) {
	const (
		rounds  = 20
		writers = 16
	)
	dir := filepath.Join(t.TempDir(), "data")
	rng := rand.New(rand.NewPCG(20260913, 1))

	var mu sync.Mutex
	var acked []id.ID

	// spawn strips every REMEM_* variable from the child, so the switch that
	// proves this test can fail has to be forwarded by name. Without this line
	// setting it on the parent changes nothing, and the "proof" runs synced
	// and passes — the vacuous result it exists to rule out.
	var extra []string
	if v := os.Getenv(unsyncedEnv); v != "" {
		extra = append(extra, unsyncedEnv+"="+v)
		t.Logf("the child runs with SyncWrites off (%s): this run is expected to FAIL", unsyncedEnv)
	}

	for round := range rounds {
		c := spawn(t, "serve", dir, extra...)
		addr := c.waitFor("ready ", 90*time.Second)

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for w := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				writeUntil(stop, addr, round, w, func(rid id.ID) {
					mu.Lock()
					acked = append(acked, rid)
					mu.Unlock()
				})
			}()
		}

		after := 200*time.Millisecond + time.Duration(rng.IntN(1800))*time.Millisecond
		time.Sleep(after)
		c.kill()
		close(stop)
		wg.Wait()

		mu.Lock()
		promised := append([]id.ID(nil), acked...)
		mu.Unlock()
		t.Logf("round %d: killed after %s; %d creates acknowledged so far", round, after, len(promised))
		verifyAfterKill(t, dir, promised, round)
	}

	// A run that acknowledged almost nothing proves almost nothing.
	if len(acked) < rounds*writers {
		t.Fatalf("only %d creates were acknowledged across %d kills; the kill window is too early to test anything",
			len(acked), rounds)
	}
}

// writeUntil is one client: mostly creates, some rewrites, some archives. It
// never hard-deletes, so every acknowledged create must still be on disk.
func writeUntil(stop <-chan struct{}, addr string, round, w int, ack func(id.ID)) {
	client := &http.Client{Timeout: 3 * time.Second}
	r := rand.New(rand.NewPCG(uint64(round), uint64(w)))
	var mine []string
	for seq := 0; ; seq++ {
		select {
		case <-stop:
			return
		default:
		}
		switch roll := r.IntN(100); {
		case roll < 70 || len(mine) == 0:
			status, body, err := call(client, "POST", addr, "/api/v1/memories", fmt.Sprintf(
				`{"content":"round %d writer %d note %d about crash safety","tags":["crash"]}`, round, w, seq))
			if err != nil || status != http.StatusCreated {
				continue // the child is being killed, or already is
			}
			var m struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(body, &m) != nil {
				continue
			}
			rid, err := id.Parse(m.ID)
			if err != nil {
				continue
			}
			ack(rid)
			mine = append(mine, m.ID)
		case roll < 90:
			_, _, _ = call(client, "PATCH", addr, "/api/v1/memories/"+mine[r.IntN(len(mine))],
				fmt.Sprintf(`{"content":"round %d writer %d rewrote this at %d"}`, round, w, seq))
		default:
			_, _, _ = call(client, "DELETE", addr, "/api/v1/memories/"+mine[r.IntN(len(mine))], "")
		}
	}
}

func call(client *http.Client, method, addr, path, body string) (int, []byte, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+addr+path, rd)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+operatorKey)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	return resp.StatusCode, out, err
}

// verifyAfterKill reopens the directory the killed child left and holds it to
// both promises.
func verifyAfterKill(t *testing.T, dir string, promised []id.ID, round int) {
	t.Helper()
	ctx := context.Background()
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatalf("round %d: reopening the directory after the kill: %v", round, err)
	}
	defer func() { _ = kv.Close() }()

	const def = tenant.ID("default")
	rep, err := inspect.CheckTenant(ctx, kv, def)
	if err != nil {
		t.Fatalf("round %d: checking the directory: %v", round, err)
	}
	if !rep.Clean() {
		var b strings.Builder
		for _, f := range rep.Findings {
			b.WriteString("  " + f.String() + "\n")
		}
		t.Fatalf("round %d: the kill left the directory inconsistent (%d records):\n%s", round, rep.Records, b.String())
	}

	for _, rid := range promised {
		if _, err := kv.Get(ctx, keys.Record(def, tenant.DefaultNamespace, keys.RecordMemory, rid)); err != nil {
			t.Fatalf("round %d: memory %s was acknowledged with 201 and is not on disk: %v", round, rid, err)
		}
	}
}
