//go:build crash

package crash

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/inspect"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
)

const (
	restartMemories = 10_000
	restartBatch    = 500
)

// restartContent is memory n's content. The token is one alphanumeric term, so
// keyword search tokenises it whole; the rest makes every content distinct, so
// an exact-content semantic search has one unambiguous answer.
func restartContent(n int) string {
	return fmt.Sprintf("restart memory %d carries token tok%dx and nothing else like it", n, n)
}

// TestRestartPreservesEverything: ten thousand memories written, the server
// stopped with SIGTERM, a new process started over the same directory — and
// every memory is fetchable by id, findable by its keyword, and found first by
// its own content, and the directory is consistent.
//
// It is the one test in this package that does not kill anything. What it
// guards is the ordinary path every deployment takes on every upgrade, which is
// also the one nobody re-checks after it has worked once.
func TestRestartPreservesEverything(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	client := &http.Client{Timeout: 30 * time.Second}

	first := spawn(t, "serve", dir)
	addr := first.waitFor("ready ", 90*time.Second)

	ids := make([]string, 0, restartMemories)
	for start := 0; start < restartMemories; start += restartBatch {
		var b strings.Builder
		b.WriteString(`{"memories":[`)
		for n := start; n < min(start+restartBatch, restartMemories); n++ {
			if n > start {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"content":%q,"tags":["restart"]}`, restartContent(n))
		}
		b.WriteString(`]}`)
		status, body, err := call(client, "POST", addr, "/api/v1/memories:batch", b.String())
		if err != nil || status != http.StatusCreated {
			t.Fatalf("creating memories %d onwards: status %d, err %v: %.300s", start, status, err, body)
		}
		var resp struct {
			Memories []struct {
				ID string `json:"id"`
			} `json:"memories"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatal(err)
		}
		for _, m := range resp.Memories {
			ids = append(ids, m.ID)
		}
	}
	if len(ids) != restartMemories {
		t.Fatalf("created %d memories, want %d", len(ids), restartMemories)
	}
	first.stop(time.Minute)

	second := spawn(t, "serve", dir)
	addr = second.waitFor("ready ", 90*time.Second)
	started := time.Now()

	// Every id, from sixteen clients at once.
	var missing atomic.Int64
	var firstMissing sync.Once
	var firstReport string
	work := make(chan int)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range work {
				status, body, err := call(client, "GET", addr, "/api/v1/memories/"+ids[n], "")
				if err != nil || status != http.StatusOK {
					missing.Add(1)
					firstMissing.Do(func() {
						firstReport = fmt.Sprintf("memory %d (%s): status %d, err %v: %.200s", n, ids[n], status, err, body)
					})
				}
			}
		}()
	}
	for n := range restartMemories {
		work <- n
	}
	close(work)
	wg.Wait()
	if missing.Load() > 0 {
		t.Fatalf("%d of %d memories could not be fetched after the restart; the first: %s",
			missing.Load(), restartMemories, firstReport)
	}

	// Every fiftieth by its keyword, every hundredth by its own content.
	for n := 0; n < restartMemories; n += 50 {
		assertTopHit(t, client, addr, "keyword", fmt.Sprintf("tok%dx", n), ids[n], n)
	}
	for n := 0; n < restartMemories; n += 100 {
		assertTopHit(t, client, addr, "semantic", restartContent(n), ids[n], n)
	}
	t.Logf("after the restart: %d fetched, %d keyword and %d semantic searches answered, in %s",
		restartMemories, restartMemories/50, restartMemories/100, time.Since(started).Round(time.Millisecond))
	second.stop(time.Minute)

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	rep, err := inspect.CheckTenant(context.Background(), kv, tenant.ID("default"))
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Clean() || rep.Records != restartMemories {
		var b strings.Builder
		for _, f := range rep.Findings {
			b.WriteString("  " + f.String() + "\n")
		}
		t.Fatalf("after two clean lives the directory holds %d records, want %d, with %d findings:\n%s",
			rep.Records, restartMemories, len(rep.Findings), b.String())
	}
}

func assertTopHit(t *testing.T, client *http.Client, addr, mode, query, want string, n int) {
	t.Helper()
	status, body, err := call(client, "POST", addr, "/api/v1/memories/search",
		fmt.Sprintf(`{"query":%q,"search_type":%q,"limit":1}`, query, mode))
	if err != nil || status != http.StatusOK {
		t.Fatalf("%s search for memory %d: status %d, err %v: %.300s", mode, n, status, err, body)
	}
	var resp struct {
		Results []struct {
			Memory struct {
				ID string `json:"id"`
			} `json:"memory"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) == 0 || resp.Results[0].Memory.ID != want {
		t.Fatalf("%s search for memory %d did not rank it first after the restart: %.300s", mode, n, body)
	}
}
