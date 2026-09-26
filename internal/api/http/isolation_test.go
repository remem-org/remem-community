package http_test

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/tenant"
)

// isoTenants and the storm's shape. Ten tenants, a hundred operations each, all
// thousand in flight together — the plan's numbers.
const (
	isoTenants   = 10
	isoWorkers   = 10 // per tenant
	isoOpsPerRun = 10 // per worker
)

func isoTenant(i int) string { return fmt.Sprintf("iso-%d", i) }

// isoMarker is one alphanumeric token per tenant, so keyword search tokenises
// it as a single term and a leak is a substring anyone can grep for.
func isoMarker(i int) string { return fmt.Sprintf("isomark%dx", i) }

// isoState is what one tenant's workers share: the ids it has created, and
// every response body and status it has received.
type isoState struct {
	mu     sync.Mutex
	ids    []string
	bodies []string
	ok2xx  int
	fives  []string
}

func (s *isoState) record(op string, rr *httptest.ResponseRecorder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bodies = append(s.bodies, op+" -> "+rr.Body.String())
	switch {
	case rr.Code >= 200 && rr.Code < 300:
		s.ok2xx++
	case rr.Code >= 500:
		s.fives = append(s.fives, fmt.Sprintf("%s: %d %s", op, rr.Code, rr.Body.String()))
	}
}

func (s *isoState) pick(r *rand.Rand) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ids) == 0 {
		return "", false
	}
	return s.ids[r.IntN(len(s.ids))], true
}

// TestTenantIsolationUnderConcurrency: every cross-tenant test before this one
// was sequential, and the isolation defects this codebase has actually had were
// state shared across requests — a context value an outer middleware read, a
// registry keyed without its tenant. Concurrency is what exposes those.
func TestTenantIsolationUnderConcurrency(t *testing.T) {
	srv := newServer(t)
	states := make([]*isoState, isoTenants)
	for i := range states {
		states[i] = &isoState{}
	}

	// Each tenant is seeded with one memory first, so the foreign-id probes
	// have something to aim at from the first operation.
	for i := range isoTenants {
		rr := sendJSONFor(t, srv, isoTenant(i), "POST", remhttp.APIPrefix+"/memories",
			fmt.Sprintf(`{"content":"seed %s","tags":[%q]}`, isoMarker(i), isoMarker(i)))
		if rr.Code != http.StatusCreated {
			t.Fatalf("seeding %s: %d %s", isoTenant(i), rr.Code, rr.Body.String())
		}
		states[i].ids = append(states[i].ids, decodeID(t, rr))
		states[i].record("seed", rr)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for ti := range isoTenants {
		for w := range isoWorkers {
			wg.Add(1)
			go func(ti, w int) {
				defer wg.Done()
				<-start
				r := rand.New(rand.NewPCG(uint64(ti), uint64(w)))
				for op := range isoOpsPerRun {
					isoOperation(t, srv, states, ti, w*isoOpsPerRun+op, r)
				}
			}(ti, w)
		}
	}
	close(start)
	wg.Wait()

	// Every id every tenant owns, known only now that the storm is over.
	owner := map[string]int{}
	for i, s := range states {
		for _, rid := range s.ids {
			owner[rid] = i
		}
	}

	for i, s := range states {
		for _, f := range s.fives {
			t.Errorf("%s: a 5xx under concurrency: %s", isoTenant(i), f)
		}
		for _, body := range s.bodies {
			for j := range isoTenants {
				if j != i && strings.Contains(body, isoMarker(j)) {
					t.Fatalf("%s received %s's marker:\n%s", isoTenant(i), isoTenant(j), clip(body))
				}
			}
			label, resp, _ := strings.Cut(body, " -> ")
			for rid, j := range owner {
				// A foreign-id probe's 404 echoes the id the caller itself sent,
				// in its detail and its instance path. That is the caller's own
				// input coming back, not another tenant's data, and the first run
				// of this test reported it as a leak.
				if label == "foreign:"+rid {
					continue
				}
				if j != i && strings.Contains(resp, rid) {
					t.Fatalf("%s received an id %s created:\n%s", isoTenant(i), isoTenant(j), clip(body))
				}
			}
		}
	}

	// The metrics saw each tenant's traffic as that tenant's: the Phase 3
	// tenantSlot defect, in concurrent form.
	scrape := send(t, srv, keyRoot, "GET", "/metrics", nil)
	if scrape.Code != http.StatusOK {
		t.Fatalf("scraping: %d", scrape.Code)
	}
	for i, s := range states {
		if got := sum2xxFor(scrape.Body.String(), isoTenant(i)); got != s.ok2xx {
			t.Errorf("metrics count %d successful requests for %s; the test observed %d",
				got, isoTenant(i), s.ok2xx)
		}
	}

	// A cursor is bound to the tenant that received it.
	first := sendFor(t, srv, keyRoot, "iso-0", "GET", remhttp.APIPrefix+"/memories?limit=1")
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.NextCursor == "" {
		t.Fatalf("iso-0's first page carries no cursor (%d): %s", first.Code, first.Body.String())
	}
	stolen := sendFor(t, srv, keyRoot, "iso-1", "GET",
		remhttp.APIPrefix+"/memories?limit=1&cursor="+page.NextCursor)
	if stolen.Code < 400 || stolen.Code >= 500 {
		t.Fatalf("iso-0's cursor presented by iso-1: %d, want a refusal: %s", stolen.Code, stolen.Body.String())
	}
	if strings.Contains(stolen.Body.String(), isoMarker(0)) {
		t.Fatal("refusing iso-0's cursor to iso-1 showed iso-1 iso-0's data")
	}
}

// isoOperation runs one drawn operation for tenant ti.
func isoOperation(t *testing.T, srv http.Handler, states []*isoState, ti, seq int, r *rand.Rand) {
	s := states[ti]
	tid, mark := isoTenant(ti), isoMarker(ti)
	mem := remhttp.APIPrefix + "/memories"

	switch roll := r.IntN(100); {
	case roll < 30: // create
		rr := sendJSONFor(t, srv, tid, "POST", mem,
			fmt.Sprintf(`{"content":"tenant %s note %d about raft and %s","tags":[%q]}`, tid, seq, mark, mark))
		s.record("create", rr)
		if rr.Code == http.StatusCreated {
			rid := decodeID(t, rr)
			s.mu.Lock()
			s.ids = append(s.ids, rid)
			s.mu.Unlock()
		}
	case roll < 45: // get own
		if rid, ok := s.pick(r); ok {
			s.record("get", sendFor(t, srv, keyRoot, tenantOf(tid), "GET", mem+"/"+rid))
		}
	case roll < 65: // search, all three modes
		mode := []string{"semantic", "keyword", "hybrid"}[r.IntN(3)]
		s.record("search:"+mode, sendJSONFor(t, srv, tid, "POST", mem+"/search",
			fmt.Sprintf(`{"query":"raft %s","search_type":%q,"limit":5}`, mark, mode)))
	case roll < 75: // list, following one cursor
		rr := sendFor(t, srv, keyRoot, tenantOf(tid), "GET", mem+"?limit=3")
		s.record("list", rr)
		var page struct {
			NextCursor string `json:"next_cursor"`
		}
		if json.Unmarshal(rr.Body.Bytes(), &page) == nil && page.NextCursor != "" {
			s.record("list:next", sendFor(t, srv, keyRoot, tenantOf(tid), "GET",
				mem+"?limit=3&cursor="+page.NextCursor))
		}
	case roll < 85: // update
		if rid, ok := s.pick(r); ok {
			s.record("update", sendJSONFor(t, srv, tid, "PATCH", mem+"/"+rid,
				fmt.Sprintf(`{"importance":0.%d}`, 1+r.IntN(9))))
		}
	case roll < 90: // history
		if rid, ok := s.pick(r); ok {
			s.record("history", sendFor(t, srv, keyRoot, tenantOf(tid), "GET", mem+"/"+rid+"/history"))
		}
	case roll < 95: // connect two of its own, then walk from one
		a, okA := s.pick(r)
		b, okB := s.pick(r)
		if okA && okB && a != b {
			s.record("connect", sendJSONFor(t, srv, tid, "POST", mem+"/"+a+"/connections",
				fmt.Sprintf(`{"target_id":%q,"relationship_type":"supports","strength":0.5}`, b)))
			s.record("related", sendFor(t, srv, keyRoot, tenantOf(tid), "GET", mem+"/"+a+"/related"))
		}
	default: // reach for another tenant's memory
		other := states[(ti+1+r.IntN(isoTenants-1))%isoTenants]
		if rid, ok := other.pick(r); ok {
			rr := sendFor(t, srv, keyRoot, tenantOf(tid), "GET", mem+"/"+rid)
			s.record("foreign:"+rid, rr)
			if rr.Code != http.StatusNotFound {
				t.Errorf("%s fetching another tenant's memory: %d, want 404", tid, rr.Code)
			}
		}
	}
}

func tenantOf(s string) tenant.ID { return tenant.ID(s) }

func sendJSONFor(t *testing.T, srv http.Handler, tid, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+keyRoot)
	r.Header.Set(remhttp.TenantHeader, tid)
	r.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	return rr
}

func decodeID(t *testing.T, rr *httptest.ResponseRecorder) string {
	var m struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil || m.ID == "" {
		t.Errorf("a created memory with no id: %s", rr.Body.String())
	}
	return m.ID
}

var requestsLine = regexp.MustCompile(`^remem_api_requests_total\{([^}]*)\} (\S+)$`)

// sum2xxFor adds every successful request the metrics attribute to tid.
func sum2xxFor(exposition, tid string) int {
	total := 0
	for _, line := range strings.Split(exposition, "\n") {
		m := requestsLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if !strings.Contains(m[1], `tenant="`+tid+`"`) || !strings.Contains(m[1], `status="2xx"`) {
			continue
		}
		v, err := strconv.ParseFloat(m[2], 64)
		if err == nil {
			total += int(v)
		}
	}
	return total
}

func clip(s string) string {
	if len(s) > 400 {
		return s[:400] + "…"
	}
	return s
}
