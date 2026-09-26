package http_test

import (
	"fmt"
	"net/http"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
)

// The listing surface is the one thing in this phase a user can see, so it is
// worth checking the things a client actually does with it: page, sort, and
// present a token it was not given.

func TestListPagesWithAnOpaqueCursor(t *testing.T) {
	srv := newServer(t)
	want := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		want = append(want, createAs(t, srv, keyAcme, fmt.Sprintf("memory %02d", i)))
	}

	seen := make([]string, 0, len(want))
	cursor := ""
	for page := 0; page < 10; page++ {
		path := remhttp.APIPrefix + "/memories?order_by=created_at&desc=false&limit=5"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		rr := send(t, srv, keyAcme, "GET", path, nil)
		if rr.Code != http.StatusOK {
			t.Fatalf("page %d returned %d: %s", page, rr.Code, rr.Body.String())
		}
		body := jsonBody(t, rr)

		for _, raw := range body["memories"].([]any) {
			seen = append(seen, raw.(map[string]any)["id"].(string))
		}
		// Always present, even when false — a flag that appears only when set
		// is a flag clients forget to check.
		if _, ok := body["has_more"]; !ok {
			t.Fatal("no has_more in the response")
		}
		if _, ok := body["truncated"]; !ok {
			t.Fatal("no truncated in the response")
		}
		if !body["has_more"].(bool) {
			break
		}
		next, ok := body["next_cursor"].(string)
		if !ok || next == "" {
			t.Fatalf("page %d says there is more and returned no cursor", page)
		}
		cursor = next
	}

	if len(seen) != len(want) {
		t.Fatalf("paging returned %d memories, want %d", len(seen), len(want))
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("position %d is %s, want %s — the ascending listing must be creation order",
				i, seen[i], want[i])
		}
	}
}

func TestListDefaultsToNewestFirst(t *testing.T) {
	srv := newServer(t)
	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, createAs(t, srv, keyAcme, fmt.Sprintf("memory %d", i)))
	}

	rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rr.Code, rr.Body.String())
	}
	got := jsonBody(t, rr)["memories"].([]any)
	if len(got) != 4 {
		t.Fatalf("returned %d memories, want 4", len(got))
	}
	if first := got[0].(map[string]any)["id"].(string); first != ids[3] {
		t.Errorf("the default listing led with %s, want the newest %s: 'my memories' means "+
			"newest first to everyone who has ever asked for it", first, ids[3])
	}
}

func TestListRefusesAnOrderingItCannotAnswer(t *testing.T) {
	srv := newServer(t)
	createAs(t, srv, keyAcme, "a memory")

	rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories?order_by=content", nil)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("content type %q", ct)
	}
	if detail, _ := jsonBody(t, rr)["detail"].(string); detail == "" {
		t.Error("the refusal carries no detail naming the orderings that do work")
	}
}

func TestAMalformedLimitIsRefusedRatherThanIgnored(t *testing.T) {
	srv := newServer(t)
	rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories?limit=lots", nil)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d: a limit that silently became the default is how a client ends up "+
			"paging ten at a time while its author is certain it asked for five hundred", rr.Code)
	}
}

// A cursor is bound to the tenant that minted it. Presenting one across the
// boundary must resolve to nothing — not to the other tenant's rows, and not to
// this tenant's rows at the other's position.
func TestACursorDoesNotCrossTheTenantBoundary(t *testing.T) {
	srv := newServer(t)
	for i := 0; i < 4; i++ {
		createAs(t, srv, keyAcme, fmt.Sprintf("acme memory %d", i))
	}
	createAs(t, srv, keyOther, "other memory")

	rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories?limit=2", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rr.Code, rr.Body.String())
	}
	cursor, _ := jsonBody(t, rr)["next_cursor"].(string)
	if cursor == "" {
		t.Fatal("the first page returned no cursor")
	}

	stolen := send(t, srv, keyOther, "GET", remhttp.APIPrefix+"/memories?limit=2&cursor="+cursor, nil)
	if stolen.Code != http.StatusUnprocessableEntity {
		t.Fatalf("another tenant resumed acme's listing and got %d: %s",
			stolen.Code, stolen.Body.String())
	}
}

func TestListExcludesArchivedUnlessAskedOverHTTP(t *testing.T) {
	srv := newServer(t)
	first := createAs(t, srv, keyAcme, "kept")
	archived := createAs(t, srv, keyAcme, "retired")

	if rr := send(t, srv, keyAcme, "DELETE", remhttp.APIPrefix+"/memories/"+archived, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("archiving returned %d", rr.Code)
	}

	rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories", nil)
	got := jsonBody(t, rr)["memories"].([]any)
	if len(got) != 1 || got[0].(map[string]any)["id"].(string) != first {
		t.Errorf("the default listing returned %d memories; an archived memory is retired from "+
			"retrieval and a listing is retrieval", len(got))
	}

	rr = send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories?include_archived", nil)
	if got := jsonBody(t, rr)["memories"].([]any); len(got) != 2 {
		t.Errorf("the widened listing returned %d memories, want 2: archiving is not deletion", len(got))
	}
}
