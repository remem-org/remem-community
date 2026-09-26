package http_test

import (
	"net/http"
	"net/url"
	"testing"
)

// Missing cursor forwarding or response fields hides or repeats neighbours.
func TestRelatedHTTPResumesIntoThePinnedRanking(t *testing.T) {
	srv := newServer(t)
	anchor := createAs(t, srv, keyAcme, "anchor")
	strong := createAs(t, srv, keyAcme, "strong")
	weak := createAs(t, srv, keyAcme, "weak")
	connect(t, srv, keyAcme, anchor, strong, "supports", 0.9)
	connect(t, srv, keyAcme, anchor, weak, "supports", 0.5)
	path := "/api/v1/memories/" + anchor + "/related?limit=1"
	rr := send(t, srv, keyAcme, "GET", path, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("first page: %d %s", rr.Code, rr.Body.String())
	}
	first := jsonBody(t, rr)
	cursor, ok := first["next_cursor"].(string)
	if !ok || cursor == "" || first["has_more"] != true {
		t.Fatalf("first page omitted continuation: %v", first)
	}
	rr = send(t, srv, keyAcme, "GET", path+"&cursor="+url.QueryEscape(cursor), nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("next page: %d %s", rr.Code, rr.Body.String())
	}
	last := jsonBody(t, rr)
	hits := last["results"].([]any)
	if len(hits) != 1 || hits[0].(map[string]any)["memory"].(map[string]any)["id"] != weak || last["has_more"] != false || last["truncated"] != false {
		t.Fatalf("last page = %v", last)
	}
	if _, ok := last["next_cursor"]; ok {
		t.Fatal("exhausted page included next_cursor")
	}
}
