package http_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/id"
)

func connectionsPath(rid string) string {
	return remhttp.APIPrefix + "/memories/" + rid + "/connections"
}

// connect issues a connect and fails the test unless it was created.
func connect(t *testing.T, srv http.Handler, key, from, to, typ string, strength float32) {
	t.Helper()
	body := fmt.Sprintf(`{"target_id":%q,"relationship_type":%q,"strength":%v}`, to, typ, strength)
	rr := send(t, srv, key, "POST", connectionsPath(from), strings.NewReader(body))
	if rr.Code != http.StatusCreated {
		t.Fatalf("connect returned %d: %s", rr.Code, rr.Body.String())
	}
}

func TestCreateAndListConnections(t *testing.T) {
	srv := newServer(t)
	a := createAs(t, srv, keyAcme, "the deployment runbook")
	b := createAs(t, srv, keyAcme, "the incident postmortem")
	connect(t, srv, keyAcme, a, b, "supports", 0.8)

	rr := send(t, srv, keyAcme, "GET", connectionsPath(a), nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list returned %d: %s", rr.Code, rr.Body.String())
	}
	conns := jsonBody(t, rr)["connections"].([]any)
	if len(conns) != 1 {
		t.Fatalf("got %d connections, want one: %s", len(conns), rr.Body.String())
	}
	c := conns[0].(map[string]any)
	if c["from"] != a || c["to"] != b || c["relationship_type"] != "supports" {
		t.Fatalf("the connection came back as %v", c)
	}
	if c["strength"].(float64) < 0.79 || c["strength"].(float64) > 0.81 {
		t.Fatalf("strength is %v, want 0.8", c["strength"])
	}

	// The in direction, from the other end. Both endpoints are named, so a
	// client can tell which way the relationship points.
	rr = send(t, srv, keyAcme, "GET", connectionsPath(b)+"?direction=in", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list in returned %d: %s", rr.Code, rr.Body.String())
	}
	conns = jsonBody(t, rr)["connections"].([]any)
	if len(conns) != 1 || conns[0].(map[string]any)["from"] != a {
		t.Fatalf("the incoming connection came back as %v", conns)
	}
}

func TestConnectingAMemoryToItselfIsRefused(t *testing.T) {
	srv := newServer(t)
	a := createAs(t, srv, keyAcme, "the only memory")

	// 422, not 400: the request is well formed and its content is refused,
	// which is the split this surface already draws.
	rr := send(t, srv, keyAcme, "POST", connectionsPath(a),
		strings.NewReader(fmt.Sprintf(`{"target_id":%q,"strength":1}`, a)))
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("got %d, want 422: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "itself") {
		t.Errorf("the refusal does not say why: %s", rr.Body.String())
	}
}

func TestConnectingToAMemoryInAnotherTenantIsNotFound(t *testing.T) {
	srv := newServer(t)
	mine := createAs(t, srv, keyAcme, "my memory")
	theirs := createAs(t, srv, keyOther, "their memory")

	rr := send(t, srv, keyAcme, "POST", connectionsPath(mine),
		strings.NewReader(fmt.Sprintf(`{"target_id":%q,"strength":0.5}`, theirs)))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404: %s", rr.Code, rr.Body.String())
	}
}

func TestAnUnknownRelationshipTypeIsRefusedByName(t *testing.T) {
	srv := newServer(t)
	a := createAs(t, srv, keyAcme, "one")
	b := createAs(t, srv, keyAcme, "two")

	rr := send(t, srv, keyAcme, "POST", connectionsPath(a),
		strings.NewReader(fmt.Sprintf(`{"target_id":%q,"relationship_type":"caused-by","strength":0.5}`, b)))
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("got %d, want 422: %s", rr.Code, rr.Body.String())
	}
	for _, name := range []string{"caused_by", "similar_to", "derived_from"} {
		if !strings.Contains(rr.Body.String(), name) {
			t.Errorf("the refusal does not list %q: %s", name, rr.Body.String())
		}
	}
}

func TestUpdateConnectionNeedsAType(t *testing.T) {
	srv := newServer(t)
	a := createAs(t, srv, keyAcme, "one")
	b := createAs(t, srv, keyAcme, "two")
	connect(t, srv, keyAcme, a, b, "supports", 0.2)

	rr := send(t, srv, keyAcme, "PATCH", connectionsPath(a)+"/"+b, strings.NewReader(`{"strength":0.9}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("an update without ?type returned %d, want 400: %s", rr.Code, rr.Body.String())
	}

	rr = send(t, srv, keyAcme, "PATCH", connectionsPath(a)+"/"+b+"?type=supports",
		strings.NewReader(`{"strength":0.9}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("update returned %d: %s", rr.Code, rr.Body.String())
	}
	if s := jsonBody(t, rr)["strength"].(float64); s < 0.89 || s > 0.91 {
		t.Fatalf("strength is %v, want 0.9", s)
	}
}

func TestDeleteConnectionWithAndWithoutAType(t *testing.T) {
	srv := newServer(t)
	a := createAs(t, srv, keyAcme, "one")
	b := createAs(t, srv, keyAcme, "two")
	connect(t, srv, keyAcme, a, b, "supports", 0.5)
	connect(t, srv, keyAcme, a, b, "references", 0.5)

	rr := send(t, srv, keyAcme, "DELETE", connectionsPath(a)+"/"+b+"?type=supports", nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("delete returned %d: %s", rr.Code, rr.Body.String())
	}
	rr = send(t, srv, keyAcme, "GET", connectionsPath(a), nil)
	if conns := jsonBody(t, rr)["connections"].([]any); len(conns) != 1 {
		t.Fatalf("got %d connections after deleting one type, want one", len(conns))
	}

	// Without a type: everything between the pair.
	rr = send(t, srv, keyAcme, "DELETE", connectionsPath(a)+"/"+b, nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("delete returned %d: %s", rr.Code, rr.Body.String())
	}
	rr = send(t, srv, keyAcme, "GET", connectionsPath(a), nil)
	if conns := jsonBody(t, rr)["connections"].([]any); len(conns) != 0 {
		t.Fatalf("%d connections survived", len(conns))
	}
}

// The REM-83 regression over HTTP: a 0.9 chain two hops out outranks a 0.1
// relationship one hop out.
func TestRelatedIsRankedByConnectionStrength(t *testing.T) {
	srv := newServer(t)
	anchor := createAs(t, srv, keyAcme, "the anchor")
	hub := createAs(t, srv, keyAcme, "the hub")
	strong := createAs(t, srv, keyAcme, "strongly connected, two hops out")
	weak := createAs(t, srv, keyAcme, "weakly connected, one hop out")

	connect(t, srv, keyAcme, anchor, weak, "related_to", 0.1)
	connect(t, srv, keyAcme, anchor, hub, "related_to", 0.9)
	connect(t, srv, keyAcme, hub, strong, "related_to", 0.9)

	rr := send(t, srv, keyAcme, "GET",
		remhttp.APIPrefix+"/memories/"+anchor+"/related?depth=2&limit=10", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("related returned %d: %s", rr.Code, rr.Body.String())
	}
	body := jsonBody(t, rr)
	results := body["results"].([]any)
	if len(results) != 3 {
		t.Fatalf("got %d results, want three: %s", len(results), rr.Body.String())
	}
	last := results[len(results)-1].(map[string]any)["memory"].(map[string]any)
	if last["id"] != weak {
		t.Fatalf("the 0.1 one-hop memory did not sort last: %s", rr.Body.String())
	}
	for i := 1; i < len(results); i++ {
		prev := results[i-1].(map[string]any)["score"].(float64)
		cur := results[i].(map[string]any)["score"].(float64)
		if prev < cur {
			t.Fatalf("results are not strongest-first: %s", rr.Body.String())
		}
	}
	if body["truncated"].(bool) {
		t.Error("a traversal that exhausted the neighbourhood reported truncation")
	}
}

func TestRelatedDepthAboveTheCapIsRefused(t *testing.T) {
	srv := newServer(t)
	a := createAs(t, srv, keyAcme, "the anchor")

	rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories/"+a+"/related?depth=99", nil)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("got %d, want 422: %s", rr.Code, rr.Body.String())
	}
}

func TestRelatedForAMemoryInAnotherTenantIsNotFound(t *testing.T) {
	srv := newServer(t)
	theirs := createAs(t, srv, keyOther, "their memory")

	rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories/"+theirs+"/related", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404: %s", rr.Code, rr.Body.String())
	}
}

func TestConnectionRoutesNeedACredential(t *testing.T) {
	srv := newServer(t)
	rid := id.New().String()
	cases := []struct{ method, path string }{
		{"POST", connectionsPath(rid)},
		{"GET", connectionsPath(rid)},
		{"PATCH", connectionsPath(rid) + "/" + id.New().String()},
		{"DELETE", connectionsPath(rid) + "/" + id.New().String()},
		{"GET", remhttp.APIPrefix + "/memories/" + rid + "/related"},
	}
	for _, c := range cases {
		rr := send(t, srv, "", c.method, c.path, strings.NewReader(`{}`))
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("%s %s returned %d without a credential, want 401", c.method, c.path, rr.Code)
		}
	}
}

func TestAMalformedTargetIdIsRefused(t *testing.T) {
	srv := newServer(t)
	a := createAs(t, srv, keyAcme, "one")

	rr := send(t, srv, keyAcme, "POST", connectionsPath(a), strings.NewReader(`{"target_id":"nonsense"}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", rr.Code, rr.Body.String())
	}
	rr = send(t, srv, keyAcme, "DELETE", connectionsPath(a)+"/nonsense", nil)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", rr.Code, rr.Body.String())
	}
}

func TestAMalformedNumericQueryIsRefused(t *testing.T) {
	srv := newServer(t)
	a := createAs(t, srv, keyAcme, "one")

	for _, q := range []string{"?min_strength=lots", "?limit=lots"} {
		rr := send(t, srv, keyAcme, "GET", connectionsPath(a)+q, nil)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("%s returned %d, want 400: %s", q, rr.Code, rr.Body.String())
		}
	}
}

// Found by the Phase 6 end-to-end verification. Without has_more, a client that
// asked for five of thirty-three related memories got five, `truncated: false`
// and no way to tell that apart from a memory with exactly five neighbours.
func TestRelatedReportsThatMoreMemoriesWereReached(t *testing.T) {
	srv := newServer(t)
	anchor := createAs(t, srv, keyAcme, "the anchor")
	for i := 0; i < 12; i++ {
		other := createAs(t, srv, keyAcme, fmt.Sprintf("neighbour number %d", i))
		connect(t, srv, keyAcme, anchor, other, "similar_to", 0.5)
	}

	rr := send(t, srv, keyAcme, "GET",
		remhttp.APIPrefix+"/memories/"+anchor+"/related?limit=5", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("related returned %d: %s", rr.Code, rr.Body.String())
	}
	body := jsonBody(t, rr)
	if len(body["results"].([]any)) != 5 {
		t.Fatalf("got %d results, want the five asked for", len(body["results"].([]any)))
	}
	if !body["has_more"].(bool) {
		t.Fatal("twelve neighbours were reached, five were returned, and has_more was false")
	}
	if body["truncated"].(bool) {
		t.Error("the node budget was not spent, so truncated must be false: it is a different fact from has_more")
	}

	// Asking for all of them says so.
	rr = send(t, srv, keyAcme, "GET",
		remhttp.APIPrefix+"/memories/"+anchor+"/related?limit=50", nil)
	body = jsonBody(t, rr)
	if len(body["results"].([]any)) != 12 {
		t.Fatalf("got %d results, want all twelve", len(body["results"].([]any)))
	}
	if body["has_more"].(bool) {
		t.Fatal("every neighbour was returned and has_more was still true")
	}
}

// Also found by the end-to-end verification: listing the connections of a
// memory that does not exist returned an empty list, where /related returned
// 404 for the same id. A client cannot tell a typo from a memory with no
// connections.
func TestListingConnectionsOfAMissingMemoryIsNotFound(t *testing.T) {
	srv := newServer(t)
	rr := send(t, srv, keyAcme, "GET", connectionsPath(id.New().String()), nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404: %s", rr.Code, rr.Body.String())
	}

	// And a memory in another tenant is absent, not forbidden: "you may not see
	// this" confirms it exists.
	theirs := createAs(t, srv, keyOther, "their memory")
	rr = send(t, srv, keyAcme, "GET", connectionsPath(theirs), nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant read returned %d, want 404: %s", rr.Code, rr.Body.String())
	}
}
