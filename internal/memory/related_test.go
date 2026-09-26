package memory_test

import (
	"encoding/binary"
	"testing"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/tenant"
)

// storeThree creates three memories and returns their ids.
func storeThree(t *testing.T, svc *memory.Service) (a, b, c id.ID) {
	t.Helper()
	ids := make([]id.ID, 3)
	for i, content := range []string{"the first memory", "the second memory", "the third memory"} {
		m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: content})
		if err != nil {
			t.Fatalf("creating memory %d: %v", i, err)
		}
		ids[i] = m.ID
	}
	return ids[0], ids[1], ids[2]
}

func TestRelateConnectsTwoMemories(t *testing.T) {
	svc := newService(t)
	a, b, _ := storeThree(t, svc)

	conn, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Type: "supports", Strength: 0.8})
	if err != nil {
		t.Fatalf("Relate: %v", err)
	}
	if conn.From != a || conn.To != b || conn.Type != "supports" || conn.Strength != 0.8 {
		t.Fatalf("Relate returned %+v", conn)
	}

	out, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: a})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Connections) != 1 || out.Connections[0].To != b {
		t.Fatalf("Connections(a) = %+v", out)
	}
	in, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: b, Direction: "in"})
	if err != nil {
		t.Fatal(err)
	}
	if len(in.Connections) != 1 || in.Connections[0].From != a {
		t.Fatalf("Connections(b, in) = %+v", in)
	}
}

// An edge is only useful if something is on the other end of it, and the graph
// has no way to notice later that there is not.
func TestRelateToAMissingMemoryIsRefused(t *testing.T) {
	svc := newService(t)
	a, _, _ := storeThree(t, svc)

	_, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: id.New(), Strength: 0.5})
	if !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
	_, err = svc.Relate(acmeCtx(), memory.RelateReq{From: id.New(), To: a, Strength: 0.5})
	if !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

func TestRelateToAnArchivedMemoryIsRefused(t *testing.T) {
	svc := newService(t)
	a, b, _ := storeThree(t, svc)
	if err := svc.Delete(acmeCtx(), b, false); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Strength: 0.5}); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

func TestRelateRefusesAnUnknownRelationshipTypeByName(t *testing.T) {
	svc := newService(t)
	a, b, _ := storeThree(t, svc)

	_, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Type: "caused-by", Strength: 0.5})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
	if !contains(err.Error(), "caused_by") {
		t.Errorf("the refusal does not list the available types: %v", err)
	}
}

// An empty type means the neutral kind, so a caller who only means "these go
// together" need not choose one.
func TestRelateDefaultsToRelatedTo(t *testing.T) {
	svc := newService(t)
	a, b, _ := storeThree(t, svc)

	conn, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Strength: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	if conn.Type != "related_to" {
		t.Fatalf("the default relationship is %q, want related_to", conn.Type)
	}
}

func TestRestrengthenChangesAnExistingRelationship(t *testing.T) {
	svc := newService(t)
	a, b, _ := storeThree(t, svc)
	if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Type: "supports", Strength: 0.2}); err != nil {
		t.Fatal(err)
	}

	conn, err := svc.Restrengthen(acmeCtx(), a, b, "supports", 0.9)
	if err != nil {
		t.Fatal(err)
	}
	if conn.Strength != 0.9 {
		t.Fatalf("strength is %v, want 0.9", conn.Strength)
	}
	if _, err := svc.Restrengthen(acmeCtx(), a, b, "contradicts", 0.9); !errs.Is(err, errs.NotFound) {
		t.Fatalf("restrengthening a relationship that is not there returned %v, want NotFound", err)
	}
}

func TestUnrelateWithoutATypeRemovesEveryRelationshipBetweenThePair(t *testing.T) {
	svc := newService(t)
	a, b, _ := storeThree(t, svc)
	for _, ty := range []string{"supports", "references", "similar_to"} {
		if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Type: ty, Strength: 0.5}); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := svc.Unrelate(acmeCtx(), a, b, "")
	if err != nil {
		t.Fatal(err)
	}
	if removed != 3 {
		t.Fatalf("removed %d, want 3", removed)
	}
	out, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: a})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Connections) != 0 {
		t.Fatalf("connections survived: %+v", out)
	}
}

func TestUnrelateWithATypeRemovesOnlyThatOne(t *testing.T) {
	svc := newService(t)
	a, b, _ := storeThree(t, svc)
	for _, ty := range []string{"supports", "references"} {
		if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Type: ty, Strength: 0.5}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := svc.Unrelate(acmeCtx(), a, b, "supports"); err != nil {
		t.Fatal(err)
	}
	out, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: a})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Connections) != 1 || out.Connections[0].Type != "references" {
		t.Fatalf("Unrelate took the wrong relationship: %+v", out)
	}
}

// The REM-83 regression at the service level: a 0.9 chain at depth 2 outranks a
// 0.1 edge at depth 1.
func TestRelatedRanksByPathStrengthNotByDepth(t *testing.T) {
	svc := newService(t)
	anchor, hub, weak := storeThree(t, svc)
	strong, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "the strongly connected memory"})
	if err != nil {
		t.Fatal(err)
	}

	relate := func(from, to id.ID, strength float32) {
		t.Helper()
		if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: from, To: to, Strength: strength}); err != nil {
			t.Fatal(err)
		}
	}
	relate(anchor, weak, 0.1)
	relate(anchor, hub, 0.9)
	relate(hub, strong.ID, 0.9)

	res, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Depth: 2, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 3 {
		t.Fatalf("got %d related memories, want three: %+v", len(res.Results), res.Results)
	}
	if res.Results[len(res.Results)-1].Memory.ID != weak {
		t.Fatalf("the 0.1 edge at depth 1 did not sort last: %+v", res.Results)
	}
	for i := 1; i < len(res.Results); i++ {
		if res.Results[i-1].Score < res.Results[i].Score {
			t.Fatalf("results are not strongest-first: %+v", res.Results)
		}
	}
	if res.Truncated {
		t.Error("a traversal that exhausted the neighbourhood reported truncation")
	}
}

// The default depth is one hop, matching Rust's find_related.
func TestRelatedDefaultsToOneHop(t *testing.T) {
	svc := newService(t)
	a, b, c := storeThree(t, svc)
	if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Strength: 0.9}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: b, To: c, Strength: 0.9}); err != nil {
		t.Fatal(err)
	}

	res, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: a, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 1 || res.Results[0].Memory.ID != b {
		t.Fatalf("the default traversal returned %+v, want the one direct neighbour", res.Results)
	}
}

// "No related memories" and "there is no such memory" are different answers, and
// a caller that cannot tell them apart reads a typo as an empty neighbourhood.
func TestRelatedFromAMissingMemoryIsNotFound(t *testing.T) {
	svc := newService(t)
	if _, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: id.New(), Limit: 10}); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

func TestRelatedExcludesArchivedMemoriesByDefault(t *testing.T) {
	svc := newService(t)
	a, b, c := storeThree(t, svc)
	if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Strength: 0.9}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: c, Strength: 0.8}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(acmeCtx(), b, false); err != nil {
		t.Fatal(err)
	}

	res, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: a, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 1 || res.Results[0].Memory.ID != c {
		t.Fatalf("an archived memory was returned as related: %+v", res.Results)
	}

	res, err = svc.Related(acmeCtx(), memory.RelatedReq{ID: a, Limit: 10, IncludeArchived: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("include_archived returned %d memories, want both", len(res.Results))
	}
}

// Traversal would otherwise spend its node budget reaching a record that no
// longer exists, so related-memory queries would silently return fewer than
// they were asked for.
func TestHardDeleteTakesTheMemorysRelationshipsWithIt(t *testing.T) {
	svc := newService(t)
	a, b, c := storeThree(t, svc)
	if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: a, To: b, Strength: 0.9}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: c, To: b, Strength: 0.9}); err != nil {
		t.Fatal(err)
	}

	if err := svc.Delete(acmeCtx(), b, true); err != nil {
		t.Fatal(err)
	}
	for _, rid := range []id.ID{a, c} {
		out, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: rid, Direction: "both"})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Connections) != 0 {
			t.Fatalf("%s still holds a relationship to the hard-deleted memory: %+v", rid, out)
		}
	}
}

func TestConnectionsRefusesAnUnknownDirectionByName(t *testing.T) {
	svc := newService(t)
	a, _, _ := storeThree(t, svc)
	_, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: a, Direction: "sideways"})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

// Removing cursor handling (or resuming at the range lower bound) makes this
// return duplicates or lose edges after the first page.
func TestConnectionsPageThroughEveryEdge(t *testing.T) {
	svc := newService(t)
	anchor := connectedCluster(t, svc, 23)

	var seen []string
	var cursor string
	for {
		res, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: anchor, Limit: 5, Cursor: cursor})
		if err != nil {
			t.Fatalf("listing connections: %v", err)
		}
		for _, c := range res.Connections {
			seen = append(seen, c.Type+"->"+c.To.String())
		}
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if len(seen) != 23 {
		t.Fatalf("paging saw %d edges, want 23", len(seen))
	}
	if dup := firstDuplicate(seen); dup != "" {
		t.Fatalf("edge %s came back on two pages", dup)
	}
}

// Two key spaces, no ordering across them, so the walk is defined rather than
// merged: every out-edge, then every in-edge. A caller paging direction=both
// must see that order rather than an arbitrary interleaving.
func TestConnectionsBothDirectionsPageOutEdgesFirst(t *testing.T) {
	svc := newService(t)
	anchor := anchorWithOutAndInEdges(t, svc, 3, 3)

	var directions []string
	var cursor string
	for {
		res, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{
			ID: anchor, Direction: "both", Limit: 2, Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("listing: %v", err)
		}
		for _, c := range res.Connections {
			directions = append(directions, c.Direction)
		}
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if len(directions) != 6 {
		t.Fatalf("saw %d edges, want 6", len(directions))
	}
	for i, d := range directions {
		want := "out"
		if i >= 3 {
			want = "in"
		}
		if d != want {
			t.Fatalf("edge %d is %s, want %s: out-edges come before in-edges", i, d, want)
		}
	}
}

// If the out range ends exactly on the page boundary, an in edge still makes
// the page resumable; otherwise that final range is silently hidden.
func TestConnectionsBothDirectionsContinuesIntoInRangeAtPageBoundary(t *testing.T) {
	svc := newService(t)
	anchor := anchorWithOutAndInEdges(t, svc, 2, 1)

	first, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: anchor, Direction: "both", Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if !first.HasMore || first.NextCursor == "" {
		t.Fatalf("full out page hid a remaining in edge: %+v", first)
	}
	second, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: anchor, Direction: "both", Limit: 2, Cursor: first.NextCursor})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second.Connections) != 1 || second.Connections[0].Direction != "in" || second.HasMore {
		t.Fatalf("second page = %+v, want the sole in edge", second)
	}
}

// A token can only name a durable graph position when its relationship code is
// one this binary understands. Accepting an unknown code lets a crafted token
// seek past real edges and manufacture a successful empty page.
func TestConnectionsRefusesCursorWithUnknownRelationshipType(t *testing.T) {
	svc := newService(t)
	anchor, _, _ := storeThree(t, svc)

	payload := make([]byte, 19)
	payload[0] = 0 // out-edge range
	binary.BigEndian.PutUint16(payload[1:3], 99)
	position := id.New()
	copy(payload[3:], position[:])
	cursor := codec.EncodeToken(codec.TokenEdges, tenant.ID("acme"), payload)

	_, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: anchor, Cursor: cursor})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("Connections with an unknown cursor relationship = %v, want Invalid", err)
	}
}

func connectedCluster(t *testing.T, svc *memory.Service, n int) id.ID {
	t.Helper()
	ids := seed(t, svc, makeContents(n+1)...)
	for _, target := range ids[1:] {
		if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: ids[0], To: target, Strength: 0.5}); err != nil {
			t.Fatalf("relating cluster: %v", err)
		}
	}
	return ids[0]
}

func anchorWithOutAndInEdges(t *testing.T, svc *memory.Service, out, in int) id.ID {
	t.Helper()
	ids := seed(t, svc, makeContents(1+out+in)...)
	anchor := ids[0]
	for _, target := range ids[1 : 1+out] {
		if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: anchor, To: target, Strength: 0.5}); err != nil {
			t.Fatalf("adding out edge: %v", err)
		}
	}
	for _, source := range ids[1+out:] {
		if _, err := svc.Relate(acmeCtx(), memory.RelateReq{From: source, To: anchor, Strength: 0.5}); err != nil {
			t.Fatalf("adding in edge: %v", err)
		}
	}
	return anchor
}

func makeContents(n int) []string {
	contents := make([]string, n)
	for i := range contents {
		contents[i] = "connection paging memory " + id.New().String()
	}
	return contents
}
