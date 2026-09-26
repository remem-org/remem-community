package graph_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// dump returns every row of one space, as ordered key/value pairs.
func dump(t *testing.T, kv storage.KV, sc graph.Scope, space keys.Space) [][2][]byte {
	t.Helper()
	lower, upper := keys.SpaceRange(sc.Tenant, tenant.DefaultNamespace, space)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	var out [][2][]byte
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, [2][]byte{
			append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...)})
	}
	if err := it.Error(); err != nil {
		t.Fatalf("scanning %s: %v", space, err)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][0], out[j][0]) < 0 })
	return out
}

func deleteRange(t *testing.T, kv storage.KV, sc graph.Scope, space keys.Space) {
	t.Helper()
	for _, row := range dump(t, kv, sc, space) {
		if err := kv.Delete(context.Background(), row[0]); err != nil {
			t.Fatalf("deleting: %v", err)
		}
	}
}

func populate(t *testing.T, s *graph.Store, kv storage.KV, n int) []id.ID {
	t.Helper()
	nodes := make([]id.ID, n)
	for i := range nodes {
		nodes[i] = id.New()
	}
	types := []graph.RelationshipType{graph.RelatedTo, graph.Supports, graph.SimilarTo}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			e := edge(nodes[i], nodes[j], types[(i+j)%len(types)], float32(i+j)/float32(2*n))
			// Metadata is what makes the byte comparison meaningful: a map is
			// the part of the body a re-encoding would be most likely to
			// reorder or drop.
			e.Meta = map[string]string{"i": string(rune('a' + i)), "j": string(rune('a' + j)), "run": "1"}
			add(t, s, kv, e)
		}
	}
	return nodes
}

// The completion criterion: delete the whole in-edge key space, rebuild it from
// the canonical out-edges, and the result is byte-identical.
//
// Byte-identical rather than merely equivalent, because the derived row is a
// copy of the canonical value and a rebuild that decoded and re-encoded it would
// silently drop any protobuf field a newer binary had written. That loss is
// invisible to every query: the edge still decodes, still ranks, still answers.
func TestRebuildReproducesTheInEdgeIndex(t *testing.T) {
	s, kv := newStore(t)
	populate(t, s, kv, 6)

	before := dump(t, kv, scope(), keys.SpaceEdgeIn)
	if len(before) != 30 {
		t.Fatalf("the fixture has %d in-edges, want 30", len(before))
	}
	deleteRange(t, kv, scope(), keys.SpaceEdgeIn)
	if got := dump(t, kv, scope(), keys.SpaceEdgeIn); len(got) != 0 {
		t.Fatalf("%d in-edges survived the deletion", len(got))
	}

	n, err := graph.RebuildIn(context.Background(), kv, scope())
	if err != nil {
		t.Fatalf("rebuilding: %v", err)
	}
	if n != len(before) {
		t.Fatalf("the rebuild wrote %d entries, want %d", n, len(before))
	}

	after := dump(t, kv, scope(), keys.SpaceEdgeIn)
	if len(after) != len(before) {
		t.Fatalf("the rebuilt index holds %d entries, want %d", len(after), len(before))
	}
	for i := range before {
		if !bytes.Equal(before[i][0], after[i][0]) {
			t.Fatalf("entry %d has key %x, want %x", i, after[i][0], before[i][0])
		}
		if !bytes.Equal(before[i][1], after[i][1]) {
			t.Fatalf("entry %d has value %x, want %x", i, after[i][1], before[i][1])
		}
	}
}

// A derived index is rebuilt, not repaired: whatever was in the space before is
// gone, including rows nothing derives.
func TestRebuildRemovesOrphanInEdges(t *testing.T) {
	s, kv := newStore(t)
	a, b, ghost := id.New(), id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.Supports, 0.5))

	// A reverse entry with no canonical edge behind it, exactly as a crash
	// between the two key spaces would leave.
	orphan := graph.InKey(scope(), ghost, graph.RelatedTo, b)
	value, err := graph.EncodeBody(edge(ghost, b, graph.RelatedTo, 0.5))
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(context.Background(), orphan, value); err != nil {
		t.Fatal(err)
	}

	if _, err := graph.RebuildIn(context.Background(), kv, scope()); err != nil {
		t.Fatal(err)
	}
	rows := dump(t, kv, scope(), keys.SpaceEdgeIn)
	if len(rows) != 1 {
		t.Fatalf("the rebuilt index holds %d entries, want only the one real edge", len(rows))
	}
	if !bytes.Equal(rows[0][0], graph.InKey(scope(), a, graph.Supports, b)) {
		t.Fatalf("the surviving entry is not the real edge's: %x", rows[0][0])
	}
}

func TestRebuildLeavesTheCanonicalOutEdgesUntouched(t *testing.T) {
	s, kv := newStore(t)
	populate(t, s, kv, 4)
	before := dump(t, kv, scope(), keys.SpaceEdgeOut)

	if _, err := graph.RebuildIn(context.Background(), kv, scope()); err != nil {
		t.Fatal(err)
	}
	after := dump(t, kv, scope(), keys.SpaceEdgeOut)
	if len(after) != len(before) {
		t.Fatalf("the rebuild changed the canonical space: %d rows, want %d", len(after), len(before))
	}
	for i := range before {
		if !bytes.Equal(before[i][0], after[i][0]) || !bytes.Equal(before[i][1], after[i][1]) {
			t.Fatalf("canonical row %d changed", i)
		}
	}
}

func TestRebuildIsTenantScoped(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.Supports, 0.5))

	other := graph.Scope{Tenant: "globex", Namespace: tenant.DefaultNamespace}
	n, err := graph.RebuildIn(context.Background(), kv, other)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rebuilding globex wrote %d entries from acme's edges", n)
	}
	if rows := dump(t, kv, scope(), keys.SpaceEdgeIn); len(rows) != 1 {
		t.Fatalf("rebuilding another tenant disturbed acme's index: %d rows", len(rows))
	}
}

func TestRebuildRefusesAnUnscopedRequest(t *testing.T) {
	_, kv := newStore(t)
	if _, err := graph.RebuildIn(context.Background(), kv, graph.Scope{}); err == nil {
		t.Fatal("an unscoped rebuild was accepted (Invariant 1)")
	}
}

// The reason the rebuild copies rather than decodes: a field written by a newer
// binary must survive being relayed by an older one.
//
// A byte comparison of two edges this binary wrote cannot see this — a
// deterministic re-encoding reproduces every field it knows about. Only a value
// carrying something it does not know about can.
func TestRebuildKeepsFieldsThisBinaryDoesNotUnderstand(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.Supports, 0.5))

	// What a newer binary writing field 99 leaves in the canonical space.
	outKey := graph.OutKey(scope(), a, graph.Supports, b)
	stored, err := kv.Get(context.Background(), outKey)
	if err != nil {
		t.Fatal(err)
	}
	fromTheFuture := appendUnknownField(stored, 99, []byte("written by a later Remem"))
	if err := kv.Set(context.Background(), outKey, fromTheFuture); err != nil {
		t.Fatal(err)
	}

	if _, err := graph.RebuildIn(context.Background(), kv, scope()); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := kv.Get(context.Background(), graph.InKey(scope(), a, graph.Supports, b))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rebuilt, fromTheFuture) {
		t.Fatalf("the rebuilt reverse entry is not the canonical value:\n got %x\nwant %x",
			rebuilt, fromTheFuture)
	}
	if !bytes.Contains(rebuilt, []byte("written by a later Remem")) {
		t.Fatal("the rebuild destroyed a field written by a newer binary; it decoded instead of copying")
	}
}

// appendUnknownField appends a length-delimited protobuf field to a framed
// value's body, which is what a newer binary writing a field this one has never
// seen produces on the wire.
func appendUnknownField(value []byte, field int, payload []byte) []byte {
	out := append([]byte(nil), value...)
	out = binary.AppendUvarint(out, uint64(field)<<3|2) // wire type 2
	out = binary.AppendUvarint(out, uint64(len(payload)))
	return append(out, payload...)
}
