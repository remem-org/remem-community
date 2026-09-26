package graph_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"pgregory.net/rapid"
)

// triple is an edge's identity in the model the property test keeps alongside
// the store.
type triple struct {
	from, to id.ID
	typ      graph.RelationshipType
}

// The completion criterion of this phase, as a property: out-edges and in-edges
// never disagree after any sequence of add, update and remove.
//
// It is a property test rather than a table because the failure it looks for is
// a missing *withdrawal*. Any single operation is easy to get right; what goes
// wrong is an update or a delete that touches one key space and not the other,
// and that shows up only as a reverse entry outliving the edge it described —
// which no ordinary read of the forward direction would ever notice.
func TestBothEdgeSpacesAgreeUnderAnyOperationSequence(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		kv := memkv.New()
		defer func() { _ = kv.Close() }()
		s := graph.NewStore(kv)
		ctx := context.Background()
		sc := scope()

		// A small fixed set of records, so operations collide: a corpus of
		// unique ids would never exercise an update or a repeated delete.
		nodes := make([]id.ID, 4)
		for i := range nodes {
			nodes[i] = id.New()
		}
		types := []graph.RelationshipType{graph.RelatedTo, graph.SimilarTo, graph.Supports}

		want := map[triple]bool{}

		n := rapid.IntRange(1, 30).Draw(rt, "operations")
		for i := 0; i < n; i++ {
			f := rapid.IntRange(0, len(nodes)-1).Draw(rt, "from")
			to := rapid.IntRange(0, len(nodes)-1).Draw(rt, "to")
			ty := types[rapid.IntRange(0, len(types)-1).Draw(rt, "type")]
			strength := rapid.Float32Range(0, 1).Draw(rt, "strength")
			del := rapid.Bool().Draw(rt, "delete")
			if f == to {
				continue // a self-edge is refused, and store_test.go pins that
			}
			key := triple{from: nodes[f], to: nodes[to], typ: ty}

			tx := txn.New(kv, txn.Sync(false))
			var err error
			if del {
				err = s.StageDelete(ctx, tx, sc, key.from, key.to, key.typ)
				delete(want, key)
			} else {
				err = s.Stage(ctx, tx, sc, edge(key.from, key.to, key.typ, strength))
				want[key] = true
			}
			if err != nil {
				tx.Close()
				rt.Fatalf("staging operation %d: %v", i, err)
			}
			if err := tx.Commit(ctx); err != nil {
				tx.Close()
				rt.Fatalf("committing operation %d: %v", i, err)
			}
			tx.Close()
		}

		outRows := dumpSpace(rt, kv, sc, keys.SpaceEdgeOut)
		inRows := dumpSpace(rt, kv, sc, keys.SpaceEdgeIn)

		if len(outRows) != len(want) {
			rt.Fatalf("the out-edge space holds %d rows, the model says %d", len(outRows), len(want))
		}
		if len(inRows) != len(want) {
			rt.Fatalf("the in-edge space holds %d rows, the model says %d: "+
				"a reverse entry outlived its edge or was never written", len(inRows), len(want))
		}

		for key := range want {
			out, ok := outRows[string(graph.OutKey(sc, key.from, key.typ, key.to))]
			if !ok {
				rt.Fatalf("the %s edge from %s to %s is missing from the canonical space",
					key.typ, key.from, key.to)
			}
			in, ok := inRows[string(graph.InKey(sc, key.from, key.typ, key.to))]
			if !ok {
				rt.Fatalf("the %s edge from %s to %s has no reverse entry", key.typ, key.from, key.to)
			}
			if !bytes.Equal(out, in) {
				rt.Fatalf("the reverse entry of the %s edge from %s to %s holds different bytes "+
					"from the edge it is derived from", key.typ, key.from, key.to)
			}
		}
	})
}

func dumpSpace(rt *rapid.T, kv *memkv.Store, sc graph.Scope, space keys.Space) map[string][]byte {
	rt.Helper()
	lower, upper := keys.SpaceRange(sc.Tenant, tenant.DefaultNamespace, space)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	out := map[string][]byte{}
	for ok := it.First(); ok; ok = it.Next() {
		out[string(it.Key())] = append([]byte(nil), it.Value()...)
	}
	if err := it.Error(); err != nil {
		rt.Fatalf("scanning the %s space: %v", space, err)
	}
	return out
}
