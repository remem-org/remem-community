package hnsw

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"pgregory.net/rapid"
)

// The residency budget is charged what residentBytes says, and residentBytes
// is a running count kept at every site that changes a list's length or the
// arena. A count that drifts evicts too early or too late, and nothing on a
// request path would ever notice — so the count is held, after every single
// operation, to the full walk it replaced.
//
// The graph is small-M on purpose: at a bound of two or three a list overflows
// within a few inserts, so pruning, the last-in-edge rescue and removal
// relinking all run, and those are the sites that change lengths by more than
// one. Some vectors are drawn as exact duplicates of an axis, the shape that
// makes the diversity heuristic prune hardest.
func TestResidentBytesMatchesARecountAfterEveryOperation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dim := rapid.IntRange(2, 6).Draw(rt, "dim")
		m := rapid.IntRange(2, 4).Draw(rt, "m")
		g := newGraph(Params{M: m, EfConstruction: 4 * m}, distance.Cosine)

		// Ids are drawn, not generated: a record's layer is a function of its
		// id, and rapid replays draws and nothing else.
		seed := rapid.Uint64().Draw(rt, "id seed")
		next := uint64(0)
		newID := func() id.ID {
			var rid id.ID
			binary.BigEndian.PutUint64(rid[0:8], seed)
			binary.BigEndian.PutUint64(rid[8:16], next)
			next++
			return rid
		}
		vec := func() []float32 {
			if rapid.Bool().Draw(rt, "duplicate") {
				v := make([]float32, dim)
				v[rapid.IntRange(0, dim-1).Draw(rt, "axis")] = 1
				return v
			}
			return newRNG(rapid.Uint64().Draw(rt, "vector seed")).unit(dim)
		}

		var live []id.ID
		check := func(what string) {
			if got, want := g.residentBytes(), g.recountResidentBytes(); got != want {
				rt.Fatalf("after %s the graph is charged %d bytes and a recount finds %d", what, got, want)
			}
		}
		check("creation")

		ops := rapid.IntRange(1, 120).Draw(rt, "ops")
		for i := range ops {
			switch op := rapid.IntRange(0, 9).Draw(rt, "op"); {
			case op < 6 || len(live) == 0:
				rid := newID()
				if _, err := g.insert(vector.Key{ID: rid}, vec()); err != nil {
					rt.Fatalf("insert: %v", err)
				}
				live = append(live, rid)
				check(fmt.Sprintf("insert %d", i))
			case op < 8:
				rid := live[rapid.IntRange(0, len(live)-1).Draw(rt, "replace")]
				if _, err := g.insert(vector.Key{ID: rid}, vec()); err != nil {
					rt.Fatalf("replace: %v", err)
				}
				check(fmt.Sprintf("replacement %d", i))
			default:
				k := rapid.IntRange(0, len(live)-1).Draw(rt, "remove")
				g.remove(g.byID[vector.Key{ID: live[k]}])
				live = append(live[:k], live[k+1:]...)
				check(fmt.Sprintf("removal %d", i))
			}
		}
	})
}

// BenchmarkResidentBytes is what the budget update costs a write, at two
// corpus sizes a hundred times apart. Before the running counts it was a walk
// of every node — finding 14 E7 measured about 4ms of it per insert at 90,000
// nodes — and the two numbers here are how that is shown to be gone rather
// than asserted to be.
//
// The graphs are built at a small width and a low construction effort, because
// the shape that matters to this measurement is the number of nodes and the
// length of their lists, and those follow M rather than the width.
func BenchmarkResidentBytes(b *testing.B) {
	for _, nodes := range []int{1_000, 100_000} {
		g := newGraph(Params{M: 16, EfConstruction: 32}, distance.Cosine)
		r := newRNG(uint64(nodes))
		for i := range nodes {
			var rid id.ID
			binary.BigEndian.PutUint64(rid[8:16], uint64(i)+1)
			if _, err := g.insert(vector.Key{ID: rid}, r.unit(16)); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			var sink int64
			for range b.N {
				sink += g.residentBytes()
			}
			if sink == 0 {
				b.Fatal("a populated graph measured at zero bytes")
			}
		})
	}
}

// A materialised graph enters the budget at its recounted size, and so does a
// rebuilt one. Materialisation is the one place lists arrive whole rather than
// through the helpers — read from node records, then filtered of neighbours
// that are gone — so it is checked on a store where some are gone: canonical
// vectors deleted behind the index's back, as a crash between a record's delete
// and the index hearing of it leaves them.
func TestAMaterialisedOrRebuiltGraphIsChargedItsRecount(t *testing.T) {
	const tid = tenant.ID("acme")
	ctx := context.Background()
	ids, vecs := resumeCorpus(600, 8)

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	first := resumeIndex(t, kv)
	for i := range ids {
		if err := first.Insert(ctx, tid, ids[i], vector.Single(vecs[i])); err != nil {
			t.Fatal(err)
		}
	}
	store := vector.NewStore(kv)
	for i := 0; i < len(ids); i += 7 {
		if err := store.Delete(ctx, tid, tenant.DefaultNamespace, ids[i]); err != nil {
			t.Fatal(err)
		}
	}

	charged := func(x *Index, what string) {
		t.Helper()
		r, ok := x.residentFor(tid)
		if !ok || r.g == nil {
			t.Fatalf("after %s the tenant is not resident", what)
		}
		want := r.g.recountResidentBytes()
		if got := r.g.residentBytes(); got != want {
			t.Fatalf("after %s the graph counts %d bytes and a recount finds %d", what, got, want)
		}
		if _, got := x.Resident(); got != want {
			t.Fatalf("after %s the budget is charged %d bytes and a recount finds %d", what, got, want)
		}
	}

	second := resumeIndex(t, kv)
	r, err := second.acquire(ctx, tid)
	if err != nil {
		t.Fatal(err)
	}
	second.release(r)
	if r.g.live == len(ids) {
		t.Fatal("no canonical vector was missing at materialisation, so the filter was never exercised")
	}
	charged(second, "materialisation")

	third := resumeIndex(t, kv)
	if err := third.Rebuild(ctx, tid, vector.NewStore(kv), vector.WithBatchSize(100)); err != nil {
		t.Fatal(err)
	}
	charged(third, "a rebuild")
}

// A write whose node records fail to commit has still changed the graph in
// memory, and memory is what the budget bounds, so it is charged regardless.
// Before the measurement moved under the write lock an error return skipped
// it, and the budget described a graph that no longer existed until the next
// successful write happened to correct it.
func TestAFailedNodeRecordCommitIsStillCharged(t *testing.T) {
	const tid = tenant.ID("acme")
	ctx := context.Background()
	ids, vecs := resumeCorpus(200, 8)

	kv := memkv.New()
	x := resumeIndex(t, kv)
	for i := range 100 {
		if err := x.Insert(ctx, tid, ids[i], vector.Single(vecs[i])); err != nil {
			t.Fatal(err)
		}
	}
	// Every commit from here on fails; the graph stays resident.
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	charged := func(what string) {
		t.Helper()
		r, ok := x.residentFor(tid)
		if !ok {
			t.Fatalf("after %s the tenant is not resident", what)
		}
		want := r.g.recountResidentBytes()
		if _, got := x.Resident(); got != want {
			t.Fatalf("after %s the budget is charged %d bytes and the graph holds %d", what, got, want)
		}
	}

	_, before := x.Resident()
	for i := 100; i < 200; i++ {
		if err := x.Indexed(ctx, tid, ids[i], vector.Single(vecs[i])); err == nil {
			t.Fatal("a node-record commit against a closed store succeeded")
		}
	}
	if _, after := x.Resident(); after <= before {
		t.Fatalf("a hundred inserts whose commits failed left the budget at %d bytes, from %d", after, before)
	}
	charged("inserts whose commits failed")

	for i := range 50 {
		if err := x.Removed(ctx, tid, ids[i]); err == nil {
			t.Fatal("a node-record commit against a closed store succeeded")
		}
	}
	charged("removals whose commits failed")
}

// Two writes to one tenant measure in the order they held the graph and may
// charge in the other. The later measurement wins whichever lands first.
func TestAnOlderMeasurementDoesNotOverwriteANewerCharge(t *testing.T) {
	x := resumeIndex(t, memkv.New())
	r := &resident{t: "acme"}
	x.tenants[r.t] = r

	x.charge(r, size{bytes: 900, seq: 2})
	x.charge(r, size{bytes: 400, seq: 1})
	if _, got := x.Resident(); got != 900 {
		t.Fatalf("the budget is charged %d bytes after a stale measurement landed second; want the newer 900", got)
	}
	x.charge(r, size{bytes: 1200, seq: 3})
	if _, got := x.Resident(); got != 1200 {
		t.Fatalf("the budget is charged %d bytes; want the newest measurement's 1200", got)
	}
}
