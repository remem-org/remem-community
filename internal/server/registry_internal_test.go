package server

import (
	"bytes"
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// TestEveryDerivedIndexHasARebuildPath walks the key spaces, which are the one
// registry every durable row cannot avoid joining, and holds each derived space
// to a registered rebuild job (Invariant 3).
//
// A CLI rebuild alone does not satisfy it: `remem-admin` needs a stopped server,
// so a derived space with only a CLI path is one a running server cannot repair.
func TestEveryDerivedIndexHasARebuildPath(t *testing.T) {
	d := mustDeps(t)
	for _, s := range keys.AllSpaces() {
		if s.Class() != keys.Derived {
			continue
		}
		typ, ok := rebuildJobFor(s)
		if !ok {
			t.Errorf("space %s is derived and no job rebuilds it (Invariant 3): a running server "+
				"cannot repair it", s)
			continue
		}
		if _, registered := d.registry.Lookup(typ); !registered {
			t.Errorf("space %s names job %s, which is not registered", s, typ)
		}
	}
}

// TestARebuildJobRestoresItsSpace is what makes each path *tested* rather than
// declared: every row of the derived space is deleted, the job that claims to
// rebuild it runs, and the space must hold what it held before.
func TestARebuildJobRestoresItsSpace(t *testing.T) {
	for _, s := range keys.AllSpaces() {
		if s.Class() != keys.Derived {
			continue
		}
		t.Run(s.String(), func(t *testing.T) {
			typ, ok := rebuildJobFor(s)
			if !ok {
				t.Skipf("no job for %s; TestEveryDerivedIndexHasARebuildPath reports it", s)
			}
			// HNSW rather than the default flat index: flat keeps no node
			// records, so its vector_index space is empty and deleting it
			// would prove nothing.
			d := mustDepsWith(t, func(c *config.Config) { c.Vector.Index = "hnsw" })
			const tid = tenant.ID("acme")
			seedConnectedMemories(t, d, tid)
			ctx := context.Background()
			if s == keys.SpaceVectorIndex {
				// Node records are written when a tenant's graph materialises,
				// which a read triggers.
				if _, err := d.vectors.Stats(ctx, tid); err != nil {
					t.Fatal(err)
				}
			}

			before := dumpSpace(t, d.kv, tid, s)
			if len(before) == 0 {
				t.Fatalf("the fixture wrote no %s rows, so deleting them would prove nothing", s)
			}
			deleteSpace(t, d.kv, tid, s)
			if len(dumpSpace(t, d.kv, tid, s)) != 0 {
				t.Fatalf("%s rows survived the delete", s)
			}

			handler, err := d.registry.Handler(typ)
			if err != nil {
				t.Fatal(err)
			}
			j := &jobs.Job{Tenant: tid, Namespace: tenant.DefaultNamespace, Type: typ}
			if err := handler.Handle(ctx, j, noCheckpoint{}); err != nil {
				t.Fatalf("%s: %v", typ, err)
			}

			after := dumpSpace(t, d.kv, tid, s)
			if s == keys.SpaceVectorIndex {
				// Node numbering after a clear is the rebuild's, not the
				// incremental writes', so the rows are compared by count: every
				// canonical vector has its node again.
				if len(after) != len(before) {
					t.Fatalf("%s: %d node records before, %d after the rebuild",
						s, len(before), len(after))
				}
				return
			}
			if !bytes.Equal(bytes.Join(before, nil), bytes.Join(after, nil)) {
				t.Fatalf("%s: %d rows before, %d after, and they differ: the job named as its "+
					"rebuild does not reproduce it", s, len(before), len(after))
			}
		})
	}
}

// dumpSpace returns every key and value in one tenant's space, each pair
// concatenated, in key order.
func dumpSpace(t *testing.T, kv storage.KV, tid tenant.ID, s keys.Space) [][]byte {
	t.Helper()
	lower, upper := keys.SpaceRange(tid, tenant.DefaultNamespace, s)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	var out [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, append(append([]byte(nil), it.Key()...), it.Value()...))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

func deleteSpace(t *testing.T, kv storage.KV, tid tenant.ID, s keys.Space) {
	t.Helper()
	lower, upper := keys.SpaceRange(tid, tenant.DefaultNamespace, s)
	it := kv.NewIterator(lower, upper)
	var doomed [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		doomed = append(doomed, append([]byte(nil), it.Key()...))
	}
	_ = it.Close()
	tx := txn.New(kv)
	defer tx.Close()
	for _, k := range doomed {
		tx.Delete(k)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}
