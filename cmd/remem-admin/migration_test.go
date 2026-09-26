package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
)

// seedMultiTenantCorpus is the directory an existing deployment serving several
// tenants leaves behind: the shape the migration path has to move.
func seedMultiTenantCorpus(t *testing.T, tenants ...tenant.ID) (dir string, memories map[tenant.ID][]id.ID) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "data")
	memories = make(map[tenant.ID][]id.ID, len(tenants))

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(text.New()))
	directory := tenantkv.New(kv, clock.System())
	edges := graph.NewService(kv, clock.System())

	for _, tid := range tenants {
		ctx := tenant.NewContext(context.Background(), tid)
		if err := directory.Create(ctx, tid, tenant.Meta{DisplayName: string(tid) + " Ltd"}); err != nil {
			t.Fatalf("creating tenant %s: %v", tid, err)
		}
		var ids []id.ID
		for i := range 3 {
			ids = append(ids, writeEmbeddedMemoryAs(t, ctx, kv, repo, tid,
				string(tid)+" memory about invoices and payments number "+string(rune('a'+i))))
		}
		if err := txn.Do(ctx, kv, func(tx txn.Tx) error {
			return edges.Add(ctx, tx, graph.Edge{
				From: ids[0], To: ids[1], Type: graph.RelatedTo, Strength: 0.9,
				Meta: map[string]string{"why": string(tid) + "'s own edge"},
			})
		}); err != nil {
			t.Fatal(err)
		}
		memories[tid] = ids
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, memories
}

// digestOf hashes every key and value in a data directory, so a test can say
// "this directory was not touched" rather than "the tenants are still listed".
func digestOf(t *testing.T, dir string) string {
	t.Helper()
	kv, err := pebble.Open(dir, pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()

	h := sha256.New()
	it := kv.NewIterator(nil, nil)
	for ok := it.First(); ok; ok = it.Next() {
		h.Write(it.Key())
		h.Write(it.Value())
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// The upgrade path of docs/MIGRATION.md, run end to end through the real
// commands: inventory the directory, export each tenant on its own, import each
// snapshot into a deployment of its own, verify it, and leave the original
// directory exactly as it was for rollback.
//
// It is one test rather than five because the claim is the sequence: any step
// on its own is already covered, and what an operator needs to know is that
// following them in this order loses nothing and changes nothing.
//
// Each destination here is itself a single-tenant deployment, whose implicit
// tenant is set to the tenant being moved in. That is the shape of the migration
// a small deployment actually wants — three tenants becoming three installs —
// and it keeps every tenant identity as it was rather than renaming anything,
// which is what the spec requires of a migration. A destination that resolves
// tenants from an authenticated identity takes the same files and needs no such
// setting.
func TestTheMultiTenantUpgradePathPreservesEveryTenant(t *testing.T) {
	t.Setenv("REMEM_TENANT_DEFAULT", "default")
	src, memories := seedMultiTenantCorpus(t, "default", "acme", "globex")
	before := digestOf(t, src)

	// 1. The inventory tells the operator what a single-tenant build will refuse
	//    and which tenants have to move.
	var out, errOut bytes.Buffer
	kv, err := pebble.Open(src, pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	code, err := inventoryOver(context.Background(), tenantkv.New(kv, clock.System()),
		src, "default", &out, &errOut)
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	if code != exitMigrationRequired {
		t.Fatalf("the inventory exited %d over three tenants, want %d", code, exitMigrationRequired)
	}

	// 2. One export and one import per tenant. The destinations are separate
	//    directories: a migration moves a tenant into a deployment, it does not
	//    merge namespaces.
	restored := make(map[tenant.ID]string, len(memories))
	for tid := range memories {
		file := filepath.Join(t.TempDir(), string(tid)+".rsnap")
		dst := filepath.Join(t.TempDir(), "restored-"+string(tid))

		// The destination serves this tenant, so this is the tenant its
		// implicit one is set to. Importing a snapshot naming any other tenant
		// into it is refused, which is the check the next test makes.
		t.Setenv("REMEM_TENANT_DEFAULT", string(tid))

		if _, err := run([]string{"export", "--data-dir", src, "--tenant", string(tid),
			"--out", file}); err != nil {
			t.Fatalf("exporting %s: %v", tid, err)
		}
		// --vectors=verbatim: these embeddings already claim the running model,
		// and a plain remem-admin has no model to recompute with. A snapshot
		// written by another implementation refuses this and says why.
		if _, err := run([]string{"import", "--data-dir", dst, "--in", file,
			"--vectors", "verbatim"}); err != nil {
			t.Fatalf("importing %s: %v", tid, err)
		}
		// 3. Verify each destination against the file it came from, before
		//    anything starts serving from it — a sweep moves exactly the fields
		//    the verifier compares.
		code, err := run([]string{"verify", "--data-dir", dst, "--in", file})
		if err != nil {
			t.Fatalf("verifying %s: %v", tid, err)
		}
		if code != 0 {
			t.Fatalf("verify exited %d on %s's freshly imported directory", code, tid)
		}
		restored[tid] = dst
	}

	// Each destination holds exactly its own tenant, under the identity it had,
	// and none of the others' memories.
	for tid, dst := range restored {
		kv, err := pebble.Open(dst, pebble.Options{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		repo := record.NewRepo(kv)
		dir := tenantkv.New(kv, clock.System())

		inv, err := tenant.TakeInventory(context.Background(), dir, tid)
		if err != nil {
			t.Fatal(err)
		}
		if !inv.HasImplicit || !inv.SingleTenant() {
			t.Fatalf("%s's destination holds %v beyond %s", tid, inv.Others, tid)
		}
		own := tenant.NewContext(context.Background(), tid)
		for _, rid := range memories[tid] {
			if _, err := repo.Get(own, rid); err != nil {
				t.Fatalf("%s's memory %s did not survive the migration: %v", tid, rid, err)
			}
		}
		for other, ids := range memories {
			if other == tid {
				continue
			}
			foreign := tenant.NewContext(context.Background(), other)
			if _, err := repo.Get(foreign, ids[0]); !errs.Is(err, errs.NotFound) {
				t.Fatalf("%s's memory arrived in %s's directory: %v", other, tid, err)
			}
		}
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// 4. And the directory the operator still has to roll back to is byte for
	//    byte what it was. Rollback is the prior binary over this directory; it
	//    is never a merge of what was migrated out.
	t.Setenv("REMEM_TENANT_DEFAULT", "default")
	if after := digestOf(t, src); after != before {
		t.Fatal("the migration changed the source directory; rollback depends on it being untouched")
	}
}
