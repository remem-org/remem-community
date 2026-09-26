package inspect_test

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/inspect"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

const acme = tenant.ID("acme")

// corpus is written the way the server writes one — through the memory
// service, with both derived indexes wired — so every row the checker reads is
// a row production would have produced. It holds four memories: two connected,
// one archived, and one hard-deleted, because the two deletion paths are the
// ones most likely to leave something behind.
func corpus(t *testing.T) (storage.KV, []id.ID) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)

	dir := tenantkv.New(kv, clk)
	base := context.Background()
	if err := dir.Create(base, acme, tenant.Meta{DisplayName: "acme"}); err != nil {
		t.Fatal(err)
	}

	texts := text.New()
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts))
	svc := memory.New(kv, repo, flat.New(vector.NewStore(kv), distance.L2), embeddingtest.New(), clk,
		attr.MustTable(), graph.NewService(kv, clk), memory.Config{}, memory.WithTextIndex(texts))
	t.Cleanup(func() { _ = svc.Close() })

	ctx := tenant.NewContext(base, acme)
	var ids []id.ID
	for _, content := range []string{
		"raft elects a leader by majority vote",
		"a leader replicates its log to followers",
		"marzipan recipes from a Bavarian bakery",
		"an invoice numbered INV-2024-8871",
	} {
		m, err := svc.Create(ctx, memory.CreateReq{Content: content, Tags: []string{"test"}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, m.ID)
	}
	if _, err := svc.Relate(ctx, memory.RelateReq{From: ids[0], To: ids[1], Type: "supports", Strength: 0.8}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(ctx, ids[2], false); err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(ctx, ids[3], true); err != nil {
		t.Fatal(err)
	}
	return kv, ids
}

func check(t *testing.T, kv storage.KV) inspect.Report {
	t.Helper()
	r, err := inspect.CheckTenant(context.Background(), kv, acme)
	if err != nil {
		t.Fatalf("CheckTenant: %v", err)
	}
	if r.Tenant != acme {
		t.Fatalf("the report is for %q, want acme", r.Tenant)
	}
	return r
}

// mustFind requires a finding in space whose problem mentions every fragment.
func mustFind(t *testing.T, r inspect.Report, space string, fragments ...string) {
	t.Helper()
	for _, f := range r.Findings {
		if f.Space != space {
			continue
		}
		all := true
		for _, frag := range fragments {
			if !strings.Contains(f.Problem, frag) {
				all = false
				break
			}
		}
		if all {
			return
		}
	}
	t.Fatalf("no %s finding mentioning %q; the report holds:\n%s", space, fragments, describe(r))
}

func describe(r inspect.Report) string {
	var b strings.Builder
	for _, f := range r.Findings {
		b.WriteString("  " + f.String() + "\n")
	}
	if b.Len() == 0 {
		return "  (no findings)"
	}
	return b.String()
}

// TestACleanCorpusHasNoFindings: what the write path produces — including an
// archive and a hard delete — is consistent by the checker's definition. The
// crash tests stand on this: a finding after a kill has to mean the kill did
// it, not that ordinary writes already look like damage.
func TestACleanCorpusHasNoFindings(t *testing.T) {
	kv, _ := corpus(t)
	r := check(t, kv)
	if !r.Clean() {
		t.Fatalf("a corpus written through the service has findings:\n%s", describe(r))
	}
	if r.Records != 3 {
		t.Fatalf("examined %d records; four were written and one hard-deleted", r.Records)
	}
}

func TestADeletedAttributeRowIsNamed(t *testing.T) {
	kv, ids := corpus(t)
	if err := kv.Delete(context.Background(), keys.AttrRow(acme, tenant.DefaultNamespace, ids[0])); err != nil {
		t.Fatal(err)
	}
	r := check(t, kv)
	mustFind(t, r, "attr_row", ids[0].String(), "no attribute row")
	if len(r.Findings) != 1 {
		t.Fatalf("one deleted row produced %d findings:\n%s", len(r.Findings), describe(r))
	}
}

// TestADeletedRecordBodyOrphansEveryIndexAndIsNamedInEach: the shape a crash
// between two halves of a write would leave, if a write were ever not atomic.
func TestADeletedRecordBodyOrphansEveryIndexAndIsNamedInEach(t *testing.T) {
	kv, ids := corpus(t)
	if err := kv.Delete(context.Background(),
		keys.Record(acme, tenant.DefaultNamespace, keys.RecordMemory, ids[1])); err != nil {
		t.Fatal(err)
	}
	r := check(t, kv)
	for _, space := range []string{"attr_row", "attr_index", "text", "vector"} {
		mustFind(t, r, space, ids[1].String(), "does not exist")
	}
	// ids[1] is the target of the one connection.
	mustFind(t, r, "edge", "missing_record")
}

func TestDeletedInEdgesAreNamed(t *testing.T) {
	kv, _ := corpus(t)
	deleteSpace(t, kv, keys.SpaceEdgeIn)
	mustFind(t, check(t, kv), "edge", "missing_reverse")
}

func TestAKeyUnderAnUndeclaredSpaceIsNamed(t *testing.T) {
	kv, _ := corpus(t)
	lower, _ := keys.NamespaceRange(acme, tenant.DefaultNamespace)
	bad := append(append([]byte(nil), lower...), 0x7F, 1, 2, 3)
	if err := kv.Set(context.Background(), bad, []byte("x")); err != nil {
		t.Fatal(err)
	}
	mustFind(t, check(t, kv), "unknown", "does not parse")
}

func TestARecordBodyThatDoesNotDecodeIsNamed(t *testing.T) {
	kv, ids := corpus(t)
	if err := kv.Set(context.Background(),
		keys.Record(acme, tenant.DefaultNamespace, keys.RecordMemory, ids[0]),
		[]byte{0xFF, 0x00, 0x13, 0x37}); err != nil {
		t.Fatal(err)
	}
	mustFind(t, check(t, kv), "record", "does not decode")
}

func deleteSpace(t *testing.T, kv storage.KV, s keys.Space) {
	t.Helper()
	lower, upper := keys.SpaceRange(acme, tenant.DefaultNamespace, s)
	it := kv.NewIterator(lower, upper)
	var doomed [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		doomed = append(doomed, append([]byte(nil), it.Key()...))
	}
	_ = it.Close()
	if len(doomed) == 0 {
		t.Fatalf("the corpus has no %s rows to delete", s)
	}
	for _, k := range doomed {
		if err := kv.Delete(context.Background(), k); err != nil {
			t.Fatal(err)
		}
	}
}
