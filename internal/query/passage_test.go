package query

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/indexes"
)

// Why the index groups by memory rather than the executor (design decision 6
// of openspec/changes/passage-vectors). With no passage cap, one memory of a
// thousand passages nearest the query, rejected by the filter, would fill a
// limit-10 search's whole widening budget — 320 candidates — with itself, and
// the page would come back empty and marked truncated. Grouped in the index it
// is one candidate, and the page fills from the memories that pass.
func TestAThousandPassageMemoryCannotCrowdAFilteredPage(t *testing.T) {
	for _, kind := range []string{"flat", "hnsw"} {
		t.Run(kind, func(t *testing.T) {
			const dim = 8
			kv := memkv.New()
			t.Cleanup(func() { _ = kv.Close() })
			slots, err := attr.Table()
			if err != nil {
				t.Fatal(err)
			}
			ix, err := indexes.Open(kv, indexes.Config{
				Kind: kind, Metric: "l2", Neighbours: 8, BuildEffort: 64, SearchEffort: 64,
				ResidentBudgetBytes: 256 << 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx := tenant.NewContext(context.Background(), widenTenant)
			rng := rand.New(rand.NewPCG(3, 3))
			q := make([]float32, dim)
			q[0] = 1

			policies := map[id.ID]string{}
			crowd := widenID(9, 0)
			passages := make([]vector.Entry, 1000)
			for p := range passages {
				v := append([]float32(nil), q...)
				v[1] = 0.001 * rng.Float32()
				passages[p] = vector.Entry{Passage: uint32(p), Vector: v}
			}
			if err := ix.Insert(ctx, widenTenant, crowd, passages); err != nil {
				t.Fatal(err)
			}
			policies[crowd] = "short_term"
			for i := range 40 {
				rid := widenID(9, uint64(i+1))
				v := make([]float32, dim)
				v[0], v[1+i%(dim-1)] = 0.5, 0.5+rng.Float32()
				if err := ix.Insert(ctx, widenTenant, rid, vector.Single(v)); err != nil {
					t.Fatal(err)
				}
				policies[rid] = "long_term"
			}
			if err := txn.Do(ctx, kv, func(tx txn.Tx) error {
				for rid, policy := range policies {
					row := attr.NewRow(attr.SchemaVersion)
					row.Set(attr.SlotPolicy, attr.Str(policy))
					enc, err := row.Encode(slots)
					if err != nil {
						return err
					}
					tx.Set(attr.RowKey(widenTenant, tenant.DefaultNamespace, rid), enc)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			query := &Query{Tenant: widenTenant, Vector: q, Limit: 10,
				Preds: attr.Preds{attr.Eq(attr.SlotPolicy, attr.Str("long_term"))}}
			plan, err := NewPlanner(slots, 0, 0).Plan(query)
			if err != nil {
				t.Fatal(err)
			}
			page, err := NewExecutor(slots, ix, distance.L2).runVector(ctx, kv, query, plan,
				Step{Kind: StepVector, Source: SourceVector})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.items) != 10 || page.truncated {
				t.Fatalf("%d results, truncated %v: the thousand-passage memory crowded the page",
					len(page.items), page.truncated)
			}
			seen := map[id.ID]bool{}
			for _, it := range page.items {
				if it.ID == crowd {
					t.Fatal("the filtered-out memory reached the page")
				}
				if seen[it.ID] {
					t.Fatalf("memory %s is on the page twice", it.ID)
				}
				seen[it.ID] = true
			}
		})
	}
}
