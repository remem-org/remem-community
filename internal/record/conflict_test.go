package record_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

func TestStaleArchiveCannotResurrectDeletedRecord(t *testing.T) {
	repo, kv := newRepo(t)
	rec := sampleRecord("acme")
	commitPut(t, repo, kv, rec)
	stale, err := repo.Get(ctx(), rec.ID)
	must(t, err)
	tx := txn.New(kv)
	defer tx.Close()
	must(t, repo.Delete(ctx(), tx, rec.ID))
	must(t, tx.Commit(ctx()))
	stale.Fields.Archived = true
	write := txn.New(kv)
	defer write.Close()
	must(t, repo.Put(ctx(), write, stale))
	if err := write.Commit(ctx()); !errs.Is(err, errs.Conflict) {
		t.Fatalf("want Conflict, got %v", err)
	}
	if _, err := repo.Get(ctx(), rec.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("record resurrected: %v", err)
	}
}

func TestLazyUpgradeMaintainsAttributeRows(t *testing.T) {
	_, kv := newRepo(t)
	table := attr.MustTable()
	repo := record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(table)))
	rec := sampleRecord("acme")
	rec.SchemaVersion = 1
	commitPut(t, repo, kv, rec)

	upgraded := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(table)),
		record.WithUpgrader(upgradeFunc(func(_ context.Context, r *record.Record, v uint32) (uint32, bool, error) {
			r.Fields.Archived = true
			return v + 1, true, nil
		})),
	)
	got, err := upgraded.Get(ctx(), rec.ID)
	must(t, err)
	if !got.Fields.Archived {
		t.Fatal("upgrade was not applied to returned record")
	}

	b, err := kv.Get(ctx(), attr.RowKey("acme", tenant.DefaultNamespace, rec.ID))
	must(t, err)
	row, err := attr.DecodeRow(b, table)
	must(t, err)
	v, ok := row.Get(attr.SlotArchived)
	archived, typed := v.AsBool()
	if !ok || !typed || !archived {
		t.Fatalf("attribute row was not upgraded: present=%v typed=%v archived=%v", ok, typed, archived)
	}
}

type upgradeFunc func(context.Context, *record.Record, uint32) (uint32, bool, error)

func (f upgradeFunc) Upgrade(c context.Context, r *record.Record, v uint32) (uint32, bool, error) {
	return f(c, r, v)
}

func TestLazyUpgradeCannotOverwriteConcurrentMutation(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "update", true: "delete"}[deleted], func(t *testing.T) {
			repo, kv := newRepo(t)
			rec := sampleRecord("acme")
			commitPut(t, repo, kv, rec)
			upgraded := record.NewRepo(kv, record.WithUpgrader(upgradeFunc(func(c context.Context, r *record.Record, v uint32) (uint32, bool, error) {
				tx := txn.New(kv)
				defer tx.Close()
				if deleted {
					must(t, repo.Delete(c, tx, r.ID))
				} else {
					fresh, err := repo.Get(c, r.ID)
					must(t, err)
					fresh.Content = "new content"
					must(t, repo.Put(c, tx, fresh))
				}
				must(t, tx.Commit(c))
				r.Content = "upgraded old content"
				return v + 1, true, nil
			})))
			_, err := upgraded.Get(ctx(), rec.ID)
			must(t, err)
			got, err := repo.Get(ctx(), rec.ID)
			if deleted {
				if !errs.Is(err, errs.NotFound) {
					t.Fatalf("upgrade resurrected record: %v", err)
				}
			} else {
				must(t, err)
				if got.Content != "new content" {
					t.Fatalf("lost update: %q", got.Content)
				}
			}
		})
	}
}
