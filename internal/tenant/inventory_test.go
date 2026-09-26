package tenant_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// fakeDirectory is a directory in a map. The inventory only reads, which is
// what this fake is here to hold: a Create or an Ensure reaching it is the
// defect the test exists to catch.
type fakeDirectory struct {
	metas   []tenant.Meta
	created int
	listErr error
}

func (d *fakeDirectory) Create(_ context.Context, id tenant.ID, m tenant.Meta) error {
	d.created++
	m.ID = id
	if m.SchemaVersion == 0 {
		m.SchemaVersion = tenant.DefaultSchemaVersion
	}
	d.metas = append(d.metas, m)
	return nil
}

func (d *fakeDirectory) Get(_ context.Context, id tenant.ID) (tenant.Meta, error) {
	for _, m := range d.metas {
		if m.ID == id {
			return m, nil
		}
	}
	return tenant.Meta{}, errs.E(errs.NotFound, "fake", errors.New("no such tenant"))
}

func (d *fakeDirectory) List(context.Context) ([]tenant.Meta, error) {
	if d.listErr != nil {
		return nil, d.listErr
	}
	return d.metas, nil
}

func (d *fakeDirectory) ForEach(ctx context.Context, fn func(tenant.ID) error) error {
	for _, m := range d.metas {
		if err := fn(m.ID); err != nil {
			return err
		}
	}
	return nil
}

func directoryOf(ids ...tenant.ID) *fakeDirectory {
	d := &fakeDirectory{}
	for _, id := range ids {
		d.metas = append(d.metas, tenant.Meta{ID: id})
	}
	return d
}

func TestInventorySeparatesTheImplicitTenantFromTheRest(t *testing.T) {
	for _, tc := range []struct {
		name        string
		registered  []tenant.ID
		wantOthers  []tenant.ID
		wantImplied bool
		wantSingle  bool
	}{
		{"a fresh directory", nil, nil, false, true},
		{"only the implicit tenant", []tenant.ID{"default"}, nil, true, true},
		{"the implicit tenant and one more", []tenant.ID{"acme", "default"}, []tenant.ID{"acme"}, true, false},
		{"no implicit tenant but others exist", []tenant.ID{"acme", "beta"}, []tenant.ID{"acme", "beta"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := directoryOf(tc.registered...)
			inv, err := tenant.TakeInventory(context.Background(), d, "default")
			if err != nil {
				t.Fatalf("TakeInventory: %v", err)
			}
			if inv.HasImplicit != tc.wantImplied {
				t.Fatalf("HasImplicit = %v, want %v", inv.HasImplicit, tc.wantImplied)
			}
			if len(inv.Others) != len(tc.wantOthers) {
				t.Fatalf("Others = %v, want %v", inv.Others, tc.wantOthers)
			}
			for i := range tc.wantOthers {
				if inv.Others[i] != tc.wantOthers[i] {
					t.Fatalf("Others = %v, want %v", inv.Others, tc.wantOthers)
				}
			}
			if inv.SingleTenant() != tc.wantSingle {
				t.Fatalf("SingleTenant = %v, want %v", inv.SingleTenant(), tc.wantSingle)
			}
			if d.created != 0 {
				t.Fatalf("taking an inventory created %d tenants; it is a read", d.created)
			}
		})
	}
}

// The refusal is the operator's instruction sheet, so it names the tenants and
// the command rather than counting them.
func TestTheMigrationRefusalNamesEveryTenantAndTheCommand(t *testing.T) {
	d := directoryOf("acme", "beta", "default")
	inv, err := tenant.TakeInventory(context.Background(), d, "default")
	if err != nil {
		t.Fatalf("TakeInventory: %v", err)
	}
	refusal := inv.MigrationRequired()
	if refusal == nil {
		t.Fatal("a directory holding three tenants must not pass a single-tenant check")
	}
	if !errs.Is(refusal, errs.MigrationRequired) {
		t.Fatalf("the refusal is %s, want migration_required", errs.KindOf(refusal))
	}
	for _, want := range []string{"acme", "beta", "default", "remem-admin export", "docs/MIGRATION.md"} {
		if !strings.Contains(refusal.Error(), want) {
			t.Fatalf("the refusal does not mention %q:\n%s", want, refusal)
		}
	}
}

func TestASingleTenantDirectoryNeedsNoMigration(t *testing.T) {
	for _, registered := range [][]tenant.ID{nil, {"default"}} {
		d := &fakeDirectory{}
		for _, id := range registered {
			d.metas = append(d.metas, tenant.Meta{ID: id})
		}
		inv, err := tenant.TakeInventory(context.Background(), d, "default")
		if err != nil {
			t.Fatalf("TakeInventory: %v", err)
		}
		if err := inv.MigrationRequired(); err != nil {
			t.Fatalf("a directory holding %v was refused: %v", registered, err)
		}
	}
}

func TestInventoryRefusesAnImplicitTenantThatIsNotATenantID(t *testing.T) {
	_, err := tenant.TakeInventory(context.Background(), directoryOf(), "../etc")
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestInventoryReportsAFailedRead(t *testing.T) {
	d := directoryOf("default")
	d.listErr = errs.E(errs.Storage, "fake", errors.New("the directory could not be read"))
	if _, err := tenant.TakeInventory(context.Background(), d, "default"); !errs.Is(err, errs.Storage) {
		t.Fatalf("got %v, want the read failure to surface", err)
	}
}
