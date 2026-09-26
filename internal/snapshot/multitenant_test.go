package snapshot_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/tenant"
)

// A multi-tenant export and import is the migration path an existing Community
// deployment leaves by: its tenants are carried into an Enterprise/Cloud
// directory one at a time, under the identities they already had. This test is
// the whole of that claim, for every kind of row a tenant owns — the directory
// row, the record, the derived indexes rebuilt from it, the graph edge with its
// metadata, and the lifecycle audit stream.
//
// TestCrossTenantRowsLandInTheirOwnTenants covers the record alone. The reason
// this one exists beside it is that a migration is only trustworthy if the
// *tenant-owned* rows nothing recomputes — the edge's metadata and the audit
// events — arrive under the right tenant too, and a snapshot that dropped them
// would still pass a record-only check.
func TestAMultiTenantSnapshotCarriesEveryTenantsOwnRows(t *testing.T) {
	w := newWorld(t)
	w.tenantNamed(t, "default", "The first deployment")
	w.tenantNamed(t, "acme", "Acme Corp")

	first := w.store(t, "default", "the first tenant's memory", tagged("shared-tag"))
	firstB := w.store(t, "default", "another of the first tenant's memories")
	second := w.store(t, "acme", "the second tenant's memory", tagged("shared-tag"))
	secondB := w.store(t, "acme", "another of the second tenant's memories")

	w.connect(t, "default", first, firstB, graph.RelatedTo, 0.9,
		map[string]string{"why": "the first tenant's own edge"})
	w.connect(t, "acme", second, secondB, graph.Supports, 0.4,
		map[string]string{"why": "the second tenant's own edge"})
	w.event(t, "default", first, events.Recalled, "the first tenant's recall")
	w.event(t, "acme", second, events.Archived, "the second tenant's archive")

	raw, stats := exportWorld(t, w, snapshot.ExportOpts{})
	if stats.Tenants != 2 || stats.Records != 4 || stats.Edges != 2 || stats.Events != 2 {
		t.Fatalf("the export counted %+v; want 2 tenants, 4 records, 2 edges, 2 events", stats)
	}

	d := newDestination(t, fakeEmbedder())
	rep := d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})
	if rep.Stats.Tenants != 2 || rep.Stats.Records != 4 || rep.Stats.Edges != 2 || rep.Stats.Events != 2 {
		t.Fatalf("the import counted %+v; want 2 tenants, 4 records, 2 edges, 2 events", rep.Stats)
	}

	// The directory rows come back with the identity and the metadata they had.
	for _, want := range []struct {
		id   tenant.ID
		name string
	}{{"default", "The first deployment"}, {"acme", "Acme Corp"}} {
		m, err := d.dst.Tenants.Get(context.Background(), want.id)
		if err != nil {
			t.Fatalf("tenant %q is not in the imported directory: %v", want.id, err)
		}
		if m.DisplayName != want.name {
			t.Fatalf("tenant %q imported as %q, want %q", want.id, m.DisplayName, want.name)
		}
		if m.SchemaVersion != tenant.DefaultSchemaVersion {
			t.Fatalf("tenant %q imported at schema version %d, want %d",
				want.id, m.SchemaVersion, tenant.DefaultSchemaVersion)
		}
	}

	// Each tenant's records are readable as that tenant and as no other.
	for _, tc := range []struct {
		owner, other tenant.ID
		rid          id.ID
	}{
		{"default", "acme", first},
		{"default", "acme", firstB},
		{"acme", "default", second},
		{"acme", "default", secondB},
	} {
		own := tenant.NewContext(context.Background(), tc.owner)
		if _, err := d.repo.Get(own, tc.rid); err != nil {
			t.Fatalf("%s's memory %s is missing after the import: %v", tc.owner, tc.rid, err)
		}
		foreign := tenant.NewContext(context.Background(), tc.other)
		if _, err := d.repo.Get(foreign, tc.rid); !errs.Is(err, errs.NotFound) {
			t.Fatalf("%s's memory %s is readable as %s: %v", tc.owner, tc.rid, tc.other, err)
		}
	}

	// Every derived space was rebuilt under each tenant separately, rather than
	// once under whichever tenant the import happened to see first.
	for _, tid := range []tenant.ID{"default", "acme"} {
		for _, space := range []keys.Space{
			keys.SpaceEdgeIn, keys.SpaceAttrRow, keys.SpaceAttrIndex, keys.SpaceText,
		} {
			lower, upper := keys.SpaceRange(tid, tenant.DefaultNamespace, space)
			it := d.kv.NewIterator(lower, upper)
			empty := !it.First()
			_ = it.Close()
			if empty {
				t.Fatalf("the import built no %s rows for tenant %q", space, tid)
			}
		}
	}

	// The edges, with the metadata the shared schema has no field for, under
	// the tenant that owned them.
	edges := graph.NewService(d.kv, clock.NewFake(clock.FakeStart))
	for _, tc := range []struct {
		owner, other tenant.ID
		from, to     id.ID
		typ          graph.RelationshipType
		strength     float32
		why          string
	}{
		{"default", "acme", first, firstB, graph.RelatedTo, 0.9, "the first tenant's own edge"},
		{"acme", "default", second, secondB, graph.Supports, 0.4, "the second tenant's own edge"},
	} {
		own := tenant.NewContext(context.Background(), tc.owner)
		e, err := edges.Get(own, tc.from, tc.to, tc.typ)
		if err != nil {
			t.Fatalf("%s's edge is missing after the import: %v", tc.owner, err)
		}
		if e.Strength != tc.strength {
			t.Fatalf("%s's edge imported at strength %v, want %v", tc.owner, e.Strength, tc.strength)
		}
		if e.Meta["why"] != tc.why {
			t.Fatalf("%s's edge metadata is %v, want why=%q", tc.owner, e.Meta, tc.why)
		}
		foreign := tenant.NewContext(context.Background(), tc.other)
		if _, err := edges.Get(foreign, tc.from, tc.to, tc.typ); !errs.Is(err, errs.NotFound) {
			t.Fatalf("%s's edge is readable as %s: %v", tc.owner, tc.other, err)
		}
	}

	// And the lifecycle audit stream, which is what makes a migrated memory able
	// to explain itself.
	for _, tc := range []struct {
		owner, other tenant.ID
		subject      id.ID
		kind         events.Kind
		reason       string
	}{
		{"default", "acme", first, events.Recalled, "the first tenant's recall"},
		{"acme", "default", second, events.Archived, "the second tenant's archive"},
	} {
		own := tenant.NewContext(context.Background(), tc.owner)
		got, err := d.dst.Events.History(own, d.kv, events.Query{
			Tenant: tc.owner, Namespace: tenant.DefaultNamespace, Subject: tc.subject, Limit: 10,
		})
		must(t, err)
		if len(got) != 1 {
			t.Fatalf("%s's memory has %d events after the import, want 1", tc.owner, len(got))
		}
		if got[0].Kind != tc.kind || got[0].Reason != tc.reason || got[0].Actor != "test" {
			t.Fatalf("%s's event imported as %+v", tc.owner, got[0])
		}

		foreign := tenant.NewContext(context.Background(), tc.other)
		other, err := d.dst.Events.History(foreign, d.kv, events.Query{
			Tenant: tc.other, Namespace: tenant.DefaultNamespace, Subject: tc.subject, Limit: 10,
		})
		must(t, err)
		if len(other) != 0 {
			t.Fatalf("%s's events are readable as %s: %+v", tc.owner, tc.other, other)
		}
	}
}

// An export of one tenant out of several is the per-tenant migration step, and
// what makes it safe is that the file names only that tenant: an operator
// migrating acme into its own deployment must not carry another tenant's rows
// along with it.
func TestAPerTenantExportCarriesOnlyThatTenant(t *testing.T) {
	w := newWorld(t)
	w.tenantNamed(t, "default", "The first deployment")
	w.tenantNamed(t, "acme", "Acme Corp")
	stayed := w.store(t, "default", "the first tenant's memory")
	moved := w.store(t, "acme", "the second tenant's memory")
	w.event(t, "acme", moved, events.Recalled, "a recall that moves with it")

	raw, stats := exportWorld(t, w, snapshot.ExportOpts{Tenant: "acme"})
	if stats.Tenants != 1 || stats.Records != 1 || stats.Events != 1 {
		t.Fatalf("a per-tenant export counted %+v; want 1 tenant, 1 record, 1 event", stats)
	}

	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})

	if _, err := d.dst.Tenants.Get(context.Background(), "acme"); err != nil {
		t.Fatalf("the migrated tenant is not in the destination directory: %v", err)
	}
	if _, err := d.dst.Tenants.Get(context.Background(), "default"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("a tenant that was not exported is in the destination directory: %v", err)
	}
	acme := tenant.NewContext(context.Background(), "acme")
	if _, err := d.repo.Get(acme, moved); err != nil {
		t.Fatalf("the migrated memory is missing: %v", err)
	}
	def := tenant.NewContext(context.Background(), "default")
	if _, err := d.repo.Get(def, stayed); !errs.Is(err, errs.NotFound) {
		t.Fatalf("a memory belonging to a tenant that was not exported arrived anyway: %v", err)
	}
}
