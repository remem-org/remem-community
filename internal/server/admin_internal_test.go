package server

import (
	"context"
	"slices"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TestTheAdminProviderReportsEveryDerivedSpace: one status per derived key
// space, in the order keys.AllSpaces lists them, so a derived space added later
// appears here without a handler change. The two indexes with a health signal
// report it; the three without one say so rather than claiming to be ok.
func TestTheAdminProviderReportsEveryDerivedSpace(t *testing.T) {
	d := mustDepsWith(t, func(c *config.Config) { c.Vector.Index = "hnsw" })
	const tid = tenant.ID("acme")
	seedMemories(t, d, tid)
	a := adminProvider{d: d}

	statuses, err := a.Indexes(context.Background(), tid)
	if err != nil {
		t.Fatal(err)
	}
	var derived []string
	for _, s := range keys.AllSpaces() {
		if s.Class() == keys.Derived {
			derived = append(derived, s.String())
		}
	}
	var got []string
	for _, st := range statuses {
		got = append(got, st.Space)
	}
	if !slices.Equal(got, derived) {
		t.Fatalf("statuses for %v, want every derived space in key order: %v", got, derived)
	}

	for _, st := range statuses {
		switch st.Space {
		case "vector_index", "text":
			if st.State != remhttp.IndexOK || st.Count == nil || *st.Count <= 0 {
				t.Errorf("%s: state %q, count %v; a seeded, healthy index is ok and counts its entries",
					st.Space, st.State, st.Count)
			}
		default:
			if st.State != remhttp.IndexUnmonitored || st.Count != nil {
				t.Errorf("%s: state %q, count %v; an index with no health signal is unmonitored, not ok",
					st.Space, st.State, st.Count)
			}
		}

		space := spaceNamed(t, st.Space)
		want, _ := rebuildJobFor(space)
		if st.RebuildJob != want.String() {
			t.Errorf("%s names rebuild job %q, want %q", st.Space, st.RebuildJob, want)
		}
		types, err := a.RebuildTypes(st.Index)
		if err != nil {
			t.Errorf("%s's own index name %q is refused: %v", st.Space, st.Index, err)
			continue
		}
		if !slices.Contains(types, want) {
			t.Errorf("rebuilding index %q queues %v, which does not include %s's job %s", st.Index, types, st.Space, want)
		}
	}
}

func TestRebuildTypesNameEveryDerivedIndexOnceAndRefuseTheRest(t *testing.T) {
	a := adminProvider{d: mustDeps(t)}

	all, err := a.RebuildTypes("all")
	if err != nil {
		t.Fatal(err)
	}
	var want []jobs.Type
	for _, s := range keys.AllSpaces() {
		if typ, ok := rebuildJobFor(s); ok && !slices.Contains(want, typ) {
			want = append(want, typ)
		}
	}
	if !slices.Equal(all, want) {
		t.Fatalf("rebuilding all queues %v, want each derived index's job once: %v", all, want)
	}

	_, err = a.RebuildTypes("postings")
	if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "vector") {
		t.Fatalf("an unknown index returned %v, want Invalid naming the indexes that exist", err)
	}
}

func TestMigrationsAreListedFromTheDirectory(t *testing.T) {
	d := mustDeps(t)
	ctx := context.Background()
	if err := schema.WriteState(ctx, d.kv, schema.MigrationState{
		ID: schema.AttrBackfillID, State: schema.StateDone, Processed: 7,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := adminProvider{d: d}.Migrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != schema.AttrBackfillID || got[0].State != "done" || got[0].Processed != 7 {
		t.Fatalf("migrations listed as %+v", got)
	}
}

func spaceNamed(t *testing.T, name string) keys.Space {
	t.Helper()
	for _, s := range keys.AllSpaces() {
		if s.String() == name {
			return s
		}
	}
	t.Fatalf("no space is named %q", name)
	return 0
}
