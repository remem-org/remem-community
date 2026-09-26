package server

import (
	"context"
	"fmt"
	"slices"
	"strings"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
)

// adminProvider answers the index and migration administration routes.
//
// It lives in the composition root because it is the one place that holds the
// indexes, the store, and the mapping from a derived key space to the job that
// rebuilds it; the delivery layer renders what it reports and decides nothing.
type adminProvider struct{ d *deps }

var _ remhttp.AdminDeps = adminProvider{}

// indexNameFor is the name an operator uses for the index a derived space
// holds. It is the vocabulary of `remem-admin rebuild --index` as well, which
// keeps its own copy (spaceIndexName) because a command-line binary does not
// import the server; TestEveryDerivedSpaceHasARebuildCommand and this package's
// admin tests hold both copies to keys.AllSpaces.
func indexNameFor(s keys.Space) string {
	switch s {
	case keys.SpaceVectorIndex:
		return "vector"
	case keys.SpaceText:
		return "text"
	case keys.SpaceAttrRow, keys.SpaceAttrIndex:
		return "attr"
	case keys.SpaceEdgeIn:
		return "graph-in"
	}
	return ""
}

// Indexes reports one status per derived space, in key order, so a derived
// space added later appears without anyone touching this function's callers.
//
// Only the vector and text indexes keep a health signal. The attribute rows,
// the attribute index and the in-edges do not, and they are reported as
// unmonitored rather than ok: a health nobody checked is not a health.
func (a adminProvider) Indexes(ctx context.Context, t tenant.ID) ([]remhttp.IndexStatus, error) {
	var out []remhttp.IndexStatus
	for _, s := range keys.AllSpaces() {
		if s.Class() != keys.Derived {
			continue
		}
		typ, _ := rebuildJobFor(s)
		st := remhttp.IndexStatus{
			Space: s.String(), Index: indexNameFor(s), RebuildJob: typ.String(),
			State: remhttp.IndexUnmonitored,
		}
		var err error
		switch s {
		case keys.SpaceVectorIndex:
			err = a.vectorStatus(ctx, t, &st)
		case keys.SpaceText:
			err = a.textStatus(ctx, t, &st)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

func (a adminProvider) vectorStatus(ctx context.Context, t tenant.ID, st *remhttp.IndexStatus) error {
	if a.d.vectors == nil {
		return nil
	}
	stats, err := a.d.vectors.Stats(ctx, t)
	if err != nil {
		return err
	}
	health, err := a.d.vectors.Health(ctx, t)
	if err != nil {
		return err
	}
	n := int64(stats.Vectors)
	st.Count = &n
	switch {
	case stats.Rebuilding:
		st.State, st.Reason = remhttp.IndexRebuilding, health.Reason
	case health.Degraded:
		st.State, st.Reason = remhttp.IndexDegraded, health.Reason
	default:
		st.State = remhttp.IndexOK
	}
	return nil
}

func (a adminProvider) textStatus(ctx context.Context, t tenant.ID, st *remhttp.IndexStatus) error {
	if a.d.texts == nil {
		return nil
	}
	ns := tenant.DefaultNamespace
	stats, err := text.ReadStats(ctx, a.d.kv, t, ns)
	if err != nil && !errs.Is(err, errs.NotFound) {
		return err
	}
	health, err := a.d.texts.Health(ctx, a.d.kv, t, ns)
	if err != nil && !errs.Is(err, errs.NotFound) {
		return err
	}
	n := int64(stats.Documents)
	st.Count = &n
	// The keyword index's only degraded state is its rebuild marker: set while a
	// rebuild runs, and left set by one that was interrupted.
	if health.Degraded {
		st.State, st.Reason = remhttp.IndexRebuilding, health.Reason
	} else {
		st.State = remhttp.IndexOK
	}
	return nil
}

// RebuildTypes maps an index name, or "all", to the job types that rebuild it,
// each once.
func (a adminProvider) RebuildTypes(index string) ([]jobs.Type, error) {
	var out []jobs.Type
	var names []string
	for _, s := range keys.AllSpaces() {
		typ, ok := rebuildJobFor(s)
		if !ok {
			continue
		}
		name := indexNameFor(s)
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
		if (index == "all" || index == name) && !slices.Contains(out, typ) {
			out = append(out, typ)
		}
	}
	if len(out) == 0 {
		return nil, errs.E(errs.Invalid, "server.RebuildTypes", fmt.Errorf(
			"%q is not a derived index; the indexes are %s, or all", index, strings.Join(names, ", ")))
	}
	return out, nil
}

// Migrations lists the directory's migration state rows.
func (a adminProvider) Migrations(ctx context.Context) ([]remhttp.MigrationStatus, error) {
	states, err := schema.ListStates(ctx, a.d.kv)
	if err != nil {
		return nil, err
	}
	out := make([]remhttp.MigrationStatus, 0, len(states))
	for _, s := range states {
		out = append(out, remhttp.MigrationStatus{
			ID: s.ID, State: s.State.String(), Processed: s.Processed,
			Source: s.Source, Target: s.Target,
			StartedAt: s.StartedAt, LastProgressAt: s.LastProgressAt, Error: s.Error,
		})
	}
	return out, nil
}
