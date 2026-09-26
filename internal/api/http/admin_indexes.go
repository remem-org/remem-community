package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// The index states an operator can see.
const (
	// IndexOK is an index whose own health signal says it can answer completely.
	IndexOK = "ok"
	// IndexDegraded is an index answering, and saying its answers may be
	// incomplete.
	IndexDegraded = "degraded"
	// IndexRebuilding is an index a rebuild is in progress on, or was
	// interrupted on.
	IndexRebuilding = "rebuilding"
	// IndexUnmonitored is an index with no health signal of its own. It is not
	// reported as ok: claiming a health nobody checked is the reassuring lie
	// vector.Index.Health exists to avoid. inspect check is how it is examined.
	IndexUnmonitored = "unmonitored"
)

// IndexStatus is one derived index, for one tenant, as an operator sees it.
type IndexStatus struct {
	// Space is the key space the index occupies, by its stable name.
	Space string `json:"space"`
	// Index is the name POST /api/v1/admin/rebuild and
	// `remem-admin rebuild --index` take for it.
	Index string `json:"index"`
	// RebuildJob is the job type that rebuilds it on a running server.
	RebuildJob string `json:"rebuild_job"`
	// State is ok, degraded, rebuilding or unmonitored.
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
	// Count is how many entries the index holds, where it keeps that number.
	Count *int64 `json:"count,omitempty"`
}

// IndexesResponse lists a tenant's derived indexes.
type IndexesResponse struct {
	Tenant  string        `json:"tenant"`
	Indexes []IndexStatus `json:"indexes"`
}

// RebuildRequest names the index to rebuild, or "all".
type RebuildRequest struct {
	Index string `json:"index"`
}

// RebuildResponse is what a rebuild request queued. A job type already waiting
// or running for the tenant is reported rather than queued a second time.
type RebuildResponse struct {
	Queued             []JobResponse `json:"queued"`
	AlreadyOutstanding []string      `json:"already_outstanding,omitempty"`
}

// MigrationStatus is one migration's durable state row.
type MigrationStatus struct {
	ID             string            `json:"id"`
	State          string            `json:"state"`
	Processed      uint64            `json:"processed"`
	Source         map[string]uint32 `json:"source,omitempty"`
	Target         map[string]uint32 `json:"target,omitempty"`
	StartedAt      time.Time         `json:"started_at"`
	LastProgressAt time.Time         `json:"last_progress_at"`
	Error          string            `json:"error,omitempty"`
}

// MigrationsResponse lists every migration this directory has a row for.
type MigrationsResponse struct {
	Migrations []MigrationStatus `json:"migrations"`
}

// AdminDeps is what the index and migration routes read. The composition root
// implements it, because only it holds the indexes, the store, and the mapping
// from a derived space to the job that rebuilds it.
type AdminDeps interface {
	// Indexes reports every derived index for one tenant.
	Indexes(ctx context.Context, t tenant.ID) ([]IndexStatus, error)
	// RebuildTypes names the job types that rebuild index, or every derived
	// index for "all", without duplicates. An unknown name is errs.Invalid and
	// says which names exist.
	RebuildTypes(index string) ([]jobs.Type, error)
	// Migrations lists the migration state rows. They are untenanted.
	Migrations(ctx context.Context) ([]MigrationStatus, error)
}

// adminAvailable refuses when the process has no index administration, so
// "not built" and "wrong URL" stay distinguishable.
func (d Deps) adminAvailable(w http.ResponseWriter, r *http.Request) bool {
	if d.Admin != nil {
		return true
	}
	WriteError(w, r, errs.E(errs.Unavailable, "http.admin", errors.New(
		"this process has no index administration")))
	return false
}

// listIndexes reports the health of every derived index for the tenant the
// request resolved to.
func (d Deps) listIndexes(w http.ResponseWriter, r *http.Request) {
	if !d.adminAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}
	statuses, err := d.Admin.Indexes(r.Context(), t)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	if statuses == nil {
		statuses = []IndexStatus{}
	}
	writeJSON(w, r, http.StatusOK, IndexesResponse{Tenant: string(t), Indexes: statuses})
}

// rebuildIndexes queues the jobs that rebuild an index, or every derived index.
//
// It queues what it can. A job type already waiting or running for the tenant
// is reported and not queued a second time — the same rule runJob applies — and
// is not a reason to refuse the others, because an operator asking for "all"
// while one rebuild is under way wants the rest, not an error. Only a request
// that could queue nothing at all is a 409.
func (d Deps) rebuildIndexes(w http.ResponseWriter, r *http.Request) {
	const op = "http.rebuildIndexes"
	if !d.jobsAvailable(w, r) || !d.adminAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}
	var body RebuildRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	index := strings.TrimSpace(body.Index)
	types, err := d.Admin.RebuildTypes(index)
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := RebuildResponse{Queued: []JobResponse{}}
	for _, typ := range types {
		entry, known := d.Jobs.Registry.Lookup(typ)
		if !known {
			WriteError(w, r, errs.E(errs.Unavailable, op, fmt.Errorf(
				"index %q is rebuilt by job type %s, which this process has not registered", index, typ)))
			return
		}
		outstanding, err := d.Jobs.Queue.HasOutstanding(r.Context(), t, typ)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		if outstanding {
			out.AlreadyOutstanding = append(out.AlreadyOutstanding, typ.String())
			continue
		}
		j := &jobs.Job{
			Tenant: t, Namespace: tenant.DefaultNamespace, Type: typ,
			MaxAttempts: entry.MaxAttempts, Priority: entry.Priority,
		}
		if err := d.Jobs.Queue.Submit(r.Context(), j); err != nil {
			WriteError(w, r, err)
			return
		}
		out.Queued = append(out.Queued, jobResponse(j))
	}

	if len(out.Queued) == 0 && len(out.AlreadyOutstanding) > 0 {
		WriteError(w, r, errs.E(errs.Conflict, op, fmt.Errorf(
			"every job that rebuilds %q is already waiting or running for this tenant: %s",
			index, strings.Join(out.AlreadyOutstanding, ", "))))
		return
	}
	// 202, as for runJob: queued, not done.
	writeJSON(w, r, http.StatusAccepted, out)
}

// listMigrations lists every migration this directory has a state row for.
func (d Deps) listMigrations(w http.ResponseWriter, r *http.Request) {
	if !d.adminAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	states, err := d.Admin.Migrations(r.Context())
	if err != nil {
		WriteError(w, r, err)
		return
	}
	if states == nil {
		states = []MigrationStatus{}
	}
	writeJSON(w, r, http.StatusOK, MigrationsResponse{Migrations: states})
}
