package http

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// JobResponse is one job as an operator sees it.
//
// # What is deliberately not here
//
// The payload and the checkpoint are reported as byte counts and never as
// content. A payload is handler-defined and a checkpoint is a cursor into
// somebody's corpus; neither is something an administrative listing should
// stream back, and the rule that memory content stays out of logs is worth
// keeping out of adjacent surfaces too. A byte count answers the question an
// operator actually has, which is whether the row is the size it should be.
type JobResponse struct {
	ID     string `json:"id"`
	Tenant string `json:"tenant"`
	Type   string `json:"type"`
	State  string `json:"state"`

	Attempts    uint32 `json:"attempts"`
	MaxAttempts uint32 `json:"max_attempts"`
	Priority    int8   `json:"priority"`

	// RunAt is when the job becomes claimable. For a job waiting to retry it is
	// the end of its backoff, which is the field that answers "why is this not
	// running".
	RunAt time.Time `json:"run_at"`

	// Owner and LeaseExpiresAt are present while the job is running.
	Owner          string     `json:"owner,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`

	// LastError is why the most recent attempt did not succeed. It survives
	// into a retry on purpose, so a running job can show it has been here
	// before.
	LastError string `json:"last_error,omitempty"`

	PayloadBytes    int `json:"payload_bytes,omitempty"`
	CheckpointBytes int `json:"checkpoint_bytes,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// JobsResponse envelopes a listing.
type JobsResponse struct {
	Jobs []JobResponse `json:"jobs"`
}

// JobTypeResponse is one registered job type.
type JobTypeResponse struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	MaxAttempts uint32 `json:"max_attempts"`
	Priority    int8   `json:"priority"`
	// EverySeconds is the recurring interval, absent for a type that only runs
	// when something enqueues it.
	EverySeconds float64 `json:"every_seconds,omitempty"`

	// Paused reports whether an operator has suppressed this type's schedule
	// for this tenant. This listing is the status surface a pause has to be
	// visible in: a suppression nothing reports is the failure the expiry and
	// these two fields exist to prevent.
	Paused bool `json:"paused"`
	// PauseExpiresAt and PausedBy are present exactly while a pause is in
	// force. An expired pause reports neither, because it is not one.
	PauseExpiresAt *time.Time `json:"pause_expires_at,omitempty"`
	PausedBy       string     `json:"paused_by,omitempty"`
}

// JobTypesResponse envelopes the registry.
type JobTypesResponse struct {
	Types []JobTypeResponse `json:"types"`
}

// listJobs shows a tenant's background work.
//
// `state` and `type` may each be repeated to widen the filter, and `state`
// selects which key partitions are read — so a listing of what is waiting does
// not walk the audit trail to find it.
func (d Deps) listJobs(w http.ResponseWriter, r *http.Request) {
	if !d.jobsAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}

	q := r.URL.Query()
	filter := jobs.Filter{}
	for _, name := range q["state"] {
		state, err := jobs.ParseState(name)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		filter.States = append(filter.States, state)
	}
	for _, name := range q["type"] {
		typ, err := jobs.ParseType(name)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		filter.Types = append(filter.Types, typ)
	}
	limit, valid := intQuery(w, r, "limit")
	if !valid {
		return
	}
	filter.Limit = limit

	list, err := d.Jobs.Queue.List(r.Context(), t, filter)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	out := JobsResponse{Jobs: make([]JobResponse, len(list))}
	for i, j := range list {
		out.Jobs[i] = jobResponse(j)
	}
	writeJSON(w, r, http.StatusOK, out)
}

// getJob shows one job.
func (d Deps) getJob(w http.ResponseWriter, r *http.Request) {
	if !d.jobsAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}
	jid, ok := jobID(w, r)
	if !ok {
		return
	}
	j, err := d.Jobs.Queue.Get(r.Context(), t, jid)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, jobResponse(j))
}

// listJobTypes shows what this binary can run.
//
// It is the introspection the admin surface needs to be usable at all: without
// it, "which types may I run" is answered by reading the source, and a
// deployment running an older binary has no way to say what it is missing.
func (d Deps) listJobTypes(w http.ResponseWriter, r *http.Request) {
	if !d.jobsAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}
	// One scan of the tenant's pauses rather than a read per type: the
	// listing is the status surface, and a surface that costs a lookup per row
	// is one that gets slower as the registry grows.
	paused, err := d.Jobs.Queue.Pauses(r.Context(), t)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	inForce := make(map[jobs.Type]*jobs.Pause, len(paused))
	for _, p := range paused {
		inForce[p.Type] = p
	}

	entries := d.Jobs.Registry.Entries()
	out := JobTypesResponse{Types: make([]JobTypeResponse, len(entries))}
	for i, e := range entries {
		out.Types[i] = JobTypeResponse{
			Type:        e.Type.String(),
			Description: e.Description,
			MaxAttempts: e.MaxAttempts,
			Priority:    e.Priority,
		}
		if e.Every > 0 {
			out.Types[i].EverySeconds = e.Every.Seconds()
		}
		if p, stopped := inForce[e.Type]; stopped {
			expires := p.ExpiresAt
			out.Types[i].Paused = true
			out.Types[i].PauseExpiresAt = &expires
			out.Types[i].PausedBy = p.SetBy
		}
	}
	writeJSON(w, r, http.StatusOK, out)
}

// cancelJob stops a job.
//
// A running job is cancelled where it stands: the row moves and the worker
// holding it finds out at its next lease renewal. A job that has already
// finished is a 409 rather than a 200, because "cancelled" and "completed
// twenty minutes ago" are different answers.
func (d Deps) cancelJob(w http.ResponseWriter, r *http.Request) {
	if !d.jobsAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}
	jid, ok := jobID(w, r)
	if !ok {
		return
	}
	var err error
	if d.Jobs.Pool != nil {
		err = d.Jobs.Pool.Cancel(r.Context(), t, jid)
	} else {
		err = d.Jobs.Queue.Cancel(r.Context(), t, jid)
	}
	if err != nil {
		WriteError(w, r, err)
		return
	}
	j, err := d.Jobs.Queue.Get(r.Context(), t, jid)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, jobResponse(j))
}

// runJob enqueues one job of a type, now.
//
// It is how an operator triggers a rebuild without stopping the server, which
// is the thing `remem-admin` cannot do — that command needs the data directory,
// and the directory is held under Pebble's exclusive lock while the server runs.
//
// A type that already has a job outstanding for this tenant is a 409. Queueing
// a second rebuild of one index behind the first spends the work twice and
// makes the first one's completion ambiguous.
func (d Deps) runJob(w http.ResponseWriter, r *http.Request) {
	if !d.jobsAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}

	typ, err := jobs.ParseType(strings.TrimSpace(r.PathValue("type")))
	if err != nil {
		WriteError(w, r, err)
		return
	}
	entry, known := d.Jobs.Registry.Lookup(typ)
	if !known {
		WriteError(w, r, errs.E(errs.NotFound, "http.runJob", errors.New(
			"this binary has no handler for that job type; GET /api/v1/admin/jobs/types lists the ones it has")))
		return
	}

	outstanding, err := d.Jobs.Queue.Outstanding(r.Context(), t, typ)
	switch {
	case err == nil:
		// The refusal names the job, because "something of this type is already
		// outstanding" otherwise sends an operator to a listing to find out
		// which one.
		WriteError(w, r, errs.E(errs.Conflict, "http.runJob", fmt.Errorf(
			"job %s of this type is already %s for this tenant; "+
				"queueing a second one spends the work twice and makes the first one's completion ambiguous",
			outstanding.ID, outstanding.State)))
		return
	case !errs.Is(err, errs.NotFound):
		WriteError(w, r, err)
		return
	}

	j := &jobs.Job{
		Tenant: t, Namespace: tenant.DefaultNamespace, Type: typ,
		MaxAttempts: entry.MaxAttempts, Priority: entry.Priority,
	}
	if err := d.Jobs.Queue.Submit(r.Context(), j); err != nil {
		WriteError(w, r, err)
		return
	}
	// 202: the job is queued, not done. Reporting 201 would suggest the work
	// has happened, which for a rebuild is minutes away.
	writeJSON(w, r, http.StatusAccepted, jobResponse(j))
}

// jobsAvailable refuses when the process was built without the job framework.
func (d Deps) jobsAvailable(w http.ResponseWriter, r *http.Request) bool {
	if d.Jobs.Queue != nil && d.Jobs.Registry != nil {
		return true
	}
	WriteError(w, r, errs.E(errs.Unavailable, "http.admin", errors.New(
		"this process is not running the background job framework")))
	return false
}

// jobTenant is which tenant's jobs an operator is asking about.
//
// It is the resolved request tenant — the default, or whatever X-Remem-Tenant
// named — so the administration surface is scoped exactly like everything else.
// There is no "all tenants" listing, for the reason Invariant 1 exists: once an
// unscoped variant is available, something calls it.
func (d Deps) jobTenant(w http.ResponseWriter, r *http.Request) (tenant.ID, bool) {
	t, err := tenant.Require(r.Context())
	if err != nil {
		WriteError(w, r, err)
		return "", false
	}
	return t, true
}

func jobID(w http.ResponseWriter, r *http.Request) (id.ID, bool) {
	jid, err := id.Parse(r.PathValue("id"))
	if err != nil {
		WriteError(w, r, err)
		return id.Zero, false
	}
	return jid, true
}

func jobResponse(j *jobs.Job) JobResponse {
	out := JobResponse{
		ID:              j.ID.String(),
		Tenant:          string(j.Tenant),
		Type:            j.Type.String(),
		State:           j.State.String(),
		Attempts:        j.Attempts,
		MaxAttempts:     j.MaxAttempts,
		Priority:        j.Priority,
		RunAt:           j.RunAt,
		LastError:       j.LastError,
		PayloadBytes:    len(j.Payload),
		CheckpointBytes: len(j.Checkpoint),
		CreatedAt:       j.CreatedAt,
		UpdatedAt:       j.UpdatedAt,
	}
	if j.Lease != nil {
		expires := j.Lease.ExpiresAt
		out.Owner = j.Lease.Owner
		out.LeaseExpiresAt = &expires
	}
	return out
}
