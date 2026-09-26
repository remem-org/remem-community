package http

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// PauseRequest asks for a recurring type's schedule to be suppressed.
//
// # Why there is no "pause until I say otherwise"
//
// The expiry is required, and it is required in the request rather than
// defaulted here, because the two ways of spelling it answer different
// questions: `expires_at` is "until the maintenance window ends" and
// `duration_seconds` is "for an hour while I look at this". One of them, never
// both — a request that says two things about one deadline is a request whose
// author did not decide, and guessing which they meant is the class of mistake
// config.Load refuses by name.
type PauseRequest struct {
	// ExpiresAt is an RFC 3339 instant. It is a string rather than a
	// time.Time so that an unparseable value is a validation failure naming
	// the field, not a decode failure naming a Go type.
	ExpiresAt string `json:"expires_at,omitempty"`
	// DurationSeconds is how long from now the pause lasts.
	DurationSeconds *int64 `json:"duration_seconds,omitempty"`
}

// PauseResponse is a type's suppression as an operator sees it.
//
// It reports the state after the call rather than echoing the request, so that
// a resume and a pause answer in the same shape and a client can render one
// without knowing which it asked for.
type PauseResponse struct {
	Type   string `json:"type"`
	Tenant string `json:"tenant"`
	Paused bool   `json:"paused"`

	// PauseExpiresAt and PausedBy are present exactly while a pause is in
	// force. A resumed type reports neither, rather than reporting a past
	// instant that reads like a pause that has not lifted yet.
	PauseExpiresAt *time.Time `json:"pause_expires_at,omitempty"`
	PausedBy       string     `json:"paused_by,omitempty"`
	PausedAt       *time.Time `json:"paused_at,omitempty"`

	// MaxPauseSeconds is the longest pause this server accepts, reported so
	// that a client can bound its own control rather than discovering the
	// limit by being refused.
	MaxPauseSeconds float64 `json:"max_pause_seconds"`
}

// pauseJobType suppresses a recurring type's scheduled enqueueing.
//
// It does not touch work already queued or running — that is
// POST /admin/jobs/{id}/cancel, which names the job — and it does not stop a
// manual run. An operator who has paused a schedule and then asks for one run
// has said two compatible things.
func (d Deps) pauseJobType(w http.ResponseWriter, r *http.Request) {
	if !d.jobsAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}
	typ, ok := d.recurringType(w, r)
	if !ok {
		return
	}

	var body PauseRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	until, err := body.until(d.now())
	if err != nil {
		WriteError(w, r, err)
		return
	}

	p, err := d.Jobs.Queue.Pause(r.Context(), t, typ, until, operatorOf(r))
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, pauseResponse(t, typ, p))
}

// resumeJobType lifts a pause.
//
// Resuming a type that is not paused is a 200 and not a 404: the
// post-condition an operator asked for — this type is scheduled again — holds
// either way, and a pause that expired while the console was open should not
// answer the resume button with an error.
func (d Deps) resumeJobType(w http.ResponseWriter, r *http.Request) {
	if !d.jobsAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}
	typ, ok := d.recurringType(w, r)
	if !ok {
		return
	}
	if err := d.Jobs.Queue.Resume(r.Context(), t, typ); err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, pauseResponse(t, typ, nil))
}

// until resolves the request's two spellings into one instant.
func (b PauseRequest) until(now time.Time) (time.Time, error) {
	const op = "http.pauseJobType"

	at := strings.TrimSpace(b.ExpiresAt)
	switch {
	case at == "" && b.DurationSeconds == nil:
		return time.Time{}, errs.E(errs.Invalid, op, errors.New(
			"a pause needs an expiry: send expires_at as an RFC 3339 instant, or duration_seconds; "+
				"a suppression that never lifts outlives the memory of setting it"))
	case at != "" && b.DurationSeconds != nil:
		return time.Time{}, errs.E(errs.Invalid, op, errors.New(
			"send expires_at or duration_seconds, not both; two answers to one deadline is a request whose author did not decide"))
	case at != "":
		parsed, err := time.Parse(time.RFC3339, at)
		if err != nil {
			return time.Time{}, errs.E(errs.Invalid, op, fmt.Errorf(
				"expires_at %q is not an RFC 3339 instant, such as %s",
				at, now.Add(time.Hour).UTC().Format(time.RFC3339)))
		}
		return parsed, nil
	default:
		return now.Add(time.Duration(*b.DurationSeconds) * time.Second), nil
	}
}

// recurringType resolves the {type} path segment to a registered recurring
// type.
//
// A type nothing schedules is refused rather than paused. Pausing it would
// report success and change nothing — the schedule it suppresses does not
// exist — and an operator acting on that answer would believe work had stopped
// that was never running on a timer in the first place.
func (d Deps) recurringType(w http.ResponseWriter, r *http.Request) (jobs.Type, bool) {
	typ, err := jobs.ParseType(strings.TrimSpace(r.PathValue("type")))
	if err != nil {
		WriteError(w, r, err)
		return "", false
	}
	entry, known := d.Jobs.Registry.Lookup(typ)
	if !known {
		WriteError(w, r, errs.E(errs.NotFound, "http.jobPause", errors.New(
			"this binary has no handler for that job type; GET /api/v1/admin/jobs/types lists the ones it has")))
		return "", false
	}
	if entry.Every <= 0 {
		WriteError(w, r, errs.E(errs.Invalid, "http.jobPause", fmt.Errorf(
			"job type %s is not recurring, so it has no schedule to pause; "+
				"it runs when something enqueues it, and an outstanding one is stopped with "+
				"POST /api/v1/admin/jobs/{id}/cancel", typ)))
		return "", false
	}
	return typ, true
}

// operatorOf names the credential that asked, for the audit the status surface
// shows. It is the credential id and never a secret.
func operatorOf(r *http.Request) string {
	if p, ok := auth.FromContext(r.Context()); ok {
		return p.ID
	}
	return "unknown"
}

func pauseResponse(t tenant.ID, typ jobs.Type, p *jobs.Pause) PauseResponse {
	out := PauseResponse{
		Type:            typ.String(),
		Tenant:          string(t),
		MaxPauseSeconds: jobs.MaxPauseDuration.Seconds(),
	}
	if p != nil {
		expires, set := p.ExpiresAt, p.SetAt
		out.Paused = true
		out.PauseExpiresAt = &expires
		out.PausedBy = p.SetBy
		out.PausedAt = &set
	}
	return out
}

// now is the router's clock, so that a duration-shaped pause is measured
// against the same clock the scheduler compares the expiry with.
func (d Deps) now() time.Time {
	if d.Clock == nil {
		return time.Now().UTC()
	}
	return d.Clock.Now().UTC()
}
