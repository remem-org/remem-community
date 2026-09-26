package http

import (
	"net/http"
	"time"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/jobs"
)

// JobRunResponse is one execution attempt as an operator sees it.
//
// # Why the counts are pointers
//
// A handler that reported nothing leaves them absent, and absent is not zero.
// "This attempt changed nothing" and "nobody counted" are different answers,
// and a zero standing in for the second is a number an operator will act on.
// Nothing here is inferred from a checkpoint, an elapsed time or the size of an
// index afterwards.
type JobRunResponse struct {
	ID     string `json:"id"`
	JobID  string `json:"job_id"`
	Tenant string `json:"tenant"`
	Type   string `json:"type"`

	// Attempt is the run number, counting from one. Several attempts of one
	// job share a job_id and differ here.
	Attempt uint32 `json:"attempt"`

	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	// DurationMs is derived from the two above and reported because every
	// client would otherwise derive it, and half of them differently.
	DurationMs int64 `json:"duration_ms"`

	// Outcome is what became of this attempt: completed, retry, failed or
	// cancelled. A retry is an attempt that failed with attempts left, so it
	// is an outcome of the attempt rather than of the job.
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
	Owner   string `json:"owner,omitempty"`

	RecordsProcessed *uint64 `json:"records_processed"`
	RecordsChanged   *uint64 `json:"records_changed"`
}

// JobHistoryResponse envelopes a history page.
type JobHistoryResponse struct {
	Runs       []JobRunResponse `json:"runs"`
	NextCursor string           `json:"next_cursor,omitempty"`
	HasMore    bool             `json:"has_more"`

	// RetentionSeconds is the window that bounds this history, reported so
	// that "there are no older runs" and "this server does not keep them that
	// long" stay distinguishable. A client that does not know the window
	// cannot tell an empty history from a short one.
	//
	// It is omitted rather than sent as zero by a process that was built
	// without one, for the reason the counts above are pointers: a zero
	// retention would read as "nothing is kept", which is a different claim
	// from "this server did not say".
	RetentionSeconds float64 `json:"retention_seconds,omitempty"`
}

// listJobHistory shows what a tenant's jobs have actually done.
//
// It is the question the job listing cannot answer. A job row aggregates an
// attempt count and a terminal state; it cannot say what each retry did, and
// at-least-once delivery makes a retry an ordinary event rather than an
// exceptional one.
//
// `type` may be repeated to widen the filter and `limit` bounds the page,
// newest first — an operator opening a history wants the last thing that
// happened.
func (d Deps) listJobHistory(w http.ResponseWriter, r *http.Request) {
	if !d.jobsAvailable(w, r) || !d.crossTenant(w, r) {
		return
	}
	t, ok := d.jobTenant(w, r)
	if !ok {
		return
	}

	filter := jobs.RunFilter{}
	for _, name := range r.URL.Query()["type"] {
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
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		key, err := codec.DecodeToken(cursor, codec.TokenHistory, t)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		filter.BeforeKey = key
	}

	page, err := d.Jobs.Queue.RunsPage(r.Context(), t, filter)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	out := JobHistoryResponse{
		Runs:             make([]JobRunResponse, len(page.Runs)),
		HasMore:          page.HasMore,
		RetentionSeconds: d.Jobs.Retention.Seconds(),
	}
	if page.HasMore && len(page.NextKey) > 0 {
		out.NextCursor = codec.EncodeToken(codec.TokenHistory, t, page.NextKey)
	}
	for i, run := range page.Runs {
		out.Runs[i] = jobRunResponse(run)
	}
	writeJSON(w, r, http.StatusOK, out)
}

func jobRunResponse(r *jobs.Run) JobRunResponse {
	return JobRunResponse{
		ID:               r.ID.String(),
		JobID:            r.JobID.String(),
		Tenant:           string(r.Tenant),
		Type:             r.Type.String(),
		Attempt:          r.Attempt,
		StartedAt:        r.StartedAt,
		FinishedAt:       r.FinishedAt,
		DurationMs:       r.Duration().Milliseconds(),
		Outcome:          r.Outcome.String(),
		Error:            r.Error,
		Owner:            r.Owner,
		RecordsProcessed: r.RecordsProcessed,
		RecordsChanged:   r.RecordsChanged,
	}
}
