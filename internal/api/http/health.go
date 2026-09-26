package http

import (
	"net/http"

	"github.com/remem-org/remem-go/internal/version"
)

// HealthResponse says the process is alive.
type HealthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	// Edition is community or business, as the binary was built.
	Edition string `json:"edition"`
	// Tenancy is the tenant policy this server was composed with:
	// "single-tenant" or "identity". A client that wants to know whether it may
	// name a tenant asks here rather than by trying and reading a 403.
	Tenancy string `json:"tenancy"`
}

// ReadyResponse says whether the process may serve traffic.
type ReadyResponse struct {
	Status string `json:"status"`
	// Reason is present only when not ready, and is what an operator reads
	// first during an incident.
	Reason string `json:"reason,omitempty"`
}

// health is liveness: this process is running and its HTTP loop is answering.
//
// It deliberately checks nothing else. A liveness probe that consulted the
// store would restart a healthy process whenever the store was briefly slow,
// turning a degradation into an outage.
func (d Deps) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, http.StatusOK, HealthResponse{
		Status: "ok", Version: version.Binary, Edition: version.Edition,
		Tenancy: d.Capability.String(),
	})
}

// ready is readiness: this process can serve requests now.
//
// It is separate from liveness because the two answers differ during startup
// and during a rebuild — a process that is up but not yet able to serve must
// fail this and pass liveness, or an orchestrator restarts it in a loop while
// it is trying to start.
func (d Deps) ready(w http.ResponseWriter, r *http.Request) {
	if d.Ready != nil {
		if err := d.Ready(); err != nil {
			writeJSON(w, r, http.StatusServiceUnavailable,
				ReadyResponse{Status: "not_ready", Reason: err.Error()})
			return
		}
	}
	writeJSON(w, r, http.StatusOK, ReadyResponse{Status: "ready"})
}
