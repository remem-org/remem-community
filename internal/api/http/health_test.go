package http_test

import (
	"net/http"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/version"
)

// Liveness says which edition is running, beside its version: an operator
// looking at a deployment should not have to know how its image was built.
func TestHealthReportsTheEdition(t *testing.T) {
	srv := newServer(t)
	rr := send(t, srv, "", "GET", remhttp.APIPrefix+"/health", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("health returned %d: %s", rr.Code, rr.Body.String())
	}
	if got := jsonBody(t, rr)["edition"]; got != version.Edition {
		t.Fatalf("health reported edition %v, want %q", got, version.Edition)
	}
}
