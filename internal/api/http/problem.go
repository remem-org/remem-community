// Package http is Remem's REST surface.
//
// It is the only package that knows HTTP status codes exist. Everything below
// it classifies with errs.Kind, and [WriteError] is the single place a kind
// becomes a number — so "what does a conflict return" has one answer, in one
// file, rather than a convention each handler follows until one does not.
//
// Routing uses the standard library's method-and-pattern ServeMux (Go 1.22+).
// There is no router dependency, because there is nothing a router would do
// here that ServeMux does not.
package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
)

// ContentType is RFC 9457's media type for a problem document.
const ContentType = "application/problem+json"

// Problem is an RFC 9457 problem document.
//
// Every error response is one, including the ones a framework would normally
// produce itself. A client that has to handle two error shapes handles one of
// them badly.
type Problem struct {
	// Type identifies the problem kind. It is a URN rather than an https URL:
	// RFC 9457 wants a stable identifier, and a URL is a promise to host a
	// page at it, which is a promise this project would break.
	Type string `json:"type"`
	// Title is the kind in human words, and does not vary by occurrence.
	Title string `json:"title"`
	// Status repeats the HTTP status, so a document that has been logged or
	// forwarded still says what happened.
	Status int `json:"status"`
	// Detail describes this occurrence. For a server-side failure it is
	// deliberately generic — see WriteError.
	Detail string `json:"detail"`
	// Instance identifies this occurrence: the path that produced it.
	Instance string `json:"instance"`
	// RequestID is the extension member that makes a report actionable. A user
	// quoting it lets an operator find the one log line that has the real
	// error in it.
	RequestID string `json:"request_id,omitempty"`
}

// problemFor describes how one errs.Kind is presented.
type problemFor struct {
	status int
	title  string
	// internal marks a kind whose message must not reach the client. The
	// message may name Pebble, a key, or a path, and spec §58 forbids exposing
	// an engine error through a public API — so those are logged and replaced.
	internal bool
}

// kinds is the whole mapping. It is a table rather than a switch so that
// adding an errs.Kind without deciding its status is a visible omission.
var kinds = map[errs.Kind]problemFor{
	errs.NotFound:            {http.StatusNotFound, "Not found", false},
	errs.Conflict:            {http.StatusConflict, "Conflict", false},
	errs.Invalid:             {http.StatusUnprocessableEntity, "Unprocessable request", false},
	errs.Unauthorized:        {http.StatusUnauthorized, "Unauthorized", false},
	errs.Forbidden:           {http.StatusForbidden, "Forbidden", false},
	errs.Storage:             {http.StatusInternalServerError, "Internal error", true},
	errs.Corruption:          {http.StatusInternalServerError, "Internal error", true},
	errs.Unavailable:         {http.StatusServiceUnavailable, "Unavailable", true},
	errs.MigrationRequired:   {http.StatusServiceUnavailable, "Unavailable", true},
	errs.IncompatibleVersion: {http.StatusInternalServerError, "Internal error", true},
	errs.NotLeader:           {http.StatusServiceUnavailable, "Unavailable", true},
	errs.Transient:           {http.StatusServiceUnavailable, "Unavailable", true},
	errs.RateLimited:         {http.StatusTooManyRequests, "Too many requests", false},
}

// urnFor renders a kind as a stable problem type.
func urnFor(k errs.Kind) string { return "urn:remem:problem:" + k.String() }

// WriteError turns a classified error into a problem document.
//
// A kind marked internal has its message replaced and the real one logged
// against the request id. That is not politeness: below this layer an error can
// name a Pebble path, a key, or a tenant, and returning it would put internal
// structure — occasionally another tenant's — into a client's hands.
//
// A 422 rather than a 400 for errs.Invalid is deliberate and is what the
// plan's test asks for: a body that will not parse is a malformed request, and
// a body that parses into something unacceptable is a well-formed request the
// server refuses to process.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	kind := errs.KindOf(err)
	spec, known := kinds[kind]
	if !known {
		spec = problemFor{http.StatusInternalServerError, "Internal error", true}
	}

	detail := err.Error()
	if spec.internal {
		obs.Logger(r.Context()).Error("request failed",
			"error", err, "kind", kind.String(), "route", routeOf(r), "status", spec.status)
		detail = "The server could not complete this request. Quote the request id when reporting it."
	}
	if kind == errs.Unauthorized {
		// RFC 9110 requires a challenge with a 401. Without it a client cannot
		// tell "you sent no credential" from "your credential was rejected".
		w.Header().Set("WWW-Authenticate", `Bearer realm="remem"`)
	}

	WriteProblem(w, r, spec.status, spec.title, detail, urnFor(kind))
}

// WriteProblem writes one problem document.
func WriteProblem(w http.ResponseWriter, r *http.Request, status int, title, detail, typ string) {
	if typ == "" {
		typ = "about:blank"
	}
	p := Problem{
		Type:      typ,
		Title:     title,
		Status:    status,
		Detail:    detail,
		Instance:  r.URL.Path,
		RequestID: obs.RequestID(r.Context()),
	}

	w.Header().Set("Content-Type", ContentType)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(p); err != nil {
		// The status is already written, so there is nothing to tell the
		// client. Log it: a body that fails to encode means the document
		// itself is wrong, which is worth knowing.
		obs.Logger(r.Context()).Error("writing a problem document failed", "error", err)
	}
}

// WriteMalformed reports a request the server could not parse. It is 400,
// distinct from the 422 a parseable-but-unacceptable request gets.
func WriteMalformed(w http.ResponseWriter, r *http.Request, detail string) {
	WriteProblem(w, r, http.StatusBadRequest, "Malformed request", detail,
		"urn:remem:problem:malformed")
}

// writeJSON writes a success response.
func writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		obs.Logger(r.Context()).Error("writing a response failed", "error", err)
	}
}

// decodeJSON reads a request body into dst.
//
// Unknown fields are refused. A client that sends `{"contnet": "..."}` and
// receives 201 has silently stored an empty memory, and will discover it much
// later; being told at once is worth the strictness.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		WriteMalformed(w, r, describeDecodeError(err))
		return false
	}
	// A second JSON value in one body is a client bug, and accepting the first
	// silently would hide it.
	if dec.More() {
		WriteMalformed(w, r, "the request body holds more than one JSON value")
		return false
	}
	return true
}

func describeDecodeError(err error) string {
	var syntax *json.SyntaxError
	var typ *json.UnmarshalTypeError
	switch {
	case errsAs(err, &syntax):
		return fmt.Sprintf("the request body is not valid JSON (at byte %d)", syntax.Offset)
	case errsAs(err, &typ):
		return fmt.Sprintf("field %q expects %s", typ.Field, typ.Type)
	default:
		return "the request body could not be read: " + err.Error()
	}
}
