package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/session"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/version"
)

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// Deps is what the MCP surface needs.
type Deps struct {
	Memories *memory.Service
	Sessions *session.Registry
	// ServerName is what the server calls itself in the handshake.
	ServerName string
}

// Handler serves MCP over Streamable HTTP.
type Handler struct {
	Deps
}

// NewHandler builds the MCP handler. It is mounted at /mcp behind the same
// authentication and tenant resolution the REST surface uses, so a credential
// means the same thing on both — an MCP client that could reach a tenant the
// REST API refuses would be an isolation hole with two doors.
func NewHandler(d Deps) *Handler {
	if d.ServerName == "" {
		d.ServerName = "remem"
	}
	return &Handler{Deps: d}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.post(w, r)
	case http.MethodDelete:
		h.terminate(w, r)
	default:
		w.Header().Set("Allow", "POST, DELETE")
		remhttp.WriteProblem(w, r, http.StatusMethodNotAllowed, "Method not allowed",
			"the MCP endpoint accepts POST for requests and DELETE to end a session",
			"urn:remem:problem:method-not-allowed")
	}
}

func (h *Handler) post(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		remhttp.WriteMalformed(w, r, "the request body could not be read")
		return
	}

	var req Request
	if err := json.Unmarshal(body, &req); err != nil {
		writeRPC(w, r, http.StatusOK, failure(nil, CodeParseError, "the request is not valid JSON"))
		return
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		writeRPC(w, r, http.StatusOK, failure(req.ID, CodeInvalidRequest,
			`a request must carry "jsonrpc":"2.0" and a method`))
		return
	}

	// initialize is the one method that runs without a session, because it is
	// what creates one.
	if req.Method == "initialize" {
		h.initialize(w, r, req)
		return
	}

	// A notification is answered with 202 and no body, per JSON-RPC. It is
	// handled before the session check so that notifications/initialized —
	// which a client sends immediately after the handshake — is not refused
	// for a session the client is in the middle of establishing.
	if req.IsNotification() {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	sess, ok := h.requireSession(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	if err := h.Sessions.Touch(ctx, sess.ID); err != nil && !errs.Is(err, errs.NotFound) {
		remhttp.WriteError(w, r, err)
		return
	}

	writeRPC(w, r, http.StatusOK, h.dispatch(ctx, req))
}

// requireSession resolves the session, distinguishing "you never initialised"
// from "re-initialise". See the package comment: conflating them strands every
// client that idled past the sweep.
func (h *Handler) requireSession(w http.ResponseWriter, r *http.Request) (session.Session, bool) {
	raw := r.Header.Get(SessionHeader)
	if raw == "" {
		remhttp.WriteProblem(w, r, http.StatusBadRequest, "No session",
			"this request carries no "+SessionHeader+"; call initialize first",
			"urn:remem:problem:no-session")
		return session.Session{}, false
	}
	sid, err := id.Parse(raw)
	if err != nil {
		// A malformed id cannot name a live session, so it means the same
		// thing to the client as an expired one: initialise again.
		remhttp.WriteProblem(w, r, http.StatusNotFound, "Unknown session",
			"this session is not known to the server; call initialize again",
			"urn:remem:problem:unknown-session")
		return session.Session{}, false
	}
	sess, err := h.Sessions.Get(r.Context(), sid)
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			remhttp.WriteProblem(w, r, http.StatusNotFound, "Unknown session",
				"this session has expired or is not known to the server; call initialize again",
				"urn:remem:problem:unknown-session")
			return session.Session{}, false
		}
		remhttp.WriteError(w, r, err)
		return session.Session{}, false
	}
	return sess, true
}

func (h *Handler) initialize(w http.ResponseWriter, r *http.Request, req Request) {
	var params InitializeParams
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			writeRPC(w, r, http.StatusOK, failure(req.ID, CodeInvalidParams,
				"the initialize parameters could not be read"))
			return
		}
	}

	principal := "anonymous"
	if p, ok := auth.FromContext(r.Context()); ok {
		principal = p.ID
	}
	negotiated := params.ProtocolVersion
	if negotiated == "" {
		negotiated = ProtocolVersion
	}

	sess, err := h.Sessions.Create(r.Context(), session.Session{
		Principal:       principal,
		ClientName:      params.ClientInfo.Name,
		ClientVersion:   params.ClientInfo.Version,
		ProtocolVersion: negotiated,
	})
	if err != nil {
		remhttp.WriteError(w, r, err)
		return
	}

	w.Header().Set(SessionHeader, sess.ID.String())
	writeRPC(w, r, http.StatusOK, result(req.ID, InitializeResult{
		ProtocolVersion: ProtocolVersion,
		Capabilities: Capabilities{
			Tools:     &ToolsCapability{ListChanged: false},
			Resources: &ResourcesCapability{Subscribe: false, ListChanged: false},
		},
		ServerInfo: ServerInfo{Name: h.ServerName, Version: version.Binary},
	}))
}

// terminate ends a session. An unknown one is a success: the post-condition —
// the session is gone — already holds, and a client tidying up on shutdown
// should not have to handle a 404.
func (h *Handler) terminate(w http.ResponseWriter, r *http.Request) {
	raw := r.Header.Get(SessionHeader)
	if raw == "" {
		remhttp.WriteProblem(w, r, http.StatusBadRequest, "No session",
			"this request carries no "+SessionHeader,
			"urn:remem:problem:no-session")
		return
	}
	sid, err := id.Parse(raw)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := h.Sessions.Delete(r.Context(), sid); err != nil {
		remhttp.WriteError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// dispatch routes one JSON-RPC method.
func (h *Handler) dispatch(ctx context.Context, req Request) Response {
	switch req.Method {
	case "ping":
		return result(req.ID, map[string]any{})

	case "tools/list":
		return result(req.ID, ToolsListResult{Tools: Tools()})

	case "tools/call":
		var params CallToolParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return failure(req.ID, CodeInvalidParams, "the tool call parameters could not be read")
		}
		return h.runTool(ctx, req, params)

	case "resources/list":
		return result(req.ID, ResourcesListResult{Resources: h.resources()})

	case "resources/read":
		var params ReadResourceParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return failure(req.ID, CodeInvalidParams, "the resource parameters could not be read")
		}
		res, err := h.readResource(ctx, params.URI)
		if err != nil {
			return failure(req.ID, CodeInvalidParams, err.Error())
		}
		return result(req.ID, res)

	default:
		return failure(req.ID, CodeMethodNotFound, "this server does not implement "+req.Method)
	}
}

// runTool executes a tool, reporting a tool's own failure inside a successful
// response.
//
// That is what MCP asks for and it is right: a memory that does not exist is
// not a protocol fault, and returning a JSON-RPC error would make the client
// treat it as "Remem is broken" rather than showing the model something it can
// act on. A protocol-level failure — an unknown tool — is still a JSON-RPC
// error.
func (h *Handler) runTool(ctx context.Context, req Request, params CallToolParams) Response {
	known := false
	for _, t := range Tools() {
		if t.Name == params.Name {
			known = true
			break
		}
	}
	if !known {
		return failure(req.ID, CodeMethodNotFound, "no tool named "+params.Name)
	}

	out, err := h.callTool(ctx, params.Name, params.Arguments)
	if err != nil {
		obs.Logger(ctx).Info("tool failed", "tool", params.Name, "kind", errs.KindOf(err).String())
		return result(req.ID, CallToolResult{
			Content: textContent(toolErrorText(err)),
			IsError: true,
		})
	}

	body, err := json.Marshal(out)
	if err != nil {
		return failure(req.ID, CodeInternalError, "the tool's answer could not be encoded")
	}
	return result(req.ID, CallToolResult{Content: textContent(string(body))})
}

// toolErrorText is what the model is shown when a tool fails.
//
// A server-side failure is replaced with a generic sentence, for the same
// reason the REST layer does it: below this point an error can name a storage
// path or a key, and a model is a channel to a user like any other.
func toolErrorText(err error) string {
	switch errs.KindOf(err) {
	case errs.NotFound, errs.Invalid, errs.Conflict, errs.Unauthorized, errs.Forbidden:
		return err.Error()
	default:
		return "the server could not complete this request"
	}
}

func writeRPC(w http.ResponseWriter, r *http.Request, status int, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		obs.Logger(r.Context()).Error("writing an MCP response failed", "error", err)
	}
}

// errNoTenant is returned when a resource is read without a resolved scope; it
// exists so resources.go can report it without importing errs twice over.
var errNoTenant = errors.New("this request carries no tenant")

// tenantOf is the scope check resources use.
func tenantOf(ctx context.Context) (tenant.ID, error) {
	t, ok := tenant.FromContext(ctx)
	if !ok {
		return "", errNoTenant
	}
	return t, nil
}
