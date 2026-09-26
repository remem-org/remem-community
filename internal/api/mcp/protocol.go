// Package mcp is Remem's Model Context Protocol surface.
//
// It speaks JSON-RPC 2.0 over the Streamable HTTP transport: one endpoint,
// POST for requests, DELETE to end a session. Responses are plain JSON rather
// than server-sent events, because nothing here streams — every Phase 3 tool
// answers in one message, and an SSE frame around a single response is
// ceremony a client still has to parse.
//
// # Sessions, and why two error codes
//
// MCP is stateful: a client initialises once and presents its session id
// afterwards. Two failures look similar and mean opposite things. No session id
// means the client never initialised, which is a bug in the client — 400. An id
// the server does not know means the session expired or was served by a node
// that has forgotten it, which is normal operation and means "initialise again"
// — 404. Serving 400 for an expired session strands every client that idled
// past the sweep: it retries with the same dead id forever, because nothing
// ever told it to start over.
package mcp

import "encoding/json"

// ProtocolVersion is the MCP revision this server implements.
const ProtocolVersion = "2025-06-18"

// SessionHeader carries the session id, in both directions.
const SessionHeader = "Mcp-Session-Id"

// JSON-RPC 2.0 error codes. The first four are the specification's; the last is
// in the implementation-defined range and is Remem's.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// There is deliberately no code for "a tool ran and failed". MCP carries that
// inside a successful response, as CallToolResult.IsError — a memory that does
// not exist is not a protocol fault, and a JSON-RPC error would make the client
// treat it as one.

// Request is a JSON-RPC 2.0 request or notification.
//
// A notification is a request with no id, and the difference is load-bearing:
// a notification must not be answered, so ID is a json.RawMessage that
// distinguishes absent from null.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether no response is expected.
func (r Request) IsNotification() bool { return len(r.ID) == 0 }

// Response is a JSON-RPC 2.0 response.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// RPCError is a JSON-RPC error object.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func result(id json.RawMessage, v any) Response {
	return Response{JSONRPC: "2.0", ID: id, Result: v}
}

func failure(id json.RawMessage, code int, message string) Response {
	return Response{JSONRPC: "2.0", ID: id, Error: &RPCError{Code: code, Message: message}}
}

// --- initialize -------------------------------------------------------------

// InitializeParams is what a client sends at initialize.
type InitializeParams struct {
	ProtocolVersion string     `json:"protocolVersion"`
	ClientInfo      ClientInfo `json:"clientInfo"`
	Capabilities    any        `json:"capabilities,omitempty"`
}

// ClientInfo names the client. It is stored on the session, because it is the
// only way to answer "which agent is doing this" when one starts misbehaving.
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// InitializeResult is the server's half of the handshake.
type InitializeResult struct {
	ProtocolVersion string       `json:"protocolVersion"`
	Capabilities    Capabilities `json:"capabilities"`
	ServerInfo      ServerInfo   `json:"serverInfo"`
}

// Capabilities declares what this server offers. Only what is implemented is
// declared: announcing a capability the server does not serve turns a clear
// "method not found" into a client that waits for something that never comes.
type Capabilities struct {
	Tools     *ToolsCapability     `json:"tools,omitempty"`
	Resources *ResourcesCapability `json:"resources,omitempty"`
}

// ToolsCapability declares tool support. ListChanged is false because the tool
// set is fixed at build time.
type ToolsCapability struct {
	ListChanged bool `json:"listChanged"`
}

// ResourcesCapability declares resource support.
type ResourcesCapability struct {
	Subscribe   bool `json:"subscribe"`
	ListChanged bool `json:"listChanged"`
}

// ServerInfo names this server.
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// --- tools ------------------------------------------------------------------

// Tool describes one callable tool.
type Tool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputSchema Schema `json:"inputSchema"`
}

// Schema is a JSON Schema object describing a tool's arguments.
//
// It is a typed struct rather than a map, so that a schema which does not
// describe an object, or names a required property it does not define, cannot
// be written by accident — the shape is checked by the compiler and the rest by
// TestToolSchemasAreValidJSONSchema.
type Schema struct {
	Type       string              `json:"type"`
	Properties map[string]Property `json:"properties"`
	Required   []string            `json:"required,omitempty"`
}

// Property describes one argument.
type Property struct {
	Type        string    `json:"type"`
	Description string    `json:"description,omitempty"`
	Items       *Property `json:"items,omitempty"`
	Default     any       `json:"default,omitempty"`
	Minimum     *int      `json:"minimum,omitempty"`
	Maximum     *int      `json:"maximum,omitempty"`
	// Enum is the closed set of values a property accepts. It is worth the
	// field: a model given the three search types in the schema picks one,
	// where a model told about them only in prose guesses a fourth.
	Enum []string `json:"enum,omitempty"`
}

// ToolsListResult is the answer to tools/list.
type ToolsListResult struct {
	Tools []Tool `json:"tools"`
}

// CallToolParams is the argument of tools/call.
type CallToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// CallToolResult is a tool's answer.
//
// IsError carries a tool's own failure inside a successful JSON-RPC response,
// which is what MCP asks for: a memory that does not exist is not a protocol
// fault, and reporting it as one would make a client treat it as a server
// problem rather than showing it to the model.
type CallToolResult struct {
	Content []Content `json:"content"`
	IsError bool      `json:"isError,omitempty"`
}

// Content is one piece of a tool's answer.
type Content struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

func textContent(s string) []Content { return []Content{{Type: "text", Text: s}} }

// --- resources --------------------------------------------------------------

// Resource is one readable resource.
type Resource struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

// ResourcesListResult is the answer to resources/list.
type ResourcesListResult struct {
	Resources []Resource `json:"resources"`
}

// ReadResourceParams names the resource to read.
type ReadResourceParams struct {
	URI string `json:"uri"`
}

// ReadResourceResult carries a resource's contents.
type ReadResourceResult struct {
	Contents []ResourceContents `json:"contents"`
}

// ResourceContents is one resource's body.
type ResourceContents struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType,omitempty"`
	Text     string `json:"text,omitempty"`
}
