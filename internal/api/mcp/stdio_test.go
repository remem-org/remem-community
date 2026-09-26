package mcp_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/api/mcp"
)

// The stdio bridge: remem-mcp's translation between a line-oriented stdio
// client and the HTTP MCP endpoint. Split from mcp_test.go in Phase 13 to keep
// that file under the project's size limit; the tests did not change.

// The bridge must capture the session id from initialize and replay it, because
// the stdio protocol has nowhere to put a header. Without this every call after
// the handshake would be a 400.
func TestTheStdioBridgeCarriesTheSessionForward(t *testing.T) {
	srv := httptest.NewServer(newServer(t))
	t.Cleanup(srv.Close)

	client := &mcp.StdioClient{Endpoint: srv.URL + "/mcp", Credential: keyAcme}

	in := strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","clientInfo":{"name":"t","version":"1"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
	}, "\n") + "\n")

	var out strings.Builder
	if err := client.Run(context.Background(), in, &out); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	// Two responses: the handshake and tools/list. The notification is not
	// answered, which is what JSON-RPC requires.
	if len(lines) != 2 {
		t.Fatalf("got %d responses, want 2:\n%s", len(lines), out.String())
	}
	var second map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatal(err)
	}
	if _, isErr := second["error"]; isErr {
		t.Fatalf("the second call failed; the session was not carried forward: %s", lines[1])
	}
	if _, ok := second["result"]; !ok {
		t.Fatalf("no result: %s", lines[1])
	}
}

// A transport failure must reach the client as something it can show, not as
// "unexpected end of JSON input".
func TestTheStdioBridgeTranslatesATransportFailure(t *testing.T) {
	srv := httptest.NewServer(newServer(t))
	t.Cleanup(srv.Close)

	client := &mcp.StdioClient{Endpoint: srv.URL + "/mcp", Credential: "wrong-key"}
	var out strings.Builder
	if err := client.Run(context.Background(),
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`+"\n"), &out); err != nil {
		t.Fatal(err)
	}
	var resp map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &resp); err != nil {
		t.Fatalf("the bridge wrote something that is not JSON-RPC: %s", out.String())
	}
	e, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error object: %s", out.String())
	}
	msg := e["message"].(string)
	if !strings.Contains(msg, "401") && !strings.Contains(strings.ToLower(msg), "unauthorized") {
		t.Fatalf("the message does not say what went wrong: %q", msg)
	}
}

// A bad line must not end the session. A bridge that died on the first
// malformed message would take the user's editor session with it.
func TestTheStdioBridgeSurvivesABadLine(t *testing.T) {
	srv := httptest.NewServer(newServer(t))
	t.Cleanup(srv.Close)

	client := &mcp.StdioClient{Endpoint: srv.URL + "/mcp", Credential: keyAcme}
	in := strings.NewReader("not json\n" +
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","clientInfo":{"name":"t","version":"1"}}}` + "\n")

	var out strings.Builder
	if err := client.Run(context.Background(), in, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d responses, want a parse error and then the handshake:\n%s", len(lines), out.String())
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first["error"].(map[string]any)["code"].(float64) != mcp.CodeParseError {
		t.Fatalf("first response = %s", lines[0])
	}
	var second map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatal(err)
	}
	if _, ok := second["result"]; !ok {
		t.Fatalf("the bridge did not recover: %s", lines[1])
	}
}
