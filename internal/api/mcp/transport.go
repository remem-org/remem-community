package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// StdioClient bridges a stdio MCP client to a Remem server over HTTP.
//
// It is a pipe and nothing more, and that is the whole design. Every MCP client
// today speaks stdio; Remem is a server. The alternative — embedding the engine
// in the stdio binary — would mean two processes writing the same data
// directory whenever a user also ran the server, which is the one thing a
// storage engine cannot survive.
//
// The one piece of state it holds is the session id, captured from initialize
// and replayed on every later request, because the stdio protocol has nowhere
// to put a header.
type StdioClient struct {
	// Endpoint is the server's /mcp URL.
	Endpoint string
	// Credential is presented as a bearer token. Empty sends no header, which
	// is what an unauthenticated development server expects.
	Credential string
	// Tenant, when set, is sent as X-Remem-Tenant. It is honoured only for a
	// credential authorised across tenants; a bound one ignores it, which is
	// the point of binding.
	Tenant string
	// HTTP is the client used. A nil value takes a default with a timeout,
	// because a stdio bridge with no timeout hangs a user's editor.
	HTTP *http.Client

	mu        sync.Mutex
	sessionID string
}

// DefaultTimeout bounds one forwarded request.
const DefaultTimeout = 60 * time.Second

// Run reads JSON-RPC messages from in, forwards each to the server, and writes
// the responses to out. It returns when in reaches EOF or ctx is cancelled.
//
// Errors are reported as JSON-RPC error responses rather than by exiting: a
// bridge that dies on the first network blip takes the user's editor session
// with it, where an error object is something the client can show and recover
// from.
func (c *StdioClient) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	if c.HTTP == nil {
		c.HTTP = &http.Client{Timeout: DefaultTimeout}
	}

	scanner := bufio.NewScanner(in)
	// A tool call carrying a batch of memories is far larger than bufio's
	// default 64 KiB line limit, and a silently truncated line is a corrupted
	// request rather than a failed one.
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	enc := json.NewEncoder(out)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			if err := enc.Encode(failure(nil, CodeParseError, "the message is not valid JSON")); err != nil {
				return err
			}
			continue
		}

		resp, notification, err := c.forward(ctx, line, req)
		if err != nil {
			if req.IsNotification() {
				continue
			}
			if err := enc.Encode(failure(req.ID, CodeInternalError, err.Error())); err != nil {
				return err
			}
			continue
		}
		if notification {
			continue
		}
		// One message per line, and exactly one: the server already terminates
		// its body with a newline, and a second one is a blank line that some
		// clients read as an empty message.
		resp = append(bytes.TrimRight(resp, "\r\n"), '\n')
		if _, err := out.Write(resp); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// forward sends one message and returns the raw response body.
//
// The body is returned unparsed and written through verbatim. Re-encoding it
// would silently drop any field this build does not know about, which is
// exactly what a bridge must not do to a protocol that is still moving.
func (c *StdioClient) forward(ctx context.Context, body []byte, req Request) (raw []byte, notification bool, err error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if c.Credential != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.Credential)
	}
	if c.Tenant != "" {
		httpReq.Header.Set("X-Remem-Tenant", c.Tenant)
	}
	if sid := c.session(); sid != "" {
		httpReq.Header.Set(SessionHeader, sid)
	}

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, false, fmt.Errorf("reaching %s: %w", c.Endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if sid := resp.Header.Get(SessionHeader); sid != "" {
		c.setSession(sid)
	}

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}

	// 202 is a notification the server accepted and will not answer.
	if resp.StatusCode == http.StatusAccepted || req.IsNotification() {
		return nil, true, nil
	}

	// A transport-level failure carries a problem document, not a JSON-RPC
	// response. Translating it here is what lets the client show something
	// meaningful instead of "unexpected end of JSON input".
	if resp.StatusCode >= 400 && !looksLikeRPC(out) {
		return nil, false, fmt.Errorf("the server answered %d: %s", resp.StatusCode, summarise(out))
	}
	return out, false, nil
}

func (c *StdioClient) session() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

func (c *StdioClient) setSession(sid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionID = sid
}

func looksLikeRPC(b []byte) bool {
	var probe struct {
		JSONRPC string `json:"jsonrpc"`
	}
	return json.Unmarshal(b, &probe) == nil && probe.JSONRPC == "2.0"
}

// summarise pulls the readable part out of a problem document, so a user sees
// "this session has expired" rather than a JSON blob.
func summarise(b []byte) string {
	var p struct {
		Title  string `json:"title"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(b, &p); err == nil && p.Detail != "" {
		return strings.TrimSpace(p.Title + ": " + p.Detail)
	}
	if len(b) > 200 {
		b = b[:200]
	}
	return string(b)
}
