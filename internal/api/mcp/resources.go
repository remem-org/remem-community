package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/remem-org/remem-go/internal/version"
)

// Resource URIs. They are opaque to a client and stable to us.
const (
	resourceServerInfo = "remem://server/info"
	resourceUsage      = "remem://server/usage"
)

// resources lists what a client may read.
//
// Phase 3 exposes two, and both are about the server rather than about the
// user's memories. A resource is content a model loads unprompted, and
// exposing memories that way would put a tenant's data into every conversation
// whether or not it was relevant — which is what search is for.
func (h *Handler) resources() []Resource {
	return []Resource{
		{
			URI:         resourceServerInfo,
			Name:        "Server information",
			Description: "Version and capabilities of this Remem server.",
			MimeType:    "application/json",
		},
		{
			URI:         resourceUsage,
			Name:        "How to use Remem",
			Description: "When to store a memory and when to search for one.",
			MimeType:    "text/markdown",
		},
	}
}

func (h *Handler) readResource(ctx context.Context, uri string) (ReadResourceResult, error) {
	switch uri {
	case resourceServerInfo:
		t, err := tenantOf(ctx)
		if err != nil {
			return ReadResourceResult{}, err
		}
		body, err := json.Marshal(map[string]any{
			"name":             h.ServerName,
			"version":          version.Binary,
			"protocol_version": ProtocolVersion,
			"tenant":           string(t),
			"tools":            toolNames(),
		})
		if err != nil {
			return ReadResourceResult{}, err
		}
		return ReadResourceResult{Contents: []ResourceContents{
			{URI: uri, MimeType: "application/json", Text: string(body)},
		}}, nil

	case resourceUsage:
		return ReadResourceResult{Contents: []ResourceContents{
			{URI: uri, MimeType: "text/markdown", Text: usage},
		}}, nil

	default:
		return ReadResourceResult{}, fmt.Errorf("no resource at %s", uri)
	}
}

func toolNames() []string {
	tools := Tools()
	out := make([]string, len(tools))
	for i, t := range tools {
		out[i] = t.Name
	}
	return out
}

// usage is guidance for the model, not documentation for a person.
//
// It is short on purpose: it is loaded into a context window, where every line
// costs the user money and displaces something else.
const usage = `# Remem

Persistent memory across conversations.

**Store** a fact when it will still matter next time — a preference, a decision,
a constraint, a name. Do not store what is already in the current context.

**Search** before assuming you do not know something. Searching is cheap;
asking the user to repeat themselves is not.

Scores run from 0 to 1 and are comparable across searches. Below about 0.3 a
result is probably unrelated.

Archiving retires a memory from search but keeps it. Deleting with ` + "`hard`" + `
cannot be undone.
`
