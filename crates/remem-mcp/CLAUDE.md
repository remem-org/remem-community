# remem-mcp

MCP server bridging LLM clients to remem-server. Supports stdio and SSE transports. Port 8000 (SSE).

## Source Layout

```
src/
├── main.rs         # Transport selection (--transport stdio|sse), reqwest client init
├── handler.rs      # JSON-RPC 2.0 dispatch — routes method names to tools/resources
├── tools.rs        # 8 MCP tool implementations
├── resources.rs    # 3 MCP resource implementations
├── protocol.rs     # JSON-RPC 2.0 types (Request, Response, Error, Notification)
└── sse.rs          # SSE transport (Axum)
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `store_memory` | Store a new memory with auto-connection discovery |
| `search_memories` | Semantic, keyword, or hybrid search |
| `get_memory` | Retrieve a memory by ID |
| `update_memory` | Update content, tags, or importance |
| `delete_memory` | Soft archive or hard delete |
| `find_related` | Graph traversal to find related memories |
| `promote_to_longterm` | Promote short-term → long-term |
| `list_recent_memories` | List recently created/accessed memories |

## MCP Resources

| Resource URI | Description |
|---|---|
| `memory://stats` | System statistics snapshot |
| `memory://collections/recent` | Recently created/accessed memories |
| `memory://collections/important` | High-importance memories |

## MCP Client Configuration

```json
{
  "mcpServers": {
    "remem": {
      "command": "docker",
      "args": ["exec", "-i", "remem-mcp-server", "remem-mcp", "--transport", "stdio"]
    }
  }
}
```

## Environment Variables

```bash
REMEM_SERVER_URL=http://remem-server:8001   # remem-server endpoint
REMEM_API_KEY=                               # forwarded as Bearer token (optional)
MCP_TRANSPORT=sse                            # sse | stdio
```

## Transport Notes

- **stdio**: Single client, no port needed. Used with Claude Desktop / Claude Code MCP config.
- **SSE**: Multi-client, binds port 8000. Events streamed as `text/event-stream`.
- Transport selected at startup via `--transport` flag or `MCP_TRANSPORT` env var.
