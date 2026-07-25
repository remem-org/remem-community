# remem-mcp

MCP server bridging LLM clients to remem-server via REST. Stdio transport only — the network (Streamable HTTP) transport now lives in-process inside remem-server at `/mcp` (see crates/remem-server/CLAUDE.md).

## Source Layout

```
src/
├── main.rs         # CLI args, reqwest client init, stdio loop
├── handler.rs      # JSON-RPC 2.0 dispatch — routes method names to tools/resources
├── tools.rs        # 8 MCP tool implementations
├── resources.rs    # 3 MCP resource implementations
└── protocol.rs     # JSON-RPC 2.0 types (Request, Response, Error, Notification)
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

Run the `remem-mcp` binary directly, pointed at `remem-server`:

```json
{
  "mcpServers": {
    "remem": {
      "command": "cargo",
      "args": ["run", "--release", "-p", "remem-mcp", "--", "--server-url", "http://localhost:4545"]
    }
  }
}
```

Clients that speak MCP Streamable HTTP directly can skip this binary
entirely and talk to `remem-server`'s `/mcp` route instead — see
crates/remem-server/CLAUDE.md.

## Environment Variables

```bash
REMEM_SERVER_URL=http://remem-server:4545   # remem-server endpoint
REMEM_API_KEY=                               # forwarded as Bearer token (optional)
```

## Transport Notes

- **stdio**: Single client, no port needed. Used with Claude Desktop / Claude Code MCP config. The only transport this crate implements.
