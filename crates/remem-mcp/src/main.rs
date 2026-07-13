mod client;
mod handler;
mod protocol;
mod resources;
mod sse;
mod tools;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use reqwest::header::{HeaderMap, HeaderValue};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing_subscriber::EnvFilter;

use crate::client::RememClient;

#[derive(Parser, Debug)]
#[command(name = "remem-mcp", about = "Remem MCP server (stdio and SSE transports)")]
struct Args {
    /// URL of the remem-server REST API.
    #[arg(long, env = "REMEM_SERVER_URL", default_value = "http://localhost:4545")]
    server_url: String,

    /// Transport to use: "stdio" or "sse".
    #[arg(long, env = "MCP_TRANSPORT", default_value = "stdio")]
    transport: String,

    /// Host for the SSE HTTP server.
    #[arg(long, env = "MCP_HOST", default_value = "0.0.0.0")]
    host: String,

    /// Port for the SSE HTTP server.
    #[arg(long, env = "MCP_PORT", default_value_t = 4546)]
    port: u16,

    /// API key for authenticating with remem-server. Empty = no auth header (dev only).
    #[arg(long, env = "REMEM_API_KEY", default_value = "")]
    api_key: String,
}

fn build_http_client(api_key: &str) -> anyhow::Result<reqwest::Client> {
    if api_key.is_empty() {
        return Ok(reqwest::Client::new());
    }
    let bearer = format!("Bearer {api_key}");
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&bearer).context("invalid REMEM_API_KEY value")?,
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .context("failed to build HTTP client")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // All logging goes to stderr; stdout is the JSON-RPC channel in stdio mode.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let base_url = args.server_url.trim_end_matches('/').to_string();
    let client = build_http_client(&args.api_key)?;
    let remem = Arc::new(RememClient::new(client, base_url.clone()));

    tracing::info!(
        transport = %args.transport,
        server_url = %base_url,
        "remem-mcp starting"
    );

    match args.transport.as_str() {
        "stdio" => run_stdio(remem).await,
        "sse" => sse::run(remem, &args.host, args.port).await,
        other => anyhow::bail!("unknown transport '{other}'; use 'stdio' or 'sse'"),
    }
}

async fn run_stdio(client: Arc<RememClient>) -> anyhow::Result<()> {
    tracing::info!("stdio transport ready");

    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            tracing::info!("stdin EOF, exiting");
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<protocol::JsonRpcRequest>(trimmed) {
            Ok(req) => handler::handle(&req, &client).await,
            Err(e) => Some(protocol::JsonRpcResponse::err(
                None,
                protocol::PARSE_ERROR,
                e.to_string(),
            )),
        };

        if let Some(resp) = response {
            let json = serde_json::to_string(&resp)?;
            stdout.write_all(json.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_api_key_builds_client() {
        build_http_client("").unwrap();
    }

    #[test]
    fn valid_api_key_builds_client() {
        build_http_client("my-secret-key-abc123").unwrap();
    }

    #[test]
    fn null_byte_in_api_key_returns_error() {
        assert!(build_http_client("bad\x00key").is_err());
    }
}
