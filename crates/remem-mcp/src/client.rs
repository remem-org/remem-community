use anyhow::{anyhow, Context};
use reqwest::Client;
use serde_json::Value;

/// Typed HTTP client for the remem-server REST API.
///
/// Owns a `reqwest::Client` and the base URL so callers never touch raw URLs.
pub struct RememClient {
    client: Client,
    base_url: String,
}

impl RememClient {
    /// Create a new client with the given HTTP client and base URL.
    pub fn new(client: Client, base_url: String) -> Self {
        Self { client, base_url }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    // ── Private request builders ─────────────────────────────────────────────

    fn get(&self, path: &str, request_id: &str) -> reqwest::RequestBuilder {
        self.client.get(self.url(path)).header("X-Request-ID", request_id)
    }

    fn post(&self, path: &str, request_id: &str) -> reqwest::RequestBuilder {
        self.client.post(self.url(path)).header("X-Request-ID", request_id)
    }

    fn put(&self, path: &str, request_id: &str) -> reqwest::RequestBuilder {
        self.client.put(self.url(path)).header("X-Request-ID", request_id)
    }

    fn delete(&self, path: &str, request_id: &str) -> reqwest::RequestBuilder {
        self.client.delete(self.url(path)).header("X-Request-ID", request_id)
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /// `POST /api/v1/memories` — create a new memory.
    pub async fn store_memory(&self, body: Value, request_id: &str) -> anyhow::Result<Value> {
        let resp = self
            .post("/api/v1/memories", request_id)
            .json(&body)
            .send()
            .await
            .context("POST /api/v1/memories")?;
        let status = resp.status();
        let data: Value = resp.json().await?;
        if !status.is_success() {
            return Err(anyhow!(
                "API {status}: {}",
                data["detail"].as_str().unwrap_or("unknown")
            ));
        }
        Ok(data)
    }

    /// `POST /api/v1/memories/search` — search memories by semantic, keyword, or hybrid query.
    pub async fn search_memories(&self, body: Value, request_id: &str) -> anyhow::Result<Value> {
        let resp = self
            .post("/api/v1/memories/search", request_id)
            .json(&body)
            .send()
            .await
            .context("POST /api/v1/memories/search")?;
        let status = resp.status();
        let data: Value = resp.json().await?;
        if !status.is_success() {
            return Err(anyhow!("API {status}"));
        }
        Ok(data)
    }

    /// `GET /api/v1/memories/{id}` — retrieve a memory by ID, optionally with connections.
    pub async fn get_memory(&self, id: &str, include_connections: bool, request_id: &str) -> anyhow::Result<Value> {
        let mut req = self.get(&format!("/api/v1/memories/{id}"), request_id);
        if include_connections {
            req = req.query(&[("include_connections", "true")]);
        }
        let resp = req.send().await.context("GET /api/v1/memories/{id}")?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(anyhow!("memory {id} not found"));
        }
        let data: Value = resp.json().await?;
        if !status.is_success() {
            return Err(anyhow!("API {status}"));
        }
        Ok(data)
    }

    /// `PUT /api/v1/memories/{id}` — update a memory's content, tags, or metadata.
    pub async fn update_memory(&self, id: &str, body: Value, request_id: &str) -> anyhow::Result<()> {
        let resp = self
            .put(&format!("/api/v1/memories/{id}"), request_id)
            .json(&body)
            .send()
            .await
            .context("PUT /api/v1/memories/{id}")?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(anyhow!("memory {id} not found"));
        }
        if !status.is_success() {
            return Err(anyhow!("API {status}"));
        }
        Ok(())
    }

    /// `DELETE /api/v1/memories/{id}` — soft archive or hard delete a memory.
    pub async fn delete_memory(&self, id: &str, hard: bool, request_id: &str) -> anyhow::Result<()> {
        let resp = self
            .delete(&format!("/api/v1/memories/{id}"), request_id)
            .query(&[("hard", if hard { "true" } else { "false" })])
            .send()
            .await
            .context("DELETE /api/v1/memories/{id}")?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(anyhow!("memory {id} not found"));
        }
        if !status.is_success() {
            return Err(anyhow!("API {status}"));
        }
        Ok(())
    }

    /// `GET /api/v1/memories/{id}/related` — traverse the connection graph to find related memories.
    pub async fn find_related(&self, id: &str, depth: i64, limit: i64, request_id: &str) -> anyhow::Result<Value> {
        let resp = self
            .get(&format!("/api/v1/memories/{id}/related"), request_id)
            .query(&[("depth", depth), ("limit", limit)])
            .send()
            .await
            .context("GET /api/v1/memories/{id}/related")?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(anyhow!("memory {id} not found"));
        }
        let data: Value = resp.json().await?;
        if !status.is_success() {
            return Err(anyhow!("API {status}"));
        }
        Ok(data)
    }

    /// `POST /api/v1/memories/{id}/promote` — promote a short-term memory to long-term.
    pub async fn promote_to_longterm(&self, id: &str, request_id: &str) -> anyhow::Result<()> {
        let resp = self
            .post(&format!("/api/v1/memories/{id}/promote"), request_id)
            .send()
            .await
            .context("POST /api/v1/memories/{id}/promote")?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(anyhow!("memory {id} not found"));
        }
        if !status.is_success() {
            return Err(anyhow!("API {status}"));
        }
        Ok(())
    }

    /// `GET /api/v1/memories` — list memories with pagination and optional type filter.
    pub async fn list_memories(
        &self,
        limit: i64,
        offset: i64,
        memory_type: Option<&str>,
        request_id: &str,
    ) -> anyhow::Result<Value> {
        let mut query: Vec<(&str, String)> = vec![
            ("limit", limit.to_string()),
            ("offset", offset.to_string()),
        ];
        if let Some(mt) = memory_type {
            query.push(("memory_type", mt.to_string()));
        }
        let resp = self
            .get("/api/v1/memories", request_id)
            .query(&query)
            .send()
            .await
            .context("GET /api/v1/memories")?;
        let status = resp.status();
        let data: Value = resp.json().await?;
        if !status.is_success() {
            return Err(anyhow!("API {status}"));
        }
        Ok(data)
    }

    /// `GET /api/v1/stats` — retrieve system statistics and memory metrics.
    pub async fn get_stats(&self, request_id: &str) -> anyhow::Result<Value> {
        let resp = self
            .get("/api/v1/stats", request_id)
            .send()
            .await
            .context("GET /api/v1/stats")?;
        Ok(resp.json().await?)
    }
}
