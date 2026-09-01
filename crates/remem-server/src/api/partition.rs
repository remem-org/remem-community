use axum::{body::Body, http::Request, middleware::Next, response::Response};

use crate::api::AppState;
use crate::engine::storage::partition::{PartitionBinding, PartitionScope};
use crate::error::{AppError, Result};
use crate::services::MemoryRepository;

/// Effective partition binding resolved at the request boundary.
///
/// REM-76 keeps the public REST/MCP API shape unchanged. Until REM-42 defines
/// authenticated tenant and namespace semantics, every request is bound to the
/// configured legacy/default partition.
#[derive(Debug, Clone)]
pub struct EffectivePartition {
    read_scope: PartitionScope,
    write_target: PartitionBinding,
}

impl EffectivePartition {
    pub fn new(read_scope: PartitionScope, write_target: PartitionBinding) -> Result<Self> {
        if !read_scope.contains_binding(&write_target) {
            return Err(AppError::Validation(
                "write target partition must be included in read scope".to_string(),
            ));
        }

        Ok(Self {
            read_scope,
            write_target,
        })
    }

    pub fn legacy_default(state: &AppState) -> Self {
        Self::legacy_default_from_repo(&state.services.repo)
    }

    pub fn legacy_default_from_repo(repo: &MemoryRepository) -> Self {
        Self::new(repo.read_scope().clone(), repo.write_target().clone())
            .expect("repository write target must be included in its read scope")
    }

    pub fn read_scope(&self) -> &PartitionScope {
        &self.read_scope
    }

    pub fn write_target(&self) -> &PartitionBinding {
        &self.write_target
    }
}

pub async fn rest_partition_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    req.extensions_mut()
        .insert(EffectivePartition::legacy_default(&state));
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::storage::partition::{PartitionId, TenantId};
    use std::sync::Arc;

    #[tokio::test]
    async fn effective_partition_uses_configured_default_partition() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            crate::engine::storage::engine::StorageEngine::new(
                crate::engine::storage::engine::EngineConfig {
                    data_dir: dir.path().to_path_buf(),
                    default_partition: crate::engine::storage::partition::PartitionId::new(
                        "finance",
                    )
                    .unwrap(),
                    sync_writes: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let repo = crate::services::MemoryRepository::new(engine);
        let partition = EffectivePartition::legacy_default_from_repo(&repo);

        assert_eq!(partition.read_scope().tenant().as_str(), "default");
        assert_eq!(partition.read_scope().partitions()[0].as_str(), "finance");
        assert_eq!(partition.write_target().tenant().as_str(), "default");
        assert_eq!(partition.write_target().partition().as_str(), "finance");
    }

    #[test]
    fn effective_partition_accepts_authorized_multi_partition_reads() {
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionId::new("finance").unwrap();
        let tech = PartitionId::new("tech").unwrap();
        let read_scope =
            PartitionScope::new(tenant.clone(), [finance.clone(), tech.clone()]).unwrap();
        let write_target = PartitionBinding::new(tenant, finance);

        let partition = EffectivePartition::new(read_scope, write_target).unwrap();

        assert_eq!(partition.read_scope().partitions().len(), 2);
        assert_eq!(partition.write_target().partition().as_str(), "finance");
    }

    #[test]
    fn effective_partition_rejects_write_target_outside_read_scope() {
        let tenant = TenantId::new("acme").unwrap();
        let read_scope =
            PartitionScope::new(tenant.clone(), [PartitionId::new("tech").unwrap()]).unwrap();
        let write_target = PartitionBinding::new(tenant, PartitionId::new("finance").unwrap());

        let err = EffectivePartition::new(read_scope, write_target).unwrap_err();

        assert!(matches!(err, AppError::Validation(_)));
    }
}
