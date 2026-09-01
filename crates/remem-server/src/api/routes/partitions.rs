use std::collections::BTreeMap;

use axum::{extract::Query, extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::api::AppState;
use crate::engine::storage::partition::{decode_record_key, PartitionBinding};
use crate::engine::StorageEngine;
use crate::error::{AppError, Result};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

#[derive(Debug, Deserialize)]
pub struct MaintenanceAuditQuery {
    pub actor: String,
    pub purpose: String,
}

#[derive(Debug, Deserialize)]
pub struct PartitionRecordQuery {
    pub actor: String,
    pub purpose: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct MaintenanceAudit {
    pub actor: String,
    pub purpose: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PartitionAccounting {
    pub tenant: String,
    pub partition: String,
    pub record_count: usize,
    pub vector_count: usize,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PartitionInventoryResponse {
    pub success: bool,
    pub audit: MaintenanceAudit,
    pub partitions: Vec<PartitionAccounting>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PartitionRecordRef {
    pub tenant: String,
    pub partition: String,
    pub physical_key: String,
    pub logical_key: String,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PartitionRecordTraversalResponse {
    pub success: bool,
    pub audit: MaintenanceAudit,
    pub records: Vec<PartitionRecordRef>,
    pub limit: usize,
    pub offset: usize,
    pub returned: usize,
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/partitions",
    params(
        ("actor" = String, Query, description = "Actor performing the maintenance read"),
        ("purpose" = String, Query, description = "Purpose for the cross-partition maintenance read")
    ),
    responses(
        (status = 200, description = "Partition inventory and per-partition accounting", body = PartitionInventoryResponse),
        (status = 400, description = "Audit context absent from the query string", body = ErrorResponse),
        (status = 422, description = "Audit context present but empty", body = ErrorResponse),
    ),
    tag = "admin"
)]
pub async fn list_partitions(
    State(state): State<AppState>,
    Query(query): Query<MaintenanceAuditQuery>,
) -> Result<Json<PartitionInventoryResponse>> {
    let audit = audit_context(query.actor, query.purpose)?;
    tracing::info!(
        actor = %audit.actor,
        purpose = %audit.purpose,
        "cross-partition partition inventory requested"
    );

    Ok(Json(PartitionInventoryResponse {
        success: true,
        audit,
        partitions: collect_partition_inventory(&state.services.engine)?,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/partitions/records",
    params(
        ("actor" = String, Query, description = "Actor performing the maintenance read"),
        ("purpose" = String, Query, description = "Purpose for the cross-partition maintenance read"),
        ("limit" = Option<usize>, Query, description = "Maximum records to return"),
        ("offset" = Option<usize>, Query, description = "Number of records to skip")
    ),
    responses(
        (status = 200, description = "Cross-partition record-key traversal", body = PartitionRecordTraversalResponse),
        (status = 400, description = "Audit context absent from the query string", body = ErrorResponse),
        (status = 422, description = "Audit context present but empty", body = ErrorResponse),
    ),
    tag = "admin"
)]
pub async fn list_partition_records(
    State(state): State<AppState>,
    Query(query): Query<PartitionRecordQuery>,
) -> Result<Json<PartitionRecordTraversalResponse>> {
    let audit = audit_context(query.actor, query.purpose)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let offset = query.offset.unwrap_or(0);
    tracing::info!(
        actor = %audit.actor,
        purpose = %audit.purpose,
        limit,
        offset,
        "cross-partition record traversal requested"
    );

    let records = collect_partition_records(&state.services.engine, limit, offset)?;
    let returned = records.len();
    Ok(Json(PartitionRecordTraversalResponse {
        success: true,
        audit,
        records,
        limit,
        offset,
        returned,
    }))
}

fn audit_context(actor: String, purpose: String) -> Result<MaintenanceAudit> {
    let actor = actor.trim().to_owned();
    let purpose = purpose.trim().to_owned();
    if actor.is_empty() || purpose.is_empty() {
        return Err(AppError::Validation(
            "cross-partition maintenance requires non-empty actor and purpose".to_string(),
        ));
    }
    Ok(MaintenanceAudit { actor, purpose })
}

fn collect_partition_inventory(engine: &StorageEngine) -> Result<Vec<PartitionAccounting>> {
    let mut counts = BTreeMap::<(String, String), PartitionAccounting>::new();
    for (_timestamp, key) in engine.maintenance_time_range_query(0, u64::MAX, None)? {
        let Some((binding, _logical_key)) = binding_and_logical_key_for_record(engine, &key)?
        else {
            continue;
        };
        counts
            .entry(accounting_key(&binding))
            .or_insert_with(|| accounting_for_binding(&binding))
            .record_count += 1;
    }

    for (binding, vector_count) in engine.partition_vector_counts() {
        counts
            .entry(accounting_key(&binding))
            .or_insert_with(|| accounting_for_binding(&binding))
            .vector_count = vector_count;
    }

    Ok(counts.into_values().collect())
}

fn collect_partition_records(
    engine: &StorageEngine,
    limit: usize,
    offset: usize,
) -> Result<Vec<PartitionRecordRef>> {
    let mut records = Vec::new();
    let mut matched = 0usize;
    for (timestamp, key) in engine.maintenance_time_range_query(0, u64::MAX, None)? {
        let Some((binding, logical_key)) = binding_and_logical_key_for_record(engine, &key)? else {
            continue;
        };
        if matched < offset {
            matched += 1;
            continue;
        }
        if records.len() >= limit {
            break;
        }
        matched += 1;
        records.push(PartitionRecordRef {
            tenant: binding.tenant().as_str().to_owned(),
            partition: binding.partition().as_str().to_owned(),
            physical_key: String::from_utf8_lossy(&key).into_owned(),
            logical_key: String::from_utf8_lossy(&logical_key).into_owned(),
            timestamp,
        });
    }

    Ok(records)
}

fn binding_and_logical_key_for_record(
    engine: &StorageEngine,
    key: &[u8],
) -> Result<Option<(PartitionBinding, Vec<u8>)>> {
    if let Some(decoded) =
        decode_record_key(key).map_err(|err| AppError::Validation(err.to_string()))?
    {
        if decoded.logical_key().starts_with(b"memory:") {
            return Ok(Some((
                decoded.binding().clone(),
                decoded.logical_key().to_vec(),
            )));
        }
        return Ok(None);
    }

    if key.starts_with(b"memory:") {
        return Ok(Some((engine.default_partition_binding(), key.to_vec())));
    }

    Ok(None)
}

fn accounting_key(binding: &PartitionBinding) -> (String, String) {
    (
        binding.tenant().as_str().to_owned(),
        binding.partition().as_str().to_owned(),
    )
}

fn accounting_for_binding(binding: &PartitionBinding) -> PartitionAccounting {
    PartitionAccounting {
        tenant: binding.tenant().as_str().to_owned(),
        partition: binding.partition().as_str().to_owned(),
        record_count: 0,
        vector_count: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::storage::engine::EngineConfig;
    use crate::engine::storage::partition::{PartitionId, TenantId};
    use crate::services::types::memory_key;
    use uuid::Uuid;

    #[test]
    fn audit_context_requires_actor_and_purpose() {
        assert!(audit_context("operator".to_string(), "repair".to_string()).is_ok());
        assert!(audit_context("".to_string(), "repair".to_string()).is_err());
        assert!(audit_context("operator".to_string(), " ".to_string()).is_err());
    }

    #[tokio::test]
    async fn inventory_counts_records_and_vectors_without_payload_reads() {
        let engine = StorageEngine::new(EngineConfig {
            data_dir: tempfile::tempdir().unwrap().keep(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let tech = PartitionBinding::new(tenant, PartitionId::new("tech").unwrap());
        let finance_id = Uuid::new_v4();
        let tech_id = Uuid::new_v4();

        engine
            .store_memory_core_partitioned(
                &finance,
                memory_key(finance_id).as_bytes(),
                bytes::Bytes::from_static(b"finance payload"),
                Some(vec![0.1; 384]),
                10,
                &[],
                None,
            )
            .await
            .unwrap();
        engine
            .store_memory_core_partitioned(
                &tech,
                memory_key(tech_id).as_bytes(),
                bytes::Bytes::from_static(b"tech payload"),
                None,
                20,
                &[],
                None,
            )
            .await
            .unwrap();

        engine.reset_payload_reads();
        let inventory = collect_partition_inventory(&engine).unwrap();

        let finance_row = inventory
            .iter()
            .find(|row| row.tenant == "acme" && row.partition == "finance")
            .unwrap();
        let tech_row = inventory
            .iter()
            .find(|row| row.tenant == "acme" && row.partition == "tech")
            .unwrap();
        assert_eq!(finance_row.record_count, 1);
        assert_eq!(finance_row.vector_count, 1);
        assert_eq!(tech_row.record_count, 1);
        assert_eq!(tech_row.vector_count, 0);
        assert_eq!(engine.payload_reads(), 0);
    }
}
