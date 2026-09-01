//! Opening a populated data directory written by the previous build.
//!
//! The migration in this change is the only part of it that can fail
//! *silently*. Everything else announces itself: a broken listing returns the
//! wrong page, a broken sweep archives the wrong memory. But a record that
//! comes through the upgrade without an entry in the next-attention index is
//! not merely undated — it is unreachable by the sweep that would date it, so
//! every lifecycle task quietly stops working for it. Nothing errors. Nothing
//! logs. The only symptom is a store that never expires anything, noticed
//! months later.
//!
//! So this rehearses the upgrade rather than trusting it: build a directory
//! the way the previous build would have, open it with this one, and check
//! that maintenance and browsing both work on records that predate the schema.

use std::sync::Arc;

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::embedding::EmbeddingService;
use crate::engine::attr::schema::AttrSchema;
use crate::engine::storage::engine::{EngineConfig, StorageEngine};
use crate::engine::storage::format::FormatManifest;
use crate::services::attrs::{
    memory_schema, project, SLOT_ARCHIVED, SLOT_CREATED_AT, SLOT_NEXT_ATTENTION_AT,
};
use crate::services::connection_manager::ConnectionManager;
use crate::services::cursor::ListCursor;
use crate::services::lifecycle_manager::LifecycleManager;
use crate::services::memory_manager::{MemoryManager, PageStart};
use crate::services::repository::MemoryRepository;
use crate::services::types::{
    now_ms, MemoryFilters, MemoryType, SortBy, SortOrder, StoredMemory, StoredMetadata,
};

/// The row the previous build would have derived: the current projection
/// minus the slot it did not know about, at the version it stamped.
///
/// Deriving it from `project` rather than copying the old body keeps the two
/// from drifting: this fixture is only a rehearsal of an upgrade while the
/// "before" state is genuinely the current state minus this change.
fn previous_project(stored: &StoredMemory) -> crate::engine::attr::row::AttrRow {
    let current = project(stored);
    let mut row = crate::engine::attr::row::AttrRow::new(1);
    for def in previous_schema().slots {
        if let Some(value) = current.get(def.slot) {
            row.set(def.slot, value);
        }
    }
    row
}

/// The schema as it stood before this change: the same nine slots, without
/// the next-attention time, at the version that shipped with them.
fn previous_schema() -> AttrSchema {
    let current = memory_schema();
    AttrSchema {
        version: 1,
        slots: current
            .slots
            .into_iter()
            .filter(|s| s.slot != SLOT_NEXT_ATTENTION_AT)
            .collect(),
    }
}

fn config(data_dir: std::path::PathBuf, schema: AttrSchema) -> EngineConfig {
    // The previous build projected without the new slot; this one projects
    // with it. Choosing by schema version keeps one helper honest for both
    // sides of the upgrade.
    let previous = schema.version == 1;
    EngineConfig {
        data_dir,
        sync_writes: true,
        attr_schema: Some(schema),
        attr_project: Some(Arc::new(move |bytes: &[u8]| {
            serde_json::from_slice::<StoredMemory>(bytes).ok().map(|s| {
                if previous {
                    previous_project(&s)
                } else {
                    project(&s)
                }
            })
        })),
        attr_index_exempt: Some(Arc::new(|row: &crate::engine::attr::row::AttrRow| {
            let archived = matches!(
                row.get(SLOT_ARCHIVED),
                Some(crate::engine::attr::value::AttrValue::Bool(true))
            );
            if archived {
                vec![SLOT_CREATED_AT]
            } else {
                Vec::new()
            }
        })),
        ..Default::default()
    }
}

fn memory(i: u32, archived: bool, memory_type: MemoryType, ttl: Option<u64>) -> StoredMemory {
    // Created well in the past, so anything with a TTL is already expired and
    // anything decaying is already a day overdue: the upgrade inherits a
    // backlog, which is the realistic case.
    let created_at = now_ms() - 10 * 86_400_000;
    StoredMemory {
        id: Uuid::from_u128(i as u128 + 1),
        content: format!("record {i}"),
        memory_type,
        metadata: StoredMetadata {
            created_at,
            updated_at: created_at,
            accessed_at: created_at,
            access_count: 0,
            source: None,
            tags: vec![],
            importance: 0.5,
            emotional_valence: 0.0,
            arousal: 0.0,
            health: 100.0,
            last_recalled_at: None,
            flashbulb_until: None,
            ttl,
            last_decay_at: None,
            last_health_check_at: None,
        },
        archived,
    }
}

/// Populate a directory the way the previous build would have left it, and
/// mark its format accordingly.
async fn data_directory_from_the_previous_build(dir: &std::path::Path) -> Vec<StoredMemory> {
    let engine = Arc::new(
        StorageEngine::new(config(dir.to_path_buf(), previous_schema()))
            .await
            .unwrap(),
    );
    let repo = MemoryRepository::new(Arc::clone(&engine));

    let mut written = Vec::new();
    for i in 0..30u32 {
        let m = match i % 3 {
            0 => memory(i, false, MemoryType::ShortTerm, Some(3600)),
            1 => memory(i, false, MemoryType::LongTerm, None),
            _ => memory(i, i % 6 == 2, MemoryType::LongTerm, None),
        };
        // `store_memory_core_partitioned`, the call `MemoryManager::create`
        // makes -- not `repo.store`. The difference matters: only this path
        // writes the timestamp entry, and the upgrade backfill walks the time
        // index to find records. A fixture that stored without timestamps
        // would present the backfill with an empty corpus and prove nothing.
        let mut tags = m.metadata.tags.clone();
        tags.push(format!("__type:{}", m.memory_type));
        engine
            .store_memory_core_partitioned(
                repo.write_target(),
                crate::services::types::memory_key(m.id).as_bytes(),
                serde_json::to_vec(&m).unwrap(),
                None,
                m.metadata.created_at,
                &tags,
                Some(&previous_project(&m)),
            )
            .await
            .unwrap();
        written.push(m);
    }
    engine.checkpoint().await.unwrap();
    drop(repo);
    drop(engine);

    // The previous build did not know about `attrs-v2`, so its FORMAT records
    // the attribute subsystem one version back. Writing it here is what makes
    // the reopen below an upgrade rather than an ordinary start.
    let mut manifest = FormatManifest::read(dir).unwrap();
    manifest.set("attr", 1);
    manifest.write_atomic(dir).unwrap();

    written
}

#[tokio::test]
async fn a_directory_from_the_previous_build_upgrades_and_stays_maintained() {
    let dir = tempfile::tempdir().unwrap();
    let written = data_directory_from_the_previous_build(dir.path()).await;
    let live: std::collections::HashSet<Uuid> = written
        .iter()
        .filter(|m| !m.archived)
        .map(|m| m.id)
        .collect();
    assert!(!live.is_empty(), "the fixture must hold live memories");

    // ── The upgrade ────────────────────────────────────────────────────────
    let engine = Arc::new(
        StorageEngine::new(config(dir.path().to_path_buf(), memory_schema()))
            .await
            .unwrap(),
    );

    let manifest = FormatManifest::read(dir.path()).unwrap();
    assert_eq!(
        manifest.version_of("attr"),
        2,
        "the upgrade must advance the attribute subsystem"
    );

    // ── Every record carries a next-attention time ─────────────────────────
    //
    // The silent failure this whole rehearsal exists to catch: a record with
    // no value here is invisible to every sweep, forever.
    let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));
    for m in &written {
        let key = repo.physical_memory_key(m.id).unwrap();
        let row = engine
            .get_attrs(key.as_ref())
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("record {} lost its attribute row in the upgrade", m.id));
        assert!(
            matches!(
                row.get(SLOT_NEXT_ATTENTION_AT),
                Some(crate::engine::attr::value::AttrValue::U64(_))
            ),
            "record {} came through the upgrade with no next-attention time, so no \
             sweep can ever reach it",
            m.id
        );
    }

    // ── Maintenance works on records that predate the schema ───────────────
    let connection = Arc::new(ConnectionManager::new(Arc::clone(&repo)));
    let lifecycle = LifecycleManager::new(
        Arc::clone(&repo),
        connection,
        Arc::new(EmbeddingService::new_for_test()),
        false,
    );

    let expired = lifecycle.expire_short_term().await.unwrap();
    assert!(
        expired > 0,
        "the fixture's short-term memories are ten days past their TTL; a sweep \
         that finds none of them is a sweep that cannot see pre-upgrade records"
    );

    let decayed = lifecycle.apply_importance_decay().await.unwrap();
    assert!(
        decayed > 0,
        "long-term memories ten days old are overdue for decay"
    );

    // ── Browsing works, and archived records are out of the order ──────────
    let (tx, _rx) = mpsc::channel(1);
    let manager = MemoryManager::new(
        Arc::clone(&repo),
        Arc::new(EmbeddingService::new_for_test()),
        tx,
        128,
    );

    let mut seen: Vec<Uuid> = Vec::new();
    let mut start = PageStart::Beginning;
    loop {
        let page = manager
            .list_page(
                &MemoryFilters::default(),
                SortBy::CreatedAt,
                SortOrder::Ascending,
                4,
                start,
            )
            .await
            .unwrap();
        seen.extend(page.memories.iter().map(|m| m.id));
        match page.next_cursor {
            Some(token) if page.has_more => {
                start = PageStart::After(ListCursor::decode(&token).unwrap());
            }
            _ => break,
        }
        assert!(seen.len() < 1_000, "paging failed to terminate");
    }

    let unique: std::collections::HashSet<Uuid> = seen.iter().copied().collect();
    assert_eq!(
        seen.len(),
        unique.len(),
        "a continuation sequence over upgraded records returned a memory twice"
    );
    for m in written.iter().filter(|m| m.archived) {
        assert!(
            !unique.contains(&m.id),
            "an already-archived record was left in the browsing order by the upgrade"
        );
    }
}

/// An upgrade interrupted before it finished runs again on the next start.
/// It has to converge, or a crash during the upgrade leaves a store whose
/// maintenance is permanently half-broken.
#[tokio::test]
async fn an_interrupted_upgrade_converges_when_it_runs_again() {
    let dir = tempfile::tempdir().unwrap();
    let written = data_directory_from_the_previous_build(dir.path()).await;

    let mut states = Vec::new();
    for _ in 0..2 {
        // Rewind the format to stand for an upgrade that died before it
        // could record that it finished. Rewinding FORMAT rather than
        // replacing the marker directly re-runs the migration as well as the
        // backfill, which is the whole path a crashed upgrade repeats.
        let mut manifest = FormatManifest::read(dir.path()).unwrap();
        manifest.set("attr", 1);
        manifest.write_atomic(dir.path()).unwrap();

        let engine = Arc::new(
            StorageEngine::new(config(dir.path().to_path_buf(), memory_schema()))
                .await
                .unwrap(),
        );
        let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));

        let mut due = Vec::new();
        for m in &written {
            let key = repo.physical_memory_key(m.id).unwrap();
            let value = engine
                .get_attrs(key.as_ref())
                .await
                .unwrap()
                .and_then(|row| row.get(SLOT_NEXT_ATTENTION_AT));
            due.push((m.id, value));
        }
        states.push(due);
        drop(repo);
        drop(engine);
    }

    assert_eq!(
        states[0], states[1],
        "running the upgrade twice must leave the same state as running it once"
    );
}
