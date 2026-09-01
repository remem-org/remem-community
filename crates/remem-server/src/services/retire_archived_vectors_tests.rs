//! Archiving retires a memory from the similarity index.
//!
//! Every path that archives -- an explicit delete, short-term expiry, active
//! forgetting -- has to do this, or the deleted memory keeps costing a row
//! lookup on every search for the whole retention window. And none of them may
//! do it at the cost of `cleanup_archived` finding the record afterwards: the
//! timestamp entry it walks has to survive.
//!
//! A sibling file rather than a module inside the services it exercises,
//! matching the listing and recall tests: the guards in `services/mod.rs`
//! inspect those files as text.

use std::sync::Arc;

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::embedding::EmbeddingService;
use crate::engine::storage::engine::{EngineConfig, StorageEngine};
use crate::services::attrs::memory_schema;
use crate::services::connection_manager::ConnectionManager;
use crate::services::lifecycle_manager::LifecycleManager;
use crate::services::memory_manager::MemoryManager;
use crate::services::repository::MemoryRepository;
use crate::services::types::{memory_key, now_ms, MemoryType, StoredMemory, StoredMetadata};

struct Fixture {
    _dir: tempfile::TempDir,
    repo: Arc<MemoryRepository>,
    manager: MemoryManager,
    lifecycle: LifecycleManager,
    _discovery_rx: mpsc::Receiver<crate::services::connection_manager::DiscoveryTask>,
}

async fn fixture(hard_delete_on_forgetting: bool) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(EngineConfig {
        data_dir: dir.path().to_path_buf(),
        sync_writes: false,
        attr_schema: Some(memory_schema()),
        ..Default::default()
    })
    .await
    .unwrap();
    let repo = Arc::new(MemoryRepository::new(Arc::new(engine)));
    let (tx, rx) = mpsc::channel(64);
    let manager = MemoryManager::new(
        Arc::clone(&repo),
        Arc::new(EmbeddingService::new_for_test()),
        tx,
        // Same bound the server's default configuration uses.
        128,
    );
    let lifecycle = LifecycleManager::new(
        Arc::clone(&repo),
        Arc::new(ConnectionManager::new(Arc::clone(&repo))),
        Arc::new(EmbeddingService::new_for_test()),
        hard_delete_on_forgetting,
    );
    Fixture {
        _dir: dir,
        repo,
        manager,
        lifecycle,
        _discovery_rx: rx,
    }
}

/// Seeds a memory that carries a vector.
///
/// Written through the repository rather than `MemoryManager::create` because
/// the test embedding service refuses to embed -- the same reason the listing
/// and recall tests seed their corpora directly.
async fn seed(f: &Fixture, id: Uuid, memory_type: MemoryType, ttl: Option<u64>, health: f32) {
    // Two days back, not `now`: active forgetting skips anything whose last
    // reinforcement or health check is less than a whole day old, so a corpus
    // stamped with the current time is invisible to that sweep entirely.
    let now = now_ms() - 2 * 86_400_000;
    let stored = StoredMemory {
        id,
        content: format!("record {id}"),
        memory_type,
        metadata: StoredMetadata {
            created_at: now,
            updated_at: now,
            accessed_at: now,
            access_count: 0,
            source: None,
            tags: vec![],
            importance: 0.5,
            emotional_valence: 0.0,
            arousal: 0.0,
            health,
            last_recalled_at: None,
            flashbulb_until: None,
            ttl,
            last_decay_at: None,
            last_health_check_at: None,
        },
        archived: false,
    };
    // Written through `store_memory_core_partitioned`, which is what
    // `MemoryManager::create` calls: it writes the timestamp and tag index
    // entries the lifecycle sweeps walk. The repository's own store helpers
    // write the record and its vector but not those, so a corpus seeded with
    // them is invisible to every sweep under test here.
    f.repo
        .engine
        .store_memory_core_partitioned(
            f.repo.write_target(),
            memory_key(id).as_bytes(),
            serde_json::to_vec(&stored).unwrap(),
            Some(vec![1.0; 384]),
            now,
            &[format!("__type:{}", stored.memory_type)],
            Some(&crate::services::attrs::project(&stored)),
        )
        .await
        .unwrap();
}

fn has_vector(f: &Fixture, id: Uuid) -> bool {
    let key = f
        .repo
        .physical_key_in(&f.repo.write_target().clone(), memory_key(id).as_bytes())
        .unwrap();
    f.repo.engine.get_vector(key.as_ref()).is_some()
}

fn in_time_index(f: &Fixture, id: Uuid) -> bool {
    let key = f
        .repo
        .physical_key_in(&f.repo.write_target().clone(), memory_key(id).as_bytes())
        .unwrap();
    f.repo
        .engine
        .time_range_query_partitioned(f.repo.read_scope(), 0, u64::MAX, None)
        .unwrap()
        .iter()
        .any(|(_ts, k)| k.as_ref() == key.as_ref())
}

#[tokio::test]
async fn soft_delete_retires_the_memory_from_the_similarity_index() {
    let f = fixture(false).await;
    let id = Uuid::new_v4();
    seed(&f, id, MemoryType::ShortTerm, Some(86_400), 100.0).await;
    assert!(has_vector(&f, id));

    f.manager.delete(id, false).await.unwrap();

    assert!(
        !has_vector(&f, id),
        "a soft-deleted memory must stop being a search candidate"
    );
    assert!(
        in_time_index(&f, id),
        "its timestamp entry must survive -- cleanup_archived walks it"
    );
}

#[tokio::test]
async fn hard_delete_still_removes_everything() {
    let f = fixture(false).await;
    let id = Uuid::new_v4();
    seed(&f, id, MemoryType::ShortTerm, Some(86_400), 100.0).await;

    f.manager.delete(id, true).await.unwrap();

    assert!(!has_vector(&f, id));
    assert!(
        !in_time_index(&f, id),
        "a hard delete clears every index, unlike an archive"
    );
}

#[tokio::test]
async fn expiring_a_short_term_memory_retires_it_but_promoting_one_does_not() {
    let f = fixture(false).await;
    let expired = Uuid::new_v4();
    let promoted = Uuid::new_v4();
    // ttl 0 with a creation timestamp of now means both are already expired;
    // access count is what decides which is archived and which is promoted.
    seed(&f, expired, MemoryType::ShortTerm, Some(0), 100.0).await;
    seed(&f, promoted, MemoryType::ShortTerm, Some(0), 100.0).await;

    // Lift the promoted one over the threshold by recalling it.
    for _ in 0..5 {
        f.repo.recall().record(
            &f.repo.write_target().clone(),
            promoted,
            now_ms() + 1_000_000,
        );
        f.lifecycle.flush_recall().await.unwrap();
    }

    f.lifecycle.expire_short_term().await.unwrap();

    assert!(
        !has_vector(&f, expired),
        "an expired, unused memory is archived, so it must be retired"
    );
    assert!(
        has_vector(&f, promoted),
        "a promoted memory is not archived, so it must stay searchable"
    );
    assert!(in_time_index(&f, expired));
}

#[tokio::test]
async fn active_forgetting_retires_a_memory_it_archives_at_zero_health() {
    let f = fixture(false).await;
    let id = Uuid::new_v4();
    seed(&f, id, MemoryType::ShortTerm, None, 0.0).await;

    f.lifecycle.active_forgetting().await.unwrap();

    assert!(
        !has_vector(&f, id),
        "a memory archived by active forgetting must be retired"
    );
    assert!(
        in_time_index(&f, id),
        "cleanup_archived still has to be able to find it"
    );
}

#[tokio::test]
async fn active_forgetting_hard_deletes_without_leaving_index_entries() {
    let f = fixture(true).await;
    let id = Uuid::new_v4();
    seed(&f, id, MemoryType::ShortTerm, None, 0.0).await;

    f.lifecycle.active_forgetting().await.unwrap();

    assert!(!has_vector(&f, id));
    assert!(
        !in_time_index(&f, id),
        "the hard-delete branch clears every index"
    );
}

#[tokio::test]
async fn cleanup_reaches_and_deletes_memories_that_were_archived_and_retired() {
    let f = fixture(false).await;
    let mut ids = Vec::new();
    for _ in 0..5 {
        let id = Uuid::new_v4();
        seed(&f, id, MemoryType::ShortTerm, Some(86_400), 100.0).await;
        f.manager.delete(id, false).await.unwrap();
        ids.push(id);
    }
    let live = Uuid::new_v4();
    seed(&f, live, MemoryType::ShortTerm, Some(86_400), 100.0).await;

    for id in &ids {
        assert!(!has_vector(&f, *id), "precondition: all five were retired");
    }

    // max_age_days of 0 puts the cutoff at now, so every archived record is
    // already past it.
    let deleted = f.lifecycle.cleanup_archived(0).await.unwrap();

    assert_eq!(
        deleted,
        ids.len(),
        "retiring a record must not put it beyond cleanup's reach"
    );
    for id in &ids {
        assert!(
            !in_time_index(&f, *id),
            "cleanup hard-deletes, which clears the timestamp entry too"
        );
    }
    assert!(
        in_time_index(&f, live),
        "a live memory is untouched by cleanup"
    );
    assert!(has_vector(&f, live));
}
