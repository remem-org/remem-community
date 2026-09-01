//! What counts as a recall, and what a recall costs.
//!
//! Recall is defined by intent, not by HTTP verb: addressing a memory by its
//! identifier is a recall, discovering memories by query is not. These tests
//! pin both halves of that, plus the coalescing rule that makes
//! `access_count` measure recall sessions rather than round trips.
//!
//! A sibling file rather than a module inside `memory_manager.rs` because the
//! listing-path guard in `services/mod.rs` inspects that file as text.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::embedding::EmbeddingService;
use crate::engine::storage::engine::{EngineConfig, StorageEngine};
use crate::engine::{QueryEngine, QueryEngineConfig};
use crate::services::attrs::memory_schema;
use crate::services::connection_manager::ConnectionManager;
use crate::services::lifecycle_manager::LifecycleManager;
use crate::services::memory_manager::{MemoryManager, UpdatePatch};
use crate::services::repository::MemoryRepository;
use crate::services::search_engine::{SearchEngine, SearchQuery};
use crate::services::types::{
    memory_key, MemoryFilters, MemoryType, SearchType, SortBy, SortOrder, StoredMemory,
    StoredMetadata,
};

struct Fixture {
    _dir: tempfile::TempDir,
    repo: Arc<MemoryRepository>,
    manager: MemoryManager,
    _discovery_rx: mpsc::Receiver<crate::services::connection_manager::DiscoveryTask>,
}

async fn fixture() -> Fixture {
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
    Fixture {
        _dir: dir,
        repo,
        manager,
        _discovery_rx: rx,
    }
}

/// An empty patch. `UpdatePatch` has no `Default` in production and this
/// doesn't add one — a patch of all-`None` is a meaningful request (it still
/// counts as a recall), so it should be spelled out where it's used.
fn patch() -> UpdatePatch {
    UpdatePatch {
        content: None,
        tags: None,
        importance: None,
        emotional_valence: None,
        arousal: None,
        health: None,
        source: None,
    }
}

/// Writes a record the way `MemoryManager::create` does, minus the embedding.
///
/// The test embedding service refuses to embed, so these tests cannot go
/// through `create` -- the same reason the listing tests write their corpus
/// directly. `store_memory_core_partitioned` is what `create` calls, so the
/// timestamp and tag index entries the lifecycle tasks walk are present.
async fn create(f: &Fixture, content: &str) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let now = crate::services::types::now_ms();
    let stored = StoredMemory {
        id,
        content: content.to_string(),
        memory_type: MemoryType::ShortTerm,
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
            health: 100.0,
            last_recalled_at: None,
            flashbulb_until: None,
            // Long enough that `expire_short_term` leaves it alone.
            ttl: Some(86_400),
            last_decay_at: None,
            last_health_check_at: None,
        },
        archived: false,
    };
    f.repo
        .engine
        .store_memory_core_partitioned(
            f.repo.write_target(),
            memory_key(id).as_bytes(),
            serde_json::to_vec(&stored).unwrap(),
            None,
            now,
            &[format!("__type:{}", stored.memory_type)],
            Some(&crate::services::attrs::project(&stored)),
        )
        .await
        .unwrap();
    id
}

// ── What a recall costs ────────────────────────────────────────────────────

/// The point of the change: a read is a read. Recording recall used to
/// re-serialize the whole record and append to the WAL with an fsync inside
/// the engine's global write lock, which put every read in contention with
/// every write in the process.
#[tokio::test]
async fn fetching_a_memory_performs_no_durable_write() {
    let f = fixture().await;
    let id = create(&f, "content").await;

    f.repo.engine.reset_wal_appends();
    for _ in 0..25 {
        f.manager.get(id).await.unwrap();
    }

    assert_eq!(
        f.repo.engine.wal_appends(),
        0,
        "a fetch must not append to the WAL"
    );
}

// ── What counts as a recall ────────────────────────────────────────────────

#[tokio::test]
async fn fetching_by_identifier_records_a_recall() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    let binding = f.repo.resolve_binding(id).await.unwrap().unwrap();

    let returned = f.manager.get(id).await.unwrap();

    assert!(
        f.repo.recall().peek(&binding, id).is_some(),
        "the fetch must record a recall"
    );
    // The caller sees its own recall even though nothing is persisted yet.
    assert_eq!(returned.metadata.access_count, 1);
    assert!(returned.metadata.last_recalled_at.is_some());
}

/// A memory an agent revises daily is a memory in active use. Before this
/// change `update` left access metadata untouched, so it decayed on the same
/// curve as one nobody had opened since it was written.
#[tokio::test]
async fn updating_by_identifier_records_a_recall() {
    let f = fixture().await;
    let id = create(&f, "content").await;

    let updated = f
        .manager
        .update(
            id,
            UpdatePatch {
                importance: Some(0.9),
                ..patch()
            },
        )
        .await
        .unwrap();

    assert_eq!(updated.metadata.access_count, 1);
    // The update already wrote the record, so the recall is durable now.
    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert_eq!(stored.metadata.access_count, 1);
    assert!(stored.metadata.last_recalled_at.is_some());
}

#[tokio::test]
async fn updating_reinforces_health() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    {
        let (binding, mut stored) = f.repo.load_bound(id).await.unwrap().unwrap();
        stored.metadata.health = 40.0;
        f.repo.store_in(&binding, &mut stored).await.unwrap();
    }

    f.manager
        .update(
            id,
            UpdatePatch {
                importance: Some(0.9),
                ..patch()
            },
        )
        .await
        .unwrap();

    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert_eq!(stored.metadata.health, 50.0);
}

/// The recall is the act of addressing the memory, not the act of changing
/// it: a no-op patch still means a client had this memory in hand.
#[tokio::test]
async fn an_update_that_changes_nothing_still_records_a_recall() {
    let f = fixture().await;
    let id = create(&f, "content").await;

    f.manager.update(id, patch()).await.unwrap();

    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert_eq!(stored.metadata.access_count, 1);
}

/// The coalescing rule. A client that fetches a memory and then updates it
/// performed one interaction, and `access_count` measures interactions --
/// otherwise the promotion threshold would be reached twice as fast by any
/// client that happens to read before it writes.
#[tokio::test]
async fn a_fetch_then_update_in_one_window_counts_once() {
    let f = fixture().await;
    let id = create(&f, "content").await;

    f.manager.get(id).await.unwrap();
    f.manager
        .update(
            id,
            UpdatePatch {
                importance: Some(0.9),
                ..patch()
            },
        )
        .await
        .unwrap();

    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert_eq!(
        stored.metadata.access_count, 1,
        "one interaction is one recall, however many round trips it took"
    );
    assert_eq!(stored.metadata.health, 100.0);
}

#[tokio::test]
async fn repeated_fetches_in_one_window_count_once() {
    let f = fixture().await;
    let id = create(&f, "content").await;

    for _ in 0..10 {
        f.manager.get(id).await.unwrap();
    }
    // Force the fold by writing the record.
    f.manager.update(id, patch()).await.unwrap();

    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert_eq!(stored.metadata.access_count, 1);
}

// ── What does not count ────────────────────────────────────────────────────

#[tokio::test]
async fn fetching_an_absent_memory_records_nothing() {
    let f = fixture().await;

    f.manager
        .get(uuid::Uuid::new_v4())
        .await
        .expect_err("an absent memory must not be returned");

    assert!(
        f.repo.recall().is_empty(),
        "a fetch that found nothing must record no recall"
    );
}

#[tokio::test]
async fn fetching_an_archived_memory_records_nothing() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    {
        let (binding, mut stored) = f.repo.load_bound(id).await.unwrap().unwrap();
        stored.archived = true;
        f.repo.store_in(&binding, &mut stored).await.unwrap();
    }

    f.manager
        .get(id)
        .await
        .expect_err("an archived memory reads as absent");

    assert!(f.repo.recall().is_empty());
}

#[tokio::test]
async fn listing_records_no_recall() {
    let f = fixture().await;
    for i in 0..5 {
        create(&f, &format!("content {i}")).await;
    }

    f.manager
        .list(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            10,
            0,
        )
        .await
        .unwrap();

    assert!(
        f.repo.recall().is_empty(),
        "enumerating memories is discovery, not recall"
    );
}

#[tokio::test]
async fn searching_records_no_recall() {
    let f = fixture().await;
    for i in 0..5 {
        create(&f, &format!("content {i}")).await;
    }
    let search = SearchEngine::new(
        Arc::clone(&f.repo),
        Arc::new(QueryEngine::new(
            Arc::clone(&f.repo.engine),
            QueryEngineConfig::default(),
        )),
        Arc::new(EmbeddingService::new_for_test()),
    );

    search
        .search(&SearchQuery {
            query: "content".to_string(),
            search_type: SearchType::Keyword,
            limit: 10,
            filters: MemoryFilters::default(),
            related_to: None,
        })
        .await
        .unwrap();

    assert!(
        f.repo.recall().is_empty(),
        "search results were discovered, not addressed"
    );
}

/// Traversal is discovery too — including the memory it started from. A
/// rendered graph is a map, not a reading of everything on it.
#[tokio::test]
async fn graph_traversal_records_no_recall() {
    let f = fixture().await;
    let a = create(&f, "first").await;
    let b = create(&f, "second").await;
    let connections = ConnectionManager::new(Arc::clone(&f.repo));
    connections
        .create(
            a,
            b,
            crate::services::types::RelationshipType::RelatedTo,
            1.0,
        )
        .await
        .unwrap();

    connections.find_related(a, 1, &[]).await.unwrap();

    assert!(
        f.repo.recall().is_empty(),
        "neither the centre nor the reached memories were addressed by the caller"
    );
}

#[tokio::test]
async fn lifecycle_tasks_record_no_recall() {
    let f = fixture().await;
    for i in 0..5 {
        create(&f, &format!("content {i}")).await;
    }
    let lifecycle = LifecycleManager::new(
        Arc::clone(&f.repo),
        Arc::new(ConnectionManager::new(Arc::clone(&f.repo))),
        Arc::new(EmbeddingService::new_for_test()),
        false,
    );

    lifecycle.expire_short_term().await.unwrap();
    lifecycle.apply_importance_decay().await.unwrap();
    lifecycle.active_forgetting().await.unwrap();
    lifecycle.cleanup_archived(30).await.unwrap();

    assert!(
        f.repo.recall().is_empty(),
        "background maintenance is not a recall"
    );
}

// ── Flushing ───────────────────────────────────────────────────────────────

fn lifecycle(f: &Fixture) -> LifecycleManager {
    LifecycleManager::new(
        Arc::clone(&f.repo),
        Arc::new(ConnectionManager::new(Arc::clone(&f.repo))),
        Arc::new(EmbeddingService::new_for_test()),
        false,
    )
}

#[tokio::test]
async fn the_flush_persists_recalls_no_write_has_carried() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    f.manager.get(id).await.unwrap();

    // Nothing has written the record, so the recall is still only in memory.
    assert_eq!(
        f.repo
            .load(id)
            .await
            .unwrap()
            .unwrap()
            .metadata
            .access_count,
        0
    );

    let written = lifecycle(&f).flush_recall().await.unwrap();

    assert_eq!(written, 1);
    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert_eq!(stored.metadata.access_count, 1);
    assert!(f.repo.recall().is_empty(), "the flush must consume the log");
}

#[tokio::test]
async fn a_flush_with_nothing_pending_writes_nothing() {
    let f = fixture().await;
    create(&f, "content").await;

    f.repo.engine.reset_wal_appends();
    let written = lifecycle(&f).flush_recall().await.unwrap();

    assert_eq!(written, 0);
    assert_eq!(
        f.repo.engine.wal_appends(),
        0,
        "an empty flush must not touch the WAL"
    );
}

/// Coalescing is per window, not for all time: a memory recalled again after
/// a flush counts again.
#[tokio::test]
async fn recalls_in_two_windows_count_twice() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    let lc = lifecycle(&f);

    f.manager.get(id).await.unwrap();
    lc.flush_recall().await.unwrap();
    f.manager.get(id).await.unwrap();
    lc.flush_recall().await.unwrap();

    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert_eq!(stored.metadata.access_count, 2);
    assert_eq!(stored.metadata.health, 100.0);
}

/// A recall for a memory deleted while it was pending has nothing to be
/// written to. It must be dropped rather than retried forever.
#[tokio::test]
async fn a_recall_for_a_deleted_memory_is_discarded() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    let binding = f.repo.resolve_binding(id).await.unwrap().unwrap();
    f.manager.get(id).await.unwrap();
    f.repo.delete_in(&binding, id).await.unwrap();

    let written = lifecycle(&f).flush_recall().await.unwrap();

    assert_eq!(written, 0);
    assert!(
        f.repo.recall().is_empty(),
        "an unwritable recall must not accumulate forever"
    );
}

// ── Forgetting ordering ────────────────────────────────────────────────────

/// The guarantee that makes recall safe to batch: the sweep reads health to
/// decide what to archive, so it has to see recalls that have not been
/// written yet. Without the drain this memory decays as though untouched.
#[tokio::test]
async fn a_memory_recalled_before_the_sweep_is_not_decayed_as_untouched() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    let two_days_ago = crate::services::types::now_ms() - 2 * 86_400_000;
    {
        let (binding, mut stored) = f.repo.load_bound(id).await.unwrap().unwrap();
        stored.metadata.health = 20.0;
        stored.metadata.accessed_at = two_days_ago;
        stored.metadata.last_recalled_at = Some(two_days_ago);
        stored.metadata.last_health_check_at = Some(two_days_ago);
        f.repo.store_in(&binding, &mut stored).await.unwrap();
    }

    // Recalled right now, but nothing has written it yet.
    f.manager.get(id).await.unwrap();
    lifecycle(&f).active_forgetting().await.unwrap();

    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert!(
        !stored.archived,
        "a memory recalled moments ago must not be forgotten"
    );
    assert_eq!(
        stored.metadata.last_recalled_at,
        Some(stored.metadata.accessed_at),
        "the pending recall must have landed before the sweep judged health"
    );
    assert!(
        stored.metadata.health > 20.0,
        "the recall's reinforcement must be visible to the sweep, got {}",
        stored.metadata.health
    );
}

/// Promotion reads `access_count`, so it observes a recall once the flush has
/// carried it — one interval late, never lost.
#[tokio::test]
async fn promotion_fires_from_flushed_recall_counts() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    let lc = lifecycle(&f);

    // Three recall sessions, each in its own window: the default promote
    // threshold.
    for _ in 0..3 {
        f.manager.get(id).await.unwrap();
        lc.flush_recall().await.unwrap();
    }
    assert_eq!(
        f.repo
            .load(id)
            .await
            .unwrap()
            .unwrap()
            .metadata
            .access_count,
        3
    );

    // Expire it so the promote-or-archive branch runs.
    {
        let (binding, mut stored) = f.repo.load_bound(id).await.unwrap().unwrap();
        stored.metadata.ttl = Some(1);
        stored.metadata.created_at = crate::services::types::now_ms() - 10_000;
        f.repo.store_in(&binding, &mut stored).await.unwrap();
    }
    lc.expire_short_term().await.unwrap();

    let stored = f.repo.load(id).await.unwrap().unwrap();
    assert_eq!(
        stored.memory_type,
        MemoryType::LongTerm,
        "a memory recalled enough times must still be promoted"
    );
}

// ── One answer per memory, whatever asked ──────────────────────────────────

/// Opening a memory and then listing it must report the same use count. They
/// used to disagree for up to a flush interval: the fetch merged its pending
/// recall into what it returned, and every other path read the stored record
/// and missed it. A caller could not tell which number was the memory's
/// actual state.
#[tokio::test]
async fn a_listing_reports_the_same_use_count_as_a_fetch() {
    let f = fixture().await;
    let id = create(&f, "content").await;

    let fetched = f.manager.get(id).await.unwrap();
    assert_eq!(fetched.metadata.access_count, 1);

    let listed = f
        .manager
        .list(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            10,
            0,
        )
        .await
        .unwrap();
    let listed = listed
        .iter()
        .find(|m| m.id == id)
        .expect("the memory must be listed");

    assert_eq!(
        listed.metadata.access_count, fetched.metadata.access_count,
        "the same memory must not report two different use counts at the same instant"
    );
    assert_eq!(listed.metadata.accessed_at, fetched.metadata.accessed_at);
    assert_eq!(listed.metadata.health, fetched.metadata.health);
}

/// Search discovers rather than addresses, so it records nothing of its own
/// -- but it still has to report what other operations recorded.
#[tokio::test]
async fn a_search_reports_the_same_use_count_as_a_fetch() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    let fetched = f.manager.get(id).await.unwrap();

    let search = SearchEngine::new(
        Arc::clone(&f.repo),
        Arc::new(QueryEngine::new(
            Arc::clone(&f.repo.engine),
            QueryEngineConfig::default(),
        )),
        Arc::new(EmbeddingService::new_for_test()),
    );
    let results = search
        .search(&SearchQuery {
            query: "content".to_string(),
            search_type: SearchType::Keyword,
            limit: 10,
            filters: MemoryFilters::default(),
            related_to: None,
        })
        .await
        .unwrap();

    let hit = results
        .results
        .iter()
        .find(|r| r.memory.id == id)
        .expect("the memory must be found");
    assert_eq!(
        hit.memory.metadata.access_count, fetched.metadata.access_count,
        "search must report recall it did not record but that has happened"
    );
    assert!(
        !f.repo.recall().is_empty(),
        "and reporting it must not consume it -- the write still has to happen"
    );
}

/// Reading the pending recall must not spend it: the flush that follows still
/// has to carry it to disk.
#[tokio::test]
async fn reporting_pending_recall_does_not_consume_it() {
    let f = fixture().await;
    let id = create(&f, "content").await;
    f.manager.get(id).await.unwrap();

    // Every read path that folds the delta, twice over.
    for _ in 0..2 {
        f.manager
            .list(
                &MemoryFilters::default(),
                SortBy::CreatedAt,
                SortOrder::Ascending,
                10,
                0,
            )
            .await
            .unwrap();
    }

    let written = lifecycle(&f).flush_recall().await.unwrap();
    assert_eq!(
        written, 1,
        "the recall must still reach disk after being reported"
    );

    let listed = f
        .manager
        .list(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            10,
            0,
        )
        .await
        .unwrap();
    assert_eq!(
        listed[0].metadata.access_count, 1,
        "and it must be counted once, not once per report"
    );
}

// ── Promotion sees what has been recorded ──────────────────────────────────

/// Promotion chooses between promoting a memory and archiving it as unused,
/// and it reads `access_count` to decide. A recall recorded moments earlier
/// must count, or a memory someone just used is archived as untouched.
#[tokio::test]
async fn the_expiry_sweep_observes_a_recall_recorded_just_before_it() {
    let f = fixture().await;
    let id = create(&f, "content").await;

    // Expire it, so the sweep has to choose between promoting and archiving.
    {
        let (binding, mut stored) = f.repo.load_bound(id).await.unwrap().unwrap();
        stored.metadata.created_at = crate::services::types::now_ms() - 10_000;
        stored.metadata.ttl = Some(1);
        f.repo.store_in(&binding, &mut stored).await.unwrap();
    }

    // Recalls, not yet flushed. The default promotion threshold is 5.
    for _ in 0..6 {
        f.manager.get(id).await.unwrap();
        lifecycle(&f).flush_recall().await.unwrap();
    }
    f.manager.get(id).await.unwrap();
    assert!(
        !f.repo.recall().is_empty(),
        "premise: the last recall must still be pending when the sweep runs"
    );

    lifecycle(&f).expire_short_term().await.unwrap();

    let (_, stored) = f.repo.load_bound(id).await.unwrap().unwrap();
    assert!(
        !stored.archived,
        "a memory used past the promotion threshold must not be archived as unused"
    );
    assert_eq!(stored.memory_type, MemoryType::LongTerm);
}

/// The drain is unconditional, but it must cost nothing when there is
/// nothing to drain.
#[tokio::test]
async fn the_expiry_sweep_writes_nothing_extra_when_no_recall_is_pending() {
    let f = fixture().await;
    create(&f, "content").await;
    assert!(f.repo.recall().is_empty());

    let written = lifecycle(&f).flush_recall().await.unwrap();
    assert_eq!(written, 0);

    lifecycle(&f).expire_short_term().await.unwrap();
    assert!(f.repo.recall().is_empty());
}

/// Folding a recall writes the record's row, but must move no index entry:
/// recall metadata carries no index any more, so the flush costs a row write
/// and nothing else. This is what makes flushing tens of thousands of pending
/// recalls affordable.
#[tokio::test]
async fn folding_a_recall_moves_no_index_entry() {
    let f = fixture().await;
    let id = create(&f, "content").await;

    let before = f
        .repo
        .engine
        .attr_indexes()
        .expect("the fixture schema registers attribute indexes")
        .range(crate::services::attrs::SLOT_CREATED_AT, 0, u64::MAX)
        .expect("created_at is indexed");

    f.manager.get(id).await.unwrap();
    lifecycle(&f).flush_recall().await.unwrap();

    let after = f
        .repo
        .engine
        .attr_indexes()
        .unwrap()
        .range(crate::services::attrs::SLOT_CREATED_AT, 0, u64::MAX)
        .unwrap();

    assert_eq!(
        before, after,
        "a recall changes accessed_at, access_count and health -- none of which \
         is indexed, so the ordering index must be untouched"
    );
    for slot in [
        crate::services::attrs::SLOT_ACCESSED_AT,
        crate::services::attrs::SLOT_ACCESS_COUNT,
        crate::services::attrs::SLOT_HEALTH,
    ] {
        assert!(
            f.repo
                .engine
                .attr_indexes()
                .unwrap()
                .range(slot, 0, u64::MAX)
                .is_none(),
            "slot {slot} must have no index to move entries in"
        );
    }

    // The row itself did change -- otherwise this asserts nothing.
    let (_, stored) = f.repo.load_bound(id).await.unwrap().unwrap();
    assert_eq!(stored.metadata.access_count, 1);
}
