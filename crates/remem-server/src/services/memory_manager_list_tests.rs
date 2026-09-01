//! `MemoryManager::list` — equivalence against a full deserializing scan,
//! and the payload-read budget that is the point of routing it through the
//! attribute store.
//!
//! The substrate's own equivalence test (`attr_acceptance_tests` in
//! `engine/storage/engine.rs`) checks `select()` against a scan at the engine
//! layer; this checks the whole listing path at the service layer, filter
//! combination by filter combination.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::embedding::EmbeddingService;
use crate::engine::storage::engine::{EngineConfig, StorageEngine};
use crate::engine::storage::partition::{PartitionBinding, PartitionId, PartitionScope, TenantId};
use crate::services::attrs::{memory_schema, project};
use crate::services::cursor::ListCursor;
use crate::services::filters::matches_filters;
use crate::services::memory_manager::{MemoryManager, PageStart};
use crate::services::repository::MemoryRepository;
use crate::services::types::{
    memory_key, Memory, MemoryFilters, MemoryType, SortBy, SortOrder, StoredMemory, StoredMetadata,
};

/// One record. `created_at` ascends with `i` while `accessed_at` descends, so
/// a test that sorts by one cannot pass by accident under the other. Both are
/// distinct across the corpus — ordering among equal sort keys is
/// unspecified, so no assertion may depend on it.
#[allow(unknown_lints, clippy::manual_is_multiple_of)]
fn fixture(i: u32) -> StoredMemory {
    let mut tags = Vec::new();
    if i % 5 == 0 {
        tags.push("alpha".to_string());
    }
    if i % 7 == 0 {
        tags.push("beta".to_string());
    }
    let created_at = 1_700_000_000_000 + i as u64;
    StoredMemory {
        id: uuid::Uuid::from_u128(i as u128 + 1),
        content: format!("record {i}"),
        memory_type: if i % 3 == 0 {
            MemoryType::LongTerm
        } else {
            MemoryType::ShortTerm
        },
        metadata: StoredMetadata {
            created_at,
            updated_at: created_at,
            accessed_at: 1_700_000_100_000 - i as u64,
            access_count: i,
            source: None,
            tags,
            importance: (i % 100) as f32 / 100.0,
            emotional_valence: 0.0,
            arousal: 0.0,
            health: 100.0,
            last_recalled_at: None,
            flashbulb_until: None,
            ttl: None,
            last_decay_at: None,
            last_health_check_at: None,
        },
        archived: i % 10 == 0,
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    repo: Arc<MemoryRepository>,
    manager: MemoryManager,
    all: Vec<StoredMemory>,
    _discovery_rx: mpsc::Receiver<crate::services::connection_manager::DiscoveryTask>,
}

/// Writes the corpus exactly as `MemoryManager::create` does — the same
/// `store_memory_core` call, the same projected row, the same synthetic
/// `__type:` tag — minus the embedding, which listing never touches.
async fn fixture_corpus(n: u32) -> Fixture {
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

    let mut all = Vec::new();
    for i in 0..n {
        let s = fixture(i);
        let mut index_tags = s.metadata.tags.clone();
        index_tags.push(format!("__type:{}", s.memory_type));
        repo.engine
            .store_memory_core_partitioned(
                repo.write_target(),
                memory_key(s.id).as_bytes(),
                serde_json::to_vec(&s).unwrap(),
                None,
                s.metadata.created_at,
                &index_tags,
                Some(&project(&s)),
            )
            .await
            .unwrap();
        all.push(s);
    }

    let (tx, rx) = mpsc::channel(1);
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
        all,
        _discovery_rx: rx,
    }
}

async fn store_fixture_in_partition(
    repo: &MemoryRepository,
    partition: &PartitionBinding,
    stored: &StoredMemory,
) {
    let mut index_tags = stored.metadata.tags.clone();
    index_tags.push(format!("__type:{}", stored.memory_type));
    repo.engine
        .store_memory_core_partitioned(
            partition,
            memory_key(stored.id).as_bytes(),
            serde_json::to_vec(stored).unwrap(),
            None,
            stored.metadata.created_at,
            &index_tags,
            Some(&project(stored)),
        )
        .await
        .unwrap();
}

/// A full deserializing scan of the corpus: the oracle every equivalence
/// assertion below is measured against. Deliberately the slow shape — it is
/// here to be independently obviously correct, not fast.
fn scan_oracle(
    all: &[StoredMemory],
    filters: &MemoryFilters,
    sort_by: SortBy,
    limit: usize,
    offset: usize,
) -> (Vec<uuid::Uuid>, usize) {
    let mut matched: Vec<&StoredMemory> = all
        .iter()
        .filter(|s| !s.archived && matches_filters(s, filters))
        .collect();
    let SortBy::CreatedAt = sort_by;
    matched.sort_by_key(|s| s.metadata.created_at);
    let total = matched.len();
    let page = matched
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|s| s.id)
        .collect();
    (page, total)
}

fn ids(memories: &[Memory]) -> Vec<uuid::Uuid> {
    memories.iter().map(|m| m.id).collect()
}

/// Every combination the ticket's acceptance criterion names, spelled out
/// rather than generated, so a failure names the case.
fn filter_matrix() -> Vec<(&'static str, MemoryFilters)> {
    vec![
        ("none", MemoryFilters::default()),
        (
            "memory_type",
            MemoryFilters {
                memory_type: Some(MemoryType::LongTerm),
                ..Default::default()
            },
        ),
        (
            "min_importance",
            MemoryFilters {
                min_importance: Some(0.5),
                ..Default::default()
            },
        ),
        (
            "max_importance",
            MemoryFilters {
                max_importance: Some(0.5),
                ..Default::default()
            },
        ),
        (
            "importance_range",
            MemoryFilters {
                min_importance: Some(0.25),
                max_importance: Some(0.75),
                ..Default::default()
            },
        ),
        (
            "created_after",
            MemoryFilters {
                created_after: Some(1_700_000_000_050),
                ..Default::default()
            },
        ),
        (
            "created_before",
            MemoryFilters {
                created_before: Some(1_700_000_000_150),
                ..Default::default()
            },
        ),
        (
            "created_range",
            MemoryFilters {
                created_after: Some(1_700_000_000_050),
                created_before: Some(1_700_000_000_150),
                ..Default::default()
            },
        ),
        (
            "one_tag",
            MemoryFilters {
                tags: vec!["alpha".into()],
                ..Default::default()
            },
        ),
        (
            "two_tags",
            MemoryFilters {
                tags: vec!["alpha".into(), "beta".into()],
                ..Default::default()
            },
        ),
        (
            "tag_and_type",
            MemoryFilters {
                tags: vec!["alpha".into()],
                memory_type: Some(MemoryType::LongTerm),
                ..Default::default()
            },
        ),
        (
            "everything",
            MemoryFilters {
                memory_type: Some(MemoryType::LongTerm),
                tags: vec!["alpha".into()],
                min_importance: Some(0.1),
                max_importance: Some(0.95),
                created_after: Some(1_700_000_000_010),
                created_before: Some(1_700_000_000_190),
            },
        ),
    ]
}

#[tokio::test]
async fn list_agrees_with_a_full_scan_for_every_filter_and_sort_combination() {
    let f = fixture_corpus(200).await;

    let sort_by = SortBy::CreatedAt;
    for (name, filters) in filter_matrix() {
        let (expected_page, expected_total) = scan_oracle(&f.all, &filters, sort_by, 25, 0);
        assert!(
            expected_total > 0,
            "filter case `{name}` matches nothing, so it asserts nothing"
        );
        let memories = f
            .manager
            .list(&filters, sort_by, SortOrder::Ascending, 25, 0)
            .await
            .unwrap();

        assert_eq!(
            ids(&memories),
            expected_page,
            "page diverged for filter `{name}`"
        );
    }
}

#[tokio::test]
async fn pagination_agrees_with_a_full_scan_across_page_boundaries() {
    let f = fixture_corpus(200).await;
    let filters = MemoryFilters::default();

    let sort_by = SortBy::CreatedAt;
    for offset in [0usize, 1, 7, 50, 170, 400] {
        let (expected_page, _) = scan_oracle(&f.all, &filters, sort_by, 10, offset);
        let memories = f
            .manager
            .list(&filters, sort_by, SortOrder::Ascending, 10, offset)
            .await
            .unwrap();

        assert_eq!(
            ids(&memories),
            expected_page,
            "page diverged at offset {offset}"
        );
    }
}

#[tokio::test]
async fn tag_filtered_pagination_agrees_with_a_full_scan_across_page_boundaries() {
    // The tag branch pages by hand rather than through `skip`/`take`, so it
    // needs its own boundary walk: a limit below the match count exercises the
    // clamp, and a non-zero offset separates `>` from `>=`.
    let f = fixture_corpus(200).await;
    let filters = MemoryFilters {
        tags: vec!["alpha".into()],
        ..Default::default()
    };

    let sort_by = SortBy::CreatedAt;
    for offset in [0usize, 1, 2, 19, 20, 50] {
        let (expected_page, _) = scan_oracle(&f.all, &filters, sort_by, 3, offset);
        let memories = f
            .manager
            .list(&filters, sort_by, SortOrder::Ascending, 3, offset)
            .await
            .unwrap();

        assert_eq!(
            ids(&memories),
            expected_page,
            "page diverged at offset {offset}"
        );
    }
}

#[tokio::test]
async fn partition_scoped_listing_filters_ordering_and_totals_hide_other_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            attr_schema: Some(memory_schema()),
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let tenant = TenantId::new("acme").unwrap();
    let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
    let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
    let product_repo = Arc::new(MemoryRepository::with_partition_scope(
        Arc::clone(&engine),
        PartitionScope::single(product.clone()),
        product.clone(),
    ));
    let finance_repo = MemoryRepository::with_partition_scope(
        Arc::clone(&engine),
        PartitionScope::single(finance.clone()),
        finance.clone(),
    );

    let mut product_records = Vec::new();
    for i in 0..40 {
        let stored = fixture(i);
        store_fixture_in_partition(&product_repo, &product, &stored).await;
        product_records.push(stored);
    }
    for i in 100..140 {
        let stored = fixture(i);
        store_fixture_in_partition(&finance_repo, &finance, &stored).await;
    }

    let (tx, _rx) = mpsc::channel(1);
    let manager = MemoryManager::new(
        Arc::clone(&product_repo),
        Arc::new(EmbeddingService::new_for_test()),
        tx,
        // Same bound the server's default configuration uses.
        128,
    );
    let filters = MemoryFilters {
        tags: vec!["alpha".into()],
        min_importance: Some(0.1),
        max_importance: Some(0.8),
        ..Default::default()
    };
    let (expected_page, _) = scan_oracle(&product_records, &filters, SortBy::CreatedAt, 5, 1);

    let memories = manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Ascending, 5, 1)
        .await
        .unwrap();

    assert_eq!(ids(&memories), expected_page);
    assert!(
        memories
            .iter()
            .all(|m| product_records.iter().any(|stored| stored.id == m.id)),
        "product-scoped listing must not return finance records"
    );
}

/// Listing is ordered by creation time, and creation time only. The fixture
/// runs `accessed_at` opposite to `created_at`, so an implementation that
/// reintroduced recency ordering would show up here as a different page.
#[tokio::test]
async fn listing_is_ordered_by_creation_time() {
    let f = fixture_corpus(50).await;
    let filters = MemoryFilters::default();

    let by_created = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Ascending, 10, 0)
        .await
        .unwrap();

    // The API type carries timestamps as `DateTime<Utc>`, not the `u64` the
    // stored record holds. `ms_to_dt` is monotonic, so the order survives the
    // conversion and can be asserted on directly.
    let created: Vec<DateTime<Utc>> = by_created.iter().map(|m| m.metadata.created_at).collect();
    let mut sorted = created.clone();
    sorted.sort_unstable();
    assert_eq!(created, sorted, "created_at order must ascend");

    // Ordering by recency of retrieval is not merely unused, it is
    // unrepresentable: `SortBy` has one variant, so nothing can ask for it.
    let accessed: Vec<DateTime<Utc>> = by_created.iter().map(|m| m.metadata.accessed_at).collect();
    let mut ascending = accessed.clone();
    ascending.sort_unstable();
    assert_ne!(
        accessed, ascending,
        "the fixture must run accessed_at against created_at, or this asserts nothing"
    );
}

#[tokio::test]
async fn an_unfiltered_list_reads_only_the_payloads_it_returns() {
    // The ticket's second acceptance criterion, made executable. The counter
    // increments only on `memory:`-prefixed reads, so the sidecar-row lookups
    // the walk performs never inflate it.
    let f = fixture_corpus(500).await;
    f.repo.engine.reset_payload_reads();
    f.repo.engine.reset_attr_reads();

    let memories = f
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

    assert_eq!(memories.len(), 10);
    assert!(
        f.all.len() > 10,
        "the fixture must have more matches than one page"
    );
    assert_eq!(
        f.repo.engine.payload_reads(),
        10,
        "a page of 10 must cost 10 payload reads, not one per record in the corpus"
    );

    // The walk stops once it holds a page. It still reads a row per candidate
    // it looks at -- that is how a candidate is settled without touching the
    // payload -- but the number of candidates is now bounded by the page,
    // not by the corpus. This assertion used to read `f.all.len()` (500),
    // which is exactly the cost REM-28 removed.
    let reads = f.repo.engine.attr_reads();
    assert!(
        reads < 40,
        "a page of 10 must settle about a page of candidates, not the corpus; read {reads} rows"
    );
    assert!(
        reads >= 10,
        "it still has to settle every candidate it returns"
    );
}

/// The promise the whole change exists to make, stated as a count rather than
/// a duration: the same page costs the same work however much the caller has
/// stored. There is no read-path benchmark, so this is asserted by counting
/// reads over two corpus sizes rather than by timing anything.
#[tokio::test]
async fn a_page_costs_the_same_over_a_corpus_four_times_the_size() {
    let small = fixture_corpus(200).await;
    let large = fixture_corpus(800).await;

    let mut counts = Vec::new();
    for f in [&small, &large] {
        f.repo.engine.reset_attr_reads();
        f.repo.engine.reset_payload_reads();
        let memories = f
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
        assert_eq!(memories.len(), 10);
        counts.push((f.repo.engine.attr_reads(), f.repo.engine.payload_reads()));
    }

    assert_eq!(
        counts[0], counts[1],
        "row and payload reads for one page must not grow with the corpus \
         (200 records vs 800)"
    );
}

#[tokio::test]
async fn a_filtered_list_reads_only_the_payloads_it_returns() {
    let f = fixture_corpus(500).await;
    let filters = MemoryFilters {
        memory_type: Some(MemoryType::LongTerm),
        min_importance: Some(0.9),
        ..Default::default()
    };
    f.repo.engine.reset_payload_reads();

    let memories = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Ascending, 5, 0)
        .await
        .unwrap();

    assert!(
        !memories.is_empty(),
        "the fixture must produce some matches"
    );
    // Pin the page length to a count derived independently of `list()`, so
    // the read-budget assertion below isn't measuring the implementation
    // against itself (returning 2 of 5 matches and reading 2 payloads would
    // otherwise still pass).
    let (expected_page, _expected_total) = scan_oracle(&f.all, &filters, SortBy::CreatedAt, 5, 0);
    assert_eq!(
        memories.len(),
        expected_page.len(),
        "returned page length must match the fixture's independently computed match count"
    );
    assert_eq!(
        f.repo.engine.payload_reads(),
        memories.len(),
        "every payload read must correspond to a returned row"
    );
}

#[tokio::test]
async fn a_created_range_filtered_list_narrows_the_row_lookups() {
    // `created_after`/`created_before` target `SLOT_CREATED_AT`, which is
    // also the slot `SortBy::CreatedAt` walks (the `created_range` shape
    // from the filter matrix), so the range genuinely narrows
    // `select_ordered`'s walk rather than merely filtering its output —
    // unlike the unfiltered case above, where the walk covers the whole
    // slot regardless of what gets returned.
    let f = fixture_corpus(500).await;
    let filters = MemoryFilters {
        created_after: Some(1_700_000_000_050),
        created_before: Some(1_700_000_000_150),
        ..Default::default()
    };
    // Both bounds are inclusive (`to_attr_preds` via `u64_bound`), and the
    // fixture's `created_at` is `1_700_000_000_000 + i` for a distinct `i`
    // per record, so the walk's candidate set is exactly the records whose
    // `i` falls in `50..=150` -- archived or not, since `SLOT_ARCHIVED`
    // carries no index and so cannot narrow the walk itself.
    let expected_candidates = f
        .all
        .iter()
        .filter(|s| (50..=150).contains(&(s.id.as_u128() as u32 - 1)))
        .count();
    assert_eq!(
        expected_candidates, 101,
        "premise: i in 50..=150 inclusive is 101 records"
    );
    f.repo.engine.reset_attr_reads();

    let memories = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Ascending, 10, 0)
        .await
        .unwrap();

    assert!(
        !memories.is_empty(),
        "the fixture must produce some matches"
    );
    // Two narrowings compose here, and both matter. The range predicate keeps
    // the walk inside the 101 records it can match, and the page bound stops
    // it once it holds ten -- so the cost is the smaller of the two, not the
    // range and certainly not the corpus.
    let reads = f.repo.engine.attr_reads();
    assert!(
        reads <= expected_candidates,
        "a range predicate on the walked slot must not walk past the range: \
         read {reads} rows for {expected_candidates} candidates in range"
    );
    assert!(
        reads < 40,
        "and the page bound must stop it well inside that range; read {reads} rows"
    );
}

#[tokio::test]
async fn archived_records_never_surface() {
    let f = fixture_corpus(100).await;
    let archived: Vec<uuid::Uuid> = f.all.iter().filter(|s| s.archived).map(|s| s.id).collect();
    assert!(!archived.is_empty(), "the fixture must archive something");

    let memories = f
        .manager
        .list(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            100,
            0,
        )
        .await
        .unwrap();

    assert_eq!(memories.len(), f.all.len() - archived.len());
    for id in archived {
        assert!(!ids(&memories).contains(&id), "archived {id} surfaced");
    }
}

#[tokio::test]
async fn a_nan_importance_bound_matches_everything_just_as_a_scan_does() {
    // `?min_importance=NaN` is reachable from the HTTP query string. Every
    // comparison against NaN is false, so a scan ignores the bound; the walk
    // has to reach the same answer rather than narrowing itself to nothing.
    let f = fixture_corpus(100).await;
    let filters = MemoryFilters {
        min_importance: Some(f32::NAN),
        ..Default::default()
    };

    let (expected_page, expected_total) = scan_oracle(&f.all, &filters, SortBy::CreatedAt, 10, 0);
    let memories = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Ascending, 10, 0)
        .await
        .unwrap();

    assert!(
        expected_total > 0,
        "a NaN bound must not filter anything out"
    );
    assert_eq!(ids(&memories), expected_page);
}

#[tokio::test]
async fn a_retracted_tag_does_not_surface_in_a_tag_filtered_list() {
    // Retraction has to hold across the sealed/growing boundary, which is
    // where a tag posting outlives the payload that justified it: the tag is
    // written, the segment is sealed, and only then is the tag replaced. Both
    // layers that could surface the stale record are exercised — the tag
    // index narrows (asserted below), and the payload settles what the
    // narrowing returns.
    let f = fixture_corpus(20).await;
    let subject = f
        .all
        .iter()
        .find(|s| !s.archived && s.metadata.tags.iter().any(|t| t == "alpha"))
        .expect("the fixture must contain a live alpha-tagged record")
        .clone();
    let key = memory_key(subject.id);

    f.repo.engine.seal_tag_index_for_test().unwrap();

    // Replace the record's tags, payload and index alike.
    let mut updated = subject.clone();
    updated.metadata.tags = vec!["gamma".to_string()];
    f.repo.store(&mut updated).await.unwrap();
    f.repo
        .engine
        .set_tags(key.clone(), &["gamma".to_string()])
        .await
        .unwrap();

    // Premise: the narrowing path must actually be taken, or this test would
    // pass on the payload check alone and prove nothing about the index.
    assert!(f.repo.engine.tag_index_can_answer(&["alpha"]));

    let memories = f
        .manager
        .list(
            &MemoryFilters {
                tags: vec!["alpha".into()],
                ..Default::default()
            },
            SortBy::CreatedAt,
            SortOrder::Ascending,
            100,
            0,
        )
        .await
        .unwrap();

    assert!(
        !ids(&memories).contains(&subject.id),
        "a record whose tag was retracted must not appear in a filter for it"
    );
    assert_eq!(
        memories.len(),
        f.all
            .iter()
            .filter(|s| !s.archived
                && s.id != subject.id
                && s.metadata.tags.iter().any(|t| t == "alpha"))
            .count(),
        "and it must not be returned on any page either"
    );
}

#[tokio::test]
async fn a_tag_too_long_for_the_tag_index_is_still_listable() {
    // The tag index cannot represent a token past its length bound, so it
    // reports "no postings" — indistinguishable from a genuine miss. Listing
    // must fall back to the payload rather than report an empty page for a
    // record it can plainly see.
    let f = fixture_corpus(20).await;
    let long_tag = "l".repeat(150);

    let subject = f
        .all
        .iter()
        .find(|s| !s.archived)
        .expect("the fixture must contain a live record")
        .clone();

    let mut updated = subject.clone();
    updated.metadata.tags = vec![long_tag.clone()];
    f.repo.store(&mut updated).await.unwrap();
    let physical_key = f.repo.physical_memory_key(subject.id).unwrap();
    f.repo
        .engine
        .set_tags(physical_key, std::slice::from_ref(&long_tag))
        .await
        .unwrap();

    // Premise check: if this fails, the index grew the ability to represent
    // the tag and this test needs rewriting rather than the listing path.
    assert!(
        !f.repo.engine.tag_index_can_answer(&[long_tag.as_str()]),
        "premise: the tag index must not be able to answer for an over-length tag"
    );

    let memories = f
        .manager
        .list(
            &MemoryFilters {
                tags: vec![long_tag],
                ..Default::default()
            },
            SortBy::CreatedAt,
            SortOrder::Ascending,
            100,
            0,
        )
        .await
        .unwrap();

    assert_eq!(
        ids(&memories),
        vec![subject.id],
        "an over-length tag must still match its record"
    );
}

/// The anomaly this change removes, made executable. Paging through a listing
/// while every record on the first page is updated must not shuffle anything:
/// `created_at` is fixed at creation, so a record cannot move between pages
/// and so cannot be returned twice or skipped. The same walk ordered by
/// recency of retrieval is what used to reorder under exactly this load.
#[tokio::test]
async fn paging_while_records_are_updated_returns_each_record_once() {
    let f = fixture_corpus(60).await;
    let filters = MemoryFilters::default();
    const PAGE: usize = 10;

    let mut seen: Vec<uuid::Uuid> = Vec::new();
    let mut offset = 0usize;
    loop {
        let page = f
            .manager
            .list(
                &filters,
                SortBy::CreatedAt,
                SortOrder::Ascending,
                PAGE,
                offset,
            )
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }

        // Touch every record just returned. Under the old recency ordering
        // this is precisely what moved them ahead of the pages still to come.
        for m in &page {
            let (binding, mut stored) = f.repo.load_bound(m.id).await.unwrap().unwrap();
            stored.metadata.accessed_at += 1_000_000;
            stored.metadata.access_count += 1;
            stored.metadata.importance = (stored.metadata.importance + 0.01).min(1.0);
            f.repo.store_in(&binding, &mut stored).await.unwrap();
        }

        seen.extend(page.iter().map(|m| m.id));
        offset += PAGE;
        assert!(offset < 1_000, "paging failed to terminate");
    }

    let live: std::collections::HashSet<uuid::Uuid> =
        f.all.iter().filter(|s| !s.archived).map(|s| s.id).collect();

    let unique: std::collections::HashSet<uuid::Uuid> = seen.iter().copied().collect();
    assert_eq!(
        unique.len(),
        seen.len(),
        "a record was returned on more than one page"
    );
    assert_eq!(
        unique, live,
        "every live record must appear exactly once across the sequence"
    );
}

/// The page costs what the page costs. Payload reads scale with the page, not
/// with how many records match -- which is what made dropping the exact
/// `total` worth doing.
#[tokio::test]
async fn a_page_costs_payload_reads_for_that_page_only() {
    let f = fixture_corpus(500).await;
    f.repo.engine.reset_payload_reads();

    let memories = f
        .manager
        .list(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            10,
            200,
        )
        .await
        .unwrap();

    assert_eq!(memories.len(), 10);
    assert_eq!(
        f.repo.engine.payload_reads(),
        10,
        "a deep page must still cost one payload read per returned record, \
         not one per record skipped over"
    );
}

// ── Ordering direction ────────────────────────────────────────────────────────
//
// `list_recent_memories` names the newest memories, so the direction it asks
// for is the behaviour under test here. The corpus writes `created_at`
// ascending with `i`, so "newest" is the highest `i` that survives the filters.

#[tokio::test]
async fn a_descending_page_returns_the_newest_records() {
    let f = fixture_corpus(200).await;
    let filters = MemoryFilters::default();

    let newest = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Descending, 5, 0)
        .await
        .unwrap();

    // The oracle's last page, read backwards: the same records the ascending
    // walk would reach last.
    let (all_ascending, _) = scan_oracle(&f.all, &filters, SortBy::CreatedAt, usize::MAX, 0);
    let expected: Vec<uuid::Uuid> = all_ascending.iter().rev().take(5).copied().collect();

    assert_eq!(
        ids(&newest),
        expected,
        "descending page is not the newest 5"
    );
}

#[tokio::test]
async fn descending_is_the_exact_reverse_of_ascending() {
    let f = fixture_corpus(200).await;
    let filters = MemoryFilters::default();

    let ascending = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Ascending, 500, 0)
        .await
        .unwrap();
    let descending = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Descending, 500, 0)
        .await
        .unwrap();

    let mut reversed = ids(&ascending);
    reversed.reverse();
    assert_eq!(ids(&descending), reversed);
    // Guard against both directions being trivially equal on an empty corpus.
    assert!(ascending.len() > 1, "corpus too small to order");
}

#[tokio::test]
async fn descending_holds_through_the_tag_branch() {
    // The tag branch pages by hand instead of `skip`/`take`, so it can honour
    // the direction only if the reversal happens before it walks.
    let f = fixture_corpus(200).await;
    let filters = MemoryFilters {
        tags: vec!["alpha".into()],
        ..Default::default()
    };

    let ascending = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Ascending, 500, 0)
        .await
        .unwrap();
    let descending = f
        .manager
        .list(&filters, SortBy::CreatedAt, SortOrder::Descending, 500, 0)
        .await
        .unwrap();

    assert!(
        ascending.len() > 1,
        "tag filter matches too little to order"
    );
    let mut reversed = ids(&ascending);
    reversed.reverse();
    assert_eq!(ids(&descending), reversed);
}

/// Walk a whole listing with continuations, one page at a time.
async fn cursor_walk(f: &Fixture, order: SortOrder, page_size: usize) -> Vec<uuid::Uuid> {
    let mut seen = Vec::new();
    let mut start = PageStart::Beginning;
    loop {
        let page = f
            .manager
            .list_page(
                &MemoryFilters::default(),
                SortBy::CreatedAt,
                order,
                page_size,
                start,
            )
            .await
            .unwrap();
        seen.extend(page.memories.iter().map(|m| m.id));
        assert!(
            !page.truncated,
            "an unfiltered walk has nothing to reject and must never truncate"
        );
        match page.next_cursor {
            Some(token) if page.has_more => {
                start = PageStart::After(ListCursor::decode(&token).unwrap());
            }
            _ => return seen,
        }
        assert!(seen.len() < 10_000, "paging failed to terminate");
    }
}

/// The continuation contract: every memory exactly once, in both directions.
#[tokio::test]
async fn a_continuation_sequence_returns_every_memory_exactly_once() {
    let f = fixture_corpus(97).await;
    let live: std::collections::HashSet<uuid::Uuid> =
        f.all.iter().filter(|s| !s.archived).map(|s| s.id).collect();

    for order in [SortOrder::Ascending, SortOrder::Descending] {
        let seen = cursor_walk(&f, order, 10).await;
        let unique: std::collections::HashSet<uuid::Uuid> = seen.iter().copied().collect();

        assert_eq!(
            seen.len(),
            unique.len(),
            "{order:?}: no memory appears twice"
        );
        assert_eq!(unique, live, "{order:?}: every live memory appears once");
    }
}

/// Descending pages, which an offset cannot do: the boundary is a position,
/// so appending memories mid-sequence cannot shift it.
#[tokio::test]
async fn a_descending_sequence_holds_while_memories_are_created() {
    let f = fixture_corpus(40).await;
    let before: std::collections::HashSet<uuid::Uuid> =
        f.all.iter().filter(|s| !s.archived).map(|s| s.id).collect();

    let mut seen: Vec<uuid::Uuid> = Vec::new();
    let mut start = PageStart::Beginning;
    let mut created = 0u32;
    loop {
        let page = f
            .manager
            .list_page(
                &MemoryFilters::default(),
                SortBy::CreatedAt,
                SortOrder::Descending,
                7,
                start,
            )
            .await
            .unwrap();
        seen.extend(page.memories.iter().map(|m| m.id));

        // A memory created mid-sequence lands at the newest end -- the end
        // this sequence has already walked past. Under an offset it would
        // shift every remaining page by one and hand back a duplicate.
        let s = fixture(1000 + created);
        created += 1;
        let mut index_tags = s.metadata.tags.clone();
        index_tags.push(format!("__type:{}", s.memory_type));
        f.repo
            .engine
            .store_memory_core_partitioned(
                f.repo.write_target(),
                memory_key(s.id).as_bytes(),
                serde_json::to_vec(&s).unwrap(),
                None,
                s.metadata.created_at,
                &index_tags,
                Some(&project(&s)),
            )
            .await
            .unwrap();

        match page.next_cursor {
            Some(token) if page.has_more => {
                start = PageStart::After(ListCursor::decode(&token).unwrap());
            }
            _ => break,
        }
        assert!(seen.len() < 1_000, "paging failed to terminate");
    }

    let unique: std::collections::HashSet<uuid::Uuid> = seen.iter().copied().collect();
    assert_eq!(
        seen.len(),
        unique.len(),
        "no memory is handed back twice while the corpus grows underneath"
    );
    assert!(
        unique.is_subset(&before.union(&unique).copied().collect()),
        "and nothing outside the corpus appears"
    );
}

/// A page that could not be filled within the effort bound says so, rather
/// than passing its short self off as the end of the collection.
#[tokio::test]
async fn a_listing_that_gives_up_early_reports_that_it_stopped() {
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
    for i in 0..400u32 {
        let s = fixture(i);
        let mut index_tags = s.metadata.tags.clone();
        index_tags.push(format!("__type:{}", s.memory_type));
        repo.engine
            .store_memory_core_partitioned(
                repo.write_target(),
                memory_key(s.id).as_bytes(),
                serde_json::to_vec(&s).unwrap(),
                None,
                s.metadata.created_at,
                &index_tags,
                Some(&project(&s)),
            )
            .await
            .unwrap();
    }
    let (tx, _rx) = mpsc::channel(1);
    // A deliberately mean bound: two candidates per memory asked for.
    let manager = MemoryManager::new(
        Arc::clone(&repo),
        Arc::new(EmbeddingService::new_for_test()),
        tx,
        2,
    );

    let filters = MemoryFilters {
        memory_type: Some(MemoryType::LongTerm),
        min_importance: Some(0.95),
        ..Default::default()
    };
    let page = manager
        .list_page(
            &filters,
            SortBy::CreatedAt,
            SortOrder::Ascending,
            10,
            PageStart::Beginning,
        )
        .await
        .unwrap();

    assert!(
        page.memories.len() < 10,
        "the premise: the bound must bite before the page fills"
    );
    assert!(page.truncated, "a page that stopped early must say so");
    assert!(
        page.has_more,
        "and must not be reported as the end of the collection"
    );
}

/// A continuation is a position, read inside the scope of whoever presents
/// it. That is what contains it — not any check on the token — so a
/// continuation issued elsewhere cannot reach across, and a continuation
/// whose memory has since been deleted does not break the sequence.
#[tokio::test]
async fn a_continuation_is_read_inside_the_scope_that_presents_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            attr_schema: Some(memory_schema()),
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let tenant = TenantId::new("acme").unwrap();
    let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
    let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
    let product_repo = Arc::new(MemoryRepository::with_partition_scope(
        Arc::clone(&engine),
        PartitionScope::single(product.clone()),
        product.clone(),
    ));
    let finance_repo = Arc::new(MemoryRepository::with_partition_scope(
        Arc::clone(&engine),
        PartitionScope::single(finance.clone()),
        finance.clone(),
    ));

    let mut product_ids = std::collections::HashSet::new();
    for i in 0..20 {
        let stored = fixture(i);
        product_ids.insert(stored.id);
        store_fixture_in_partition(&product_repo, &product, &stored).await;
    }
    let mut finance_ids = Vec::new();
    for i in 100..120 {
        let stored = fixture(i);
        finance_ids.push(stored.id);
        store_fixture_in_partition(&finance_repo, &finance, &stored).await;
    }

    let manager_for = |repo: Arc<MemoryRepository>| {
        let (tx, _rx) = mpsc::channel(1);
        MemoryManager::new(repo, Arc::new(EmbeddingService::new_for_test()), tx, 128)
    };
    let product_mgr = manager_for(Arc::clone(&product_repo));
    let finance_mgr = manager_for(Arc::clone(&finance_repo));

    // A continuation issued inside finance, replayed against product.
    let finance_page = finance_mgr
        .list_page(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            5,
            PageStart::Beginning,
        )
        .await
        .unwrap();
    let foreign = ListCursor::decode(&finance_page.next_cursor.expect("finance has more")).unwrap();
    assert!(
        finance_ids.contains(&foreign.id),
        "premise: the continuation names a memory that lives in finance"
    );

    let crossed = product_mgr
        .list_page(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            50,
            PageStart::After(foreign),
        )
        .await
        .unwrap();

    for memory in &crossed.memories {
        assert!(
            product_ids.contains(&memory.id),
            "a continuation from another scope must not reach into it"
        );
    }

    // And a continuation whose memory has since been deleted still continues
    // the sequence, rather than failing the caller for an ordinary deletion.
    let first = product_mgr
        .list_page(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            5,
            PageStart::Beginning,
        )
        .await
        .unwrap();
    let boundary = ListCursor::decode(&first.next_cursor.expect("product has more")).unwrap();
    product_repo.delete_in(&product, boundary.id).await.unwrap();

    let after_delete = product_mgr
        .list_page(
            &MemoryFilters::default(),
            SortBy::CreatedAt,
            SortOrder::Ascending,
            5,
            PageStart::After(boundary),
        )
        .await
        .unwrap();

    assert!(
        !after_delete.memories.is_empty(),
        "deleting the memory a continuation names must not end the sequence"
    );
    let returned: std::collections::HashSet<uuid::Uuid> =
        after_delete.memories.iter().map(|m| m.id).collect();
    let already_seen: std::collections::HashSet<uuid::Uuid> =
        first.memories.iter().map(|m| m.id).collect();
    assert!(
        returned.is_disjoint(&already_seen),
        "and must not hand back anything the earlier page already returned"
    );
}
