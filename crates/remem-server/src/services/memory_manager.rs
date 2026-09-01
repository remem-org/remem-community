use std::sync::Arc;
use uuid::Uuid;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::embedding::EmbeddingService;
use crate::engine::storage::partition::PartitionBinding;
use crate::error::{AppError, Result};
use crate::services::attrs::SLOT_CREATED_AT;
use crate::services::connection_manager::DiscoveryTask;
use crate::services::cursor::ListCursor;
use crate::services::filters::to_attr_preds;
use crate::services::repository::MemoryRepository;
use crate::services::types::{
    memory_key, now_ms, parse_memory_id, Connection, Memory, MemoryFilters, MemoryType, SortBy,
    SortOrder, StoredMemory, StoredMetadata,
};

// serde_json is used directly in create() to pass the serialized bytes to
// store_memory_core, which combines KV + timestamp + tag writes in one WAL lock.
use serde_json;

pub struct MemoryManager {
    repo: Arc<MemoryRepository>,
    embedding: Arc<EmbeddingService>,
    discovery_tx: mpsc::Sender<DiscoveryTask>,
    /// How far a listing may walk before it reports the page truncated, as a
    /// multiple of the page requested. See `SearchConfig::list_max_factor`.
    list_max_factor: usize,
}

/// Where a page begins.
#[derive(Debug, Clone)]
pub enum PageStart {
    /// The first page of the ordering.
    Beginning,
    /// Skip this many matches first. The offset form: kept because callers
    /// depend on it, and honest about its cost -- skipping N matches means
    /// walking N matches.
    Offset(usize),
    /// Resume immediately after a position a previous page reported.
    After(ListCursor),
}

/// One page of memories, and what the walk learned about the rest.
pub struct MemoryPage {
    pub memories: Vec<Memory>,
    /// The continuation to pass back for the next page, when there is one.
    pub next_cursor: Option<String>,
    /// Whether further matches exist. False only when the walk reached the
    /// end of the ordering.
    pub has_more: bool,
    /// Whether the walk stopped at its effort bound before it could fill the
    /// page or establish that nothing remains. A short page with this set is
    /// not the end of the collection -- it is as far as we looked.
    pub truncated: bool,
}

pub struct CreateOpts {
    pub memory_type: MemoryType,
    pub tags: Vec<String>,
    pub importance: f32,
    pub emotional_valence: f32,
    pub arousal: f32,
    pub health: Option<f32>,
    pub ttl: Option<u64>,
    pub source: Option<String>,
}

impl Default for CreateOpts {
    fn default() -> Self {
        Self {
            memory_type: MemoryType::ShortTerm,
            tags: Vec::new(),
            importance: 0.5,
            emotional_valence: 0.0,
            arousal: 0.0,
            health: None,
            ttl: Some(3600),
            source: None,
        }
    }
}

pub struct UpdatePatch {
    pub content: Option<String>,
    pub tags: Option<Vec<String>>,
    pub importance: Option<f32>,
    pub emotional_valence: Option<f32>,
    pub arousal: Option<f32>,
    pub health: Option<f32>,
    pub source: Option<String>,
}

impl MemoryManager {
    pub fn new(
        repo: Arc<MemoryRepository>,
        embedding: Arc<EmbeddingService>,
        discovery_tx: mpsc::Sender<DiscoveryTask>,
        list_max_factor: usize,
    ) -> Self {
        Self {
            repo,
            embedding,
            discovery_tx,
            list_max_factor,
        }
    }

    pub async fn create(&self, content: &str, opts: CreateOpts) -> Result<(Memory, Vec<f32>)> {
        let id = Uuid::new_v4();
        let now = now_ms();
        const FLASHBULB_AROUSAL_THRESHOLD: f32 = 0.8;
        const FLASHBULB_PROTECTION_MS: u64 = 30 * 86_400_000;

        let arousal = opts.arousal.clamp(0.0, 1.0);
        let emotional_valence = opts.emotional_valence.clamp(-1.0, 1.0);
        let is_flashbulb = arousal >= FLASHBULB_AROUSAL_THRESHOLD;
        let memory_type = if is_flashbulb {
            MemoryType::LongTerm
        } else {
            opts.memory_type.clone()
        };
        let flashbulb_until = is_flashbulb.then_some(now + FLASHBULB_PROTECTION_MS);

        let stored = StoredMemory {
            id,
            content: content.to_owned(),
            memory_type: memory_type.clone(),
            metadata: StoredMetadata {
                created_at: now,
                updated_at: now,
                accessed_at: now,
                access_count: 0,
                source: opts.source,
                tags: opts.tags.clone(),
                importance: if is_flashbulb {
                    opts.importance.clamp(0.0, 1.0).max(0.9)
                } else {
                    opts.importance.clamp(0.0, 1.0)
                },
                emotional_valence,
                arousal,
                health: opts.health.unwrap_or(100.0).clamp(0.0, 100.0),
                last_recalled_at: None,
                flashbulb_until,
                ttl: match memory_type {
                    MemoryType::ShortTerm => opts.ttl.or(Some(3600)),
                    MemoryType::LongTerm => None,
                },
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        };

        let embedding = self.embedding.embed(content).await?;
        let json = serde_json::to_vec(&stored)?;
        let key = memory_key(id);

        let mut index_tags = opts.tags.clone();
        index_tags.push(format!("__type:{}", memory_type));
        if is_flashbulb {
            index_tags.push("__flashbulb__".to_owned());
        }

        let row = crate::services::attrs::project(&stored);
        self.repo
            .engine
            .store_memory_core_partitioned(
                self.repo.write_target(),
                key.as_bytes(),
                json,
                Some(embedding.clone()),
                now,
                &index_tags,
                Some(&row),
            )
            .await?;

        Ok((stored.into_api(Vec::new()), embedding))
    }

    pub async fn get(&self, id: Uuid) -> Result<Memory> {
        let _guard = self.repo.lock(id).await;

        let (binding, mut stored) = self
            .repo
            .load_bound(id)
            .await?
            .ok_or(AppError::NotFound(id))?;

        if stored.archived {
            return Err(AppError::NotFound(id));
        }

        // Fetching a memory by its id is a recall. Recording it costs no
        // write: recall is telemetry, and making a read append to the WAL
        // and fsync -- inside the engine's global write lock -- is what put
        // reads in contention with every write in the process. The delta is
        // folded into the record the next time anything writes it.
        let recall = self.repo.recall();
        recall.record(&binding, id, now_ms());

        // The caller has to see its own recall even though nothing has been
        // persisted yet, so merge the pending delta into the copy being
        // returned. `peek`, not `take`: taking it here would answer the
        // caller and then drop the recall before any write could apply it.
        if let Some(delta) = recall.peek(&binding, id) {
            delta.apply(&mut stored.metadata);
        }

        Ok(stored.into_api(Vec::new()))
    }

    pub async fn update(&self, id: Uuid, patch: UpdatePatch) -> Result<Memory> {
        let _guard = self.repo.lock(id).await;

        let (binding, mut stored) = self
            .repo
            .load_bound(id)
            .await?
            .ok_or(AppError::NotFound(id))?;

        if stored.archived {
            return Err(AppError::NotFound(id));
        }

        // Updating a memory by its id is a recall: a client that revises a
        // memory necessarily consulted it first. Recorded rather than
        // applied here, so the fold in `store_in` picks it up on the write
        // this method is already making -- no extra write, and coalesced
        // with the fetch that preceded it if the client made one.
        self.repo.recall().record(&binding, id, now_ms());

        let content_changed = patch.content.is_some();
        let tags_changed = patch.tags.is_some();
        if let Some(c) = patch.content {
            stored.content = c;
        }
        if let Some(t) = patch.tags {
            stored.metadata.tags = t;
        }
        if let Some(i) = patch.importance {
            stored.metadata.importance = i.clamp(0.0, 1.0);
        }
        if let Some(v) = patch.emotional_valence {
            stored.metadata.emotional_valence = v.clamp(-1.0, 1.0);
        }
        if let Some(a) = patch.arousal {
            stored.metadata.arousal = a.clamp(0.0, 1.0);
        }
        if let Some(h) = patch.health {
            stored.metadata.health = h.clamp(0.0, 100.0);
        }
        if let Some(s) = patch.source {
            stored.metadata.source = Some(s);
        }
        stored.metadata.updated_at = now_ms();

        if content_changed {
            let embedding = self.embedding.embed(&stored.content).await?;
            self.repo
                .store_with_embedding_in(&binding, &mut stored, embedding.clone())
                .await?;

            // Remove stale connections — new ones will be discovered asynchronously.
            let key = memory_key(id);
            let physical_key = self.repo.physical_key_in(&binding, key.as_bytes())?;
            let _ = self
                .repo
                .engine
                .remove_all_edges(physical_key.as_ref())
                .await;
            let _ = self
                .discovery_tx
                .try_send(DiscoveryTask::new(&self.repo, id, embedding, 0.7, 5)?);
        } else {
            self.repo.store_in(&binding, &mut stored).await?;
        }

        // Replace the inverted index entries when tags change, so stale tags are removed.
        if tags_changed {
            let key = memory_key(id);
            let physical_key = self.repo.physical_key_in(&binding, key.as_bytes())?;
            let mut index_tags = stored.metadata.tags.clone();
            index_tags.push(format!("__type:{}", stored.memory_type));
            self.repo.engine.set_tags(physical_key, &index_tags).await?;
        }

        Ok(stored.into_api(Vec::new()))
    }

    pub async fn delete(&self, id: Uuid, hard: bool) -> Result<()> {
        let _guard = self.repo.lock(id).await;

        if hard {
            let binding = self
                .repo
                .resolve_binding(id)
                .await?
                .ok_or(AppError::NotFound(id))?;
            self.repo.delete_in(&binding, id).await?;
        } else {
            let (binding, mut stored) = self
                .repo
                .load_bound(id)
                .await?
                .ok_or(AppError::NotFound(id))?;
            stored.archived = true;
            stored.metadata.updated_at = now_ms();
            self.repo.store_in(&binding, &mut stored).await?;
            // Keep timestamp/tag index entries so cleanup_archived can find the
            // archived record later; user-facing reads filter `archived=true`.
            let key = memory_key(id);
            let physical_key = self.repo.physical_key_in(&binding, key.as_bytes())?;
            self.repo
                .engine
                .add_tags(physical_key, &["__archived__".to_owned()])
                .await?;
            // Retire *after* the record is archived, never before: a
            // retirement that lands while the write then fails leaves a live
            // memory that search can no longer find, with nothing to signal
            // it. This order fails the other way -- the memory is archived,
            // the `archived` predicate still excludes it, and it costs one
            // candidate slot until cleanup removes it.
            if let Err(e) = self.repo.retire_from_retrieval_in(&binding, id).await {
                tracing::warn!(
                    memory_id = %id,
                    error = %e,
                    "failed to retire archived memory from search and browsing; it keeps costing \
                     retrieval work until cleanup"
                );
            }
        }
        Ok(())
    }

    /// One page of memories matching `filters`, ordered by `sort_by` and read
    /// from the end `order` names.
    ///
    /// The walk is bounded: it asks the ordering index for candidates a batch
    /// at a time and stops as soon as it has a full page, so the work is
    /// proportional to the page rather than to how much the caller has stored.
    /// Two things stop it short of that, and the response tells them apart --
    /// reaching the end of the ordering (`has_more: false`), and reaching the
    /// effort bound with candidates left unexamined (`truncated: true`). A
    /// caller that cannot distinguish those reads "we stopped looking" as
    /// "that was everything".
    ///
    /// Direction is a parameter, and with a cursor both directions page
    /// safely: a position keeps its meaning as memories are appended, which
    /// an offset does not. `created_at` never changes once set (REM-79), and
    /// that immutability is what the whole scheme rests on.
    ///
    /// One gap worth naming, unchanged from before: a record hard-deleted
    /// between the key walk and its payload read goes missing from the page,
    /// costing it a slot. Closing that needs a read snapshot spanning both
    /// (REM-79), not a second pass here.
    pub async fn list_page(
        &self,
        filters: &MemoryFilters,
        sort_by: SortBy,
        order: SortOrder,
        limit: usize,
        start: PageStart,
    ) -> Result<MemoryPage> {
        let SortBy::CreatedAt = sort_by;
        let descending = order == SortOrder::Descending;

        let (mut after, mut to_skip) = match start {
            PageStart::Beginning => (None, 0usize),
            PageStart::Offset(offset) => (None, offset),
            PageStart::After(cursor) => (Some(self.position_of(cursor).await?), 0usize),
        };

        let preds = to_attr_preds(filters);
        // Every filter but tags is settled from the row during the walk, so
        // only a tag filter can reject a candidate after it has been paid for.
        let tag_filtered = !filters.tags.is_empty();
        let wanted: Vec<&str> = filters.tags.iter().map(|s| s.as_str()).collect();
        let narrow_by_tag = tag_filtered && self.repo.engine.tag_index_can_answer(&wanted);
        let tagged: Option<std::collections::HashSet<Bytes>> = if narrow_by_tag {
            Some(
                self.repo
                    .engine
                    .tag_search_and_partitioned(self.repo.read_scope(), &wanted)?
                    .into_iter()
                    .collect(),
            )
        } else {
            None
        };

        let wanted_total = to_skip.saturating_add(limit);
        // The bound is on candidates *examined*, so it has to cover the
        // records being skipped as well as the ones being returned.
        let effort = wanted_total.saturating_mul(self.list_max_factor).max(limit);

        let mut page = MemoryPage {
            memories: Vec::with_capacity(limit),
            next_cursor: None,
            has_more: true,
            truncated: false,
        };
        let mut examined = 0usize;

        while page.memories.len() < limit {
            let batch = self
                .repo
                .engine
                .select_page_partitioned(
                    self.repo.read_scope(),
                    crate::engine::attr::select::OrderedSelect {
                        preds: &preds,
                        order_slot: SLOT_CREATED_AT,
                        after: after.clone(),
                        descending,
                        limit: wanted_total.saturating_sub(page.memories.len()).max(1),
                        effort: effort.saturating_sub(examined).max(1),
                    },
                )
                .await?;

            let exhausted = batch.exhausted;
            let stopped_early = batch.truncated;
            after = batch.next.clone();
            // Denominated in candidates looked at, which is what the bound
            // means -- counting returned keys would let a selective filter
            // walk far past the budget while appearing to have spent none.
            examined += batch.examined;

            for key in batch.keys {
                if let Some(tagged) = &tagged {
                    if !tagged.contains(&key) {
                        continue;
                    }
                }
                // Skipping without reading, when the row has already settled
                // every filter: an offset means walking the records it skips,
                // not loading them. Only a tag filter forces the payload,
                // because tags carry no row of their own and a skipped record
                // still has to be confirmed as a match before it counts
                // against the offset.
                if !tag_filtered && to_skip > 0 {
                    to_skip -= 1;
                    continue;
                }
                let Some((binding, stored)) = self.repo.load_bound_by_key(key.as_ref()).await?
                else {
                    continue;
                };
                if tag_filtered {
                    let held: std::collections::HashSet<&str> =
                        stored.metadata.tags.iter().map(|s| s.as_str()).collect();
                    if !filters.tags.iter().all(|t| held.contains(t.as_str())) {
                        continue;
                    }
                    if to_skip > 0 {
                        to_skip -= 1;
                        continue;
                    }
                }
                let created_at = stored.metadata.created_at;
                let id = stored.id;
                page.memories
                    .push(self.with_pending_recall(&binding, stored));
                if page.memories.len() == limit {
                    page.next_cursor = Some(Self::cursor_at(created_at, id));
                    break;
                }
            }

            if page.memories.len() >= limit {
                break;
            }
            if exhausted {
                // The ordering ran out. A short page here is the whole
                // remaining answer, not an interrupted one.
                page.has_more = false;
                break;
            }
            if stopped_early || examined >= effort {
                page.truncated = true;
                break;
            }
        }

        // A page that ended exactly on the last match still reports more until
        // a further request finds nothing: knowing otherwise would mean
        // reading one past the page, which costs a row on every request to
        // save one request at the very end of a sequence.
        if !page.has_more {
            page.next_cursor = None;
        }
        Ok(page)
    }

    /// The ordering position a cursor names, expressed the way the walk needs
    /// it.
    ///
    /// The cursor carries the memory's own id, never the physical record key,
    /// so the key is rebuilt here inside the caller's own read scope. A cursor
    /// naming a memory this caller cannot see therefore cannot address it:
    /// the key that comes out either belongs to a partition in scope or the
    /// request is refused.
    ///
    /// A memory deleted since the page that issued the cursor is not an error
    /// -- the position is a boundary, not a record, and a sequence must
    /// survive its boundary being deleted. The key is then rebuilt in the
    /// write partition, which is the same partition in every single-partition
    /// deployment.
    async fn position_of(&self, cursor: ListCursor) -> Result<crate::engine::index::IndexPosition> {
        let binding = match self.repo.resolve_binding(cursor.id).await? {
            Some(binding) => binding,
            None => self.repo.write_target().clone(),
        };
        let key = memory_key(cursor.id);
        let physical = self.repo.physical_key_in(&binding, key.as_bytes())?;
        Ok((cursor.order, physical))
    }

    fn cursor_at(created_at: u64, id: Uuid) -> String {
        use crate::engine::attr::value::AttrValue;
        ListCursor::new(AttrValue::U64(created_at).order_key(), id).encode()
    }

    /// One page of memories, without the paging signals.
    ///
    /// The offset-shaped call the REST listing and the backoffice already
    /// make. Runs on the same bounded walk as `list_page`; skipping `offset`
    /// matches costs walking them, which is what an offset means.
    pub async fn list(
        &self,
        filters: &MemoryFilters,
        sort_by: SortBy,
        order: SortOrder,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Memory>> {
        Ok(self
            .list_page(filters, sort_by, order, limit, PageStart::Offset(offset))
            .await?
            .memories)
    }

    /// Merge any recall recorded for this memory but not yet written.
    ///
    /// Every retrieval path applies this, not just retrieval by identity:
    /// otherwise opening a memory and then listing it report different use
    /// counts for up to a flush interval, and a caller cannot tell which one
    /// is the memory's actual state. `peek`, not `take` -- reading must never
    /// consume a recall that no write has applied yet.
    fn with_pending_recall(&self, binding: &PartitionBinding, mut stored: StoredMemory) -> Memory {
        if let Some(delta) = self.repo.recall().peek(binding, stored.id) {
            delta.apply(&mut stored.metadata);
        }
        stored.into_api(Vec::new())
    }

    pub async fn fetch_connections(&self, id: Uuid) -> Result<Vec<Connection>> {
        // Resolve the record's own partition rather than assuming the write
        // target: under a multi-partition read scope the memory may live in
        // any authorized partition, and encoding the write target's prefix
        // would address a key that does not exist and report "no
        // connections" for a memory that has them.
        let Some(binding) = self.repo.resolve_binding(id).await? else {
            return Ok(Vec::new());
        };
        let key = memory_key(id);
        let physical_key = self.repo.physical_key_in(&binding, key.as_bytes())?;
        let neighbors = self
            .repo
            .engine
            .get_neighbors_partitioned(self.repo.read_scope(), physical_key.as_ref())?;

        let mut connections = Vec::new();
        for (target_bytes, edge_type_str, weight, edge_ts) in neighbors {
            if let Some(target_id) = parse_memory_id(target_bytes.as_ref()) {
                if let Ok(rel) =
                    crate::services::types::RelationshipType::try_from(edge_type_str.as_str())
                {
                    connections.push(Connection {
                        target_id,
                        relationship_type: rel,
                        strength: weight,
                        created_at: crate::services::types::ms_to_dt(edge_ts),
                    });
                }
            }
        }
        Ok(connections)
    }
}
