//! Query executor that runs execution plans against the storage engine
//!
//! Each step runs against one index and produces an independently ranked list;
//! when there is more than one, the lists are fused by rank (RRF).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::engine::attr::select::{matches as row_matches, AttrPred};
use crate::engine::error::{Result, StorageError};
use crate::engine::storage::partition::PartitionScope;
use crate::engine::storage::StorageEngine;

use super::merge::{RrfMerger, ScoreNormalizer};
use super::planner::{ExecutionPlan, ExecutionStep};
use super::types::{BooleanMode, Evidence, FusedItem, QueryResult, ResultItem, SourceKind};

/// How a candidate's predicates get settled, decided once per query.
///
/// `get_attrs` returns `None` both when no attribute schema is registered and
/// when a particular record has no row, and the two mean opposite things: the
/// first says "this deployment cannot answer predicates", the second says
/// "this index entry is stale". Resolving which world we are in once, at
/// entry, is what keeps a deployment with attribute storage switched off from
/// reporting every record as non-matching — which would return zero results
/// for every search (REM-78, design D3). `select_inner` guards the same trap
/// the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateMode {
    /// A schema is registered: the stored row is the authority, and its
    /// absence means the record is gone.
    Rows,
    /// No schema, but a projector: the payload is read and projected. Costs
    /// what pre-REM-78 search cost, and is correct.
    Projected,
    /// Neither. Predicates cannot be evaluated at all; only liveness can.
    Unavailable,
}

/// Everything needed to decide whether one candidate belongs in the result,
/// resolved once per query rather than per candidate.
struct Settler {
    mode: PredicateMode,
    preds: Vec<AttrPred>,
    /// Keys carrying every requested tag, when the tag index could answer for
    /// all of them. `None` means either no tag filter, or an index that
    /// cannot answer — the two are distinguished by `tag_filter_deferred`.
    allowed: Option<std::collections::HashSet<Bytes>>,
    /// A tag filter the index could not settle, left for the caller to apply
    /// against payloads it has already loaded.
    tag_filter_deferred: bool,
}

impl Settler {
    /// Nothing to settle beyond liveness, so a step can skip the per-candidate
    /// lookup entirely.
    fn is_noop(&self) -> bool {
        self.preds.is_empty() && self.allowed.is_none()
    }
}

/// Query executor that runs execution plans
pub struct QueryExecutor {
    /// Storage engine reference
    engine: Arc<StorageEngine>,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Execute a plan and return results
    #[allow(dead_code)]
    pub(crate) async fn execute(&self, plan: ExecutionPlan) -> Result<QueryResult> {
        self.execute_inner(plan, None).await
    }

    pub async fn execute_partitioned(
        &self,
        plan: ExecutionPlan,
        scope: &PartitionScope,
    ) -> Result<QueryResult> {
        self.execute_inner(plan, Some(scope)).await
    }

    async fn execute_inner(
        &self,
        plan: ExecutionPlan,
        scope: Option<&PartitionScope>,
    ) -> Result<QueryResult> {
        let mode = self.predicate_mode();

        // Refusing is the only honest answer here. Dropping the predicates
        // and returning would hand back archived and non-matching records as
        // if they had passed a filter the caller asked for; returning nothing
        // would claim the corpus holds no matches. Neither is true, and a
        // deployment reaching this has no attribute schema *and* no
        // projector, which `main.rs` never produces.
        if !plan.preds.is_empty() && mode == PredicateMode::Unavailable {
            return Err(StorageError::InvalidArgument(
                "query carries attribute predicates but no attribute schema or \
                 projector is registered, so they cannot be evaluated"
                    .to_string(),
            ));
        }

        // Tags carry no attribute slot, so a tag filter is settled by
        // intersecting the tag index's answer rather than by reading a row.
        // The index can only answer when it holds a posting list for every
        // requested tag; when it cannot it reports "no matches", which is
        // indistinguishable from a real miss, so the filter is deferred to the
        // payload instead of being trusted (REM-78, design D6).
        let mut tag_filter_deferred = false;
        let allowed = if plan.tag_filter.is_empty() {
            None
        } else {
            let wanted: Vec<&str> = plan.tag_filter.iter().map(|s| s.as_str()).collect();
            if self.engine.tag_index_can_answer(&wanted) {
                let keys = match scope {
                    Some(scope) => self.engine.tag_search_and_partitioned(scope, &wanted)?,
                    None => self.engine.tag_search_and(&wanted)?,
                };
                Some(keys.into_iter().collect())
            } else {
                tag_filter_deferred = true;
                None
            }
        };

        let settler = Settler {
            mode,
            preds: plan.preds.clone(),
            allowed,
            tag_filter_deferred,
        };

        let (items, truncated) = if plan.is_hybrid() {
            self.execute_hybrid(plan, scope, &settler).await?
        } else {
            self.execute_single(plan, scope, &settler).await?
        };

        Ok(QueryResult {
            items,
            truncated,
            tag_filter_deferred: settler.tag_filter_deferred,
        })
    }

    /// Which world this deployment is in, decided once per query.
    fn predicate_mode(&self) -> PredicateMode {
        if self.engine.attr_schema().is_some() {
            PredicateMode::Rows
        } else if self.engine.attr_project().is_some() {
            PredicateMode::Projected
        } else {
            PredicateMode::Unavailable
        }
    }

    /// Record that a step gave up at its effort bound.
    ///
    /// Both a counter and a log line: the counter is the aggregate REM-103 is
    /// gated on, and the log line is what makes a single slow, short query
    /// explicable after the fact.
    fn note_cap_hit(&self, step: &str, found: usize, wanted: usize) {
        self.engine.note_search_widen_cap_hit();
        tracing::debug!(
            step,
            found,
            wanted,
            "filtered retrieval stopped at its effort bound; result marked truncated"
        );
    }

    /// Whether a candidate is live and satisfies every predicate.
    ///
    /// This is the one place a candidate is settled, and it settles liveness
    /// and predicates together: a deleted record has no attribute row
    /// (`delete_with_attrs` removes both halves), so "no row" *is* the
    /// phantom check that used to cost a full payload fetch of its own.
    async fn admits(&self, settler: &Settler, key: &[u8]) -> Result<bool> {
        if let Some(allowed) = &settler.allowed {
            if !allowed.contains(key) {
                return Ok(false);
            }
        }
        let preds = &settler.preds;
        match settler.mode {
            PredicateMode::Rows => {
                let Some(row) = self.engine.get_attrs(key).await? else {
                    // Either the record is gone — the ordinary phantom — or
                    // it was written while attribute support was off and
                    // never backfilled, which nothing else in the system can
                    // detect (`attr/select.rs`: verification catches a stale
                    // entry, never a missing one). Distinguishing them costs
                    // one lookup on a path that is already exceptional, and
                    // an unbacked record silently vanishing from every
                    // filtered search is worth a warning.
                    if self.engine.get(key).await?.is_some() {
                        tracing::warn!(
                            key = %String::from_utf8_lossy(key),
                            "record has no attribute row; it is invisible to filtered search \
                             until the attribute store is rebuilt"
                        );
                    }
                    return Ok(false);
                };
                Ok(row_matches(&row, preds))
            }
            PredicateMode::Projected => {
                let Some(bytes) = self.engine.get(key).await? else {
                    return Ok(false);
                };
                let Some(project) = self.engine.attr_project() else {
                    return Ok(false);
                };
                match project(&bytes) {
                    Some(row) => Ok(row_matches(&row, preds)),
                    // Unprojectable payload: not a memory record, or not
                    // parseable as one. It cannot satisfy a predicate.
                    None => Ok(false),
                }
            }
            // `execute_inner` rejects predicates in this mode, so there is
            // nothing left to settle but liveness.
            PredicateMode::Unavailable => Ok(self.engine.get(key).await?.is_some()),
        }
    }

    /// Execute a single-step plan.
    ///
    /// Nothing is fused, but the result still carries its source and rank so a
    /// single-index query is shape-compatible with a fused one (REM-74).
    async fn execute_single(
        &self,
        plan: ExecutionPlan,
        scope: Option<&PartitionScope>,
        settler: &Settler,
    ) -> Result<(Vec<FusedItem>, bool)> {
        let step = plan
            .steps
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::InvalidArgument("Empty execution plan".to_string()))?;

        let (kind, mut results, truncated) = self
            .execute_step(step, scope, settler, plan.effort_cap)
            .await?;
        results.truncate(plan.final_limit);

        Ok((
            results
                .into_iter()
                .enumerate()
                .map(|(rank, item)| FusedItem::single(kind, rank, item))
                .collect(),
            truncated,
        ))
    }

    /// Execute a multi-step plan, fusing the per-index rankings
    async fn execute_hybrid(
        &self,
        plan: ExecutionPlan,
        scope: Option<&PartitionScope>,
        settler: &Settler,
    ) -> Result<(Vec<FusedItem>, bool)> {
        let mut all_results: Vec<(SourceKind, Vec<ResultItem>)> =
            Vec::with_capacity(plan.steps.len());

        // One step giving up early makes the fused result incomplete, so the
        // flag is the disjunction: the caller is told "more may exist" if any
        // source could not finish looking.
        let mut truncated = false;
        for step in plan.steps {
            let (kind, items, step_truncated) = self
                .execute_step(step, scope, settler, plan.effort_cap)
                .await?;
            truncated |= step_truncated;
            all_results.push((kind, items));
        }

        let mut final_results = match plan.merge {
            Some(merge) => RrfMerger::new(merge.rrf_k).merge(all_results, merge.limit),
            // No merge step: a plan with multiple steps always carries one, but
            // concatenating is the honest fallback rather than dropping results.
            None => all_results
                .into_iter()
                .flat_map(|(kind, items)| {
                    items
                        .into_iter()
                        .enumerate()
                        .map(move |(rank, item)| FusedItem::single(kind, rank, item))
                })
                .collect(),
        };

        final_results.truncate(plan.final_limit);
        Ok((final_results, truncated))
    }

    /// Execute a single step, tagged with the index that produced it
    /// Execute a single step, tagged with the index that produced it.
    ///
    /// Predicates are applied *here*, inside the step, rather than after
    /// fusion. `SourceContribution.rank` is the position an item held in this
    /// index's ranked list and is retained so a coordinator can re-fuse
    /// across shards (REM-74); filtering after the merge would leave those
    /// ranks describing a list the caller never saw (REM-78, design D2).
    async fn execute_step(
        &self,
        step: ExecutionStep,
        scope: Option<&PartitionScope>,
        settler: &Settler,
        effort_cap: usize,
    ) -> Result<(SourceKind, Vec<ResultItem>, bool)> {
        match step {
            ExecutionStep::VectorSearch { embedding, k } => {
                let (items, truncated) = self
                    .execute_vector_search(embedding, k, scope, settler, effort_cap)
                    .await?;
                Ok((SourceKind::Vector, items, truncated))
            }

            ExecutionStep::GraphTraversal {
                start,
                max_depth,
                limit,
            } => {
                let candidates = self.execute_graph_traversal(start, max_depth, limit, scope)?;
                let (items, truncated) = self
                    .retain_admitted(candidates, limit, settler, effort_cap)
                    .await?;
                Ok((SourceKind::Graph, items, truncated))
            }

            ExecutionStep::TagSearch {
                tokens,
                mode: bool_mode,
                limit,
            } => {
                // Filtered before the truncation, not after: the posting list
                // is materialized in full and then cut to `limit`, so cutting
                // first would discard matches in favour of non-matches and
                // leave the step short for reasons fusion cannot see
                // (REM-78, design D5).
                let candidates = self.execute_tag_search(tokens, bool_mode, scope)?;
                let (items, truncated) = self
                    .retain_admitted(candidates, limit, settler, effort_cap)
                    .await?;
                Ok((SourceKind::Tag, items, truncated))
            }

            ExecutionStep::ContentScan { tokens, limit } => {
                // The scan stops at its own limit, so it is asked for as many
                // as the effort bound allows and cut to `limit` only after
                // the predicates have had their say — otherwise `limit`
                // counts candidates rather than survivors.
                let candidates = self
                    .execute_content_scan(tokens, effort_cap.max(limit), scope)
                    .await?;
                let (items, truncated) = self
                    .retain_admitted(candidates, limit, settler, effort_cap)
                    .await?;
                Ok((SourceKind::Content, items, truncated))
            }
        }
    }

    /// Keep the candidates that pass, up to `limit`, without examining more
    /// than `effort_cap` of them.
    ///
    /// Returns whether the bound stopped the walk while it was still short —
    /// the difference between "these are all the matches" and "this is as far
    /// as we looked".
    async fn retain_admitted(
        &self,
        candidates: Vec<ResultItem>,
        limit: usize,
        settler: &Settler,
        effort_cap: usize,
    ) -> Result<(Vec<ResultItem>, bool)> {
        // Nothing to settle: the pre-REM-78 shape, kept because a step with
        // no predicates should not pay a lookup per candidate to learn that
        // everything passes.
        if settler.is_noop() {
            let mut items = candidates;
            items.truncate(limit);
            return Ok((items, false));
        }

        let mut kept = Vec::with_capacity(limit.min(candidates.len()));

        // `examined` is the count of candidates already settled, which is
        // exactly the loop index.
        for (examined, item) in candidates.into_iter().enumerate() {
            if kept.len() >= limit {
                break;
            }
            if examined >= effort_cap {
                // Stopped with candidates left unexamined and room to spare.
                self.note_cap_hit("candidate-list", kept.len(), limit);
                return Ok((kept, true));
            }
            if self.admits(settler, item.key.as_ref()).await? {
                kept.push(item);
            }
        }

        Ok((kept, false))
    }

    /// Execute vector search, widening until enough admitted candidates
    /// surface.
    ///
    /// The index retains entries a search cannot use: retired ones, whose
    /// slots stay in the graph because removal is a tombstone rather than an
    /// excision, and live ones a filter rejects. Both losses are made up the
    /// same way — ask the index for more — and both are settled by the same
    /// attribute-row lookup, so widening for a filter costs no more per
    /// candidate than widening for a retired entry does (REM-78, design
    /// D3/D4).
    ///
    /// `connection_manager::auto_discover` calls `engine.vector_search`
    /// directly and is outside this widening.
    ///
    /// Three ways out, and the caller must be able to tell them apart:
    /// enough candidates were admitted; the authorized scope was exhausted,
    /// so these are genuinely all of them; or the effort bound was reached
    /// while still short, which returns `true` for truncated because more may
    /// exist and this cannot know.
    async fn execute_vector_search(
        &self,
        embedding: Vec<f32>,
        k: usize,
        scope: Option<&PartitionScope>,
        settler: &Settler,
        effort_cap: usize,
    ) -> Result<(Vec<ResultItem>, bool)> {
        // Bounded by what the *authorized* partitions hold, not the whole
        // corpus: the loop's only job is to stop widening once it has asked
        // for everything it is allowed to see.
        //
        // Node slots, not live vectors. The two differ by the retired entries
        // the index still holds, and "everything it is allowed to see" is a
        // statement about what a traversal can step over, not about what
        // survives the filter afterwards. Bounding on the live figure ends the
        // loop as soon as it has asked for that many candidates -- which, in a
        // corpus where most records are archived, is long before the traversal
        // has reached the live ones. The exit below would then report a page
        // that is missing matches as complete, which is the one thing a caller
        // cannot recover from.
        let vector_count = match scope {
            Some(scope) => self.engine.scoped_vector_node_count(scope).max(1),
            None => self.engine.vector_node_count().max(1),
        };
        // The bound never sits below the target, or a query would truncate
        // before it had asked for what it wanted even once.
        let cap = effort_cap.max(k);
        let mut k_actual = k.min(vector_count);
        // `HnswIndex::search` always explores at least ef_search candidates.
        // Keep that whole ranked window so widening beneath its floor does not
        // launch the identical traversal again. Once the requested window
        // crosses the cached range, replace it with one fresh larger search;
        // a smaller HNSW beam cannot safely be resumed at a larger effort.
        let search_floor = self.engine.vector_search_floor()?;
        let mut cached_through = 0;
        let mut candidates = Vec::new();
        let mut admissions = HashMap::new();

        loop {
            let requested = k_actual.max(search_floor).min(vector_count);
            if requested > cached_through {
                candidates = if let Some(scope) = scope {
                    self.engine
                        .vector_search_partitioned(scope, &embedding, requested, None)
                        .await?
                } else {
                    self.engine.vector_search(&embedding, requested).await?
                };
                cached_through = requested;
            }

            let mut live = Vec::with_capacity(candidates.len());
            for candidate in candidates.iter().take(k_actual) {
                // The cached floor can exceed this widening round. Preserve
                // the old effort contract by settling only the current raw
                // prefix; later rounds reuse the saved decisions for that
                // prefix and settle only its newly exposed suffix.
                if live.len() == k {
                    break;
                }
                let key = &candidate.key;
                let admitted = match admissions.get(key) {
                    Some(admitted) => *admitted,
                    None => {
                        let admitted = self.admits(settler, key.as_ref()).await?;
                        admissions.insert(key.clone(), admitted);
                        admitted
                    }
                };
                if !admitted {
                    continue;
                }
                // The raw distance is retained: it is the only value that can
                // be converted back to a metric-appropriate similarity
                // downstream.
                live.push(ResultItem::with_evidence(
                    key.clone(),
                    ScoreNormalizer::normalize_vector_distance(candidate.distance),
                    Evidence::Vector {
                        distance: candidate.distance,
                    },
                ));
            }

            if live.len() >= k {
                live.truncate(k);
                return Ok((live, false));
            }
            if k_actual >= vector_count {
                // Everything the caller is allowed to see has been asked for
                // and this is what survived. Short, but complete.
                return Ok((live, false));
            }
            if k_actual >= cap {
                self.note_cap_hit("vector", live.len(), k);
                return Ok((live, true));
            }

            k_actual = k_actual.saturating_mul(2).min(vector_count).min(cap);
        }
    }

    /// Execute graph traversal
    fn execute_graph_traversal(
        &self,
        start: Bytes,
        max_depth: usize,
        limit: usize,
        scope: Option<&PartitionScope>,
    ) -> Result<Vec<ResultItem>> {
        let traversal_results = if let Some(scope) = scope {
            self.engine
                .traverse_graph_partitioned(scope, &start, max_depth, None)?
        } else {
            self.engine.traverse_graph(&start, max_depth, None)?
        };

        let mut items: Vec<ResultItem> = traversal_results
            .into_iter()
            .filter(|r| r.node_id != start) // Exclude start node
            .map(|r| {
                ResultItem::with_evidence(
                    r.node_id,
                    ScoreNormalizer::normalize_graph_depth(r.depth),
                    Evidence::Graph { depth: r.depth },
                )
            })
            .collect();

        items.truncate(limit);

        Ok(items)
    }

    /// Execute tag search
    fn execute_tag_search(
        &self,
        tokens: Vec<String>,
        mode: BooleanMode,
        scope: Option<&PartitionScope>,
    ) -> Result<Vec<ResultItem>> {
        let token_refs: Vec<&str> = tokens.iter().map(|s| s.as_str()).collect();

        let results = match mode {
            BooleanMode::And => {
                let keys = if let Some(scope) = scope {
                    self.engine.tag_search_and_partitioned(scope, &token_refs)?
                } else {
                    self.engine.tag_search_and(&token_refs)?
                };
                keys.into_iter().map(|k| (k, 1.0)).collect::<Vec<_>>()
            }
            BooleanMode::Or => {
                if let Some(scope) = scope {
                    self.engine
                        .tag_search_scored_partitioned(scope, &token_refs)?
                } else {
                    self.engine.tag_search_scored(&token_refs)?
                }
            }
        };

        let mut items: Vec<ResultItem> = results
            .into_iter()
            .map(|(key, score)| ResultItem::new(key, score))
            .collect();

        // Normalize tag scores before they are handed to rank fusion
        ScoreNormalizer::normalize_tag_scores(&mut items);

        // Deliberately untruncated: the caller cuts to `limit` after
        // predicates have been applied, so `limit` counts survivors rather
        // than candidates (REM-78, design D5).
        Ok(items)
    }

    /// Execute a full-corpus content scan
    ///
    /// Scores are already the fraction of query tokens matched, so unlike tag
    /// scores they need no normalization before rank fusion.
    async fn execute_content_scan(
        &self,
        tokens: Vec<String>,
        limit: usize,
        scope: Option<&PartitionScope>,
    ) -> Result<Vec<ResultItem>> {
        let token_refs: Vec<&str> = tokens.iter().map(|s| s.as_str()).collect();

        let results = if let Some(scope) = scope {
            self.engine
                .content_scan_partitioned(scope, &token_refs, limit)
                .await?
        } else {
            self.engine.content_scan(&token_refs, limit).await?
        };

        Ok(results
            .into_iter()
            .map(|(key, score)| ResultItem::new(key, score))
            .collect())
    }
}
