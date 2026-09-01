//! Score normalization and rank fusion for hybrid search
//!
//! Results arriving from different indexes are not score-comparable: HNSW
//! returns distances, the tag index returns TF-ish scores, and graph traversal
//! returns depths. Rather than trying to reconcile the scales, the lists are
//! fused by *rank* using RRF (Reciprocal Rank Fusion).
//!
//! Fusion determines *ordering*; it does not measure relevance. Each index's own
//! score is carried through as a [`SourceContribution`] rather than being
//! overwritten by the RRF value, so callers get both the ranking and the
//! evidence behind it (REM-74).

use std::collections::HashMap;

use bytes::Bytes;

use super::types::{FusedItem, ResultItem, SourceContribution, SourceKind};

/// Reciprocal Rank Fusion merger
pub struct RrfMerger {
    /// RRF k parameter — larger values flatten the contribution of top ranks
    k: usize,
}

impl RrfMerger {
    /// Create a new RRF merger
    pub fn new(k: usize) -> Self {
        Self { k }
    }

    /// Merge labelled ranked result lists using RRF.
    ///
    /// RRF score = sum over lists of `1 / (k + rank + 1)` for each list the item
    /// appears in, so items surfaced by several indexes rank above items found
    /// by only one.
    ///
    /// Each list arrives tagged with the index that produced it; every list an
    /// item appears in becomes one [`SourceContribution`] on the fused result,
    /// retaining that index's native score, the rank the item held in it, and
    /// its evidence.
    pub fn merge(
        &self,
        result_lists: Vec<(SourceKind, Vec<ResultItem>)>,
        limit: usize,
    ) -> Vec<FusedItem> {
        // Map from key to (accumulated RRF score, contributions seen so far)
        let mut fused: HashMap<Bytes, (f32, Vec<SourceContribution>)> = HashMap::new();

        for (kind, results) in result_lists {
            for (rank, item) in results.into_iter().enumerate() {
                let rrf_score = 1.0 / (self.k as f32 + rank as f32 + 1.0);
                let contribution = SourceContribution {
                    kind,
                    score: item.score,
                    rank,
                    evidence: item.evidence,
                };

                let entry = fused.entry(item.key).or_insert((0.0, Vec::new()));
                entry.0 += rrf_score;
                entry.1.push(contribution);
            }
        }

        let mut merged: Vec<FusedItem> = fused
            .into_iter()
            .map(|(key, (fused_score, mut sources))| {
                // Best-scoring index first, so the strongest evidence leads
                sources.sort_by(|a, b| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                FusedItem {
                    key,
                    fused_score,
                    sources,
                }
            })
            .collect();

        // Sort by RRF score descending, breaking ties on key. Ties are the norm
        // on the keyword path (tag-rank r and content-rank r both score exactly
        // 1.0/(k + r + 1)); without a deterministic tiebreak, `sort_by`'s
        // stability just preserves the HashMap's randomized iteration order, so
        // identical queries could return different truncated result *sets*
        // across calls, not merely different orders.
        merged.sort_by(|a, b| {
            b.fused_score
                .partial_cmp(&a.fused_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });

        merged.truncate(limit);
        merged
    }
}

/// Score normalizer for the different index result types
pub struct ScoreNormalizer;

impl ScoreNormalizer {
    /// Normalize vector search distances to similarity scores (0-1)
    ///
    /// Uses the formula: score = 1 / (1 + distance)
    /// This maps distance 0 -> score 1, distance infinity -> score 0
    pub fn normalize_vector_distance(distance: f32) -> f32 {
        1.0 / (1.0 + distance)
    }

    /// Normalize tag TF scores to 0-1 range
    ///
    /// Uses max normalization: score = raw_score / max_score
    pub fn normalize_tag_scores(results: &mut [ResultItem]) {
        if results.is_empty() {
            return;
        }

        let max_score = results
            .iter()
            .map(|r| r.score)
            .fold(0.0_f32, |a, b| a.max(b));

        if max_score > 0.0 {
            for item in results {
                item.score /= max_score;
            }
        }
    }

    /// Normalize graph depths to scores (closer = higher score)
    ///
    /// Uses the formula: score = 1 / (1 + depth)
    pub fn normalize_graph_depth(depth: usize) -> f32 {
        1.0 / (1.0 + depth as f32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::query::types::Evidence;

    fn make_item(key: &str, score: f32) -> ResultItem {
        ResultItem::new(Bytes::from(key.to_string()), score)
    }

    fn find<'a>(merged: &'a [FusedItem], key: &[u8]) -> &'a FusedItem {
        merged.iter().find(|i| i.key.as_ref() == key).unwrap()
    }

    #[test]
    fn test_rrf_merge_basic() {
        let merger = RrfMerger::new(60);

        let list1 = vec![
            make_item("a", 0.9),
            make_item("b", 0.8),
            make_item("c", 0.7),
        ];
        let list2 = vec![
            make_item("b", 0.95),
            make_item("a", 0.85),
            make_item("d", 0.75),
        ];

        let merged = merger.merge(
            vec![(SourceKind::Vector, list1), (SourceKind::Tag, list2)],
            10,
        );

        assert_eq!(merged.len(), 4);

        // Items appearing in both lists must outrank items found by one index
        assert!(find(&merged, b"a").fused_score > find(&merged, b"c").fused_score);
    }

    #[test]
    fn test_rrf_merge_is_sorted_descending() {
        let merger = RrfMerger::new(60);

        let list1 = vec![make_item("a", 0.9), make_item("b", 0.8)];
        let list2 = vec![make_item("b", 0.95), make_item("c", 0.75)];

        let merged = merger.merge(
            vec![(SourceKind::Vector, list1), (SourceKind::Tag, list2)],
            10,
        );

        // 'b' is in both lists, so it must come first
        assert_eq!(merged[0].key.as_ref(), b"b");
        for pair in merged.windows(2) {
            assert!(pair[0].fused_score >= pair[1].fused_score);
        }
    }

    // ── REM-88: ties must resolve deterministically, not by HashMap iteration order ──

    #[test]
    fn merge_output_is_deterministic_across_repeated_calls() {
        // Ties are the norm on the keyword path: an item at tag-rank r and an
        // item at content-rank r both get exactly `1.0/(k + r + 1)`. Without a
        // tiebreak on key, the post-truncation sort's stability only preserves
        // the underlying HashMap's randomized iteration order, so identical
        // queries could return different result *sets* (not merely different
        // orders) across repeated calls.
        let merger = RrfMerger::new(60);

        // Five keys, each present in exactly one single-item list at rank 0:
        // all five tie at fused_score = 1/61. Truncating to 3 forces the
        // tiebreak to decide *which* three survive, not just their order.
        let keys = ["e", "d", "c", "b", "a"];
        let build_lists = || {
            keys.iter()
                .map(|k| (SourceKind::Vector, vec![make_item(k, 0.5)]))
                .collect::<Vec<_>>()
        };

        let first_order: Vec<Vec<u8>> = merger
            .merge(build_lists(), 3)
            .iter()
            .map(|i| i.key.to_vec())
            .collect();

        for _ in 0..10 {
            let order: Vec<Vec<u8>> = merger
                .merge(build_lists(), 3)
                .iter()
                .map(|i| i.key.to_vec())
                .collect();
            assert_eq!(
                order, first_order,
                "identical queries must return the identical result set and order every time"
            );
        }

        // With the tiebreak, ties resolve by ascending key.
        assert_eq!(
            first_order,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn test_rrf_with_limit() {
        let merger = RrfMerger::new(60);

        let list1 = vec![
            make_item("a", 0.9),
            make_item("b", 0.8),
            make_item("c", 0.7),
        ];

        let merged = merger.merge(vec![(SourceKind::Vector, list1)], 2);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_rrf_k_flattens_rank_contribution() {
        let list = || {
            vec![(
                SourceKind::Vector,
                vec![make_item("a", 0.9), make_item("b", 0.8)],
            )]
        };

        let small_k = RrfMerger::new(1).merge(list(), 10);
        let large_k = RrfMerger::new(1000).merge(list(), 10);

        let gap = |m: &[FusedItem]| m[0].fused_score - m[1].fused_score;
        assert!(gap(&small_k) > gap(&large_k));
    }

    // ── REM-74: fusion must preserve per-source relevance, not overwrite it ────

    #[test]
    fn fusion_preserves_native_scores_per_source() {
        let merger = RrfMerger::new(60);

        let vector = vec![make_item("a", 0.91), make_item("b", 0.42)];
        let tags = vec![make_item("b", 0.77), make_item("a", 0.33)];

        let merged = merger.merge(
            vec![(SourceKind::Vector, vector), (SourceKind::Tag, tags)],
            10,
        );

        let a = find(&merged, b"a");
        assert_eq!(a.sources.len(), 2, "both indexes must be represented");

        let vec_c = a
            .sources
            .iter()
            .find(|s| s.kind == SourceKind::Vector)
            .unwrap();
        let tag_c = a
            .sources
            .iter()
            .find(|s| s.kind == SourceKind::Tag)
            .unwrap();

        // The native scores survive fusion untouched — this is the whole point.
        assert_eq!(vec_c.score, 0.91);
        assert_eq!(tag_c.score, 0.33);

        // And the fused score is emphatically not one of them.
        assert!(a.fused_score < 0.1);
    }

    #[test]
    fn fusion_records_the_rank_each_index_gave() {
        let merger = RrfMerger::new(60);

        let vector = vec![make_item("a", 0.91), make_item("b", 0.42)];
        let tags = vec![make_item("b", 0.77), make_item("a", 0.33)];

        let merged = merger.merge(
            vec![(SourceKind::Vector, vector), (SourceKind::Tag, tags)],
            10,
        );

        // 'a' was first by vector, second by tag. A sharding coordinator needs
        // exactly this to re-fuse across nodes; it is not recoverable from score.
        let a = find(&merged, b"a");
        let rank_of = |item: &FusedItem, kind: SourceKind| {
            item.sources.iter().find(|s| s.kind == kind).unwrap().rank
        };
        assert_eq!(rank_of(a, SourceKind::Vector), 0);
        assert_eq!(rank_of(a, SourceKind::Tag), 1);

        let b = find(&merged, b"b");
        assert_eq!(rank_of(b, SourceKind::Vector), 1);
        assert_eq!(rank_of(b, SourceKind::Tag), 0);
    }

    #[test]
    fn single_source_hit_carries_one_contribution() {
        let merger = RrfMerger::new(60);

        let merged = merger.merge(
            vec![
                (SourceKind::Vector, vec![make_item("a", 0.9)]),
                (SourceKind::Tag, vec![make_item("b", 0.5)]),
            ],
            10,
        );

        assert_eq!(find(&merged, b"a").sources.len(), 1);
        assert_eq!(find(&merged, b"a").sources[0].kind, SourceKind::Vector);
        assert_eq!(find(&merged, b"b").sources[0].kind, SourceKind::Tag);
    }

    #[test]
    fn contributions_are_ordered_best_score_first() {
        let merger = RrfMerger::new(60);

        // 'a' scores poorly on vector and well on tags; the tag contribution
        // must lead regardless of the order the lists were supplied in.
        let merged = merger.merge(
            vec![
                (SourceKind::Vector, vec![make_item("a", 0.2)]),
                (SourceKind::Tag, vec![make_item("a", 0.9)]),
            ],
            10,
        );

        let a = find(&merged, b"a");
        assert_eq!(a.sources[0].kind, SourceKind::Tag);
        assert_eq!(a.sources[1].kind, SourceKind::Vector);
    }

    #[test]
    fn evidence_survives_fusion() {
        let merger = RrfMerger::new(60);

        let vector = vec![ResultItem::with_evidence(
            Bytes::from("a"),
            0.8,
            Evidence::Vector { distance: 0.5 },
        )];
        let graph = vec![ResultItem::with_evidence(
            Bytes::from("a"),
            0.5,
            Evidence::Graph { depth: 1 },
        )];

        let merged = merger.merge(
            vec![(SourceKind::Vector, vector), (SourceKind::Graph, graph)],
            10,
        );

        let a = find(&merged, b"a");
        let evidence: Vec<&Evidence> = a.sources.iter().map(|s| &s.evidence).collect();
        assert!(evidence.contains(&&Evidence::Vector { distance: 0.5 }));
        assert!(evidence.contains(&&Evidence::Graph { depth: 1 }));
    }

    #[test]
    fn test_score_normalizer_vector() {
        // Distance 0 should give score 1
        assert_eq!(ScoreNormalizer::normalize_vector_distance(0.0), 1.0);

        // Distance 1 should give score 0.5
        assert!((ScoreNormalizer::normalize_vector_distance(1.0) - 0.5).abs() < 0.001);

        // Higher distance should give lower score
        assert!(
            ScoreNormalizer::normalize_vector_distance(2.0)
                < ScoreNormalizer::normalize_vector_distance(1.0)
        );
    }

    #[test]
    fn test_score_normalizer_graph_depth() {
        // Depth 0 should give score 1
        assert_eq!(ScoreNormalizer::normalize_graph_depth(0), 1.0);

        // Higher depth should give lower score
        assert!(
            ScoreNormalizer::normalize_graph_depth(2) < ScoreNormalizer::normalize_graph_depth(1)
        );
    }

    #[test]
    fn test_normalize_tag_scores() {
        let mut results = vec![
            make_item("a", 4.0),
            make_item("b", 2.0),
            make_item("c", 0.0),
        ];

        ScoreNormalizer::normalize_tag_scores(&mut results);

        assert_eq!(results[0].score, 1.0);
        assert_eq!(results[1].score, 0.5);
        assert_eq!(results[2].score, 0.0);
    }

    #[test]
    fn test_normalize_tag_scores_all_zero_is_noop() {
        let mut results = vec![make_item("a", 0.0), make_item("b", 0.0)];
        ScoreNormalizer::normalize_tag_scores(&mut results);
        assert!(results.iter().all(|i| i.score == 0.0));
    }
}
