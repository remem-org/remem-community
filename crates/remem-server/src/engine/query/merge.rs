//! Score normalization and rank fusion for hybrid search
//!
//! Results arriving from different indexes are not score-comparable: HNSW
//! returns distances, the tag index returns TF-ish scores, and graph traversal
//! returns depths. Rather than trying to reconcile the scales, the lists are
//! fused by *rank* using RRF (Reciprocal Rank Fusion).
//!
//! Note that fusion currently replaces each item's score with its RRF score,
//! which destroys the per-index scores — REM-74 revisits the result contract.

use std::collections::HashMap;

use bytes::Bytes;

use super::types::ResultItem;

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

    /// Merge multiple ranked result lists using RRF.
    ///
    /// RRF score = sum over lists of `1 / (k + rank + 1)` for each list the item
    /// appears in, so items surfaced by several indexes rank above items found
    /// by only one.
    pub fn merge(&self, result_lists: Vec<Vec<ResultItem>>, limit: usize) -> Vec<ResultItem> {
        // Map from key to (accumulated RRF score, best item seen so far)
        let mut scores: HashMap<Bytes, (f32, ResultItem)> = HashMap::new();

        for results in result_lists {
            for (rank, item) in results.into_iter().enumerate() {
                let rrf_score = 1.0 / (self.k as f32 + rank as f32 + 1.0);

                scores
                    .entry(item.key.clone())
                    .and_modify(|(score, existing)| {
                        *score += rrf_score;
                        // Keep the item with the better original score
                        if item.score > existing.score {
                            *existing = item.clone();
                        }
                    })
                    .or_insert((rrf_score, item));
            }
        }

        let mut merged: Vec<ResultItem> = scores
            .into_values()
            .map(|(rrf_score, mut item)| {
                item.score = rrf_score;
                item
            })
            .collect();

        // Sort by RRF score descending
        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
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

    fn make_item(key: &str, score: f32) -> ResultItem {
        ResultItem::new(Bytes::from(key.to_string()), score)
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

        let merged = merger.merge(vec![list1, list2], 10);

        assert_eq!(merged.len(), 4);

        let a_item = merged.iter().find(|i| i.key.as_ref() == b"a").unwrap();
        let c_item = merged.iter().find(|i| i.key.as_ref() == b"c").unwrap();

        // Items appearing in both lists must outrank items found by one index
        assert!(a_item.score > c_item.score);
    }

    #[test]
    fn test_rrf_merge_is_sorted_descending() {
        let merger = RrfMerger::new(60);

        let list1 = vec![make_item("a", 0.9), make_item("b", 0.8)];
        let list2 = vec![make_item("b", 0.95), make_item("c", 0.75)];

        let merged = merger.merge(vec![list1, list2], 10);

        // 'b' is in both lists, so it must come first
        assert_eq!(merged[0].key.as_ref(), b"b");
        for pair in merged.windows(2) {
            assert!(pair[0].score >= pair[1].score);
        }
    }

    #[test]
    fn test_rrf_with_limit() {
        let merger = RrfMerger::new(60);

        let list1 = vec![
            make_item("a", 0.9),
            make_item("b", 0.8),
            make_item("c", 0.7),
        ];

        let merged = merger.merge(vec![list1], 2);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_rrf_k_flattens_rank_contribution() {
        let list = || vec![make_item("a", 0.9), make_item("b", 0.8)];

        let small_k = RrfMerger::new(1).merge(vec![list()], 10);
        let large_k = RrfMerger::new(1000).merge(vec![list()], 10);

        let gap = |m: &[ResultItem]| m[0].score - m[1].score;
        assert!(gap(&small_k) > gap(&large_k));
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
