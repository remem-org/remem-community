//! Score merging and normalization for hybrid search
#![allow(dead_code)]
//!
//! This module implements various strategies for combining results from
//! multiple search sources:
//! - RRF (Reciprocal Rank Fusion) - rank-based fusion
//! - Weighted Sum - score-based fusion with weights
//! - Intersection - only items present in all results
//! - Union - all items from all results

use std::collections::HashMap;

use bytes::Bytes;

use super::types::{ComponentWeights, ResultItem, ResultSource};

/// Strategy for merging search results
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    /// Reciprocal Rank Fusion
    Rrf { k: usize },

    /// Weighted sum of normalized scores
    WeightedSum { weights: ComponentWeights },

    /// Intersection (items must appear in all sources)
    Intersection,

    /// Union (all items from all sources)
    Union,
}

impl MergeStrategy {
    /// Create an RRF merger with default k=60
    pub fn rrf() -> Self {
        Self::Rrf { k: 60 }
    }

    /// Create an RRF merger with custom k
    pub fn rrf_with_k(k: usize) -> Self {
        Self::Rrf { k }
    }

    /// Create a weighted sum merger
    pub fn weighted(weights: ComponentWeights) -> Self {
        Self::WeightedSum { weights }
    }
}

/// Merger for combining results from multiple sources
pub struct RrfMerger {
    /// RRF k parameter
    k: usize,
}

impl RrfMerger {
    /// Create a new RRF merger
    pub fn new(k: usize) -> Self {
        Self { k }
    }

    /// Merge multiple result lists using RRF
    ///
    /// RRF score = sum(1 / (k + rank)) for each list where item appears
    pub fn merge(&self, result_lists: Vec<Vec<ResultItem>>, limit: usize) -> Vec<ResultItem> {
        // Map from key to (accumulated RRF score, best item)
        let mut scores: HashMap<Bytes, (f32, ResultItem)> = HashMap::new();

        for results in result_lists {
            for (rank, item) in results.into_iter().enumerate() {
                let rrf_score = 1.0 / (self.k as f32 + rank as f32 + 1.0);

                scores
                    .entry(item.key.clone())
                    .and_modify(|(score, existing)| {
                        *score += rrf_score;
                        // Keep the item with better original score
                        if item.score > existing.score {
                            *existing = item.clone();
                        }
                        existing.source = ResultSource::Hybrid;
                    })
                    .or_insert((rrf_score, item));
            }
        }

        // Convert to vec and sort by RRF score
        let mut merged: Vec<ResultItem> = scores
            .into_iter()
            .map(|(_, (rrf_score, mut item))| {
                item.score = rrf_score;
                item.source = ResultSource::Hybrid;
                item
            })
            .collect();

        // Sort by RRF score descending
        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Apply limit
        merged.truncate(limit);
        merged
    }

    /// Merge with source tracking for debugging
    pub fn merge_with_sources(
        &self,
        result_lists: Vec<(ResultSource, Vec<ResultItem>)>,
        limit: usize,
    ) -> Vec<ResultItem> {
        let mut scores: HashMap<Bytes, MergeEntry> = HashMap::new();

        for (source, results) in result_lists {
            for (rank, item) in results.into_iter().enumerate() {
                let rrf_score = 1.0 / (self.k as f32 + rank as f32 + 1.0);

                scores
                    .entry(item.key.clone())
                    .and_modify(|entry| {
                        entry.rrf_score += rrf_score;
                        entry.sources.push(source);
                        if item.score > entry.best_item.score {
                            entry.best_item = item.clone();
                        }
                    })
                    .or_insert(MergeEntry {
                        rrf_score,
                        sources: vec![source],
                        best_item: item,
                    });
            }
        }

        let mut merged: Vec<ResultItem> = scores
            .into_iter()
            .map(|(_, entry)| {
                let mut item = entry.best_item;
                item.score = entry.rrf_score;
                item.source = if entry.sources.len() > 1 {
                    ResultSource::Hybrid
                } else {
                    entry.sources[0]
                };
                item
            })
            .collect();

        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        merged
    }
}

impl Default for RrfMerger {
    fn default() -> Self {
        Self::new(60)
    }
}

/// Entry for tracking merge state
struct MergeEntry {
    rrf_score: f32,
    sources: Vec<ResultSource>,
    best_item: ResultItem,
}

/// Score normalizer for different result types
pub struct ScoreNormalizer;

impl ScoreNormalizer {
    /// Normalize vector search distances to similarity scores (0-1)
    ///
    /// Uses the formula: score = 1 / (1 + distance)
    /// This maps distance 0 -> score 1, distance infinity -> score 0
    pub fn normalize_vector_distance(distance: f32) -> f32 {
        1.0 / (1.0 + distance)
    }

    /// Normalize vector distances for a list of results
    pub fn normalize_vector_results(results: &mut [ResultItem]) {
        for item in results {
            // Assume score currently holds distance
            item.score = Self::normalize_vector_distance(item.score);
        }
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

    /// Min-max normalization for arbitrary scores
    pub fn min_max_normalize(results: &mut [ResultItem]) {
        if results.is_empty() {
            return;
        }

        let min_score = results
            .iter()
            .map(|r| r.score)
            .fold(f32::INFINITY, |a, b| a.min(b));
        let max_score = results
            .iter()
            .map(|r| r.score)
            .fold(f32::NEG_INFINITY, |a, b| a.max(b));

        let range = max_score - min_score;
        if range > 0.0 {
            for item in results {
                item.score = (item.score - min_score) / range;
            }
        } else {
            // All scores are the same, normalize to 1.0
            for item in results {
                item.score = 1.0;
            }
        }
    }
}

/// Weighted sum merger
pub struct WeightedMerger {
    weights: ComponentWeights,
}

impl WeightedMerger {
    /// Create a new weighted merger
    pub fn new(weights: ComponentWeights) -> Self {
        Self { weights }
    }

    /// Merge results using weighted sum of normalized scores
    pub fn merge(
        &self,
        result_lists: Vec<(ResultSource, Vec<ResultItem>)>,
        limit: usize,
    ) -> Vec<ResultItem> {
        let mut scores: HashMap<Bytes, (f32, f32, ResultItem)> = HashMap::new();

        for (source, mut results) in result_lists {
            // Normalize scores first
            ScoreNormalizer::min_max_normalize(&mut results);

            // Get weight for this source
            let weight = match source {
                ResultSource::Vector => self.weights.vector,
                ResultSource::Tag => self.weights.tag,
                ResultSource::Graph => self.weights.graph,
                _ => 1.0,
            };

            for item in results {
                let weighted_score = item.score * weight;

                scores
                    .entry(item.key.clone())
                    .and_modify(|(total_score, total_weight, existing)| {
                        *total_score += weighted_score;
                        *total_weight += weight;
                        if item.score > existing.score {
                            *existing = item.clone();
                        }
                    })
                    .or_insert((weighted_score, weight, item));
            }
        }

        // Compute final normalized scores
        let mut merged: Vec<ResultItem> = scores
            .into_iter()
            .map(|(_, (total_score, total_weight, mut item))| {
                // Normalize by total weight to handle items appearing in different numbers of lists
                item.score = if total_weight > 0.0 {
                    total_score / total_weight
                } else {
                    0.0
                };
                item.source = ResultSource::Hybrid;
                item
            })
            .collect();

        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        merged
    }
}

/// Intersection merger - only keeps items that appear in all result lists
pub struct IntersectionMerger;

impl IntersectionMerger {
    /// Merge by intersection
    pub fn merge(result_lists: Vec<Vec<ResultItem>>, limit: usize) -> Vec<ResultItem> {
        if result_lists.is_empty() {
            return Vec::new();
        }

        if result_lists.len() == 1 {
            let mut results = result_lists.into_iter().next().unwrap();
            results.truncate(limit);
            return results;
        }

        // Count occurrences of each key
        let list_count = result_lists.len();
        let mut occurrences: HashMap<Bytes, (usize, f32, ResultItem)> = HashMap::new();

        for results in result_lists {
            for item in results {
                occurrences
                    .entry(item.key.clone())
                    .and_modify(|(count, total_score, existing)| {
                        *count += 1;
                        *total_score += item.score;
                        if item.score > existing.score {
                            *existing = item.clone();
                        }
                    })
                    .or_insert((1, item.score, item));
            }
        }

        // Keep only items that appear in all lists
        let mut merged: Vec<ResultItem> = occurrences
            .into_iter()
            .filter(|(_, (count, _, _))| *count == list_count)
            .map(|(_, (count, total_score, mut item))| {
                item.score = total_score / count as f32;
                item.source = ResultSource::Hybrid;
                item
            })
            .collect();

        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        merged
    }
}

/// Union merger - keeps all items from all result lists
pub struct UnionMerger;

impl UnionMerger {
    /// Merge by union (deduplicated)
    pub fn merge(result_lists: Vec<Vec<ResultItem>>, limit: usize) -> Vec<ResultItem> {
        let mut seen: HashMap<Bytes, ResultItem> = HashMap::new();

        for results in result_lists {
            for item in results {
                seen.entry(item.key.clone())
                    .and_modify(|existing| {
                        // Keep the one with higher score
                        if item.score > existing.score {
                            *existing = item.clone();
                        }
                        existing.source = ResultSource::Hybrid;
                    })
                    .or_insert(item);
            }
        }

        let mut merged: Vec<ResultItem> = seen.into_values().collect();
        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        merged
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

        // 'a' and 'b' should be at top since they appear in both lists
        assert!(!merged.is_empty());

        // Find 'a' and 'b' - they should have higher scores than 'c' and 'd'
        let a_item = merged.iter().find(|i| i.key.as_ref() == b"a");
        let b_item = merged.iter().find(|i| i.key.as_ref() == b"b");
        let c_item = merged.iter().find(|i| i.key.as_ref() == b"c");

        assert!(a_item.is_some());
        assert!(b_item.is_some());
        assert!(c_item.is_some());

        // Items in both lists should have higher RRF scores
        assert!(a_item.unwrap().score > c_item.unwrap().score);
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
    fn test_min_max_normalize() {
        let mut results = vec![
            make_item("a", 10.0),
            make_item("b", 5.0),
            make_item("c", 0.0),
        ];

        ScoreNormalizer::min_max_normalize(&mut results);

        assert_eq!(results[0].score, 1.0); // Max -> 1
        assert_eq!(results[1].score, 0.5); // Mid -> 0.5
        assert_eq!(results[2].score, 0.0); // Min -> 0
    }

    #[test]
    fn test_intersection_merge() {
        let list1 = vec![make_item("a", 0.9), make_item("b", 0.8)];
        let list2 = vec![make_item("b", 0.95), make_item("c", 0.75)];

        let merged = IntersectionMerger::merge(vec![list1, list2], 10);

        // Only 'b' appears in both
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].key.as_ref(), b"b");
    }

    #[test]
    fn test_union_merge() {
        let list1 = vec![make_item("a", 0.9), make_item("b", 0.8)];
        let list2 = vec![make_item("b", 0.95), make_item("c", 0.75)];

        let merged = UnionMerger::merge(vec![list1, list2], 10);

        // All three items should be present
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn test_weighted_merger() {
        let weights = ComponentWeights {
            vector: 2.0,
            tag: 1.0,
            graph: 0.5,
        };
        let merger = WeightedMerger::new(weights);

        let list1 = vec![make_item("a", 0.8)];
        let list2 = vec![make_item("a", 0.6)];

        let merged = merger.merge(
            vec![(ResultSource::Vector, list1), (ResultSource::Tag, list2)],
            10,
        );

        assert_eq!(merged.len(), 1);
        // Score should be weighted average
    }
}
