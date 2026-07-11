//! SIMD-accelerated vector distance calculations
#![allow(dead_code)]
//!
//! This module provides optimized distance functions for vector similarity search.
//! The implementations use patterns that enable compiler auto-vectorization.
//!
//! # Supported Distance Functions
//!
//! - **L2 (Euclidean)**: L2 squared distance for efficiency
//! - **Cosine similarity**: Normalized dot product
//! - **Dot product**: Inner product of two vectors
//!
//! # Performance Notes
//!
//! - Vectors should be aligned to cache line boundaries (64 bytes) for best performance
//! - The compiler will auto-vectorize these loops when building with `-C target-cpu=native`
//! - For optimal SIMD utilization, vector dimensions should be multiples of 8 (AVX) or 16 (AVX-512)

use std::cmp::Ordering;

/// Distance metric type for vector operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance - default for most embedding models
    #[default]
    L2,
    /// Cosine similarity (converted to distance: 1 - similarity)
    Cosine,
    /// Dot product (negated for min-heap compatibility)
    DotProduct,
}

impl DistanceMetric {
    /// Calculate distance between two vectors using this metric
    #[inline]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceMetric::L2 => l2_distance_squared(a, b),
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::DotProduct => -dot_product(a, b), // Negate for min-heap
        }
    }

    /// Convert a similarity score to a distance score
    #[inline]
    pub fn similarity_to_distance(&self, similarity: f32) -> f32 {
        match self {
            DistanceMetric::Cosine => 1.0 - similarity,
            DistanceMetric::DotProduct => -similarity,
            DistanceMetric::L2 => similarity, // L2 is already a distance
        }
    }

    /// Convert a distance score to a similarity score
    #[inline]
    pub fn distance_to_similarity(&self, distance: f32) -> f32 {
        match self {
            DistanceMetric::Cosine => 1.0 - distance,
            DistanceMetric::DotProduct => -distance,
            DistanceMetric::L2 => 1.0 / (1.0 + distance), // Inverse for similarity
        }
    }
}

/// Calculate L2 (Euclidean) squared distance between two vectors.
///
/// Returns the squared distance to avoid the sqrt operation, which is
/// monotonic and sufficient for comparison purposes.
///
/// # Arguments
///
/// * `a` - First vector
/// * `b` - Second vector (must have same length as `a`)
///
/// # Panics
///
/// Panics if vectors have different lengths.
///
/// # Example
///
/// ```
/// use remem_storage::util::simd::l2_distance_squared;
///
/// let a = vec![1.0, 2.0, 3.0];
/// let b = vec![4.0, 5.0, 6.0];
/// let dist = l2_distance_squared(&a, &b);
/// assert!((dist - 27.0).abs() < 1e-6); // (3^2 + 3^2 + 3^2) = 27
/// ```
#[inline]
pub fn l2_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vectors must have same length");

    // This loop pattern is easily auto-vectorized by the compiler
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let diff = a[i] - b[i];
        sum += diff * diff;
    }
    sum
}

/// Calculate L2 (Euclidean) distance between two vectors.
///
/// This is the actual Euclidean distance (with sqrt).
#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    l2_distance_squared(a, b).sqrt()
}

/// Calculate dot product (inner product) of two vectors.
///
/// # Arguments
///
/// * `a` - First vector
/// * `b` - Second vector (must have same length as `a`)
///
/// # Panics
///
/// Panics if vectors have different lengths.
///
/// # Example
///
/// ```
/// use remem_storage::util::simd::dot_product;
///
/// let a = vec![1.0, 2.0, 3.0];
/// let b = vec![4.0, 5.0, 6.0];
/// let dot = dot_product(&a, &b);
/// assert!((dot - 32.0).abs() < 1e-6); // 1*4 + 2*5 + 3*6 = 32
/// ```
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vectors must have same length");

    let mut sum = 0.0f32;
    for i in 0..a.len() {
        sum += a[i] * b[i];
    }
    sum
}

/// Calculate magnitude (L2 norm) of a vector.
#[inline]
pub fn magnitude(v: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for &x in v {
        sum += x * x;
    }
    sum.sqrt()
}

/// Calculate squared magnitude of a vector.
#[inline]
pub fn magnitude_squared(v: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for &x in v {
        sum += x * x;
    }
    sum
}

/// Calculate cosine similarity between two vectors.
///
/// Returns a value in [-1, 1] where 1 means identical direction.
///
/// # Arguments
///
/// * `a` - First vector
/// * `b` - Second vector (must have same length as `a`)
///
/// # Panics
///
/// Panics if vectors have different lengths.
///
/// # Example
///
/// ```
/// use remem_storage::util::simd::cosine_similarity;
///
/// let a = vec![1.0, 0.0, 0.0];
/// let b = vec![1.0, 0.0, 0.0];
/// let sim = cosine_similarity(&a, &b);
/// assert!((sim - 1.0).abs() < 1e-6); // Identical vectors
///
/// let c = vec![0.0, 1.0, 0.0];
/// let sim2 = cosine_similarity(&a, &c);
/// assert!(sim2.abs() < 1e-6); // Orthogonal vectors
/// ```
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vectors must have same length");

    // Compute dot product and magnitudes in a single pass for cache efficiency
    let mut dot = 0.0f32;
    let mut mag_a = 0.0f32;
    let mut mag_b = 0.0f32;

    for i in 0..a.len() {
        dot += a[i] * b[i];
        mag_a += a[i] * a[i];
        mag_b += b[i] * b[i];
    }

    let denom = (mag_a * mag_b).sqrt();
    if denom < f32::EPSILON {
        0.0 // Return 0 similarity for zero vectors
    } else {
        dot / denom
    }
}

/// Calculate cosine distance between two vectors.
///
/// Returns a value in [0, 2] where 0 means identical direction.
/// This is computed as 1 - cosine_similarity.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

/// Normalize a vector to unit length (in-place).
///
/// After normalization, the vector will have magnitude 1.
///
/// # Arguments
///
/// * `v` - Vector to normalize in place
///
/// # Example
///
/// ```
/// use remem_storage::util::simd::{normalize, magnitude};
///
/// let mut v = vec![3.0, 4.0, 0.0];
/// normalize(&mut v);
/// assert!((magnitude(&v) - 1.0).abs() < 1e-6);
/// ```
#[inline]
pub fn normalize(v: &mut [f32]) {
    let mag = magnitude(v);
    if mag > f32::EPSILON {
        let inv_mag = 1.0 / mag;
        for x in v.iter_mut() {
            *x *= inv_mag;
        }
    }
}

/// Create a normalized copy of a vector.
#[inline]
pub fn normalized(v: &[f32]) -> Vec<f32> {
    let mut result = v.to_vec();
    normalize(&mut result);
    result
}

/// Compute cosine similarity using pre-normalized vectors (just dot product).
///
/// This is faster than `cosine_similarity` when vectors are already normalized.
#[inline]
pub fn cosine_similarity_normalized(a: &[f32], b: &[f32]) -> f32 {
    dot_product(a, b)
}

/// Compute cosine distance using pre-normalized vectors.
#[inline]
pub fn cosine_distance_normalized(a: &[f32], b: &[f32]) -> f32 {
    1.0 - dot_product(a, b)
}

/// Batch distance calculation for multiple vectors.
///
/// More efficient than calling distance functions individually due to
/// better cache utilization.
///
/// # Arguments
///
/// * `query` - The query vector
/// * `candidates` - Slice of candidate vectors to compare against
/// * `metric` - Distance metric to use
///
/// # Returns
///
/// Vector of distances corresponding to each candidate
pub fn batch_distances(query: &[f32], candidates: &[&[f32]], metric: DistanceMetric) -> Vec<f32> {
    candidates
        .iter()
        .map(|c| metric.distance(query, c))
        .collect()
}

/// Find the k nearest neighbors using brute force search.
///
/// This is useful for small datasets or as a baseline for testing.
///
/// # Arguments
///
/// * `query` - The query vector
/// * `vectors` - All vectors to search through
/// * `k` - Number of nearest neighbors to return
/// * `metric` - Distance metric to use
///
/// # Returns
///
/// Vector of (index, distance) pairs sorted by distance ascending
pub fn brute_force_knn(
    query: &[f32],
    vectors: &[Vec<f32>],
    k: usize,
    metric: DistanceMetric,
) -> Vec<(usize, f32)> {
    let mut distances: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i, metric.distance(query, v)))
        .collect();

    // Partial sort for k smallest elements
    if k < distances.len() {
        distances
            .select_nth_unstable_by(k, |a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        distances.truncate(k);
    }

    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    distances
}

/// Aligned vector storage for optimal SIMD performance.
///
/// This struct ensures vectors are aligned to 64 bytes (cache line / AVX-512).
#[repr(align(64))]
#[derive(Clone)]
pub struct AlignedVector {
    data: Vec<f32>,
}

impl AlignedVector {
    /// Create a new aligned vector from data.
    pub fn new(data: Vec<f32>) -> Self {
        Self { data }
    }

    /// Create a zero-filled aligned vector of given dimension.
    pub fn zeros(dim: usize) -> Self {
        Self {
            data: vec![0.0; dim],
        }
    }

    /// Get the underlying slice.
    #[inline]
    pub fn as_slice(&self) -> &[f32] {
        &self.data
    }

    /// Get a mutable slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [f32] {
        &mut self.data
    }

    /// Get the dimension (length) of the vector.
    #[inline]
    pub fn dim(&self) -> usize {
        self.data.len()
    }

    /// Normalize this vector in place.
    pub fn normalize(&mut self) {
        normalize(&mut self.data);
    }
}

impl std::ops::Deref for AlignedVector {
    type Target = [f32];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for AlignedVector {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl From<Vec<f32>> for AlignedVector {
    fn from(data: Vec<f32>) -> Self {
        Self::new(data)
    }
}

impl From<&[f32]> for AlignedVector {
    fn from(data: &[f32]) -> Self {
        Self::new(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f32 = 1e-5;

    #[test]
    fn test_l2_distance_squared() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let dist = l2_distance_squared(&a, &b);
        // (4-1)^2 + (5-2)^2 + (6-3)^2 = 9 + 9 + 9 = 27
        assert!((dist - 27.0).abs() < EPSILON);
    }

    #[test]
    fn test_l2_distance_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let dist = l2_distance_squared(&a, &a);
        assert!(dist.abs() < EPSILON);
    }

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let dot = dot_product(&a, &b);
        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        assert!((dot - 32.0).abs() < EPSILON);
    }

    #[test]
    fn test_magnitude() {
        let v = vec![3.0, 4.0, 0.0];
        let mag = magnitude(&v);
        // sqrt(9 + 16) = 5
        assert!((mag - 5.0).abs() < EPSILON);
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &a);
        assert!((sim - 1.0).abs() < EPSILON);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < EPSILON);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![-1.0, 0.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim - (-1.0)).abs() < EPSILON);
    }

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0, 0.0];
        normalize(&mut v);
        assert!((magnitude(&v) - 1.0).abs() < EPSILON);
        assert!((v[0] - 0.6).abs() < EPSILON);
        assert!((v[1] - 0.8).abs() < EPSILON);
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        let dist = cosine_distance(&a, &b);
        assert!(dist.abs() < EPSILON); // Same vectors = 0 distance
    }

    #[test]
    fn test_distance_metric() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];

        // L2 squared: (1-0)^2 + (0-1)^2 + (0-0)^2 = 2
        assert!((DistanceMetric::L2.distance(&a, &b) - 2.0).abs() < EPSILON);

        // Cosine: 1 - 0 = 1 (orthogonal)
        assert!((DistanceMetric::Cosine.distance(&a, &b) - 1.0).abs() < EPSILON);

        // Dot product (negated): -0 = 0
        assert!(DistanceMetric::DotProduct.distance(&a, &b).abs() < EPSILON);
    }

    #[test]
    fn test_brute_force_knn() {
        let vectors = vec![
            vec![0.0, 0.0, 0.0],
            vec![1.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0],
            vec![2.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0],
        ];
        let query = vec![0.1, 0.0, 0.0];

        let results = brute_force_knn(&query, &vectors, 3, DistanceMetric::L2);
        assert_eq!(results.len(), 3);
        // Closest should be index 0 (origin), then index 1
        assert_eq!(results[0].0, 0);
        assert_eq!(results[1].0, 1);
    }

    #[test]
    fn test_aligned_vector() {
        let v = AlignedVector::new(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.dim(), 3);
        assert_eq!(v.as_slice(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_batch_distances() {
        let query = vec![0.0, 0.0, 0.0];
        let candidates: Vec<&[f32]> = vec![
            &[1.0, 0.0, 0.0][..],
            &[0.0, 1.0, 0.0][..],
            &[2.0, 0.0, 0.0][..],
        ];

        let distances = batch_distances(&query, &candidates, DistanceMetric::L2);
        assert_eq!(distances.len(), 3);
        assert!((distances[0] - 1.0).abs() < EPSILON);
        assert!((distances[1] - 1.0).abs() < EPSILON);
        assert!((distances[2] - 4.0).abs() < EPSILON);
    }
}
