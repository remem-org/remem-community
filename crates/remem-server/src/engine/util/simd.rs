//! SIMD-accelerated vector distance calculations
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
/// use remem_server::engine::util::simd::l2_distance_squared;
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
/// use remem_server::engine::util::simd::dot_product;
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
/// use remem_server::engine::util::simd::cosine_similarity;
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
}
