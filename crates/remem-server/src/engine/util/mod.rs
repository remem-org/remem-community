//! Utility modules for the storage engine
#![allow(unused_imports)]

pub mod bloom;
pub mod simd;

pub use bloom::BloomFilter;
pub use simd::{
    cosine_distance, cosine_similarity, dot_product, l2_distance, l2_distance_squared, magnitude,
    normalize, AlignedVector, DistanceMetric,
};
