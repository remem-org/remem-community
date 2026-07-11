//! Bloom filter implementation for efficient membership testing
#![allow(dead_code)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A space-efficient probabilistic data structure for membership testing
///
/// Bloom filters can have false positives but never false negatives.
/// This means `may_contain` returning `true` means the key might be present,
/// while `false` means the key is definitely not present.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit vector storing the filter state
    bits: Vec<u64>,
    /// Number of bits in the filter
    num_bits: u32,
    /// Number of hash functions to use
    num_hashes: u8,
}

impl BloomFilter {
    /// Create a new bloom filter with optimal size for expected items and false positive rate
    ///
    /// # Arguments
    /// * `expected_items` - Expected number of items to insert
    /// * `fp_rate` - Desired false positive rate (e.g., 0.01 for 1%)
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        // Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let num_bits = Self::optimal_num_bits(expected_items, fp_rate);
        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        let num_hashes = Self::optimal_num_hashes(num_bits, expected_items);

        let num_words = (num_bits as usize + 63) / 64;

        Self {
            bits: vec![0; num_words],
            num_bits,
            num_hashes,
        }
    }

    /// Create a bloom filter with specific parameters
    pub fn with_params(num_bits: u32, num_hashes: u8) -> Self {
        let num_words = (num_bits as usize + 63) / 64;
        Self {
            bits: vec![0; num_words],
            num_bits,
            num_hashes,
        }
    }

    /// Calculate optimal number of bits for given items and false positive rate
    fn optimal_num_bits(n: usize, fp_rate: f64) -> u32 {
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let m = -(n as f64) * fp_rate.ln() / ln2_squared;
        // Round up to nearest 64 for word alignment
        let m = ((m as u32 + 63) / 64) * 64;
        m.max(64) // Minimum 64 bits
    }

    /// Calculate optimal number of hash functions
    fn optimal_num_hashes(num_bits: u32, num_items: usize) -> u8 {
        if num_items == 0 {
            return 1;
        }
        let k = (num_bits as f64 / num_items as f64) * std::f64::consts::LN_2;
        (k.ceil() as u8).clamp(1, 30) // Limit to reasonable range
    }

    /// Insert a key into the bloom filter
    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K) {
        let (h1, h2) = self.hash_key(key.as_ref());

        for i in 0..self.num_hashes as u64 {
            let bit_pos = self.get_bit_position(h1, h2, i);
            self.set_bit(bit_pos);
        }
    }

    /// Check if a key may be present in the bloom filter
    ///
    /// Returns `false` if the key is definitely not present.
    /// Returns `true` if the key might be present (possible false positive).
    pub fn may_contain<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let (h1, h2) = self.hash_key(key.as_ref());

        for i in 0..self.num_hashes as u64 {
            let bit_pos = self.get_bit_position(h1, h2, i);
            if !self.get_bit(bit_pos) {
                return false;
            }
        }
        true
    }

    /// Compute two hash values for double hashing
    fn hash_key(&self, key: &[u8]) -> (u64, u64) {
        // First hash
        let mut hasher1 = DefaultHasher::new();
        key.hash(&mut hasher1);
        let h1 = hasher1.finish();

        // Second hash (with salt)
        let mut hasher2 = DefaultHasher::new();
        hasher2.write_u64(h1);
        hasher2.write(&[0x9e, 0x37, 0x79, 0xb9]); // Salt
        key.hash(&mut hasher2);
        let h2 = hasher2.finish();

        (h1, h2)
    }

    /// Get bit position using double hashing: h(i) = h1 + i * h2
    fn get_bit_position(&self, h1: u64, h2: u64, i: u64) -> usize {
        let hash = h1.wrapping_add(i.wrapping_mul(h2));
        (hash % self.num_bits as u64) as usize
    }

    /// Set a bit at the given position
    fn set_bit(&mut self, pos: usize) {
        let word_idx = pos / 64;
        let bit_idx = pos % 64;
        self.bits[word_idx] |= 1 << bit_idx;
    }

    /// Get a bit at the given position
    fn get_bit(&self, pos: usize) -> bool {
        let word_idx = pos / 64;
        let bit_idx = pos % 64;
        (self.bits[word_idx] >> bit_idx) & 1 == 1
    }

    /// Get the number of bits in the filter
    pub fn num_bits(&self) -> u32 {
        self.num_bits
    }

    /// Get the number of hash functions
    pub fn num_hashes(&self) -> u8 {
        self.num_hashes
    }

    /// Encode the bloom filter to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(5 + self.bits.len() * 8);

        // Write header
        buf.extend_from_slice(&self.num_bits.to_le_bytes());
        buf.push(self.num_hashes);

        // Write bit vector
        for word in &self.bits {
            buf.extend_from_slice(&word.to_le_bytes());
        }

        buf
    }

    /// Decode a bloom filter from bytes
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 5 {
            return None;
        }

        let num_bits = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let num_hashes = data[4];

        let num_words = (num_bits as usize + 63) / 64;
        let expected_len = 5 + num_words * 8;

        if data.len() < expected_len {
            return None;
        }

        let mut bits = Vec::with_capacity(num_words);
        let mut offset = 5;

        for _ in 0..num_words {
            let word = u64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            bits.push(word);
            offset += 8;
        }

        Some(Self {
            bits,
            num_bits,
            num_hashes,
        })
    }

    /// Estimate the current false positive rate based on fill ratio
    pub fn estimated_fp_rate(&self) -> f64 {
        let set_bits: usize = self.bits.iter().map(|w| w.count_ones() as usize).sum();
        let fill_ratio = set_bits as f64 / self.num_bits as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    /// Check if the filter is empty
    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|&w| w == 0)
    }

    /// Clear all bits in the filter
    pub fn clear(&mut self) {
        for word in &mut self.bits {
            *word = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_check() {
        let mut bloom = BloomFilter::new(1000, 0.01);

        // Insert some keys
        bloom.insert(b"hello");
        bloom.insert(b"world");
        bloom.insert(b"test");

        // Check inserted keys
        assert!(bloom.may_contain(b"hello"));
        assert!(bloom.may_contain(b"world"));
        assert!(bloom.may_contain(b"test"));
    }

    #[test]
    fn test_false_negatives() {
        let mut bloom = BloomFilter::new(1000, 0.01);

        // Insert keys
        for i in 0..100 {
            bloom.insert(format!("key{}", i).as_bytes());
        }

        // All inserted keys must be found (no false negatives)
        for i in 0..100 {
            assert!(bloom.may_contain(format!("key{}", i).as_bytes()));
        }
    }

    #[test]
    fn test_false_positive_rate() {
        let num_items = 10000;
        let expected_fp_rate = 0.01;
        let mut bloom = BloomFilter::new(num_items, expected_fp_rate);

        // Insert items
        for i in 0..num_items {
            bloom.insert(format!("insert{}", i).as_bytes());
        }

        // Check false positive rate with non-inserted items
        let test_count = 10000;
        let mut false_positives = 0;

        for i in 0..test_count {
            if bloom.may_contain(format!("check{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let actual_fp_rate = false_positives as f64 / test_count as f64;

        // Allow some tolerance (2x expected rate)
        assert!(
            actual_fp_rate < expected_fp_rate * 2.0,
            "False positive rate {} too high (expected < {})",
            actual_fp_rate,
            expected_fp_rate * 2.0
        );
    }

    #[test]
    fn test_encode_decode() {
        let mut bloom = BloomFilter::new(100, 0.01);

        bloom.insert(b"key1");
        bloom.insert(b"key2");
        bloom.insert(b"key3");

        let encoded = bloom.encode();
        let decoded = BloomFilter::decode(&encoded).unwrap();

        assert_eq!(bloom.num_bits(), decoded.num_bits());
        assert_eq!(bloom.num_hashes(), decoded.num_hashes());
        assert!(decoded.may_contain(b"key1"));
        assert!(decoded.may_contain(b"key2"));
        assert!(decoded.may_contain(b"key3"));
    }

    #[test]
    fn test_empty_filter() {
        let bloom = BloomFilter::new(100, 0.01);
        assert!(bloom.is_empty());

        let mut bloom2 = BloomFilter::new(100, 0.01);
        bloom2.insert(b"test");
        assert!(!bloom2.is_empty());
    }

    #[test]
    fn test_clear() {
        let mut bloom = BloomFilter::new(100, 0.01);
        bloom.insert(b"test");
        assert!(!bloom.is_empty());

        bloom.clear();
        assert!(bloom.is_empty());
    }
}
