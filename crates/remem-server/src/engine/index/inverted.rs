//! Inverted index for tag and text search
//!
//! This module implements an inverted index optimized for:
//! - Tag-based lookups: Find records with specific tags
//! - Text search: Find records containing specific words
//! - Boolean queries: AND/OR combinations
//!
//! # Design
//!
//! The inverted index maps tokens (tags/words) to lists of document keys:
//! - tokens: HashMap<String, PostingList>
//! - PostingList: Sorted list of (key, score) pairs
//!
//! For efficient boolean operations, posting lists are kept sorted by key.

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::engine::error::{Result, StorageError};

/// Configuration for the inverted index
#[derive(Debug, Clone)]
pub struct InvertedIndexConfig {
    /// Whether to normalize tokens to lowercase
    pub lowercase: bool,
    /// Minimum token length to index
    pub min_token_length: usize,
    /// Maximum token length to index
    pub max_token_length: usize,
}

impl Default for InvertedIndexConfig {
    fn default() -> Self {
        Self {
            lowercase: true,
            min_token_length: 1,
            max_token_length: 100,
        }
    }
}

impl InvertedIndexConfig {
    /// Set lowercase normalization
    pub fn lowercase(mut self, lowercase: bool) -> Self {
        self.lowercase = lowercase;
        self
    }

    /// Set minimum token length
    pub fn min_token_length(mut self, len: usize) -> Self {
        self.min_token_length = len;
        self
    }
}

/// A posting in the inverted index
#[derive(Debug, Clone)]
struct Posting {
    /// Document key
    key: Bytes,
    /// Term frequency or relevance score
    score: f32,
}

impl Posting {
    fn new(key: Bytes, score: f32) -> Self {
        Self { key, score }
    }
}

/// Posting list for a single term
#[derive(Debug, Clone, Default)]
struct PostingList {
    /// List of postings, sorted by key for efficient merge operations
    postings: Vec<Posting>,
}

impl PostingList {
    fn new() -> Self {
        Self {
            postings: Vec::new(),
        }
    }

    fn add(&mut self, key: Bytes, score: f32) {
        // Keep sorted by key for efficient intersection/union
        match self.postings.binary_search_by(|p| p.key.cmp(&key)) {
            Ok(pos) => {
                // Update existing posting (accumulate score)
                self.postings[pos].score += score;
            }
            Err(pos) => {
                // Insert at sorted position
                self.postings.insert(pos, Posting::new(key, score));
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> bool {
        if let Ok(pos) = self.postings.binary_search_by(|p| p.key.as_ref().cmp(key)) {
            self.postings.remove(pos);
            true
        } else {
            false
        }
    }

    fn get_keys(&self) -> Vec<Bytes> {
        self.postings.iter().map(|p| p.key.clone()).collect()
    }

    fn get_postings(&self) -> &[Posting] {
        &self.postings
    }

    fn len(&self) -> usize {
        self.postings.len()
    }

    fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }
}

/// Inverted index for tag and text search
pub struct InvertedIndex {
    /// Configuration
    config: InvertedIndexConfig,

    /// Token -> Posting list mapping
    index: RwLock<HashMap<String, PostingList>>,

    /// Key -> Tokens mapping (for deletion)
    key_to_tokens: RwLock<HashMap<Bytes, Vec<String>>>,

    /// Total number of indexed documents
    doc_count: AtomicUsize,

    /// Total number of tokens (including duplicates)
    token_count: AtomicUsize,

    /// Whether the index has been modified
    dirty: AtomicBool,
}

impl InvertedIndex {
    /// Create a new empty inverted index
    pub fn new(config: InvertedIndexConfig) -> Self {
        Self {
            config,
            index: RwLock::new(HashMap::new()),
            key_to_tokens: RwLock::new(HashMap::new()),
            doc_count: AtomicUsize::new(0),
            token_count: AtomicUsize::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    /// Normalize a token according to config
    fn normalize_token(&self, token: &str) -> Option<String> {
        let token = if self.config.lowercase {
            token.to_lowercase()
        } else {
            token.to_string()
        };

        if token.len() < self.config.min_token_length || token.len() > self.config.max_token_length
        {
            return None;
        }

        Some(token)
    }

    /// Add a tag to a document (preserves existing tags)
    pub fn add_tag(&self, key: impl Into<Bytes>, tag: &str) -> Result<()> {
        let key = key.into();
        let token = self
            .normalize_token(tag)
            .ok_or_else(|| StorageError::InvalidArgument(format!("Invalid tag: {}", tag)))?;

        let was_new_doc = {
            let mut key_to_tokens = self.key_to_tokens.write();

            // Check if this document already has this token
            if let Some(tokens) = key_to_tokens.get_mut(&key) {
                if tokens.contains(&token) {
                    // Already has this tag, nothing to do
                    return Ok(());
                }
                tokens.push(token.clone());
                false
            } else {
                key_to_tokens.insert(key.clone(), vec![token.clone()]);
                true
            }
        };

        // Add to posting list
        {
            let mut index = self.index.write();
            index.entry(token).or_default().add(key, 1.0);
        }

        if was_new_doc {
            self.doc_count.fetch_add(1, Ordering::Relaxed);
        }
        self.token_count.fetch_add(1, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Add multiple tags to a document (preserves existing tags)
    pub fn add_tags(&self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let key = key.into();

        for tag in tags {
            if let Some(token) = self.normalize_token(tag) {
                self.add_tag(key.clone(), &token)?;
            }
        }

        Ok(())
    }

    /// Set tags for a document (replaces all existing tags)
    pub fn set_tags(&self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let key = key.into();
        let tokens: Vec<String> = tags
            .iter()
            .filter_map(|t| self.normalize_token(t))
            .collect();

        if tokens.is_empty() {
            // If no valid tokens, just remove the document
            self.remove(&key)?;
            return Ok(());
        }

        self.index_tokens(key, tokens)
    }

    /// Internal: index a set of tokens for a key
    fn index_tokens(&self, key: Bytes, tokens: Vec<String>) -> Result<()> {
        // Remove existing tokens for this key first
        let was_new = {
            let mut key_to_tokens = self.key_to_tokens.write();
            let mut index = self.index.write();

            if let Some(old_tokens) = key_to_tokens.get(&key) {
                for token in old_tokens {
                    if let Some(posting_list) = index.get_mut(token) {
                        posting_list.remove(&key);
                        if posting_list.is_empty() {
                            index.remove(token);
                        }
                    }
                }
                key_to_tokens.remove(&key);
                false
            } else {
                true
            }
        };

        // Count token frequencies for TF scoring
        let mut token_freqs: HashMap<&str, usize> = HashMap::new();
        for token in &tokens {
            *token_freqs.entry(token).or_insert(0) += 1;
        }

        // Add new tokens
        {
            let mut index = self.index.write();
            let mut key_to_tokens = self.key_to_tokens.write();

            let unique_tokens: Vec<String> = token_freqs.keys().map(|&s| s.to_string()).collect();

            for (token, freq) in token_freqs {
                // Use TF as score (term frequency)
                let score = freq as f32;
                index
                    .entry(token.to_string())
                    .or_default()
                    .add(key.clone(), score);
            }

            key_to_tokens.insert(key, unique_tokens);
        }

        if was_new {
            self.doc_count.fetch_add(1, Ordering::Relaxed);
        }
        self.token_count.fetch_add(tokens.len(), Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);

        Ok(())
    }

    /// Remove a document from the index
    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let mut key_to_tokens = self.key_to_tokens.write();
        let mut index = self.index.write();

        let Some(tokens) = key_to_tokens.remove(key) else {
            return Ok(false);
        };

        for token in &tokens {
            if let Some(posting_list) = index.get_mut(token) {
                posting_list.remove(key);
                if posting_list.is_empty() {
                    index.remove(token);
                }
            }
        }

        self.doc_count.fetch_sub(1, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
        Ok(true)
    }

    /// Whether this index can represent `token` at all.
    ///
    /// `add_tags` drops a tag that fails normalization, so a tag outside the
    /// configured length bounds lives in the payload with no posting list of
    /// its own. `search_and` returns nothing for such a token, which a caller
    /// cannot tell apart from a genuine miss — so a caller narrowing a result
    /// set through this index has to ask first.
    pub fn can_represent(&self, token: &str) -> bool {
        self.normalize_token(token).is_some()
    }

    /// Search for documents containing a single token/tag
    pub fn search(&self, query: &str) -> Vec<Bytes> {
        let token = match self.normalize_token(query) {
            Some(t) => t,
            None => return Vec::new(),
        };

        let index = self.index.read();
        index
            .get(&token)
            .map(|pl| pl.get_keys())
            .unwrap_or_default()
    }

    /// Search with scoring (returns keys sorted by relevance)
    pub fn search_scored(&self, query: &str) -> Vec<(Bytes, f32)> {
        let token = match self.normalize_token(query) {
            Some(t) => t,
            None => return Vec::new(),
        };

        let index = self.index.read();
        let Some(posting_list) = index.get(&token) else {
            return Vec::new();
        };

        let mut results: Vec<(Bytes, f32)> = posting_list
            .get_postings()
            .iter()
            .map(|p| (p.key.clone(), p.score))
            .collect();

        // Sort by score descending
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Search for documents containing ALL of the given tokens (AND query)
    pub fn search_and(&self, queries: &[&str]) -> Vec<Bytes> {
        if queries.is_empty() {
            return Vec::new();
        }

        let tokens: Vec<String> = queries
            .iter()
            .filter_map(|q| self.normalize_token(q))
            .collect();

        if tokens.is_empty() {
            return Vec::new();
        }

        let index = self.index.read();

        // Get posting lists for all tokens
        let mut posting_lists: Vec<&PostingList> = Vec::new();
        for token in &tokens {
            match index.get(token) {
                Some(pl) => posting_lists.push(pl),
                None => return Vec::new(), // If any token is missing, no results
            }
        }

        // Sort by size for efficient intersection (smallest first)
        posting_lists.sort_by_key(|pl| pl.len());

        // Start with smallest list
        let mut result_set: HashSet<Bytes> = posting_lists[0].get_keys().into_iter().collect();

        // Intersect with remaining lists
        for pl in posting_lists.iter().skip(1) {
            let keys: HashSet<Bytes> = pl.get_keys().into_iter().collect();
            result_set.retain(|k| keys.contains(k));

            if result_set.is_empty() {
                break;
            }
        }

        result_set.into_iter().collect()
    }

    /// Search with OR and scoring (results sorted by number of matching tokens)
    pub fn search_or_scored(&self, queries: &[&str]) -> Vec<(Bytes, f32)> {
        let tokens: Vec<String> = queries
            .iter()
            .filter_map(|q| self.normalize_token(q))
            .collect();

        if tokens.is_empty() {
            return Vec::new();
        }

        let index = self.index.read();
        let mut scores: HashMap<Bytes, f32> = HashMap::new();

        for token in &tokens {
            if let Some(pl) = index.get(token) {
                for posting in pl.get_postings() {
                    *scores.entry(posting.key.clone()).or_insert(0.0) += posting.score;
                }
            }
        }

        let mut results: Vec<(Bytes, f32)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Get all tokens for a document
    pub fn get_tokens(&self, key: &[u8]) -> Vec<String> {
        let key_to_tokens = self.key_to_tokens.read();
        key_to_tokens.get(key).cloned().unwrap_or_default()
    }

    /// Get all unique tokens in the index
    pub fn all_tokens(&self) -> Vec<String> {
        let index = self.index.read();
        index.keys().cloned().collect()
    }

    /// Get number of indexed documents
    pub fn len(&self) -> usize {
        self.doc_count.load(Ordering::Relaxed)
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if the index has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    /// Load an index from a file
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = std::fs::File::open(path)?;
        let mut file = std::io::BufReader::new(file);

        // Read and verify magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != b"INVI" {
            return Err(StorageError::invalid_format(
                path,
                "Invalid inverted index magic",
            ));
        }

        // Read version
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4)?;
        let version = u32::from_le_bytes(buf4);
        if version != 1 {
            return Err(StorageError::invalid_format(
                path,
                format!("Unsupported inverted index version: {}", version),
            ));
        }

        // Read config
        let mut bool_byte = [0u8; 1];
        file.read_exact(&mut bool_byte)?;
        let lowercase = bool_byte[0] != 0;

        file.read_exact(&mut buf4)?;
        let min_token_length = u32::from_le_bytes(buf4) as usize;

        file.read_exact(&mut buf4)?;
        let max_token_length = u32::from_le_bytes(buf4) as usize;

        // `token_separators` was dropped from `InvertedIndexConfig` in REM-36
        // (dead field once `tokenize()`/`index_text()` -- its only readers --
        // were deleted as dead code). The on-disk `.idx` format still has a
        // length-prefixed separator string at this position, so the bytes
        // must still be consumed to keep the reader aligned for the fields
        // that follow, even though the value itself is now discarded. Same
        // treatment as the `BTIX` field-skip handling from sub-task 6b.
        file.read_exact(&mut buf4)?;
        let sep_len = u32::from_le_bytes(buf4) as usize;
        let mut sep_bytes = vec![0u8; sep_len];
        file.read_exact(&mut sep_bytes)?;
        drop(sep_bytes);

        let config = InvertedIndexConfig {
            lowercase,
            min_token_length,
            max_token_length,
        };

        // Read index
        file.read_exact(&mut buf4)?;
        let token_count = u32::from_le_bytes(buf4) as usize;

        let mut index = HashMap::with_capacity(token_count);

        for _ in 0..token_count {
            file.read_exact(&mut buf4)?;
            let token_len = u32::from_le_bytes(buf4) as usize;
            let mut token_bytes = vec![0u8; token_len];
            file.read_exact(&mut token_bytes)?;
            let token = String::from_utf8(token_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            file.read_exact(&mut buf4)?;
            let posting_count = u32::from_le_bytes(buf4) as usize;

            let mut posting_list = PostingList::new();
            for _ in 0..posting_count {
                file.read_exact(&mut buf4)?;
                let key_len = u32::from_le_bytes(buf4) as usize;
                let mut key_bytes = vec![0u8; key_len];
                file.read_exact(&mut key_bytes)?;
                let key = Bytes::from(key_bytes);

                file.read_exact(&mut buf4)?;
                let score = f32::from_le_bytes(buf4);

                posting_list.postings.push(Posting::new(key, score));
            }

            index.insert(token, posting_list);
        }

        // Read key_to_tokens mapping
        file.read_exact(&mut buf4)?;
        let doc_count = u32::from_le_bytes(buf4) as usize;

        let mut key_to_tokens = HashMap::with_capacity(doc_count);

        for _ in 0..doc_count {
            file.read_exact(&mut buf4)?;
            let key_len = u32::from_le_bytes(buf4) as usize;
            let mut key_bytes = vec![0u8; key_len];
            file.read_exact(&mut key_bytes)?;
            let key = Bytes::from(key_bytes);

            file.read_exact(&mut buf4)?;
            let token_count = u32::from_le_bytes(buf4) as usize;

            let mut tokens = Vec::with_capacity(token_count);
            for _ in 0..token_count {
                file.read_exact(&mut buf4)?;
                let token_len = u32::from_le_bytes(buf4) as usize;
                let mut token_bytes = vec![0u8; token_len];
                file.read_exact(&mut token_bytes)?;
                let token = String::from_utf8(token_bytes)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                tokens.push(token);
            }

            key_to_tokens.insert(key, tokens);
        }

        Ok(Self {
            config,
            index: RwLock::new(index),
            key_to_tokens: RwLock::new(key_to_tokens),
            doc_count: AtomicUsize::new(doc_count),
            token_count: AtomicUsize::new(0), // Not persisted, recalculated if needed
            dirty: AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_index() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(index.search("test").is_empty());
    }

    #[test]
    fn can_represent_rejects_a_token_over_the_length_bound() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());
        let too_long = "a".repeat(101);
        assert!(!index.can_represent(&too_long));
        assert!(index.can_represent("rust"));
    }

    #[test]
    fn test_add_and_search_tag() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index.add_tag(b"doc1".to_vec(), "rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "python").unwrap();

        let rust_docs = index.search("rust");
        assert_eq!(rust_docs.len(), 2);

        let python_docs = index.search("python");
        assert_eq!(python_docs.len(), 1);
        assert_eq!(python_docs[0].as_ref(), b"doc2");
    }

    #[test]
    fn test_case_insensitive() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index.add_tag(b"doc1".to_vec(), "Rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "rust").unwrap();
        index.add_tag(b"doc3".to_vec(), "RUST").unwrap();

        let docs = index.search("rust");
        assert_eq!(docs.len(), 3);

        let docs = index.search("RUST");
        assert_eq!(docs.len(), 3);
    }

    #[test]
    fn test_and_query() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index
            .add_tags(
                b"doc1".to_vec(),
                &["rust".to_string(), "programming".to_string()],
            )
            .unwrap();
        index
            .add_tags(b"doc2".to_vec(), &["rust".to_string(), "web".to_string()])
            .unwrap();
        index
            .add_tags(b"doc3".to_vec(), &["programming".to_string()])
            .unwrap();

        let docs = index.search_and(&["rust", "programming"]);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].as_ref(), b"doc1");
    }

    #[test]
    fn test_scored_search() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        // Doc1 has "rust" twice (set_tags/index_tokens computes TF, so a
        // repeated tag in the same call counts twice -- was originally
        // written with the now-deleted `index_text("rust rust programming")`;
        // `index_text`/`tokenize` were dead code cascaded from REM-36's
        // removal of `StorageEngine::index_text`, but the TF-scoring
        // behavior under test here is unrelated and still live).
        index
            .set_tags(
                b"doc1".to_vec(),
                &[
                    "rust".to_string(),
                    "rust".to_string(),
                    "programming".to_string(),
                ],
            )
            .unwrap();
        // Doc2 has "rust" once
        index
            .set_tags(
                b"doc2".to_vec(),
                &["rust".to_string(), "programming".to_string()],
            )
            .unwrap();

        let results = index.search_scored("rust");
        assert_eq!(results.len(), 2);
        // Doc1 should have higher score
        assert_eq!(results[0].0.as_ref(), b"doc1");
        assert!(results[0].1 > results[1].1);
    }

    #[test]
    fn test_remove() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index.add_tag(b"doc1".to_vec(), "rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "rust").unwrap();

        assert_eq!(index.len(), 2);

        index.remove(b"doc1").unwrap();
        assert_eq!(index.len(), 1);

        let docs = index.search("rust");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].as_ref(), b"doc2");
    }

    #[test]
    fn test_get_tokens() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index
            .add_tags(
                b"doc1".to_vec(),
                &["rust".to_string(), "programming".to_string()],
            )
            .unwrap();

        let tokens = index.get_tokens(b"doc1");
        assert_eq!(tokens.len(), 2);
        assert!(tokens.contains(&"rust".to_string()));
        assert!(tokens.contains(&"programming".to_string()));
    }

    #[test]
    fn test_set_tags_replaces_existing_tags() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index
            .set_tags(b"doc1".to_vec(), &["old".to_string()])
            .unwrap();
        assert_eq!(index.get_tokens(b"doc1"), vec!["old".to_string()]);

        // set_tags replaces the previous tag set rather than merging with it
        index
            .set_tags(b"doc1".to_vec(), &["new".to_string()])
            .unwrap();
        let tokens = index.get_tokens(b"doc1");
        assert_eq!(tokens.len(), 1);
        assert!(tokens.contains(&"new".to_string()));
        assert!(!tokens.contains(&"old".to_string()));

        // add_tags, by contrast, preserves existing tags
        index
            .add_tags(b"doc1".to_vec(), &["another".to_string()])
            .unwrap();
        let tokens = index.get_tokens(b"doc1");
        assert_eq!(tokens.len(), 2);
        assert!(tokens.contains(&"new".to_string()));
        assert!(tokens.contains(&"another".to_string()));
    }

    #[test]
    fn test_all_tokens() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index.add_tag(b"doc1".to_vec(), "rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "python").unwrap();
        index.add_tag(b"doc3".to_vec(), "java").unwrap();

        let tokens = index.all_tokens();
        assert_eq!(tokens.len(), 3);
    }

    #[test]
    fn test_min_token_length() {
        let config = InvertedIndexConfig::default().min_token_length(3);
        let index = InvertedIndex::new(config);

        // Was originally written with the now-deleted `index_text("a ab abc
        // abcd")`; `normalize_token` (called by `add_tags` too, not just the
        // deleted `tokenize`/`index_text`) applies the same `min_token_length`
        // filter, so `add_tags` exercises the identical behavior.
        index
            .add_tags(
                b"doc1".to_vec(),
                &[
                    "a".to_string(),
                    "ab".to_string(),
                    "abc".to_string(),
                    "abcd".to_string(),
                ],
            )
            .unwrap();

        // Short tokens should be filtered out
        assert!(index.search("a").is_empty());
        assert!(index.search("ab").is_empty());
        assert!(!index.search("abc").is_empty());
        assert!(!index.search("abcd").is_empty());
    }
}
