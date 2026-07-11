//! Inverted index for tag and text search
#![allow(dead_code)]
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
use std::io::{Read, Write};
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
    /// Characters that separate tokens (default: whitespace + punctuation)
    pub token_separators: String,
}

impl Default for InvertedIndexConfig {
    fn default() -> Self {
        Self {
            lowercase: true,
            min_token_length: 1,
            max_token_length: 100,
            token_separators: " \t\n\r,.;:!?()[]{}\"'`~@#$%^&*-+=<>/\\|".to_string(),
        }
    }
}

impl InvertedIndexConfig {
    /// Create a config for exact tag matching (no normalization)
    pub fn exact_tags() -> Self {
        Self {
            lowercase: false,
            min_token_length: 1,
            max_token_length: 200,
            token_separators: String::new(),
        }
    }

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

    /// Get the minimum token length configuration
    pub fn min_token_length(&self) -> usize {
        self.config.min_token_length
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

    /// Tokenize text into individual tokens
    fn tokenize(&self, text: &str) -> Vec<String> {
        if self.config.token_separators.is_empty() {
            // No tokenization, treat as single token
            return self.normalize_token(text).into_iter().collect();
        }

        let separator_chars: HashSet<char> = self.config.token_separators.chars().collect();

        text.split(|c| separator_chars.contains(&c))
            .filter_map(|t| self.normalize_token(t))
            .collect()
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
            index
                .entry(token)
                .or_insert_with(PostingList::new)
                .add(key, 1.0);
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

    /// Index text content for a document
    pub fn index_text(&self, key: impl Into<Bytes>, text: &str) -> Result<()> {
        let key = key.into();
        let tokens = self.tokenize(text);

        if tokens.is_empty() {
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
                    .or_insert_with(PostingList::new)
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

    /// Search for documents containing ANY of the given tokens (OR query)
    pub fn search_or(&self, queries: &[&str]) -> Vec<Bytes> {
        let tokens: Vec<String> = queries
            .iter()
            .filter_map(|q| self.normalize_token(q))
            .collect();

        if tokens.is_empty() {
            return Vec::new();
        }

        let index = self.index.read();
        let mut result_set: HashSet<Bytes> = HashSet::new();

        for token in &tokens {
            if let Some(pl) = index.get(token) {
                for key in pl.get_keys() {
                    result_set.insert(key);
                }
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

    /// Check if a document key exists in the index
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let key_to_tokens = self.key_to_tokens.read();
        key_to_tokens.contains_key(key)
    }

    /// Check if a document has a specific tag/token
    pub fn has_token(&self, key: &[u8], token: &str) -> bool {
        let normalized = match self.normalize_token(token) {
            Some(t) => t,
            None => return false,
        };

        let key_to_tokens = self.key_to_tokens.read();
        key_to_tokens
            .get(key)
            .map(|tokens| tokens.contains(&normalized))
            .unwrap_or(false)
    }

    /// Get all unique tokens in the index
    pub fn all_tokens(&self) -> Vec<String> {
        let index = self.index.read();
        index.keys().cloned().collect()
    }

    /// Get token count for a specific token
    pub fn token_doc_count(&self, token: &str) -> usize {
        let normalized = match self.normalize_token(token) {
            Some(t) => t,
            None => return 0,
        };

        let index = self.index.read();
        index.get(&normalized).map(|pl| pl.len()).unwrap_or(0)
    }

    /// Get number of indexed documents
    pub fn len(&self) -> usize {
        self.doc_count.load(Ordering::Relaxed)
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get number of unique tokens
    pub fn unique_token_count(&self) -> usize {
        self.index.read().len()
    }

    /// Check if the index has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    /// Mark the index as clean
    pub fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Relaxed);
    }

    /// Save the index to a file.
    ///
    /// Both read locks (`index` and `key_to_tokens`) are held simultaneously
    /// only for the in-memory snapshot, not during disk I/O. This gives a
    /// consistent point-in-time view and releases locks before any blocking
    /// writes.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        // --- Snapshot while holding both read locks simultaneously ---
        // Type aliases keep the snapshot types readable.
        // index_snap: Vec<(token, Vec<(key, score)>)>
        // ktt_snap:   Vec<(key, Vec<token>)>
        let (index_snap, ktt_snap) = {
            let index = self.index.read();
            let key_to_tokens = self.key_to_tokens.read();

            let idx: Vec<(String, Vec<(Bytes, f32)>)> = index
                .iter()
                .map(|(token, pl)| {
                    let postings = pl
                        .get_postings()
                        .iter()
                        .map(|p| (p.key.clone(), p.score))
                        .collect();
                    (token.clone(), postings)
                })
                .collect();

            let ktt: Vec<(Bytes, Vec<String>)> = key_to_tokens
                .iter()
                .map(|(k, ts)| (k.clone(), ts.clone()))
                .collect();

            (idx, ktt)
            // both read locks released here
        };

        // --- Write snapshot to disk without holding any lock ---
        let tmp_path = path.with_extension("tmp");
        let file = std::fs::File::create(&tmp_path)?;
        let mut writer = std::io::BufWriter::new(file);

        // Write header
        writer.write_all(b"INVI")?;
        writer.write_all(&1u32.to_le_bytes())?;

        // Write config
        writer.write_all(&[self.config.lowercase as u8])?;
        writer.write_all(&(self.config.min_token_length as u32).to_le_bytes())?;
        writer.write_all(&(self.config.max_token_length as u32).to_le_bytes())?;
        let sep_bytes = self.config.token_separators.as_bytes();
        writer.write_all(&(sep_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(sep_bytes)?;

        // Write index
        writer.write_all(&(index_snap.len() as u32).to_le_bytes())?;
        for (token, postings) in &index_snap {
            let token_bytes = token.as_bytes();
            writer.write_all(&(token_bytes.len() as u32).to_le_bytes())?;
            writer.write_all(token_bytes)?;
            writer.write_all(&(postings.len() as u32).to_le_bytes())?;
            for (key, score) in postings {
                writer.write_all(&(key.len() as u32).to_le_bytes())?;
                writer.write_all(key)?;
                writer.write_all(&score.to_le_bytes())?;
            }
        }

        // Write key_to_tokens mapping
        writer.write_all(&(ktt_snap.len() as u32).to_le_bytes())?;
        for (key, tokens) in &ktt_snap {
            writer.write_all(&(key.len() as u32).to_le_bytes())?;
            writer.write_all(key)?;
            writer.write_all(&(tokens.len() as u32).to_le_bytes())?;
            for token in tokens {
                let token_bytes = token.as_bytes();
                writer.write_all(&(token_bytes.len() as u32).to_le_bytes())?;
                writer.write_all(token_bytes)?;
            }
        }

        writer.flush()?;
        drop(writer);
        std::fs::rename(&tmp_path, path)?;
        self.mark_clean();
        Ok(())
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

        file.read_exact(&mut buf4)?;
        let sep_len = u32::from_le_bytes(buf4) as usize;
        let mut sep_bytes = vec![0u8; sep_len];
        file.read_exact(&mut sep_bytes)?;
        let token_separators =
            String::from_utf8(sep_bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;

        let config = InvertedIndexConfig {
            lowercase,
            min_token_length,
            max_token_length,
            token_separators,
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

    /// Clear all entries
    pub fn clear(&self) {
        self.index.write().clear();
        self.key_to_tokens.write().clear();
        self.doc_count.store(0, Ordering::Relaxed);
        self.token_count.store(0, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
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
    fn test_add_tags() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index
            .add_tags(
                b"doc1".to_vec(),
                &["rust".to_string(), "programming".to_string()],
            )
            .unwrap();

        assert!(index.has_token(b"doc1", "rust"));
        assert!(index.has_token(b"doc1", "programming"));
        assert!(!index.has_token(b"doc1", "java"));
    }

    #[test]
    fn test_index_text() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index
            .index_text(b"doc1".to_vec(), "The quick brown fox")
            .unwrap();
        index
            .index_text(b"doc2".to_vec(), "The lazy brown dog")
            .unwrap();

        let quick_docs = index.search("quick");
        assert_eq!(quick_docs.len(), 1);

        let brown_docs = index.search("brown");
        assert_eq!(brown_docs.len(), 2);
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
    fn test_exact_tags() {
        let index = InvertedIndex::new(InvertedIndexConfig::exact_tags());

        index.add_tag(b"doc1".to_vec(), "Rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "rust").unwrap();

        // Case-sensitive
        let docs = index.search("Rust");
        assert_eq!(docs.len(), 1);

        let docs = index.search("rust");
        assert_eq!(docs.len(), 1);
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
    fn test_or_query() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index.add_tag(b"doc1".to_vec(), "rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "python").unwrap();
        index.add_tag(b"doc3".to_vec(), "java").unwrap();

        let docs = index.search_or(&["rust", "python"]);
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_scored_search() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        // Doc1 has "rust" twice
        index
            .index_text(b"doc1".to_vec(), "rust rust programming")
            .unwrap();
        // Doc2 has "rust" once
        index
            .index_text(b"doc2".to_vec(), "rust programming")
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
    fn test_all_tokens() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index.add_tag(b"doc1".to_vec(), "rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "python").unwrap();
        index.add_tag(b"doc3".to_vec(), "java").unwrap();

        let tokens = index.all_tokens();
        assert_eq!(tokens.len(), 3);
    }

    #[test]
    fn test_save_and_load() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index.add_tag(b"doc1".to_vec(), "rust").unwrap();
        index.index_text(b"doc2".to_vec(), "hello world").unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.inv");

        index.save(&path).unwrap();

        let loaded = InvertedIndex::load(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        let rust_docs = loaded.search("rust");
        assert_eq!(rust_docs.len(), 1);

        let hello_docs = loaded.search("hello");
        assert_eq!(hello_docs.len(), 1);
    }

    #[test]
    fn test_update_document() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index
            .set_tags(b"doc1".to_vec(), &["old".to_string()])
            .unwrap();
        assert!(index.has_token(b"doc1", "old"));

        // Update with new tags (set_tags replaces all tags)
        index
            .set_tags(b"doc1".to_vec(), &["new".to_string()])
            .unwrap();
        assert!(!index.has_token(b"doc1", "old"));
        assert!(index.has_token(b"doc1", "new"));

        // Verify add_tags preserves existing
        index
            .add_tags(b"doc1".to_vec(), &["another".to_string()])
            .unwrap();
        assert!(index.has_token(b"doc1", "new"));
        assert!(index.has_token(b"doc1", "another"));
    }

    #[test]
    fn test_min_token_length() {
        let config = InvertedIndexConfig::default().min_token_length(3);
        let index = InvertedIndex::new(config);

        index.index_text(b"doc1".to_vec(), "a ab abc abcd").unwrap();

        // Short tokens should be filtered out
        assert!(index.search("a").is_empty());
        assert!(index.search("ab").is_empty());
        assert!(!index.search("abc").is_empty());
        assert!(!index.search("abcd").is_empty());
    }

    #[test]
    fn test_clear() {
        let index = InvertedIndex::new(InvertedIndexConfig::default());

        index.add_tag(b"doc1".to_vec(), "rust").unwrap();
        index.add_tag(b"doc2".to_vec(), "python").unwrap();

        assert_eq!(index.len(), 2);

        index.clear();
        assert!(index.is_empty());
        assert_eq!(index.unique_token_count(), 0);
    }
}
