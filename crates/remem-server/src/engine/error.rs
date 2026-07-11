//! Error types for the storage engine
#![allow(dead_code)]

use std::path::PathBuf;
use thiserror::Error;

/// Result type alias for storage operations
pub type Result<T> = std::result::Result<T, StorageError>;

/// Storage engine error types
#[derive(Error, Debug)]
pub enum StorageError {
    /// I/O error during file operations
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Data corruption detected
    #[error("Data corruption in {file}: {message}")]
    Corruption { file: PathBuf, message: String },

    /// Checksum mismatch
    #[error("Checksum mismatch in {file}: expected {expected:#x}, got {actual:#x}")]
    ChecksumMismatch {
        file: PathBuf,
        expected: u32,
        actual: u32,
    },

    /// Invalid file format
    #[error("Invalid file format in {file}: {message}")]
    InvalidFormat { file: PathBuf, message: String },

    /// Key not found
    #[error("Key not found")]
    KeyNotFound,

    /// MemTable is full
    #[error("MemTable is full (size: {current} bytes, max: {max} bytes)")]
    MemTableFull { current: usize, max: usize },

    /// WAL replay error
    #[error("WAL replay error at offset {offset}: {message}")]
    WalReplayError { offset: u64, message: String },

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Compression error
    #[error("Compression error: {0}")]
    Compression(String),

    /// Decompression error
    #[error("Decompression error: {0}")]
    Decompression(String),

    /// Invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Engine is shutting down
    #[error("Storage engine is shutting down")]
    ShuttingDown,

    /// Compaction error
    #[error("Compaction error: {0}")]
    Compaction(String),

    /// Index not enabled
    #[error("Index not enabled: {0}")]
    IndexNotEnabled(String),
}

impl StorageError {
    /// Create a corruption error
    pub fn corruption(file: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::Corruption {
            file: file.into(),
            message: message.into(),
        }
    }

    /// Create a checksum mismatch error
    pub fn checksum_mismatch(file: impl Into<PathBuf>, expected: u32, actual: u32) -> Self {
        Self::ChecksumMismatch {
            file: file.into(),
            expected,
            actual,
        }
    }

    /// Create an invalid format error
    pub fn invalid_format(file: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::InvalidFormat {
            file: file.into(),
            message: message.into(),
        }
    }

    /// Create a WAL replay error
    pub fn wal_replay(offset: u64, message: impl Into<String>) -> Self {
        Self::WalReplayError {
            offset,
            message: message.into(),
        }
    }
}
