//! Core storage engine components
#![allow(unused_imports)]
//!
//! This module contains the LSM-tree based storage engine implementation:
//! - MemTable: In-memory sorted storage using skip list
//! - WAL: Write-ahead log for durability
//! - SSTable: Sorted string tables for persistent storage
//! - Compaction: Background compaction for space reclamation
//! - Engine: High-level storage engine API

pub mod compaction;
pub mod engine;
pub(self) mod init;
pub mod memtable;
pub(self) mod recovery;
pub mod sstable;
pub(self) mod tasks;
pub mod wal;

pub use compaction::{CompactionConfig, CompactionManager};
pub use engine::{
    EngineConfig, GraphIndexConfig, StorageEngine, StorageStats, TagIndexConfig, TimeSeriesConfig,
    VectorConfig, VectorSearchResult,
};
pub use memtable::MemTable;
pub use sstable::Compression;
pub use sstable::{SSTableReader, SSTableWriter};
pub use wal::WAL;
