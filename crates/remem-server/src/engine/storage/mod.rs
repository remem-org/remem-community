//! Core storage engine components
//!
//! This module contains the LSM-tree based storage engine implementation:
//! - MemTable: In-memory sorted storage using skip list
//! - WAL: Write-ahead log for durability
//! - SSTable: Sorted string tables for persistent storage
//! - Compaction: Background compaction for space reclamation
//! - Engine: High-level storage engine API

pub mod compaction;
pub mod durable_rename;
pub mod engine;
pub(crate) mod format;
mod init;
pub mod memtable;
pub(crate) mod migrations;
pub mod partition;
pub mod partition_catalog;
pub mod partitioned_hnsw;
pub(crate) mod partitioned_indexes;
mod recovery;
mod sequence;
pub mod sstable;
mod tasks;
mod tmp_sweep;
pub mod wal;
pub(crate) mod wal_commit;

pub use engine::StorageEngine;

#[cfg(test)]
mod wal_ownership_tests {
    #[test]
    fn runtime_engine_code_does_not_bypass_the_wal_coordinator() {
        let engine = include_str!("engine.rs");
        let production_engine = engine
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("engine source has a production section");
        let tasks = include_str!("tasks.rs");

        for (source_name, source) in [("engine.rs", production_engine), ("tasks.rs", tasks)] {
            for forbidden in [
                "wal.lock()",
                "wal.append(",
                "wal.append_batch(",
                "wal.sync()",
            ] {
                assert!(
                    !source.contains(forbidden),
                    "{source_name} bypasses WalCommitCoordinator with `{forbidden}`"
                );
            }
        }
    }
}
