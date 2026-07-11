//! SSTable: Sorted String Table implementation
#![allow(unused_imports)]
//!
//! SSTables are immutable, sorted files that store key-value pairs on disk.
//! They are the persistent storage layer of the LSM-tree.
//!
//! ## File Format
//!
//! ```text
//! +-----------------+
//! | Header (64B)    |
//! +-----------------+
//! | Data Block 0    |
//! | Data Block 1    |
//! | ...             |
//! +-----------------+
//! | Index Block     |
//! +-----------------+
//! | Bloom Filter    |
//! +-----------------+
//! | Footer (32B)    |
//! +-----------------+
//! ```

mod block;
mod format;
mod reader;
mod writer;

pub use block::{Block, BlockCache};
pub use format::{Compression, IndexEntry, Record, SSTableMeta, BLOCK_SIZE, MAGIC};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;
