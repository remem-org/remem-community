//! SSTable: Sorted String Table implementation
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

pub use block::BlockCache;
pub use format::{Compression, Record};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;
