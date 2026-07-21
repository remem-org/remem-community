//! Crash-safe file replacement.
//!
//! `std::fs::rename` alone is not enough: on ext4 (and other journaling
//! filesystems), a rename is not guaranteed durable until the *parent
//! directory's* inode is fsynced -- a power loss right after `rename()`
//! returns can leave the directory entry pointing at the old file, or at
//! neither file, depending on journal replay. Every tmp+rename write path
//! in the engine (segment manifests, `.seg` chunk files, the HNSW
//! deleted-node set) should route its rename through this helper.

use std::fs::File;
use std::io::Result;
use std::path::Path;

/// Rename `tmp` to `dest`, then fsync `dest`'s parent directory.
///
/// Callers must fsync `tmp`'s file contents themselves *before* calling
/// this (typically right before dropping their open write handle) --
/// this function only makes the rename itself durable, not the bytes
/// written into `tmp`.
pub fn durable_rename(tmp: &Path, dest: &Path) -> Result<()> {
    std::fs::rename(tmp, dest)?;
    let dir = dest.parent().unwrap_or_else(|| Path::new("."));
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn durable_rename_moves_tmp_to_dest() {
        let dir = tempdir().unwrap();
        let tmp = dir.path().join("file.tmp");
        let dest = dir.path().join("file.dat");

        std::fs::write(&tmp, b"hello").unwrap();
        durable_rename(&tmp, &dest).unwrap();

        assert!(!tmp.exists());
        assert_eq!(std::fs::read(&dest).unwrap(), b"hello");
    }
}
