//! Startup sweep for orphaned `.tmp` files.
//!
//! Every tmp+rename write path in the engine (manifest, segment writer,
//! HNSW deleted-nodes file, SSTable writer -- see [`super::durable_rename`])
//! writes its `.tmp` file, fsyncs it, then renames it onto the final path.
//! A crash between the write and the rename leaves the `.tmp` file behind:
//! harmless (readers only look at sealed extensions, and the next write to
//! that path overwrites the leftover tmp), but it accumulates as disk
//! clutter after repeated crashes. This sweep removes any leftover `.tmp`
//! file it finds in a directory at startup, before that directory's
//! writers get a chance to reuse the name.

use std::path::Path;

/// Remove every top-level file in `dir` whose extension is `tmp`.
///
/// Best-effort: a directory that doesn't exist yet is not an error (fresh
/// data dir), and a failure to remove an individual file only logs a
/// warning and continues -- a leftover tmp file is disk clutter, not a
/// correctness problem, so a sweep failure must never block startup.
/// Returns the number of files removed.
pub fn sweep_orphaned_tmp_files(dir: &Path) -> usize {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
        Err(e) => {
            tracing::warn!("Failed to scan {:?} for orphaned tmp files: {}", dir, e);
            return 0;
        }
    };

    let mut removed = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let is_tmp = path.extension().is_some_and(|ext| ext == "tmp");
        if !is_tmp || !path.is_file() {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                tracing::info!(
                    "Removed orphaned tmp file left by a crash mid-write: {:?}",
                    path
                );
                removed += 1;
            }
            Err(e) => {
                tracing::warn!("Failed to remove orphaned tmp file {:?}: {}", path, e);
            }
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn removes_orphaned_manifest_tmp_file() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("hnsw.manifest.tmp"), b"stale").unwrap();

        let removed = sweep_orphaned_tmp_files(dir.path());

        assert_eq!(removed, 1);
        assert!(!dir.path().join("hnsw.manifest.tmp").exists());
    }

    #[test]
    fn removes_orphaned_seg_tmp_file() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("nodes_0_100.seg.tmp"), b"stale").unwrap();

        let removed = sweep_orphaned_tmp_files(dir.path());

        assert_eq!(removed, 1);
        assert!(!dir.path().join("nodes_0_100.seg.tmp").exists());
    }

    #[test]
    fn removes_orphaned_hnsw_deleted_nodes_tmp_file() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("deleted_nodes.bin.tmp"), b"stale").unwrap();

        let removed = sweep_orphaned_tmp_files(dir.path());

        assert_eq!(removed, 1);
        assert!(!dir.path().join("deleted_nodes.bin.tmp").exists());
    }

    #[test]
    fn removes_orphaned_sstable_tmp_file() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("000123.sst.tmp"), b"stale").unwrap();

        let removed = sweep_orphaned_tmp_files(dir.path());

        assert_eq!(removed, 1);
        assert!(!dir.path().join("000123.sst.tmp").exists());
    }

    #[test]
    fn leaves_sealed_files_alone() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("hnsw.manifest"), b"real").unwrap();
        std::fs::write(dir.path().join("000123.sst"), b"real").unwrap();

        let removed = sweep_orphaned_tmp_files(dir.path());

        assert_eq!(removed, 0);
        assert!(dir.path().join("hnsw.manifest").exists());
        assert!(dir.path().join("000123.sst").exists());
    }

    #[test]
    fn missing_directory_is_not_an_error() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");

        let removed = sweep_orphaned_tmp_files(&missing);

        assert_eq!(removed, 0);
    }

    #[test]
    fn ignores_subdirectories_named_like_tmp_files() {
        let dir = tempdir().unwrap();
        std::fs::create_dir(dir.path().join("nested.tmp")).unwrap();

        let removed = sweep_orphaned_tmp_files(dir.path());

        assert_eq!(removed, 0);
        assert!(dir.path().join("nested.tmp").is_dir());
    }
}
