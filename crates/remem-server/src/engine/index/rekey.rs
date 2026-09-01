//! Crash-safe publication of a rebuilt segmented index.
//!
//! `rewrite_keys_in_dir` (REM-99) rebuilds a whole index into a staging
//! directory and then has to swap it for the live one. The obvious
//! implementation — delete the live artifacts, then rename the new ones in —
//! has no recovery point: a crash between the two leaves the directory with
//! neither copy, and because the partition migration only advances `FORMAT`
//! after every step returns, the *rerun* then reads an empty index, finds
//! nothing to rewrite, reports success, and lets `FORMAT` advance over a
//! silently emptied index.
//!
//! So the swap goes through a backup directory and commits on a single
//! observable bit: **the manifest**.
//!
//! ```text
//! 1. move the live manifest into `.rekey-backup/`   <- manifest gone: uncommitted
//! 2. move the live segments into `.rekey-backup/`
//! 3. move the rebuilt segments into place
//! 4. move the rebuilt manifest into place           <- manifest back: committed
//! 5. drop the backup
//! ```
//!
//! The manifest leaves first and arrives last, so at any crash point:
//!
//! - backup exists, `dir` has **no** manifest  -> step 1..3, uncommitted, roll back
//! - backup exists, `dir` **has** a manifest   -> step 4 done, committed, drop backup
//! - no backup                                 -> nothing was in flight
//!
//! [`recover_interrupted_publish`] applies that rule and is called from every
//! `load_from_dir`, so an interrupted publish is repaired by the next open —
//! whether that open is the migration's own rerun or a normal startup.

use std::path::{Path, PathBuf};

use crate::engine::error::Result;
use crate::engine::storage::durable_rename::durable_rename;

/// Directory (inside the index directory) holding the previous generation
/// while a rebuilt index is being swapped in.
const BACKUP_DIR: &str = ".rekey-backup";

/// Names the artifacts one index family owns inside a shared directory.
///
/// The three segmented indexes live side by side in `index/`, so a publish
/// must move only its own files: the graph's rebuild must not carry off the
/// tag manifest.
pub(crate) struct IndexArtifacts {
    /// `"timeseries"`, `"tags"`, `"graph"`.
    pub name: &'static str,
}

impl IndexArtifacts {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }

    fn manifest_file(&self) -> String {
        format!("{}.manifest", self.name)
    }

    /// Whether `file_name` is a segment (or deletion bitset) of this index.
    fn is_segment(&self, file_name: &str) -> bool {
        file_name.starts_with(&format!("{}_", self.name))
            && (file_name.ends_with(".seg") || file_name.ends_with(".del"))
    }

    fn owns(&self, file_name: &str) -> bool {
        file_name == self.manifest_file() || self.is_segment(file_name)
    }

    fn backup_dir(&self, dir: &Path) -> PathBuf {
        dir.join(format!("{}-{}", BACKUP_DIR, self.name))
    }
}

/// Repair a publish that was interrupted partway through.
///
/// Cheap when nothing is in flight: one `exists()` on a path that is absent
/// in the steady state. Safe to call on a directory that has never been
/// rekeyed, and safe to call repeatedly.
pub(crate) fn recover_interrupted_publish(dir: &Path, artifacts: &IndexArtifacts) -> Result<()> {
    let backup = artifacts.backup_dir(dir);
    if !backup.exists() {
        return Ok(());
    }

    if dir.join(artifacts.manifest_file()).exists() {
        // The rebuilt manifest landed, so the swap committed. Everything in
        // the backup is the superseded generation.
        tracing::warn!(
            "Found {:?} left by an interrupted index rekey; the replacement is \
             committed, discarding the superseded generation",
            backup
        );
        std::fs::remove_dir_all(&backup)?;
        std::fs::File::open(dir)?.sync_all()?;
        return Ok(());
    }

    tracing::warn!(
        "Found {:?} left by an interrupted index rekey with no committed manifest; \
         rolling {} back to the pre-rekey generation",
        backup,
        artifacts.name
    );

    // Uncommitted. Clear whatever partial new-generation files arrived, then
    // put the originals back.
    remove_owned_files(dir, artifacts)?;
    for entry in std::fs::read_dir(&backup)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        durable_rename(&entry.path(), &dir.join(entry.file_name()))?;
    }
    std::fs::remove_dir_all(&backup)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Swap the index rebuilt in `staging` for the one live in `dir`.
///
/// `staging` is consumed. See the module docs for the crash-safety argument.
pub(crate) fn publish_rebuilt_index(
    dir: &Path,
    staging: &Path,
    artifacts: &IndexArtifacts,
) -> Result<()> {
    // A backup from an earlier attempt must be resolved, not overwritten:
    // overwriting it would throw away the only remaining copy of whichever
    // generation that attempt was holding.
    recover_interrupted_publish(dir, artifacts)?;

    let backup = artifacts.backup_dir(dir);
    std::fs::create_dir_all(&backup)?;

    // 1. The manifest leaves first: from here until step 4 the directory
    //    reads as "uncommitted" to `recover_interrupted_publish`.
    let manifest = dir.join(artifacts.manifest_file());
    if manifest.exists() {
        durable_rename(&manifest, &backup.join(artifacts.manifest_file()))?;
    }

    // 2. Then the segments it referenced.
    for path in owned_files(dir, artifacts)? {
        let Some(name) = path.file_name() else {
            continue;
        };
        durable_rename(&path, &backup.join(name))?;
    }

    // 3. The rebuilt segments arrive before the manifest that names them.
    let mut staged_manifest = None;
    for entry in std::fs::read_dir(staging)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if entry.file_name().to_string_lossy() == artifacts.manifest_file() {
            staged_manifest = Some(entry.path());
            continue;
        }
        durable_rename(&entry.path(), &dir.join(entry.file_name()))?;
    }

    // 4. Commit.
    if let Some(staged_manifest) = staged_manifest {
        durable_rename(&staged_manifest, &manifest)?;
    }

    // 5. Past the commit point: cleanup only.
    std::fs::remove_dir_all(&backup)?;
    let _ = std::fs::remove_dir_all(staging);
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

fn owned_files(dir: &Path, artifacts: &IndexArtifacts) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if artifacts.owns(&entry.file_name().to_string_lossy()) {
            out.push(entry.path());
        }
    }
    Ok(out)
}

fn remove_owned_files(dir: &Path, artifacts: &IndexArtifacts) -> Result<()> {
    for path in owned_files(dir, artifacts)? {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn artifacts() -> IndexArtifacts {
        IndexArtifacts::new("timeseries")
    }

    /// Build a directory holding one generation of a "timeseries" index.
    fn seed_live(dir: &Path, marker: &[u8]) {
        std::fs::write(dir.join("timeseries.manifest"), marker).unwrap();
        std::fs::write(dir.join("timeseries_0001.seg"), marker).unwrap();
        std::fs::write(dir.join("timeseries_0001.del"), marker).unwrap();
    }

    fn seed_staging(staging: &Path, marker: &[u8]) {
        std::fs::create_dir_all(staging).unwrap();
        std::fs::write(staging.join("timeseries.manifest"), marker).unwrap();
        std::fs::write(staging.join("timeseries_0001.seg"), marker).unwrap();
    }

    #[test]
    fn publish_replaces_the_live_generation_and_cleans_up() {
        let dir = tempdir().unwrap();
        seed_live(dir.path(), b"old");
        let staging = dir.path().join(".rekey.tmp");
        seed_staging(&staging, b"new");

        publish_rebuilt_index(dir.path(), &staging, &artifacts()).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("timeseries.manifest")).unwrap(),
            b"new"
        );
        assert_eq!(
            std::fs::read(dir.path().join("timeseries_0001.seg")).unwrap(),
            b"new"
        );
        assert!(
            !dir.path().join("timeseries_0001.del").exists(),
            "a stale deletion bitset from the old generation must not survive"
        );
        assert!(!staging.exists());
        assert!(!artifacts().backup_dir(dir.path()).exists());
    }

    #[test]
    fn publish_leaves_other_indexes_in_the_same_directory_alone() {
        let dir = tempdir().unwrap();
        seed_live(dir.path(), b"old");
        std::fs::write(dir.path().join("tags.manifest"), b"tags").unwrap();
        std::fs::write(dir.path().join("graph_0001.seg"), b"graph").unwrap();
        let staging = dir.path().join(".rekey.tmp");
        seed_staging(&staging, b"new");

        publish_rebuilt_index(dir.path(), &staging, &artifacts()).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("tags.manifest")).unwrap(),
            b"tags"
        );
        assert_eq!(
            std::fs::read(dir.path().join("graph_0001.seg")).unwrap(),
            b"graph"
        );
    }

    /// The crash this whole module exists for: the old generation is already
    /// in the backup and no new manifest has landed. The next open must put
    /// the old generation back rather than leave an empty index behind.
    #[test]
    fn an_uncommitted_interruption_rolls_back_to_the_previous_generation() {
        let dir = tempdir().unwrap();
        let backup = artifacts().backup_dir(dir.path());
        std::fs::create_dir_all(&backup).unwrap();
        std::fs::write(backup.join("timeseries.manifest"), b"old").unwrap();
        std::fs::write(backup.join("timeseries_0001.seg"), b"old").unwrap();
        // A partially arrived new-generation segment.
        std::fs::write(dir.path().join("timeseries_0001.seg"), b"new").unwrap();

        recover_interrupted_publish(dir.path(), &artifacts()).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("timeseries.manifest")).unwrap(),
            b"old"
        );
        assert_eq!(
            std::fs::read(dir.path().join("timeseries_0001.seg")).unwrap(),
            b"old",
            "the half-published segment must be replaced, not merged with"
        );
        assert!(!backup.exists());
    }

    /// A crash *after* the manifest landed is a completed publish: the backup
    /// holds the superseded generation and must be discarded, not restored.
    #[test]
    fn a_committed_interruption_keeps_the_new_generation() {
        let dir = tempdir().unwrap();
        let backup = artifacts().backup_dir(dir.path());
        std::fs::create_dir_all(&backup).unwrap();
        std::fs::write(backup.join("timeseries.manifest"), b"old").unwrap();
        seed_live(dir.path(), b"new");

        recover_interrupted_publish(dir.path(), &artifacts()).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("timeseries.manifest")).unwrap(),
            b"new"
        );
        assert!(!backup.exists());
    }

    #[test]
    fn recovery_is_a_no_op_without_a_backup_directory() {
        let dir = tempdir().unwrap();
        seed_live(dir.path(), b"live");

        recover_interrupted_publish(dir.path(), &artifacts()).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("timeseries.manifest")).unwrap(),
            b"live"
        );
    }

    /// Publishing on top of an unresolved backup must resolve it first,
    /// otherwise the second attempt overwrites the only copy of whichever
    /// generation the first attempt was holding.
    #[test]
    fn publish_resolves_a_leftover_backup_before_starting() {
        let dir = tempdir().unwrap();
        let backup = artifacts().backup_dir(dir.path());
        std::fs::create_dir_all(&backup).unwrap();
        std::fs::write(backup.join("timeseries.manifest"), b"old").unwrap();
        std::fs::write(backup.join("timeseries_0001.seg"), b"old").unwrap();
        let staging = dir.path().join(".rekey.tmp");
        seed_staging(&staging, b"new");

        publish_rebuilt_index(dir.path(), &staging, &artifacts()).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("timeseries.manifest")).unwrap(),
            b"new"
        );
        assert!(!backup.exists());
    }
}
