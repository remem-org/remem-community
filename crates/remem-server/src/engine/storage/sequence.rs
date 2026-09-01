//! Persisted high-water mark for the store's record-version counter.
//!
//! Record versions have to keep ordering writes across a restart, but the
//! counter lives in memory. This file is what carries it over.
//!
//! It is written at checkpoint — the same moment the WAL is truncated — and
//! that pairing is what makes a durable write per issued version unnecessary:
//! any version issued after the last checkpoint is still in the WAL, so replay
//! observes it and startup seeds above it. A crash therefore cannot leave the
//! counter seeded below a version already written.
//!
//! It is engine state, not part of the record format: nothing here describes
//! how a record is laid out, and losing the file costs a scan, not data.

use std::path::{Path, PathBuf};

use crate::engine::error::Result;

use super::durable_rename::durable_rename;

/// File name inside the data directory.
const SEQUENCE_FILE: &str = "SEQUENCE";

fn path_in(data_dir: &Path) -> PathBuf {
    data_dir.join(SEQUENCE_FILE)
}

/// Read the persisted high-water mark.
///
/// Returns `None` when the directory has never had one (an upgrade), and also
/// when the file is empty or unreadable. All three take the same path on
/// purpose: the recovery for "we do not know the mark" is to scan for it, and
/// scanning is safe to do in any of those cases. Refusing to start would turn
/// a recoverable state into an outage.
pub(super) fn read(data_dir: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(path_in(data_dir)).ok()?;
    match raw.trim().parse::<u64>() {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::warn!(
                file = %path_in(data_dir).display(),
                "record-version high-water mark is unreadable; recovering it by scanning stored versions"
            );
            None
        }
    }
}

/// Write `value` as the high-water mark, atomically.
///
/// `tmp` + `durable_rename`, the same shape the segment manifest uses: a crash
/// leaves either the previous value or the new one, never a torn file. A
/// leftover `.tmp` is swept at startup like any other.
pub(super) fn write(data_dir: &Path, value: u64) -> Result<()> {
    let dest = path_in(data_dir);
    let tmp = dest.with_extension("tmp");
    std::fs::write(&tmp, value.to_string())?;
    durable_rename(&tmp, &dest)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn a_written_mark_reads_back() {
        let dir = tempdir().unwrap();
        write(dir.path(), 4242).unwrap();
        assert_eq!(read(dir.path()), Some(4242));
    }

    #[test]
    fn a_rewritten_mark_replaces_the_previous_one() {
        let dir = tempdir().unwrap();
        write(dir.path(), 7).unwrap();
        write(dir.path(), 9000).unwrap();
        assert_eq!(read(dir.path()), Some(9000));
    }

    /// The three "we do not know" cases have to be indistinguishable to the
    /// caller, because each one is answered the same way: scan for the mark.
    #[test]
    fn an_absent_mark_reads_as_unknown() {
        let dir = tempdir().unwrap();
        assert_eq!(read(dir.path()), None);
    }

    #[test]
    fn an_empty_mark_reads_as_unknown() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(SEQUENCE_FILE), "").unwrap();
        assert_eq!(read(dir.path()), None);
    }

    #[test]
    fn an_unparseable_mark_reads_as_unknown() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(SEQUENCE_FILE), "not-a-number").unwrap();
        assert_eq!(read(dir.path()), None);
    }

    #[test]
    fn writing_leaves_no_tmp_file_behind() {
        let dir = tempdir().unwrap();
        write(dir.path(), 11).unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
            .collect();
        assert!(leftovers.is_empty(), "tmp file left behind: {leftovers:?}");
    }
}
