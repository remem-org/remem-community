//! On-disk format version manifest.
//!
//! `<data_dir>/FORMAT` records the version of every on-disk subsystem. It is
//! read before any artifact is opened, so "this directory is newer than I am"
//! can be distinguished from "this file is damaged" -- two conditions that
//! need opposite responses and are otherwise indistinguishable.
//!
//! This module owns version *comparison* only. Deciding which migration steps
//! close a gap belongs to `migrations`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

use super::durable_rename::durable_rename;
use crate::engine::error::{Result, StorageError};

/// Filename of the manifest inside the data directory.
pub const FORMAT_FILE: &str = "FORMAT";

/// Highest version of each on-disk subsystem this binary understands.
///
/// A subsystem name is a *format concern*, not a file: `payload` is the
/// `StoredMemory` JSON living inside SSTable values, so advancing it rewrites
/// SSTables even though the SSTable container format is unchanged.
pub const SUPPORTED: &[(&str, u32)] = &[
    ("wal", 1),
    ("sstable", 1),
    ("payload", 1),
    ("index.hnsw", 1),
    ("index.graph", 1),
    ("index.timeseries", 2),
    ("index.tags", 3),
    ("attr", 2),
    ("index.attr", 2),
    // REM-76 partition-aware physical layout for primary records.
    ("partition.layout", 1),
];

/// The partition legacy (pre-REM-76) data was assigned to when this directory
/// was migrated.
///
/// Unlike every other setting in `EngineConfig`, `default_partition` is baked
/// into the bytes on disk: it becomes part of every primary record key, every
/// index entry, and the HNSW directory layout. Changing it afterwards does not
/// re-target anything — it just makes the engine look for records under a
/// prefix nothing was ever written to, and the whole corpus reads as empty
/// with no error anywhere. Recording the value here turns that silent,
/// total, and easily-mistaken-for-data-loss failure into a refusal at
/// startup that names both partitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionMarker {
    pub tenant: String,
    pub partition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FormatManifest {
    /// Crate version that last wrote this file. Diagnostic only.
    pub written_by: String,
    pub written_at_ms: u64,
    pub subsystems: BTreeMap<String, u32>,
    /// Absent in manifests written before this field existed, and in
    /// directories that have not been bound to a partition yet.
    #[serde(default)]
    pub partition: Option<PartitionMarker>,
}

impl FormatManifest {
    /// A manifest with no subsystems recorded -- every version reads as 0.
    ///
    /// This is what an absent `FORMAT` file means, which collapses "brand new
    /// directory" and "directory written before this ticket" into one path:
    /// both are simply behind, and the ordinary migration logic handles them.
    pub fn zero() -> Self {
        Self {
            written_by: env!("CARGO_PKG_VERSION").to_string(),
            written_at_ms: now_ms(),
            subsystems: BTreeMap::new(),
            partition: None,
        }
    }

    /// A manifest naming every subsystem at the version this binary writes.
    #[allow(dead_code)] // only reached from tests -- production always starts from `read`
    pub fn current() -> Self {
        let mut m = Self::zero();
        for (name, version) in SUPPORTED {
            m.set(name, *version);
        }
        m
    }

    /// Version of `name`, or 0 when the manifest does not mention it.
    pub fn version_of(&self, name: &str) -> u32 {
        self.subsystems.get(name).copied().unwrap_or(0)
    }

    pub fn set(&mut self, name: &str, version: u32) {
        self.subsystems.insert(name.to_string(), version);
    }

    /// Read `<data_dir>/FORMAT`, or an all-zero manifest when it is absent.
    ///
    /// Unparseable JSON is an error: a directory whose manifest cannot be read
    /// must not be treated as unversioned, because that would silently invite
    /// the adoption migration to run over an already-migrated directory.
    pub fn read(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(FORMAT_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::zero()),
            Err(e) => return Err(e.into()),
        };
        serde_json::from_slice(&bytes)
            .map_err(|e| StorageError::invalid_format(&path, format!("unreadable manifest: {e}")))
    }

    /// Write the manifest atomically, refreshing `written_by`/`written_at_ms`.
    pub fn write_atomic(&self, data_dir: &Path) -> Result<()> {
        use std::io::Write;

        let mut out = self.clone();
        out.written_by = env!("CARGO_PKG_VERSION").to_string();
        out.written_at_ms = now_ms();

        let dest = data_dir.join(FORMAT_FILE);
        let tmp = data_dir.join(format!("{FORMAT_FILE}.tmp"));

        let json = serde_json::to_vec_pretty(&out)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&json)?;
            f.write_all(b"\n")?;
            f.sync_all()?;
        }
        durable_rename(&tmp, &dest)?;
        Ok(())
    }

    /// Refuse a directory this binary cannot safely open.
    ///
    /// A subsystem *below* supported is fine -- that is what migrations are
    /// for. A subsystem *above* supported, or one this binary has never heard
    /// of, means files exist that cannot be interpreted.
    pub fn check(&self) -> Result<()> {
        for (name, found) in &self.subsystems {
            let Some((_, supported)) = SUPPORTED.iter().find(|(n, _)| n == name) else {
                return Err(StorageError::invalid_format(
                    Path::new(FORMAT_FILE),
                    format!(
                        "unknown on-disk subsystem {name:?} (version {found}): this data \
                         directory was written by a newer remem. Upgrade the server."
                    ),
                ));
            };
            if found > supported {
                return Err(StorageError::invalid_format(
                    Path::new(FORMAT_FILE),
                    format!(
                        "subsystem {name:?} is version {found}, this binary supports up to \
                         {supported}: this data directory was written by a newer remem. \
                         Upgrade the server. Downgrade is never automatic."
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// Bind this data directory to the configured default partition, or refuse if
/// it is already bound to a different one.
///
/// Called after migrations have run, so the value recorded is the one legacy
/// data was actually assigned to. See [`PartitionMarker`] for why a mismatch
/// has to be a hard error rather than a warning.
///
/// `had_data` distinguishes a brand-new directory (nothing to get wrong, bind
/// silently) from one whose records predate this check (bind to whatever is
/// configured now, but say so, because that value is a guess).
pub(super) fn bind_default_partition(
    data_dir: &Path,
    manifest: &mut FormatManifest,
    tenant: &str,
    partition: &str,
    had_data: bool,
) -> Result<()> {
    match &manifest.partition {
        Some(bound) if bound.tenant == tenant && bound.partition == partition => Ok(()),
        Some(bound) => Err(StorageError::invalid_format(
            Path::new(FORMAT_FILE),
            format!(
                "this data directory stores its records under partition \
                 {:?} (tenant {:?}), but the server is configured for partition {partition:?} \
                 (tenant {tenant:?}). The partition is part of every record key on disk, so \
                 opening with a different one would read as an empty corpus rather than \
                 migrate anything. Set storage.default_partition (or \
                 REMEM_DEFAULT_PARTITION) back to {:?}, or point the server at a different \
                 data directory.",
                bound.partition, bound.tenant, bound.partition
            ),
        )),
        None => {
            if had_data {
                tracing::warn!(
                    "Binding existing data directory to partition {:?} (tenant {:?}); \
                     this directory predates the partition marker, so the value is taken \
                     from configuration and not verified against the records on disk",
                    partition,
                    tenant
                );
            }
            manifest.partition = Some(PartitionMarker {
                tenant: tenant.to_string(),
                partition: partition.to_string(),
            });
            manifest.write_atomic(data_dir)
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn absent_file_reads_as_all_zeros() {
        let dir = tempdir().unwrap();
        let m = FormatManifest::read(dir.path()).unwrap();
        assert_eq!(m.version_of("wal"), 0);
        assert_eq!(m.version_of("sstable"), 0);
        assert_eq!(m.version_of("nonexistent"), 0);
    }

    #[test]
    fn current_names_every_supported_subsystem() {
        let m = FormatManifest::current();
        for (name, version) in SUPPORTED {
            assert_eq!(m.version_of(name), *version, "subsystem {name}");
        }
    }

    #[test]
    fn current_records_tag_index_v3() {
        assert_eq!(FormatManifest::current().version_of("index.tags"), 3);
    }

    #[test]
    fn current_records_partition_layout_v1() {
        assert_eq!(FormatManifest::current().version_of("partition.layout"), 1);
    }

    #[test]
    fn write_then_read_round_trips() {
        let dir = tempdir().unwrap();
        let mut written = FormatManifest::current();
        written.set("wal", 3);
        written.write_atomic(dir.path()).unwrap();

        let read = FormatManifest::read(dir.path()).unwrap();
        assert_eq!(read.version_of("wal"), 3);
        assert_eq!(read.written_by, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn check_accepts_a_current_manifest() {
        FormatManifest::current().check().unwrap();
    }

    #[test]
    fn check_accepts_a_manifest_that_is_behind() {
        let mut m = FormatManifest::current();
        m.set("wal", 0);
        m.check().unwrap();
    }

    #[test]
    fn check_rejects_a_subsystem_newer_than_supported() {
        let mut m = FormatManifest::current();
        m.set("index.hnsw", 99);
        let err = m.check().unwrap_err().to_string();
        assert!(
            err.contains("index.hnsw"),
            "error should name the subsystem: {err}"
        );
        assert!(
            err.contains("99"),
            "error should report the found version: {err}"
        );
    }

    #[test]
    fn check_rejects_a_newer_tag_index_version() {
        let mut m = FormatManifest::current();
        m.set("index.tags", 4);
        let err = m.check().unwrap_err().to_string();
        assert!(err.contains("index.tags"), "{err}");
        assert!(err.contains("4"), "{err}");
    }

    #[test]
    fn check_rejects_a_newer_partition_layout_version() {
        let mut m = FormatManifest::current();
        m.set("partition.layout", 2);
        let err = m.check().unwrap_err().to_string();
        assert!(err.contains("partition.layout"), "{err}");
        assert!(err.contains("2"), "{err}");
    }

    #[test]
    fn check_rejects_an_unknown_subsystem() {
        let mut m = FormatManifest::current();
        m.set("index.attributes", 1);
        let err = m.check().unwrap_err().to_string();
        assert!(
            err.contains("index.attributes"),
            "error should name the key: {err}"
        );
    }

    #[test]
    fn corrupt_json_is_an_error_not_a_silent_reset() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(FORMAT_FILE), b"{ not json").unwrap();
        assert!(FormatManifest::read(dir.path()).is_err());
    }

    /// `SUPPORTED["wal"]` and the WAL module's own format-version constant
    /// must move together. Bumping one without the other produces a
    /// confusing error from the wrong layer (see REM-46 review finding 6).
    #[test]
    fn supported_wal_version_matches_the_wal_modules_constant() {
        let (_, supported) = SUPPORTED.iter().find(|(name, _)| *name == "wal").unwrap();
        assert_eq!(
            *supported,
            u32::from(crate::engine::storage::wal::WAL_FORMAT_VERSION),
            "SUPPORTED[\"wal\"] and wal::WAL_FORMAT_VERSION have drifted apart"
        );
    }

    /// Same coupling for `SUPPORTED["sstable"]` and the SSTable format's own
    /// version constant.
    #[test]
    fn supported_sstable_version_matches_the_sstable_modules_constant() {
        let (_, supported) = SUPPORTED
            .iter()
            .find(|(name, _)| *name == "sstable")
            .unwrap();
        assert_eq!(
            *supported,
            crate::engine::storage::sstable::format::VERSION,
            "SUPPORTED[\"sstable\"] and sstable::format::VERSION have drifted apart"
        );
    }
}
