//! Ordered, crash-safe on-disk format migrations.
//!
//! ## The contract every migration must honour
//!
//! Do the work into temp files, fsync, atomically rename, and only then let
//! the runner advance `FORMAT`. A crash before the `FORMAT` write reruns the
//! step on the next boot, so **every step must be safe to rerun** against
//! originals that are still intact.
//!
//! Execution is a single ordered list, not a compatibility matrix: each step
//! declares which subsystem versions it requires and which it sets. Versions
//! are tracked per subsystem; steps still run in one line.

use std::fs::File;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::engine::EngineConfig;
use super::format::{FormatManifest, SUPPORTED};
use super::partition::{decode_record_key, encode_record_key, PartitionBinding, TenantId};
use super::sstable::{Compression, SSTableReader, SSTableWriter};
use super::wal::{WalRecord, WalRecordType, WAL};
use crate::engine::error::{Result, StorageError};
#[cfg(not(feature = "kuzu"))]
use crate::engine::index::SegmentedCsrGraph;
use crate::engine::index::{BTreeConfig, GraphConfig, InvertedIndexConfig};
use crate::engine::index::{SegmentedBTreeIndex, SegmentedInvertedIndex};

pub struct MigrationContext<'a> {
    pub data_dir: &'a Path,
    pub config: &'a EngineConfig,
}

#[derive(Copy, Clone)]
pub struct Migration {
    /// Stable identifier, used in logs.
    pub name: &'static str,
    /// Subsystem versions that must hold exactly for this step to apply.
    pub requires: &'static [(&'static str, u32)],
    /// Subsystem versions this step sets once it succeeds.
    pub advances: &'static [(&'static str, u32)],
    /// Whether this step rewrites existing files. Triggers a backup.
    pub rewrites_files: bool,
    /// The work. Must be safe to rerun.
    pub run: fn(&MigrationContext<'_>) -> Result<()>,
}

/// The steps this binary ships, in execution order.
static MIGRATIONS: &[Migration] = &[
    ADOPT,
    ATTRS_V1,
    INDEX_TAGS_V2,
    PARTITION_LAYOUT_V1,
    INDEX_TIMESERIES_V2,
    INDEX_TAGS_V3,
    INDEX_ATTR_V2,
    ATTRS_V2,
];

/// Name every subsystem for the first time, and give the WAL a header.
///
/// Every subsystem's v1 is exactly what is already on disk, so this is pure
/// bookkeeping for all of them except the WAL, which gains a 16-byte header.
static ADOPT: Migration = Migration {
    name: "adopt-v1",
    requires: &[
        ("wal", 0),
        ("sstable", 0),
        ("payload", 0),
        ("index.hnsw", 0),
        ("index.graph", 0),
        ("index.timeseries", 0),
        ("index.tags", 0),
    ],
    advances: &[
        ("wal", 1),
        ("sstable", 1),
        ("payload", 1),
        ("index.hnsw", 1),
        ("index.graph", 1),
        ("index.timeseries", 1),
        ("index.tags", 1),
    ],
    rewrites_files: true,
    run: adopt_v1,
};

fn adopt_v1(ctx: &MigrationContext<'_>) -> Result<()> {
    use std::io::Write;

    let data_dir = ctx.data_dir;
    let wal_path = data_dir.join("wal").join("current.wal");
    if !wal_path.exists() {
        return Ok(());
    }

    let bytes = std::fs::read(&wal_path)?;

    // Safe to rerun: a crash between the rename below and the FORMAT write
    // brings us back here with the header already in place.
    if bytes.len() >= 8 && bytes[..8] == super::wal::WAL_MAGIC {
        return Ok(());
    }

    let tmp = wal_path.with_extension("wal.adopt.tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&crate::engine::storage::wal::encode_wal_header())?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    super::durable_rename::durable_rename(&tmp, &wal_path)?;

    tracing::info!("Added a format header to {:?}", wal_path);
    Ok(())
}

/// Filename (at the data-directory root) of the marker recording that
/// existing records still need an attribute sidecar row.
///
/// Lives at the data-directory root rather than under `index/attr/`: this
/// migration runs before any schema is known -- `index/attr/` is created by
/// `open_attrs` (`init.rs`) only when `EngineConfig.attr_schema` is `Some`,
/// which may never be true for a given directory. The marker must exist
/// independent of that.
pub(super) const ATTR_BACKFILL_MARKER: &str = "ATTR_BACKFILL_PENDING";

/// Register the `attr` and `index.attr` subsystems, and record whether
/// existing records are owed a sidecar attribute row.
///
/// See also `attrs_v2`, which owes a backfill for a different reason: not
/// "these records have no row" but "these rows predate a slot".
///
/// This step does no data work: it never opens a `StorageEngine`. It runs
/// from inside `super::migrations::run`, which `StorageEngine::new` calls
/// while it is itself still under construction (`engine.rs`) -- opening a
/// second engine on the same directory here would recurse into this very
/// migration, since FORMAT has not advanced yet on the way in. Projecting a
/// payload into a row also needs the engine's merged read path (memtable +
/// SSTables + WAL replay), which a one-shot step run from a bare `&Path`
/// cannot reproduce without duplicating that logic.
///
/// So the actual backfill happens in `StorageEngine::new`, once the engine
/// is fully open and that read path works normally -- see
/// `backfill_attrs_if_marked` in `engine.rs`. This step only decides
/// whether that backfill is owed and drops [`ATTR_BACKFILL_MARKER`] for it
/// to find.
///
/// `requires` names `payload` alongside `attr`/`index.attr` even though
/// `attrs_v1` never touches a payload: `MIGRATIONS`'s fixed order already
/// puts `ADOPT` (which advances `payload`) first, so this is redundant
/// today, but `requires` exists precisely so that ordering is declared
/// rather than merely implied by list position -- without it, a future
/// reorder or an inserted step could let this run against a pre-`ADOPT`
/// WAL undetected.
/// Re-derive every record's attribute row against the current schema, and
/// take retired records back out of the ordering indexes.
///
/// The schema gained a slot (`next_attention_at`) that background work
/// *selects on*. A record with no entry in that slot's index is not merely
/// undated — it is unreachable by the sweep that would date it, so every
/// sweep would silently stop working for every record written before the
/// upgrade. That is the failure this step exists to prevent, and it is
/// invisible if missed: nothing errors, maintenance just quietly stops.
///
/// Does no data work itself, for exactly the reasons `attrs_v1` documents:
/// it drops the same marker and lets `backfill_attrs_if_marked` do the work
/// once the engine is open and its merged read path works. Re-projecting a
/// row that already exists is a harmless overwrite, so re-running after an
/// interrupted upgrade converges.
static ATTRS_V2: Migration = Migration {
    name: "attrs-v2",
    requires: &[("attr", 1), ("index.attr", 2)],
    advances: &[("attr", 2)],
    rewrites_files: false,
    run: attrs_v2,
};

fn attrs_v2(ctx: &MigrationContext<'_>) -> Result<()> {
    // Any record at all, not just records missing a row: an existing row is
    // as stale as an absent one once the schema gains a slot, so the two
    // cases need the same re-derivation. A directory holding nothing owes
    // nothing.
    if holds_any_records(ctx.data_dir)? {
        std::fs::write(ctx.data_dir.join(ATTR_BACKFILL_MARKER), b"")?;
    }
    Ok(())
}

static ATTRS_V1: Migration = Migration {
    name: "attrs-v1",
    requires: &[("payload", 1), ("attr", 0), ("index.attr", 0)],
    advances: &[("attr", 1), ("index.attr", 1)],
    rewrites_files: false,
    run: attrs_v1,
};

fn attrs_v1(ctx: &MigrationContext<'_>) -> Result<()> {
    let data_dir = ctx.data_dir;
    if holds_any_records(data_dir)? {
        std::fs::write(data_dir.join(ATTR_BACKFILL_MARKER), b"")?;
    }
    Ok(())
}

/// Rewrite segmented tag-index chunks so sealed entries carry a durable
/// key-to-local-doc-id directory for deletion bitsets.
static INDEX_TAGS_V2: Migration = Migration {
    name: "index-tags-v2",
    requires: &[("index.tags", 1)],
    advances: &[("index.tags", 2)],
    rewrites_files: true,
    run: index_tags_v2,
};

fn index_tags_v2(ctx: &MigrationContext<'_>) -> Result<()> {
    crate::engine::index::inverted_segmented::migrate_segments_to_v2(&ctx.data_dir.join("index"))
}

/// Assign legacy primary records to the configured default partition.
static PARTITION_LAYOUT_V1: Migration = Migration {
    name: "partition-layout-v1",
    requires: &[
        ("payload", 1),
        ("wal", 1),
        ("sstable", 1),
        ("partition.layout", 0),
    ],
    advances: &[("partition.layout", 1)],
    rewrites_files: true,
    run: partition_layout_v1,
};

static INDEX_TIMESERIES_V2: Migration = Migration {
    name: "index-timeseries-v2",
    requires: &[("index.timeseries", 1), ("partition.layout", 1)],
    advances: &[("index.timeseries", 2)],
    rewrites_files: true,
    run: index_timeseries_v2,
};

static INDEX_TAGS_V3: Migration = Migration {
    name: "index-tags-v3",
    requires: &[("index.tags", 2), ("partition.layout", 1)],
    advances: &[("index.tags", 3)],
    rewrites_files: true,
    run: index_tags_v3,
};

static INDEX_ATTR_V2: Migration = Migration {
    name: "index-attr-v2",
    requires: &[("attr", 1), ("index.attr", 1), ("partition.layout", 1)],
    advances: &[("index.attr", 2)],
    rewrites_files: true,
    run: index_attr_v2,
};

fn index_timeseries_v2(ctx: &MigrationContext<'_>) -> Result<()> {
    let index_root = ctx.data_dir.join("index");
    let root = index_root.join("timeseries");
    promote_legacy_files(&index_root, &root, "timeseries")?;
    let target = root
        .join(TenantId::default_legacy().as_str())
        .join(ctx.config.default_partition.as_str());
    copy_family_generation(&root, &target, &[])
}

fn index_tags_v3(ctx: &MigrationContext<'_>) -> Result<()> {
    let index_root = ctx.data_dir.join("index");
    let root = index_root.join("tags");
    promote_legacy_files(&index_root, &root, "tags")?;
    let target = root
        .join(TenantId::default_legacy().as_str())
        .join(ctx.config.default_partition.as_str());
    copy_family_generation(&root, &target, &[])
}

fn index_attr_v2(ctx: &MigrationContext<'_>) -> Result<()> {
    let root = ctx.data_dir.join("index").join("attr");
    let target = root
        .join(TenantId::default_legacy().as_str())
        .join(ctx.config.default_partition.as_str());
    copy_family_generation(&root, &target, &["ord."])
}

/// Copy the complete legacy generation into a partition child. The target is
/// staged beside its final path and renamed as one directory, so a restart
/// sees either the old generation or the complete child generation. Existing
/// child generations are treated as an idempotent successful rerun.
fn copy_family_generation(root: &Path, target: &Path, directory_prefixes: &[&str]) -> Result<()> {
    if target.join(".generation-complete").exists() {
        return Ok(());
    }
    if !root.exists() {
        return Ok(());
    }
    let staging = root.join(".partition-generation.tmp");
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    std::fs::create_dir_all(&staging)?;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || name == target.file_name().unwrap_or_default().to_string_lossy()
        {
            continue;
        }
        let path = entry.path();
        if entry.file_type()?.is_file() {
            std::fs::copy(&path, staging.join(entry.file_name()))?;
        } else if directory_prefixes
            .iter()
            .any(|prefix| name.starts_with(prefix))
        {
            copy_dir_recursive(&path, &staging.join(entry.file_name()))?;
        }
    }
    std::fs::write(
        staging.join(".generation-complete"),
        b"partition-generation-v1",
    )?;
    if target.exists() {
        std::fs::remove_dir_all(target)?;
    }
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::rename(&staging, target)?;
    Ok(())
}

fn promote_legacy_files(index_root: &Path, family_root: &Path, stem: &str) -> Result<()> {
    if family_root.join(format!("{stem}.manifest")).exists()
        || family_root.join(format!("{stem}.idx")).exists()
    {
        return Ok(());
    }
    std::fs::create_dir_all(family_root)?;
    for entry in std::fs::read_dir(index_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == format!("{stem}.manifest")
            || name == format!("{stem}.idx")
            || name.starts_with(&format!("{stem}_"))
        {
            std::fs::copy(entry.path(), family_root.join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn partition_layout_v1(ctx: &MigrationContext<'_>) -> Result<()> {
    let binding = PartitionBinding::new(
        TenantId::default_legacy(),
        ctx.config.default_partition.clone(),
    );
    rewrite_primary_wal_keys(ctx.data_dir, &binding)?;
    rewrite_primary_sstable_keys(ctx.data_dir, &binding, ctx.config.compaction.compression)?;
    rewrite_secondary_index_keys(ctx.data_dir, &binding, ctx.config)?;
    Ok(())
}

fn rewrite_primary_wal_keys(data_dir: &Path, binding: &PartitionBinding) -> Result<()> {
    let wal_path = data_dir.join("wal").join("current.wal");
    if !wal_path.exists() {
        return Ok(());
    }

    let wal = WAL::open(&wal_path)?;
    let records = wal.iter()?.collect::<Result<Vec<_>>>()?;
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(records.len());
    for record in records {
        let (record, key_changed) = partition_wal_record_key(record, binding)?;
        changed |= key_changed;
        rewritten.push(record);
    }

    if !changed {
        return Ok(());
    }

    let tmp = wal_path.with_extension("wal.partition.tmp");
    {
        let mut wal = WAL::create(&tmp)?;
        wal.append_batch(&rewritten)?;
        wal.sync()?;
    }
    super::durable_rename::durable_rename(&tmp, &wal_path)?;
    Ok(())
}

fn partition_wal_record_key(
    mut record: WalRecord,
    binding: &PartitionBinding,
) -> Result<(WalRecord, bool)> {
    match record.record_type {
        WalRecordType::Insert
        | WalRecordType::InsertWithEmbedding
        | WalRecordType::Delete
        | WalRecordType::SetTimestamp
        | WalRecordType::AddTags
        | WalRecordType::SetTags
        | WalRecordType::RemoveTimestamp
        | WalRecordType::RemoveTags
        | WalRecordType::RemoveVector
        | WalRecordType::RemoveAttrIndexEntry
        | WalRecordType::PutAttrs => {
            // `partition_legacy_record_or_attr_key`, not the plain primary-key
            // form: `delete_with_attrs` logs a second `Delete` for the sidecar
            // key `attr:memory:<id>`. Rewriting only the payload key would
            // replay the payload tombstone at the new partitioned key and the
            // sidecar tombstone at the old one, leaving an orphaned attribute
            // row for a record that no longer exists. The SSTable rewrite has
            // always handled both forms; this keeps the two paths in step.
            let Some(key) = partition_legacy_record_or_attr_key(&record.key, binding)? else {
                return Ok((record, false));
            };
            record.key = key;
            Ok((record, true))
        }
        WalRecordType::AddEdge | WalRecordType::RemoveEdge => {
            let mut changed = false;
            if let Some(source) = record.edge_source.take() {
                if let Some(key) = partition_legacy_primary_key(&source, binding)? {
                    record.edge_source = Some(key);
                    changed = true;
                } else {
                    record.edge_source = Some(source);
                }
            }
            if let Some(target) = record.edge_target.take() {
                if let Some(key) = partition_legacy_primary_key(&target, binding)? {
                    record.edge_target = Some(key);
                    changed = true;
                } else {
                    record.edge_target = Some(target);
                }
            }
            Ok((record, changed))
        }
    }
}

fn rewrite_primary_sstable_keys(
    data_dir: &Path,
    binding: &PartitionBinding,
    compression: Compression,
) -> Result<()> {
    for (level, path) in sstable_paths(&data_dir.join("sstables"))? {
        let reader = SSTableReader::open_with_cache(&path, None, level)?;
        let mut changed = false;
        let mut records = Vec::new();
        for record in reader.iter() {
            let record = record?;
            let key = match partition_legacy_record_or_attr_key(&record.key, binding)? {
                Some(key) => {
                    changed = true;
                    key
                }
                None => record.key.clone(),
            };
            records.push((key, record.value.clone(), record.timestamp));
        }

        if !changed {
            continue;
        }

        records.sort_by(|left, right| left.0.as_ref().cmp(right.0.as_ref()));

        // Temp file + atomic rename, per this module's contract. Writing over
        // `path` in place would leave a truncated SSTable if the process died
        // mid-write, and the rerun (FORMAT has not advanced) would then read
        // the corpse rather than the original.
        drop(reader);
        let tmp = path.with_extension("sst.partition.tmp");
        let mut writer = SSTableWriter::with_level(&tmp, compression, level)?;
        for (key, value, timestamp) in records {
            writer.add(key, value, timestamp)?;
        }
        writer.finish()?;
        super::durable_rename::durable_rename(&tmp, &path)?;
    }
    Ok(())
}

fn rewrite_secondary_index_keys(
    data_dir: &Path,
    binding: &PartitionBinding,
    config: &EngineConfig,
) -> Result<()> {
    let index_dir = data_dir.join("index");
    if !index_dir.exists() {
        return Ok(());
    }

    // Deliberately *not* gated on `config.<index>.enabled`. The format
    // version advances either way, so skipping a disabled index would leave
    // its entries unpartitioned forever — and the operator who enables it
    // later gets an index full of `memory:…` keys whose payloads were
    // renamed out from under them, with no migration left to fix it. An
    // index directory with no manifest and no legacy file rekeys to a no-op,
    // so running unconditionally is cheap.
    let time_config = BTreeConfig::default();
    SegmentedBTreeIndex::convert_legacy_file_in_dir(time_config.clone(), &index_dir)?;
    SegmentedBTreeIndex::rewrite_keys_in_dir(time_config, &index_dir, |key| {
        Ok(partition_legacy_primary_key(key, binding)?.unwrap_or_else(|| key.clone()))
    })?;

    let tag_config = InvertedIndexConfig::default()
        .lowercase(config.tag_index.lowercase)
        .min_token_length(config.tag_index.min_token_length);
    SegmentedInvertedIndex::convert_legacy_file_in_dir(tag_config.clone(), &index_dir)?;
    SegmentedInvertedIndex::rewrite_keys_in_dir(tag_config, &index_dir, |key| {
        Ok(partition_legacy_primary_key(key, binding)?.unwrap_or_else(|| key.clone()))
    })?;

    #[cfg(not(feature = "kuzu"))]
    {
        let graph_config = GraphConfig::default().directed(config.graph.directed);
        SegmentedCsrGraph::convert_legacy_file_in_dir(graph_config.clone(), &index_dir)?;
        SegmentedCsrGraph::rewrite_keys_in_dir(graph_config, &index_dir, |key| {
            Ok(partition_legacy_primary_key(key, binding)?.unwrap_or_else(|| key.clone()))
        })?;
    }

    let attr_dir = index_dir.join("attr");
    if attr_dir.exists() {
        for entry in std::fs::read_dir(&attr_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            if !name.to_string_lossy().starts_with("ord.") {
                continue;
            }
            SegmentedBTreeIndex::rewrite_keys_in_dir(
                BTreeConfig::default(),
                &entry.path(),
                |key| Ok(partition_legacy_primary_key(key, binding)?.unwrap_or_else(|| key.clone())),
            )?;
        }
    }

    Ok(())
}

fn partition_legacy_primary_key(key: &Bytes, binding: &PartitionBinding) -> Result<Option<Bytes>> {
    if decode_record_key(key)
        .map_err(|err| StorageError::InvalidArgument(err.to_string()))?
        .is_some()
    {
        return Ok(None);
    }
    if !key.starts_with(b"memory:") {
        return Ok(None);
    }
    encode_record_key(binding, key).map(Some).map_err(|err| {
        StorageError::InvalidArgument(format!("failed to partition legacy primary key: {err}"))
    })
}

fn partition_legacy_record_or_attr_key(
    key: &Bytes,
    binding: &PartitionBinding,
) -> Result<Option<Bytes>> {
    if let Some(key) = partition_legacy_primary_key(key, binding)? {
        return Ok(Some(key));
    }

    let Some(record_key) = key.strip_prefix(crate::engine::attr::ATTR_PREFIX) else {
        return Ok(None);
    };
    let record_key = Bytes::copy_from_slice(record_key);
    let Some(partitioned_record_key) = partition_legacy_primary_key(&record_key, binding)? else {
        return Ok(None);
    };
    Ok(Some(crate::engine::attr::attr_key(&partitioned_record_key)))
}

fn sstable_paths(root: &Path) -> Result<Vec<(usize, PathBuf)>> {
    let mut out = Vec::new();
    let Ok(levels) = std::fs::read_dir(root) else {
        return Ok(out);
    };

    for level in levels {
        let level = level?;
        if !level.file_type()?.is_dir() {
            continue;
        }
        let level_no = level
            .file_name()
            .to_string_lossy()
            .strip_prefix("level")
            .and_then(|n| n.parse::<usize>().ok())
            .unwrap_or(0);
        for entry in std::fs::read_dir(level.path())? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "sst") {
                out.push((level_no, path));
            }
        }
    }
    out.sort_by(|left, right| left.1.cmp(&right.1));
    Ok(out)
}

/// Whether `data_dir` already holds records written before this ticket.
///
/// Decided by content, not by directory existence: `StorageEngine::new`
/// creates `sstables/` and `wal/` unconditionally before migrations run
/// (see `engine.rs`), so a directory-existence check would flag every
/// brand-new install as owing a backfill. A freshly created `wal/current.wal`
/// carries only its header (`WAL::create`), so a WAL longer than that header
/// means unreplayed records are waiting; any file under `sstables/` means
/// checkpointed records are waiting. Safe to rerun: it only reads, and a
/// second run of `attrs_v1` recomputes the same answer and overwrites the
/// same marker.
fn holds_any_records(data_dir: &Path) -> Result<bool> {
    if dir_contains_a_file(&data_dir.join("sstables")) {
        return Ok(true);
    }
    match std::fs::metadata(data_dir.join("wal").join("current.wal")) {
        Ok(meta) => Ok(meta.len() > super::wal::WAL_HEADER_LEN),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Bring `data_dir` up to the versions this binary writes.
pub(super) fn run(config: &EngineConfig, manifest: &mut FormatManifest) -> Result<()> {
    let ctx = MigrationContext {
        data_dir: &config.data_dir,
        config,
    };
    run_list(&ctx, manifest, MIGRATIONS)
}

fn run_list(
    ctx: &MigrationContext<'_>,
    manifest: &mut FormatManifest,
    list: &[Migration],
) -> Result<()> {
    let mut backed_up = false;

    for step in list {
        if !is_pending(manifest, step) {
            continue;
        }

        if step.rewrites_files && !backed_up && data_dir_has_data(ctx.data_dir) {
            if let Some(dest) = back_up(ctx.data_dir)? {
                tracing::info!(
                    "Backed up {:?} to {:?} before migration {}",
                    ctx.data_dir,
                    dest,
                    step.name
                );
            }
            backed_up = true;
        }

        tracing::info!("Running on-disk format migration {}", step.name);
        (step.run)(ctx)?;

        for (name, version) in step.advances {
            manifest.set(name, *version);
        }
        manifest.write_atomic(ctx.data_dir)?;
    }

    Ok(())
}

fn is_pending(manifest: &FormatManifest, step: &Migration) -> bool {
    step.requires
        .iter()
        .all(|(name, want)| manifest.version_of(name) == *want)
        && step
            .advances
            .iter()
            .any(|(name, want)| manifest.version_of(name) < *want)
}

/// Fail loudly when the registry cannot close a gap, rather than opening a
/// directory whose format nothing has actually migrated.
pub(super) fn verify_current(manifest: &FormatManifest) -> Result<()> {
    for (name, want) in SUPPORTED {
        let have = manifest.version_of(name);
        if have != *want {
            return Err(StorageError::invalid_format(
                Path::new(super::format::FORMAT_FILE),
                format!(
                    "no migration path for subsystem {name:?}: on disk it is version {have}, \
                     this binary writes version {want}"
                ),
            ));
        }
    }
    Ok(())
}

/// Name of the directory (inside `data_dir`) that holds pre-migration backups.
const BACKUPS_DIR: &str = ".backups";

/// Copy the data directory to `<data_dir>/.backups/pre-<unix-seconds>/`.
///
/// The backup lives inside `data_dir` itself so it lands on the same volume
/// as the data it protects -- a sibling of `data_dir` is not guaranteed to be
/// on a durable, persisted mount (e.g. a container's writable overlay, wiped
/// by `docker compose down`, when `data_dir` is a bind mount).
///
/// Returns `Ok(None)` when the env var opts out; `Ok(Some(dest))` when a copy was made.
fn back_up(data_dir: &Path) -> Result<Option<PathBuf>> {
    if std::env::var("REMEM_SKIP_MIGRATION_BACKUP").as_deref() == Ok("1") {
        tracing::warn!(
            "REMEM_SKIP_MIGRATION_BACKUP=1 is set; migrating {:?} without a backup",
            data_dir
        );
        return Ok(None);
    }

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let dest = data_dir.join(BACKUPS_DIR).join(format!("pre-{stamp}"));

    copy_dir_recursive(data_dir, &dest)?;
    Ok(Some(dest))
}

/// Recursively copy `src` to `dst`, fsyncing every copied file and every
/// directory before returning.
///
/// The backup exists to survive a crash mid-migration, so it must not exist
/// only in page cache while the migration goes on to mutate the originals --
/// a power loss between the copy and the migration's own fsyncs must not be
/// able to leave both a truncated backup and a half-migrated original.
///
/// Skips any entry named `.backups`: the backup destination lives inside
/// `data_dir` (see [`back_up`]), so without this the walk would recurse into
/// its own output.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        if entry.file_name() == std::ffi::OsStr::new(BACKUPS_DIR) {
            continue;
        }
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
            File::open(&to)?.sync_all()?;
        }
    }
    File::open(dst)?.sync_all()?;
    Ok(())
}

/// Whether there is anything worth backing up.
///
/// Deliberately narrow: only `wal/`, `sstables/`, and `index/` are checked,
/// so a leftover `.backups/` from a previous migration is never mistaken for
/// data needing its own backup.
pub(super) fn data_dir_has_data(data_dir: &Path) -> bool {
    if data_dir.join("wal").join("current.wal").exists() {
        return true;
    }
    ["sstables", "index"]
        .iter()
        .any(|sub| dir_contains_a_file(&data_dir.join(sub)))
}

fn dir_contains_a_file(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if dir_contains_a_file(&path) {
                return true;
            }
        } else {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Marker file each test migration drops, so a run is observable.
    fn touch(dir: &Path, name: &str) -> Result<()> {
        std::fs::write(dir.join(name), b"ran")?;
        Ok(())
    }

    fn seed_data(dir: &Path) {
        std::fs::create_dir_all(dir.join("sstables")).unwrap();
        std::fs::write(dir.join("sstables").join("000001.sst"), b"payload").unwrap();
    }

    static NO_OP: Migration = Migration {
        name: "test-no-op",
        requires: &[("wal", 0)],
        advances: &[("wal", 1)],
        rewrites_files: false,
        run: |ctx| touch(ctx.data_dir, "no-op.marker"),
    };

    static REWRITER: Migration = Migration {
        name: "test-rewriter",
        requires: &[("wal", 1)],
        advances: &[("wal", 2)],
        rewrites_files: true,
        run: |ctx| touch(ctx.data_dir, "rewriter.marker"),
    };

    static REWRITER_2: Migration = Migration {
        name: "test-rewriter-2",
        requires: &[("wal", 2)],
        advances: &[("wal", 3)],
        rewrites_files: true,
        run: |ctx| touch(ctx.data_dir, "rewriter-2.marker"),
    };

    static FAILING: Migration = Migration {
        name: "test-failing",
        requires: &[("wal", 0)],
        advances: &[("wal", 1)],
        rewrites_files: false,
        run: |_| Err(StorageError::Compaction("boom".into())),
    };

    static CONFIG_READER: Migration = Migration {
        name: "test-config-reader",
        requires: &[("wal", 0)],
        advances: &[("wal", 1)],
        rewrites_files: false,
        run: |ctx| {
            std::fs::write(
                ctx.data_dir.join("default-partition.marker"),
                ctx.config.default_partition.as_str(),
            )?;
            Ok(())
        },
    };

    fn test_config(data_dir: &Path) -> EngineConfig {
        EngineConfig {
            data_dir: data_dir.to_path_buf(),
            ..EngineConfig::default()
        }
    }

    fn run_for_dir(data_dir: &Path, manifest: &mut FormatManifest) -> Result<()> {
        run(&test_config(data_dir), manifest)
    }

    fn run_list_for_dir(
        data_dir: &Path,
        manifest: &mut FormatManifest,
        list: &[Migration],
    ) -> Result<()> {
        let config = test_config(data_dir);
        let ctx = MigrationContext {
            data_dir,
            config: &config,
        };
        run_list(&ctx, manifest, list)
    }

    fn run_step_for_dir(step: &Migration, data_dir: &Path) -> Result<()> {
        let config = test_config(data_dir);
        let ctx = MigrationContext {
            data_dir,
            config: &config,
        };
        (step.run)(&ctx)
    }

    fn finance_binding() -> PartitionBinding {
        PartitionBinding::new(
            TenantId::default_legacy(),
            crate::engine::storage::partition::PartitionId::new("finance").unwrap(),
        )
    }

    fn assert_record_key_partitioned(key: &Bytes) {
        let decoded = decode_record_key(key)
            .unwrap()
            .expect("record key should be partitioned");
        assert_eq!(
            decoded.binding().tenant().as_str(),
            TenantId::default_legacy().as_str()
        );
        assert_eq!(decoded.binding().partition().as_str(), "finance");
    }

    #[test]
    fn migration_context_exposes_engine_config_to_steps() {
        let dir = tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.default_partition =
            crate::engine::storage::partition::PartitionId::new("finance").unwrap();
        let ctx = MigrationContext {
            data_dir: dir.path(),
            config: &config,
        };
        let mut m = FormatManifest::zero();

        run_list(&ctx, &mut m, std::slice::from_ref(&CONFIG_READER)).unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.path().join("default-partition.marker")).unwrap(),
            "finance"
        );
    }

    #[test]
    fn partition_layout_rewrites_legacy_wal_secondary_index_records() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_path = wal_dir.join("current.wal");
        {
            let mut wal = WAL::create(&wal_path).unwrap();
            wal.append(&WalRecord::set_timestamp(
                Bytes::from_static(b"memory:time"),
                100,
                1,
            ))
            .unwrap();
            wal.append(&WalRecord::add_tags(
                Bytes::from_static(b"memory:tags"),
                vec!["rust".to_string()],
                2,
            ))
            .unwrap();
            wal.append(&WalRecord::put_attrs(
                Bytes::from_static(b"memory:attrs"),
                Bytes::from_static(b"encoded-row"),
                3,
            ))
            .unwrap();
            wal.append(&WalRecord::add_edge(
                Bytes::from_static(b"memory:source"),
                Bytes::from_static(b"memory:target"),
                Some("related_to".to_string()),
                Some(0.8),
                4,
            ))
            .unwrap();
            wal.sync().unwrap();
        }

        rewrite_primary_wal_keys(dir.path(), &finance_binding()).unwrap();

        let wal = WAL::open(&wal_path).unwrap();
        let records = wal.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(records.len(), 4);
        for record in &records[..3] {
            assert_record_key_partitioned(&record.key);
        }
        let edge = &records[3];
        assert_record_key_partitioned(edge.edge_source.as_ref().unwrap());
        assert_record_key_partitioned(edge.edge_target.as_ref().unwrap());
    }

    #[test]
    fn partition_layout_wal_rewrite_is_idempotent_after_interruption_resume() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_path = wal_dir.join("current.wal");
        {
            let mut wal = WAL::create(&wal_path).unwrap();
            wal.append(&WalRecord::set_tags(
                Bytes::from_static(b"memory:tags"),
                vec!["rust".to_string(), "storage".to_string()],
                1,
            ))
            .unwrap();
            wal.append(&WalRecord::add_edge(
                Bytes::from_static(b"memory:source"),
                Bytes::from_static(b"memory:target"),
                Some("related_to".to_string()),
                Some(0.8),
                2,
            ))
            .unwrap();
            wal.sync().unwrap();
        }

        rewrite_primary_wal_keys(dir.path(), &finance_binding()).unwrap();
        let after_first = std::fs::read(&wal_path).unwrap();
        rewrite_primary_wal_keys(dir.path(), &finance_binding()).unwrap();

        assert_eq!(
            std::fs::read(&wal_path).unwrap(),
            after_first,
            "rerunning after a crash before FORMAT advances must not rewrite already partitioned records"
        );
    }

    #[test]
    fn runs_a_pending_step_and_advances_the_manifest() {
        let dir = tempdir().unwrap();
        let mut m = FormatManifest::zero();

        run_list_for_dir(dir.path(), &mut m, std::slice::from_ref(&NO_OP)).unwrap();

        assert!(dir.path().join("no-op.marker").exists());
        assert_eq!(m.version_of("wal"), 1);
        assert_eq!(
            FormatManifest::read(dir.path()).unwrap().version_of("wal"),
            1,
            "FORMAT must be persisted, not only advanced in memory"
        );
    }

    #[test]
    fn skips_a_step_that_has_already_run() {
        let dir = tempdir().unwrap();
        let mut m = FormatManifest::zero();
        m.set("wal", 1);

        run_list_for_dir(dir.path(), &mut m, std::slice::from_ref(&NO_OP)).unwrap();

        assert!(!dir.path().join("no-op.marker").exists());
    }

    #[test]
    fn runs_steps_in_order_across_a_gap_of_two() {
        let dir = tempdir().unwrap();
        let mut m = FormatManifest::zero();

        run_list_for_dir(dir.path(), &mut m, &[NO_OP, REWRITER]).unwrap();

        assert!(dir.path().join("no-op.marker").exists());
        assert!(dir.path().join("rewriter.marker").exists());
        assert_eq!(m.version_of("wal"), 2);
    }

    #[test]
    fn backs_up_before_the_first_rewriting_step() {
        let dir = tempdir().unwrap();
        seed_data(dir.path());
        let mut m = FormatManifest::zero();
        m.set("wal", 1);

        run_list_for_dir(dir.path(), &mut m, std::slice::from_ref(&REWRITER)).unwrap();

        let backup = find_backup(dir.path()).expect("a backup should exist inside data_dir");
        assert!(
            backup.starts_with(dir.path().join(BACKUPS_DIR)),
            "backup {backup:?} must live inside data_dir, not beside it"
        );
        assert_eq!(
            std::fs::read(backup.join("sstables").join("000001.sst")).unwrap(),
            b"payload",
            "backup must contain the pre-migration bytes"
        );

        // The old convention (a `<data_dir>.pre-*.bak` sibling) must not
        // reappear: it evaporates under `docker compose down` when data_dir
        // is a bind mount, which is exactly the bug this backup exists to
        // avoid (REM-46 review finding 1). Scoped to a prefix derived from
        // this test's own tempdir name -- the shared /tmp this test runs in
        // can carry unrelated `*.bak` entries (other tests, other sessions)
        // that a bare `ends_with(".bak")` scan would misfire on.
        let this_dir_name = dir.path().file_name().unwrap().to_str().unwrap();
        let old_style_prefix = format!("{this_dir_name}.pre-");
        let leftover_sibling = std::fs::read_dir(dir.path().parent().unwrap())
            .unwrap()
            .flatten()
            .any(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.starts_with(&old_style_prefix) && name.ends_with(".bak")
            });
        assert!(
            !leftover_sibling,
            "no backup should be created outside data_dir"
        );
    }

    #[test]
    fn does_not_back_up_for_a_non_rewriting_step() {
        let dir = tempdir().unwrap();
        seed_data(dir.path());
        let mut m = FormatManifest::zero();

        run_list_for_dir(dir.path(), &mut m, std::slice::from_ref(&NO_OP)).unwrap();

        assert!(find_backup(dir.path()).is_none());
    }

    #[test]
    fn does_not_back_up_an_empty_data_directory() {
        let dir = tempdir().unwrap();
        let mut m = FormatManifest::zero();
        m.set("wal", 1);

        run_list_for_dir(dir.path(), &mut m, std::slice::from_ref(&REWRITER)).unwrap();

        assert!(
            find_backup(dir.path()).is_none(),
            "a fresh install must not leave a stray empty .bak behind"
        );
    }

    #[test]
    fn a_failing_step_leaves_the_manifest_untouched_so_it_reruns() {
        let dir = tempdir().unwrap();
        let mut m = FormatManifest::zero();

        assert!(run_list_for_dir(dir.path(), &mut m, std::slice::from_ref(&FAILING)).is_err());

        assert_eq!(m.version_of("wal"), 0);
        assert_eq!(
            FormatManifest::read(dir.path()).unwrap().version_of("wal"),
            0,
            "a crash or failure before the FORMAT write must rerun the step"
        );
    }

    #[test]
    fn a_failed_backup_aborts_before_the_step_runs() {
        let dir = tempdir().unwrap();
        let data = dir.path().join("remem");
        std::fs::create_dir_all(&data).unwrap();
        seed_data(&data);

        // The backup now lives inside data_dir (`data_dir/.backups/pre-*`),
        // so making data_dir itself unwritable (rather than its parent) is
        // what blocks the backup: `back_up` cannot create `.backups` under
        // a read-only directory.
        let mut perms = std::fs::metadata(&data).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&data, perms).unwrap();

        // Permission bits do not constrain root, and the suite runs as root
        // in some containers. Probe rather than assert something false.
        let running_as_root = std::fs::create_dir(data.join("probe")).is_ok();

        let mut m = FormatManifest::zero();
        m.set("wal", 1);
        let result = run_list_for_dir(&data, &mut m, std::slice::from_ref(&REWRITER));

        // Restore write access so the tempdir can be cleaned up.
        let mut perms = std::fs::metadata(&data).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&data, perms).unwrap();

        if running_as_root {
            return;
        }

        assert!(
            result.is_err(),
            "an unwritable backup destination must abort"
        );
        assert!(
            !data.join("rewriter.marker").exists(),
            "the migration must not run when its backup failed"
        );
        assert_eq!(m.version_of("wal"), 1, "FORMAT must not advance");
        assert_eq!(
            std::fs::read(data.join("sstables").join("000001.sst")).unwrap(),
            b"payload",
            "the original data must be untouched"
        );
    }

    #[test]
    fn verify_current_rejects_a_gap_with_no_migration() {
        let m = FormatManifest::zero();
        let err = verify_current(&m).unwrap_err().to_string();
        assert!(err.contains("no migration path"), "{err}");
    }

    #[test]
    fn verify_current_accepts_a_fully_migrated_manifest() {
        verify_current(&FormatManifest::current()).unwrap();
    }

    /// The registry the server actually ships must be able to bring a
    /// brand-new directory to current. Catches a migration added to
    /// `SUPPORTED` without a corresponding step.
    #[test]
    fn the_real_registry_brings_a_zero_manifest_to_current() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wal")).unwrap();
        let mut m = FormatManifest::zero();

        run_for_dir(dir.path(), &mut m).unwrap();
        verify_current(&m).unwrap();
    }

    #[test]
    fn the_real_registry_is_safe_to_rerun_when_current() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wal")).unwrap();
        let mut m = FormatManifest::zero();

        run_for_dir(dir.path(), &mut m).unwrap();
        let after_first = FormatManifest::read(dir.path()).unwrap();
        run_for_dir(dir.path(), &mut m).unwrap();

        assert_eq!(m.subsystems, after_first.subsystems);
        verify_current(&m).unwrap();
    }

    #[test]
    fn skipping_the_backup_still_runs_the_migration() {
        let dir = tempdir().unwrap();
        seed_data(dir.path());
        std::env::set_var("REMEM_SKIP_MIGRATION_BACKUP", "1");
        let mut m = FormatManifest::zero();
        m.set("wal", 1);

        run_list_for_dir(dir.path(), &mut m, std::slice::from_ref(&REWRITER)).unwrap();

        assert!(dir.path().join("rewriter.marker").exists());
        assert_eq!(m.version_of("wal"), 2);
        assert!(
            find_backup(dir.path()).is_none(),
            "no backup should exist when REMEM_SKIP_MIGRATION_BACKUP=1"
        );
        std::env::remove_var("REMEM_SKIP_MIGRATION_BACKUP");
    }

    #[test]
    fn backs_up_only_once_across_two_rewriting_steps() {
        let dir = tempdir().unwrap();
        seed_data(dir.path());
        let mut m = FormatManifest::zero();
        m.set("wal", 1);

        run_list_for_dir(dir.path(), &mut m, &[REWRITER, REWRITER_2]).unwrap();

        assert!(dir.path().join("rewriter.marker").exists());
        assert!(dir.path().join("rewriter-2.marker").exists());
        assert_eq!(m.version_of("wal"), 3);

        let backup_count = std::fs::read_dir(dir.path().join(BACKUPS_DIR))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("pre-"))
            .count();
        assert_eq!(
            backup_count, 1,
            "exactly one backup should exist after two rewriting steps"
        );
    }

    /// The backup destination lives inside `data_dir` (`.backups/pre-*`), so
    /// `copy_dir_recursive` must not walk into its own output -- otherwise a
    /// second migration's backup would recursively contain the first one,
    /// growing without bound.
    #[test]
    fn copy_dir_recursive_does_not_walk_into_the_backups_directory() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("data");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("real.txt"), b"real").unwrap();

        // An earlier backup, already sitting inside `src`.
        let old_backup = src.join(BACKUPS_DIR).join("pre-1");
        std::fs::create_dir_all(&old_backup).unwrap();
        std::fs::write(old_backup.join("real.txt"), b"real").unwrap();

        let dest = src.join(BACKUPS_DIR).join("pre-2");
        copy_dir_recursive(&src, &dest).unwrap();

        assert_eq!(std::fs::read(dest.join("real.txt")).unwrap(), b"real");
        assert!(
            !dest.join(BACKUPS_DIR).exists(),
            "copying data_dir into its own .backups subtree must not recurse into .backups"
        );
    }

    /// A leftover backup from a prior migration must not, by itself, make a
    /// directory look like it has data worth backing up again.
    #[test]
    fn a_directory_containing_only_a_leftover_backup_is_not_treated_as_having_data() {
        let dir = tempdir().unwrap();
        let backup = dir.path().join(BACKUPS_DIR).join("pre-1");
        std::fs::create_dir_all(&backup).unwrap();
        std::fs::write(backup.join("marker"), b"x").unwrap();

        assert!(
            !data_dir_has_data(dir.path()),
            "a directory containing only .backups/ must read as having no data"
        );
    }

    #[test]
    fn adoption_prepends_a_header_to_a_headerless_wal() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_path = wal_dir.join("current.wal");

        // A WAL written before the header existed: records with no header.
        let mut legacy = Vec::new();
        legacy.extend_from_slice(
            &crate::engine::storage::wal::WalRecord::insert(
                bytes::Bytes::from("k"),
                bytes::Bytes::from("v"),
                42,
            )
            .encode(),
        );
        std::fs::write(&wal_path, &legacy).unwrap();

        let mut m = FormatManifest::zero();
        run_for_dir(dir.path(), &mut m).unwrap();

        let bytes = std::fs::read(&wal_path).unwrap();
        assert_eq!(&bytes[..8], &crate::engine::storage::wal::WAL_MAGIC);
        assert_eq!(
            &bytes[crate::engine::storage::wal::WAL_HEADER_LEN as usize..],
            &legacy[..],
            "records must survive byte-for-byte"
        );
        assert_eq!(m.version_of("wal"), 1);
    }

    #[test]
    fn adoption_is_a_no_op_on_a_directory_with_no_wal() {
        let dir = tempdir().unwrap();
        let mut m = FormatManifest::zero();

        run_for_dir(dir.path(), &mut m).unwrap();

        verify_current(&m).unwrap();
        assert!(!dir.path().join("wal").join("current.wal").exists());
    }

    /// A crash between the rename and the FORMAT write reruns the step.
    #[test]
    fn adoption_rerun_leaves_an_already_headered_wal_alone() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_path = wal_dir.join("current.wal");

        let mut m = FormatManifest::zero();
        std::fs::write(
            &wal_path,
            WalRecord::insert(
                Bytes::from_static(b"memory:legacy-rerun"),
                Bytes::from_static(b"value"),
                7,
            )
            .encode(),
        )
        .unwrap();
        run_for_dir(dir.path(), &mut m).unwrap();
        let after_first = std::fs::read(&wal_path).unwrap();

        // Simulate the crash: the file is migrated, FORMAT never advanced.
        let mut rewound = FormatManifest::zero();
        run_for_dir(dir.path(), &mut rewound).unwrap();

        assert_eq!(std::fs::read(&wal_path).unwrap(), after_first);
    }

    /// Locate a backup created under `<data_dir>/.backups/`, if any.
    fn find_backup(data_dir: &Path) -> Option<PathBuf> {
        std::fs::read_dir(data_dir.join(BACKUPS_DIR))
            .ok()?
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("pre-"))
            })
    }

    // ── attrs-v1 ──────────────────────────────────────────────────────────

    #[test]
    fn attrs_v1_leaves_no_marker_for_a_brand_new_directory() {
        // Mirrors what `StorageEngine::new` actually hands the migration
        // runner: `sstables/`, `wal/` and `index/` all exist (created
        // unconditionally before migrations run -- see `engine.rs`), but
        // are empty. A directory-existence check would misread this as
        // "has data"; the migration must look at content instead.
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sstables")).unwrap();
        std::fs::create_dir_all(dir.path().join("wal")).unwrap();

        run_step_for_dir(&ATTRS_V1, dir.path()).unwrap();

        assert!(!dir.path().join(ATTR_BACKFILL_MARKER).exists());
    }

    #[test]
    fn attrs_v1_ignores_a_wal_that_only_has_a_header() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::write(
            wal_dir.join("current.wal"),
            crate::engine::storage::wal::encode_wal_header(),
        )
        .unwrap();

        run_step_for_dir(&ATTRS_V1, dir.path()).unwrap();

        assert!(!dir.path().join(ATTR_BACKFILL_MARKER).exists());
    }

    #[test]
    fn attrs_v1_flags_a_wal_holding_unflushed_records() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let mut bytes = crate::engine::storage::wal::encode_wal_header().to_vec();
        bytes.extend_from_slice(b"pretend-record");
        std::fs::write(wal_dir.join("current.wal"), bytes).unwrap();

        run_step_for_dir(&ATTRS_V1, dir.path()).unwrap();

        assert!(dir.path().join(ATTR_BACKFILL_MARKER).exists());
    }

    #[test]
    fn attrs_v1_flags_a_directory_with_checkpointed_sstables() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sstables")).unwrap();
        std::fs::write(dir.path().join("sstables").join("000001.sst"), b"payload").unwrap();

        run_step_for_dir(&ATTRS_V1, dir.path()).unwrap();

        assert!(dir.path().join(ATTR_BACKFILL_MARKER).exists());
    }

    #[test]
    fn attrs_v1_is_safe_to_rerun() {
        // A crash before the FORMAT write re-executes the step against a
        // directory it already looked at once.
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sstables")).unwrap();
        std::fs::write(dir.path().join("sstables").join("000001.sst"), b"payload").unwrap();

        run_step_for_dir(&ATTRS_V1, dir.path()).unwrap();
        run_step_for_dir(&ATTRS_V1, dir.path()).unwrap();

        assert!(dir.path().join(ATTR_BACKFILL_MARKER).exists());
    }

    #[test]
    fn the_real_registry_advances_attr_subsystems_on_a_zero_manifest() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wal")).unwrap();
        let mut m = FormatManifest::zero();

        run_for_dir(dir.path(), &mut m).unwrap();

        assert_eq!(m.version_of("attr"), 2);
        assert_eq!(m.version_of("index.attr"), 2);
        assert!(
            !dir.path().join(ATTR_BACKFILL_MARKER).exists(),
            "a brand-new directory owes no backfill"
        );
    }

    #[test]
    fn the_real_registry_advances_tag_index_to_v3_on_a_zero_manifest() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wal")).unwrap();
        let mut m = FormatManifest::zero();

        run_for_dir(dir.path(), &mut m).unwrap();

        assert_eq!(m.version_of("index.tags"), 3);
    }

    #[test]
    fn index_tags_v2_is_a_no_op_without_a_tag_manifest() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("index")).unwrap();

        run_step_for_dir(&INDEX_TAGS_V2, dir.path()).unwrap();

        assert!(!dir.path().join("index").join("tags.manifest").exists());
    }

    #[test]
    fn partition_index_generation_is_published_and_rerunnable() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("index").join("timeseries");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("timeseries.manifest"), b"manifest").unwrap();
        std::fs::write(root.join("timeseries_0000.seg"), b"segment").unwrap();
        let target = root.join("default").join("finance");

        copy_family_generation(&root, &target, &[]).unwrap();
        assert_eq!(
            std::fs::read(target.join("timeseries_0000.seg")).unwrap(),
            b"segment"
        );
        assert!(target.join(".generation-complete").exists());
        let marker = std::fs::metadata(target.join(".generation-complete"))
            .unwrap()
            .modified()
            .unwrap();
        copy_family_generation(&root, &target, &[]).unwrap();
        assert_eq!(
            std::fs::metadata(target.join(".generation-complete"))
                .unwrap()
                .modified()
                .unwrap(),
            marker
        );
    }

    #[test]
    fn partition_layout_v1_rewrites_legacy_primary_wal_keys_and_reruns() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_path = wal_dir.join("current.wal");
        {
            let mut wal = WAL::create(&wal_path).unwrap();
            wal.append(&WalRecord::insert(
                Bytes::from_static(b"memory:legacy"),
                Bytes::from_static(b"payload"),
                7,
            ))
            .unwrap();
            wal.append(&WalRecord::set_timestamp(
                Bytes::from_static(b"memory:legacy"),
                123,
                8,
            ))
            .unwrap();
            wal.sync().unwrap();
        }

        let mut config = test_config(dir.path());
        config.default_partition =
            crate::engine::storage::partition::PartitionId::new("finance").unwrap();
        let ctx = MigrationContext {
            data_dir: dir.path(),
            config: &config,
        };

        partition_layout_v1(&ctx).unwrap();
        let after_first = std::fs::read(&wal_path).unwrap();
        partition_layout_v1(&ctx).unwrap();
        assert_eq!(std::fs::read(&wal_path).unwrap(), after_first);

        let wal = WAL::open(&wal_path).unwrap();
        let records = wal.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(
            records[0].key,
            Bytes::from_static(b"partition:default:finance:memory:legacy")
        );
        // A `SetTimestamp` record has to be rekeyed with everything else, not
        // deferred to the index migration: replay feeds it straight into the
        // time-series index, so leaving it unpartitioned would reintroduce an
        // unpartitioned entry pointing at a record whose payload key this
        // very migration just moved.
        assert_eq!(
            records[1].key,
            Bytes::from_static(b"partition:default:finance:memory:legacy")
        );
    }

    #[test]
    fn partition_layout_v1_rewrites_legacy_primary_sstable_keys() {
        let dir = tempdir().unwrap();
        let level0 = dir.path().join("sstables").join("level0");
        std::fs::create_dir_all(&level0).unwrap();
        let path = level0.join("000001.sst");
        {
            let mut writer = SSTableWriter::with_level(&path, Compression::None, 0).unwrap();
            writer
                .add(
                    Bytes::from_static(b"attr:memory:legacy"),
                    Some(Bytes::from_static(b"sidecar")),
                    1,
                )
                .unwrap();
            writer
                .add(
                    Bytes::from_static(b"memory:legacy"),
                    Some(Bytes::from_static(b"payload")),
                    2,
                )
                .unwrap();
            writer.finish().unwrap();
        }

        run_step_for_dir(&PARTITION_LAYOUT_V1, dir.path()).unwrap();

        let reader = SSTableReader::open_with_cache(&path, None, 0).unwrap();
        assert!(reader.get(b"memory:legacy").unwrap().is_none());
        assert_eq!(
            reader
                .get(b"partition:default:default:memory:legacy")
                .unwrap()
                .unwrap()
                .value
                .unwrap(),
            Bytes::from_static(b"payload")
        );
        // The sidecar row travels with its payload. Leaving it at
        // `attr:memory:legacy` while the payload moved under the partition
        // prefix would orphan the row: nothing would ever read it again, and
        // nothing would ever delete it either.
        assert!(reader.get(b"attr:memory:legacy").unwrap().is_none());
        assert_eq!(
            reader
                .get(b"attr:partition:default:default:memory:legacy")
                .unwrap()
                .unwrap()
                .value
                .unwrap(),
            Bytes::from_static(b"sidecar")
        );
    }

    /// The format version advances for the whole directory, so a rekey that
    /// is skipped because an index happened to be switched off at upgrade
    /// time never gets a second chance. The operator who enables that index
    /// later inherits postings pointing at `memory:…` keys whose payloads
    /// this migration renamed — dangling, and no migration left to fix them.
    #[test]
    fn secondary_indexes_are_rekeyed_even_when_disabled_in_config() {
        use crate::engine::index::{EdgeMetadata, SegmentedCsrGraph};

        let dir = tempdir().unwrap();
        let index_dir = dir.path().join("index");
        std::fs::create_dir_all(&index_dir).unwrap();
        {
            let mut time = SegmentedBTreeIndex::new(BTreeConfig::default(), index_dir.clone());
            time.insert(7, Bytes::from_static(b"memory:legacy"))
                .unwrap();
            time.seal_growing().unwrap();

            let mut tags =
                SegmentedInvertedIndex::new(InvertedIndexConfig::default(), index_dir.clone());
            tags.add_tags(Bytes::from_static(b"memory:legacy"), &["rust".to_string()])
                .unwrap();
            tags.seal_growing().unwrap();

            let mut graph = SegmentedCsrGraph::new(GraphConfig::default(), index_dir.clone());
            graph
                .add_edge(
                    Bytes::from_static(b"memory:legacy"),
                    Bytes::from_static(b"memory:other"),
                    EdgeMetadata::with_type("related_to"),
                )
                .unwrap();
            graph.save_if_dirty().unwrap();
        }

        // Every secondary index switched off, as an operator running
        // vector-only might have it at upgrade time.
        let mut config = test_config(dir.path());
        config.time_series.enabled = false;
        config.tag_index.enabled = false;
        config.graph.enabled = false;

        rewrite_secondary_index_keys(dir.path(), &finance_binding(), &config).unwrap();

        let expected = Bytes::from_static(b"partition:default:finance:memory:legacy");
        let time =
            SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), index_dir.clone()).unwrap();
        assert_eq!(time.range(0, u64::MAX), vec![(7, expected.clone())]);

        let tags = SegmentedInvertedIndex::load_from_dir(
            InvertedIndexConfig::default(),
            index_dir.clone(),
        )
        .unwrap();
        assert_eq!(tags.search_and(&["rust"]), vec![expected.clone()]);

        let graph = SegmentedCsrGraph::load_from_dir(GraphConfig::default(), index_dir).unwrap();
        let neighbors = graph.get_neighbors(expected.as_ref()).unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(
            neighbors[0].0,
            Bytes::from_static(b"partition:default:finance:memory:other")
        );
    }

    /// `delete_with_attrs` logs a `Delete` for `attr:memory:<id>` alongside
    /// the payload's own. Rewriting only the payload key would replay the
    /// payload tombstone at the new key and the sidecar tombstone at the old
    /// one, leaving the deleted record's attribute row behind forever.
    #[test]
    fn wal_rewrite_moves_attribute_sidecar_keys_with_their_payload() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_path = wal_dir.join("current.wal");
        {
            let mut wal = WAL::create(&wal_path).unwrap();
            wal.append(&WalRecord::delete(Bytes::from_static(b"memory:gone"), 1))
                .unwrap();
            wal.append(&WalRecord::delete(
                crate::engine::attr::attr_key(b"memory:gone"),
                2,
            ))
            .unwrap();
            wal.sync().unwrap();
        }

        rewrite_primary_wal_keys(dir.path(), &finance_binding()).unwrap();

        let wal = WAL::open(&wal_path).unwrap();
        let records = wal.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(
            records[0].key,
            Bytes::from_static(b"partition:default:finance:memory:gone")
        );
        assert_eq!(
            records[1].key,
            Bytes::from_static(b"attr:partition:default:finance:memory:gone")
        );
    }

    #[test]
    fn a_newer_attr_version_is_refused() {
        let dir = tempdir().unwrap();
        let mut m = FormatManifest::current();
        // One past whatever this build supports: the point of the test is a
        // directory from a *future* build, so it has to move with SUPPORTED.
        m.set("attr", m.version_of("attr") + 1);
        m.write_atomic(dir.path()).unwrap();
        let err = FormatManifest::read(dir.path())
            .unwrap()
            .check()
            .unwrap_err();
        assert!(err.to_string().contains("attr"));
    }

    /// The scenario this migration exists for: a directory written by a
    /// build that supports attributes but was opened with no schema (so
    /// FORMAT already advanced past `attr`/`index.attr` on its very first
    /// open, independent of `attr_schema`), rolled back to look like a
    /// genuine pre-REM-75 directory, then opened for the first time with a
    /// schema.
    ///
    /// Opening the same directory twice with a schema the second time
    /// (rather than rolling FORMAT back) would not exercise this path: the
    /// first open already advances `attr`/`index.attr` to current whether
    /// or not a schema is registered, so a second open would find nothing
    /// pending (Defect B in the REM-75 Task 10 investigation).
    #[tokio::test]
    async fn a_pre_attr_directory_gets_backfilled_on_first_open_with_a_schema() {
        use crate::engine::attr::value::AttrValue;
        use crate::services::attrs::{memory_schema, SLOT_IMPORTANCE};

        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        {
            let engine = crate::engine::storage::engine::StorageEngine::new(
                crate::engine::storage::engine::EngineConfig {
                    data_dir: data_dir.clone(),
                    sync_writes: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let stored = serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000001",
                "content": "hello",
                "memory_type": "short_term",
                "metadata": {
                    "created_at": 5, "updated_at": 5, "accessed_at": 5, "access_count": 0,
                    "source": null, "tags": [], "importance": 0.8, "emotional_valence": 0.0,
                    "arousal": 0.0, "health": 100.0, "last_recalled_at": null,
                    "flashbulb_until": null, "ttl": null, "last_decay_at": null,
                    "last_health_check_at": null
                },
                "archived": false
            });
            engine
                .store_memory_core(
                    "memory:00000000-0000-0000-0000-000000000001".to_string(),
                    serde_json::to_vec(&stored).unwrap(),
                    None,
                    5,
                    &[],
                    None,
                )
                .await
                .unwrap();
            engine.checkpoint().await.unwrap();
            engine.graceful_shutdown().await.unwrap();
        }

        // Roll FORMAT back to what a genuine pre-REM-75 directory looked
        // like. The schema-less open above never created `index/attr/`
        // (`open_attrs` in `init.rs` only does that when a schema is
        // registered), so there is nothing to remove there -- this mirrors
        // a real upgrade, where a data directory that has never been
        // opened with a schema has no such directory either.
        let mut manifest = FormatManifest::read(&data_dir).unwrap();
        manifest.subsystems.remove("attr");
        manifest.subsystems.remove("index.attr");
        manifest.write_atomic(&data_dir).unwrap();

        let engine = crate::engine::storage::engine::StorageEngine::new(
            crate::engine::storage::engine::EngineConfig {
                data_dir: data_dir.clone(),
                sync_writes: true,
                attr_schema: Some(memory_schema()),
                // Wires the same closure `main.rs` supplies in production,
                // so this test exercises the real config-driven hook
                // (`EngineConfig::attr_project`) rather than a stand-in --
                // without it, `backfill_attrs_if_marked` has no way to turn
                // payload bytes into a row and silently leaves the marker
                // in place (see the no-projector test below).
                attr_project: Some(std::sync::Arc::new(|bytes: &[u8]| {
                    serde_json::from_slice::<crate::services::types::StoredMemory>(bytes)
                        .ok()
                        .map(|stored| crate::services::attrs::project(&stored))
                })),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let row = engine
            .get_attrs(b"memory:00000000-0000-0000-0000-000000000001")
            .await
            .unwrap()
            .expect("migration must backfill a row for every pre-existing record");
        assert_eq!(row.get(SLOT_IMPORTANCE), Some(AttrValue::F32(0.8)));

        let manifest = FormatManifest::read(&data_dir).unwrap();
        assert_eq!(manifest.version_of("attr"), 2);
        assert_eq!(manifest.version_of("index.attr"), 2);
        assert!(
            !data_dir.join(ATTR_BACKFILL_MARKER).exists(),
            "the marker must be removed once the backfill completes"
        );
    }

    /// Without a projection function, the backfill has no way to turn
    /// payload bytes into a row, so it must leave both the record
    /// unbackfilled and the marker in place -- confirming the gate in
    /// `backfill_attrs_if_marked` actually gates, rather than the marker
    /// simply being irrelevant once a schema is present.
    #[tokio::test]
    async fn a_schema_with_no_projector_leaves_the_marker_and_backfills_nothing() {
        use crate::services::attrs::memory_schema;

        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        {
            let engine = crate::engine::storage::engine::StorageEngine::new(
                crate::engine::storage::engine::EngineConfig {
                    data_dir: data_dir.clone(),
                    sync_writes: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            engine
                .store_memory_core(
                    "memory:no-projector".to_string(),
                    b"{}".to_vec(),
                    None,
                    5,
                    &[],
                    None,
                )
                .await
                .unwrap();
            engine.checkpoint().await.unwrap();
            engine.graceful_shutdown().await.unwrap();
        }

        let mut manifest = FormatManifest::read(&data_dir).unwrap();
        manifest.subsystems.remove("attr");
        manifest.subsystems.remove("index.attr");
        manifest.write_atomic(&data_dir).unwrap();

        let engine = crate::engine::storage::engine::StorageEngine::new(
            crate::engine::storage::engine::EngineConfig {
                data_dir: data_dir.clone(),
                sync_writes: true,
                attr_schema: Some(memory_schema()),
                // No `attr_project` supplied.
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(
            engine
                .get_attrs(b"memory:no-projector")
                .await
                .unwrap()
                .is_none(),
            "no projector means no row can be derived"
        );
        assert!(
            data_dir.join(ATTR_BACKFILL_MARKER).exists(),
            "the marker must survive for a future open that supplies a projector"
        );
    }
}
