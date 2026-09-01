//! The registered attribute schema: which slot means what, how wide it is,
//! and whether it carries an ordered index.
//!
//! Slot ids are never reused. A retired slot keeps its declared type so that
//! rows written before the retirement can still be decoded — the width is
//! what lets a reader skip past a value it no longer has a name for.

use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::row::MAX_ENCODABLE_SLOT;
use super::value::AttrType;
use crate::engine::error::{Result, StorageError};
use crate::engine::storage::durable_rename::durable_rename;

/// Filename of the schema inside the attribute index directory.
pub const SCHEMA_FILE: &str = "SCHEMA";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDef {
    pub slot: u16,
    pub name: String,
    pub ty: AttrType,
    /// Whether this slot carries an ordered index usable as an access path.
    pub indexed: bool,
    /// Retired slots are never written to new rows and have no index, but
    /// their type is retained so older rows stay decodable.
    pub retired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttrSchema {
    pub version: u16,
    pub slots: Vec<SlotDef>,
}

impl AttrSchema {
    pub fn get(&self, slot: u16) -> Option<&SlotDef> {
        self.slots.iter().find(|s| s.slot == slot)
    }

    pub fn type_of(&self, slot: u16) -> Option<AttrType> {
        self.get(slot).map(|s| s.ty)
    }

    pub fn is_retired(&self, slot: u16) -> bool {
        self.get(slot).is_some_and(|s| s.retired)
    }

    pub fn max_slot(&self) -> u16 {
        self.slots.iter().map(|s| s.slot).max().unwrap_or(0)
    }

    /// Bitmap bytes needed to address every slot in this schema.
    pub fn bitmap_len(&self) -> usize {
        (self.max_slot() as usize / 8) + 1
    }

    pub fn indexed_slots(&self) -> impl Iterator<Item = &SlotDef> {
        self.slots.iter().filter(|s| s.indexed && !s.retired)
    }

    pub fn read(dir: &Path) -> Result<Option<Self>> {
        let path = dir.join(SCHEMA_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| StorageError::invalid_format(&path, format!("unreadable schema: {e}")))
    }

    pub fn write_atomic(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let dest = dir.join(SCHEMA_FILE);
        let tmp = dir.join(format!("{SCHEMA_FILE}.tmp"));
        let json = serde_json::to_vec_pretty(self)
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

    /// Refuse a registered schema that cannot read what is already on disk.
    ///
    /// Adding slots is always fine. Changing or dropping one is not: existing
    /// rows were encoded against the persisted definition, and a mismatch
    /// silently misparses every one of them.
    pub fn validate_against(&self, persisted: &Self) -> Result<()> {
        for old in &persisted.slots {
            let Some(new) = self.get(old.slot) else {
                return Err(StorageError::invalid_format(
                    Path::new(SCHEMA_FILE),
                    format!(
                        "slot {} ({:?}) exists on disk but not in the registered schema: \
                         retire slots, never drop them",
                        old.slot, old.name
                    ),
                ));
            };
            if new.ty != old.ty || new.name != old.name {
                return Err(StorageError::invalid_format(
                    Path::new(SCHEMA_FILE),
                    format!(
                        "slot {} changed from {:?}/{:?} to {:?}/{:?}: retire the slot and \
                         add a new one instead",
                        old.slot, old.name, old.ty, new.name, new.ty
                    ),
                ));
            }
            if old.retired && !new.retired {
                return Err(StorageError::invalid_format(
                    Path::new(SCHEMA_FILE),
                    format!(
                        "slot {} ({:?}) is retired on disk and cannot be revived",
                        old.slot, old.name
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Refuse a schema that cannot be safely encoded into rows.
    ///
    /// The row header stores `bitmap_len` in a single byte. A schema whose
    /// `max_slot()` exceeds 2039 would require a `bitmap_len` > 255, which would
    /// silently truncate on cast and corrupt every row written with this schema.
    pub fn check_encodable(&self) -> Result<()> {
        let max = self.max_slot();
        if max > MAX_ENCODABLE_SLOT {
            return Err(StorageError::invalid_format(
                Path::new(SCHEMA_FILE),
                format!(
                    "slot {max} exceeds maximum encodable slot {MAX_ENCODABLE_SLOT}: \
                     row header stores bitmap_len in one byte, limiting schemas to 255 bytes (2040 slots)"
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(slot: u16, name: &str, ty: AttrType, indexed: bool) -> SlotDef {
        SlotDef {
            slot,
            name: name.to_string(),
            ty,
            indexed,
            retired: false,
        }
    }

    fn base() -> AttrSchema {
        AttrSchema {
            version: 1,
            slots: vec![
                slot(0, "archived", AttrType::Bool, false),
                slot(1, "importance", AttrType::F32, true),
            ],
        }
    }

    #[test]
    fn schema_round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let s = base();
        s.write_atomic(dir.path()).unwrap();
        let back = AttrSchema::read(dir.path()).unwrap().unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn absent_schema_file_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(AttrSchema::read(dir.path()).unwrap().is_none());
    }

    #[test]
    fn bitmap_len_covers_the_highest_slot() {
        let mut s = base();
        assert_eq!(s.bitmap_len(), 1); // slots 0..=1
        s.slots.push(slot(8, "health", AttrType::F32, true));
        assert_eq!(s.bitmap_len(), 2); // slot 8 needs a second byte
    }

    #[test]
    fn adding_a_slot_validates() {
        let persisted = base();
        let mut current = base();
        current.slots.push(slot(2, "health", AttrType::F32, true));
        current.version = 2;
        assert!(current.validate_against(&persisted).is_ok());
    }

    #[test]
    fn changing_a_slot_type_is_refused() {
        let persisted = base();
        let mut current = base();
        current.slots[1].ty = AttrType::U32;
        let err = current.validate_against(&persisted).unwrap_err();
        assert!(
            err.to_string().contains("importance"),
            "the error must name the offending slot, got: {err}"
        );
    }

    #[test]
    fn dropping_a_persisted_slot_is_refused() {
        // Retiring is how a slot goes away; deleting it loses the width
        // needed to skip it when decoding older rows.
        let persisted = base();
        let mut current = base();
        current.slots.remove(1);
        assert!(current.validate_against(&persisted).is_err());
    }

    #[test]
    fn un_retiring_a_slot_is_refused() {
        let mut persisted = base();
        persisted.slots[1].retired = true;
        let current = base(); // slot 1 retired = false
        assert!(current.validate_against(&persisted).is_err());
    }

    #[test]
    fn retired_slots_keep_their_type_for_width_skipping() {
        let mut s = base();
        s.slots[1].retired = true;
        assert_eq!(s.type_of(1), Some(AttrType::F32));
        assert!(s.is_retired(1));
    }

    #[test]
    fn a_normal_schema_passes_check_encodable() {
        let s = base();
        assert!(s.check_encodable().is_ok());
    }

    #[test]
    fn a_schema_with_a_slot_at_the_limit_passes() {
        let s = AttrSchema {
            version: 1,
            slots: vec![slot(
                crate::engine::attr::row::MAX_ENCODABLE_SLOT,
                "max",
                AttrType::U32,
                false,
            )],
        };
        assert!(s.check_encodable().is_ok());
    }

    #[test]
    fn a_schema_one_past_the_limit_is_refused() {
        let s = AttrSchema {
            version: 1,
            slots: vec![slot(
                crate::engine::attr::row::MAX_ENCODABLE_SLOT + 1,
                "too_high",
                AttrType::U32,
                false,
            )],
        };
        let err = s.check_encodable().unwrap_err();
        assert!(
            err.to_string().contains("2040"),
            "the error must name the offending slot 2040, got: {err}"
        );
    }
}
