//! The packed sidecar row: a record's attribute values, addressed by slot.
//!
//! Layout:
//! ```text
//! [u8  row_format_version]
//! [u16 schema_version]
//! [u8  bitmap_len]
//! [bitmap: bitmap_len bytes]   bit i set => slot i present
//! [values, fixed-width, ascending slot order, present slots only]
//! ```
//!
//! `bitmap_len` is stored rather than derived from the schema because the
//! schema grows: a row written when the highest slot was 8 carries a 2-byte
//! bitmap, and a reader sizing the bitmap from a later schema would misparse
//! it. Storing the length is what makes "adding a slot rewrites nothing" true.

use std::collections::BTreeMap;

use super::schema::AttrSchema;
use super::value::{AttrType, AttrValue};
use crate::engine::error::{Result, StorageError};

pub const ROW_FORMAT_VERSION: u8 = 1;

/// The maximum slot id that can be encoded in a row.
///
/// The row header stores `bitmap_len` in a single byte, which can encode at
/// most 255 bytes, addressing slots 0..=2039. Schemas exceeding this limit
/// cannot be safely opened, as encoding would truncate the bitmap_len on cast
/// and cause silent corruption of every row written thereafter. Call
/// `schema.check_encodable()` before opening any schema.
pub const MAX_ENCODABLE_SLOT: u16 = 2039;

#[derive(Debug, Clone, PartialEq)]
pub struct AttrRow {
    pub schema_version: u16,
    /// BTreeMap so iteration is in ascending slot order, which the wire
    /// format requires.
    values: BTreeMap<u16, AttrValue>,
}

impl AttrRow {
    pub fn new(schema_version: u16) -> Self {
        Self {
            schema_version,
            values: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, slot: u16, value: AttrValue) {
        self.values.insert(slot, value);
    }

    pub fn get(&self, slot: u16) -> Option<AttrValue> {
        self.values.get(&slot).copied()
    }

    pub fn encode(&self, schema: &AttrSchema) -> Vec<u8> {
        let bitmap_len = schema.bitmap_len();
        let mut out = Vec::with_capacity(4 + bitmap_len + 8 * self.values.len());
        out.push(ROW_FORMAT_VERSION);
        out.extend_from_slice(&self.schema_version.to_le_bytes());
        out.push(bitmap_len as u8);

        let mut bitmap = vec![0u8; bitmap_len];
        for slot in self.values.keys() {
            bitmap[*slot as usize / 8] |= 1 << (*slot % 8);
        }
        out.extend_from_slice(&bitmap);

        for value in self.values.values() {
            value.write_to(&mut out);
        }
        out
    }

    pub fn decode(bytes: &[u8], schema: &AttrSchema) -> Result<Self> {
        let bad = |msg: String| StorageError::InvalidArgument(msg);

        if bytes.len() < 4 {
            return Err(bad(format!(
                "attribute row is {} bytes, need at least 4",
                bytes.len()
            )));
        }
        let version = bytes[0];
        if version != ROW_FORMAT_VERSION {
            return Err(bad(format!(
                "unsupported attribute row format version {version}"
            )));
        }
        let schema_version = u16::from_le_bytes([bytes[1], bytes[2]]);
        let bitmap_len = bytes[3] as usize;

        let bitmap_start = 4;
        let values_start = bitmap_start + bitmap_len;
        if bytes.len() < values_start {
            return Err(bad(
                "attribute row is truncated inside its bitmap".to_string()
            ));
        }
        let bitmap = &bytes[bitmap_start..values_start];

        let mut row = AttrRow::new(schema_version);
        let mut offset = values_start;

        for slot in 0..(bitmap_len * 8) as u16 {
            if bitmap[slot as usize / 8] & (1 << (slot % 8)) == 0 {
                continue;
            }
            let Some(ty) = schema.type_of(slot) else {
                return Err(bad(format!(
                    "attribute row references slot {slot}, which the schema does not define"
                )));
            };
            let value = AttrValue::read_from(ty, &bytes[offset..])?;
            offset += AttrType::width(ty);
            // Retired slots are consumed for their width but not surfaced.
            if !schema.is_retired(slot) {
                row.set(slot, value);
            }
        }

        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::attr::schema::SlotDef;

    fn slot(slot: u16, name: &str, ty: AttrType, retired: bool) -> SlotDef {
        SlotDef {
            slot,
            name: name.to_string(),
            ty,
            indexed: false,
            retired,
        }
    }

    fn schema() -> AttrSchema {
        AttrSchema {
            version: 1,
            slots: vec![
                slot(0, "archived", AttrType::Bool, false),
                slot(1, "kind", AttrType::U8, false),
                slot(2, "importance", AttrType::F32, false),
                slot(9, "created_at", AttrType::U64, false),
            ],
        }
    }

    #[test]
    fn row_round_trips() {
        let s = schema();
        let mut row = AttrRow::new(s.version);
        row.set(0, AttrValue::Bool(false));
        row.set(1, AttrValue::U8(2));
        row.set(2, AttrValue::F32(0.75));
        row.set(9, AttrValue::U64(1_700_000_000_000));

        let bytes = row.encode(&s);
        let back = AttrRow::decode(&bytes, &s).unwrap();

        assert_eq!(back.get(0), Some(AttrValue::Bool(false)));
        assert_eq!(back.get(1), Some(AttrValue::U8(2)));
        assert_eq!(back.get(2), Some(AttrValue::F32(0.75)));
        assert_eq!(back.get(9), Some(AttrValue::U64(1_700_000_000_000)));
    }

    #[test]
    fn a_slot_absent_from_the_row_reads_as_none() {
        // This is how a newly added slot behaves against pre-existing rows:
        // no rewrite is needed, the reader just sees None.
        let s = schema();
        let mut row = AttrRow::new(s.version);
        row.set(0, AttrValue::Bool(true));
        let back = AttrRow::decode(&row.encode(&s), &s).unwrap();
        assert_eq!(back.get(0), Some(AttrValue::Bool(true)));
        assert_eq!(back.get(2), None);
    }

    #[test]
    fn a_retired_slot_is_skipped_by_width_not_by_guess() {
        // Encode with slot 1 live, then decode under a schema that retired it.
        // Slot 2's value must still land correctly, which only works if the
        // reader consumed exactly one byte for the retired slot.
        let writing = schema();
        let mut row = AttrRow::new(writing.version);
        row.set(1, AttrValue::U8(9));
        row.set(2, AttrValue::F32(-0.25));
        let bytes = row.encode(&writing);

        let mut reading = schema();
        reading.slots[1].retired = true;
        let back = AttrRow::decode(&bytes, &reading).unwrap();

        assert_eq!(back.get(1), None, "retired slots are not surfaced");
        assert_eq!(
            back.get(2),
            Some(AttrValue::F32(-0.25)),
            "following slot must still align"
        );
    }

    #[test]
    fn rows_survive_the_schema_growing_past_a_bitmap_byte() {
        // A row written when the highest slot was 9 has a 2-byte bitmap.
        // After slot 20 is added the schema implies 3, and a reader deriving
        // the length from the current schema would misparse the row.
        let writing = schema();
        let mut row = AttrRow::new(writing.version);
        row.set(2, AttrValue::F32(0.5));
        row.set(9, AttrValue::U64(7));
        let bytes = row.encode(&writing);

        let mut grown = schema();
        grown.slots.push(slot(20, "later", AttrType::U32, false));
        assert_eq!(grown.bitmap_len(), 3);

        let back = AttrRow::decode(&bytes, &grown).unwrap();
        assert_eq!(back.get(2), Some(AttrValue::F32(0.5)));
        assert_eq!(back.get(9), Some(AttrValue::U64(7)));
        assert_eq!(back.get(20), None);
    }

    #[test]
    fn an_unknown_slot_in_the_row_is_an_error() {
        let s = schema();
        let mut row = AttrRow::new(s.version);
        row.set(2, AttrValue::F32(0.5));
        let mut bytes = row.encode(&s);
        // Light up slot 3, which the schema does not define.
        bytes[4] |= 1 << 3;
        assert!(AttrRow::decode(&bytes, &s).is_err());
    }

    #[test]
    fn a_truncated_row_is_an_error() {
        let s = schema();
        let mut row = AttrRow::new(s.version);
        row.set(9, AttrValue::U64(1));
        let bytes = row.encode(&s);
        assert!(AttrRow::decode(&bytes[..bytes.len() - 3], &s).is_err());
    }

    #[test]
    fn nine_slot_row_is_44_bytes() {
        // Guards the size claim in the spec; a regression here means the
        // sidecar got fat and the write-amplification budget moved.
        let s = AttrSchema {
            version: 1,
            slots: vec![
                slot(0, "archived", AttrType::Bool, false),
                slot(1, "memory_type", AttrType::U8, false),
                slot(2, "importance", AttrType::F32, false),
                slot(3, "created_at", AttrType::U64, false),
                slot(4, "accessed_at", AttrType::U64, false),
                slot(5, "access_count", AttrType::U32, false),
                slot(6, "health", AttrType::F32, false),
                slot(7, "emotional_valence", AttrType::F32, false),
                slot(8, "arousal", AttrType::F32, false),
            ],
        };
        let mut row = AttrRow::new(1);
        row.set(0, AttrValue::Bool(false));
        row.set(1, AttrValue::U8(0));
        row.set(2, AttrValue::F32(0.5));
        row.set(3, AttrValue::U64(0));
        row.set(4, AttrValue::U64(0));
        row.set(5, AttrValue::U32(0));
        row.set(6, AttrValue::F32(100.0));
        row.set(7, AttrValue::F32(0.0));
        row.set(8, AttrValue::F32(0.0));
        assert_eq!(row.encode(&s).len(), 44);
    }
}
