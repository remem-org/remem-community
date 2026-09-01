//! Schema-generic attribute storage: the queryable half of a record.
//!
//! A record's filterable attributes live in a compact "sidecar" row beside
//! its opaque payload, so a predicate can be evaluated without deserializing
//! anything the caller did not ask for. See
//! `docs/superpowers/specs/2026-08-12-rem-75-attribute-store-design.md`.

#![allow(dead_code)]

pub mod index;
pub mod row;
pub mod schema;
pub mod select;
pub mod value;

use bytes::Bytes;

/// Prefix distinguishing a sidecar row from the record it describes.
pub const ATTR_PREFIX: &[u8] = b"attr:";

/// The sidecar key for `record_key`.
///
/// The whole record key is prefixed rather than a uuid spliced in, so this
/// module never parses a key or assumes what a key contains. REM-76 changes
/// this one function to make the layout partition-aware.
pub fn attr_key(record_key: &[u8]) -> Bytes {
    let mut k = Vec::with_capacity(ATTR_PREFIX.len() + record_key.len());
    k.extend_from_slice(ATTR_PREFIX);
    k.extend_from_slice(record_key);
    Bytes::from(k)
}
