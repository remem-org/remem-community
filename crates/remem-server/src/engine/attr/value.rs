//! Attribute types, their fixed-width wire codec, and the order encoding
//! that lets an ordered index key on them.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::engine::error::{Result, StorageError};

/// The type of an attribute slot. Every variant is fixed-width, which is what
/// lets a reader skip a retired slot it no longer understands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttrType {
    Bool,
    U8,
    U32,
    U64,
    F32,
}

impl AttrType {
    pub const fn width(self) -> usize {
        match self {
            AttrType::Bool | AttrType::U8 => 1,
            AttrType::U32 | AttrType::F32 => 4,
            AttrType::U64 => 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AttrValue {
    Bool(bool),
    U8(u8),
    U32(u32),
    U64(u64),
    F32(f32),
}

impl AttrValue {
    pub fn ty(&self) -> AttrType {
        match self {
            AttrValue::Bool(_) => AttrType::Bool,
            AttrValue::U8(_) => AttrType::U8,
            AttrValue::U32(_) => AttrType::U32,
            AttrValue::U64(_) => AttrType::U64,
            AttrValue::F32(_) => AttrType::F32,
        }
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            AttrValue::Bool(b) => out.push(*b as u8),
            AttrValue::U8(x) => out.push(*x),
            AttrValue::U32(x) => out.extend_from_slice(&x.to_le_bytes()),
            AttrValue::U64(x) => out.extend_from_slice(&x.to_le_bytes()),
            AttrValue::F32(x) => out.extend_from_slice(&x.to_le_bytes()),
        }
    }

    /// Decode exactly `ty.width()` bytes from the front of `src`.
    pub fn read_from(ty: AttrType, src: &[u8]) -> Result<Self> {
        if src.len() < ty.width() {
            return Err(StorageError::InvalidArgument(format!(
                "attribute value needs {} bytes, got {}",
                ty.width(),
                src.len()
            )));
        }
        Ok(match ty {
            AttrType::Bool => AttrValue::Bool(src[0] != 0),
            AttrType::U8 => AttrValue::U8(src[0]),
            AttrType::U32 => AttrValue::U32(u32::from_le_bytes(src[..4].try_into().unwrap())),
            AttrType::U64 => AttrValue::U64(u64::from_le_bytes(src[..8].try_into().unwrap())),
            AttrType::F32 => AttrValue::F32(f32::from_le_bytes(src[..4].try_into().unwrap())),
        })
    }

    /// A `u64` whose natural ordering matches this value's semantic ordering,
    /// so a `u64`-keyed ordered index can range over it.
    ///
    /// IEEE-754 floats do not order correctly as raw bits: negatives sort
    /// descending and sit above positives. Flipping the sign bit on
    /// non-negatives and every bit on negatives produces a monotonic mapping.
    /// NaN's position in that mapping depends on its sign bit, not just its
    /// payload: a positive-signed NaN takes the flip-the-sign-bit branch and
    /// lands above every finite value, but a negative-signed NaN takes the
    /// invert-every-bit branch and lands within the negative range instead —
    /// below every finite negative, not above everything. Projections must
    /// not emit NaN either way; its placement here is not a documented
    /// ordering guarantee for callers to rely on.
    ///
    /// `-0.0` and `0.0` compare equal under `PartialEq` (and under this
    /// type's own `Eq` matching), so they are normalised to the same bit
    /// pattern before encoding. Without this, `Eq(0.0)` and `Eq(-0.0)` would
    /// each key to a different, adjacent `u64` and an `Eq` access path would
    /// narrow the index walk to the wrong single point — never visiting an
    /// index entry for the other, equal value and silently dropping a
    /// matching, correctly-indexed row from the result.
    pub fn order_key(&self) -> u64 {
        match self {
            AttrValue::Bool(b) => *b as u64,
            AttrValue::U8(x) => *x as u64,
            AttrValue::U32(x) => *x as u64,
            AttrValue::U64(x) => *x,
            AttrValue::F32(f) => {
                let f = if *f == 0.0 { 0.0 } else { *f };
                let bits = f.to_bits();
                let flipped = if bits & 0x8000_0000 != 0 {
                    !bits
                } else {
                    bits ^ 0x8000_0000
                };
                flipped as u64
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_key_is_monotonic_across_zero() {
        // emotional_valence spans -1.0..1.0, so ordering must survive the
        // sign boundary. A naive to_bits() cast reverses the negatives.
        //
        // -0.0 and 0.0 are the one adjacent pair that must produce the SAME
        // key rather than a strictly increasing one: they compare equal
        // under `PartialEq`, and an access path that keys equal values
        // differently silently drops a matching row (see order_key's doc
        // comment). Every other pair must still be strictly increasing, so
        // this test keeps catching a naive `to_bits()` cast everywhere else.
        let ascending = [
            -1.0f32,
            -0.5,
            -f32::MIN_POSITIVE,
            -0.0,
            0.0,
            f32::MIN_POSITIVE,
            0.5,
            1.0,
        ];
        let keys: Vec<u64> = ascending
            .iter()
            .map(|f| AttrValue::F32(*f).order_key())
            .collect();
        for (i, pair) in keys.windows(2).enumerate() {
            if ascending[i] == 0.0 && ascending[i + 1] == 0.0 {
                assert_eq!(
                    pair[0], pair[1],
                    "-0.0 and 0.0 are equal values and must share an order_key"
                );
                continue;
            }
            assert!(
                pair[0] < pair[1],
                "order_key must be strictly increasing, got {} then {}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn order_key_of_negative_zero_equals_order_key_of_zero() {
        assert_eq!(
            AttrValue::F32(-0.0).order_key(),
            AttrValue::F32(0.0).order_key()
        );
    }

    #[test]
    fn integer_order_keys_are_identity() {
        assert_eq!(AttrValue::U64(42).order_key(), 42);
        assert_eq!(AttrValue::U32(7).order_key(), 7);
        assert_eq!(AttrValue::U8(3).order_key(), 3);
        assert_eq!(AttrValue::Bool(true).order_key(), 1);
        assert_eq!(AttrValue::Bool(false).order_key(), 0);
    }

    #[test]
    fn values_round_trip_through_the_fixed_width_codec() {
        let cases = [
            AttrValue::Bool(true),
            AttrValue::U8(200),
            AttrValue::U32(4_000_000_000),
            AttrValue::U64(u64::MAX),
            AttrValue::F32(-0.75),
        ];
        for v in cases {
            let mut buf = Vec::new();
            v.write_to(&mut buf);
            assert_eq!(
                buf.len(),
                v.ty().width(),
                "encoded width must match declared width"
            );
            let back = AttrValue::read_from(v.ty(), &buf).unwrap();
            assert_eq!(back, v);
        }
    }

    #[test]
    fn read_from_rejects_a_short_buffer() {
        let err = AttrValue::read_from(AttrType::U64, &[0u8; 3]);
        assert!(
            err.is_err(),
            "a truncated buffer must be an error, not a partial read"
        );
    }
}
