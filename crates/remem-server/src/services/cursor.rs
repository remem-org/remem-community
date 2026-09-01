//! The continuation a listing hands back so a caller can ask for the next
//! page.
//!
//! ## Why a position rather than an offset
//!
//! An offset says "skip the first N matches", which the system can only honour
//! by walking those N again — so the cost of page ten is ten pages of work —
//! and which stops meaning the same thing the moment anything is written: a
//! memory created during a newest-first sequence shifts every later page down
//! by one and hands the caller a memory it already received.
//!
//! A position says "resume after this exact point in the ordering". The
//! ordering attribute never changes once a memory is created (REM-79), so the
//! point keeps its meaning for as long as the memory exists, and a page costs
//! the page.
//!
//! ## What it carries, and what it must not
//!
//! The ordering value and the memory's own id: enough to name the boundary,
//! and nothing a caller does not already have. Deliberately *not* the physical
//! record key — that embeds the tenant and partition the record lives in, so
//! handing it out would disclose the deployment's partitioning and let a
//! copied token address another tenant's rows. The physical key is rebuilt
//! from the id inside the scope of the request that presents the token, so a
//! token can only ever resume within what its presenter is already allowed to
//! read.
//!
//! Opaque on purpose. A caller that parses a continuation is a caller that
//! will eventually construct one, and the format is ours to change.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use uuid::Uuid;

/// Current token layout. Bumped if the payload ever changes shape, so an old
/// token is refused rather than misread as a new one.
const CURSOR_VERSION: u8 = 1;

/// 1 version byte + 8 ordering bytes + 16 uuid bytes.
const CURSOR_LEN: usize = 1 + 8 + 16;

/// A decoded position in a listing's ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCursor {
    /// The order-encoded value of the ordering attribute at the boundary.
    pub order: u64,
    /// The memory the boundary sits at.
    pub id: Uuid,
}

/// The single failure a caller can observe.
///
/// One variant, deliberately. A token that is malformed, one built for a
/// different layout, and one naming a memory the caller cannot see are all
/// reported identically: distinguishing them would let a caller use the error
/// to learn whether a given memory exists somewhere it is not allowed to look.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCursor;

impl std::fmt::Display for InvalidCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cursor is not valid for this request")
    }
}

impl ListCursor {
    pub fn new(order: u64, id: Uuid) -> Self {
        Self { order, id }
    }

    pub fn encode(&self) -> String {
        let mut bytes = Vec::with_capacity(CURSOR_LEN);
        bytes.push(CURSOR_VERSION);
        bytes.extend_from_slice(&self.order.to_be_bytes());
        bytes.extend_from_slice(self.id.as_bytes());
        URL_SAFE_NO_PAD.encode(bytes)
    }

    pub fn decode(token: &str) -> std::result::Result<Self, InvalidCursor> {
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| InvalidCursor)?;
        if bytes.len() != CURSOR_LEN || bytes[0] != CURSOR_VERSION {
            return Err(InvalidCursor);
        }
        let order = u64::from_be_bytes(bytes[1..9].try_into().map_err(|_| InvalidCursor)?);
        let id_bytes: [u8; 16] = bytes[9..25].try_into().map_err(|_| InvalidCursor)?;
        Ok(Self {
            order,
            id: Uuid::from_bytes(id_bytes),
        })
    }
}

/// A position in a connection listing.
///
/// A connection has no identity of its own — it is an edge between two
/// memories — so a boundary needs both ends: which source the walk had
/// reached, and how far into that source's edges it had got. The source half
/// is an ordinary listing position, because connections are walked in the
/// order their source memories are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionCursor {
    pub source: ListCursor,
    pub target: Uuid,
}

/// 1 version byte + 8 ordering bytes + two uuids.
const CONNECTION_CURSOR_LEN: usize = 1 + 8 + 16 + 16;
const CONNECTION_CURSOR_VERSION: u8 = 1;

impl ConnectionCursor {
    pub fn new(source: ListCursor, target: Uuid) -> Self {
        Self { source, target }
    }

    pub fn encode(&self) -> String {
        let mut bytes = Vec::with_capacity(CONNECTION_CURSOR_LEN);
        bytes.push(CONNECTION_CURSOR_VERSION);
        bytes.extend_from_slice(&self.source.order.to_be_bytes());
        bytes.extend_from_slice(self.source.id.as_bytes());
        bytes.extend_from_slice(self.target.as_bytes());
        URL_SAFE_NO_PAD.encode(bytes)
    }

    pub fn decode(token: &str) -> std::result::Result<Self, InvalidCursor> {
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| InvalidCursor)?;
        if bytes.len() != CONNECTION_CURSOR_LEN || bytes[0] != CONNECTION_CURSOR_VERSION {
            return Err(InvalidCursor);
        }
        let order = u64::from_be_bytes(bytes[1..9].try_into().map_err(|_| InvalidCursor)?);
        let source: [u8; 16] = bytes[9..25].try_into().map_err(|_| InvalidCursor)?;
        let target: [u8; 16] = bytes[25..41].try_into().map_err(|_| InvalidCursor)?;
        Ok(Self {
            source: ListCursor::new(order, Uuid::from_bytes(source)),
            target: Uuid::from_bytes(target),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_survives_a_round_trip() {
        let cursor = ListCursor::new(1_700_000_000_000, Uuid::new_v4());
        assert_eq!(ListCursor::decode(&cursor.encode()).unwrap(), cursor);
    }

    #[test]
    fn a_cursor_is_url_safe_and_carries_no_padding() {
        let token = ListCursor::new(u64::MAX, Uuid::new_v4()).encode();
        assert!(
            token
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "a continuation travels in a query string: {token}"
        );
    }

    #[test]
    fn a_token_that_is_not_one_of_ours_is_refused() {
        for bad in ["", "not-base64!!", "c2hvcnQ", &"A".repeat(64)] {
            assert!(
                ListCursor::decode(bad).is_err(),
                "{bad:?} must not be read as a position"
            );
        }
    }

    #[test]
    fn a_token_from_a_different_layout_is_refused() {
        let mut bytes = vec![CURSOR_VERSION + 1];
        bytes.extend_from_slice(&0u64.to_be_bytes());
        bytes.extend_from_slice(Uuid::new_v4().as_bytes());

        assert!(ListCursor::decode(&URL_SAFE_NO_PAD.encode(bytes)).is_err());
    }

    #[test]
    fn a_cursor_does_not_disclose_where_a_record_is_stored() {
        // The whole reason the token carries an id rather than a record key:
        // the key names a tenant and a partition, and a caller must not learn
        // either from a page boundary.
        let token = ListCursor::new(42, Uuid::new_v4()).encode();
        let decoded = URL_SAFE_NO_PAD.decode(&token).unwrap();

        assert_eq!(decoded.len(), CURSOR_LEN, "nothing else fits in it");
    }

    #[test]
    fn a_connection_cursor_survives_a_round_trip() {
        let cursor = ConnectionCursor::new(
            ListCursor::new(1_700_000_000_000, Uuid::new_v4()),
            Uuid::new_v4(),
        );
        assert_eq!(ConnectionCursor::decode(&cursor.encode()).unwrap(), cursor);
    }

    #[test]
    fn the_two_cursor_kinds_do_not_decode_as_each_other() {
        // They share a version byte and a prefix; only the length tells them
        // apart. Reading one as the other would resume a walk at a position
        // that means nothing in it.
        let listing = ListCursor::new(7, Uuid::new_v4());
        let connection = ConnectionCursor::new(listing, Uuid::new_v4());

        assert!(ConnectionCursor::decode(&listing.encode()).is_err());
        assert!(ListCursor::decode(&connection.encode()).is_err());
    }
}
