//! Partition identity and scope types for storage access.
//!
//! Normal reads are bound to a non-empty partition scope. Normal writes are
//! bound to one target partition.

use std::collections::BTreeSet;
use std::fmt;

use bytes::Bytes;

const MAX_ID_LEN: usize = 128;
const DEFAULT_TENANT: &str = "default";
const DEFAULT_PARTITION: &str = "default";
pub const PARTITIONED_RECORD_PREFIX: &[u8] = b"partition:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionIdError {
    Empty,
    TooLong { max: usize },
    InvalidChar { ch: char },
}

impl fmt::Display for PartitionIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionIdError::Empty => write!(f, "partition identifier cannot be empty"),
            PartitionIdError::TooLong { max } => {
                write!(f, "partition identifier cannot be longer than {max} bytes")
            }
            PartitionIdError::InvalidChar { ch } => {
                write!(f, "partition identifier contains invalid character {ch:?}")
            }
        }
    }
}

impl std::error::Error for PartitionIdError {}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionScopeError {
    Empty,
}

impl fmt::Display for PartitionScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionScopeError::Empty => write!(f, "partition scope cannot be empty"),
        }
    }
}

impl std::error::Error for PartitionScopeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionKeyError {
    EmptyLogicalKey,
    Malformed,
    InvalidUtf8,
    InvalidId(PartitionIdError),
}

impl fmt::Display for PartitionKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionKeyError::EmptyLogicalKey => write!(f, "logical key cannot be empty"),
            PartitionKeyError::Malformed => write!(f, "partitioned record key is malformed"),
            PartitionKeyError::InvalidUtf8 => {
                write!(f, "partitioned record key contains invalid UTF-8")
            }
            PartitionKeyError::InvalidId(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for PartitionKeyError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(value: impl Into<String>) -> Result<Self, PartitionIdError> {
        validate_id(&value.into()).map(Self)
    }

    pub fn default_legacy() -> Self {
        Self(DEFAULT_TENANT.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(String);

impl PartitionId {
    pub fn new(value: impl Into<String>) -> Result<Self, PartitionIdError> {
        validate_id(&value.into()).map(Self)
    }

    pub fn default_legacy() -> Self {
        Self(DEFAULT_PARTITION.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionBinding {
    tenant: TenantId,
    partition: PartitionId,
}

impl PartitionBinding {
    pub fn new(tenant: TenantId, partition: PartitionId) -> Self {
        Self { tenant, partition }
    }

    #[allow(dead_code)]
    pub fn legacy_default() -> Self {
        Self::new(TenantId::default_legacy(), PartitionId::default_legacy())
    }

    pub fn legacy_tenant(partition: PartitionId) -> Self {
        Self::new(TenantId::default_legacy(), partition)
    }

    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionScope {
    tenant: TenantId,
    partitions: Vec<PartitionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedRecordKey {
    binding: PartitionBinding,
    logical_key: Bytes,
}

impl PartitionedRecordKey {
    pub fn binding(&self) -> &PartitionBinding {
        &self.binding
    }

    pub fn logical_key(&self) -> &[u8] {
        &self.logical_key
    }
}

impl PartitionScope {
    #[allow(dead_code)]
    pub fn new(
        tenant: TenantId,
        partitions: impl IntoIterator<Item = PartitionId>,
    ) -> Result<Self, PartitionScopeError> {
        let partitions = partitions.into_iter().collect::<BTreeSet<_>>();
        if partitions.is_empty() {
            return Err(PartitionScopeError::Empty);
        }

        Ok(Self {
            tenant,
            partitions: partitions.into_iter().collect(),
        })
    }

    pub fn single(binding: PartitionBinding) -> Self {
        Self {
            tenant: binding.tenant,
            partitions: vec![binding.partition],
        }
    }

    #[allow(dead_code)]
    pub fn legacy_default() -> Self {
        Self::single(PartitionBinding::legacy_default())
    }

    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn partitions(&self) -> &[PartitionId] {
        &self.partitions
    }

    pub fn contains(&self, partition: &PartitionId) -> bool {
        self.partitions.binary_search(partition).is_ok()
    }

    pub fn contains_binding(&self, binding: &PartitionBinding) -> bool {
        self.tenant == *binding.tenant() && self.contains(binding.partition())
    }
}

pub fn encode_record_key(
    binding: &PartitionBinding,
    logical_key: impl AsRef<[u8]>,
) -> Result<Bytes, PartitionKeyError> {
    let logical_key = logical_key.as_ref();
    if logical_key.is_empty() {
        return Err(PartitionKeyError::EmptyLogicalKey);
    }

    let mut out = Vec::with_capacity(
        PARTITIONED_RECORD_PREFIX.len()
            + binding.tenant.as_str().len()
            + binding.partition.as_str().len()
            + logical_key.len()
            + 2,
    );
    out.extend_from_slice(PARTITIONED_RECORD_PREFIX);
    out.extend_from_slice(binding.tenant.as_str().as_bytes());
    out.push(b':');
    out.extend_from_slice(binding.partition.as_str().as_bytes());
    out.push(b':');
    out.extend_from_slice(logical_key);
    Ok(Bytes::from(out))
}

pub fn decode_record_key(key: &[u8]) -> Result<Option<PartitionedRecordKey>, PartitionKeyError> {
    let Some(rest) = key.strip_prefix(PARTITIONED_RECORD_PREFIX) else {
        return Ok(None);
    };

    let first = rest
        .iter()
        .position(|b| *b == b':')
        .ok_or(PartitionKeyError::Malformed)?;
    let second = rest[first + 1..]
        .iter()
        .position(|b| *b == b':')
        .map(|pos| first + 1 + pos)
        .ok_or(PartitionKeyError::Malformed)?;

    let tenant = std::str::from_utf8(&rest[..first]).map_err(|_| PartitionKeyError::InvalidUtf8)?;
    let partition = std::str::from_utf8(&rest[first + 1..second])
        .map_err(|_| PartitionKeyError::InvalidUtf8)?;
    let logical_key = &rest[second + 1..];
    if logical_key.is_empty() {
        return Err(PartitionKeyError::EmptyLogicalKey);
    }

    Ok(Some(PartitionedRecordKey {
        binding: PartitionBinding::new(
            TenantId::new(tenant).map_err(PartitionKeyError::InvalidId)?,
            PartitionId::new(partition).map_err(PartitionKeyError::InvalidId)?,
        ),
        logical_key: Bytes::copy_from_slice(logical_key),
    }))
}

fn validate_id(value: &str) -> Result<String, PartitionIdError> {
    if value.is_empty() {
        return Err(PartitionIdError::Empty);
    }
    if value.len() > MAX_ID_LEN {
        return Err(PartitionIdError::TooLong { max: MAX_ID_LEN });
    }
    if let Some(ch) = value
        .chars()
        .find(|ch| !ch.is_ascii_alphanumeric() && !matches!(ch, '-' | '_' | '.'))
    {
        return Err(PartitionIdError::InvalidChar { ch });
    }

    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_id_rejects_empty_values() {
        assert_eq!(PartitionId::new("").unwrap_err(), PartitionIdError::Empty);
    }

    #[test]
    fn partition_id_rejects_path_and_key_separators() {
        assert!(matches!(
            PartitionId::new("finance/q4").unwrap_err(),
            PartitionIdError::InvalidChar { ch: '/' }
        ));
        assert!(matches!(
            TenantId::new("acme:corp").unwrap_err(),
            PartitionIdError::InvalidChar { ch: ':' }
        ));
    }

    #[test]
    fn partition_scope_is_non_empty_and_deduplicated() {
        let tenant = TenantId::new("acme").unwrap();
        let scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("tech").unwrap(),
                PartitionId::new("finance").unwrap(),
                PartitionId::new("tech").unwrap(),
            ],
        )
        .unwrap();

        assert_eq!(
            scope.partitions(),
            &[
                PartitionId::new("finance").unwrap(),
                PartitionId::new("tech").unwrap()
            ]
        );
    }

    #[test]
    fn partition_scope_rejects_empty_sets() {
        let tenant = TenantId::new("acme").unwrap();
        assert_eq!(
            PartitionScope::new(tenant, []).unwrap_err(),
            PartitionScopeError::Empty
        );
    }

    #[test]
    fn legacy_default_scope_is_available() {
        let scope = PartitionScope::legacy_default();
        assert_eq!(scope.tenant().as_str(), "default");
        assert_eq!(scope.partitions()[0].as_str(), "default");
    }

    #[test]
    fn binding_exposes_write_target() {
        let binding = PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new("finance").unwrap(),
        );

        assert_eq!(binding.tenant().as_str(), "acme");
        assert_eq!(binding.partition().as_str(), "finance");
    }

    #[test]
    fn scope_checks_membership() {
        let scope = PartitionScope::new(
            TenantId::new("acme").unwrap(),
            [
                PartitionId::new("product").unwrap(),
                PartitionId::new("tech").unwrap(),
            ],
        )
        .unwrap();

        assert!(scope.contains(&PartitionId::new("product").unwrap()));
        assert!(!scope.contains(&PartitionId::new("finance").unwrap()));
    }

    #[test]
    fn partitioned_record_key_is_distinct_from_legacy_memory_key() {
        let binding = PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new("finance").unwrap(),
        );

        let key =
            encode_record_key(&binding, b"memory:00000000-0000-0000-0000-000000000001").unwrap();

        assert!(key.starts_with(PARTITIONED_RECORD_PREFIX));
        assert!(!key.starts_with(b"memory:"));
    }

    #[test]
    fn partitioned_record_key_roundtrips_logical_keys_containing_colons() {
        let binding = PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new("product").unwrap(),
        );
        let logical = b"memory:00000000-0000-0000-0000-000000000001";

        let encoded = encode_record_key(&binding, logical).unwrap();
        let decoded = decode_record_key(&encoded).unwrap().unwrap();

        assert_eq!(decoded.binding().tenant().as_str(), "acme");
        assert_eq!(decoded.binding().partition().as_str(), "product");
        assert_eq!(decoded.logical_key(), logical);
    }

    #[test]
    fn decode_record_key_returns_none_for_legacy_keys() {
        assert!(
            decode_record_key(b"memory:00000000-0000-0000-0000-000000000001")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn partitioned_record_key_rejects_empty_logical_key() {
        let binding = PartitionBinding::legacy_default();
        assert_eq!(
            encode_record_key(&binding, b"").unwrap_err(),
            PartitionKeyError::EmptyLogicalKey
        );
        assert_eq!(
            decode_record_key(b"partition:default:default:").unwrap_err(),
            PartitionKeyError::EmptyLogicalKey
        );
    }

    #[test]
    fn decode_record_key_rejects_malformed_partitioned_keys() {
        assert_eq!(
            decode_record_key(b"partition:default").unwrap_err(),
            PartitionKeyError::Malformed
        );
        assert!(matches!(
            decode_record_key(b"partition:bad/id:default:memory:x").unwrap_err(),
            PartitionKeyError::InvalidId(PartitionIdError::InvalidChar { ch: '/' })
        ));
    }
}
