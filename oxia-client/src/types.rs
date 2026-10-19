//! Public domain types returned by client operations.

use crate::errors::OxiaError;
use crate::key::KeyOrder;
use crate::proto;
use bytes::Bytes;
use std::cmp::Ordering;
use std::fmt;

/// Metadata about the state of a record.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Version {
    /// The unique identifier of this version of the record. It changes on every
    /// modification and is the value to pass to
    /// [`expected_version_id`](crate::PutBuilder::expected_version_id) for
    /// compare-and-swap operations.
    pub version_id: i64,
    /// The number of modifications made to the record since it was created.
    pub modifications_count: i64,
    /// The creation timestamp of the record, in milliseconds since the epoch.
    pub created_timestamp: u64,
    /// The timestamp of the latest modification, in milliseconds since the epoch.
    pub modified_timestamp: u64,
    /// The identifier of the owning session, when the record is ephemeral.
    pub session_id: Option<i64>,
    /// The identity of the client that last modified an ephemeral record.
    pub client_identity: Option<String>,
}

impl Version {
    /// Whether the record is ephemeral (bound to a client session).
    pub fn is_ephemeral(&self) -> bool {
        self.session_id.is_some()
    }
}

impl From<proto::Version> for Version {
    fn from(v: proto::Version) -> Self {
        Version {
            version_id: v.version_id,
            modifications_count: v.modifications_count,
            created_timestamp: v.created_timestamp,
            modified_timestamp: v.modified_timestamp,
            session_id: v.session_id,
            client_identity: v.client_identity,
        }
    }
}

/// The key comparison applied by a [`get`](crate::OxiaClient::get) operation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ComparisonType {
    /// The stored key must be equal to the requested key.
    #[default]
    Equal,
    /// Return the record with the highest key `<=` the requested key.
    Floor,
    /// Return the record with the lowest key `>=` the requested key.
    Ceiling,
    /// Return the record with the highest key `<` the requested key.
    Lower,
    /// Return the record with the lowest key `>` the requested key.
    Higher,
}

impl ComparisonType {
    pub(crate) fn to_proto(self) -> proto::KeyComparisonType {
        match self {
            ComparisonType::Equal => proto::KeyComparisonType::Equal,
            ComparisonType::Floor => proto::KeyComparisonType::Floor,
            ComparisonType::Ceiling => proto::KeyComparisonType::Ceiling,
            ComparisonType::Lower => proto::KeyComparisonType::Lower,
            ComparisonType::Higher => proto::KeyComparisonType::Higher,
        }
    }
}

/// The result of a successful [`put`](crate::OxiaClient::put).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PutResult {
    /// The key of the stored record. Differs from the requested key when the
    /// server generated it (sequential keys).
    pub key: String,
    /// The version of the newly written record.
    pub version: Version,
}

/// A record returned by [`get`](crate::OxiaClient::get) or
/// [`range_scan`](crate::OxiaClient::range_scan).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct GetResult {
    /// The key of the record. Differs from the requested key for non-`Equal`
    /// comparisons (e.g. floor/ceiling queries).
    pub key: String,
    /// The value, unless the record has none or the query excluded it with
    /// [`include_value(false)`](crate::GetBuilder::include_value).
    pub value: Option<Bytes>,
    /// The version of the record.
    pub version: Version,
    /// The matching secondary-index key, when the query used
    /// [`use_index`](crate::GetBuilder::use_index).
    pub secondary_index_key: Option<String>,
}

impl GetResult {
    /// Converts a wire response. The server omits the key for exact-match gets
    /// (it equals the request key, passed as `fallback_key`); scan records must
    /// always carry one.
    pub(crate) fn from_proto(
        response: proto::GetResponse,
        fallback_key: Option<String>,
    ) -> Result<Self, OxiaError> {
        let version = response
            .version
            .ok_or_else(|| OxiaError::Decode("missing version in get response".to_string()))?;
        let key = response
            .key
            .or(fallback_key)
            .ok_or_else(|| OxiaError::Decode("missing key in record".to_string()))?;
        Ok(GetResult {
            key,
            value: response.value,
            version: version.into(),
            secondary_index_key: response.secondary_index_key,
        })
    }

    pub(crate) fn compare_keys(&self, other: &Self, order: KeyOrder) -> Ordering {
        order.compare(&self.key, &other.key)
    }
}

/// A change notification streamed from
/// [`OxiaClient::notifications`](crate::OxiaClient::notifications).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Notification {
    /// A new record was created.
    KeyCreated {
        /// The key of the created record.
        key: String,
        /// The version id of the created record.
        version_id: Option<i64>,
    },
    /// An existing record was modified.
    KeyModified {
        /// The key of the modified record.
        key: String,
        /// The new version id of the record.
        version_id: Option<i64>,
    },
    /// A record was deleted.
    KeyDeleted {
        /// The key of the deleted record.
        key: String,
    },
    /// A range of records was deleted.
    KeyRangeDeleted {
        /// The start of the deleted range (inclusive).
        key: String,
        /// The end of the deleted range (exclusive). An empty end covers all
        /// the records from `key` on.
        key_range_last: Option<String>,
    },
}

impl Notification {
    /// Converts a wire notification entry; `None` for types this client version
    /// does not recognize (they are skipped).
    pub(crate) fn from_proto(entry: proto::NotificationEntry) -> Option<Self> {
        let key = entry.key.unwrap_or_default();
        let notification = entry.value?;
        match proto::NotificationType::try_from(notification.r#type).ok()? {
            proto::NotificationType::KeyCreated => Some(Notification::KeyCreated {
                key,
                version_id: notification.version_id,
            }),
            proto::NotificationType::KeyModified => Some(Notification::KeyModified {
                key,
                version_id: notification.version_id,
            }),
            proto::NotificationType::KeyDeleted => Some(Notification::KeyDeleted { key }),
            proto::NotificationType::KeyRangeDeleted => Some(Notification::KeyRangeDeleted {
                key,
                key_range_last: notification.key_range_last,
            }),
        }
    }

    /// The key the notification refers to (the range start for
    /// [`Notification::KeyRangeDeleted`]).
    pub fn key(&self) -> &str {
        match self {
            Notification::KeyCreated { key, .. }
            | Notification::KeyModified { key, .. }
            | Notification::KeyDeleted { key }
            | Notification::KeyRangeDeleted { key, .. } => key,
        }
    }
}

impl fmt::Display for Notification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Notification::KeyCreated { key, version_id } => {
                write!(f, "KeyCreated(key={key}, version_id={version_id:?})")
            }
            Notification::KeyModified { key, version_id } => {
                write!(f, "KeyModified(key={key}, version_id={version_id:?})")
            }
            Notification::KeyDeleted { key } => write!(f, "KeyDeleted(key={key})"),
            Notification::KeyRangeDeleted {
                key,
                key_range_last,
            } => {
                write!(f, "KeyRangeDeleted(key={key}, last={key_range_last:?})")
            }
        }
    }
}

/// The version id that a conditional operation compares against to assert that
/// the record does not exist.
pub(crate) const VERSION_ID_NOT_EXISTS: i64 = -1;
