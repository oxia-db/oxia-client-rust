//! Fluent request builders: every [`OxiaClient`] operation returns one of
//! these; chain options onto it and `.await` it (they implement
//! [`IntoFuture`]), or call an alternate finisher such as
//! [`ListBuilder::stream`].

use crate::batcher::WriteOp;
use crate::client::{OxiaClient, check_status};
use crate::errors::OxiaError;
use crate::proto;
use crate::streams::{ListStream, Notifications, RangeScanStream, SequenceUpdates};
use crate::types::{ComparisonType, GetResult, PutResult, VERSION_ID_NOT_EXISTS};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use std::future::IntoFuture;

const DEFAULT_SUBSCRIPTION_BUFFER: usize = 100;

/// A pending `put`, created with [`OxiaClient::put`]. Chain options and
/// `.await` it.
#[derive(Debug)]
#[must_use = "builders do nothing unless awaited"]
pub struct PutBuilder {
    client: OxiaClient,
    key: String,
    value: Bytes,
    expected_version_id: Option<i64>,
    partition_key: Option<String>,
    sequence_key_deltas: Vec<u64>,
    secondary_indexes: Vec<proto::SecondaryIndex>,
    ephemeral: bool,
}

impl PutBuilder {
    pub(crate) fn new(client: OxiaClient, key: String, value: Bytes) -> Self {
        PutBuilder {
            client,
            key,
            value,
            expected_version_id: None,
            partition_key: None,
            sequence_key_deltas: Vec::new(),
            secondary_indexes: Vec::new(),
            ephemeral: false,
        }
    }

    /// Makes the put conditional: it succeeds only if the record's current
    /// version id matches (compare-and-swap). Fails with
    /// [`OxiaError::UnexpectedVersionId`] otherwise.
    pub fn expected_version_id(mut self, version_id: i64) -> Self {
        self.expected_version_id = Some(version_id);
        self
    }

    /// Makes the put conditional on the record not existing yet. Fails with
    /// [`OxiaError::UnexpectedVersionId`] if it does.
    pub fn expected_record_not_exists(mut self) -> Self {
        self.expected_version_id = Some(VERSION_ID_NOT_EXISTS);
        self
    }

    /// Routes (and co-locates) the record by `partition_key` instead of by its
    /// own key. Records sharing a partition key always live on the same shard.
    pub fn partition_key(mut self, partition_key: impl Into<String>) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    /// Turns the key into a server-generated sequential key: each delta is
    /// added to the current highest sequence and appended as a zero-padded
    /// suffix. When the highest key of the sequence has more suffixes than
    /// there are deltas, the missing deltas are 0 on the servers with
    /// `FEATURE_SEQUENCE_KEY_VALIDATION`.
    ///
    /// Requires [`partition_key`](PutBuilder::partition_key); every delta must
    /// be greater than zero; and it cannot be combined with
    /// [`expected_version_id`](PutBuilder::expected_version_id). Awaiting a put
    /// that violates any of these fails with [`OxiaError::InvalidArgument`].
    pub fn sequence_key_deltas(mut self, deltas: impl IntoIterator<Item = u64>) -> Self {
        self.sequence_key_deltas = deltas.into_iter().collect();
        self
    }

    /// Additionally indexes the record under `secondary_key` in the named
    /// secondary index. May be called multiple times.
    ///
    /// The index name must not contain `/` (the secondary key may). Awaiting a
    /// put with such a name fails with [`OxiaError::InvalidArgument`].
    pub fn secondary_index(
        mut self,
        index_name: impl Into<String>,
        secondary_key: impl Into<String>,
    ) -> Self {
        self.secondary_indexes.push(proto::SecondaryIndex {
            index_name: index_name.into(),
            secondary_key: secondary_key.into(),
        });
        self
    }

    /// Makes the record ephemeral: it is automatically deleted when this
    /// client's session closes or expires.
    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }

    async fn execute(self) -> Result<PutResult, OxiaError> {
        let client = self.client;
        client.ensure_open()?;
        // Reject secondary index names containing '/' up front (matching the
        // Go client): the server stores index entries under
        // `__oxia/idx/{index_name}/{secondary_key}`, where such a name is
        // ambiguous, and servers that predate the check accept it silently.
        if let Some(index) = self
            .secondary_indexes
            .iter()
            .find(|index| index.index_name.contains('/'))
        {
            return Err(OxiaError::InvalidArgument(format!(
                "secondary index name {:?} must not contain '/'",
                index.index_name
            )));
        }
        // Validate sequence-key puts up front (matching the Java client's
        // `PutOperation`): sequential keys require a partition key, are
        // incompatible with an expected version, and every delta must be
        // positive. Negative deltas are already impossible (`u64`).
        if !self.sequence_key_deltas.is_empty() {
            if self.partition_key.is_none() {
                return Err(OxiaError::InvalidArgument(
                    "sequence_key_deltas requires a partition_key".to_string(),
                ));
            }
            if self.expected_version_id.is_some() {
                return Err(OxiaError::InvalidArgument(
                    "sequence_key_deltas cannot be combined with expected_version_id".to_string(),
                ));
            }
            if self.sequence_key_deltas.contains(&0) {
                return Err(OxiaError::InvalidArgument(
                    "every sequence delta must be greater than zero".to_string(),
                ));
            }
        }
        let routing_key = self
            .partition_key
            .as_deref()
            .unwrap_or(&self.key)
            .to_string();
        let PutBuilder {
            key,
            value,
            expected_version_id,
            partition_key,
            sequence_key_deltas,
            secondary_indexes,
            ephemeral,
            ..
        } = self;
        let identity = client.identity().to_string();
        let value_size = value.len();
        let start = std::time::Instant::now();
        // Re-resolve the shard and (for ephemeral puts) its session on each
        // attempt, so a split/merge re-routes to the new shard.
        let op_result = async {
            let response = client
                .with_shard_retry(&routing_key, |shard| {
                    let client = client.clone();
                    let key = key.clone();
                    let value = value.clone();
                    let partition_key = partition_key.clone();
                    let sequence_key_deltas = sequence_key_deltas.clone();
                    let secondary_indexes = secondary_indexes.clone();
                    let identity = identity.clone();
                    async move {
                        let session_id = if ephemeral {
                            Some(client.session_id_for(shard).await?)
                        } else {
                            None
                        };
                        let request = proto::PutRequest {
                            key,
                            value,
                            expected_version_id,
                            session_id,
                            client_identity: Some(identity),
                            partition_key,
                            sequence_key_delta: sequence_key_deltas,
                            secondary_indexes,
                            override_version_id: None,
                            override_modifications_count: None,
                            // Assigned by the write batcher.
                            op_index: 0,
                        };
                        client.submit_write(shard, request, WriteOp::Put).await
                    }
                })
                .await?;
            check_status(response.status)?;
            let version = response
                .version
                .ok_or_else(|| OxiaError::Decode("missing version in put response".to_string()))?;
            Ok(PutResult {
                // The server returns the key only when it generated it
                // (sequential keys).
                key: response.key.unwrap_or(key),
                version: version.into(),
            })
        }
        .await;
        client
            .metrics()
            .record_op("put", start.elapsed(), Some(value_size), op_result.is_ok());
        op_result
    }
}

impl IntoFuture for PutBuilder {
    type Output = Result<PutResult, OxiaError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}

/// A pending `get`, created with [`OxiaClient::get`]. Chain options and
/// `.await` it.
#[derive(Debug)]
#[must_use = "builders do nothing unless awaited"]
pub struct GetBuilder {
    client: OxiaClient,
    key: String,
    comparison: ComparisonType,
    partition_key: Option<String>,
    include_value: bool,
    secondary_index_name: Option<String>,
}

impl GetBuilder {
    pub(crate) fn new(client: OxiaClient, key: String) -> Self {
        GetBuilder {
            client,
            key,
            comparison: ComparisonType::Equal,
            partition_key: None,
            include_value: true,
            secondary_index_name: None,
        }
    }

    /// Applies a non-exact key comparison (floor/ceiling/lower/higher); the
    /// returned record's key may then differ from the requested key.
    pub fn comparison(mut self, comparison: ComparisonType) -> Self {
        self.comparison = comparison;
        self
    }

    /// Restricts the query to the shard owning `partition_key`. Required to
    /// read records that were written with a partition key.
    pub fn partition_key(mut self, partition_key: impl Into<String>) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    /// Excludes the value from the response (metadata-only query) when `false`.
    /// Defaults to `true`.
    pub fn include_value(mut self, include_value: bool) -> Self {
        self.include_value = include_value;
        self
    }

    /// Queries the named secondary index instead of the primary key space.
    /// Index names must not contain `/`.
    pub fn use_index(mut self, index_name: impl Into<String>) -> Self {
        self.secondary_index_name = Some(index_name.into());
        self
    }

    async fn execute(self) -> Result<GetResult, OxiaError> {
        let client = self.client;
        client.ensure_open()?;
        let single_shard = is_single_shard_get(
            self.partition_key.is_some(),
            self.comparison,
            self.secondary_index_name.is_some(),
        );
        let request = proto::GetRequest {
            key: self.key.clone(),
            include_value: self.include_value,
            comparison_type: self.comparison.to_proto() as i32,
            secondary_index_name: self.secondary_index_name,
        };
        let start = std::time::Instant::now();
        let op_result: Result<GetResult, OxiaError> = async {
            if single_shard {
                let route_key = self.partition_key.unwrap_or_else(|| self.key.clone());
                let response = client
                    .with_shard_retry(&route_key, |shard| {
                        let client = client.clone();
                        let request = request.clone();
                        async move { client.submit_get(shard, request).await }
                    })
                    .await?;
                check_status(response.status)?;
                GetResult::from_proto(response, Some(self.key))
            } else {
                client.broadcast_get(request, self.comparison).await
            }
        }
        .await;
        let value_size = op_result
            .as_ref()
            .ok()
            .and_then(|r| r.value.as_ref().map(|v| v.len()));
        client
            .metrics()
            .record_op("get", start.elapsed(), value_size, op_result.is_ok());
        op_result
    }
}

impl IntoFuture for GetBuilder {
    type Output = Result<GetResult, OxiaError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}

/// Whether a get is pinned to a single shard (routed by partition key, or by
/// the primary key for an exact match) rather than broadcast to every shard.
///
/// A partition key always pins the query. Without one, only an exact
/// ([`Equal`](ComparisonType::Equal)) primary-key lookup owns a single shard;
/// an inequality comparison (its nearest match may sit on any shard) or a
/// secondary-index lookup (indexed independently on each shard) must fan out.
fn is_single_shard_get(
    has_partition_key: bool,
    comparison: ComparisonType,
    has_secondary_index: bool,
) -> bool {
    has_partition_key || (comparison == ComparisonType::Equal && !has_secondary_index)
}

/// A pending `delete`, created with [`OxiaClient::delete`]. Chain options and
/// `.await` it.
#[derive(Debug)]
#[must_use = "builders do nothing unless awaited"]
pub struct DeleteBuilder {
    client: OxiaClient,
    key: String,
    expected_version_id: Option<i64>,
    partition_key: Option<String>,
}

impl DeleteBuilder {
    pub(crate) fn new(client: OxiaClient, key: String) -> Self {
        DeleteBuilder {
            client,
            key,
            expected_version_id: None,
            partition_key: None,
        }
    }

    /// Makes the delete conditional: it succeeds only if the record's current
    /// version id matches. Fails with [`OxiaError::UnexpectedVersionId`]
    /// otherwise.
    pub fn expected_version_id(mut self, version_id: i64) -> Self {
        self.expected_version_id = Some(version_id);
        self
    }

    /// Routes the delete to the shard owning `partition_key`. Required for
    /// records that were written with a partition key.
    pub fn partition_key(mut self, partition_key: impl Into<String>) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    async fn execute(self) -> Result<(), OxiaError> {
        let client = self.client;
        client.ensure_open()?;
        let routing_key = self
            .partition_key
            .as_deref()
            .unwrap_or(&self.key)
            .to_string();
        let request = proto::DeleteRequest {
            key: self.key,
            expected_version_id: self.expected_version_id,
            partition_key: self.partition_key,
            // Assigned by the write batcher.
            op_index: 0,
        };
        let start = std::time::Instant::now();
        let op_result = async {
            let response = client
                .with_shard_retry(&routing_key, |shard| {
                    let client = client.clone();
                    let request = request.clone();
                    async move { client.submit_write(shard, request, WriteOp::Delete).await }
                })
                .await?;
            check_status(response.status)
        }
        .await;
        client
            .metrics()
            .record_op("delete", start.elapsed(), None, op_result.is_ok());
        op_result
    }
}

impl IntoFuture for DeleteBuilder {
    type Output = Result<(), OxiaError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}

/// A pending `delete_range`, created with [`OxiaClient::delete_range`]. Chain
/// options and `.await` it.
#[derive(Debug)]
#[must_use = "builders do nothing unless awaited"]
pub struct DeleteRangeBuilder {
    client: OxiaClient,
    min_key_inclusive: String,
    max_key_exclusive: String,
    partition_key: Option<String>,
}

impl DeleteRangeBuilder {
    pub(crate) fn new(
        client: OxiaClient,
        min_key_inclusive: String,
        max_key_exclusive: String,
    ) -> Self {
        DeleteRangeBuilder {
            client,
            min_key_inclusive,
            max_key_exclusive,
            partition_key: None,
        }
    }

    /// Restricts the range deletion to the shard owning `partition_key`
    /// instead of applying it on every shard.
    pub fn partition_key(mut self, partition_key: impl Into<String>) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    async fn execute(self) -> Result<(), OxiaError> {
        let client = self.client;
        client.ensure_open()?;
        let request = proto::DeleteRangeRequest {
            start_inclusive: self.min_key_inclusive,
            end_exclusive: self.max_key_exclusive,
            // Assigned by the write batcher.
            op_index: 0,
        };
        let start = std::time::Instant::now();
        let op_result = async {
            match self.partition_key {
                Some(partition_key) => {
                    let response = client
                        .with_shard_retry(&partition_key, |shard| {
                            let client = client.clone();
                            let request = request.clone();
                            async move {
                                client
                                    .submit_write(shard, request, WriteOp::DeleteRange)
                                    .await
                            }
                        })
                        .await?;
                    check_status(response.status)
                }
                None => client.broadcast_delete_range(request).await,
            }
        }
        .await;
        client
            .metrics()
            .record_op("delete_range", start.elapsed(), None, op_result.is_ok());
        op_result
    }
}

impl IntoFuture for DeleteRangeBuilder {
    type Output = Result<(), OxiaError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}

/// A pending `list`, created with [`OxiaClient::list`].
///
/// `.await` it for all matching keys at once (bounded by the request timeout),
/// or call [`stream`](ListBuilder::stream) for an incremental, ordered
/// [`ListStream`] without an overall deadline.
#[derive(Debug)]
#[must_use = "builders do nothing unless awaited"]
pub struct ListBuilder {
    client: OxiaClient,
    min_key_inclusive: String,
    max_key_exclusive: String,
    partition_key: Option<String>,
    secondary_index_name: Option<String>,
    include_internal_keys: bool,
    end_inclusive: bool,
}

impl ListBuilder {
    pub(crate) fn new(
        client: OxiaClient,
        min_key_inclusive: String,
        max_key_exclusive: String,
    ) -> Self {
        ListBuilder {
            client,
            min_key_inclusive,
            max_key_exclusive,
            partition_key: None,
            secondary_index_name: None,
            include_internal_keys: false,
            end_inclusive: false,
        }
    }

    /// Restricts the listing to the shard owning `partition_key`.
    pub fn partition_key(mut self, partition_key: impl Into<String>) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    /// Lists keys from the named secondary index instead of the primary key
    /// space. Index names must not contain `/`.
    pub fn use_index(mut self, index_name: impl Into<String>) -> Self {
        self.secondary_index_name = Some(index_name.into());
        self
    }

    /// Also returns Oxia's internal keys (hidden by default).
    pub fn include_internal_keys(mut self, include: bool) -> Self {
        self.include_internal_keys = include;
        self
    }

    /// Includes the end of the range: the key `max_key_exclusive`, or with
    /// [`use_index`](ListBuilder::use_index), the records whose secondary key
    /// is `max_key_exclusive`. An empty end is then the empty key rather than
    /// no bound, and with an index, a range from a key to the same key lists
    /// the records whose secondary key is that key.
    ///
    /// The leader of a shard includes the end once all the servers of the
    /// shard support it. Until then, the list fails with
    /// [`OxiaError::EndInclusiveUnsupported`] rather than leave the end out.
    pub fn end_inclusive(mut self) -> Self {
        self.end_inclusive = true;
        self
    }

    fn into_request(self) -> (OxiaClient, Option<String>, proto::ListRequest) {
        let request = proto::ListRequest {
            shard: None,
            start_inclusive: self.min_key_inclusive,
            end_exclusive: self.max_key_exclusive,
            secondary_index_name: self.secondary_index_name,
            include_internal_keys: self.include_internal_keys,
            end_inclusive: self.end_inclusive,
        };
        (self.client, self.partition_key, request)
    }

    /// Opens an incremental, ordered stream of the matching keys.
    pub async fn stream(self) -> Result<ListStream, OxiaError> {
        let (client, partition_key, request) = self.into_request();
        client
            .open_list_stream(request, partition_key.as_deref())
            .await
    }

    async fn execute(self) -> Result<Vec<String>, OxiaError> {
        let timeout = self.client.request_timeout();
        let stream = self.stream();
        tokio::time::timeout(timeout, async move { stream.await?.try_collect().await })
            .await
            .map_err(|_| OxiaError::Timeout)?
    }
}

impl IntoFuture for ListBuilder {
    type Output = Result<Vec<String>, OxiaError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}

/// A pending `range_scan`, created with [`OxiaClient::range_scan`].
///
/// `.await` it for all matching records at once (bounded by the request
/// timeout), or call [`stream`](RangeScanBuilder::stream) for an incremental,
/// ordered [`RangeScanStream`] with bounded memory and no overall deadline.
#[derive(Debug)]
#[must_use = "builders do nothing unless awaited"]
pub struct RangeScanBuilder {
    client: OxiaClient,
    min_key_inclusive: String,
    max_key_exclusive: String,
    partition_key: Option<String>,
    secondary_index_name: Option<String>,
    include_internal_keys: bool,
    end_inclusive: bool,
}

impl RangeScanBuilder {
    pub(crate) fn new(
        client: OxiaClient,
        min_key_inclusive: String,
        max_key_exclusive: String,
    ) -> Self {
        RangeScanBuilder {
            client,
            min_key_inclusive,
            max_key_exclusive,
            partition_key: None,
            secondary_index_name: None,
            include_internal_keys: false,
            end_inclusive: false,
        }
    }

    /// Restricts the scan to the shard owning `partition_key`.
    pub fn partition_key(mut self, partition_key: impl Into<String>) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    /// Scans the named secondary index instead of the primary key space.
    /// Index names must not contain `/`.
    pub fn use_index(mut self, index_name: impl Into<String>) -> Self {
        self.secondary_index_name = Some(index_name.into());
        self
    }

    /// Also returns Oxia's internal keys (hidden by default).
    pub fn include_internal_keys(mut self, include: bool) -> Self {
        self.include_internal_keys = include;
        self
    }

    /// Includes the end of the range: the record whose key is
    /// `max_key_exclusive`, or with [`use_index`](RangeScanBuilder::use_index),
    /// the records whose secondary key is `max_key_exclusive`. An empty end is
    /// then the empty key rather than no bound, and with an index, a range
    /// from a key to the same key scans the records whose secondary key is
    /// that key.
    ///
    /// The leader of a shard includes the end once all the servers of the
    /// shard support it. Until then, the scan fails with
    /// [`OxiaError::EndInclusiveUnsupported`] rather than leave the end out.
    pub fn end_inclusive(mut self) -> Self {
        self.end_inclusive = true;
        self
    }

    fn into_request(self) -> (OxiaClient, Option<String>, proto::RangeScanRequest) {
        let request = proto::RangeScanRequest {
            shard: None,
            start_inclusive: self.min_key_inclusive,
            end_exclusive: self.max_key_exclusive,
            secondary_index_name: self.secondary_index_name,
            include_internal_keys: self.include_internal_keys,
            end_inclusive: self.end_inclusive,
        };
        (self.client, self.partition_key, request)
    }

    /// Opens an incremental, ordered stream of the matching records.
    pub async fn stream(self) -> Result<RangeScanStream, OxiaError> {
        let (client, partition_key, request) = self.into_request();
        client
            .open_range_scan_stream(request, partition_key.as_deref())
            .await
    }

    async fn execute(self) -> Result<Vec<GetResult>, OxiaError> {
        let timeout = self.client.request_timeout();
        let stream = self.stream();
        tokio::time::timeout(timeout, async move { stream.await?.try_collect().await })
            .await
            .map_err(|_| OxiaError::Timeout)?
    }
}

impl IntoFuture for RangeScanBuilder {
    type Output = Result<Vec<GetResult>, OxiaError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}

/// A pending notifications subscription, created with
/// [`OxiaClient::notifications`]. `.await` it to obtain the
/// [`Notifications`] stream.
#[derive(Debug)]
#[must_use = "builders do nothing unless awaited"]
pub struct NotificationsBuilder {
    client: OxiaClient,
    buffer_size: usize,
}

impl NotificationsBuilder {
    pub(crate) fn new(client: OxiaClient) -> Self {
        NotificationsBuilder {
            client,
            buffer_size: DEFAULT_SUBSCRIPTION_BUFFER,
        }
    }

    /// Sets the subscription's channel capacity (default 100). When the buffer
    /// is full, delivery applies backpressure to the per-shard listeners.
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }
}

impl IntoFuture for NotificationsBuilder {
    type Output = Result<Notifications, OxiaError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.client.open_notifications(self.buffer_size).await })
    }
}

/// A pending sequence-updates subscription, created with
/// [`OxiaClient::sequence_updates`]. `.await` it to obtain the
/// [`SequenceUpdates`] stream.
#[derive(Debug)]
#[must_use = "builders do nothing unless awaited"]
pub struct SequenceUpdatesBuilder {
    client: OxiaClient,
    key: String,
    partition_key: String,
    buffer_size: usize,
}

impl SequenceUpdatesBuilder {
    pub(crate) fn new(client: OxiaClient, key: String, partition_key: String) -> Self {
        SequenceUpdatesBuilder {
            client,
            key,
            partition_key,
            buffer_size: DEFAULT_SUBSCRIPTION_BUFFER,
        }
    }

    /// Sets the subscription's channel capacity (default 100).
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }
}

impl IntoFuture for SequenceUpdatesBuilder {
    type Output = Result<SequenceUpdates, OxiaError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.client
                .open_sequence_updates(self.key, self.partition_key, self.buffer_size)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_primary_key_get_is_single_shard() {
        // The common case: exact get by key, no partition key, no index.
        assert!(is_single_shard_get(false, ComparisonType::Equal, false));
    }

    #[test]
    fn a_partition_key_always_pins_the_shard() {
        for comparison in [
            ComparisonType::Equal,
            ComparisonType::Floor,
            ComparisonType::Ceiling,
            ComparisonType::Lower,
            ComparisonType::Higher,
        ] {
            assert!(is_single_shard_get(true, comparison, false));
            // Even a secondary-index lookup is pinned when a partition key routes it.
            assert!(is_single_shard_get(true, comparison, true));
        }
    }

    #[test]
    fn inequality_gets_without_partition_key_broadcast() {
        for comparison in [
            ComparisonType::Floor,
            ComparisonType::Ceiling,
            ComparisonType::Lower,
            ComparisonType::Higher,
        ] {
            assert!(!is_single_shard_get(false, comparison, false));
        }
    }

    #[test]
    fn secondary_index_get_without_partition_key_broadcasts() {
        // Even an exact match must fan out when it targets a secondary index.
        assert!(!is_single_shard_get(false, ComparisonType::Equal, true));
    }
}
