//! The Oxia client: construction, per-operation execution, and shutdown.

use crate::batcher::{BatcherDeps, ReadBatcher, WriteBatcher, WriteOp, pending_write};
use crate::client_builder::OxiaClientBuilder;
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use crate::key::KeyOrder;
use crate::metrics::Metrics;
use crate::notification_manager::NotificationManager;
use crate::operations::Pending;
use crate::proto;
use crate::provider_manager::ProviderManager;
use crate::requests::{
    DeleteBuilder, DeleteRangeBuilder, GetBuilder, ListBuilder, NotificationsBuilder, PutBuilder,
    RangeScanBuilder, SequenceUpdatesBuilder,
};
use crate::retry::{retry_delay, retry_until};
use crate::sequence_updates_manager::SequenceUpdatesManager;
use crate::server_error::{DecodedStatus, LeaderHint, decode_status};
use crate::session_manager::SessionManager;
use crate::shard_manager::{ShardManager, ShardManagerOptions};
use crate::streams::{
    ListStream, Notifications, RangeScanStream, SequenceUpdates, ShardStream, merged,
};
use crate::types::{ComparisonType, GetResult};
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::try_join_all;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tracing::warn;

pub(crate) struct Inner {
    pub(crate) options: OxiaClientOptions,
    pub(crate) provider_manager: Arc<ProviderManager>,
    pub(crate) shard_manager: Arc<ShardManager>,
    pub(crate) session_manager: Arc<SessionManager>,
    batcher_deps: Arc<BatcherDeps>,
    metrics: Metrics,
    notification_managers: DashMap<u64, NotificationManager>,
    sequence_managers: DashMap<u64, SequenceUpdatesManager>,
    next_subscription_id: AtomicU64,
    write_batchers: DashMap<i64, Arc<WriteBatcher>>,
    read_batchers: DashMap<i64, Arc<ReadBatcher>>,
    closed: AtomicBool,
}

/// Which registry a [`SubscriptionGuard`] cleans up.
enum SubscriptionKind {
    Notification,
    Sequence,
}

/// Held by a [`Notifications`]/[`SequenceUpdates`] handle; on drop it removes
/// the backing manager from the client, stopping its listener task(s). A [`Weak`]
/// reference means a dropped handle after the client is gone is a no-op.
pub(crate) struct SubscriptionGuard {
    inner: Weak<Inner>,
    id: u64,
    kind: SubscriptionKind,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        match self.kind {
            SubscriptionKind::Notification => {
                inner.notification_managers.remove(&self.id);
            }
            SubscriptionKind::Sequence => {
                inner.sequence_managers.remove(&self.id);
            }
        }
    }
}

/// The Oxia client.
///
/// Cheap to clone (all clones share one set of connections, batchers and
/// sessions) and safe to share across tasks. Obtain one with
/// [`OxiaClient::connect`] or [`OxiaClient::builder`]; every data operation
/// returns a request builder that is executed by `.await`ing it.
///
/// Dropping the last clone tears down all background work abruptly; call
/// [`close`](OxiaClient::close) for a graceful shutdown that flushes pending
/// batches and closes sessions.
///
/// # Cancellation
///
/// Dropping an operation's future stops *waiting* but does not recall the
/// operation: once submitted to a batch it may still execute on the server.
/// Use conditional operations (e.g.
/// [`expected_version_id`](crate::PutBuilder::expected_version_id)) when you
/// need certainty about what was applied.
#[derive(Clone)]
pub struct OxiaClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for OxiaClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OxiaClient")
            .field("namespace", &self.inner.options.namespace)
            .field("service_address", &self.inner.options.service_address)
            .finish_non_exhaustive()
    }
}

impl OxiaClient {
    /// Returns a builder for configuring and creating a client.
    pub fn builder() -> OxiaClientBuilder {
        OxiaClientBuilder::new()
    }

    /// Connects to the given service address (`host:port`) with default options.
    ///
    /// Use [`OxiaClient::builder`] to customize the namespace, timeouts, or
    /// batching before connecting.
    ///
    /// # Errors
    ///
    /// Fails fast — within the request timeout — when the cluster is
    /// unreachable ([`OxiaError::Disconnected`] / [`OxiaError::Timeout`]), or
    /// with [`OxiaError::Grpc`] when the namespace does not exist
    /// (`NotFound`) or the credentials are rejected (`Unauthenticated`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), oxia::OxiaError> {
    /// let client = oxia::OxiaClient::connect("localhost:6648").await?;
    /// # Ok(()) }
    /// ```
    pub async fn connect(service_address: impl Into<String>) -> Result<OxiaClient, OxiaError> {
        OxiaClientBuilder::new()
            .service_address(service_address)
            .build()
            .await
    }

    pub(crate) async fn new(
        options: OxiaClientOptions,
        metrics: Metrics,
    ) -> Result<OxiaClient, OxiaError> {
        let provider_manager = ProviderManager::new(options.request_timeout);
        let provider_manager = provider_manager.with_auth(options.auth.clone());
        #[cfg(feature = "tls")]
        let provider_manager =
            provider_manager.with_tls(options.tls.as_ref().map(|tls| tls.to_client_tls_config()));
        let provider_manager = Arc::new(provider_manager);
        let shard_manager = Arc::new(
            ShardManager::new(ShardManagerOptions {
                address: options.service_address.clone(),
                namespace: options.namespace.clone(),
                provider_manager: provider_manager.clone(),
                request_timeout: options.request_timeout,
            })
            .await?,
        );
        let batcher_deps = Arc::new(BatcherDeps {
            namespace: options.namespace.clone(),
            shard_manager: shard_manager.clone(),
            provider_manager: provider_manager.clone(),
            max_batch_size: options.batch_max_size as usize,
            max_requests_per_batch: options.max_requests_per_batch as usize,
            max_write_batches_in_flight: options.max_write_batches_in_flight as usize,
            max_read_batches_in_flight: options.max_read_batches_in_flight as usize,
            request_timeout: options.request_timeout,
        });
        let session_manager = Arc::new(SessionManager::new(
            options.identity.clone(),
            options.session_timeout,
            options.session_keep_alive,
            options.request_timeout,
            shard_manager.clone(),
            provider_manager.clone(),
        ));
        Ok(OxiaClient {
            inner: Arc::new(Inner {
                options,
                provider_manager,
                shard_manager,
                session_manager,
                batcher_deps,
                metrics,
                notification_managers: DashMap::new(),
                sequence_managers: DashMap::new(),
                next_subscription_id: AtomicU64::new(0),
                write_batchers: DashMap::new(),
                read_batchers: DashMap::new(),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Stores `value` under `key`, creating or overwriting the record.
    ///
    /// Returns a [`PutBuilder`]; chain options
    /// ([`ephemeral`](PutBuilder::ephemeral),
    /// [`expected_version_id`](PutBuilder::expected_version_id),
    /// [`partition_key`](PutBuilder::partition_key),
    /// [`sequence_key_deltas`](PutBuilder::sequence_key_deltas),
    /// [`secondary_index`](PutBuilder::secondary_index), …) and `.await` it.
    /// The value is `Bytes`-backed and is not copied on its way to the wire.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns:
    /// - [`OxiaError::InvalidArgument`] when
    ///   [`sequence_key_deltas`](PutBuilder::sequence_key_deltas) is used
    ///   without a partition key, with a zero delta, or together with an
    ///   expected version, or when the server rejects the put as invalid
    ///   (e.g. a secondary index name containing `/`);
    /// - [`OxiaError::UnexpectedVersionId`] when a
    ///   [`expected_version_id`](PutBuilder::expected_version_id) /
    ///   [`expected_record_not_exists`](PutBuilder::expected_record_not_exists)
    ///   condition does not hold;
    /// - [`OxiaError::SessionExpired`] when an
    ///   [`ephemeral`](PutBuilder::ephemeral) put races a session expiry
    ///   (retrying creates a fresh session);
    /// - [`OxiaError::RequestTooLarge`] when key + value exceed the maximum
    ///   batch size ([`batch_max_size`](crate::OxiaClientBuilder::batch_max_size));
    /// - [`OxiaError::Timeout`] when the request timeout elapses;
    /// - [`OxiaError::Closed`] after [`close`](OxiaClient::close);
    /// - transient routing/transport errors
    ///   ([`OxiaError::is_retryable`](crate::OxiaError::is_retryable)).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: &oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// let res = client.put("config/mode", "primary").await?;
    /// // Compare-and-swap on the returned version id:
    /// client
    ///     .put("config/mode", "standby")
    ///     .expected_version_id(res.version.version_id)
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn put(&self, key: impl Into<String>, value: impl Into<Bytes>) -> PutBuilder {
        PutBuilder::new(self.clone(), key.into(), value.into())
    }

    /// Reads the record associated with `key`.
    ///
    /// Returns a [`GetBuilder`]; chain options
    /// ([`comparison`](GetBuilder::comparison),
    /// [`partition_key`](GetBuilder::partition_key),
    /// [`include_value`](GetBuilder::include_value),
    /// [`use_index`](GetBuilder::use_index), …) and `.await` it. With a
    /// non-[`Equal`](crate::ComparisonType::Equal) comparison the returned
    /// record's key is the *found* key, which may differ from the requested
    /// one. Records written with a partition key must be read with the same
    /// partition key.
    ///
    /// An exact-match get is routed to the single shard that owns the key (or
    /// the partition key, when given); an inequality comparison or a
    /// secondary-index lookup instead fans out across all shards.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns:
    /// - [`OxiaError::KeyNotFound`] when no record matches;
    /// - [`OxiaError::Timeout`] when the request timeout elapses;
    /// - [`OxiaError::Closed`] after [`close`](OxiaClient::close);
    /// - transient routing/transport errors
    ///   ([`OxiaError::is_retryable`](crate::OxiaError::is_retryable)).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: &oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// use oxia::ComparisonType;
    ///
    /// let exact = client.get("config/mode").await?;
    /// let floor = client
    ///     .get("config/zz")
    ///     .comparison(ComparisonType::Floor)
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn get(&self, key: impl Into<String>) -> GetBuilder {
        GetBuilder::new(self.clone(), key.into())
    }

    /// Deletes the record associated with `key`.
    ///
    /// Returns a [`DeleteBuilder`]; optionally chain
    /// [`expected_version_id`](DeleteBuilder::expected_version_id) for a
    /// conditional delete or
    /// [`partition_key`](DeleteBuilder::partition_key) for records written
    /// with one, and `.await` it.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns:
    /// - [`OxiaError::KeyNotFound`] when the record does not exist;
    /// - [`OxiaError::UnexpectedVersionId`] when an
    ///   [`expected_version_id`](DeleteBuilder::expected_version_id)
    ///   condition does not hold;
    /// - [`OxiaError::Timeout`] / [`OxiaError::Closed`] / transient errors as
    ///   for the other operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: &oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// client.delete("config/mode").await?;
    /// # Ok(()) }
    /// ```
    pub fn delete(&self, key: impl Into<String>) -> DeleteBuilder {
        DeleteBuilder::new(self.clone(), key.into())
    }

    /// Deletes every record with `min_key_inclusive <= key < max_key_exclusive`
    /// (in the key order of the namespace).
    ///
    /// Without a [`partition_key`](DeleteRangeBuilder::partition_key) the
    /// deletion is applied on every shard; it is not atomic across shards.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns [`OxiaError::Timeout`],
    /// [`OxiaError::Closed`], or transient routing/transport errors. Deleting
    /// an empty range is not an error.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: &oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// // Everything under the "config/" prefix:
    /// client.delete_range("config/", "config/~").await?;
    /// # Ok(()) }
    /// ```
    pub fn delete_range(
        &self,
        min_key_inclusive: impl Into<String>,
        max_key_exclusive: impl Into<String>,
    ) -> DeleteRangeBuilder {
        DeleteRangeBuilder::new(
            self.clone(),
            min_key_inclusive.into(),
            max_key_exclusive.into(),
        )
    }

    /// Lists the keys with `min_key_inclusive <= key < max_key_exclusive`
    /// (in the key order of the namespace), or `<= max_key_exclusive` with
    /// [`end_inclusive`](ListBuilder::end_inclusive).
    ///
    /// `.await` the builder for all keys at once (bounded by the request
    /// timeout), or call [`stream()`](ListBuilder::stream) for an
    /// incremental [`ListStream`](crate::ListStream) with no overall
    /// deadline. Both deliver keys in the server's global key order, merged
    /// across shards.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns [`OxiaError::Timeout`] when collecting
    /// all keys exceeds the request timeout, [`OxiaError::Closed`] after
    /// [`close`](OxiaClient::close), [`OxiaError::EndInclusiveUnsupported`]
    /// when [`end_inclusive`](ListBuilder::end_inclusive) reaches a shard
    /// that leaves the end out, or transient routing/transport errors. An
    /// empty range yields an empty `Vec`, not an error.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: &oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// let keys = client.list("config/", "config/~").await?;
    /// # Ok(()) }
    /// ```
    pub fn list(
        &self,
        min_key_inclusive: impl Into<String>,
        max_key_exclusive: impl Into<String>,
    ) -> ListBuilder {
        ListBuilder::new(
            self.clone(),
            min_key_inclusive.into(),
            max_key_exclusive.into(),
        )
    }

    /// Reads every record with `min_key_inclusive <= key < max_key_exclusive`
    /// (in the key order of the namespace), or `<= max_key_exclusive` with
    /// [`end_inclusive`](RangeScanBuilder::end_inclusive).
    ///
    /// `.await` the builder for all records at once (bounded by the request
    /// timeout), or call [`stream()`](RangeScanBuilder::stream) for an
    /// incremental [`RangeScanStream`](crate::RangeScanStream) that holds at
    /// most one buffered record per shard — prefer it for large ranges. Both
    /// deliver records in the server's global key order, merged across
    /// shards.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns [`OxiaError::Timeout`] when collecting
    /// all records exceeds the request timeout, [`OxiaError::Closed`] after
    /// [`close`](OxiaClient::close), [`OxiaError::EndInclusiveUnsupported`]
    /// when [`end_inclusive`](RangeScanBuilder::end_inclusive) reaches a
    /// shard that leaves the end out, or transient routing/transport errors.
    /// An empty range yields an empty `Vec`, not an error.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: &oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// use futures::TryStreamExt;
    ///
    /// let mut scan = client.range_scan("logs/", "logs/~").stream().await?;
    /// while let Some(record) = scan.try_next().await? {
    ///     println!("{} => {:?}", record.key, record.value);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn range_scan(
        &self,
        min_key_inclusive: impl Into<String>,
        max_key_exclusive: impl Into<String>,
    ) -> RangeScanBuilder {
        RangeScanBuilder::new(
            self.clone(),
            min_key_inclusive.into(),
            max_key_exclusive.into(),
        )
    }

    /// Subscribes to all changes applied to the database, streamed as
    /// [`Notification`](crate::Notification)s.
    ///
    /// Awaiting the builder yields a [`Notifications`](crate::Notifications)
    /// handle. Per-shard subscriptions are established in the background and
    /// re-established automatically after connection loss, resuming from the
    /// last delivered offset so no notification in between is lost. Events
    /// from the same shard arrive in order; interleaving across shards is
    /// unspecified. Changes made before the subscription is fully established
    /// may not be delivered.
    ///
    /// Dropping the handle ends the subscription. When the channel buffer
    /// ([`buffer_size`](NotificationsBuilder::buffer_size)) is full, delivery
    /// applies backpressure to the per-shard listeners.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns [`OxiaError::Closed`] after
    /// [`close`](OxiaClient::close).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: &oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// let mut notifications = client.notifications().await?;
    /// while let Some(event) = notifications.recv().await {
    ///     println!("{event}");
    /// }
    /// # Ok(()) }
    /// ```
    pub fn notifications(&self) -> NotificationsBuilder {
        NotificationsBuilder::new(self.clone())
    }

    /// Subscribes to updates of the sequence rooted at `key`, delivering the
    /// highest assigned sequence key as it advances. `partition_key` must be
    /// the partition key the sequential records are written with
    /// (see [`PutBuilder::sequence_key_deltas`]).
    ///
    /// Awaiting the builder yields a
    /// [`SequenceUpdates`](crate::SequenceUpdates) handle. Consecutive
    /// advances may be coalesced: each delivery carries the *highest*
    /// sequence key at that moment. The subscription reconnects automatically
    /// after connection loss; dropping the handle ends it.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns [`OxiaError::Closed`] after
    /// [`close`](OxiaClient::close).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: &oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// let mut updates = client.sequence_updates("seq/events", "pk-0").await?;
    /// client
    ///     .put("seq/events", "data")
    ///     .partition_key("pk-0")
    ///     .sequence_key_deltas([1])
    ///     .await?;
    /// let highest = updates.recv().await;
    /// # Ok(()) }
    /// ```
    pub fn sequence_updates(
        &self,
        key: impl Into<String>,
        partition_key: impl Into<String>,
    ) -> SequenceUpdatesBuilder {
        SequenceUpdatesBuilder::new(self.clone(), key.into(), partition_key.into())
    }

    /// Closes the client gracefully: pending batches are flushed, subscriptions
    /// stop, sessions are closed server-side (immediately removing this
    /// client's ephemeral records), and connections are released.
    ///
    /// Idempotent: subsequent calls (from any clone) return `Ok(())`.
    /// Operations submitted after `close` fail with [`OxiaError::Closed`].
    /// Merely dropping every clone instead tears the client down abruptly:
    /// queued operations are not flushed and ephemeral records linger until
    /// their session times out.
    ///
    /// # Errors
    ///
    /// Returns the first error encountered while shutting down (shutdown
    /// still proceeds through every component). Only the first call can
    /// return an error.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(client: oxia::OxiaClient) -> Result<(), oxia::OxiaError> {
    /// client.close().await?;
    /// # Ok(()) }
    /// ```
    pub async fn close(&self) -> Result<(), OxiaError> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let mut first_err: Option<OxiaError> = None;

        // Flush and stop the batchers (writes first, so queued mutations land).
        let write_shards: Vec<i64> = self.inner.write_batchers.iter().map(|e| *e.key()).collect();
        for shard in write_shards {
            if let Some((_, batcher)) = self.inner.write_batchers.remove(&shard) {
                track(&mut first_err, batcher.close().await, "write batcher");
            }
        }
        let read_shards: Vec<i64> = self.inner.read_batchers.iter().map(|e| *e.key()).collect();
        for shard in read_shards {
            if let Some((_, batcher)) = self.inner.read_batchers.remove(&shard) {
                track(&mut first_err, batcher.close().await, "read batcher");
            }
        }
        let notification_ids: Vec<u64> = self
            .inner
            .notification_managers
            .iter()
            .map(|e| *e.key())
            .collect();
        for id in notification_ids {
            if let Some((_, manager)) = self.inner.notification_managers.remove(&id) {
                track(&mut first_err, manager.shutdown().await, "notifications");
            }
        }
        let sequence_ids: Vec<u64> = self
            .inner
            .sequence_managers
            .iter()
            .map(|e| *e.key())
            .collect();
        for id in sequence_ids {
            if let Some((_, manager)) = self.inner.sequence_managers.remove(&id) {
                track(&mut first_err, manager.shutdown().await, "sequence updates");
            }
        }
        track(
            &mut first_err,
            self.inner.session_manager.close().await,
            "sessions",
        );
        track(
            &mut first_err,
            self.inner.shard_manager.close().await,
            "shard manager",
        );
        self.inner.provider_manager.clear();
        match first_err {
            None => Ok(()),
            Some(err) => Err(err),
        }
    }

    // ---- internals shared by the request builders -------------------------

    pub(crate) fn ensure_open(&self) -> Result<(), OxiaError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(OxiaError::Closed);
        }
        Ok(())
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.inner.options.request_timeout
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    pub(crate) fn identity(&self) -> &str {
        &self.inner.options.identity
    }

    /// Resolves the shard that owns `routing_key`.
    pub(crate) fn shard_for(&self, routing_key: &str) -> Result<i64, OxiaError> {
        self.inner
            .shard_manager
            .get_shard(routing_key)
            .ok_or_else(|| OxiaError::NoShardForKey {
                key: routing_key.to_string(),
            })
    }

    pub(crate) async fn session_id_for(&self, shard: i64) -> Result<i64, OxiaError> {
        self.inner.session_manager.get_session_id(shard).await
    }

    /// Runs `attempt` against the shard that owns `routing_key`, re-resolving
    /// the shard and retrying (backoff, bounded by the request timeout) while
    /// routing is unavailable — `NoShardForKey` (assignments not loaded yet) or
    /// `ShardMoved` (the shard just split or merged). This is how an operation
    /// re-routes to the new shard after a split/merge.
    pub(crate) async fn with_shard_retry<T, F, Fut>(
        &self,
        routing_key: &str,
        attempt: F,
    ) -> Result<T, OxiaError>
    where
        F: Fn(i64) -> Fut,
        Fut: std::future::Future<Output = Result<T, OxiaError>>,
    {
        retry_until(
            self.request_timeout(),
            |err| matches!(err, OxiaError::ShardMoved | OxiaError::NoShardForKey { .. }),
            || async {
                let shard = self.shard_for(routing_key)?;
                attempt(shard).await
            },
        )
        .await
    }

    /// Retries `op` while it fails with `ShardMoved` — for whole-operation
    /// re-routing (broadcasts) where a single failing shard means the shard set
    /// changed and the operation must be re-issued against the new set.
    pub(crate) async fn with_reroute_retry<T, F, Fut>(&self, op: F) -> Result<T, OxiaError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, OxiaError>>,
    {
        retry_until(
            self.request_timeout(),
            |err| matches!(err, OxiaError::ShardMoved),
            op,
        )
        .await
    }

    /// Returns (creating on first use) the write batcher for a shard.
    fn write_batcher(&self, shard_id: i64) -> Arc<WriteBatcher> {
        self.inner
            .write_batchers
            .entry(shard_id)
            .or_insert_with(|| {
                Arc::new(WriteBatcher::new(shard_id, self.inner.batcher_deps.clone()))
            })
            .clone()
    }

    /// Returns (creating on first use) the read batcher for a shard.
    fn read_batcher(&self, shard_id: i64) -> Arc<ReadBatcher> {
        self.inner
            .read_batchers
            .entry(shard_id)
            .or_insert_with(|| {
                Arc::new(ReadBatcher::new(shard_id, self.inner.batcher_deps.clone()))
            })
            .clone()
    }

    /// Submits a write to a shard's batcher and awaits its response, bounded
    /// by the request timeout.
    pub(crate) async fn submit_write<Req, Resp>(
        &self,
        shard_id: i64,
        request: Req,
        wrap: fn(Pending<Req, Resp>) -> WriteOp,
    ) -> Result<Resp, OxiaError> {
        let (op, rx) = pending_write(request, wrap);
        self.write_batcher(shard_id).add(op);
        self.await_response(rx).await
    }

    /// Submits a get to a shard's batcher and awaits its response, bounded by
    /// the request timeout.
    pub(crate) async fn submit_get(
        &self,
        shard_id: i64,
        request: proto::GetRequest,
    ) -> Result<proto::GetResponse, OxiaError> {
        let (pending, rx) = Pending::new(request);
        self.read_batcher(shard_id).add(pending);
        self.await_response(rx).await
    }

    pub(crate) async fn await_response<Resp>(
        &self,
        rx: oneshot::Receiver<Result<Resp, OxiaError>>,
    ) -> Result<Resp, OxiaError> {
        match tokio::time::timeout(self.request_timeout(), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                if self.inner.closed.load(Ordering::Acquire) {
                    Err(OxiaError::Closed)
                } else {
                    Err(OxiaError::Disconnected(
                        "operation was dropped without a response".to_string(),
                    ))
                }
            }
            Err(_) => Err(OxiaError::Timeout),
        }
    }

    /// Runs a get against every shard and reduces the per-shard winners
    /// according to the comparison type. Used only when the query is not pinned
    /// to a single shard — i.e. an inequality comparison or a secondary-index
    /// lookup with no partition key; an exact-match primary-key get is routed
    /// directly to its owning shard instead.
    pub(crate) async fn broadcast_get(
        &self,
        request: proto::GetRequest,
        comparison: ComparisonType,
    ) -> Result<GetResult, OxiaError> {
        // Re-issue against the current shard set if a shard splits mid-flight.
        self.with_reroute_retry(|| {
            let request = request.clone();
            async move {
                let order = self.inner.shard_manager.key_order();
                let mut join_set = JoinSet::new();
                for shard in self.inner.shard_manager.get_shard_ids() {
                    let client = self.clone();
                    let request = request.clone();
                    join_set.spawn(async move {
                        let requested_key = request.key.clone();
                        let response = client.submit_get(shard, request).await?;
                        check_status(response.status)?;
                        GetResult::from_proto(response, Some(requested_key))
                    });
                }
                let mut best: Option<GetResult> = None;
                let mut join_error: Option<OxiaError> = None;
                while let Some(joined) = join_set.join_next().await {
                    let result = match joined {
                        Ok(result) => result,
                        Err(err) => {
                            join_error = Some(OxiaError::Disconnected(err.to_string()));
                            continue;
                        }
                    };
                    match result {
                        Ok(candidate) => {
                            best = Some(match best.take() {
                                None => candidate,
                                Some(current) => {
                                    pick_get_winner(current, candidate, comparison, order)
                                }
                            })
                        }
                        // Not every shard has a matching record.
                        Err(OxiaError::KeyNotFound) => {}
                        Err(err) => return Err(err),
                    }
                }
                if let Some(err) = join_error {
                    return Err(err);
                }
                best.ok_or(OxiaError::KeyNotFound)
            }
        })
        .await
    }

    /// Resolves the target shards for a list/scan: either the single shard
    /// owning `partition_key`, or all shards.
    fn target_shards(&self, partition_key: Option<&str>) -> Result<Vec<i64>, OxiaError> {
        match partition_key {
            Some(pk) => Ok(vec![self.shard_for(pk)?]),
            None => Ok(self.inner.shard_manager.get_shard_ids()),
        }
    }

    pub(crate) async fn open_list_stream(
        &self,
        request: proto::ListRequest,
        partition_key: Option<&str>,
    ) -> Result<ListStream, OxiaError> {
        self.ensure_open()?;
        let end_inclusive = request.end_inclusive;
        let opens = self.target_shards(partition_key)?.into_iter().map(|shard| {
            let client = self.clone();
            let request = request.clone();
            async move {
                let (first, streaming) = client
                    .open_streaming_with_retry(shard, move |mut provider| {
                        let mut request = request.clone();
                        request.shard = Some(shard);
                        async move { provider.list(request).await }
                    })
                    .await?;
                check_end_inclusive(end_inclusive, first.as_ref().map(|r| r.end_inclusive))?;
                let to_keys = move |response: proto::ListResponse| match check_end_inclusive(
                    end_inclusive,
                    Some(response.end_inclusive),
                ) {
                    Ok(()) => response.keys.into_iter().map(Ok).collect(),
                    Err(err) => vec![Err(err)],
                };
                let head = first.map(to_keys).unwrap_or_default();
                let stream: ShardStream<String> = Box::pin(
                    futures::stream::iter(head).chain(
                        streaming
                            .map(move |chunk| match chunk {
                                Ok(response) => to_keys(response),
                                Err(status) => vec![Err(OxiaError::from(status))],
                            })
                            .flat_map(futures::stream::iter),
                    ),
                );
                Ok::<_, OxiaError>(stream)
            }
        });
        Ok(ListStream::new(merged(
            try_join_all(opens).await?,
            self.inner.shard_manager.key_order(),
        )))
    }

    pub(crate) async fn open_range_scan_stream(
        &self,
        request: proto::RangeScanRequest,
        partition_key: Option<&str>,
    ) -> Result<RangeScanStream, OxiaError> {
        self.ensure_open()?;
        let end_inclusive = request.end_inclusive;
        let opens = self.target_shards(partition_key)?.into_iter().map(|shard| {
            let client = self.clone();
            let request = request.clone();
            async move {
                let (first, streaming) = client
                    .open_streaming_with_retry(shard, move |mut provider| {
                        let mut request = request.clone();
                        request.shard = Some(shard);
                        async move { provider.range_scan(request).await }
                    })
                    .await?;
                check_end_inclusive(end_inclusive, first.as_ref().map(|r| r.end_inclusive))?;
                let to_items = move |response: proto::RangeScanResponse| match check_end_inclusive(
                    end_inclusive,
                    Some(response.end_inclusive),
                ) {
                    Ok(()) => response
                        .records
                        .into_iter()
                        .map(|record| GetResult::from_proto(record, None))
                        .collect(),
                    Err(err) => vec![Err(err)],
                };
                let head = first.map(to_items).unwrap_or_default();
                let stream: ShardStream<GetResult> = Box::pin(
                    futures::stream::iter(head).chain(
                        streaming
                            .map(move |chunk| match chunk {
                                Ok(response) => to_items(response),
                                Err(status) => vec![Err(OxiaError::from(status))],
                            })
                            .flat_map(futures::stream::iter),
                    ),
                );
                Ok::<_, OxiaError>(stream)
            }
        });
        Ok(RangeScanStream::new(merged(
            try_join_all(opens).await?,
            self.inner.shard_manager.key_order(),
        )))
    }

    /// Opens a per-shard server stream, retrying — with backoff and leader
    /// hints, bounded by the request timeout — until the first response (or a
    /// clean end of stream) has been received. Past that point the caller
    /// consumes the stream without retries, so results are never duplicated
    /// (the reference client's "verified" pattern).
    async fn open_streaming_with_retry<Resp, Fut>(
        &self,
        shard: i64,
        call: impl Fn(
            proto::oxia_client_client::OxiaClientClient<crate::provider_manager::AuthChannel>,
        ) -> Fut,
    ) -> Result<(Option<Resp>, tonic::Streaming<Resp>), OxiaError>
    where
        Fut: Future<Output = Result<tonic::Response<tonic::Streaming<Resp>>, tonic::Status>>,
    {
        let deadline = tokio::time::Instant::now() + self.request_timeout();
        let mut hint: Option<LeaderHint> = None;
        let mut attempt: u32 = 0;
        loop {
            let outcome: Result<_, DecodedStatus> = async {
                let target = match hint.as_ref().and_then(|h| h.address_for(shard)) {
                    Some(address) => crate::address::ensure_protocol(address.to_string()),
                    None => {
                        self.inner
                            .shard_manager
                            .get_leader(shard)
                            .ok_or_else(|| {
                                DecodedStatus::from(OxiaError::LeaderNotFound { shard })
                            })?
                            .service_address
                    }
                };
                let provider = self
                    .inner
                    .provider_manager
                    .get_provider(target)
                    .await
                    .map_err(DecodedStatus::from)?;
                let mut streaming = call(provider).await.map_err(decode_status)?.into_inner();
                match streaming.next().await {
                    Some(Ok(first)) => Ok((Some(first), streaming)),
                    None => Ok((None, streaming)),
                    Some(Err(status)) => Err(decode_status(status)),
                }
            }
            .await;
            match outcome {
                Ok(opened) => return Ok(opened),
                Err(decoded) => {
                    if decoded.leader_hint.is_some() {
                        hint = decoded.leader_hint;
                    }
                    let delay = retry_delay(attempt);
                    attempt += 1;
                    if !decoded.error.is_retryable()
                        || tokio::time::Instant::now() + delay >= deadline
                    {
                        return Err(decoded.error);
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub(crate) async fn open_notifications(
        &self,
        buffer_size: usize,
    ) -> Result<Notifications, OxiaError> {
        self.ensure_open()?;
        let shards = self.inner.shard_manager.get_shard_ids();
        let shard_count = shards.len();
        let (tx, rx) = mpsc::channel(buffer_size);
        let (init_tx, mut init_rx) = mpsc::channel(shard_count.max(1));
        let manager = NotificationManager::new(
            shards,
            self.inner.shard_manager.clone(),
            self.inner.provider_manager.clone(),
            tx,
            init_tx,
        );
        // Make the subscription live before returning, so no change in the
        // window between here and the first stream is missed. Returning early
        // (on error/timeout) drops `manager`, cancelling every listener.
        let deadline = tokio::time::Instant::now() + self.request_timeout();
        let mut ready = 0;
        while ready < shard_count {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, init_rx.recv()).await {
                Ok(Some(Ok(()))) => ready += 1,
                Ok(Some(Err(err))) => return Err(err),
                Ok(None) => break,
                Err(_) => return Err(OxiaError::Timeout),
            }
        }
        let id = self.register_notification_manager(manager);
        Ok(Notifications::new(
            rx,
            self.subscription_guard(id, SubscriptionKind::Notification),
        ))
    }

    pub(crate) async fn open_sequence_updates(
        &self,
        key: String,
        partition_key: String,
        buffer_size: usize,
    ) -> Result<SequenceUpdates, OxiaError> {
        self.ensure_open()?;
        let (tx, rx) = mpsc::channel(buffer_size);
        let manager = SequenceUpdatesManager::new(
            key,
            partition_key,
            self.inner.shard_manager.clone(),
            self.inner.provider_manager.clone(),
            tx,
        );
        let id = self
            .inner
            .next_subscription_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner.sequence_managers.insert(id, manager);
        Ok(SequenceUpdates::new(
            rx,
            self.subscription_guard(id, SubscriptionKind::Sequence),
        ))
    }

    /// Registers a live notification manager so [`close`](Self::close) can shut
    /// it down, returning the id used to remove it when its handle is dropped.
    fn register_notification_manager(&self, manager: NotificationManager) -> u64 {
        let id = self
            .inner
            .next_subscription_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner.notification_managers.insert(id, manager);
        id
    }

    /// Builds the guard a subscription handle holds; dropping the handle removes
    /// (and thereby stops) the registered manager.
    fn subscription_guard(&self, id: u64, kind: SubscriptionKind) -> SubscriptionGuard {
        SubscriptionGuard {
            inner: Arc::downgrade(&self.inner),
            id,
            kind,
        }
    }

    /// Broadcasts a delete-range to every shard, re-issuing against the current
    /// shard set if one splits or merges mid-flight (idempotent).
    pub(crate) async fn broadcast_delete_range(
        &self,
        request: proto::DeleteRangeRequest,
    ) -> Result<(), OxiaError> {
        self.with_reroute_retry(|| {
            let request = request.clone();
            async move {
                let mut join_set = JoinSet::new();
                for shard in self.inner.shard_manager.get_shard_ids() {
                    let client = self.clone();
                    let request = request.clone();
                    join_set.spawn(async move {
                        let response = client
                            .submit_write(shard, request, WriteOp::DeleteRange)
                            .await?;
                        check_status(response.status)
                    });
                }
                while let Some(joined) = join_set.join_next().await {
                    joined.map_err(|err| OxiaError::Disconnected(err.to_string()))??;
                }
                Ok(())
            }
        })
        .await
    }
}

fn track(first_err: &mut Option<OxiaError>, result: Result<(), OxiaError>, what: &str) {
    if let Err(err) = result {
        warn!("error while closing {what}: {err}");
        first_err.get_or_insert(err);
    }
}

/// Checks that a shard included the end of the range of a list or a range
/// scan that asked for it ([`end_inclusive`](crate::ListBuilder::end_inclusive)),
/// given the flag of one of its responses, or `None` when it sent none.
/// Older servers ignore the request's flag and leave the end out, while a
/// leader that includes it says so in all its responses, and sends at least
/// one.
fn check_end_inclusive(requested: bool, included: Option<bool>) -> Result<(), OxiaError> {
    if requested && included != Some(true) {
        return Err(OxiaError::EndInclusiveUnsupported);
    }
    Ok(())
}

/// Reduces two per-shard get results to the winner for the comparison type,
/// comparing their keys in the key order of the namespace.
fn pick_get_winner(
    a: GetResult,
    b: GetResult,
    comparison: ComparisonType,
    order: KeyOrder,
) -> GetResult {
    let a_wins = match comparison {
        // The smallest matching key wins.
        ComparisonType::Equal | ComparisonType::Ceiling | ComparisonType::Higher => {
            a.compare_keys(&b, order).is_le()
        }
        // The largest matching key wins.
        ComparisonType::Floor | ComparisonType::Lower => a.compare_keys(&b, order).is_ge(),
    };
    if a_wins { a } else { b }
}

/// Maps a wire-level status to the domain error.
pub(crate) fn check_status(status: i32) -> Result<(), OxiaError> {
    match proto::Status::try_from(status) {
        Ok(proto::Status::Ok) => Ok(()),
        Ok(proto::Status::KeyNotFound) => Err(OxiaError::KeyNotFound),
        Ok(proto::Status::UnexpectedVersionId) => Err(OxiaError::UnexpectedVersionId),
        Ok(proto::Status::SessionDoesNotExist) => Err(OxiaError::SessionExpired),
        Ok(proto::Status::InvalidArgument) => Err(OxiaError::InvalidArgument(
            "rejected by the server".to_string(),
        )),
        Err(err) => Err(OxiaError::Decode(format!("invalid status code: {err}"))),
    }
}
