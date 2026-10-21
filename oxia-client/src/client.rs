// Copyright 2025-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The Oxia client: construction, per-operation execution, and shutdown.

use crate::batcher::{
    BatcherDeps, ReadBatcher, WriteManager, WriteOp, absent_shard_error, pending_write,
};
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
use crate::shard_manager::{ShardManager, ShardManagerOptions, ShardRange};
use crate::streams::{
    ListStream, Notifications, RangeScanStream, SequenceUpdates, ShardStream, merged,
};
use crate::types::{ComparisonType, GetResult};
use bytes::Bytes;
use futures::StreamExt;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

pub(crate) struct Inner {
    pub(crate) options: OxiaClientOptions,
    pub(crate) provider_manager: Rc<ProviderManager>,
    pub(crate) shard_manager: Rc<ShardManager>,
    pub(crate) session_manager: Rc<SessionManager>,
    batcher_deps: Rc<BatcherDeps>,
    notification_managers: RefCell<HashMap<u64, NotificationManager>>,
    sequence_managers: RefCell<HashMap<u64, SequenceUpdatesManager>>,
    next_subscription_id: Cell<u64>,
    write_manager: Rc<WriteManager>,
    read_batchers: RefCell<HashMap<i64, Rc<ReadBatcher>>>,
    closed: Cell<bool>,
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
        // Never panics on a registry in use: the handle may be dropped where
        // a panic aborts, as compio drops the futures of cancelled tasks.
        // Each manager is dropped once its registry is no longer borrowed.
        match self.kind {
            SubscriptionKind::Notification => {
                let removed = inner
                    .notification_managers
                    .try_borrow_mut()
                    .ok()
                    .and_then(|mut managers| managers.remove(&self.id));
                drop(removed);
            }
            SubscriptionKind::Sequence => {
                let removed = inner
                    .sequence_managers
                    .try_borrow_mut()
                    .ok()
                    .and_then(|mut managers| managers.remove(&self.id));
                drop(removed);
            }
        }
    }
}

/// The Oxia client.
///
/// Cheap to clone: all clones share one set of connections, batchers and
/// sessions. The client lives on the thread that built it, inside its compio
/// runtime: it is neither `Send` nor `Sync`, and the tasks of that thread
/// share it (see [Runtime](crate#runtime)). Obtain one with
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
    inner: Rc<Inner>,
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
        let provider_manager = provider_manager.with_tls(
            options
                .tls
                .as_ref()
                .map(|tls| tls.to_tls_config())
                .transpose()?,
        );
        let provider_manager = Rc::new(provider_manager);
        let shard_manager = Rc::new(
            ShardManager::new(ShardManagerOptions {
                address: options.service_address.clone(),
                namespace: options.namespace.clone(),
                provider_manager: provider_manager.clone(),
                request_timeout: options.request_timeout,
            })
            .await?,
        );
        let batcher_deps = Rc::new(BatcherDeps {
            namespace: options.namespace.clone(),
            shard_manager: shard_manager.clone(),
            provider_manager: provider_manager.clone(),
            max_batch_size: options.batch_max_size as usize,
            max_requests_per_batch: options.max_requests_per_batch as usize,
            max_write_batches_in_flight: options.max_write_batches_in_flight as usize,
            max_read_batches_in_flight: options.max_read_batches_in_flight as usize,
            request_timeout: options.request_timeout,
            metrics,
        });
        let session_manager = Rc::new(SessionManager::new(
            options.identity.clone(),
            options.session_timeout,
            options.session_keep_alive,
            options.request_timeout,
            shard_manager.clone(),
            provider_manager.clone(),
        ));
        Ok(OxiaClient {
            inner: Rc::new(Inner {
                options,
                provider_manager,
                shard_manager,
                session_manager,
                write_manager: WriteManager::new(batcher_deps.clone()),
                batcher_deps,
                notification_managers: RefCell::new(HashMap::new()),
                sequence_managers: RefCell::new(HashMap::new()),
                next_subscription_id: Cell::new(0),
                read_batchers: RefCell::new(HashMap::new()),
                closed: Cell::new(false),
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
    ///   expected version, when a
    ///   [`secondary_index`](PutBuilder::secondary_index) name contains `/`,
    ///   or when the server rejects the put as invalid;
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
    /// (in the key order of the namespace). On the servers with
    /// `FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS`, an empty
    /// `max_key_exclusive` deletes all the records from `min_key_inclusive`
    /// on, up to Oxia's internal keys, which it leaves.
    ///
    /// Without a [`partition_key`](DeleteRangeBuilder::partition_key) the
    /// deletion is applied on every shard; it is not atomic across shards.
    ///
    /// # Errors
    ///
    /// Awaiting the builder returns [`OxiaError::InvalidArgument`] when the
    /// range covers both records and Oxia's internal keys (on the servers with
    /// `FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS`), [`OxiaError::Timeout`],
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
    /// that leaves the end out, [`OxiaError::ShardMoved`] when a shard is
    /// split or merged before its keys start coming (listing again reads the
    /// new shards), or transient routing/transport errors. An empty range
    /// yields an empty `Vec`, not an error.
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
    /// shard that leaves the end out, [`OxiaError::ShardMoved`] when a shard
    /// is split or merged before its records start coming (scanning again
    /// reads the new shards), or transient routing/transport errors. An empty
    /// range yields an empty `Vec`, not an error.
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
        if self.inner.closed.replace(true) {
            return Ok(());
        }
        let mut first_err: Option<OxiaError> = None;

        // Flush and stop the batchers (writes first, so queued mutations land).
        track(
            &mut first_err,
            self.inner.write_manager.close().await,
            "write batcher",
        );
        // Each component is removed from its registry before it is awaited:
        // a registry must not stay borrowed across an await, as the
        // operations still running need it.
        let read_shards: Vec<i64> = self.inner.read_batchers.borrow().keys().copied().collect();
        for shard in read_shards {
            let batcher = self.inner.read_batchers.borrow_mut().remove(&shard);
            if let Some(batcher) = batcher {
                track(&mut first_err, batcher.close().await, "read batcher");
            }
        }
        let notification_ids: Vec<u64> = self
            .inner
            .notification_managers
            .borrow()
            .keys()
            .copied()
            .collect();
        for id in notification_ids {
            let manager = self.inner.notification_managers.borrow_mut().remove(&id);
            if let Some(manager) = manager {
                track(&mut first_err, manager.shutdown().await, "notifications");
            }
        }
        let sequence_ids: Vec<u64> = self
            .inner
            .sequence_managers
            .borrow()
            .keys()
            .copied()
            .collect();
        for id in sequence_ids {
            let manager = self.inner.sequence_managers.borrow_mut().remove(&id);
            if let Some(manager) = manager {
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
        if self.inner.closed.get() {
            return Err(OxiaError::Closed);
        }
        Ok(())
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.inner.options.request_timeout
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        &self.inner.batcher_deps.metrics
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

    /// The shard manager, for the tests that wait for the client to get new
    /// assignments.
    #[cfg(test)]
    pub(crate) fn shard_manager(&self) -> &ShardManager {
        &self.inner.shard_manager
    }

    pub(crate) async fn session_id_for(&self, shard: i64) -> Result<i64, OxiaError> {
        self.inner.session_manager.get_session_id(shard).await
    }

    /// Runs `attempt` against the shard that owns `routing_key`, re-resolving
    /// the shard and retrying (backoff, bounded by the request timeout) while
    /// routing is unavailable — `NoShardForKey` (assignments not loaded yet) or
    /// `ShardMoved` (the shard just split or merged). This is how a get
    /// re-routes to the new shard after a split/merge. The batcher of a split
    /// shard hands its pending writes off to the new shards instead, in order
    /// ([`WriteManager`]): a write fails with `ShardMoved` only when no shard
    /// took over its key.
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
    /// changed and the operation must be re-issued against the new set. Only
    /// for reads: re-issuing a write applies it again on the shards that
    /// already applied it.
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

    /// Returns (creating on first use) the read batcher for a shard.
    fn read_batcher(&self, shard_id: i64) -> Rc<ReadBatcher> {
        self.inner
            .read_batchers
            .borrow_mut()
            .entry(shard_id)
            .or_insert_with(|| Rc::new(ReadBatcher::new(shard_id, self.inner.batcher_deps.clone())))
            .clone()
    }

    /// Submits a write to a shard's batcher and awaits its response, bounded
    /// by the request timeout.
    pub(crate) async fn submit_write<Req, Resp>(
        &self,
        shard_id: i64,
        request: Req,
        wrap: impl FnOnce(Pending<Req, Resp>) -> WriteOp,
    ) -> Result<Resp, OxiaError> {
        let (op, rx) = pending_write(request, wrap);
        self.inner.write_manager.add(shard_id, op);
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
        match compio::time::timeout(self.request_timeout(), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                if self.inner.closed.get() {
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
                let mut gets: FuturesUnordered<_> = self
                    .inner
                    .shard_manager
                    .get_shard_ids()
                    .into_iter()
                    .map(|shard| {
                        let request = request.clone();
                        async move {
                            let requested_key = request.key.clone();
                            let response = self.submit_get(shard, request).await?;
                            check_status(response.status)?;
                            GetResult::from_proto(response, Some(requested_key))
                        }
                    })
                    .collect();
                let mut best: Option<GetResult> = None;
                while let Some(result) = gets.next().await {
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
    /// (the reference client's "verified" pattern). Once the shard is split or
    /// merged away, fails with [`OxiaError::ShardMoved`] without retrying, as
    /// the Go client fails with `ErrShardNotFound`.
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
        let deadline = Instant::now() + self.request_timeout();
        let mut hint: Option<LeaderHint> = None;
        let mut attempt: u32 = 0;
        loop {
            let outcome: Result<_, DecodedStatus> = async {
                let target = match hint.as_ref().and_then(|h| h.address_for(shard)) {
                    // As in the batchers, the hint of a shard split or merged
                    // away is stale: the open fails with `ShardMoved`.
                    Some(address) if self.inner.shard_manager.shard_exists(shard) => {
                        crate::address::ensure_protocol(address.to_string())
                    }
                    _ => {
                        self.inner
                            .shard_manager
                            .get_leader(shard)
                            .ok_or_else(|| {
                                DecodedStatus::from(absent_shard_error(
                                    &self.inner.batcher_deps,
                                    shard,
                                ))
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
                    // Opening a moved shard again cannot succeed.
                    if !decoded.error.is_retryable()
                        || matches!(decoded.error, OxiaError::ShardMoved)
                        || Instant::now() + delay >= deadline
                    {
                        return Err(decoded.error);
                    }
                    compio::time::sleep(delay).await;
                }
            }
        }
    }

    pub(crate) async fn open_notifications(
        &self,
        buffer_size: usize,
    ) -> Result<Notifications, OxiaError> {
        self.ensure_open()?;
        let shards = self.inner.shard_manager.get_shard_ranges();
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
        let deadline = Instant::now() + self.request_timeout();
        let mut ready = 0;
        while ready < shard_count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match compio::time::timeout(remaining, init_rx.recv()).await {
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
        let id = self.next_subscription_id();
        self.inner
            .sequence_managers
            .borrow_mut()
            .insert(id, manager);
        Ok(SequenceUpdates::new(
            rx,
            self.subscription_guard(id, SubscriptionKind::Sequence),
        ))
    }

    /// Registers a live notification manager so [`close`](Self::close) can shut
    /// it down, returning the id used to remove it when its handle is dropped.
    fn register_notification_manager(&self, manager: NotificationManager) -> u64 {
        let id = self.next_subscription_id();
        self.inner
            .notification_managers
            .borrow_mut()
            .insert(id, manager);
        id
    }

    fn next_subscription_id(&self) -> u64 {
        let id = self.inner.next_subscription_id.get();
        self.inner.next_subscription_id.set(id + 1);
        id
    }

    /// Builds the guard a subscription handle holds; dropping the handle removes
    /// (and thereby stops) the registered manager.
    fn subscription_guard(&self, id: u64, kind: SubscriptionKind) -> SubscriptionGuard {
        SubscriptionGuard {
            inner: Rc::downgrade(&self.inner),
            id,
            kind,
        }
    }

    /// Broadcasts a delete-range to every shard, bounded by the request
    /// timeout. When a shard splits or merges mid-flight, its copy is re-sent
    /// only to the shards that took over its hash range: applying a delete
    /// range twice on a shard is not idempotent, as the second time deletes
    /// the keys written there since the first.
    pub(crate) async fn broadcast_delete_range(
        &self,
        request: proto::DeleteRangeRequest,
    ) -> Result<(), OxiaError> {
        send_to_every_shard(&self.inner.shard_manager, self.request_timeout(), |shard| {
            // Queue the copy when it is sent, before the first await, as
            // `submit_write` does for a single write, so that each copy
            // precedes in its shard's batch the writes issued after this
            // delete range. Queued only once polled, a copy could follow those
            // writes, and be applied after them. (A copy rerouted after a
            // split can only follow what the successors already have.)
            let (op, rx) = pending_write(request.clone(), |pending| {
                WriteOp::DeleteRange(pending, None)
            });
            self.inner.write_manager.add(shard, op);
            async move { check_status(self.await_response(rx).await?.status) }
        })
        .await
    }
}

/// Runs `send` for every shard in the current assignments. Succeeds once every
/// copy has succeeded; fails with the first error other than
/// [`OxiaError::ShardMoved`], or with [`OxiaError::Timeout`] once `timeout`
/// has elapsed.
///
/// A copy fails with `ShardMoved` when its shard was split or merged away
/// before the copy ran, and the write batcher did not hand it off to the
/// shards that replaced it ([`WriteManager`]). The copy is then sent to that
/// shard's successors, the shards now owning its hash range, except those
/// that already have a copy in flight or applied. A successor that is split
/// away in turn gets the same treatment, by its own range. No other shard is
/// sent a second copy.
async fn send_to_every_shard<F, Fut>(
    shard_manager: &ShardManager,
    timeout: Duration,
    send: F,
) -> Result<(), OxiaError>
where
    F: Fn(i64) -> Fut,
    Fut: Future<Output = Result<(), OxiaError>>,
{
    let deadline = Instant::now() + timeout;
    let send_copy = |range: ShardRange| {
        let copy = send(range.shard);
        async move { (range, copy.await) }
    };
    let mut copies = FuturesUnordered::new();
    // The shards with a copy in flight or applied, each covering its range.
    let mut covered = HashSet::new();
    for range in shard_manager.get_shard_ranges() {
        covered.insert(range.shard);
        copies.push(send_copy(range));
    }
    while let Some((range, result)) = compio::time::timeout_at(deadline, copies.next())
        .await
        .map_err(|_| OxiaError::Timeout)?
    {
        match result {
            Ok(()) => {}
            Err(OxiaError::ShardMoved) => {
                covered.remove(&range.shard);
                let successors = shard_manager.get_successors(&range);
                if successors.is_empty() {
                    return Err(OxiaError::ShardMoved);
                }
                for successor in successors {
                    if covered.insert(successor.shard) {
                        copies.push(send_copy(successor));
                    }
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
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
/// comparing them in the key order of the namespace: through an index, by
/// their secondary keys first ([`GetResult::compare_keys`]).
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

#[cfg(test)]
mod tests {
    use super::*;
    use compio::runtime::JoinHandle;
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

    const TIMEOUT: Duration = Duration::from_secs(1);

    /// A copy sent by the broadcast: its shard, and the channel the test
    /// completes it through, as the write batcher would.
    #[derive(Debug)]
    struct SentCopy {
        shard: i64,
        done: oneshot::Sender<Result<(), OxiaError>>,
    }

    impl SentCopy {
        fn complete(self, result: Result<(), OxiaError>) {
            let _ = self.done.send(result);
        }
    }

    /// Runs `send_to_every_shard` in the background, handing every copy it
    /// sends to the test.
    fn start_broadcast(
        shards: Rc<ShardManager>,
    ) -> (
        JoinHandle<Result<(), OxiaError>>,
        UnboundedReceiver<SentCopy>,
    ) {
        let (tx, rx) = unbounded_channel();
        let broadcast = compio::runtime::spawn(async move {
            send_to_every_shard(&shards, TIMEOUT, |shard| {
                let (done, result) = oneshot::channel();
                let _ = tx.send(SentCopy { shard, done });
                async move {
                    result.await.unwrap_or_else(|_| {
                        Err(OxiaError::Disconnected("copy dropped".to_string()))
                    })
                }
            })
            .await
        });
        (broadcast, rx)
    }

    /// Receives the next `n` copies, ordered by shard.
    async fn next_copies(copies: &mut UnboundedReceiver<SentCopy>, n: usize) -> Vec<SentCopy> {
        let mut received = Vec::with_capacity(n);
        for _ in 0..n {
            let copy = compio::time::timeout(Duration::from_secs(1), copies.recv())
                .await
                .expect("the broadcast sends a copy")
                .expect("the broadcast is running");
            received.push(copy);
        }
        received.sort_by_key(|copy| copy.shard);
        received
    }

    fn shards_of(copies: &[SentCopy]) -> Vec<i64> {
        copies.iter().map(|copy| copy.shard).collect()
    }

    async fn assert_pending(broadcast: &mut JoinHandle<Result<(), OxiaError>>) {
        let waited = compio::time::timeout(Duration::from_millis(100), broadcast).await;
        assert!(waited.is_err(), "the broadcast must still be waiting");
    }

    /// Waits for the broadcast's result, and checks that it sent no copy the
    /// test did not receive.
    async fn finish(
        broadcast: JoinHandle<Result<(), OxiaError>>,
        mut copies: UnboundedReceiver<SentCopy>,
    ) -> Result<(), OxiaError> {
        let result = compio::time::timeout(2 * TIMEOUT, broadcast).await;
        let unexpected: Vec<i64> = std::iter::from_fn(|| copies.try_recv().ok())
            .map(|copy| copy.shard)
            .collect();
        assert!(unexpected.is_empty(), "unexpected copies to {unexpected:?}");
        result
            .expect("the broadcast completes")
            .expect("the broadcast does not panic")
    }

    /// Shard 0 is split into shards 2 and 3 before its copy runs: the copy is
    /// re-sent to the two children only, and shard 1, unrelated to the
    /// split, gets exactly one copy.
    #[compio::test]
    async fn split_shard_copy_goes_to_its_children_only() {
        let shards = ShardManager::detached_for_tests();
        shards.set_assignments_for_tests(&[(0, 0, 99), (1, 100, 199)]);
        let (broadcast, mut copies) = start_broadcast(shards.clone());

        let [to_0, to_1]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        assert_eq!((to_0.shard, to_1.shard), (0, 1));
        to_1.complete(Ok(()));
        // The write batcher fails a copy with `ShardMoved` when its shard is
        // no longer in the assignments.
        shards.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]);
        to_0.complete(Err(OxiaError::ShardMoved));

        let rerouted = next_copies(&mut copies, 2).await;
        assert_eq!(shards_of(&rerouted), vec![2, 3]);
        for copy in rerouted {
            copy.complete(Ok(()));
        }
        finish(broadcast, copies).await.unwrap();
    }

    /// The broadcast succeeds only once every copy has, the rerouted ones
    /// included, whatever the order they complete in.
    #[compio::test]
    async fn broadcast_waits_for_every_copy() {
        let shards = ShardManager::detached_for_tests();
        shards.set_assignments_for_tests(&[(0, 0, 99), (1, 100, 199)]);
        let (mut broadcast, mut copies) = start_broadcast(shards.clone());

        let [to_0, to_1]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        shards.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]);
        to_0.complete(Err(OxiaError::ShardMoved));
        let [to_2, to_3]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        assert_eq!((to_2.shard, to_3.shard), (2, 3));

        to_2.complete(Ok(()));
        assert_pending(&mut broadcast).await;
        to_1.complete(Ok(()));
        assert_pending(&mut broadcast).await;
        to_3.complete(Ok(()));
        finish(broadcast, copies).await.unwrap();
    }

    /// Any error but `ShardMoved` fails the broadcast at once, without
    /// waiting for the copies still in flight.
    #[compio::test]
    async fn broadcast_fails_on_the_first_other_error() {
        let shards = ShardManager::detached_for_tests();
        shards.set_assignments_for_tests(&[(0, 0, 99), (1, 100, 199)]);
        let (broadcast, mut copies) = start_broadcast(shards.clone());

        let [to_0, to_1]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        shards.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]);
        to_0.complete(Err(OxiaError::ShardMoved));
        let [to_2, to_3]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        assert_eq!((to_2.shard, to_3.shard), (2, 3));

        to_3.complete(Err(OxiaError::LeaderNotFound { shard: 3 }));
        let result = finish(broadcast, copies).await;
        assert!(
            matches!(result, Err(OxiaError::LeaderNotFound { shard: 3 })),
            "got {result:?}"
        );
        // The copies to shards 1 and 2 were still in flight.
        drop((to_1, to_2));
    }

    /// Shard 3, a child of shard 0, is split in turn before the rerouted copy
    /// runs: that copy goes to shard 3's own children, by shard 3's range.
    #[compio::test]
    async fn nested_split_reroutes_by_the_childs_range() {
        let shards = ShardManager::detached_for_tests();
        shards.set_assignments_for_tests(&[(0, 0, 99), (1, 100, 199)]);
        let (mut broadcast, mut copies) = start_broadcast(shards.clone());

        let [to_0, to_1]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        to_1.complete(Ok(()));
        shards.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]);
        to_0.complete(Err(OxiaError::ShardMoved));
        let [to_2, to_3]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        assert_eq!((to_2.shard, to_3.shard), (2, 3));

        shards.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (4, 50, 74), (5, 75, 99)]);
        to_3.complete(Err(OxiaError::ShardMoved));
        let rerouted = next_copies(&mut copies, 2).await;
        assert_eq!(shards_of(&rerouted), vec![4, 5]);

        to_2.complete(Ok(()));
        let [to_4, to_5]: [SentCopy; 2] = rerouted.try_into().unwrap();
        to_4.complete(Ok(()));
        assert_pending(&mut broadcast).await;
        to_5.complete(Ok(()));
        finish(broadcast, copies).await.unwrap();
    }

    /// Assignments listing the children next to the parent they replace
    /// (published between the coordinator's updates): the parent's copy is
    /// not re-sent to the children, which have their own.
    #[compio::test]
    async fn copy_is_not_resent_to_shards_that_have_one() {
        let shards = ShardManager::detached_for_tests();
        shards.set_assignments_for_tests(&[(0, 0, 99), (1, 100, 199), (2, 0, 49), (3, 50, 99)]);
        let (broadcast, mut copies) = start_broadcast(shards.clone());

        let sent = next_copies(&mut copies, 4).await;
        assert_eq!(shards_of(&sent), vec![0, 1, 2, 3]);
        shards.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]);
        for copy in sent {
            let result = match copy.shard {
                0 => Err(OxiaError::ShardMoved),
                _ => Ok(()),
            };
            copy.complete(result);
        }
        finish(broadcast, copies).await.unwrap();
    }

    /// A copy whose shard's range has no owner in the assignments cannot be
    /// rerouted: the broadcast fails with `ShardMoved`.
    #[compio::test]
    async fn copy_without_successors_fails_with_shard_moved() {
        let shards = ShardManager::detached_for_tests();
        shards.set_assignments_for_tests(&[(0, 0, 99), (1, 100, 199)]);
        let (broadcast, mut copies) = start_broadcast(shards.clone());

        let [to_0, to_1]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        to_1.complete(Ok(()));
        shards.set_assignments_for_tests(&[(1, 100, 199)]);
        to_0.complete(Err(OxiaError::ShardMoved));
        let result = finish(broadcast, copies).await;
        assert!(
            matches!(result, Err(OxiaError::ShardMoved)),
            "got {result:?}"
        );
    }

    /// The timeout bounds the broadcast as a whole, rerouted copies included.
    #[compio::test]
    async fn broadcast_is_bounded_by_the_timeout() {
        let start = Instant::now();
        let shards = ShardManager::detached_for_tests();
        shards.set_assignments_for_tests(&[(0, 0, 99), (1, 100, 199)]);
        let (broadcast, mut copies) = start_broadcast(shards.clone());

        let [to_0, to_1]: [SentCopy; 2] = next_copies(&mut copies, 2).await.try_into().unwrap();
        to_1.complete(Ok(()));
        compio::time::sleep(TIMEOUT / 2).await;
        shards.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]);
        to_0.complete(Err(OxiaError::ShardMoved));
        // The rerouted copies never complete.
        let _rerouted = next_copies(&mut copies, 2).await;

        let result = finish(broadcast, copies).await;
        assert!(matches!(result, Err(OxiaError::Timeout)), "got {result:?}");
        let elapsed = start.elapsed();
        assert!(
            elapsed >= TIMEOUT && elapsed < TIMEOUT + TIMEOUT / 2,
            "timed out after {elapsed:?}"
        );
    }
}
