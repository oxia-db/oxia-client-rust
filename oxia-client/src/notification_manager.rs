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

//! Per-shard notification subscriptions for one [`Notifications`] handle.
//!
//! One listener task per shard streams that shard's change notifications into a
//! shared channel, reconnecting (resuming from the last delivered offset) after
//! connection loss. All listeners share one [`CancellationToken`], so dropping
//! the manager — which happens when the [`Notifications`] handle is dropped or
//! the client is closed — stops every listener promptly.
//!
//! Once its shard is split, a listener hands over to listeners on the shards
//! that replaced it, which resume after the last batch it received: each of
//! them holds the batches of the split shard, and delivers the notifications
//! on its side of the split before its own, so that each notification is
//! delivered once.
//!
//! Notifications from one shard are delivered in the order the server applied
//! them (the batch is a server-sorted `repeated NotificationEntry`); ordering
//! across shards is unspecified.
//!
//! [`Notifications`]: crate::Notifications

use crate::errors::OxiaError;
use crate::proto::NotificationsRequest;
use crate::provider_manager::ProviderManager;
use crate::retry::{RetryError, retry_until_cancelled};
use crate::shard_manager::{ShardManager, ShardRange};
use crate::types::Notification;
use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::{debug, info, warn};

pub(crate) struct NotificationManager {
    context: CancellationToken,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl Drop for NotificationManager {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

impl NotificationManager {
    /// Spawns a listener per shard. Each reports once, via `init_tx`, whether
    /// it established its first subscription (`Ok`) or hit a permanent error
    /// (`Err`); the caller waits on these to make the subscription live before
    /// returning to the user. The listener of a shard split before its first
    /// subscription reports once the listeners of the shards that replaced it
    /// have established theirs.
    pub(crate) fn new(
        shards: Vec<ShardRange>,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        sender: Sender<Notification>,
        init_tx: tokio::sync::mpsc::Sender<Result<(), OxiaError>>,
    ) -> Self {
        let context = CancellationToken::new();
        let subscription = Arc::new(Subscription {
            shard_manager,
            provider_manager,
            sender,
            context: context.clone(),
        });
        let handles = shards
            .into_iter()
            .map(|range| {
                let init = Arc::new(ShardInit::new(init_tx.clone()));
                tokio::spawn(listen(subscription.clone(), range, None, Some(init)))
            })
            .collect();
        NotificationManager {
            context,
            handles: Mutex::new(handles),
        }
    }

    pub(crate) async fn shutdown(self) -> Result<(), OxiaError> {
        self.context.cancel();
        for handle in self.handles.lock().await.drain(..) {
            handle
                .await
                .map_err(|err| OxiaError::Disconnected(err.to_string()))?;
        }
        Ok(())
    }
}

/// What the listeners of one subscription share.
struct Subscription {
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    sender: Sender<Notification>,
    context: CancellationToken,
}

impl Subscription {
    /// The shards that replaced the shard of `range` once it was split: it is
    /// no longer assigned, and other shards own its hash range. `None` while
    /// it is assigned.
    fn successors(&self, range: &ShardRange) -> Option<Vec<ShardRange>> {
        if self.shard_manager.shard_exists(range.shard) {
            return None;
        }
        let successors = self.shard_manager.get_successors(range);
        (!successors.is_empty()).then_some(successors)
    }
}

/// The wait for the subscription to be established on a shard of the
/// assignments it started with: once the shard's listener has received its
/// first batch or, if the shard is split before, once the listeners of the
/// shards that replaced it have received theirs.
struct ShardInit {
    /// The listeners that have yet to receive their first batch.
    pending: AtomicUsize,
    tx: tokio::sync::mpsc::Sender<Result<(), OxiaError>>,
}

impl ShardInit {
    fn new(tx: tokio::sync::mpsc::Sender<Result<(), OxiaError>>) -> Self {
        ShardInit {
            pending: AtomicUsize::new(1),
            tx,
        }
    }

    /// One of the listeners received its first batch.
    async fn established(&self) {
        if self.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _ = self.tx.send(Ok(())).await;
        }
    }
}

/// What a listener keeps across its attempts.
struct ListenerState {
    /// The offset of the last batch received, tracked across reconnects, or
    /// `None` before the first: the first request then omits the offset,
    /// which the server answers with an initial empty batch that primes the
    /// notification cursor. Reconnects resume from the last offset so nothing
    /// in the gap is missed.
    last_offset: Option<i64>,
    /// The wait for the subscription to be established, until the listener
    /// receives its first batch: a reconnect must not signal it again, or the
    /// init wait could over-count and return before a slower shard is live.
    init: Option<Arc<ShardInit>>,
    /// The shards that replaced the listener's shard, once it found it split.
    successors: Option<Vec<ShardRange>>,
}

/// Listens to the notifications of the shard of `range`, after the batch at
/// `last_offset`, or from where the server starts a new subscription, until
/// the subscription stops. Once the shard is split, listens to the shards that
/// replaced it instead, after the last batch received from it.
fn listen(
    subscription: Arc<Subscription>,
    range: ShardRange,
    last_offset: Option<i64>,
    init: Option<Arc<ShardInit>>,
) -> BoxFuture<'static, ()> {
    async move {
        let state = Arc::new(std::sync::Mutex::new(ListenerState {
            last_offset,
            init,
            successors: None,
        }));
        retry_until_cancelled(&subscription.context, "notifications", || {
            stream_notifications(subscription.clone(), range, state.clone())
        })
        .await;

        let (successors, last_offset, init) = {
            let mut state = state.lock().expect("notification listener poisoned");
            (
                state.successors.take(),
                state.last_offset,
                state.init.take(),
            )
        };
        let Some(successors) = successors else {
            return;
        };
        if subscription.context.is_cancelled() {
            return;
        }
        let successor_ids: Vec<i64> = successors.iter().map(|successor| successor.shard).collect();
        info!(
            shard = range.shard,
            successors = ?successor_ids,
            offset = ?last_offset,
            "notification listener following the split of its shard"
        );
        if let Some(init) = &init {
            // The listeners of the successors establish the subscription in
            // place of this one.
            init.pending
                .fetch_add(successors.len() - 1, Ordering::AcqRel);
        }
        join_all(
            successors.into_iter().map(|successor| {
                listen(subscription.clone(), successor, last_offset, init.clone())
            }),
        )
        .await;
    }
    .boxed()
}

/// One attempt of a listener: streams the notifications of the shard of
/// `range` until the subscription stops, the stream fails, or the shard turns
/// out split, which it records in `state` for the listener to follow.
async fn stream_notifications(
    subscription: Arc<Subscription>,
    range: ShardRange,
    state: Arc<std::sync::Mutex<ListenerState>>,
) -> Result<(), RetryError> {
    let shard_id = range.shard;
    // Taken before the shard is looked up: a split from then on ends the
    // stream, which the node of the split shard may keep open until it
    // deletes the shard.
    let mut changes = subscription.shard_manager.subscribe_changes();
    let split = || match subscription.successors(&range) {
        Some(successors) => {
            state
                .lock()
                .expect("notification listener poisoned")
                .successors = Some(successors);
            true
        }
        None => false,
    };
    if split() {
        return Ok(());
    }

    let mut provider = match subscription.shard_manager.get_leader(shard_id) {
        None => {
            return Err(RetryError::transient(OxiaError::LeaderNotFound {
                shard: shard_id,
            }));
        }
        Some(leader) => subscription
            .provider_manager
            .get_provider(leader.service_address)
            .await
            .map_err(RetryError::transient)?,
    };
    let start_offset_exclusive = state
        .lock()
        .expect("notification listener poisoned")
        .last_offset;
    let mut streaming = match provider
        .get_notifications(NotificationsRequest {
            shard: shard_id,
            start_offset_exclusive,
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) => {
            let err = OxiaError::from(status);
            // A permanent failure (e.g. notifications disabled server-side)
            // is reported to the caller so the subscription call fails fast
            // instead of retrying forever.
            if err.is_retryable() {
                return Err(RetryError::transient(err));
            }
            let init = state
                .lock()
                .expect("notification listener poisoned")
                .init
                .take();
            if let Some(init) = init {
                let _ = init.tx.send(Err(err.clone())).await;
            }
            return Err(RetryError::fatal(err));
        }
    };
    loop {
        tokio::select! {
            _ = subscription.context.cancelled() => {
                debug!(shard = shard_id, "notification listener stopped: cancelled");
                return Ok(());
            }
            Ok(()) = changes.changed() => {
                if split() {
                    return Ok(());
                }
            }
            message = streaming.next() => match message {
                None => {
                    return Err(RetryError::transient(OxiaError::Disconnected(
                        "notification stream closed by server".to_string(),
                    )));
                }
                Some(notification) => {
                    let batch = notification
                        .map_err(|err| RetryError::transient(OxiaError::from(err)))?;
                    let init = {
                        let mut state = state.lock().expect("notification listener poisoned");
                        state.last_offset = Some(batch.offset);
                        state.init.take()
                    };
                    // The first batch received confirms the subscription
                    // is live: it is the server's empty cursor-priming
                    // batch, so the loop below emits nothing for it.
                    if let Some(init) = init {
                        init.established().await;
                    }
                    for entry in batch.notifications {
                        let Some(notification) = Notification::from_proto(entry) else {
                            warn!("skipping unrecognized notification type");
                            continue;
                        };
                        if subscription.sender.send(notification).await.is_err() {
                            // The Notifications handle was dropped.
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}
