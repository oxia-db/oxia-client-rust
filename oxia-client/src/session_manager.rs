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

use crate::errors::OxiaError;
use crate::proto::{CloseSessionRequest, CreateSessionRequest, SessionHeartbeat};
use crate::provider_manager::ProviderManager;
use crate::retry::{RetryError, retry_until, retry_until_cancelled};
use crate::server_error::decode_status;
use crate::shard_manager::ShardManager;
use compio::runtime::JoinHandle;
use compio::time::interval;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::time::Duration;
use tokio::sync::{OnceCell, watch};
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tracing::{info, warn};

struct Inner {
    shard_id: i64,
    id: i64,
    shard_manager: Rc<ShardManager>,
    provider_manager: Rc<ProviderManager>,
}

struct Session {
    context: CancellationToken,
    handle: RefCell<Option<JoinHandle<()>>>,
    /// Set to `false` by the keep-alive task once the server reports the session
    /// no longer exists, so [`SessionManager::get_session_id`] re-creates it.
    alive: Rc<Cell<bool>>,
    request_timeout: Duration,
    inner: Rc<Inner>,
}

impl Session {
    pub fn new(
        shard_id: i64,
        session_id: i64,
        keep_alive_interval: Duration,
        request_timeout: Duration,
        shard_manager: Rc<ShardManager>,
        provider_manager: Rc<ProviderManager>,
    ) -> Self {
        let context = CancellationToken::new();
        let alive = Rc::new(Cell::new(true));
        let handle = RefCell::new(Some(compio::runtime::spawn(start_keep_alive(
            context.clone(),
            shard_manager.clone(),
            provider_manager.clone(),
            shard_id,
            session_id,
            keep_alive_interval,
            request_timeout,
            alive.clone(),
        ))));
        Self {
            handle,
            context,
            alive,
            request_timeout,
            inner: Rc::new(Inner {
                id: session_id,
                shard_id,
                shard_manager,
                provider_manager,
            }),
        }
    }

    /// Whether the session is still believed valid. Turns `false` once the
    /// keep-alive observes that the server has expired it.
    fn is_alive(&self) -> bool {
        self.alive.get()
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_keep_alive(
    context: CancellationToken,
    shard_manager: Rc<ShardManager>,
    provider_manager: Rc<ProviderManager>,
    shard_id: i64,
    session_id: i64,
    keep_alive_interval: Duration,
    request_timeout: Duration,
    alive: Rc<Cell<bool>>,
) {
    let op_defer = || {
        let local_shard_manager = shard_manager.clone();
        let local_provider_manager = provider_manager.clone();
        let local_context = context.clone();
        let local_alive = alive.clone();
        async move {
            match local_shard_manager.get_leader(shard_id) {
                None => Err(RetryError::transient(OxiaError::LeaderNotFound {
                    shard: shard_id,
                })),
                Some(leader) => {
                    let mut provider = local_provider_manager
                        .get_provider(leader.service_address)
                        .await
                        .map_err(RetryError::transient)?;
                    let mut ticker = interval(keep_alive_interval);
                    loop {
                        tokio::select! {
                            _ = local_context.cancelled() => {
                                info!("Session keep-alive exit due to cancellation.");
                                return Ok(());
                            },
                            _ = ticker.tick() => {
                                let mut heartbeat = Request::new(SessionHeartbeat { shard: shard_id, session_id });
                                heartbeat.set_timeout(request_timeout);
                                if let Err(err) = provider.keep_alive(heartbeat).await
                                {
                                    // The server reports an expired session as
                                    // NotFound with ErrorInfo reason SESSION_NOT_FOUND.
                                    let decoded = decode_status(err);
                                    if matches!(decoded.error, OxiaError::SessionExpired) {
                                        info!("Session {session_id} on shard {shard_id} expired; it will be re-created on next use.");
                                        // Mark dead so get_session_id re-creates it, then stop.
                                        local_alive.set(false);
                                        return Err(RetryError::fatal(OxiaError::SessionExpired));
                                    }
                                    return Err(RetryError::transient(decoded.error));
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    retry_until_cancelled(&context, "session-keep-alive", op_defer).await;
}

impl Session {
    pub(crate) async fn shutdown(self) -> Result<(), OxiaError> {
        let shard_manager = self.inner.shard_manager.clone();
        let provider_manager = self.inner.provider_manager.clone();
        if let Some(node) = shard_manager.get_leader(self.inner.shard_id) {
            let mut provider = provider_manager.get_provider(node.service_address).await?;
            let mut close = Request::new(CloseSessionRequest {
                shard: self.inner.shard_id,
                session_id: self.inner.id,
            });
            close.set_timeout(self.request_timeout);
            let result = provider.close_session(close).await.map_err(OxiaError::from);
            if let Err(err) = result {
                warn!(
                    "Failed to close session. shard_id={:?} session_id={:?} error={:?}",
                    self.inner.shard_id, self.inner.id, err
                );
            }
        } else {
            warn!("Shard leader not found. shard_id={:?}", self.inner.shard_id);
        }

        self.context.cancel();
        let handle = self.handle.borrow_mut().take();
        if let Some(handle) = handle {
            handle
                .await
                .map_err(|err| OxiaError::Disconnected(err.to_string()))?
        }
        Ok(())
    }
}

pub(crate) struct SessionManager {
    identity: String,
    session_timeout: Duration,
    session_keep_alive: Duration,
    request_timeout: Duration,
    sessions: RefCell<HashMap<i64, Rc<OnceCell<Session>>>>,
    shard_manager: Rc<ShardManager>,
    provider_manager: Rc<ProviderManager>,
    /// Cancels the task that follows the splits of the shards with a session.
    context: CancellationToken,
    follow_splits_handle: RefCell<Option<JoinHandle<()>>>,
}

/// Has `manager` follow the splits of the shards with a session on every
/// change of the shard assignments, until the manager is dropped or `context`
/// is cancelled.
async fn follow_splits_on_assignments_change(
    context: CancellationToken,
    mut changes: watch::Receiver<u64>,
    manager: Weak<SessionManager>,
) {
    loop {
        tokio::select! {
            _ = context.cancelled() => return,
            changed = changes.changed() => {
                // The sender lives in the shard manager, which the session
                // manager holds, so this can only fail once that is gone.
                if changed.is_err() {
                    return;
                }
                match manager.upgrade() {
                    None => return,
                    Some(manager) => manager.follow_splits(),
                }
            }
        }
    }
}

impl SessionManager {
    pub(crate) fn new(
        identity: String,
        session_timeout: Duration,
        session_keep_alive: Duration,
        request_timeout: Duration,
        shard_manager: Rc<ShardManager>,
        provider_manager: Rc<ProviderManager>,
    ) -> Rc<Self> {
        let manager = Rc::new(SessionManager {
            identity,
            session_timeout,
            session_keep_alive,
            request_timeout,
            sessions: RefCell::new(HashMap::new()),
            shard_manager: shard_manager.clone(),
            provider_manager,
            context: CancellationToken::new(),
            follow_splits_handle: RefCell::new(None),
        });
        // Subscribe here rather than in the task, which starts later: a
        // change of the assignments in between would be missed.
        let changes = shard_manager.subscribe_changes();
        // Weak, so the task does not keep the manager alive: it stops as soon
        // as the client drops it.
        let handle = compio::runtime::spawn(follow_splits_on_assignments_change(
            manager.context.clone(),
            changes,
            Rc::downgrade(&manager),
        ));
        *manager.follow_splits_handle.borrow_mut() = Some(handle);
        manager
    }

    /// Moves the sessions of the shards that were split or merged away to the
    /// shards that replaced them. Each of those inherited the session, with
    /// the ephemeral records of its own hash range, and expires it — deleting
    /// those records — unless the session is kept alive there too. The shards
    /// that replaced a shard started to count the session's timeout when they
    /// were elected, before the client could know them, so this runs on every
    /// change of the assignments as well as before a session is created.
    fn follow_splits(&self) {
        let mut sessions = self.sessions.borrow_mut();
        let split: Vec<i64> = sessions
            .keys()
            .copied()
            .filter(|shard_id| !self.shard_manager.shard_exists(*shard_id))
            .collect();
        for shard_id in split {
            // Only an established, live session can be inherited: one still
            // being created has no id to inherit, and the operation waiting
            // for it re-routes to the shard that took over its key.
            let Some(session_id) = sessions
                .get(&shard_id)
                .and_then(|session_cell| session_cell.get())
                .filter(|session| session.is_alive())
                .map(|session| session.inner.id)
            else {
                continue;
            };
            let successors = self.shard_manager.current_successors(shard_id);
            if successors.is_empty() {
                continue;
            }
            info!(
                "Moving the session to the shards that replaced its shard. \
                 shard_id={shard_id:?} session_id={session_id:?} shards={successors:?}"
            );
            for successor in successors {
                // A shard that already has a session of its own keeps it.
                sessions.entry(successor).or_insert_with(|| {
                    Rc::new(OnceCell::new_with(Some(
                        self.new_session(successor, session_id),
                    )))
                });
            }
            // Dropping the session stops its keep-alive. The session is not
            // closed server-side: the shard that held it is gone, and the
            // shards that replaced it now keep the session alive.
            sessions.remove(&shard_id);
        }
    }

    /// A session with `session_id` on `shard_id`, kept alive from now on.
    fn new_session(&self, shard_id: i64, session_id: i64) -> Session {
        Session::new(
            shard_id,
            session_id,
            self.session_keep_alive,
            self.request_timeout,
            self.shard_manager.clone(),
            self.provider_manager.clone(),
        )
    }

    pub(crate) async fn get_session_id(&self, shard_id: i64) -> Result<i64, OxiaError> {
        if !self.sessions.borrow().contains_key(&shard_id) {
            // The shard may have replaced a shard that was split and
            // inherited its session, before the task that follows the splits
            // could move it here.
            self.follow_splits();
        }
        // Take the cell out of the map: the map must not stay borrowed while
        // the session is created. The callers that need the session of a
        // shard while it is being created wait for the same one.
        let session_cell = {
            let mut sessions = self.sessions.borrow_mut();
            let session_cell = sessions.entry(shard_id).or_default();
            // If a previously created session has expired (its keep-alive saw
            // SESSION_NOT_FOUND), discard it so a fresh one is created below —
            // the caller then transparently gets a working session.
            if session_cell.get().is_some_and(|s| !s.is_alive()) {
                *session_cell = Rc::default();
            }
            session_cell.clone()
        };
        let session = session_cell
            .get_or_try_init(|| {
                // Retry while the shard has no leader yet (one is being
                // elected) or its server is briefly unavailable, as the Go
                // client's session creation does: an operation that needs a
                // session must not fail where the same operation without one
                // would be retried. A shard that is gone is not retried here:
                // `ShardMoved` has the caller re-route to the shard that took
                // over its key.
                retry_until(
                    self.request_timeout,
                    |err| err.is_retryable() && !matches!(err, OxiaError::ShardMoved),
                    || self.create_session(shard_id),
                )
            })
            .await?;
        Ok(session.inner.id)
    }

    /// Creates a session on `shard_id`, kept alive from now on.
    async fn create_session(&self, shard_id: i64) -> Result<Session, OxiaError> {
        match self.shard_manager.get_leader(shard_id) {
            // The shard was split or merged away while the operation was
            // routed to it, as the Go client's session creation handles when a
            // split interrupts it.
            None if !self.shard_manager.shard_exists(shard_id) => Err(OxiaError::ShardMoved),
            None => Err(OxiaError::LeaderNotFound { shard: shard_id }),
            Some(node) => {
                let mut provider = self
                    .provider_manager
                    .get_provider(node.service_address)
                    .await?;
                let mut create = Request::new(CreateSessionRequest {
                    shard: shard_id,
                    session_timeout_ms: self.session_timeout.as_millis() as u32,
                    client_identity: self.identity.clone(),
                });
                create.set_timeout(self.request_timeout);
                let response = provider
                    .create_session(create)
                    .await
                    .map_err(OxiaError::from)?;
                let session_id = response.into_inner().session_id;
                info!(
                    "Created a new session. shard_id={:?} session_id={:?}",
                    shard_id, session_id
                );
                Ok(self.new_session(shard_id, session_id))
            }
        }
    }

    pub(crate) async fn close(&self) -> Result<(), OxiaError> {
        self.context.cancel();
        let follow_splits_handle = self.follow_splits_handle.borrow_mut().take();
        if let Some(handle) = follow_splits_handle {
            handle
                .await
                .map_err(|err| OxiaError::Disconnected(err.to_string()))?;
        }
        let session_cells: Vec<Rc<OnceCell<Session>>> = self
            .sessions
            .borrow_mut()
            .drain()
            .map(|(_, session_cell)| session_cell)
            .collect();
        // A cell still shared is a session being created: its creator keeps it.
        let sessions = session_cells
            .into_iter()
            .filter_map(|session_cell| Rc::into_inner(session_cell)?.into_inner());
        for result in futures::future::join_all(sessions.map(Session::shutdown)).await {
            result?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A manager whose shard assignments the test drives, with no server to
    /// create sessions on: the tests establish the sessions themselves.
    fn session_manager(shard_manager: Rc<ShardManager>) -> Rc<SessionManager> {
        SessionManager::new(
            "test-client".to_string(),
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::from_secs(1),
            shard_manager,
            Rc::new(ProviderManager::new(Duration::from_secs(1))),
        )
    }

    /// Adds an established session on `shard_id`, as a successful
    /// `CreateSession` would. Its keep-alive runs against an address that
    /// does not resolve, which only makes it retry in the background.
    fn establish(manager: &SessionManager, shard_id: i64, session_id: i64) {
        let session = manager.new_session(shard_id, session_id);
        manager
            .sessions
            .borrow_mut()
            .insert(shard_id, Rc::new(OnceCell::new_with(Some(session))));
    }

    /// The id of the session of each shard that has an established one, by
    /// shard. A shard whose session is still being created is reported with
    /// `None`.
    fn sessions_by_shard(manager: &SessionManager) -> Vec<(i64, Option<i64>)> {
        let mut sessions: Vec<(i64, Option<i64>)> = manager
            .sessions
            .borrow()
            .iter()
            .map(|(shard_id, session_cell)| {
                (
                    *shard_id,
                    session_cell.get().map(|session| session.inner.id),
                )
            })
            .collect();
        sessions.sort_unstable();
        sessions
    }

    #[compio::test]
    async fn a_split_moves_the_session_to_the_shards_that_replaced_it() {
        let shard_manager = ShardManager::detached_for_tests();
        shard_manager.set_assignments_for_tests(&[(1, 0, u32::MAX)]);
        let manager = session_manager(shard_manager.clone());
        establish(&manager, 1, 42);

        shard_manager.set_assignments_for_tests(&[(2, 0, 100), (3, 101, u32::MAX)]);
        manager.follow_splits();

        // Both shards that replaced shard 1 keep its session alive: each
        // inherited the ephemeral records in its own hash range.
        assert_eq!(
            sessions_by_shard(&manager),
            vec![(2, Some(42)), (3, Some(42))]
        );
    }

    #[compio::test]
    async fn the_session_follows_a_successor_that_was_split_in_turn() {
        let shard_manager = ShardManager::detached_for_tests();
        shard_manager.set_assignments_for_tests(&[(1, 0, u32::MAX)]);
        let manager = session_manager(shard_manager.clone());
        establish(&manager, 1, 42);

        // Shard 1 is replaced by 2 and 3, and 3 by 4 and 5, before the
        // session has followed the first split.
        shard_manager.set_assignments_for_tests(&[(2, 0, 100), (3, 101, u32::MAX)]);
        shard_manager.set_assignments_for_tests(&[(2, 0, 100), (4, 101, 200), (5, 201, u32::MAX)]);
        manager.follow_splits();

        assert_eq!(
            sessions_by_shard(&manager),
            vec![(2, Some(42)), (4, Some(42)), (5, Some(42))]
        );
    }

    #[compio::test]
    async fn a_shard_that_already_has_a_session_keeps_it() {
        let shard_manager = ShardManager::detached_for_tests();
        shard_manager.set_assignments_for_tests(&[(1, 0, 100), (2, 101, u32::MAX)]);
        let manager = session_manager(shard_manager.clone());
        establish(&manager, 1, 42);
        establish(&manager, 2, 7);

        // Shard 1 is merged into shard 2, which has a session of its own.
        shard_manager.set_assignments_for_tests(&[(2, 0, u32::MAX)]);
        manager.follow_splits();

        assert_eq!(sessions_by_shard(&manager), vec![(2, Some(7))]);
    }

    #[compio::test]
    async fn a_session_still_being_created_is_not_moved() {
        let shard_manager = ShardManager::detached_for_tests();
        shard_manager.set_assignments_for_tests(&[(1, 0, u32::MAX)]);
        let manager = session_manager(shard_manager.clone());
        // A cell with no session yet: its `CreateSession` is in flight.
        manager
            .sessions
            .borrow_mut()
            .insert(1, Rc::new(OnceCell::new()));

        shard_manager.set_assignments_for_tests(&[(2, 0, 100), (3, 101, u32::MAX)]);
        manager.follow_splits();

        // There is no session id to inherit; the operation waiting for it
        // re-routes to the shard that took over its key.
        assert_eq!(sessions_by_shard(&manager), vec![(1, None)]);
    }

    #[compio::test]
    async fn a_change_of_the_assignments_moves_the_session_on_its_own() {
        let shard_manager = ShardManager::detached_for_tests();
        shard_manager.set_assignments_for_tests(&[(1, 0, u32::MAX)]);
        let manager = session_manager(shard_manager.clone());
        establish(&manager, 1, 42);

        shard_manager.set_assignments_for_tests(&[(2, 0, 100), (3, 101, u32::MAX)]);
        // Nobody asks for a session: the task that watches the assignments is
        // what keeps the inherited session alive on the new shards.
        compio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            sessions_by_shard(&manager),
            vec![(2, Some(42)), (3, Some(42))]
        );
    }

    #[compio::test]
    async fn a_session_of_a_shard_that_is_gone_is_not_created_again() {
        let shard_manager = ShardManager::detached_for_tests();
        shard_manager.set_assignments_for_tests(&[(1, 0, u32::MAX)]);
        let manager = session_manager(shard_manager.clone());

        shard_manager.set_assignments_for_tests(&[(2, 0, u32::MAX)]);

        // The write was routed to shard 1 before the split: it must re-route
        // rather than wait for a leader of a shard that no longer exists.
        assert!(matches!(
            manager.get_session_id(1).await,
            Err(OxiaError::ShardMoved)
        ));
    }
}
