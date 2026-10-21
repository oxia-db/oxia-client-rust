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
use crate::retry::{RetryError, retry_until_cancelled};
use crate::server_error::decode_status;
use crate::shard_manager::ShardManager;
use compio::runtime::JoinHandle;
use compio::time::interval;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::OnceCell;
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
        let session = Self {
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
        };
        info!(
            "Created a new session. shard_id={:?} session_id={:?}",
            shard_id, session_id
        );
        session
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
}

impl SessionManager {
    pub(crate) fn new(
        identity: String,
        session_timeout: Duration,
        session_keep_alive: Duration,
        request_timeout: Duration,
        shard_manager: Rc<ShardManager>,
        provider_manager: Rc<ProviderManager>,
    ) -> Self {
        SessionManager {
            identity,
            session_timeout,
            session_keep_alive,
            request_timeout,
            sessions: RefCell::new(HashMap::new()),
            shard_manager,
            provider_manager,
        }
    }

    pub(crate) async fn get_session_id(&self, shard_id: i64) -> Result<i64, OxiaError> {
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
            .get_or_try_init(|| async {
                match self.shard_manager.get_leader(shard_id) {
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
                        Ok(Session::new(
                            shard_id,
                            session_id,
                            self.session_keep_alive,
                            self.request_timeout,
                            self.shard_manager.clone(),
                            self.provider_manager.clone(),
                        ))
                    }
                }
            })
            .await?;
        Ok(session.inner.id)
    }

    pub(crate) async fn close(&self) -> Result<(), OxiaError> {
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
