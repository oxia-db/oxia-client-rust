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
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tracing::{info, warn};

struct Inner {
    shard_id: i64,
    id: i64,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
}

struct Session {
    context: CancellationToken,
    handle: Mutex<Option<JoinHandle<()>>>,
    /// Set to `false` by the keep-alive task once the server reports the session
    /// no longer exists, so [`SessionManager::get_session_id`] re-creates it.
    alive: Arc<AtomicBool>,
    request_timeout: Duration,
    inner: Arc<Inner>,
}

impl Session {
    pub fn new(
        shard_id: i64,
        session_id: i64,
        keep_alive_interval: Duration,
        request_timeout: Duration,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
    ) -> Self {
        let context = CancellationToken::new();
        let alive = Arc::new(AtomicBool::new(true));
        let handle = Mutex::new(Some(tokio::spawn(start_keep_alive(
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
            inner: Arc::new(Inner {
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
        self.alive.load(Ordering::Acquire)
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
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    shard_id: i64,
    session_id: i64,
    keep_alive_interval: Duration,
    request_timeout: Duration,
    alive: Arc<AtomicBool>,
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
                                        local_alive.store(false, Ordering::Release);
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
        let mut guard = self.handle.lock().await;
        if let Some(handle) = guard.take() {
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
    sessions: DashMap<i64, OnceCell<Session>>,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
}

impl SessionManager {
    pub(crate) fn new(
        identity: String,
        session_timeout: Duration,
        session_keep_alive: Duration,
        request_timeout: Duration,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
    ) -> Self {
        SessionManager {
            identity,
            session_timeout,
            session_keep_alive,
            request_timeout,
            sessions: DashMap::new(),
            shard_manager,
            provider_manager,
        }
    }

    pub(crate) async fn get_session_id(&self, shard_id: i64) -> Result<i64, OxiaError> {
        let mut session_cell = self.sessions.entry(shard_id).or_default();
        // If a previously created session has expired (its keep-alive saw
        // SESSION_NOT_FOUND), discard it so a fresh one is created below — the
        // caller then transparently gets a working session.
        if session_cell.get().is_some_and(|s| !s.is_alive()) {
            *session_cell = OnceCell::new();
        }
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
        let shards: Vec<i64> = self.sessions.iter().map(|entry| *entry.key()).collect();
        let mut joiner = JoinSet::new();
        for shard in shards {
            if let Some((_, session_cell)) = self.sessions.remove(&shard) {
                if let Some(session) = session_cell.into_inner() {
                    joiner.spawn(session.shutdown());
                }
            }
        }
        while let Some(result) = joiner.join_next().await {
            result.map_err(|err| {
                OxiaError::Disconnected(format!("Session task failed to join: {}", err))
            })??;
        }
        Ok(())
    }
}
