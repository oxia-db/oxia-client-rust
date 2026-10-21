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
use crate::proto::GetSequenceUpdatesRequest;
use crate::provider_manager::ProviderManager;
use crate::retry::{RetryError, retry_until_cancelled};
use crate::shard_manager::ShardManager;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::info;

pub struct SequenceUpdatesManager {
    context: CancellationToken,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for SequenceUpdatesManager {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

impl SequenceUpdatesManager {
    pub fn new(
        key: String,
        partition_key: String,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        sender: Sender<String>,
    ) -> Self {
        let context = CancellationToken::new();
        let handle = tokio::spawn(start_listener(
            context.clone(),
            key.clone(),
            partition_key.clone(),
            shard_manager.clone(),
            provider_manager.clone(),
            sender.clone(),
        ));
        SequenceUpdatesManager {
            context,
            handle: Mutex::new(Some(handle)),
        }
    }

    pub async fn shutdown(self) -> Result<(), OxiaError> {
        self.context.cancel();
        if let Some(handle) = self.handle.lock().await.take() {
            handle
                .await
                .map_err(|err| OxiaError::Disconnected(err.to_string()))?;
        }
        Ok(())
    }
}

#[inline]
async fn start_listener(
    context: CancellationToken,
    key: String,
    partition_key: String,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    sender: Sender<String>,
) {
    let defer = || {
        let context = context.clone();
        let key = key.clone();
        let partition_key = partition_key.clone();
        let sender = sender.clone();
        let provider_manager = provider_manager.clone();
        let shard_manager = shard_manager.clone();
        async move {
            match shard_manager.get_shard(&partition_key) {
                None => Err(RetryError::transient(OxiaError::NoShardForKey {
                    key: partition_key.clone(),
                })),
                Some(shard) => match shard_manager.get_leader(shard) {
                    None => Err(RetryError::transient(OxiaError::LeaderNotFound { shard })),
                    Some(leader) => {
                        let mut provider = provider_manager
                            .get_provider(leader.service_address)
                            .await
                            .map_err(RetryError::transient)?;
                        let mut streaming = provider
                            .get_sequence_updates(Request::new(GetSequenceUpdatesRequest {
                                shard,
                                key,
                            }))
                            .await
                            .map_err(|err| RetryError::transient(OxiaError::from(err)))?
                            .into_inner();
                        loop {
                            tokio::select! {
                                _ = context.cancelled() => {
                                    info!("Exit sequence update listener due to context canceled.");
                                    return Ok(());
                                },
                                next_response = streaming.next() => {
                                    match next_response {
                                    None => {
                                        info!("Sequence updates stream closed by server.");
                                        return Err(RetryError::transient(OxiaError::Disconnected(
                                            "sequence updates stream closed by server".to_string(),
                                        )));
                                    }
                                    Some(result) => {
                                        let response = result
                                            .map_err(|err| RetryError::transient(OxiaError::from(err)))?;
                                        if sender.send(response.highest_sequence_key).await.is_err() {
                                            // Receiver dropped, stop listening
                                            return Ok(());
                                        }
                                    }}
                                }
                            }
                        }
                    }
                },
            }
        }
    };
    retry_until_cancelled(&context, "sequence-updates", defer).await;
}
