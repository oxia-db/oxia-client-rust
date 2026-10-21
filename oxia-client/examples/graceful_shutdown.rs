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

//! Graceful shutdown: stop producing, drain in-flight operations, then close.
//!
//! `close()` flushes operations already submitted to batches before tearing
//! the client down, and ephemeral records are removed with the session. The
//! application's job is the ordering: (1) stop issuing new operations,
//! (2) wait for in-flight ones, (3) `close()`. Operations still racing a
//! closed client fail with `OxiaError::Closed` — treat that as shutdown, not
//! as an error.
//!
//! Runs until Ctrl-C (or stops by itself after 10 seconds).

use oxia::{OxiaClient, OxiaError};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::level_filters::LevelFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_target(false)
        .init();

    let client = OxiaClient::connect("localhost:6648").await.unwrap();
    let shutdown = CancellationToken::new();

    // A few producers writing continuously. Each checks the shutdown signal
    // between operations and treats `Closed` as a normal way to stop.
    let mut producers = tokio::task::JoinSet::new();
    for worker in 0..4 {
        let client = client.clone();
        let shutdown = shutdown.clone();
        producers.spawn(async move {
            let mut written = 0u64;
            while !shutdown.is_cancelled() {
                let key = format!("shutdown/w{worker}/{written}");
                match client.put(key, "payload").await {
                    Ok(_) => written += 1,
                    // The client was closed while this op was in flight:
                    // shutdown has begun, just stop.
                    Err(OxiaError::Closed) => break,
                    Err(e) => panic!("write failed: {e}"),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            written
        });
    }
    info!("Producers running — Ctrl-C to shut down");

    // Wait for Ctrl-C (or a timer, so the example ends on its own).
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("Ctrl-C received"),
        _ = tokio::time::sleep(Duration::from_secs(10)) => info!("Demo timer elapsed"),
    }

    // 1. Stop issuing new operations.
    shutdown.cancel();

    // 2. Let every in-flight operation finish.
    let mut total = 0u64;
    while let Some(written) = producers.join_next().await {
        total += written.unwrap();
    }
    info!("Producers drained: {total} writes completed");

    // 3. Close: flushes anything still batched, then releases the session.
    client.close().await.unwrap();
    info!("Client closed cleanly");
}
