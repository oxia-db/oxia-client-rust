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

//! Application-level retry patterns.
//!
//! The client already retries transient failures internally — re-routing via
//! leader hints, re-hashing across shard splits, with backoff, bounded by the
//! request timeout — so every error you observe is post-retry. What remains
//! is application judgment:
//!
//! 1. Semantic outcomes (`KeyNotFound`, `UnexpectedVersionId`) are domain
//!    results to match on, not failures to retry.
//! 2. Errors with `is_retryable() == true` are safe to retry unconditionally
//!    for idempotent operations (gets, version-conditioned writes).
//! 3. An *unconditional* put that failed may still have been applied by the
//!    server (at-least-once under retries) — make writes idempotent with a
//!    version condition to get exactly-once, then retry freely.

use oxia::{OxiaClient, OxiaError, PutResult};
use std::time::Duration;
use tracing::level_filters::LevelFilter;
use tracing::{info, warn};

/// Retries a fallible operation on retryable errors, with exponential
/// backoff, up to `max_attempts`. Request builders implement `IntoFuture`,
/// so `op` can return one directly.
async fn with_retries<T, Fut>(
    max_attempts: u32,
    mut op: impl FnMut() -> Fut,
) -> Result<T, OxiaError>
where
    Fut: IntoFuture<Output = Result<T, OxiaError>>,
{
    let mut backoff = Duration::from_millis(100);
    let mut attempt = 1;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) if e.is_retryable() && attempt < max_attempts => {
                warn!("attempt {attempt} failed ({e}), retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Compare-and-swap loop: re-read and re-apply on version conflicts.
/// Version-conditioned writes are exactly-once, so losing a race is the only
/// way this fails — just refresh and try again.
async fn increment_counter(client: &OxiaClient, key: &str) -> Result<PutResult, OxiaError> {
    loop {
        let (current, version_id) = match client.get(key).await {
            Ok(record) => {
                let n: u64 = String::from_utf8_lossy(record.value.as_deref().unwrap_or(b"0"))
                    .parse()
                    .unwrap_or(0);
                (n, Some(record.version.version_id))
            }
            // Semantic outcome, not a failure: the counter just doesn't exist yet.
            Err(OxiaError::KeyNotFound) => (0, None),
            Err(e) => return Err(e),
        };

        let put = client.put(key, (current + 1).to_string());
        let result = match version_id {
            Some(v) => put.expected_version_id(v).await,
            None => put.expected_record_not_exists().await,
        };
        match result {
            Ok(r) => return Ok(r),
            // Someone else won the race: refresh and retry.
            Err(OxiaError::UnexpectedVersionId) => {
                info!("version conflict on {key}, retrying");
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

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

    // Pattern 1: KeyNotFound is a result, not an error to retry.
    match client.get("retry/missing").await {
        Ok(record) => info!("found {:?}", record.key),
        Err(OxiaError::KeyNotFound) => info!("retry/missing does not exist (expected)"),
        Err(e) => panic!("lookup failed: {e}"),
    }

    // Pattern 2: retry retryable errors for idempotent operations.
    let record = with_retries(5, || client.get("retry/config")).await;
    info!("bounded-retry get returned: {record:?}");

    // Pattern 3: exactly-once increments via CAS, racing three tasks.
    client.delete("retry/counter").await.ok();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..3 {
        let client = client.clone();
        tasks.spawn(async move { increment_counter(&client, "retry/counter").await.unwrap() });
    }
    while tasks.join_next().await.is_some() {}
    let counter = client.get("retry/counter").await.unwrap();
    info!(
        "counter after 3 concurrent CAS increments: {:?}",
        counter.value.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    client.close().await.unwrap();
}
