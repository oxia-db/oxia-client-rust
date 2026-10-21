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

//! Retry loop for the client's long-running background tasks (shard-assignment
//! watcher, session keep-alive, notification and sequence-update listeners).
//!
//! This replaces the unmaintained `backoff` crate. Unlike `backoff`'s default
//! policy, there is **no** maximum elapsed time: a background loop retries until
//! its [`CancellationToken`] is cancelled, so a transient outage never silently
//! kills the loop. The delay resets after any attempt that ran long enough to
//! look healthy, so a stream that worked for a while before a hiccup reconnects
//! promptly instead of waiting out a maxed-out backoff.

use crate::errors::OxiaError;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const INITIAL_DELAY: Duration = Duration::from_millis(100);
const MAX_DELAY: Duration = Duration::from_secs(30);
/// An attempt that ran at least this long is treated as progress and resets the
/// backoff, so long-lived streams reconnect quickly after an occasional error.
const RESET_AFTER: Duration = Duration::from_secs(10);

/// Backoff for operation-level retries (failed batches, stream opens).
/// Attempts are bounded by the request timeout, so the cap stays low.
const OP_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100);
const OP_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

/// The delay before retry `attempt` (0-based) of a failed operation.
pub(crate) fn retry_delay(attempt: u32) -> Duration {
    OP_RETRY_INITIAL_DELAY
        .saturating_mul(1u32 << attempt.min(16))
        .min(OP_RETRY_MAX_DELAY)
}

/// Repeatedly runs `attempt` until it returns a value or an error for which
/// `is_retryable` is false, or `timeout` elapses (returning the last retryable
/// error). Backs off between attempts. This drives the client's operation-level
/// re-routing/retries (shard split/merge, assignments not yet loaded).
pub(crate) async fn retry_until<T, A, Fut>(
    timeout: Duration,
    is_retryable: impl Fn(&OxiaError) -> bool,
    mut attempt: A,
) -> Result<T, OxiaError>
where
    A: FnMut() -> Fut,
    Fut: Future<Output = Result<T, OxiaError>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    let mut n = 0u32;
    loop {
        match attempt().await {
            Err(err) if is_retryable(&err) => {
                let delay = retry_delay(n);
                n += 1;
                if tokio::time::Instant::now() + delay >= deadline {
                    return Err(err);
                }
                tokio::time::sleep(delay).await;
            }
            other => return other,
        }
    }
}

/// The failure mode of a single attempt of a retried operation.
pub(crate) enum RetryError {
    /// A transient failure; retry after a backoff delay.
    Transient(OxiaError),
    /// A permanent failure; stop the loop without retrying.
    Fatal(OxiaError),
}

impl RetryError {
    /// Marks `err` as transient (will be retried). Usable as a `map_err` fn.
    pub(crate) fn transient(err: OxiaError) -> Self {
        RetryError::Transient(err)
    }

    /// Marks `err` as permanent (stops the loop). Usable as a `map_err` fn.
    pub(crate) fn fatal(err: OxiaError) -> Self {
        RetryError::Fatal(err)
    }
}

struct Backoff {
    current: Duration,
}

impl Backoff {
    fn new() -> Self {
        Backoff {
            current: INITIAL_DELAY,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(MAX_DELAY);
        delay
    }

    fn reset(&mut self) {
        self.current = INITIAL_DELAY;
    }
}

/// Runs `op` repeatedly until it returns `Ok(())` (finished gracefully) or a
/// [`RetryError::Fatal`], or `token` is cancelled.
///
/// [`RetryError::Transient`] results are retried with unbounded exponential
/// backoff. Cancellation interrupts the delay between attempts; the operation
/// itself is expected to observe `token` (typically via `tokio::select!`) to
/// interrupt a live attempt.
pub(crate) async fn retry_until_cancelled<F, Fut>(token: &CancellationToken, what: &str, mut op: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), RetryError>>,
{
    let mut backoff = Backoff::new();
    loop {
        let started = Instant::now();
        let result = op().await;
        let healthy = started.elapsed() >= RESET_AFTER;
        match result {
            Ok(()) => return,
            Err(RetryError::Fatal(err)) => {
                warn!("Background task '{what}' stopped permanently: {err}");
                return;
            }
            Err(RetryError::Transient(err)) => {
                if healthy {
                    backoff.reset();
                }
                let delay = backoff.next_delay();
                warn!("Background task '{what}' failed, retrying in {delay:?}: {err}");
                tokio::select! {
                    _ = token.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_caps_and_resets() {
        let mut b = Backoff::new();
        assert_eq!(b.next_delay(), Duration::from_millis(100));
        assert_eq!(b.next_delay(), Duration::from_millis(200));
        assert_eq!(b.next_delay(), Duration::from_millis(400));
        // Keep growing until it saturates at MAX_DELAY.
        for _ in 0..20 {
            b.next_delay();
        }
        assert_eq!(b.next_delay(), MAX_DELAY);
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_millis(100));
    }

    #[tokio::test]
    async fn stops_on_cancellation_during_delay() {
        let token = CancellationToken::new();
        token.cancel();
        // Every attempt fails transiently; with the token already cancelled the
        // loop must return instead of retrying forever.
        retry_until_cancelled(&token, "test", || async {
            Err(RetryError::transient(OxiaError::Timeout))
        })
        .await;
    }

    #[tokio::test]
    async fn retry_until_returns_ok_without_retrying() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = AtomicU32::new(0);
        let out: Result<u32, OxiaError> = retry_until(
            Duration::from_secs(60),
            |_| true,
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(7)
            },
        )
        .await;
        assert_eq!(out.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_until_returns_non_retryable_immediately() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = AtomicU32::new(0);
        let out: Result<(), OxiaError> = retry_until(
            Duration::from_secs(60),
            |e| e.is_retryable(),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(OxiaError::KeyNotFound)
            },
        )
        .await;
        assert!(matches!(out, Err(OxiaError::KeyNotFound)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_until_retries_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering};
        // Fail with a retryable error twice (e.g. shard still moving), then
        // succeed once the new assignment lands. Paused time makes the backoff
        // sleeps instant.
        let calls = AtomicU32::new(0);
        let out: Result<u32, OxiaError> = retry_until(
            Duration::from_secs(60),
            |e| e.is_retryable(),
            || async {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(OxiaError::ShardMoved)
                } else {
                    Ok(n)
                }
            },
        )
        .await;
        assert_eq!(out.unwrap(), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_until_gives_up_at_deadline_with_last_error() {
        let out: Result<(), OxiaError> = retry_until(
            Duration::from_secs(1),
            |e| e.is_retryable(),
            || async { Err(OxiaError::ShardMoved) },
        )
        .await;
        assert!(matches!(out, Err(OxiaError::ShardMoved)));
    }

    #[tokio::test]
    async fn stops_on_fatal() {
        let token = CancellationToken::new();
        retry_until_cancelled(&token, "test", || async {
            Err(RetryError::fatal(OxiaError::SessionExpired))
        })
        .await;
    }
}
