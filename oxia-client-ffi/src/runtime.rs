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

//! The client handle and the two threads behind it.
//!
//! The native client is single-threaded: it lives on a thread of its own, in
//! a compio runtime, and every operation runs there. Operations submitted
//! from several threads run concurrently and batch together.
//!
//! Completion callbacks run on a second thread, the dispatcher, rather than
//! on the client's. The client's thread runs all of the client's work, so a
//! callback that blocked it — which a callback bridging into another
//! language's runtime may well do — would stall every other operation; one
//! that made a blocking Oxia call from it would deadlock. On the dispatcher
//! both are merely slow.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as sync_mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use futures::StreamExt;
use futures::channel::mpsc;
use futures::future::LocalBoxFuture;
use oxia::{OxiaClient as NativeOxiaClient, OxiaClientBuilder, OxiaError};
use tokio::sync::watch;

use crate::error::panic_text;

/// An operation to run on the client's thread.
type Job = Box<dyn FnOnce(NativeOxiaClient) -> LocalBoxFuture<'static, ()> + Send>;

/// A callback to invoke on the dispatcher thread.
type Completion = Box<dyn FnOnce() + Send>;

/// Counts the stream tasks still running on the client's thread.
///
/// Teardown has to wait for them. `oxia-client`'s own `close` does not return
/// while a subscription handle is alive, and a stream task holds one for as
/// long as it runs, so closing the client before they have finished
/// deadlocks. (That is a bug in the native client — `close` is documented to
/// stop subscriptions — and this count is what keeps it from reaching a C
/// caller.)
#[derive(Default)]
pub(crate) struct LiveStreams {
    count: Mutex<usize>,
    drained: Condvar,
}

impl LiveStreams {
    fn opened(&self) {
        *self.count.lock().unwrap_or_else(|e| e.into_inner()) += 1;
    }

    /// Called by a stream task as it finishes, on the client's thread.
    pub(crate) fn finished(&self) {
        let mut count = self.count.lock().unwrap_or_else(|e| e.into_inner());
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.drained.notify_all();
        }
    }

    /// Waits for every stream task to finish, giving up after `limit` so that
    /// a stuck task cannot wedge a caller's teardown for good.
    fn wait_until_drained(&self, limit: std::time::Duration) {
        let deadline = std::time::Instant::now() + limit;
        let mut count = self.count.lock().unwrap_or_else(|e| e.into_inner());
        while *count > 0 {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return;
            }
            let (next, _) = self
                .drained
                .wait_timeout(count, left)
                .unwrap_or_else(|e| e.into_inner());
            count = next;
        }
    }
}

/// The closure that hands an operation's outcome to a caller's callback.
type Deliver<T> = Box<dyn FnOnce(Result<T, OxiaError>) + Send>;

/// An open client.
///
/// Opaque to C: created by `oxia_client_new` and destroyed by
/// `oxia_client_free`. Operations may be submitted from any thread.
pub struct oxia_client {
    jobs: mpsc::UnboundedSender<Job>,
    /// `None` only while [`Drop`] is draining the dispatcher.
    completions: Option<sync_mpsc::Sender<Completion>>,
    /// Set to true to tell the long-lived stream tasks to stop.
    ///
    /// A one-shot operation's task always finishes on its own, but a stream's
    /// task waits for the next request from its handle, which may never come:
    /// the handle can outlive the client. compio's `block_on` does not return
    /// until every task it spawned has finished, so without this the client's
    /// thread would never stop and [`Drop`] would wait for it forever.
    shutdown: watch::Sender<bool>,
    live_streams: Arc<LiveStreams>,
    worker: Option<JoinHandle<()>>,
    dispatcher: Option<JoinHandle<()>>,
}

/// Delivers an asynchronous operation's result exactly once.
///
/// A wrapper SDK allocates a context per operation and releases it in the
/// callback, so a callback that never fires is a leak. The operation's task
/// is dropped mid-await when the client is freed with the operation still in
/// flight; this guard turns that drop into an
/// [`OxiaError::Closed`] completion, which makes "exactly one callback per
/// accepted operation" a property the caller can rely on.
struct Pending<T: Send + 'static> {
    completions: sync_mpsc::Sender<Completion>,
    deliver: Option<Deliver<T>>,
    /// Cleared when the job never reached the client's thread, so that a
    /// rejected submission reports through its return code only.
    armed: Arc<AtomicBool>,
}

impl<T: Send + 'static> Pending<T> {
    fn deliver(&mut self, outcome: Result<T, OxiaError>) {
        if let Some(deliver) = self.deliver.take() {
            let _ = self.completions.send(Box::new(move || deliver(outcome)));
        }
    }
}

impl<T: Send + 'static> Drop for Pending<T> {
    fn drop(&mut self) {
        if self.armed.load(Ordering::Acquire) {
            self.deliver(Err(OxiaError::Closed));
        }
    }
}

impl oxia_client {
    /// Builds the client on a thread of its own and returns once it is
    /// connected, or with the error that stopped it.
    pub(crate) fn start(builder: OxiaClientBuilder) -> Result<oxia_client, OxiaError> {
        let (completions, completion_rx) = sync_mpsc::channel::<Completion>();
        let dispatcher = std::thread::Builder::new()
            .name("oxia-callbacks".to_string())
            .spawn(move || {
                for completion in completion_rx {
                    // A panic in a caller's callback must not poison the
                    // dispatcher: the remaining callbacks still have to run.
                    if let Err(payload) =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(completion))
                    {
                        let text = panic_text(&*payload);
                        eprintln!("oxia: a completion callback panicked: {text}");
                    }
                }
            })
            .map_err(|err| OxiaError::Disconnected(format!("cannot start a thread: {err}")))?;

        let (shutdown, _) = watch::channel(false);
        let (ready_tx, ready_rx) = sync_mpsc::channel();
        let (jobs, mut job_rx) = mpsc::unbounded::<Job>();
        let worker = std::thread::Builder::new()
            .name("oxia-client".to_string())
            .spawn(move || {
                let runtime = match compio::runtime::Runtime::new() {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = ready_tx.send(Err(OxiaError::Disconnected(format!(
                            "cannot start the compio runtime: {err}"
                        ))));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let client = match builder.build().await {
                        Ok(client) => client,
                        Err(err) => {
                            let _ = ready_tx.send(Err(err));
                            return;
                        }
                    };
                    let _ = ready_tx.send(Ok(()));
                    while let Some(job) = job_rx.next().await {
                        compio::runtime::spawn(job(client.clone())).detach();
                    }
                });
            })
            .map_err(|err| OxiaError::Disconnected(format!("cannot start a thread: {err}")))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(oxia_client {
                jobs,
                completions: Some(completions),
                shutdown,
                live_streams: Arc::new(LiveStreams::default()),
                worker: Some(worker),
                dispatcher: Some(dispatcher),
            }),
            // The worker has already returned, so joining both threads is
            // immediate; dropping the senders stops the dispatcher.
            Ok(Err(err)) => {
                let _ = worker.join();
                drop(completions);
                let _ = dispatcher.join();
                Err(err)
            }
            Err(_) => {
                let _ = worker.join();
                drop(completions);
                let _ = dispatcher.join();
                Err(OxiaError::Closed)
            }
        }
    }

    /// Runs `op` on the client's thread and waits for its result.
    pub(crate) fn run<T, F, Fut>(&self, op: F) -> Result<T, OxiaError>
    where
        T: Send + 'static,
        F: FnOnce(NativeOxiaClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, OxiaError>> + 'static,
    {
        let (tx, rx) = sync_mpsc::channel();
        let job: Job = Box::new(move |client| {
            Box::pin(async move {
                let _ = tx.send(op(client).await);
            })
        });
        if self.jobs.unbounded_send(job).is_err() {
            return Err(OxiaError::Closed);
        }
        // The job is dropped unanswered when the client's thread has stopped.
        rx.recv().unwrap_or(Err(OxiaError::Closed))
    }

    /// Submits `op` and returns immediately. `deliver` runs on the
    /// dispatcher thread, exactly once, if and only if this returns `Ok`.
    pub(crate) fn submit<T, F, Fut>(
        &self,
        op: F,
        deliver: impl FnOnce(Result<T, OxiaError>) + Send + 'static,
    ) -> Result<(), OxiaError>
    where
        T: Send + 'static,
        F: FnOnce(NativeOxiaClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, OxiaError>> + 'static,
    {
        let completions = self.completions.clone().ok_or(OxiaError::Closed)?;
        let armed = Arc::new(AtomicBool::new(true));
        let mut pending = Pending {
            completions,
            deliver: Some(Box::new(deliver)),
            armed: Arc::clone(&armed),
        };
        let job: Job = Box::new(move |client| {
            Box::pin(async move {
                let outcome = op(client).await;
                pending.deliver(outcome);
            })
        });
        if let Err(rejected) = self.jobs.unbounded_send(job) {
            // Defuse the guard before the rejected job — which owns it — is
            // dropped, so a submission that failed does not also call back.
            armed.store(false, Ordering::Release);
            drop(rejected);
            return Err(OxiaError::Closed);
        }
        Ok(())
    }

    /// Flushes and shuts the native client down. Idempotent: the handle stays
    /// valid, and every later operation fails with
    /// [`OxiaError::Closed`].
    pub(crate) fn close(&self) -> Result<(), OxiaError> {
        self.run(|client| async move { client.close().await })
    }

    /// Opens a subscription or a streaming read and returns a handle a foreign
    /// thread can pull from.
    ///
    /// `open` runs on the client's thread and the stream it returns stays
    /// there — none of the native stream types is `Send`. This blocks until
    /// the stream is open, or until opening it fails, and then leaves a task
    /// behind that serves one item per request from the handle. See
    /// [`crate::streams`] for the protocol.
    pub(crate) fn open_stream<T, F, Fut, S>(
        &self,
        open: F,
    ) -> Result<crate::streams::StreamHandle<T>, OxiaError>
    where
        T: Send + 'static,
        F: FnOnce(NativeOxiaClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, OxiaError>> + 'static,
        S: futures::Stream<Item = Result<T, OxiaError>> + 'static,
    {
        let (ready_tx, ready_rx) = sync_mpsc::channel::<Result<(), OxiaError>>();
        let (requests, request_rx) = mpsc::unbounded::<()>();
        let (items_tx, items) = sync_mpsc::channel::<crate::streams::StreamItem<T>>();
        let (cancel, cancelled) = futures::channel::oneshot::channel::<()>();
        let shutdown = self.shutdown.subscribe();
        let live = Arc::clone(&self.live_streams);
        live.opened();

        let job: Job = Box::new(move |client| {
            Box::pin(crate::streams::serve(
                open(client),
                ready_tx,
                request_rx,
                items_tx,
                cancelled,
                shutdown,
                live,
            ))
        });
        if self.jobs.unbounded_send(job).is_err() {
            return Err(OxiaError::Closed);
        }
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(crate::streams::StreamHandle::new(requests, items, cancel)),
            Ok(Err(err)) => Err(err),
            // The client's thread stopped before it could answer.
            Err(_) => Err(OxiaError::Closed),
        }
    }
}

impl Drop for oxia_client {
    fn drop(&mut self) {
        // Tell the stream tasks to stop before anything waits on the
        // client's thread: each of them is parked on its handle's next
        // request, and a handle may outlive this client.
        let _ = self.shutdown.send(true);
        // Then wait for them to actually finish and let go of their
        // subscription handles, because the close below would otherwise
        // deadlock against one (see `LiveStreams`).
        self.live_streams
            .wait_until_drained(std::time::Duration::from_secs(10));
        // Close next, so the batchers flush and the operations in flight
        // resolve instead of being dropped mid-await.
        let _ = self.close();
        // Then stop the client's thread. A job still queued, or a task still
        // pending, has its `Pending` guard dropped here, which queues a
        // closed completion.
        self.jobs.close_channel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        // Only now drop the sender and drain the dispatcher, so that every
        // callback has run by the time this returns.
        self.completions = None;
        if let Some(dispatcher) = self.dispatcher.take() {
            let _ = dispatcher.join();
        }
    }
}
