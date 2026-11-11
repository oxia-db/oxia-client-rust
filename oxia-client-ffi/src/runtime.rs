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
//!
//! Submitting is bounded by [`QueueBudget`]: a submission that cannot be
//! charged against the budget is refused instead of being queued. See that
//! type for why the client's own `max_in_flight_bytes` limiter cannot do
//! this job on its own.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc as sync_mpsc;
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

/// The closure that hands an operation's outcome to a caller's callback.
type Deliver<T> = Box<dyn FnOnce(Result<T, OxiaError>) + Send>;

/// Caps the bytes the asynchronous entry points hold for the operations they
/// have accepted but not yet called back for.
///
/// The blocking entry points need nothing like this: they run the native
/// operation and park the calling thread until it answers, so the client's
/// own `max_in_flight_bytes` limiter — which a write awaits before it is
/// queued — pushes back on the caller by making the call take longer. The
/// asynchronous ones return before the operation has been admitted, so that
/// limiter slows the tasks down while this layer's queue grows: the memory
/// moves out of the batcher and into the jobs waiting here, each holding its
/// key and value.
///
/// The client's limiter cannot be reused for it. It lives on the client's
/// thread, behind an `Rc<RefCell<..>>`, and a submission happens on a
/// foreign thread that has no handle on the client at all; and charging a
/// submission against it there would deadlock, since the operation's own
/// `put(..).await` reserves the same bytes a second time and would wait for
/// a release that only its own completion can make. So this is a second
/// accounting of the same bytes over a longer window — submission to
/// callback, which contains the client's own window — configured by the
/// same `max_in_flight_bytes` number, so that a caller has one knob rather
/// than two to reconcile.
///
/// Admission mirrors the client's limiter: a submission is admitted while
/// usage is *below* the limit and then charged in full, so an operation
/// larger than the whole budget still goes through when nothing else is
/// outstanding, and usage can overshoot by one operation.
pub(crate) struct QueueBudget {
    /// `0` means unlimited: every submission is admitted untracked.
    limit: usize,
    held: AtomicUsize,
}

impl QueueBudget {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(QueueBudget {
            limit,
            held: AtomicUsize::new(0),
        })
    }

    /// Charges `size` bytes if there is room for them right now, or reports
    /// how full the budget is so a caller can be told.
    fn try_reserve(self: &Arc<Self>, size: usize) -> Result<QueueReservation, (usize, usize)> {
        if self.limit == 0 {
            return Ok(QueueReservation {
                budget: None,
                size: 0,
            });
        }
        let mut held = self.held.load(Ordering::Acquire);
        loop {
            if held >= self.limit {
                return Err((held, self.limit));
            }
            match self.held.compare_exchange_weak(
                held,
                held.saturating_add(size),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(QueueReservation {
                        budget: Some(Arc::clone(self)),
                        size,
                    });
                }
                Err(current) => held = current,
            }
        }
    }
}

/// Bytes charged to the submission budget, returned when dropped.
pub(crate) struct QueueReservation {
    /// `None` when there is no limit: nothing to return.
    budget: Option<Arc<QueueBudget>>,
    size: usize,
}

impl Drop for QueueReservation {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.take() {
            budget.held.fetch_sub(self.size, Ordering::AcqRel);
        }
    }
}

/// Why a submission was not accepted. Either way its callback does not run.
pub(crate) enum Rejected {
    /// The client has been closed.
    Closed,
    /// The submission budget is used up: the bytes held, and the limit.
    QueueFull(usize, usize),
}

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
    queue_budget: Arc<QueueBudget>,
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
    /// The submission's bytes, released once the caller's callback has
    /// returned — the point at which this layer has let go of the operation
    /// and of its result.
    budget: Option<QueueReservation>,
}

impl<T: Send + 'static> Pending<T> {
    fn deliver(&mut self, outcome: Result<T, OxiaError>) {
        if let Some(deliver) = self.deliver.take() {
            let budget = self.budget.take();
            let _ = self.completions.send(Box::new(move || {
                deliver(outcome);
                // Explicit, because what frees the room for the next
                // submission is the callback having run, not the operation
                // having finished.
                drop(budget);
            }));
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
    /// `max_in_flight_bytes` is the client's own cap, applied here as the
    /// budget for the asynchronous submissions as well; `0` means unlimited.
    pub(crate) fn start(
        builder: OxiaClientBuilder,
        max_in_flight_bytes: usize,
    ) -> Result<oxia_client, OxiaError> {
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
                queue_budget: QueueBudget::new(max_in_flight_bytes),
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
    ///
    /// `size` is the bytes this operation holds — its key and value — which
    /// are charged against the submission budget until its callback has run.
    /// A submission that does not fit is refused rather than queued.
    pub(crate) fn submit<T, F, Fut>(
        &self,
        size: usize,
        op: F,
        deliver: impl FnOnce(Result<T, OxiaError>) + Send + 'static,
    ) -> Result<(), Rejected>
    where
        T: Send + 'static,
        F: FnOnce(NativeOxiaClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, OxiaError>> + 'static,
    {
        let completions = self.completions.clone().ok_or(Rejected::Closed)?;
        let budget = match self.queue_budget.try_reserve(size) {
            Ok(reservation) => reservation,
            Err((held, limit)) => return Err(Rejected::QueueFull(held, limit)),
        };
        let armed = Arc::new(AtomicBool::new(true));
        let mut pending = Pending {
            completions,
            deliver: Some(Box::new(deliver)),
            armed: Arc::clone(&armed),
            budget: Some(budget),
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
            // Dropping it also returns the bytes it reserved.
            armed.store(false, Ordering::Release);
            drop(rejected);
            return Err(Rejected::Closed);
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

        let job: Job = Box::new(move |client| {
            Box::pin(crate::streams::serve(
                open(client),
                ready_tx,
                request_rx,
                items_tx,
                cancelled,
                shutdown,
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
        // The order of the four steps below matters.
        //
        // First tell the stream tasks to stop, before anything waits on the
        // client's thread: each of them is parked on its handle's next
        // request, and a handle may outlive this client.
        let _ = self.shutdown.send(true);
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
