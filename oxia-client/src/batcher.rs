//! Rate-adaptive batching with a bounded per-shard in-flight window.
//!
//! This ports the reference Java client's dispatch-window design: at most
//! `max_*_batches_in_flight` batches may be outstanding per shard. While the
//! window has capacity, operations are dispatched almost immediately (a batch
//! forms only from operations arriving in the same scheduler tick), so a fast
//! server sees small batches and minimal latency. When the window is
//! exhausted, the open batch keeps accumulating instead of being flushed, so
//! batch size automatically tracks the server's service rate — a saturated
//! server receives ever larger batches up to the byte/count limits, and full
//! batches queue in FIFO order awaiting a free slot.
//!
//! There is no batcher task, timer, or linger: batching is driven entirely by
//! operation arrival and batch completion. The only spawned work is
//! - a tiny *flusher* future per open batch (one scheduler yield, so
//!   concurrently-arriving operations coalesce before dispatch),
//! - for writes, a response *pump* that exists only while batches are in
//!   flight on the shard's write stream (it parks the response stream and
//!   exits when the shard goes idle), and
//! - for reads, one future per in-flight read RPC.
//!
//! Writes on a shard must reach the server in submission order: every dispatch
//! decision *and* the hand-off to the write stream happen under one mutex, so
//! stream order always equals dispatch order. Reads have no ordering
//! constraint and run as independent RPCs.

use crate::address::ensure_protocol;
use crate::errors::OxiaError;
use crate::operations::{Pending, PendingDelete, PendingDeleteRange, PendingGet, PendingPut};
use crate::proto;
use crate::provider_manager::ProviderManager;
use crate::retry::retry_delay;
use crate::server_error::{DecodedStatus, LeaderHint, decode_status};
use crate::shard_manager::ShardManager;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::{Notify, oneshot};
use tonic::Streaming;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, warn};

const WRITE_STREAM_HEADER_NAMESPACE: &str = "namespace";
const WRITE_STREAM_HEADER_SHARD_ID: &str = "shard-id";

/// Dependencies and limits shared by every shard batcher of one client.
pub(crate) struct BatcherDeps {
    pub(crate) namespace: String,
    pub(crate) shard_manager: Arc<ShardManager>,
    pub(crate) provider_manager: Arc<ProviderManager>,
    pub(crate) max_batch_size: usize,
    pub(crate) max_requests_per_batch: usize,
    pub(crate) max_write_batches_in_flight: usize,
    pub(crate) max_read_batches_in_flight: usize,
    pub(crate) request_timeout: Duration,
}

/// A write operation queued for batching.
pub(crate) enum WriteOp {
    Put(PendingPut),
    Delete(PendingDelete),
    DeleteRange(PendingDeleteRange),
}

impl WriteOp {
    /// Cheap size estimate used for batch byte accounting; mirrors the Java
    /// client (key length + value length — proto framing overhead ignored).
    fn size(&self) -> usize {
        match self {
            WriteOp::Put(p) => p.request.key.len() + p.request.value.len(),
            WriteOp::Delete(d) => d.request.key.len(),
            WriteOp::DeleteRange(r) => {
                r.request.start_inclusive.len() + r.request.end_exclusive.len()
            }
        }
    }

    fn fail(self, err: OxiaError) {
        match self {
            WriteOp::Put(p) => {
                let _ = p.callback.send(Err(err));
            }
            WriteOp::Delete(d) => {
                let _ = d.callback.send(Err(err));
            }
            WriteOp::DeleteRange(r) => {
                let _ = r.callback.send(Err(err));
            }
        }
    }
}

/// The open/ready bookkeeping every window shares. `open` is `Some` only while
/// non-empty; `ready` holds sealed batches awaiting a free window slot.
struct Queues<B> {
    open: Option<B>,
    ready: VecDeque<B>,
    closed: bool,
    flush_scheduled: bool,
}

impl<B> Default for Queues<B> {
    fn default() -> Self {
        Queues {
            open: None,
            ready: VecDeque::new(),
            closed: false,
            flush_scheduled: false,
        }
    }
}

impl<B> Queues<B> {
    /// The next batch to dispatch once a slot frees: sealed batches first (they
    /// are older), then the open one.
    fn take_next(&mut self) -> Option<B> {
        self.ready.pop_front().or_else(|| self.open.take())
    }

    fn is_drained(&self) -> bool {
        self.open.is_none() && self.ready.is_empty()
    }
}

fn fail_all<Resp>(
    callbacks: impl IntoIterator<Item = oneshot::Sender<Result<Resp, OxiaError>>>,
    err: &OxiaError,
) {
    for callback in callbacks {
        let _ = callback.send(Err(err.clone()));
    }
}

/// Completes `callbacks` in order from `responses`; leftovers on either side
/// indicate a server bug and fail with a decode error.
fn complete_all<Resp>(
    callbacks: impl IntoIterator<Item = oneshot::Sender<Result<Resp, OxiaError>>>,
    responses: impl IntoIterator<Item = Resp>,
    what: &str,
) {
    let mut responses = responses.into_iter();
    for callback in callbacks {
        match responses.next() {
            Some(response) => {
                let _ = callback.send(Ok(response));
            }
            None => {
                let _ = callback.send(Err(OxiaError::Decode(format!(
                    "missing {what} response from server"
                ))));
            }
        }
    }
    if responses.next().is_some() {
        warn!("server returned more {what} responses than requested");
    }
}

// ---------------------------------------------------------------------------
// Write batching
// ---------------------------------------------------------------------------

/// An accumulating write batch: wire requests plus their callbacks.
#[derive(Default)]
struct WriteBody {
    puts: Vec<PendingPut>,
    deletes: Vec<PendingDelete>,
    delete_ranges: Vec<PendingDeleteRange>,
    count: usize,
    bytes: usize,
}

impl WriteBody {
    fn push(&mut self, op: WriteOp, size: usize) {
        // The op's position among all the ops of the batch: a server with
        // FEATURE_ORDERED_WRITES applies them in this order rather than all
        // the puts, then the deletes, then the delete ranges. `count` is
        // bounded by `max_requests_per_batch` (a u32), so the cast is lossless.
        let op_index = self.count as u32;
        match op {
            WriteOp::Put(mut p) => {
                p.request.op_index = op_index;
                self.puts.push(p);
            }
            WriteOp::Delete(mut d) => {
                d.request.op_index = op_index;
                self.deletes.push(d);
            }
            WriteOp::DeleteRange(mut r) => {
                r.request.op_index = op_index;
                self.delete_ranges.push(r);
            }
        }
        self.count += 1;
        self.bytes += size;
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Splits into the wire request (requests moved, not cloned) and the
    /// callback set kept for completion.
    fn into_request(self, shard: i64) -> (proto::WriteRequest, WriteCallbacks) {
        let (put_reqs, put_cbs): (Vec<_>, Vec<_>) = self
            .puts
            .into_iter()
            .map(|p| (p.request, p.callback))
            .unzip();
        let (delete_reqs, delete_cbs): (Vec<_>, Vec<_>) = self
            .deletes
            .into_iter()
            .map(|p| (p.request, p.callback))
            .unzip();
        let (dr_reqs, dr_cbs): (Vec<_>, Vec<_>) = self
            .delete_ranges
            .into_iter()
            .map(|p| (p.request, p.callback))
            .unzip();
        (
            proto::WriteRequest {
                shard: Some(shard),
                puts: put_reqs,
                deletes: delete_reqs,
                delete_ranges: dr_reqs,
            },
            WriteCallbacks {
                puts: put_cbs,
                deletes: delete_cbs,
                delete_ranges: dr_cbs,
            },
        )
    }
}

/// The callbacks of one in-flight write batch, completed FIFO from the shard's
/// response stream.
struct WriteCallbacks {
    puts: Vec<oneshot::Sender<Result<proto::PutResponse, OxiaError>>>,
    deletes: Vec<oneshot::Sender<Result<proto::DeleteResponse, OxiaError>>>,
    delete_ranges: Vec<oneshot::Sender<Result<proto::DeleteRangeResponse, OxiaError>>>,
}

impl WriteCallbacks {
    fn complete(self, response: proto::WriteResponse) {
        complete_all(self.puts, response.puts, "put");
        complete_all(self.deletes, response.deletes, "delete");
        complete_all(self.delete_ranges, response.delete_ranges, "delete_range");
    }

    fn fail(self, err: &OxiaError) {
        fail_all(self.puts, err);
        fail_all(self.deletes, err);
        fail_all(self.delete_ranges, err);
    }
}

/// An in-flight (or retryable) write batch: the wire request is retained so a
/// retryable stream failure can re-send it — values are `Bytes`, so keeping it
/// is refcount-cheap.
struct InFlightWrite {
    request: proto::WriteRequest,
    callbacks: WriteCallbacks,
    /// When the batch was first dispatched; retries stop once the batch is
    /// older than the request timeout.
    first_dispatch: Instant,
}

struct WriteState {
    queues: Queues<WriteBody>,
    /// In-flight batches, FIFO-matched with stream responses.
    /// `pending.len()` *is* the number of batches in flight.
    pending: VecDeque<InFlightWrite>,
    /// Batches whose stream failed retryably, awaiting redispatch. Older than
    /// anything in `queues`, so they go out first.
    retry: VecDeque<InFlightWrite>,
    /// Leader hint from the last routing failure; steers the next stream
    /// connect until a response confirms the leader.
    hint: Option<LeaderHint>,
    /// True while a post-failure backoff sleep is pending; blocks dispatch so
    /// a dead leader is not hammered in a tight reconnect loop.
    reconnecting: bool,
    failure_streak: u32,
    /// Sender feeding the shard's write-stream RPC; `None` until the first
    /// dispatch and after a stream failure.
    stream_tx: Option<mpsc::UnboundedSender<proto::WriteRequest>>,
    /// The response stream, parked here while the shard is idle (no pump).
    parked: Option<Streaming<proto::WriteResponse>>,
    pump_running: bool,
}

/// Batches write operations for one shard.
/// Whether `shard` still exists; if not, an op targeting it must re-route.
fn absent_shard_error(deps: &BatcherDeps, shard: i64) -> OxiaError {
    if deps.shard_manager.shard_exists(shard) {
        OxiaError::LeaderNotFound { shard }
    } else {
        // Split or merged away: the operation builders re-route to the new shard.
        OxiaError::ShardMoved
    }
}

pub(crate) struct WriteBatcher {
    shard: i64,
    deps: Arc<BatcherDeps>,
    state: Mutex<WriteState>,
    drained: Notify,
}

impl WriteBatcher {
    pub(crate) fn new(shard: i64, deps: Arc<BatcherDeps>) -> Self {
        WriteBatcher {
            shard,
            deps,
            state: Mutex::new(WriteState {
                queues: Queues::default(),
                pending: VecDeque::new(),
                retry: VecDeque::new(),
                hint: None,
                reconnecting: false,
                failure_streak: 0,
                stream_tx: None,
                parked: None,
                pump_running: false,
            }),
            drained: Notify::new(),
        }
    }

    /// Queues a write. Infallible: failures are delivered through the
    /// operation's callback.
    pub(crate) fn add(self: &Arc<Self>, op: WriteOp) {
        let size = op.size();
        if size > self.deps.max_batch_size {
            op.fail(OxiaError::RequestTooLarge);
            return;
        }
        let mut spawn_flusher = false;
        let rejected = {
            let mut st = self.state.lock().expect("write batcher poisoned");
            if st.queues.closed {
                Some(op)
            } else {
                let mut open = st.queues.open.take().unwrap_or_default();
                // Seal the open batch when this operation would overflow it.
                if !open.is_empty() && open.bytes + size > self.deps.max_batch_size {
                    if self.can_dispatch(&st) {
                        self.dispatch_locked(&mut st, open);
                    } else {
                        st.queues.ready.push_back(open);
                    }
                    open = WriteBody::default();
                }
                open.push(op, size);
                if open.count >= self.deps.max_requests_per_batch {
                    if self.can_dispatch(&st) {
                        self.dispatch_locked(&mut st, open);
                    } else {
                        st.queues.ready.push_back(open);
                    }
                } else {
                    st.queues.open = Some(open);
                    // Adaptive dispatch: with window capacity available, flush
                    // the open batch after one scheduler yield (coalescing
                    // whatever arrives in the meantime). With the window
                    // exhausted (or a reconnect backoff pending), do nothing —
                    // a completion or the reconnect will pick it up.
                    if self.can_dispatch(&st) && !st.queues.flush_scheduled {
                        st.queues.flush_scheduled = true;
                        spawn_flusher = true;
                    }
                }
                None
            }
        };
        if let Some(op) = rejected {
            op.fail(OxiaError::Closed);
            return;
        }
        if spawn_flusher {
            let this = self.clone();
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                this.flush_open();
            });
        }
    }

    /// Dispatches the open batch if the window still has capacity.
    fn flush_open(self: &Arc<Self>) {
        let mut st = self.state.lock().expect("write batcher poisoned");
        st.queues.flush_scheduled = false;
        self.fill_capacity(&mut st);
    }

    fn can_dispatch(&self, st: &WriteState) -> bool {
        !st.reconnecting && st.pending.len() < self.deps.max_write_batches_in_flight
    }

    /// Dispatches queued batches — retries first (they are the oldest), then
    /// sealed batches, then the open one — while the window has capacity.
    /// Retries whose deadline has passed fail here instead of being re-sent.
    /// Must be called with the state lock held.
    fn fill_capacity(self: &Arc<Self>, st: &mut WriteState) {
        while self.can_dispatch(st) {
            if let Some(inflight) = st.retry.pop_front() {
                if inflight.first_dispatch.elapsed() >= self.deps.request_timeout {
                    inflight.callbacks.fail(&OxiaError::Timeout);
                    continue;
                }
                self.send_locked(st, inflight);
                continue;
            }
            match st.queues.take_next() {
                Some(body) => self.dispatch_locked(st, body),
                None => break,
            }
        }
    }

    /// Hands a batch to the shard's write stream. The pending-callback push and
    /// the stream send happen under the state lock, so response FIFO order and
    /// server-side apply order both equal dispatch order.
    fn dispatch_locked(self: &Arc<Self>, st: &mut WriteState, body: WriteBody) {
        let (request, callbacks) = body.into_request(self.shard);
        self.send_locked(
            st,
            InFlightWrite {
                request,
                callbacks,
                first_dispatch: Instant::now(),
            },
        );
    }

    /// Sends an in-flight batch (fresh or retried) on the stream, creating the
    /// stream and pump as needed. Must be called with the state lock held.
    fn send_locked(self: &Arc<Self>, st: &mut WriteState, inflight: InFlightWrite) {
        let request = inflight.request.clone();
        st.pending.push_back(inflight);

        if st.stream_tx.is_none() {
            debug!(shard = self.shard, "creating write stream");
            // Fresh stream: requests queue in the channel while the pump
            // connects (tonic requires the first message to be queued before
            // the RPC call for the stream to open).
            let (tx, rx) = mpsc::unbounded_channel();
            st.stream_tx = Some(tx);
            st.parked = None;
            st.pump_running = true;
            let this = self.clone();
            tokio::spawn(async move { this.run_pump(PumpInit::Connect(rx)).await });
        } else if !st.pump_running {
            let streaming = st
                .parked
                .take()
                .expect("idle write stream must have a parked response stream");
            st.pump_running = true;
            debug!(shard = self.shard, "resuming write-stream pump");
            let this = self.clone();
            tokio::spawn(async move { this.run_pump(PumpInit::Resume(Box::new(streaming))).await });
        }

        if let Some(tx) = &st.stream_tx {
            // The receiver lives until the pump's death path clears
            // `stream_tx` under this same lock, so this send cannot fail.
            let _ = tx.send(request);
        }
    }

    async fn run_pump(self: Arc<Self>, init: PumpInit) {
        let version = self.deps.shard_manager.assignments_version();
        let mut streaming = match init {
            PumpInit::Resume(streaming) => *streaming,
            PumpInit::Connect(rx) => match self.connect_stream(rx).await {
                Ok(streaming) => streaming,
                Err(err) => {
                    self.pump_died(err, version);
                    return;
                }
            },
        };
        loop {
            let next = tokio::time::timeout(self.deps.request_timeout, streaming.next()).await;
            let response = match next {
                // No response within the request timeout while batches are in
                // flight: the stream is wedged. Not retried — the batch may
                // have been applied.
                Err(_) => return self.pump_died(OxiaError::Timeout.into(), version),
                Ok(None) => {
                    return self.pump_died(
                        OxiaError::Disconnected("write stream closed by server".to_string()).into(),
                        version,
                    );
                }
                Ok(Some(Err(status))) => return self.pump_died(decode_status(status), version),
                Ok(Some(Ok(response))) => response,
            };

            let mut st = self.state.lock().expect("write batcher poisoned");
            // A response confirms the current leader and a healthy stream.
            st.failure_streak = 0;
            st.hint = None;
            let Some(inflight) = st.pending.pop_front() else {
                warn!(shard = self.shard, "unexpected write response; ignoring");
                continue;
            };
            let callbacks = inflight.callbacks;
            // Release the window slot before completing callbacks, so the
            // next batch is on the wire first.
            self.fill_capacity(&mut st);
            if st.pending.is_empty() {
                // Shard idle: park the response stream and exit.
                st.parked = Some(streaming);
                st.pump_running = false;
                let drained = st.queues.closed && self.is_drained(&st);
                drop(st);
                callbacks.complete(response);
                if drained {
                    self.drained.notify_waiters();
                }
                return;
            }
            drop(st);
            callbacks.complete(response);
        }
    }

    /// Opens the shard's write-stream RPC, feeding it the already-queued
    /// requests in `rx`. A leader hint from the previous failure, when present
    /// for this shard, overrides the (possibly stale) shard-manager leader.
    async fn connect_stream(
        &self,
        rx: UnboundedReceiver<proto::WriteRequest>,
    ) -> Result<Streaming<proto::WriteResponse>, DecodedStatus> {
        let hinted = {
            let st = self.state.lock().expect("write batcher poisoned");
            st.hint
                .as_ref()
                .and_then(|hint| hint.address_for(self.shard).map(str::to_string))
        };
        let target = match hinted {
            Some(address) => ensure_protocol(address),
            None => {
                self.deps
                    .shard_manager
                    .get_leader(self.shard)
                    .ok_or_else(|| DecodedStatus::from(absent_shard_error(&self.deps, self.shard)))?
                    .service_address
            }
        };
        let mut provider = self
            .deps
            .provider_manager
            .get_provider(target)
            .await
            .map_err(DecodedStatus::from)?;
        let mut request = tonic::Request::new(UnboundedReceiverStream::new(rx));
        let metadata = request.metadata_mut();
        metadata.insert(
            WRITE_STREAM_HEADER_NAMESPACE,
            self.deps.namespace.parse().map_err(|_| {
                DecodedStatus::from(OxiaError::InvalidArgument("namespace".to_string()))
            })?,
        );
        metadata.insert(
            WRITE_STREAM_HEADER_SHARD_ID,
            self.shard
                .to_string()
                .parse()
                .expect("shard id is valid metadata"),
        );
        match provider.write_stream(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(decode_status(status)),
        }
    }

    /// The stream failed (or could not be created). Retryable failures requeue
    /// the in-flight batches (they re-send on a fresh stream after a backoff,
    /// steered by the leader hint when one was attached); everything else, and
    /// batches older than the request timeout, fail. Retrying re-sends batches
    /// whose fate is unknown, so unconditional writes may apply more than once
    /// — the same at-least-once semantics as the reference clients.
    ///
    /// The backoff ends early once the shard assignments are updated past
    /// `version`, the one the pump started with: a leader elected or moved
    /// meanwhile is tried right away, not when the backoff ends — possibly
    /// past the request timeout.
    fn pump_died(self: &Arc<Self>, decoded: DecodedStatus, version: u64) {
        let DecodedStatus { error, leader_hint } = decoded;
        warn!(shard = self.shard, error = %error, "write stream failed");
        let (expired, failed, drained) = {
            let mut st = self.state.lock().expect("write batcher poisoned");
            st.pump_running = false;
            st.stream_tx = None;
            st.parked = None;
            if let Some(hint) = leader_hint {
                st.hint = Some(hint);
            }
            let mut expired: Vec<WriteCallbacks> = Vec::new();
            let mut failed: Vec<WriteCallbacks> = Vec::new();
            // A split/merged shard is retryable-by-rerouting, not by re-sending
            // to the same (now dead) shard: let it fall through to fail so the
            // builders re-route.
            if error.is_retryable() && !matches!(error, OxiaError::ShardMoved) && !st.queues.closed
            {
                st.failure_streak += 1;
                // Requeue in dispatch order ahead of previously-requeued
                // batches (which are newer), dropping the ones out of time.
                let mut still: VecDeque<InFlightWrite> = VecDeque::new();
                for inflight in st.pending.drain(..) {
                    if inflight.first_dispatch.elapsed() >= self.deps.request_timeout {
                        expired.push(inflight.callbacks);
                    } else {
                        still.push_back(inflight);
                    }
                }
                still.append(&mut st.retry);
                st.retry = still;
                // Hold dispatch until the backoff elapses (or the assignments
                // change), then reconnect.
                st.reconnecting = true;
                let delay = retry_delay(st.failure_streak - 1);
                let this = self.clone();
                tokio::spawn(async move {
                    this.deps
                        .shard_manager
                        .wait_for_update(version, delay)
                        .await;
                    let mut st = this.state.lock().expect("write batcher poisoned");
                    st.reconnecting = false;
                    this.fill_capacity(&mut st);
                });
            } else {
                failed = st.pending.drain(..).map(|i| i.callbacks).collect();
                // Queued batches still get a fresh stream; each batch fails at
                // most once.
                self.fill_capacity(&mut st);
            }
            let drained = st.queues.closed && self.is_drained(&st);
            (expired, failed, drained)
        };
        for callbacks in expired {
            callbacks.fail(&OxiaError::Timeout);
        }
        for callbacks in failed {
            callbacks.fail(&error);
        }
        if drained {
            self.drained.notify_waiters();
        }
    }

    fn is_drained(&self, st: &WriteState) -> bool {
        st.queues.is_drained() && st.pending.is_empty() && st.retry.is_empty()
    }

    /// Flushes everything queued and waits (bounded) for in-flight batches to
    /// complete, then tears the stream down. Late leftovers fail with
    /// [`OxiaError::Closed`].
    pub(crate) async fn close(self: &Arc<Self>) -> Result<(), OxiaError> {
        {
            let mut st = self.state.lock().expect("write batcher poisoned");
            st.queues.closed = true;
            // Drain fast: cancel any reconnect hold and dispatch everything,
            // ignoring the window bound (stream ordering is unaffected).
            st.reconnecting = false;
            while let Some(inflight) = st.retry.pop_front() {
                self.send_locked(&mut st, inflight);
            }
            while let Some(body) = st.queues.take_next() {
                self.dispatch_locked(&mut st, body);
            }
        }
        let deadline = tokio::time::Instant::now() + self.deps.request_timeout;
        loop {
            let notified = self.drained.notified();
            {
                let st = self.state.lock().expect("write batcher poisoned");
                if self.is_drained(&st) {
                    break;
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                let leftovers: Vec<WriteCallbacks> = {
                    let mut st = self.state.lock().expect("write batcher poisoned");
                    let mut leftovers: Vec<WriteCallbacks> =
                        st.pending.drain(..).map(|i| i.callbacks).collect();
                    leftovers.extend(st.retry.drain(..).map(|i| i.callbacks));
                    leftovers
                };
                for callbacks in leftovers {
                    callbacks.fail(&OxiaError::Closed);
                }
                break;
            }
        }
        // Drop the stream: the request channel closes, the server completes
        // the RPC, and any parked/pumping response stream winds down.
        let mut st = self.state.lock().expect("write batcher poisoned");
        st.stream_tx = None;
        st.parked = None;
        Ok(())
    }
}

enum PumpInit {
    Connect(UnboundedReceiver<proto::WriteRequest>),
    Resume(Box<Streaming<proto::WriteResponse>>),
}

// ---------------------------------------------------------------------------
// Read batching
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ReadBody {
    gets: Vec<PendingGet>,
}

struct ReadState {
    queues: Queues<ReadBody>,
    in_flight: usize,
}

/// Batches get operations for one shard. Reads have no byte accounting (the
/// requests are small) and no ordering constraint: each batch is an
/// independent `Read` RPC, up to `max_read_batches_in_flight` concurrently.
pub(crate) struct ReadBatcher {
    shard: i64,
    deps: Arc<BatcherDeps>,
    state: Mutex<ReadState>,
    drained: Notify,
}

impl ReadBatcher {
    pub(crate) fn new(shard: i64, deps: Arc<BatcherDeps>) -> Self {
        ReadBatcher {
            shard,
            deps,
            state: Mutex::new(ReadState {
                queues: Queues::default(),
                in_flight: 0,
            }),
            drained: Notify::new(),
        }
    }

    /// Queues a get. Infallible: failures are delivered through the callback.
    pub(crate) fn add(self: &Arc<Self>, get: PendingGet) {
        let mut spawn_flusher = false;
        let rejected = {
            let mut st = self.state.lock().expect("read batcher poisoned");
            if st.queues.closed {
                Some(get)
            } else {
                let mut open = st.queues.open.take().unwrap_or_default();
                open.gets.push(get);
                if open.gets.len() >= self.deps.max_requests_per_batch {
                    if st.in_flight < self.deps.max_read_batches_in_flight {
                        self.dispatch_locked(&mut st, open);
                    } else {
                        st.queues.ready.push_back(open);
                    }
                } else {
                    st.queues.open = Some(open);
                    if st.in_flight < self.deps.max_read_batches_in_flight
                        && !st.queues.flush_scheduled
                    {
                        st.queues.flush_scheduled = true;
                        spawn_flusher = true;
                    }
                }
                None
            }
        };
        if let Some(get) = rejected {
            let _ = get.callback.send(Err(OxiaError::Closed));
            return;
        }
        if spawn_flusher {
            let this = self.clone();
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                this.flush_open();
            });
        }
    }

    fn flush_open(self: &Arc<Self>) {
        let mut st = self.state.lock().expect("read batcher poisoned");
        st.queues.flush_scheduled = false;
        self.fill_capacity(&mut st);
    }

    fn fill_capacity(self: &Arc<Self>, st: &mut ReadState) {
        while st.in_flight < self.deps.max_read_batches_in_flight {
            match st.queues.take_next() {
                Some(body) => self.dispatch_locked(st, body),
                None => break,
            }
        }
    }

    fn dispatch_locked(self: &Arc<Self>, st: &mut ReadState, body: ReadBody) {
        st.in_flight += 1;
        let this = self.clone();
        tokio::spawn(async move {
            this.send_batch(body).await;
            let drained = {
                let mut st = this.state.lock().expect("read batcher poisoned");
                st.in_flight -= 1;
                this.fill_capacity(&mut st);
                st.queues.closed && st.queues.is_drained() && st.in_flight == 0
            };
            if drained {
                this.drained.notify_waiters();
            }
        });
    }

    /// Executes one read batch, retrying retryable failures with backoff and
    /// leader hints until the request timeout. Responses are collected before
    /// any callback completes, so a retried attempt never double-completes.
    async fn send_batch(&self, body: ReadBody) {
        let (requests, callbacks): (Vec<_>, Vec<_>) = body
            .gets
            .into_iter()
            .map(|p| (p.request, p.callback))
            .unzip();

        let deadline = Instant::now() + self.deps.request_timeout;
        let mut hint: Option<LeaderHint> = None;
        let mut attempt: u32 = 0;
        loop {
            match self.attempt_read(&requests, hint.as_ref()).await {
                Ok(responses) => {
                    complete_all(callbacks, responses, "get");
                    return;
                }
                Err(decoded) => {
                    if decoded.leader_hint.is_some() {
                        hint = decoded.leader_hint;
                    }
                    let delay = retry_delay(attempt);
                    attempt += 1;
                    if !decoded.error.is_retryable()
                        || matches!(decoded.error, OxiaError::ShardMoved)
                        || Instant::now() + delay >= deadline
                    {
                        fail_all(callbacks, &decoded.error);
                        return;
                    }
                    warn!(
                        shard = self.shard,
                        error = %decoded.error,
                        "read batch failed, retrying in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// One attempt: resolve the target (hint first), issue the RPC, and
    /// collect every response before completing anything.
    async fn attempt_read(
        &self,
        requests: &[proto::GetRequest],
        hint: Option<&LeaderHint>,
    ) -> Result<Vec<proto::GetResponse>, DecodedStatus> {
        let target = match hint.and_then(|h| h.address_for(self.shard)) {
            Some(address) => ensure_protocol(address.to_string()),
            None => {
                self.deps
                    .shard_manager
                    .get_leader(self.shard)
                    .ok_or_else(|| DecodedStatus::from(absent_shard_error(&self.deps, self.shard)))?
                    .service_address
            }
        };
        let mut provider = self
            .deps
            .provider_manager
            .get_provider(target)
            .await
            .map_err(DecodedStatus::from)?;
        let mut request = tonic::Request::new(proto::ReadRequest {
            shard: Some(self.shard),
            gets: requests.to_vec(),
        });
        request.set_timeout(self.deps.request_timeout);
        let mut streaming = match provider.read(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => return Err(decode_status(status)),
        };
        let mut responses = Vec::with_capacity(requests.len());
        while let Some(next) = streaming.next().await {
            match next {
                Ok(read_response) => responses.extend(read_response.gets),
                Err(status) => return Err(decode_status(status)),
            }
        }
        Ok(responses)
    }

    /// Flushes everything queued and waits (bounded) for in-flight reads.
    pub(crate) async fn close(self: &Arc<Self>) -> Result<(), OxiaError> {
        {
            let mut st = self.state.lock().expect("read batcher poisoned");
            st.queues.closed = true;
            while let Some(body) = st.queues.take_next() {
                self.dispatch_locked(&mut st, body);
            }
        }
        let deadline = tokio::time::Instant::now() + self.deps.request_timeout;
        loop {
            let notified = self.drained.notified();
            {
                let st = self.state.lock().expect("read batcher poisoned");
                if st.queues.is_drained() && st.in_flight == 0 {
                    break;
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                debug!(shard = self.shard, "read batcher close timed out");
                break;
            }
        }
        Ok(())
    }
}

/// Creates a `Pending` write op and its receiver in one step.
pub(crate) fn pending_write<Req, Resp>(
    request: Req,
    wrap: fn(Pending<Req, Resp>) -> WriteOp,
) -> (WriteOp, oneshot::Receiver<Result<Resp, OxiaError>>) {
    let (pending, rx) = Pending::new(request);
    (wrap(pending), rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn put_op(
        key: &str,
        value: &[u8],
    ) -> (
        WriteOp,
        oneshot::Receiver<Result<proto::PutResponse, OxiaError>>,
    ) {
        let request = proto::PutRequest {
            key: key.to_string(),
            value: Bytes::copy_from_slice(value),
            ..Default::default()
        };
        pending_write(request, WriteOp::Put)
    }

    #[test]
    fn write_op_size_estimates() {
        let (op, _rx) = put_op("key", b"value");
        assert_eq!(op.size(), 3 + 5);

        let (delete, _rx) = pending_write(
            proto::DeleteRequest {
                key: "abcd".to_string(),
                ..Default::default()
            },
            WriteOp::Delete,
        );
        assert_eq!(delete.size(), 4);

        let (dr, _rx) = pending_write(
            proto::DeleteRangeRequest {
                start_inclusive: "ab".to_string(),
                end_exclusive: "abc".to_string(),
                ..Default::default()
            },
            WriteOp::DeleteRange,
        );
        assert_eq!(dr.size(), 5);
    }

    #[test]
    fn queues_dispatch_order_is_sealed_then_open() {
        let mut queues: Queues<i32> = Queues::default();
        queues.ready.push_back(1);
        queues.ready.push_back(2);
        queues.open = Some(3);
        assert_eq!(queues.take_next(), Some(1));
        assert_eq!(queues.take_next(), Some(2));
        assert_eq!(queues.take_next(), Some(3));
        assert_eq!(queues.take_next(), None);
        assert!(queues.is_drained());
    }

    #[test]
    fn write_body_accounting() {
        let mut body = WriteBody::default();
        let (op, _rx) = put_op("k", b"0123456789");
        let size = op.size();
        body.push(op, size);
        assert_eq!(body.count, 1);
        assert_eq!(body.bytes, 11);
        let (request, callbacks) = body.into_request(7);
        assert_eq!(request.shard, Some(7));
        assert_eq!(request.puts.len(), 1);
        assert_eq!(callbacks.puts.len(), 1);
    }

    #[test]
    fn write_body_assigns_op_index_in_issue_order() {
        let delete_op = |key: &str| {
            pending_write(
                proto::DeleteRequest {
                    key: key.to_string(),
                    ..Default::default()
                },
                WriteOp::Delete,
            )
        };
        let (put_0, _rx0) = put_op("put-0", b"v");
        let (delete_1, _rx1) = delete_op("delete-1");
        let (put_2, _rx2) = put_op("put-2", b"v");
        let (range_3, _rx3) = pending_write(
            proto::DeleteRangeRequest {
                start_inclusive: "range-3".to_string(),
                end_exclusive: "range-4".to_string(),
                ..Default::default()
            },
            WriteOp::DeleteRange,
        );
        let (delete_4, _rx4) = delete_op("delete-4");

        let mut body = WriteBody::default();
        for op in [put_0, delete_1, put_2, range_3, delete_4] {
            let size = op.size();
            body.push(op, size);
        }

        let (request, _callbacks) = body.into_request(0);
        let puts: Vec<_> = request
            .puts
            .iter()
            .map(|p| (p.key.as_str(), p.op_index))
            .collect();
        assert_eq!(puts, [("put-0", 0), ("put-2", 2)]);
        let deletes: Vec<_> = request
            .deletes
            .iter()
            .map(|d| (d.key.as_str(), d.op_index))
            .collect();
        assert_eq!(deletes, [("delete-1", 1), ("delete-4", 4)]);
        let delete_ranges: Vec<_> = request
            .delete_ranges
            .iter()
            .map(|r| (r.start_inclusive.as_str(), r.op_index))
            .collect();
        assert_eq!(delete_ranges, [("range-3", 3)]);
    }

    fn test_deps(max_batch_size: usize) -> Arc<BatcherDeps> {
        // Managers are only touched at dispatch time; the rejection paths
        // exercised here never dispatch. Use an address nothing connects to.
        Arc::new(BatcherDeps {
            namespace: "default".to_string(),
            shard_manager: ShardManager::detached_for_tests(),
            provider_manager: Arc::new(ProviderManager::new(Duration::from_secs(1))),
            max_batch_size,
            max_requests_per_batch: 1000,
            max_write_batches_in_flight: 4,
            max_read_batches_in_flight: 4,
            request_timeout: Duration::from_secs(1),
        })
    }

    #[tokio::test]
    async fn oversized_write_is_rejected_before_batching() {
        let batcher = Arc::new(WriteBatcher::new(0, test_deps(16)));
        let (op, rx) = put_op("key", &[0u8; 64]);
        batcher.add(op);
        let err = rx.await.expect("callback must fire").unwrap_err();
        assert!(matches!(err, OxiaError::RequestTooLarge));
        // Nothing was queued or dispatched.
        let st = batcher.state.lock().unwrap();
        assert!(st.queues.is_drained());
        assert!(st.pending.is_empty());
    }

    #[tokio::test]
    async fn writes_after_close_fail_with_closed() {
        let batcher = Arc::new(WriteBatcher::new(0, test_deps(1024)));
        batcher.close().await.unwrap();
        let (op, rx) = put_op("key", b"value");
        batcher.add(op);
        let err = rx.await.expect("callback must fire").unwrap_err();
        assert!(matches!(err, OxiaError::Closed));
    }

    #[tokio::test]
    async fn reads_after_close_fail_with_closed() {
        let batcher = Arc::new(ReadBatcher::new(0, test_deps(1024)));
        batcher.close().await.unwrap();
        let (pending, rx) = Pending::new(proto::GetRequest::default());
        batcher.add(pending);
        let err = rx.await.expect("callback must fire").unwrap_err();
        assert!(matches!(err, OxiaError::Closed));
    }
}
