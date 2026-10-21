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
//!
//! The order also holds across a shard split ([`WriteManager`]): the batcher
//! of the split shard hands its pending writes off to the batchers of the
//! shards that replaced it, in the order they were issued, and those hold the
//! writes issued after the split until it has.

use crate::address::ensure_protocol;
use crate::errors::OxiaError;
use crate::operations::{Pending, PendingDelete, PendingDeleteRange, PendingGet, PendingPut};
use crate::proto;
use crate::provider_manager::ProviderManager;
use crate::retry::retry_delay;
use crate::server_error::{DecodedStatus, LeaderHint, decode_status};
use crate::shard_manager::{ShardManager, ShardReplacement};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures::stream::FuturesUnordered;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
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
    /// With the partition key the delete range was routed with: `None` for
    /// the copy of a delete range sent to every shard.
    DeleteRange(PendingDeleteRange, Option<String>),
}

impl WriteOp {
    /// Cheap size estimate used for batch byte accounting; mirrors the Java
    /// client (key length + value length — proto framing overhead ignored).
    fn size(&self) -> usize {
        match self {
            WriteOp::Put(p) => p.request.key.len() + p.request.value.len(),
            WriteOp::Delete(d) => d.request.key.len(),
            WriteOp::DeleteRange(r, _) => {
                r.request.start_inclusive.len() + r.request.end_exclusive.len()
            }
        }
    }

    /// The key that routes the op to its shard: its partition key, or its
    /// key. `None` for the copy of a delete range sent to every shard.
    fn routing_key(&self) -> Option<&str> {
        match self {
            WriteOp::Put(p) => Some(p.request.partition_key.as_deref().unwrap_or(&p.request.key)),
            WriteOp::Delete(d) => {
                Some(d.request.partition_key.as_deref().unwrap_or(&d.request.key))
            }
            WriteOp::DeleteRange(_, partition_key) => partition_key.as_deref(),
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
            WriteOp::DeleteRange(r, _) => {
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
    /// With the partition key each delete range was routed with.
    delete_ranges: Vec<(PendingDeleteRange, Option<String>)>,
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
            WriteOp::DeleteRange(mut r, partition_key) => {
                r.request.op_index = op_index;
                self.delete_ranges.push((r, partition_key));
            }
        }
        self.count += 1;
        self.bytes += size;
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether an op of `size` fits next to the ops already in the batch.
    fn fits(&self, size: usize, max_requests: usize, max_bytes: usize) -> bool {
        self.is_empty() || (self.count < max_requests && self.bytes + size <= max_bytes)
    }

    /// The ops of the batch, in the order they were added.
    fn into_ops(self) -> Vec<WriteOp> {
        let (request, callbacks) = self.into_request(0);
        ops_in_issue_order(request, callbacks)
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
            .map(|(p, partition_key)| (p.request, (p.callback, partition_key)))
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

type DeleteRangeCallback = oneshot::Sender<Result<proto::DeleteRangeResponse, OxiaError>>;

/// The callbacks of one in-flight write batch, completed FIFO from the shard's
/// response stream.
struct WriteCallbacks {
    puts: Vec<oneshot::Sender<Result<proto::PutResponse, OxiaError>>>,
    deletes: Vec<oneshot::Sender<Result<proto::DeleteResponse, OxiaError>>>,
    /// With the partition key each delete range was routed with.
    delete_ranges: Vec<(DeleteRangeCallback, Option<String>)>,
}

impl WriteCallbacks {
    fn complete(self, response: proto::WriteResponse) {
        complete_all(self.puts, response.puts, "put");
        complete_all(self.deletes, response.deletes, "delete");
        let delete_ranges = self.delete_ranges.into_iter().map(|(callback, _)| callback);
        complete_all(delete_ranges, response.delete_ranges, "delete_range");
    }

    fn fail(self, err: &OxiaError) {
        fail_all(self.puts, err);
        fail_all(self.deletes, err);
        fail_all(
            self.delete_ranges.into_iter().map(|(callback, _)| callback),
            err,
        );
    }
}

/// The ops of a batch, with their callbacks, in `op_index` order: the order
/// they were issued in.
fn ops_in_issue_order(request: proto::WriteRequest, callbacks: WriteCallbacks) -> Vec<WriteOp> {
    let puts = request
        .puts
        .into_iter()
        .zip(callbacks.puts)
        .map(|(request, callback)| {
            (
                request.op_index,
                WriteOp::Put(Pending { request, callback }),
            )
        });
    let deletes = request
        .deletes
        .into_iter()
        .zip(callbacks.deletes)
        .map(|(request, callback)| {
            (
                request.op_index,
                WriteOp::Delete(Pending { request, callback }),
            )
        });
    let delete_ranges = request
        .delete_ranges
        .into_iter()
        .zip(callbacks.delete_ranges)
        .map(|(request, (callback, partition_key))| {
            let op_index = request.op_index;
            (
                op_index,
                WriteOp::DeleteRange(Pending { request, callback }, partition_key),
            )
        });
    let mut ops: Vec<(u32, WriteOp)> = puts.chain(deletes).chain(delete_ranges).collect();
    ops.sort_by_key(|(op_index, _)| *op_index);
    ops.into_iter().map(|(_, op)| op).collect()
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
    /// Writes handed off by the shards that this one replaced, after a split.
    /// Issued before the writes in `queues`, they go out first.
    handed_off: VecDeque<WriteBody>,
    /// The number of shards that this one replaced and that have not handed
    /// off all their writes yet: until then, the writes in `queues` wait.
    held: usize,
    /// Set once the shard has been removed from the assignments.
    retired: Option<Retired>,
}

/// A shard removed from the assignments, after a split: the writes queued for
/// it go to the shards that replaced it.
struct Retired {
    replacement: Arc<ShardReplacement>,
    /// The batchers of the shards in `replacement.successors`, in order.
    successors: Vec<Arc<WriteBatcher>>,
    /// Whether the successors no longer hold their writes for this shard.
    released: bool,
}

impl Retired {
    /// Where `op` goes, by position in `successors`: to the successor that
    /// took over its key, or to each one, for the copy of a delete range sent
    /// to every shard. Without a successor for it, the op fails with
    /// `ShardMoved`, and its builder re-routes it.
    fn route(&self, op: WriteOp) -> Vec<(usize, WriteOp)> {
        let successor = op
            .routing_key()
            .map(|key| self.replacement.successor_for(key));
        match (successor, op) {
            (Some(Some(index)), op) => vec![(index, op)],
            (None, WriteOp::DeleteRange(pending, None)) if !self.successors.is_empty() => {
                copy_delete_range(pending, self.successors.len())
                    .into_iter()
                    .map(|copy| WriteOp::DeleteRange(copy, None))
                    .enumerate()
                    .collect()
            }
            (_, op) => {
                op.fail(OxiaError::ShardMoved);
                Vec::new()
            }
        }
    }

    /// Hands `ops` off to the successors, in order.
    fn hand_off(&self, ops: Vec<WriteOp>) {
        let mut handed_off: Vec<Vec<WriteOp>> =
            self.successors.iter().map(|_| Vec::new()).collect();
        for op in ops {
            for (index, op) in self.route(op) {
                handed_off[index].push(op);
            }
        }
        for (successor, ops) in self.successors.iter().zip(handed_off) {
            if !ops.is_empty() {
                successor.hand_off(ops);
            }
        }
    }
}

/// Copies a delete range for `shards` shards. Its callback gets the first
/// failure of a copy, or, once they have all succeeded, the response of the
/// last one.
fn copy_delete_range(pending: PendingDeleteRange, shards: usize) -> Vec<PendingDeleteRange> {
    if shards == 1 {
        return vec![pending];
    }
    let Pending { request, callback } = pending;
    let (copies, responses): (Vec<PendingDeleteRange>, Vec<_>) =
        (0..shards).map(|_| Pending::new(request.clone())).unzip();
    tokio::spawn(async move {
        let mut responses: FuturesUnordered<_> = responses.into_iter().collect();
        let mut result = None;
        while let Some(response) = responses.next().await {
            let response = response.unwrap_or_else(|_| {
                Err(OxiaError::Disconnected(
                    "operation was dropped without a response".to_string(),
                ))
            });
            let failed = !matches!(&response, Ok(r) if r.status == proto::Status::Ok as i32);
            result = Some(response);
            if failed {
                break;
            }
        }
        if let Some(result) = result {
            let _ = callback.send(result);
        }
    });
    copies
}

/// Batches write operations for one shard.
/// Whether `shard` still exists; if not, an op targeting it must re-route.
pub(crate) fn absent_shard_error(deps: &BatcherDeps, shard: i64) -> OxiaError {
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
    /// Retires the shard once it leaves the assignments.
    manager: Weak<WriteManager>,
    state: Mutex<WriteState>,
    drained: Notify,
}

impl WriteBatcher {
    pub(crate) fn new(shard: i64, deps: Arc<BatcherDeps>, manager: Weak<WriteManager>) -> Self {
        WriteBatcher {
            shard,
            deps,
            manager,
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
                handed_off: VecDeque::new(),
                held: 0,
                retired: None,
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
            } else if let Some(retired) = &st.retired {
                // Routed with assignments that still had the shard: on to the
                // shard that took over its key, behind the writes this one
                // hands off to it.
                let forwarded: Vec<_> = retired
                    .route(op)
                    .into_iter()
                    .map(|(index, op)| (retired.successors[index].clone(), op))
                    .collect();
                drop(st);
                for (successor, op) in forwarded {
                    successor.add(op);
                }
                return;
            } else {
                let mut open = st.queues.open.take().unwrap_or_default();
                // Seal the open batch when this operation would overflow it.
                if !open.is_empty() && open.bytes + size > self.deps.max_batch_size {
                    if self.can_dispatch_own(&st) {
                        self.dispatch_locked(&mut st, open);
                    } else {
                        st.queues.ready.push_back(open);
                    }
                    open = WriteBody::default();
                }
                open.push(op, size);
                if open.count >= self.deps.max_requests_per_batch {
                    if self.can_dispatch_own(&st) {
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
                    if self.can_dispatch_own(&st) && !st.queues.flush_scheduled {
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

    /// Whether a batch can go out: none can once the shard is retired.
    fn can_dispatch(&self, st: &WriteState) -> bool {
        st.retired.is_none()
            && !st.reconnecting
            && st.pending.len() < self.deps.max_write_batches_in_flight
    }

    /// Whether a batch of the writes in `queues` can go out: not before the
    /// writes handed off to the shard, which are older.
    fn can_dispatch_own(&self, st: &WriteState) -> bool {
        st.held == 0 && st.handed_off.is_empty() && self.can_dispatch(st)
    }

    /// Dispatches queued batches — retries first (they are the oldest), then
    /// those handed off by the shards this one replaced, then sealed batches,
    /// then the open one — while the window has capacity. Retries whose
    /// deadline has passed fail here instead of being re-sent. Must be called
    /// with the state lock held.
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
            if let Some(body) = st.handed_off.pop_front() {
                self.dispatch_locked(st, body);
                continue;
            }
            if st.held > 0 {
                break;
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

    /// Holds the writes queued here from now on until [`release`]: a shard
    /// that this one replaced has writes to hand off first.
    ///
    /// [`release`]: WriteBatcher::release
    fn hold(&self) {
        self.state.lock().expect("write batcher poisoned").held += 1;
    }

    /// A shard that this one replaced has handed off all its writes.
    fn release(self: &Arc<Self>) {
        let mut st = self.state.lock().expect("write batcher poisoned");
        st.held -= 1;
        self.make_progress(&mut st);
    }

    /// Takes over writes that were pending on a shard that this one replaced:
    /// they go out before the writes queued here.
    fn hand_off(self: &Arc<Self>, ops: Vec<WriteOp>) {
        let mut st = self.state.lock().expect("write batcher poisoned");
        for op in ops {
            let size = op.size();
            let fits = st.handed_off.back().is_some_and(|body| {
                body.fits(
                    size,
                    self.deps.max_requests_per_batch,
                    self.deps.max_batch_size,
                )
            });
            if !fits {
                st.handed_off.push_back(WriteBody::default());
            }
            let body = st.handed_off.back_mut().expect("a batch to add to");
            body.push(op, size);
        }
        self.make_progress(&mut st);
    }

    /// Retires the shard, removed from the assignments: from now on, its
    /// writes go to the shards that replaced it.
    fn retire(self: &Arc<Self>, retired: Retired) {
        let mut st = self.state.lock().expect("write batcher poisoned");
        st.retired = Some(retired);
        self.make_progress(&mut st);
    }

    /// Sends what the window allows, or, once the shard is retired, hands it
    /// off. Must be called with the state lock held.
    fn make_progress(self: &Arc<Self>, st: &mut WriteState) {
        if st.retired.is_some() {
            self.drain_retired(st);
        } else {
            self.fill_capacity(st);
        }
        if st.queues.closed && self.is_drained(st) {
            self.drained.notify_waiters();
        }
    }

    /// Hands off the writes of a retired shard once none is in flight on its
    /// stream, in the order they were issued: first the writes older than
    /// those queued here (the batches to retry, then the writes handed off to
    /// this shard), then, once the shards it replaced have handed off theirs,
    /// the writes queued here. Then releases the successors. Must be called
    /// with the state lock held.
    fn drain_retired(self: &Arc<Self>, st: &mut WriteState) {
        if st.retired.is_none() || st.pump_running {
            return;
        }
        let mut ops = Vec::new();
        for inflight in st.retry.drain(..) {
            if inflight.first_dispatch.elapsed() >= self.deps.request_timeout {
                inflight.callbacks.fail(&OxiaError::Timeout);
            } else {
                ops.extend(ops_in_issue_order(inflight.request, inflight.callbacks));
            }
        }
        for body in st.handed_off.drain(..) {
            ops.extend(body.into_ops());
        }
        let held = st.held > 0;
        if !held {
            while let Some(body) = st.queues.take_next() {
                ops.extend(body.into_ops());
            }
        }
        // Nothing goes out on the stream anymore.
        st.stream_tx = None;
        st.parked = None;
        let retired = st.retired.as_mut().expect("the shard is retired");
        retired.hand_off(ops);
        if !held && !retired.released {
            retired.released = true;
            for successor in &retired.successors {
                successor.release();
            }
        }
    }

    /// Retires the shard once it has left the assignments.
    fn retire_if_removed(&self) {
        if self.deps.shard_manager.shard_exists(self.shard) {
            return;
        }
        if let Some(manager) = self.manager.upgrade() {
            manager.process_replacements();
        }
    }

    /// The stream failed, or could not be created. A shard that has left the
    /// assignments is retired first, so that its writes go to the shards
    /// that replaced it rather than to a new stream.
    fn stream_failed(self: &Arc<Self>, decoded: DecodedStatus, version: u64) {
        self.retire_if_removed();
        self.pump_died(decoded, version);
    }

    async fn run_pump(self: Arc<Self>, init: PumpInit) {
        let version = self.deps.shard_manager.assignments_version();
        let mut streaming = match init {
            PumpInit::Resume(streaming) => *streaming,
            PumpInit::Connect(rx) => match self.connect_stream(rx).await {
                Ok(streaming) => streaming,
                Err(err) => {
                    self.stream_failed(err, version);
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
                Err(_) => return self.stream_failed(OxiaError::Timeout.into(), version),
                Ok(None) => {
                    return self.stream_failed(
                        OxiaError::Disconnected("write stream closed by server".to_string()).into(),
                        version,
                    );
                }
                Ok(Some(Err(status))) => return self.stream_failed(decode_status(status), version),
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
                // Nothing is in flight anymore: a retired shard hands off the
                // writes queued here.
                self.drain_retired(&mut st);
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
    /// for this shard, overrides the (possibly stale) shard-manager leader —
    /// as long as the shard still exists.
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
            // Once the shard is split or merged away, its hint is stale too:
            // fall through, so the write fails with `ShardMoved` and reroutes.
            Some(address) if self.deps.shard_manager.shard_exists(self.shard) => {
                ensure_protocol(address)
            }
            _ => {
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
    ///
    /// Once the shard is retired, the batches that would be retried are
    /// handed off to the shards that replaced it instead.
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
            // A retired shard hands off the batches it would retry. Otherwise,
            // a split/merged shard is retryable-by-rerouting, not by re-sending
            // to the same (now dead) shard: let it fall through to fail so the
            // builders re-route.
            if st.retired.is_some() {
                if error.is_retryable() {
                    // Ahead of the batches requeued before them, which are
                    // newer.
                    let mut retry = std::mem::take(&mut st.pending);
                    retry.append(&mut st.retry);
                    st.retry = retry;
                } else {
                    failed = st.pending.drain(..).map(|i| i.callbacks).collect();
                }
                self.drain_retired(&mut st);
            } else if error.is_retryable()
                && !matches!(error, OxiaError::ShardMoved)
                && !st.queues.closed
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
                    // A shard that has left the assignments hands off its
                    // writes rather than reconnecting.
                    this.retire_if_removed();
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
        st.queues.is_drained()
            && st.pending.is_empty()
            && st.retry.is_empty()
            && st.handed_off.is_empty()
    }

    /// Flushes everything queued and waits (bounded) for in-flight batches to
    /// complete, then tears the stream down. Late leftovers fail with
    /// [`OxiaError::Closed`].
    pub(crate) async fn close(self: &Arc<Self>) -> Result<(), OxiaError> {
        {
            let mut st = self.state.lock().expect("write batcher poisoned");
            st.queues.closed = true;
            // A retired shard hands its writes off instead, once none is in
            // flight.
            if st.retired.is_none() {
                // Drain fast: cancel any reconnect hold and dispatch everything,
                // ignoring the window bound (stream ordering is unaffected).
                st.reconnecting = false;
                while let Some(inflight) = st.retry.pop_front() {
                    self.send_locked(&mut st, inflight);
                }
                while let Some(body) = st.handed_off.pop_front() {
                    self.dispatch_locked(&mut st, body);
                }
                // The writes queued here wait for the shards that this one
                // replaced to hand off theirs.
                if st.held == 0 {
                    while let Some(body) = st.queues.take_next() {
                        self.dispatch_locked(&mut st, body);
                    }
                }
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
                    let mut queued: Vec<WriteBody> = st.handed_off.drain(..).collect();
                    while let Some(body) = st.queues.take_next() {
                        queued.push(body);
                    }
                    leftovers.extend(
                        queued
                            .into_iter()
                            .map(|body| body.into_request(self.shard).1),
                    );
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

/// The write batchers of a client, one per shard, created on the shard's
/// first write.
///
/// Writes keep their order across a shard split, as in the Go client
/// (oxia-db/oxia#1360): the batcher of the split shard hands off its pending
/// writes to the batchers of the shards that replaced it, in the order they
/// were issued, and these hold their own writes until it has, so that a
/// write issued once the client has the new assignments is not applied
/// before an older write to the same key.
pub(crate) struct WriteManager {
    deps: Arc<BatcherDeps>,
    batchers: DashMap<i64, Arc<WriteBatcher>>,
    /// The number of shard replacements processed, in the order the shard
    /// manager recorded them.
    processed: Mutex<usize>,
    this: Weak<WriteManager>,
}

impl WriteManager {
    pub(crate) fn new(deps: Arc<BatcherDeps>) -> Arc<Self> {
        Arc::new_cyclic(|this| WriteManager {
            deps,
            batchers: DashMap::new(),
            processed: Mutex::new(0),
            this: this.clone(),
        })
    }

    /// Queues a write for `shard`. Infallible: failures are delivered through
    /// the operation's callback.
    pub(crate) fn add(&self, shard: i64, op: WriteOp) {
        self.batcher(shard).add(op);
    }

    fn batcher(&self, shard: i64) -> Arc<WriteBatcher> {
        if let Some(batcher) = self.batchers.get(&shard) {
            return batcher.clone();
        }
        // The shard's first write. A shard that replaced split ones must hold
        // it until they have handed off their writes: retire them first.
        self.process_replacements();
        self.get_or_create(shard, |_| {}).0
    }

    /// The batcher of `shard`, and whether it was just created: then by
    /// `init`, before any write can reach it.
    fn get_or_create(
        &self,
        shard: i64,
        init: impl FnOnce(&Arc<WriteBatcher>),
    ) -> (Arc<WriteBatcher>, bool) {
        match self.batchers.entry(shard) {
            Entry::Occupied(entry) => (entry.get().clone(), false),
            Entry::Vacant(entry) => {
                let batcher = Arc::new(WriteBatcher::new(
                    shard,
                    self.deps.clone(),
                    self.this.clone(),
                ));
                init(&batcher);
                entry.insert(batcher.clone());
                (batcher, true)
            }
        }
    }

    /// Retires the shards removed from the assignments since the last call,
    /// in the order they were removed: the batcher of each one hands off its
    /// pending writes to the batchers of the shards that replaced it, which
    /// hold their own writes until then. A removed shard without a batcher
    /// gets one, which forwards the writes still routed to it.
    fn process_replacements(&self) {
        let mut processed = self.processed.lock().expect("write manager poisoned");
        for replacement in self.deps.shard_manager.replacements(*processed) {
            *processed += 1;
            let successors: Vec<Arc<WriteBatcher>> = replacement
                .successors
                .iter()
                .map(|range| {
                    let (successor, created) =
                        self.get_or_create(range.shard, |successor| successor.hold());
                    if !created {
                        successor.hold();
                    }
                    successor
                })
                .collect();
            let mut retired = Some(Retired {
                replacement: replacement.clone(),
                successors,
                released: false,
            });
            // A batcher created for the removed shard is retired before any
            // write can reach it. Having none to hand off, it releases the
            // successors at once.
            let (removed, _) = self.get_or_create(replacement.removed, |removed| {
                removed.retire(retired.take().expect("retired once"));
            });
            if let Some(retired) = retired {
                removed.retire(retired);
            }
        }
    }

    /// Flushes every batcher, and waits (bounded) for each one: those of the
    /// removed shards first, in the order they were removed, as they hand
    /// off their writes to the batchers of the shards that replaced them,
    /// which may be created meanwhile.
    pub(crate) async fn close(&self) -> Result<(), OxiaError> {
        let mut closed = HashSet::new();
        loop {
            let mut shards: Vec<i64> = self
                .deps
                .shard_manager
                .replacements(0)
                .iter()
                .map(|replacement| replacement.removed)
                .collect();
            shards.extend(self.batchers.iter().map(|entry| *entry.key()));
            shards.retain(|shard| !closed.contains(shard));
            if shards.is_empty() {
                return Ok(());
            }
            for shard in shards {
                let batcher = self.batchers.get(&shard).map(|entry| entry.clone());
                if closed.insert(shard) {
                    if let Some(batcher) = batcher {
                        batcher.close().await?;
                    }
                }
            }
        }
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

    /// One attempt: resolve the target (hint first, as long as the shard
    /// still exists), issue the RPC, and collect every response before
    /// completing anything.
    async fn attempt_read(
        &self,
        requests: &[proto::GetRequest],
        hint: Option<&LeaderHint>,
    ) -> Result<Vec<proto::GetResponse>, DecodedStatus> {
        let target = match hint.and_then(|h| h.address_for(self.shard)) {
            // Once the shard is split or merged away, its hint is stale too:
            // fall through, so the read fails with `ShardMoved` and reroutes.
            Some(address) if self.deps.shard_manager.shard_exists(self.shard) => {
                ensure_protocol(address.to_string())
            }
            _ => {
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
    wrap: impl FnOnce(Pending<Req, Resp>) -> WriteOp,
) -> (WriteOp, oneshot::Receiver<Result<Resp, OxiaError>>) {
    let (pending, rx) = Pending::new(request);
    (wrap(pending), rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_server::{Behavior, MockOxia};
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
            |pending| WriteOp::DeleteRange(pending, None),
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
            |pending| WriteOp::DeleteRange(pending, None),
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
        let batcher = Arc::new(WriteBatcher::new(0, test_deps(16), Weak::new()));
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
        let batcher = Arc::new(WriteBatcher::new(0, test_deps(1024), Weak::new()));
        batcher.close().await.unwrap();
        let (op, rx) = put_op("key", b"value");
        batcher.add(op);
        let err = rx.await.expect("callback must fire").unwrap_err();
        assert!(matches!(err, OxiaError::Closed));
    }

    /// A leader hint left over for a shard that has since been split away
    /// must not keep steering the write stream to the hinted leader: the
    /// write fails with `ShardMoved`, so the caller reroutes it, instead of
    /// retrying against the hint until the request timeout.
    #[tokio::test]
    async fn stale_hint_for_a_removed_shard_fails_with_shard_moved() {
        // The detached shard manager has no assignments: shard 0 is gone.
        let batcher = Arc::new(WriteBatcher::new(0, test_deps(1024), Weak::new()));
        batcher.state.lock().unwrap().hint = Some(LeaderHint {
            shard: 0,
            leader: "127.0.0.1:1".to_string(),
        });
        let (op, rx) = put_op("key", b"value");
        batcher.add(op);
        let err = rx.await.expect("callback must fire").unwrap_err();
        assert!(matches!(err, OxiaError::ShardMoved), "got {err:?}");
    }

    /// While the shard exists, its hint still overrides the leader in the
    /// assignments.
    #[tokio::test]
    async fn hint_for_an_existing_shard_overrides_the_assigned_leader() {
        let mock = MockOxia::start(Behavior::default()).await;
        let deps = test_deps(1024);
        // The assigned leader, `server-0:6648`, does not exist.
        deps.shard_manager
            .set_assignments_for_tests(&[(0, 0, u32::MAX)]);
        let batcher = Arc::new(WriteBatcher::new(0, deps, Weak::new()));
        batcher.state.lock().unwrap().hint = Some(LeaderHint {
            shard: 0,
            leader: mock.address.clone(),
        });
        let (op, rx) = put_op("key", b"value");
        batcher.add(op);
        let response = rx
            .await
            .expect("callback must fire")
            .expect("the write goes to the hinted leader");
        assert_eq!(response.status, proto::Status::Ok as i32);
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

    /// The same holds for a read batch that a failure left with a leader
    /// hint: once the shard has been split away, the next attempt fails with
    /// `ShardMoved`, so the gets reroute, instead of retrying against the
    /// hint until the request timeout.
    #[tokio::test]
    async fn stale_hint_for_a_removed_shard_fails_reads_with_shard_moved() {
        // The detached shard manager has no assignments: shard 0 is gone.
        let batcher = ReadBatcher::new(0, test_deps(1024));
        let hint = LeaderHint {
            shard: 0,
            leader: "127.0.0.1:1".to_string(),
        };
        let err = batcher
            .attempt_read(&[proto::GetRequest::default()], Some(&hint))
            .await
            .unwrap_err()
            .error;
        assert!(matches!(err, OxiaError::ShardMoved), "got {err:?}");
    }

    /// While the shard exists, its hint still overrides the leader in the
    /// assignments for reads too.
    #[tokio::test]
    async fn hint_for_an_existing_shard_overrides_the_assigned_leader_for_reads() {
        let mock = MockOxia::start(Behavior::default()).await;
        let deps = test_deps(1024);
        // The assigned leader, `server-0:6648`, does not exist.
        deps.shard_manager
            .set_assignments_for_tests(&[(0, 0, u32::MAX)]);
        let batcher = ReadBatcher::new(0, deps);
        let hint = LeaderHint {
            shard: 0,
            leader: mock.address.clone(),
        };
        let responses = batcher
            .attempt_read(&[proto::GetRequest::default()], Some(&hint))
            .await
            .map_err(|decoded| decoded.error)
            .expect("the read goes to the hinted leader");
        let statuses: Vec<i32> = responses.iter().map(|r| r.status).collect();
        assert_eq!(statuses, [proto::Status::Ok as i32]);
    }

    /// A delete range copied for the shards that replaced a split one
    /// succeeds once every copy has, and fails with the first copy that
    /// fails, an error or a status other than OK.
    #[tokio::test]
    async fn copied_delete_range_reports_every_copy() {
        let status = |status: proto::Status| proto::DeleteRangeResponse {
            status: status as i32,
        };
        let (pending, rx) = Pending::new(proto::DeleteRangeRequest::default());
        let copies = copy_delete_range(pending, 3);
        assert_eq!(copies.len(), 3);
        for copy in copies {
            let _ = copy.callback.send(Ok(status(proto::Status::Ok)));
        }
        let response = rx.await.expect("callback must fire").expect("delete range");
        assert_eq!(response.status, proto::Status::Ok as i32);

        // The third copy never completes.
        for failure in [
            Err(OxiaError::Timeout),
            Ok(status(proto::Status::InvalidArgument)),
        ] {
            let (pending, rx) = Pending::new(proto::DeleteRangeRequest::default());
            let [first, second, _third]: [PendingDeleteRange; 3] = copy_delete_range(pending, 3)
                .try_into()
                .unwrap_or_else(|_| panic!("three copies"));
            let _ = first.callback.send(Ok(status(proto::Status::Ok)));
            let expected = format!("{failure:?}");
            let _ = second.callback.send(failure);
            let result = rx.await.expect("callback must fire");
            assert_eq!(format!("{result:?}"), expected);
        }
    }
}
