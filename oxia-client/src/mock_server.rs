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

//! A programmable in-process Oxia server for hermetic client tests.
//!
//! Serves the real gRPC service on an ephemeral local port (no Docker, no
//! containers) and advertises itself as the leader of every shard (a single
//! one by default), so an unmodified [`OxiaClient`](crate::OxiaClient)
//! connects to it exactly as it would to a real cluster. The server is
//! tonic's, which runs on tokio: it gets a tokio runtime and threads of its
//! own, apart from the compio runtime of the client under test. Tests script server
//! behavior — delayed, missing, erroneous, or absent responses; torn streams;
//! the keys each shard returns; shard splits — and assert on what the server
//! observed (write requests, batch sizes, connection counts) and on what the
//! client surfaced.

use crate::proto;
use crate::proto::oxia_client_server::{OxiaClient as OxiaService, OxiaClientServer};
use futures::StreamExt;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status, Streaming};
use tonic_types::{ErrorDetails, StatusExt};

/// Scripted behavior of the write stream.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum WriteMode {
    /// Acknowledge every operation successfully.
    Ok,
    /// Acknowledge successfully after the delay (holds the in-flight window
    /// open so later operations accumulate into sealed batches).
    DelayResponses(Duration),
    /// Receive requests but never respond.
    Hang,
    /// Respond to every request with an empty `WriteResponse`.
    OmitResponses,
    /// Respond to every put with this proto `Status` code.
    PutStatus(i32),
    /// Respond to every delete with this proto `Status` code.
    DeleteStatus(i32),
}

/// Scripted behavior of read RPCs.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ReadMode {
    /// Every get succeeds with value `b"mock-value"`.
    Ok,
    /// Accept the RPC but never produce a response.
    Hang,
    /// Respond to every get with this proto `Status` code.
    GetStatus(i32),
    /// Every get on a shard finds the first of the shard's keys
    /// ([`Behavior::shard_keys`]), as a floor or ceiling get would, or gets
    /// `KEY_NOT_FOUND` on a shard without keys.
    ShardKeys,
}

pub(crate) struct Behavior {
    pub(crate) write: WriteMode,
    /// Write-stream connections with index below this read one request and
    /// then die, leaving the batch's fate unknown (the client must requeue).
    pub(crate) torn_write_connections: u32,
    pub(crate) read: ReadMode,
    /// When set, the assignments stream fails with this gRPC status.
    pub(crate) assignments_error: Option<tonic::Code>,
    /// The number of shards, splitting the hash space into equal ranges.
    pub(crate) shards: u32,
    /// The key sorting that the assignments report for the namespace.
    pub(crate) key_sorting: proto::KeySorting,
    /// The keys of each shard, by shard id, in the order of the shard: a list
    /// or a range scan of a shard returns all of them, one per response,
    /// whatever its range.
    pub(crate) shard_keys: HashMap<i64, Vec<&'static str>>,
    /// The secondary keys of the records, by key: a get or a range scan
    /// through an index returns a record with its secondary key, like the
    /// servers with oxia-db/oxia#1508, and without one if it has none here.
    pub(crate) secondary_keys: HashMap<&'static str, &'static str>,
    /// The shards whose leader includes the end of the range of a list or a
    /// range scan that asks for it, like a server with
    /// FEATURE_RANGE_END_INCLUSIVE: their responses say so, and they send one
    /// without keys when they have none. The other shards ignore it, like an
    /// older server.
    pub(crate) end_inclusive_shards: Vec<i64>,
    /// How long after the start the shards have no leader, as while one is
    /// elected: the assignments name no leader until the server publishes
    /// itself as the leader when this elapses.
    pub(crate) leaderless_for: Duration,
}

impl Default for Behavior {
    fn default() -> Self {
        Behavior {
            write: WriteMode::Ok,
            torn_write_connections: 0,
            read: ReadMode::Ok,
            assignments_error: None,
            shards: 1,
            key_sorting: proto::KeySorting::Unknown,
            shard_keys: HashMap::new(),
            secondary_keys: HashMap::new(),
            end_inclusive_shards: Vec::new(),
            leaderless_for: Duration::ZERO,
        }
    }
}

struct State {
    behavior: Mutex<Behavior>,
    /// The shards of the namespace, as `(shard, min_hash, max_hash)`: those
    /// of [`Behavior::shards`], then as [`MockOxia::split_shard`] left them.
    shards: Mutex<Vec<(i64, u32, u32)>>,
    /// The shards split away, with whether the requests for one get a
    /// leader hint.
    split_shards: Mutex<HashMap<i64, bool>>,
    /// The requests that failed because their shard was split away.
    split_shard_requests: AtomicU32,
    /// The shards that reject the writes (see [`MockOxia::freeze_shard`]).
    frozen: Mutex<HashSet<i64>>,
    /// When the shards get their leader (see [`Behavior::leaderless_for`]).
    leader_elected_at: Instant,
    /// Every received `WriteRequest`, in order.
    write_requests: Mutex<Vec<proto::WriteRequest>>,
    /// Operations per received `WriteRequest` (puts + deletes + delete-ranges).
    write_batch_sizes: Mutex<Vec<usize>>,
    write_connections: AtomicU32,
    /// Keeps assignment streams open for the lifetime of the mock, with the
    /// namespace that each one asked for.
    assignment_senders: Mutex<Vec<(String, AssignmentSender)>>,
    /// The notification batches of each shard, by shard id.
    notifications: Mutex<HashMap<i64, ShardNotifications>>,
    /// Signalled when the notifications change, for the notification streams
    /// to send what they can.
    notifications_changed: tokio::sync::watch::Sender<()>,
    /// Every received `NotificationsRequest`, in order.
    notification_requests: Mutex<Vec<proto::NotificationsRequest>>,
}

/// The sending end of a client's assignment stream.
type AssignmentSender = mpsc::UnboundedSender<Result<proto::ShardAssignments, Status>>;

/// The notification batches of a shard, as its leader keeps them.
#[derive(Default)]
struct ShardNotifications {
    /// The batches, in offset order: those of the shard it was split from, on
    /// its side of the split, then those published on it.
    batches: Vec<proto::NotificationBatch>,
    /// Whether the streams of the shard hold back its batches, as for a
    /// subscriber that lags behind.
    held: bool,
    /// Bumped to end the streams open on the shard.
    generation: u32,
}

impl ShardNotifications {
    /// The offset of the last batch, or -1: where a subscription without a
    /// start offset starts.
    fn last_offset(&self) -> i64 {
        self.batches.last().map_or(-1, |batch| batch.offset)
    }
}

/// What a notification stream does next.
enum NextBatch {
    /// Send the batch.
    Send(proto::NotificationBatch),
    /// Wait for the notifications to change.
    Wait,
    /// End with an error: the streams of the shard were closed.
    Close,
}

impl State {
    /// The current assignments of `namespace`, every shard led by `leader`.
    fn assignments(&self, namespace: &str, leader: &str) -> proto::ShardAssignments {
        let key_sorting = self.behavior.lock().expect("behavior lock").key_sorting;
        let shards = self.shards.lock().expect("shards lock");
        proto::ShardAssignments {
            namespaces: [(
                namespace.to_string(),
                proto::NamespaceShardsAssignment {
                    assignments: shards
                        .iter()
                        .map(|&(shard, min_hash_inclusive, max_hash_inclusive)| {
                            proto::ShardAssignment {
                                shard,
                                leader: leader.to_string(),
                                shard_boundaries: Some(
                                    proto::shard_assignment::ShardBoundaries::Int32HashRange(
                                        proto::Int32HashRange {
                                            min_hash_inclusive,
                                            max_hash_inclusive,
                                        },
                                    ),
                                ),
                            }
                        })
                        .collect(),
                    shard_key_router: proto::ShardKeyRouter::Xxhash3 as i32,
                    key_sorting: key_sorting as i32,
                },
            )]
            .into(),
            allowed_authorities: vec![],
        }
    }

    /// What a notification stream on `shard`, opened at `generation`, does
    /// after it sent the batch at offset `cursor`.
    fn next_notification_batch(&self, shard: i64, generation: u32, cursor: i64) -> NextBatch {
        let notifications = self.notifications.lock().expect("notifications lock");
        let Some(shard_notifications) = notifications.get(&shard) else {
            return NextBatch::Wait;
        };
        if shard_notifications.generation != generation {
            return NextBatch::Close;
        }
        if shard_notifications.held {
            return NextBatch::Wait;
        }
        match shard_notifications
            .batches
            .iter()
            .find(|batch| batch.offset > cursor)
        {
            Some(batch) => NextBatch::Send(batch.clone()),
            None => NextBatch::Wait,
        }
    }
}

/// A running mock server. Dropping it stops the server.
pub(crate) struct MockOxia {
    pub(crate) address: String,
    state: Arc<State>,
    /// The runtime serving the mock, on threads of its own.
    runtime: Option<tokio::runtime::Runtime>,
}

impl Drop for MockOxia {
    fn drop(&mut self) {
        // Without waiting for the streams the client keeps open to end.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl MockOxia {
    pub(crate) async fn start(behavior: Behavior) -> MockOxia {
        let shards = (0..behavior.shards)
            .map(|shard| {
                let (min, max) = hash_range(shard, behavior.shards);
                (shard.into(), min, max)
            })
            .collect();
        let state = Arc::new(State {
            leader_elected_at: Instant::now() + behavior.leaderless_for,
            behavior: Mutex::new(behavior),
            shards: Mutex::new(shards),
            split_shards: Mutex::new(HashMap::new()),
            split_shard_requests: AtomicU32::new(0),
            frozen: Mutex::new(HashSet::new()),
            write_requests: Mutex::new(Vec::new()),
            write_batch_sizes: Mutex::new(Vec::new()),
            write_connections: AtomicU32::new(0),
            assignment_senders: Mutex::new(Vec::new()),
            notifications: Mutex::new(HashMap::new()),
            notifications_changed: tokio::sync::watch::Sender::new(()),
            notification_requests: Mutex::new(Vec::new()),
        });
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock listener");
        listener
            .set_nonblocking(true)
            .expect("non-blocking mock listener");
        let port = listener.local_addr().expect("mock local addr").port();
        let address = format!("127.0.0.1:{port}");

        let service = Service {
            state: state.clone(),
            advertised: address.clone(),
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("mock runtime");
        runtime.spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("mock listener");
            let incoming = futures::stream::unfold(listener, |listener| async move {
                let accepted = listener.accept().await.map(|(stream, _)| stream);
                Some((accepted, listener))
            });
            tonic::transport::Server::builder()
                .add_service(OxiaClientServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .expect("mock server failed");
        });

        MockOxia {
            address,
            state,
            runtime: Some(runtime),
        }
    }

    pub(crate) fn set_read_mode(&self, read: ReadMode) {
        self.state.behavior.lock().expect("behavior lock").read = read;
    }

    /// Makes `shard` reject the writes, failing their stream with a
    /// retryable error, as the leader of a shard does once it is frozen for
    /// the cutover of its split, and its node once the split has removed it.
    pub(crate) fn freeze_shard(&self, shard: i64) {
        self.state.frozen.lock().expect("frozen lock").insert(shard);
    }

    /// Splits `shard` into two new shards, with a half of its hash range
    /// each, and returns their ids. Only the server sees the split until
    /// [`publish_assignments`](Self::publish_assignments): from now on, the
    /// writes to `shard` fail as on a frozen shard
    /// ([`freeze_shard`](Self::freeze_shard)), and a read, a list, a range
    /// scan or a notification subscription of `shard` fails as on the node
    /// of a parent that the split deleted, with
    /// `NODE_IS_NOT_LEADER`, and with a leader hint at this server when
    /// `leader_hint` is set, as when the node's assignments still list the
    /// parent. The notification streams open on `shard` stay open, as until
    /// the node deletes the parent
    /// ([`close_notification_streams`](Self::close_notification_streams)).
    /// Each child holds the notification batches of `shard`, at their
    /// offsets, with the notifications on its side of the split, as a server
    /// does.
    pub(crate) fn split_shard(&self, shard: i64, leader_hint: bool) -> (i64, i64) {
        self.freeze_shard(shard);
        let mut shards = self.state.shards.lock().expect("shards lock");
        let next = shards.iter().map(|&(id, _, _)| id).max().expect("shards") + 1;
        let index = shards
            .iter()
            .position(|&(id, _, _)| id == shard)
            .expect("the shard to split exists");
        let (_, min, max) = shards.remove(index);
        let middle = min + (max - min) / 2;
        let children = [(next, min, middle), (next + 1, middle + 1, max)];
        shards.extend(children);
        self.state
            .split_shards
            .lock()
            .expect("split shards lock")
            .insert(shard, leader_hint);

        let mut notifications = self.state.notifications.lock().expect("notifications lock");
        let inherited = notifications.entry(shard).or_default().batches.clone();
        for (child, min, max) in children {
            let batches = inherited
                .iter()
                .map(|batch| proto::NotificationBatch {
                    shard: child,
                    notifications: batch
                        .notifications
                        .iter()
                        .filter(|entry| (min..=max).contains(&crate::hash::xxh3_32(entry.key())))
                        .cloned()
                        .collect(),
                    ..batch.clone()
                })
                .collect();
            notifications.insert(
                child,
                ShardNotifications {
                    batches,
                    ..ShardNotifications::default()
                },
            );
        }
        (next, next + 1)
    }

    /// Sends the current shard assignments to the connected clients.
    pub(crate) fn publish_assignments(&self) {
        let senders = self.state.assignment_senders.lock().expect("senders lock");
        for (namespace, sender) in senders.iter() {
            let _ = sender.unbounded_send(Ok(self.state.assignments(namespace, &self.address)));
        }
    }

    /// The reads, lists, range scans and notification subscriptions that
    /// failed because their shard was split away.
    pub(crate) fn split_shard_requests(&self) -> u32 {
        self.state.split_shard_requests.load(Ordering::SeqCst)
    }

    /// The `WriteRequest`s the server has received, in order.
    pub(crate) fn write_requests(&self) -> Vec<proto::WriteRequest> {
        self.state
            .write_requests
            .lock()
            .expect("requests lock")
            .clone()
    }

    /// Operations per `WriteRequest` the server has received, in order.
    pub(crate) fn write_batch_sizes(&self) -> Vec<usize> {
        self.state
            .write_batch_sizes
            .lock()
            .expect("sizes lock")
            .clone()
    }

    pub(crate) fn write_connections(&self) -> u32 {
        self.state.write_connections.load(Ordering::SeqCst)
    }

    /// Publishes a notification batch on `shard`, at the offset after its
    /// last one, with the creation of each of `keys`, and returns its offset.
    pub(crate) fn notify(&self, shard: i64, keys: &[&str]) -> i64 {
        let mut notifications = self.state.notifications.lock().expect("notifications lock");
        let shard_notifications = notifications.entry(shard).or_default();
        let offset = shard_notifications.last_offset() + 1;
        shard_notifications.batches.push(proto::NotificationBatch {
            shard,
            offset,
            timestamp: 1,
            notifications: keys
                .iter()
                .map(|&key| proto::NotificationEntry {
                    key: Some(key.to_string()),
                    value: Some(proto::Notification {
                        r#type: proto::NotificationType::KeyCreated as i32,
                        version_id: Some(offset),
                        key_range_last: None,
                    }),
                })
                .collect(),
        });
        drop(notifications);
        self.state.notifications_changed.send_replace(());
        offset
    }

    /// From now on, the notification streams of `shard` hold back its
    /// batches, as for a subscriber that lags behind.
    pub(crate) fn hold_notifications(&self, shard: i64) {
        let mut notifications = self.state.notifications.lock().expect("notifications lock");
        notifications.entry(shard).or_default().held = true;
    }

    /// Ends the notification streams open on `shard` with `UNAVAILABLE`, as
    /// the node of a shard does once it deletes it.
    pub(crate) fn close_notification_streams(&self, shard: i64) {
        let mut notifications = self.state.notifications.lock().expect("notifications lock");
        notifications.entry(shard).or_default().generation += 1;
        drop(notifications);
        self.state.notifications_changed.send_replace(());
    }

    /// The notification subscriptions the server has received, in order.
    pub(crate) fn notification_requests(&self) -> Vec<proto::NotificationsRequest> {
        self.state
            .notification_requests
            .lock()
            .expect("requests lock")
            .clone()
    }
}

fn ok_version() -> proto::Version {
    proto::Version {
        version_id: 1,
        modifications_count: 0,
        created_timestamp: 1,
        modified_timestamp: 1,
        session_id: None,
        client_identity: None,
    }
}

/// The record of `key`, as a get or a range scan finds it, with its secondary
/// key when the read goes through an index.
fn record(key: &str, secondary_key: Option<&str>) -> proto::GetResponse {
    proto::GetResponse {
        status: proto::Status::Ok as i32,
        version: Some(ok_version()),
        value: Some(bytes::Bytes::from(key.to_string())),
        key: Some(key.to_string()),
        secondary_index_key: secondary_key.map(String::from),
    }
}

/// The inclusive hash range of `shard` when the hash space is split into
/// `shards` equal ranges.
fn hash_range(shard: u32, shards: u32) -> (u32, u32) {
    let size = (u64::from(u32::MAX) + 1) / u64::from(shards);
    let min = u64::from(shard) * size;
    let max = if shard + 1 == shards {
        u64::from(u32::MAX)
    } else {
        min + size - 1
    };
    (min as u32, max as u32)
}

fn respond_to(request: &proto::WriteRequest, mode: WriteMode) -> proto::WriteResponse {
    if mode == WriteMode::OmitResponses {
        return proto::WriteResponse::default();
    }
    let put_status = match mode {
        WriteMode::PutStatus(code) => code,
        _ => proto::Status::Ok as i32,
    };
    let delete_status = match mode {
        WriteMode::DeleteStatus(code) => code,
        _ => proto::Status::Ok as i32,
    };
    proto::WriteResponse {
        puts: request
            .puts
            .iter()
            .map(|_| proto::PutResponse {
                status: put_status,
                version: (put_status == proto::Status::Ok as i32).then(ok_version),
                key: None,
            })
            .collect(),
        deletes: request
            .deletes
            .iter()
            .map(|_| proto::DeleteResponse {
                status: delete_status,
            })
            .collect(),
        delete_ranges: request
            .delete_ranges
            .iter()
            .map(|_| proto::DeleteRangeResponse {
                status: proto::Status::Ok as i32,
            })
            .collect(),
    }
}

struct Service {
    state: Arc<State>,
    advertised: String,
}

impl Service {
    /// The scripted keys of a shard.
    fn shard_keys(&self, shard: Option<i64>) -> Vec<&'static str> {
        let behavior = self.state.behavior.lock().expect("behavior lock");
        shard
            .and_then(|shard| behavior.shard_keys.get(&shard))
            .cloned()
            .unwrap_or_default()
    }

    /// The scripted secondary key of the record of `key`, which a read
    /// through an index returns with it.
    fn secondary_key(&self, key: &str, through_index: bool) -> Option<&'static str> {
        if !through_index {
            return None;
        }
        let behavior = self.state.behavior.lock().expect("behavior lock");
        behavior.secondary_keys.get(key).copied()
    }

    /// The keys of the responses of a shard to a list or a range scan, and
    /// whether the responses say that they include the end of the range.
    fn scan(&self, shard: Option<i64>, end_inclusive: bool) -> (Vec<Vec<&'static str>>, bool) {
        let keys = self.shard_keys(shard);
        let included = end_inclusive
            && shard.is_some_and(|shard| {
                let behavior = self.state.behavior.lock().expect("behavior lock");
                behavior.end_inclusive_shards.contains(&shard)
            });
        if keys.is_empty() && included {
            return (vec![Vec::new()], true);
        }
        (keys.into_iter().map(|key| vec![key]).collect(), included)
    }

    /// Fails a read, a list, a range scan or a notification subscription of a
    /// shard split away, as the node of the deleted parent does: with
    /// `NODE_IS_NOT_LEADER`, and with a leader hint at this server if
    /// [`MockOxia::split_shard`] asked for one.
    fn reject_split_shard(&self, shard: Option<i64>) -> Result<(), Status> {
        let Some(shard) = shard else {
            return Ok(());
        };
        let split_shards = self.state.split_shards.lock().expect("split shards lock");
        let Some(&leader_hint) = split_shards.get(&shard) else {
            return Ok(());
        };
        drop(split_shards);
        self.state
            .split_shard_requests
            .fetch_add(1, Ordering::SeqCst);
        let metadata = if leader_hint {
            HashMap::from([
                ("shard".to_string(), shard.to_string()),
                ("leader".to_string(), self.advertised.clone()),
            ])
        } else {
            HashMap::new()
        };
        let mut details = ErrorDetails::new();
        details.set_error_info("NODE_IS_NOT_LEADER", "oxia.io", metadata);
        Err(Status::with_error_details(
            tonic::Code::Aborted,
            "node is not leader",
            details,
        ))
    }
}

type Rpc<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl OxiaService for Service {
    type GetShardAssignmentsStream = BoxStream<'static, Result<proto::ShardAssignments, Status>>;

    async fn get_shard_assignments(
        &self,
        request: Request<proto::ShardAssignmentsRequest>,
    ) -> Rpc<Self::GetShardAssignmentsStream> {
        let scripted_error = self
            .state
            .behavior
            .lock()
            .expect("behavior lock")
            .assignments_error;
        if let Some(code) = scripted_error {
            return Err(Status::new(code, "scripted assignments failure"));
        }
        let namespace = request.into_inner().namespace;
        let (tx, rx) = mpsc::unbounded();
        let leaderless_for = self
            .state
            .leader_elected_at
            .saturating_duration_since(Instant::now());
        if leaderless_for.is_zero() {
            tx.unbounded_send(Ok(self.state.assignments(&namespace, &self.advertised)))
                .expect("first assignment");
        } else {
            // No leader yet: the coordinator publishes an empty one.
            tx.unbounded_send(Ok(self.state.assignments(&namespace, "")))
                .expect("first assignment");
            let elected = self.state.assignments(&namespace, &self.advertised);
            let tx = tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(leaderless_for).await;
                let _ = tx.unbounded_send(Ok(elected));
            });
        }
        // Park the sender so the stream stays open, like a real server.
        self.state
            .assignment_senders
            .lock()
            .expect("senders lock")
            .push((namespace, tx));
        Ok(Response::new(rx.boxed()))
    }

    async fn write(&self, _: Request<proto::WriteRequest>) -> Rpc<proto::WriteResponse> {
        Err(Status::unimplemented("deprecated unary write"))
    }

    type WriteStreamStream = BoxStream<'static, Result<proto::WriteResponse, Status>>;

    async fn write_stream(
        &self,
        request: Request<Streaming<proto::WriteRequest>>,
    ) -> Rpc<Self::WriteStreamStream> {
        let connection = self.state.write_connections.fetch_add(1, Ordering::SeqCst);
        let state = self.state.clone();
        let (mode, torn) = {
            let behavior = state.behavior.lock().expect("behavior lock");
            (behavior.write, behavior.torn_write_connections)
        };
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::unbounded();
        tokio::spawn(async move {
            while let Some(Ok(request)) = inbound.next().await {
                state
                    .write_requests
                    .lock()
                    .expect("requests lock")
                    .push(request.clone());
                let ops = request.puts.len() + request.deletes.len() + request.delete_ranges.len();
                state
                    .write_batch_sizes
                    .lock()
                    .expect("sizes lock")
                    .push(ops);
                if connection < torn {
                    // Received but unacknowledged: the batch's fate is
                    // unknown, so the client must requeue it on a new stream.
                    let _ = tx.unbounded_send(Err(Status::unavailable("scripted torn stream")));
                    return;
                }
                match mode {
                    WriteMode::Hang => continue,
                    WriteMode::DelayResponses(delay) => {
                        tokio::time::sleep(delay).await;
                    }
                    _ => {}
                }
                let frozen = request.shard.is_some_and(|shard| {
                    state.frozen.lock().expect("frozen lock").contains(&shard)
                });
                if frozen {
                    // As the leader of a frozen shard does, even with the
                    // request in flight when it was frozen: the client
                    // retries the batch.
                    let _ = tx.unbounded_send(Err(Status::aborted("node is not leader")));
                    return;
                }
                if tx.unbounded_send(Ok(respond_to(&request, mode))).is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(rx.boxed()))
    }

    type ReadStream = BoxStream<'static, Result<proto::ReadResponse, Status>>;

    async fn read(&self, request: Request<proto::ReadRequest>) -> Rpc<Self::ReadStream> {
        let mode = self.state.behavior.lock().expect("behavior lock").read;
        let request = request.into_inner();
        self.reject_split_shard(request.shard)?;
        let gets = request.gets;
        match mode {
            ReadMode::Hang => {
                let (tx, rx) = mpsc::unbounded();
                // Keep the RPC open without ever responding.
                std::mem::forget(tx);
                Ok(Response::new(rx.boxed()))
            }
            ReadMode::ShardKeys => {
                let found = self.shard_keys(request.shard).first().copied();
                let response = proto::ReadResponse {
                    gets: gets
                        .iter()
                        .map(|get| match found {
                            Some(key) => record(
                                key,
                                self.secondary_key(key, get.secondary_index_name.is_some()),
                            ),
                            None => proto::GetResponse {
                                status: proto::Status::KeyNotFound as i32,
                                ..proto::GetResponse::default()
                            },
                        })
                        .collect(),
                };
                Ok(Response::new(futures::stream::iter([Ok(response)]).boxed()))
            }
            ReadMode::Ok | ReadMode::GetStatus(_) => {
                let status = match mode {
                    ReadMode::GetStatus(code) => code,
                    _ => proto::Status::Ok as i32,
                };
                let response = proto::ReadResponse {
                    gets: gets
                        .iter()
                        .map(|_| proto::GetResponse {
                            status,
                            version: (status == proto::Status::Ok as i32).then(ok_version),
                            value: (status == proto::Status::Ok as i32)
                                .then(|| bytes::Bytes::from_static(b"mock-value")),
                            key: None,
                            secondary_index_key: None,
                        })
                        .collect(),
                };
                Ok(Response::new(futures::stream::iter([Ok(response)]).boxed()))
            }
        }
    }

    type ListStream = BoxStream<'static, Result<proto::ListResponse, Status>>;

    async fn list(&self, request: Request<proto::ListRequest>) -> Rpc<Self::ListStream> {
        let request = request.into_inner();
        self.reject_split_shard(request.shard)?;
        let (responses, end_inclusive) = self.scan(request.shard, request.end_inclusive);
        let responses: Vec<_> = responses
            .into_iter()
            .map(|keys| {
                Ok(proto::ListResponse {
                    keys: keys.into_iter().map(String::from).collect(),
                    end_inclusive,
                })
            })
            .collect();
        Ok(Response::new(futures::stream::iter(responses).boxed()))
    }

    type RangeScanStream = BoxStream<'static, Result<proto::RangeScanResponse, Status>>;

    async fn range_scan(
        &self,
        request: Request<proto::RangeScanRequest>,
    ) -> Rpc<Self::RangeScanStream> {
        let request = request.into_inner();
        self.reject_split_shard(request.shard)?;
        let (responses, end_inclusive) = self.scan(request.shard, request.end_inclusive);
        let through_index = request.secondary_index_name.is_some();
        let responses: Vec<_> = responses
            .into_iter()
            .map(|keys| {
                Ok(proto::RangeScanResponse {
                    records: keys
                        .into_iter()
                        .map(|key| record(key, self.secondary_key(key, through_index)))
                        .collect(),
                    end_inclusive,
                })
            })
            .collect();
        Ok(Response::new(futures::stream::iter(responses).boxed()))
    }

    type GetSequenceUpdatesStream =
        BoxStream<'static, Result<proto::GetSequenceUpdatesResponse, Status>>;

    async fn get_sequence_updates(
        &self,
        _: Request<proto::GetSequenceUpdatesRequest>,
    ) -> Rpc<Self::GetSequenceUpdatesStream> {
        Err(Status::unimplemented("not scripted"))
    }

    type GetNotificationsStream = BoxStream<'static, Result<proto::NotificationBatch, Status>>;

    async fn get_notifications(
        &self,
        request: Request<proto::NotificationsRequest>,
    ) -> Rpc<Self::GetNotificationsStream> {
        let request = request.into_inner();
        let (shard, start_offset_exclusive) = (request.shard, request.start_offset_exclusive);
        self.state
            .notification_requests
            .lock()
            .expect("requests lock")
            .push(request);
        self.reject_split_shard(Some(shard))?;
        // Taken before the batches are read: a batch published from then on
        // wakes the stream.
        let changed = self.state.notifications_changed.subscribe();
        let (start, generation) = {
            let mut notifications = self.state.notifications.lock().expect("notifications lock");
            let shard_notifications = notifications.entry(shard).or_default();
            (
                start_offset_exclusive.unwrap_or_else(|| shard_notifications.last_offset()),
                shard_notifications.generation,
            )
        };
        // As a server, confirm the subscription with an empty batch at its
        // start offset, then send the batches after it as they come.
        let confirmation = proto::NotificationBatch {
            shard,
            offset: start,
            timestamp: 0,
            notifications: Vec::new(),
        };
        let state = self.state.clone();
        let batches = futures::stream::unfold(Some((start, changed)), move |cursor| {
            let state = state.clone();
            async move {
                let (cursor, mut changed) = cursor?;
                loop {
                    match state.next_notification_batch(shard, generation, cursor) {
                        NextBatch::Send(batch) => {
                            let offset = batch.offset;
                            return Some((Ok(batch), Some((offset, changed))));
                        }
                        NextBatch::Wait => changed.changed().await.ok()?,
                        NextBatch::Close => {
                            return Some((Err(Status::unavailable("the shard was deleted")), None));
                        }
                    }
                }
            }
        });
        Ok(Response::new(
            futures::stream::once(async { Ok(confirmation) })
                .chain(batches)
                .boxed(),
        ))
    }

    async fn create_session(
        &self,
        _: Request<proto::CreateSessionRequest>,
    ) -> Rpc<proto::CreateSessionResponse> {
        Err(Status::unimplemented("not scripted"))
    }

    async fn keep_alive(
        &self,
        _: Request<proto::SessionHeartbeat>,
    ) -> Rpc<proto::KeepAliveResponse> {
        Err(Status::unimplemented("not scripted"))
    }

    async fn close_session(
        &self,
        _: Request<proto::CloseSessionRequest>,
    ) -> Rpc<proto::CloseSessionResponse> {
        Err(Status::unimplemented("not scripted"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OxiaClient, OxiaClientBuilder, OxiaError};
    use std::time::Instant;

    async fn connect(mock: &MockOxia, request_timeout: Duration) -> OxiaClient {
        OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(request_timeout)
            .max_write_batches_in_flight(1)
            .build()
            .await
            .expect("client connects to mock")
    }

    /// With the in-flight window saturated, accumulating batches seal at
    /// `max_requests_per_batch`: 1 immediate dispatch, then 3 + 3.
    #[compio::test]
    async fn batches_seal_at_max_requests() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::DelayResponses(Duration::from_millis(200)),
            ..Behavior::default()
        })
        .await;
        let client = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_secs(10))
            .max_write_batches_in_flight(1)
            .max_requests_per_batch(3)
            .build()
            .await
            .expect("connect");

        let first = compio::runtime::spawn({
            let client = client.clone();
            async move { client.put("count/first", "x").await }
        });
        // Let the first put dispatch alone, then queue six more behind it.
        compio::time::sleep(Duration::from_millis(50)).await;
        let rest: Vec<_> = (0..6)
            .map(|i| {
                let client = client.clone();
                compio::runtime::spawn(async move { client.put(format!("count/{i}"), "x").await })
            })
            .collect();
        first.await.expect("join").expect("first put");
        for put in rest {
            put.await.expect("join").expect("put");
        }

        assert_eq!(mock.write_batch_sizes(), vec![1, 3, 3]);
    }

    /// With the window saturated, accumulating batches seal when the next
    /// operation would exceed `batch_max_size`.
    #[compio::test]
    async fn batches_seal_at_max_bytes() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::DelayResponses(Duration::from_millis(200)),
            ..Behavior::default()
        })
        .await;
        let client = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_secs(10))
            .max_write_batches_in_flight(1)
            // Two ~110-byte puts fit; a third (~330 bytes total) does not.
            .batch_max_size(300)
            .build()
            .await
            .expect("connect");

        let first = compio::runtime::spawn({
            let client = client.clone();
            async move { client.put("bytes/first", vec![0u8; 100]).await }
        });
        compio::time::sleep(Duration::from_millis(50)).await;
        let rest: Vec<_> = (0..6)
            .map(|i| {
                let client = client.clone();
                compio::runtime::spawn(async move {
                    client.put(format!("bytes/{i}"), vec![0u8; 100]).await
                })
            })
            .collect();
        first.await.expect("join").expect("first put");
        for put in rest {
            put.await.expect("join").expect("put");
        }

        let sizes = mock.write_batch_sizes();
        assert_eq!(sizes.iter().sum::<usize>(), 7, "sizes: {sizes:?}");
        assert!(
            sizes.iter().all(|&s| s <= 2),
            "byte limit must cap batches at 2 puts, got {sizes:?}"
        );
        assert!(
            sizes.contains(&2),
            "accumulation must pack 2 puts per batch, got {sizes:?}"
        );
    }

    /// A write larger than `batch_max_size` goes out in a batch of its own,
    /// after the writes issued before it and ahead of those issued after it.
    #[compio::test]
    async fn oversized_write_goes_out_alone_in_issue_order() {
        let mock = MockOxia::start(Behavior::default()).await;
        let client = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_secs(10))
            .batch_max_size(100)
            .build()
            .await
            .expect("connect");

        // `biased` polls the puts in order, and each one joins the open
        // batch on its first poll: the small ones would share a batch with
        // the large one if it took them.
        let (a, b, large, c, d) = tokio::join!(
            biased;
            client.put("a", "x"),
            client.put("b", "x"),
            client.put("large", vec![0u8; 1000]),
            client.put("c", "x"),
            client.put("d", "x"),
        );
        for put in [a, b, large, c, d] {
            put.expect("put");
        }

        let batches: Vec<Vec<String>> = mock
            .write_requests()
            .into_iter()
            .map(|request| request.puts.into_iter().map(|put| put.key).collect())
            .collect();
        assert_eq!(batches, [vec!["a", "b"], vec!["large"], vec!["c", "d"]]);
    }

    /// No more write batches than `max_write_batches_in_flight` are ever
    /// outstanding on a shard.
    #[compio::test]
    async fn write_window_is_bounded() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::Hang,
            ..Behavior::default()
        })
        .await;
        let client = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_millis(800))
            .max_write_batches_in_flight(2)
            .build()
            .await
            .expect("connect");

        // Stagger the first two puts so each dispatches as its own batch and
        // fills one window slot; the remaining four must then queue.
        let mut puts = Vec::new();
        for i in 0..6 {
            let client = client.clone();
            puts.push(compio::runtime::spawn(async move {
                client.put(format!("window/{i}"), "x").await
            }));
            if i < 2 {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        // Give the batcher ample time to dispatch everything it is willing to.
        compio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            mock.write_batch_sizes().len(),
            2,
            "only the window's worth of batches may be dispatched"
        );

        // The server never answers, so every operation times out.
        for put in puts {
            assert!(matches!(put.await.expect("join"), Err(OxiaError::Timeout)));
        }
    }

    /// A server that acknowledges a batch with missing per-operation
    /// responses produces a decode error, not a hang.
    #[compio::test]
    async fn missing_responses_fail_the_batch() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::OmitResponses,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        let result = client.put("missing/key", "x").await;
        match result {
            Err(OxiaError::Decode(message)) => {
                assert!(message.contains("missing put response"), "got: {message}")
            }
            other => panic!("expected a decode error, got {other:?}"),
        }
    }

    /// A stream torn after the server received a batch (fate unknown) makes
    /// the client requeue and re-send it on a fresh stream.
    #[compio::test]
    async fn torn_stream_requeues_and_retries() {
        let mock = MockOxia::start(Behavior {
            torn_write_connections: 1,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(10)).await;

        client
            .put("torn/key", "x")
            .await
            .expect("put succeeds after retry");
        assert!(
            mock.write_connections() >= 2,
            "the batch must have been re-sent on a fresh stream"
        );
        assert!(
            mock.write_batch_sizes().len() >= 2,
            "the batch must have been received twice (at-least-once)"
        );
    }

    /// A write the server never acknowledges fails with `Timeout` once the
    /// request timeout elapses — it does not hang.
    #[compio::test]
    async fn hung_write_times_out() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::Hang,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_millis(500)).await;

        let start = Instant::now();
        let result = client.put("hang/key", "x").await;
        assert!(matches!(result, Err(OxiaError::Timeout)), "got {result:?}");
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    /// A read the server never answers fails with `Timeout` as well.
    #[compio::test]
    async fn hung_read_times_out() {
        let mock = MockOxia::start(Behavior {
            read: ReadMode::Hang,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_millis(500)).await;

        let start = Instant::now();
        let result = client.get("hang/key").await;
        assert!(matches!(result, Err(OxiaError::Timeout)), "got {result:?}");
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    /// Per-operation proto statuses in write responses map to the typed
    /// errors.
    #[compio::test]
    async fn write_statuses_map_to_typed_errors() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::PutStatus(proto::Status::UnexpectedVersionId as i32),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;
        let result = client.put("map/key", "x").expected_version_id(7).await;
        assert!(
            matches!(result, Err(OxiaError::UnexpectedVersionId)),
            "got {result:?}"
        );
        drop(client);

        let mock = MockOxia::start(Behavior {
            write: WriteMode::DeleteStatus(proto::Status::KeyNotFound as i32),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;
        let result = client.delete("map/other").await;
        assert!(
            matches!(result, Err(OxiaError::KeyNotFound)),
            "got {result:?}"
        );
    }

    /// The `INVALID_ARGUMENT` status a server returns for an invalid request
    /// maps to `InvalidArgument`, not to a decode error. The put itself is
    /// valid, so that it is the server, not the client, that rejects it.
    #[compio::test]
    async fn invalid_argument_status_maps_to_invalid_argument() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::PutStatus(proto::Status::InvalidArgument as i32),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        let result = client.put("invalid/key", "x").await;
        assert!(
            matches!(result, Err(OxiaError::InvalidArgument(_))),
            "got {result:?}"
        );
        assert_eq!(mock.write_requests().len(), 1, "the put reaches the server");
    }

    /// A put with a `/` in a secondary index name fails with `InvalidArgument`
    /// before it is batched, since a server that predates the check would
    /// store it; a `/` in the secondary key is valid.
    #[compio::test]
    async fn slash_in_index_name_fails_before_reaching_the_server() {
        let mock = MockOxia::start(Behavior::default()).await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        let result = client
            .put("index/key", "x")
            .secondary_index("by-name", "alice")
            .secondary_index("tenant/users", "email")
            .await;
        match result {
            Err(OxiaError::InvalidArgument(message)) => assert!(
                message.contains("\"tenant/users\""),
                "the error must name the offending index, got: {message}"
            ),
            other => panic!("expected an invalid argument, got {other:?}"),
        }
        assert!(
            mock.write_requests().is_empty(),
            "nothing reaches the server"
        );

        client
            .put("index/key", "x")
            .secondary_index("tenant", "users/email")
            .await
            .expect("a '/' in the secondary key is valid");
        // Had the rejected put been sent anyway, it would precede the valid one
        // on the same stream: once that is acknowledged, it must be the only
        // put the server has seen.
        let indexes: Vec<_> = mock
            .write_requests()
            .into_iter()
            .flat_map(|request| request.puts)
            .map(|put| put.secondary_indexes)
            .collect();
        assert_eq!(
            indexes,
            vec![vec![proto::SecondaryIndex {
                index_name: "tenant".to_string(),
                secondary_key: "users/email".to_string(),
            }]]
        );
    }

    /// Per-operation proto statuses in read responses map to the typed
    /// errors, and a healthy read produces the served value.
    #[compio::test]
    async fn read_statuses_map_to_typed_errors() {
        let mock = MockOxia::start(Behavior::default()).await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        let record = client.get("read/key").await.expect("scripted get succeeds");
        assert_eq!(record.value.as_deref(), Some(b"mock-value".as_ref()));

        mock.set_read_mode(ReadMode::GetStatus(proto::Status::KeyNotFound as i32));
        let result = client.get("read/key").await;
        assert!(
            matches!(result, Err(OxiaError::KeyNotFound)),
            "got {result:?}"
        );
    }

    /// A permanent gRPC error on the assignments stream fails `build()` fast
    /// instead of retrying until the deadline.
    #[compio::test]
    async fn permanent_error_fails_build_fast() {
        let mock = MockOxia::start(Behavior {
            assignments_error: Some(tonic::Code::Unauthenticated),
            ..Behavior::default()
        })
        .await;

        let start = Instant::now();
        let result = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_secs(30))
            .build()
            .await;
        assert!(result.is_err());
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "a permanent error must fail fast, took {:?}",
            start.elapsed()
        );
    }

    /// A delete carries its partition key on the wire, so that a shard split
    /// replays it only on the child that holds the record; without one, the
    /// field stays absent.
    #[compio::test]
    async fn delete_sends_its_partition_key() {
        let mock = MockOxia::start(Behavior::default()).await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        client
            .delete("pk/with")
            .partition_key("pk")
            .await
            .expect("delete with a partition key");
        client
            .delete("pk/without")
            .await
            .expect("delete without a partition key");

        let deletes: Vec<_> = mock
            .write_requests()
            .into_iter()
            .flat_map(|request| request.deletes)
            .map(|delete| (delete.key, delete.partition_key))
            .collect();
        assert_eq!(
            deletes,
            vec![
                ("pk/with".to_string(), Some("pk".to_string())),
                ("pk/without".to_string(), None),
            ]
        );
    }

    /// An explicitly empty partition key is a partition key like any other: a
    /// put or delete with one is routed by `hash("")` and carries it on the
    /// wire, rather than being treated as absent and routed by its own key.
    #[compio::test]
    async fn empty_partition_key_is_hashed_normally() {
        let mock = MockOxia::start(Behavior {
            shards: 2,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;
        let shard_of = |routing_key: &str| {
            let hash = crate::hash::xxh3_32(routing_key);
            (0..2)
                .find(|&shard| {
                    let (min, max) = hash_range(shard, 2);
                    (min..=max).contains(&hash)
                })
                .map(i64::from)
        };
        // A record key owned by the other shard, so that routing by it instead
        // of by the empty partition key would show.
        let key = (0..)
            .map(|i| format!("empty-pk/{i}"))
            .find(|key| shard_of(key) != shard_of(""))
            .expect("a key on the other shard");

        client
            .put(key.clone(), "x")
            .partition_key("")
            .await
            .expect("put");
        client
            .delete(key.clone())
            .partition_key("")
            .await
            .expect("delete");

        let requests = mock.write_requests();
        assert_eq!(requests.len(), 2, "requests: {requests:?}");
        for request in &requests {
            assert_eq!(request.shard, shard_of(""), "request: {request:?}");
        }
        assert_eq!(requests[0].puts[0].partition_key.as_deref(), Some(""));
        assert_eq!(requests[1].deletes[0].partition_key.as_deref(), Some(""));
    }

    /// A delete range without a partition key joins the batch of every shard
    /// when it is issued, as a put or a delete does: the writes issued after
    /// it follow it in the batch, with a higher `op_index`, so that a server
    /// with ordered writes applies it before them.
    #[compio::test]
    async fn delete_range_precedes_the_writes_issued_after_it() {
        let mock = MockOxia::start(Behavior {
            shards: 2,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        // `biased` polls the operations in order, and each one joins the open
        // batches on its first poll. On this single-threaded runtime no batch
        // is flushed before all of them are in it.
        let (delete_range, put_a, put_b, put_c, put_d) = tokio::join!(
            biased;
            client.delete_range("range/", "range/~"),
            client.put("range/a", "x"),
            client.put("range/b", "x"),
            client.put("range/c", "x"),
            client.put("range/d", "x"),
        );
        delete_range.expect("delete range");
        for put in [put_a, put_b, put_c, put_d] {
            put.expect("put");
        }

        // One batch per shard: the delete range, then the puts routed there.
        let requests = mock.write_requests();
        assert_eq!(requests.len(), 2, "requests: {requests:?}");
        for request in &requests {
            let delete_ranges: Vec<_> = request.delete_ranges.iter().map(|r| r.op_index).collect();
            let puts: Vec<_> = request.puts.iter().map(|p| p.op_index).collect();
            assert!(!puts.is_empty(), "every shard gets a put: {requests:?}");
            assert_eq!(delete_ranges, [0], "shard {:?}", request.shard);
            assert_eq!(
                puts,
                (1..=puts.len() as u32).collect::<Vec<_>>(),
                "shard {:?}",
                request.shard
            );
        }
    }

    /// A floor or ceiling get without a partition key goes to every shard,
    /// and the client keeps the largest or smallest answer in the key sorting
    /// that the assignments report. A server that reports none keeps the
    /// legacy slash-aware order. The natural and hierarchical cases are the
    /// examples of oxia-db/oxia#1388, where the legacy order picks the other
    /// answer.
    #[compio::test]
    async fn multi_shard_gets_pick_in_the_key_sorting() {
        use crate::ComparisonType::{Ceiling, Floor};
        use proto::KeySorting::{Hierarchical, Natural, Unknown};

        for (key_sorting, comparison, key, answers, expected) in [
            (Natural, Floor, "b", ["a/y/z", "a0"], "a0"),
            (Natural, Ceiling, "a", ["a0", "a/y/z"], "a/y/z"),
            (Hierarchical, Floor, "a/z", ["ab/y", "a/x"], "a/x"),
            (Hierarchical, Ceiling, "a", ["a/x", "ab/y"], "ab/y"),
            (Unknown, Floor, "a/z", ["ab/y", "a/x"], "ab/y"),
            (Unknown, Ceiling, "a", ["a/x", "ab/y"], "a/x"),
        ] {
            // The third shard has no answer.
            let mock = MockOxia::start(Behavior {
                read: ReadMode::ShardKeys,
                shards: 3,
                key_sorting,
                shard_keys: HashMap::from([(0, vec![answers[0]]), (1, vec![answers[1]])]),
                ..Behavior::default()
            })
            .await;
            let client = connect(&mock, Duration::from_secs(5)).await;

            let found = client
                .get(key)
                .comparison(comparison)
                .await
                .expect("a shard has an answer");
            assert_eq!(
                found.key, expected,
                "{key_sorting:?} {comparison:?} {key:?} among {answers:?}"
            );
        }
    }

    /// A list or a range scan without a partition key merges the keys of the
    /// shards, which each shard returns in the key sorting of the namespace,
    /// in the same order (the vectors of the Go reference's merge tests).
    #[compio::test]
    async fn list_and_range_scan_merge_in_the_key_sorting() {
        use proto::KeySorting::{Hierarchical, Natural, Unknown};

        for (key_sorting, shards, expected) in [
            (
                Natural,
                [vec!["a/x", "a0"], vec!["a/y/z", "ab/y"]],
                ["a/x", "a/y/z", "a0", "ab/y"],
            ),
            (
                Hierarchical,
                [vec!["a0", "a/x"], vec!["ab/y", "a/y/z"]],
                ["a0", "ab/y", "a/x", "a/y/z"],
            ),
            (
                Unknown,
                [vec!["a0", "a/x"], vec!["a/y/z", "ab/y"]],
                ["a0", "a/x", "a/y/z", "ab/y"],
            ),
        ] {
            let [shard_0, shard_1] = shards;
            let mock = MockOxia::start(Behavior {
                shards: 2,
                key_sorting,
                shard_keys: HashMap::from([(0, shard_0), (1, shard_1)]),
                ..Behavior::default()
            })
            .await;
            let client = connect(&mock, Duration::from_secs(5)).await;

            let listed = client.list("", "").await.expect("list");
            assert_eq!(listed, expected, "list, {key_sorting:?}");
            let scanned: Vec<_> = client
                .range_scan("", "")
                .await
                .expect("range scan")
                .into_iter()
                .map(|record| record.key)
                .collect();
            assert_eq!(scanned, expected, "range scan, {key_sorting:?}");
        }
    }

    /// A get through an index without a partition key goes to every shard,
    /// and the client keeps the largest or smallest answer by the secondary
    /// keys, in the key sorting of the namespace (the vectors of the Go
    /// reference, `TestMultiShardIndexGetKeySorting`). In the natural cases,
    /// comparing the keys instead picks the other answer.
    #[compio::test]
    async fn multi_shard_index_gets_pick_by_the_secondary_keys() {
        use crate::ComparisonType::{Ceiling, Floor};
        use proto::KeySorting::{Hierarchical, Natural, Unknown};

        for (key_sorting, comparison, expected) in [
            // "a/y/z" < "a0"
            (Natural, Floor, "p1"),
            (Natural, Ceiling, "p2"),
            // "a0" < "a/y/z"
            (Hierarchical, Floor, "p2"),
            (Hierarchical, Ceiling, "p1"),
            // "a0" < "a/y/z" in the legacy order
            (Unknown, Floor, "p2"),
            (Unknown, Ceiling, "p1"),
        ] {
            let mock = MockOxia::start(Behavior {
                read: ReadMode::ShardKeys,
                shards: 2,
                key_sorting,
                shard_keys: HashMap::from([(0, vec!["p1"]), (1, vec!["p2"])]),
                secondary_keys: HashMap::from([("p1", "a0"), ("p2", "a/y/z")]),
                ..Behavior::default()
            })
            .await;
            let client = connect(&mock, Duration::from_secs(5)).await;

            let found = client
                .get("b")
                .comparison(comparison)
                .use_index("idx")
                .await
                .expect("a shard has an answer");
            assert_eq!(found.key, expected, "{key_sorting:?} {comparison:?}");
        }
    }

    /// A range scan through an index without a partition key merges the
    /// records of the shards, which each shard returns in the order of their
    /// secondary keys, by the secondary keys that come with them, in the key
    /// sorting of the namespace (the vectors of the Go reference,
    /// `TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys`). Comparing
    /// the keys instead puts the records out of order in every case.
    #[compio::test]
    async fn index_range_scans_merge_by_the_secondary_keys() {
        use proto::KeySorting::{Hierarchical, Natural, Unknown};

        for (case, key_sorting, [shard_0, shard_1], expected) in [
            (
                "natural",
                Natural,
                [
                    vec![("p4", "a/x"), ("p2", "a0")],
                    vec![("p3", "a/y/z"), ("p1", "ab/y")],
                ],
                vec!["p4", "p3", "p2", "p1"],
            ),
            (
                "hierarchical",
                Hierarchical,
                [
                    vec![("p4", "a0"), ("p2", "a/x")],
                    vec![("p3", "ab/y"), ("p1", "a/y/z")],
                ],
                vec!["p4", "p3", "p2", "p1"],
            ),
            (
                "unknown",
                Unknown,
                [
                    vec![("p4", "a0"), ("p2", "a/x")],
                    vec![("p3", "a/y/z"), ("p1", "ab/y")],
                ],
                vec!["p4", "p2", "p3", "p1"],
            ),
            (
                "same secondary key",
                Natural,
                [
                    vec![("p2", "a"), ("p3", "b")],
                    vec![("p1", "b"), ("p4", "c")],
                ],
                vec!["p2", "p1", "p3", "p4"],
            ),
            (
                "empty secondary key",
                Natural,
                [vec![("p2", "")], vec![("p1", "a")]],
                vec!["p2", "p1"],
            ),
        ] {
            let keys = |records: &[(&'static str, &'static str)]| -> Vec<&'static str> {
                records.iter().map(|(key, _)| *key).collect()
            };
            let mock = MockOxia::start(Behavior {
                shards: 2,
                key_sorting,
                shard_keys: HashMap::from([(0, keys(&shard_0)), (1, keys(&shard_1))]),
                secondary_keys: shard_0.iter().chain(&shard_1).copied().collect(),
                ..Behavior::default()
            })
            .await;
            let client = connect(&mock, Duration::from_secs(5)).await;

            let scanned: Vec<_> = client
                .range_scan("a", "z")
                .use_index("idx")
                .await
                .expect("range scan")
                .into_iter()
                .map(|record| record.key)
                .collect();
            assert_eq!(scanned, expected, "{case}");
        }
    }

    /// A list or a range scan with `end_inclusive` fails with
    /// `EndInclusiveUnsupported` unless the leader of every shard includes
    /// the end of the range, which it says in all its responses, sending one
    /// at least: an older server ignores the flag and leaves the end out.
    /// The cases are those of the Go reference (`endInclusiveTests`).
    #[compio::test]
    async fn end_inclusive_needs_every_shard_to_include_the_end() {
        for (case, end_inclusive, [shard_0, shard_1], end_inclusive_shards, expected) in [
            (
                "included",
                true,
                [vec!["a", "b"], vec!["c"]],
                vec![0, 1],
                Some(vec!["a", "b", "c"]),
            ),
            (
                "included, empty shard",
                true,
                [vec!["a"], vec![]],
                vec![0, 1],
                Some(vec!["a"]),
            ),
            (
                "older server",
                true,
                [vec!["a"], vec!["c", "d"]],
                vec![0],
                None,
            ),
            (
                "older server, empty shard",
                true,
                [vec!["a"], vec![]],
                vec![0],
                None,
            ),
            (
                "not requested",
                false,
                [vec!["a"], vec!["c"]],
                vec![],
                Some(vec!["a", "c"]),
            ),
        ] {
            let mock = MockOxia::start(Behavior {
                shards: 2,
                shard_keys: HashMap::from([(0, shard_0), (1, shard_1)]),
                end_inclusive_shards,
                ..Behavior::default()
            })
            .await;
            let client = connect(&mock, Duration::from_secs(5)).await;

            let (mut list, mut scan) = (client.list("a", "z"), client.range_scan("a", "z"));
            if end_inclusive {
                (list, scan) = (list.end_inclusive(), scan.end_inclusive());
            }
            let listed = list.await;
            let scanned = scan
                .await
                .map(|records| records.into_iter().map(|r| r.key).collect::<Vec<_>>());
            match expected {
                Some(expected) => {
                    assert_eq!(listed.expect(case), expected, "list, {case}");
                    assert_eq!(scanned.expect(case), expected, "range scan, {case}");
                }
                None => {
                    assert!(
                        matches!(listed, Err(OxiaError::EndInclusiveUnsupported)),
                        "list, {case}: {listed:?}"
                    );
                    assert!(
                        matches!(scanned, Err(OxiaError::EndInclusiveUnsupported)),
                        "range scan, {case}: {scanned:?}"
                    );
                }
            }
        }
    }

    /// Waits until the mock has failed `count` requests for shards split
    /// away.
    async fn until_split_shard_requests(mock: &MockOxia, count: u32) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while mock.split_shard_requests() < count {
            assert!(
                Instant::now() < deadline,
                "no request reached the split shard"
            );
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Of the shards `lower` and `upper`, which own a half of the hash space
    /// each, the one that owns `key`.
    fn half_owning(key: &str, (lower, upper): (i64, i64)) -> i64 {
        if crate::hash::xxh3_32(key) <= u32::MAX / 2 {
            lower
        } else {
            upper
        }
    }

    /// A get whose shard is split away before the get reaches the server:
    /// the node of the deleted parent rejects it, with a leader hint at
    /// itself while its assignments still list the parent, and once the
    /// client has the new assignments, it sends the get to the child that
    /// owns the key, rather than retry the parent until the request timeout.
    /// A get that goes to every shard, as a floor get does, goes to the
    /// children.
    #[compio::test]
    async fn gets_reroute_across_a_split() {
        use crate::ComparisonType::{Equal, Floor};

        for (leader_hint, comparison) in
            [(false, Equal), (true, Equal), (false, Floor), (true, Floor)]
        {
            let case = format!("leader hint: {leader_hint}, {comparison:?}");
            let mock = MockOxia::start(Behavior {
                read: ReadMode::ShardKeys,
                // The children of shard 0.
                shard_keys: HashMap::from([(1, vec!["a"]), (2, vec!["b"])]),
                ..Behavior::default()
            })
            .await;
            let client = connect(&mock, Duration::from_secs(5)).await;
            let children = mock.split_shard(0, leader_hint);
            assert_eq!(children, (1, 2));

            let get =
                compio::runtime::spawn(client.get("key").comparison(comparison).into_future());
            until_split_shard_requests(&mock, 1).await;
            mock.publish_assignments();
            let found = get.await.expect("join").expect(&case);
            let expected = match comparison {
                Equal if half_owning("key", children) == 1 => "a",
                Equal => "b",
                // The largest of the children's answers.
                _ => "b",
            };
            assert_eq!(found.key, expected, "{case}");
        }
    }

    /// A list or a range scan that opens a stream on a shard split away
    /// fails with `ShardMoved` once the client has the new assignments,
    /// rather than retry the shard until the request timeout, as the Go
    /// client fails with `ErrShardNotFound`; with a partition key on the
    /// shard too.
    #[compio::test]
    async fn lists_and_range_scans_of_a_split_shard_fail_with_shard_moved() {
        for leader_hint in [false, true] {
            for partition_key in [None, Some("pk")] {
                for scan in [false, true] {
                    let case = format!(
                        "leader hint: {leader_hint}, partition key: {partition_key:?}, \
                         range scan: {scan}"
                    );
                    let mock = MockOxia::start(Behavior {
                        shards: 2,
                        ..Behavior::default()
                    })
                    .await;
                    let client = connect(&mock, Duration::from_secs(10)).await;
                    let shard = partition_key.map_or(0, |key| half_owning(key, (0, 1)));
                    mock.split_shard(shard, leader_hint);

                    let result = compio::runtime::spawn({
                        let client = client.clone();
                        async move {
                            if scan {
                                let mut builder = client.range_scan("", "");
                                if let Some(key) = partition_key {
                                    builder = builder.partition_key(key);
                                }
                                builder.await.map(|records| records.len())
                            } else {
                                let mut builder = client.list("", "");
                                if let Some(key) = partition_key {
                                    builder = builder.partition_key(key);
                                }
                                builder.await.map(|keys| keys.len())
                            }
                        }
                    });
                    until_split_shard_requests(&mock, 1).await;
                    let published = Instant::now();
                    mock.publish_assignments();
                    let result = result.await.expect("join");
                    assert!(
                        matches!(result, Err(OxiaError::ShardMoved)),
                        "{case}: {result:?}"
                    );
                    assert!(
                        published.elapsed() < Duration::from_secs(3),
                        "{case}: failed after {:?}",
                        published.elapsed()
                    );
                }
            }
        }
    }

    /// A key starting with `prefix` whose hash is in `[min, max]`.
    fn key_hashed_in(prefix: &str, min: u32, max: u32) -> String {
        (0u32..)
            .map(|n| format!("{prefix}{n}"))
            .find(|key| (min..=max).contains(&crate::hash::xxh3_32(key)))
            .expect("a key in the range")
    }

    /// The keys of the next `n` notifications, sorted: the order across
    /// shards is unspecified.
    async fn next_keys(notifications: &mut crate::Notifications, n: usize) -> Vec<String> {
        let mut keys = Vec::with_capacity(n);
        for _ in 0..n {
            let notification = compio::time::timeout(Duration::from_secs(5), notifications.recv())
                .await
                .expect("a notification arrives")
                .expect("the subscription is open");
            keys.push(notification.key().to_string());
        }
        keys.sort();
        keys
    }

    async fn assert_no_more_notifications(notifications: &mut crate::Notifications) {
        let next = compio::time::timeout(Duration::from_millis(200), notifications.recv()).await;
        assert!(next.is_err(), "unexpected notification: {next:?}");
    }

    /// The start offsets of the notification subscriptions to `shard`.
    fn start_offsets(mock: &MockOxia, shard: i64) -> Vec<Option<i64>> {
        mock.notification_requests()
            .into_iter()
            .filter(|request| request.shard == shard)
            .map(|request| request.start_offset_exclusive)
            .collect()
    }

    /// A subscription follows the split of a shard: it subscribes to the
    /// children from the last batch it received from the parent, whether
    /// the parent's stream stays open, as until the node deletes the parent,
    /// or ends before the client has the new assignments. Each child
    /// delivers the parent's batches that the subscription had not received,
    /// on its side of the split, then its own: every notification arrives
    /// once. A child split in turn hands the subscription over the same way.
    #[compio::test]
    async fn notifications_follow_a_split_from_the_last_batch_received() {
        for parent_stream_ends in [false, true] {
            let case = format!("parent stream ends: {parent_stream_ends}");
            let mock = MockOxia::start(Behavior::default()).await;
            let client = connect(&mock, Duration::from_secs(5)).await;
            mock.notify(0, &["before"]);
            let mut notifications = client.notifications().await.expect("subscribe");
            assert_eq!(mock.notify(0, &["a"]), 1);
            assert_eq!(next_keys(&mut notifications, 1).await, ["a"], "{case}");

            // The subscriber lags behind as shard 0 is split into shards 1
            // and 2: it does not receive the last batches of the parent.
            let lower = key_hashed_in("lower-", 0, u32::MAX / 2);
            let upper = key_hashed_in("upper-", u32::MAX / 2 + 1, u32::MAX);
            mock.hold_notifications(0);
            mock.notify(0, &[&lower]);
            mock.notify(0, &[&upper]);
            assert_eq!(mock.split_shard(0, false), (1, 2));
            if parent_stream_ends {
                mock.close_notification_streams(0);
                until_split_shard_requests(&mock, 1).await;
            }
            mock.publish_assignments();
            assert_eq!(
                next_keys(&mut notifications, 2).await,
                [lower, upper],
                "{case}"
            );
            assert_eq!(mock.notify(1, &["b"]), 4);
            assert_eq!(mock.notify(2, &["c"]), 4);
            assert_eq!(next_keys(&mut notifications, 2).await, ["b", "c"], "{case}");
            for child in [1, 2] {
                assert_eq!(start_offsets(&mock, child), [Some(1)], "{case}: {child}");
            }

            // Shard 2 is split into shards 3 and 4, while the subscriber
            // lags behind again.
            let third_quarter = key_hashed_in("third-", u32::MAX / 2 + 1, u32::MAX / 4 * 3);
            mock.hold_notifications(2);
            mock.notify(2, &[&third_quarter]);
            assert_eq!(mock.split_shard(2, false), (3, 4));
            mock.publish_assignments();
            assert_eq!(
                next_keys(&mut notifications, 1).await,
                [third_quarter],
                "{case}"
            );
            mock.notify(3, &["d"]);
            mock.notify(4, &["e"]);
            assert_eq!(next_keys(&mut notifications, 2).await, ["d", "e"], "{case}");
            for grandchild in [3, 4] {
                assert_eq!(
                    start_offsets(&mock, grandchild),
                    [Some(4)],
                    "{case}: {grandchild}"
                );
            }
            assert_no_more_notifications(&mut notifications).await;
        }
    }

    /// A shard split before the subscription to it is established: the
    /// subscriptions to the children establish it in its place, from where
    /// they start, as no batch of the parent tells where to start.
    #[compio::test]
    async fn notifications_follow_a_split_before_the_subscription_is_established() {
        let mock = MockOxia::start(Behavior::default()).await;
        let client = connect(&mock, Duration::from_secs(5)).await;
        // Only the server sees the split: it rejects the subscription to the
        // parent.
        assert_eq!(mock.split_shard(0, false), (1, 2));
        let subscription = compio::runtime::spawn(client.notifications().into_future());
        until_split_shard_requests(&mock, 1).await;
        mock.publish_assignments();
        let mut notifications = subscription
            .await
            .expect("join")
            .expect("the subscription is established");
        for child in [1, 2] {
            assert_eq!(start_offsets(&mock, child), [None], "shard {child}");
        }
        mock.notify(1, &["a"]);
        mock.notify(2, &["b"]);
        assert_eq!(next_keys(&mut notifications, 2).await, ["a", "b"]);
        assert_no_more_notifications(&mut notifications).await;
    }

    /// A shard without a leader fails with `LeaderNotFound`; its empty leader
    /// address is not dialed.
    #[compio::test]
    async fn leaderless_shard_reports_leader_not_found() {
        let mock = MockOxia::start(Behavior {
            leaderless_for: Duration::from_secs(60),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_millis(500)).await;

        let result = client.get("leaderless/key").await;
        assert!(
            matches!(result, Err(OxiaError::LeaderNotFound { shard: 0 })),
            "got {result:?}"
        );
    }

    /// A write waiting for a shard leader goes out as soon as one is elected,
    /// not when its reconnect backoff ends.
    #[compio::test]
    async fn write_goes_out_once_the_shard_gets_a_leader() {
        // The reconnects back off 100, 200, 400, then 800 ms: the leader is
        // elected 1 s in, during the last wait, which ends after the request
        // timeout.
        let mock = MockOxia::start(Behavior {
            leaderless_for: Duration::from_secs(1),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_millis(1300)).await;

        client
            .put("election/key", "x")
            .await
            .expect("put once the leader is elected");
    }

    /// Closing the client while operations are in flight, and while a
    /// subscription is dropped, neither panics nor hangs: the client's state
    /// is shared by the tasks of its thread, and `close` must not keep it
    /// borrowed while it waits. Each operation completes, or fails with
    /// `Closed`.
    #[compio::test]
    async fn close_races_in_flight_operations() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::DelayResponses(Duration::from_millis(100)),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;
        let notifications = client.notifications().await.expect("subscribe");
        let ops: Vec<_> = (0..20)
            .map(|i| {
                let client = client.clone();
                compio::runtime::spawn(async move {
                    if i % 2 == 0 {
                        client.put(format!("close/{i}"), "x").await.map(drop)
                    } else {
                        client.get(format!("close/{i}")).await.map(drop)
                    }
                })
            })
            .collect();
        // Let the operations reach their batchers.
        compio::time::sleep(Duration::from_millis(20)).await;

        let (closed, ()) = futures::join!(client.close(), async move { drop(notifications) });
        closed.expect("close");
        for op in ops {
            let result = op.await.expect("the operation does not panic");
            assert!(
                matches!(result, Ok(()) | Err(OxiaError::Closed)),
                "got {result:?}"
            );
        }
    }

    /// A key starting with `prefix` whose hash lies in `[min_hash, max_hash]`.
    fn key_in(prefix: &str, min_hash: u32, max_hash: u32) -> String {
        (0u32..)
            .map(|n| format!("{prefix}{n}"))
            .find(|key| (min_hash..=max_hash).contains(&crate::hash::xxh3_32(key)))
            .expect("a key lands in the hash range")
    }

    /// Runs a write in a task of its own.
    fn spawn_write<T: 'static>(
        write: impl Future<Output = Result<T, OxiaError>> + 'static,
    ) -> compio::runtime::JoinHandle<Result<(), OxiaError>> {
        compio::runtime::spawn(async move { write.await.map(drop) })
    }

    async fn wait_until(what: &str, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "timed out waiting until {what}");
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The writes that the mock received for `shard`, in the order the shard
    /// applies them: request by request, each in `op_index` order.
    fn applied_on(mock: &MockOxia, shard: i64) -> Vec<String> {
        mock.write_requests()
            .into_iter()
            .filter(|request| request.shard == Some(shard))
            .flat_map(|request| {
                let puts = request.puts.iter().map(|put| {
                    let value = String::from_utf8_lossy(&put.value);
                    (put.op_index, format!("put {}={value}", put.key))
                });
                let deletes = request
                    .deletes
                    .iter()
                    .map(|delete| (delete.op_index, format!("delete {}", delete.key)));
                let delete_ranges = request.delete_ranges.iter().map(|range| {
                    let op = format!(
                        "delete_range {}..{}",
                        range.start_inclusive, range.end_exclusive
                    );
                    (range.op_index, op)
                });
                let mut ops: Vec<_> = puts.chain(deletes).chain(delete_ranges).collect();
                ops.sort();
                ops.into_iter().map(|(_, op)| op)
            })
            .collect()
    }

    /// The writes pending on a shard when it is split go to the shards that
    /// replaced it, in the order they were issued: a delete range to every
    /// one of them, unless it has a partition key. They go ahead of the
    /// writes issued once the client has the new assignments, and of those
    /// still routed to the split shard.
    #[compio::test]
    async fn split_hands_off_the_pending_writes_in_issue_order() {
        let mock = MockOxia::start(Behavior::default()).await;
        let client = connect(&mock, Duration::from_secs(10)).await;
        let middle = u32::MAX / 2;
        let lower = key_in("lower-", 0, middle);
        let upper = key_in("upper-", middle + 1, u32::MAX);

        // The shard rejects the writes for the cutover of its split: the
        // client retries the batch until it gets the new assignments.
        mock.freeze_shard(0);
        // Each from a task of its own, as independent callers would issue
        // them: the runtime runs the tasks in turn, so the writes join the
        // batch in this order.
        let pending = [
            spawn_write(client.put(lower.clone(), "1").into_future()),
            spawn_write(client.put(upper.clone(), "2").into_future()),
            spawn_write(
                client
                    .delete_range("p", "q")
                    .partition_key(lower.clone())
                    .into_future(),
            ),
            spawn_write(client.delete(lower.clone()).into_future()),
            spawn_write(client.put(lower.clone(), "3").into_future()),
            spawn_write(client.delete_range("a", "z").into_future()),
        ];
        wait_until("the shard rejects the batch", || {
            !mock.write_requests().is_empty()
        })
        .await;
        let (left, right) = mock.split_shard(0, false);
        mock.publish_assignments();
        wait_until("the client has the new assignments", || {
            client.shard_manager().shard_exists(left)
        })
        .await;

        // Routed with the new assignments.
        client
            .put(upper.clone(), "4")
            .await
            .expect("put issued after the split");
        // Routed with the old assignments, to the split shard.
        let request = proto::PutRequest {
            key: lower.clone(),
            value: bytes::Bytes::from_static(b"5"),
            ..Default::default()
        };
        client
            .submit_write(0, request, crate::batcher::WriteOp::Put)
            .await
            .expect("put routed to the split shard");
        for write in pending {
            write.await.expect("write task").expect("pending write");
        }

        assert_eq!(
            applied_on(&mock, left),
            [
                format!("put {lower}=1"),
                "delete_range p..q".to_string(),
                format!("delete {lower}"),
                format!("put {lower}=3"),
                "delete_range a..z".to_string(),
                format!("put {lower}=5"),
            ]
        );
        assert_eq!(
            applied_on(&mock, right),
            [
                format!("put {upper}=2"),
                "delete_range a..z".to_string(),
                format!("put {upper}=4"),
            ]
        );
    }

    /// A batch still in flight on a shard when the client gets the
    /// assignments without it is handed off once the shard rejects it. Until
    /// then, the shards that replaced it hold the writes issued meanwhile.
    #[compio::test]
    async fn split_hands_off_a_batch_in_flight_once_the_shard_rejects_it() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::DelayResponses(Duration::from_millis(300)),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(10)).await;
        let key = key_in("key-", 0, u32::MAX / 2);

        let pending = spawn_write(client.put(key.clone(), "1").into_future());
        wait_until("shard 0 receives the put", || {
            !mock.write_requests().is_empty()
        })
        .await;
        // The shard rejects the put when it gets to it.
        let (left, _) = mock.split_shard(0, false);
        mock.publish_assignments();
        wait_until("the client has the new assignments", || {
            client.shard_manager().shard_exists(left)
        })
        .await;

        client
            .put(key.clone(), "2")
            .await
            .expect("put issued after the split");
        pending
            .await
            .expect("write task")
            .expect("put in flight during the split");
        assert_eq!(
            applied_on(&mock, left),
            [format!("put {key}=1"), format!("put {key}=2")]
        );
    }

    /// A shard split again before it has applied the writes handed off to it
    /// hands them off in turn, in order, ahead of the writes issued once the
    /// client has the newest assignments.
    #[compio::test]
    async fn nested_split_keeps_the_write_order() {
        let mock = MockOxia::start(Behavior::default()).await;
        let client = connect(&mock, Duration::from_secs(10)).await;
        // The halves of the split of shard 0, and the quarters of the split of
        // its lower half.
        let middle = u32::MAX / 2;
        let first = key_in("first-", 0, middle / 2);
        let second = key_in("second-", middle / 2 + 1, middle);
        let upper = key_in("upper-", middle + 1, u32::MAX);

        mock.freeze_shard(0);
        let pending = compio::runtime::spawn({
            let client = client.clone();
            let (first, second, upper) = (first.clone(), second.clone(), upper.clone());
            async move {
                tokio::join!(
                    biased;
                    client.put(first.clone(), "1"),
                    client.delete(first.clone()),
                    client.put(second, "2"),
                    client.put(upper, "3"),
                    client.put(first, "4"),
                )
            }
        });
        wait_until("shard 0 rejects the batch", || {
            !mock.write_requests().is_empty()
        })
        .await;
        let (left, right) = mock.split_shard(0, false);
        // The shard that gets the lower half is split in turn.
        mock.freeze_shard(left);
        mock.publish_assignments();
        wait_until("the lower half rejects the writes handed off to it", || {
            mock.write_requests()
                .iter()
                .any(|request| request.shard == Some(left))
        })
        .await;
        let (lower_left, lower_right) = mock.split_shard(left, false);
        mock.publish_assignments();
        wait_until("the client has the newest assignments", || {
            client.shard_manager().shard_exists(lower_left)
        })
        .await;

        client
            .put(first.clone(), "5")
            .await
            .expect("put issued after the splits");
        let (put_1, delete, put_2, put_3, put_4) = pending.await.expect("pending writes");
        put_1.expect("put 1");
        delete.expect("delete");
        put_2.expect("put 2");
        put_3.expect("put 3");
        put_4.expect("put 4");

        assert_eq!(
            applied_on(&mock, lower_left),
            [
                format!("put {first}=1"),
                format!("delete {first}"),
                format!("put {first}=4"),
                format!("put {first}=5"),
            ]
        );
        assert_eq!(applied_on(&mock, lower_right), [format!("put {second}=2")]);
        assert_eq!(applied_on(&mock, right), [format!("put {upper}=3")]);
    }
}
