//! A programmable in-process Oxia server for hermetic client tests.
//!
//! Serves the real gRPC service on an ephemeral local port (no Docker, no
//! containers) and advertises itself as the leader of every shard (a single
//! one by default), so an unmodified [`OxiaClient`](crate::OxiaClient)
//! connects to it exactly as it would to a real cluster. Tests script server
//! behavior — delayed, missing, erroneous, or absent responses; torn streams;
//! the keys each shard returns — and assert on what the server observed
//! (write requests, batch sizes, connection counts) and on what the client
//! surfaced.

use crate::proto;
use crate::proto::oxia_client_server::{OxiaClient as OxiaService, OxiaClientServer};
use futures::StreamExt;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::{Request, Response, Status, Streaming};

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
    /// The shards whose leader includes the end of the range of a list or a
    /// range scan that asks for it, like a server with
    /// FEATURE_RANGE_END_INCLUSIVE: their responses say so, and they send one
    /// without keys when they have none. The other shards ignore it, like an
    /// older server.
    pub(crate) end_inclusive_shards: Vec<i64>,
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
            end_inclusive_shards: Vec::new(),
        }
    }
}

struct State {
    behavior: Mutex<Behavior>,
    /// Every received `WriteRequest`, in order.
    write_requests: Mutex<Vec<proto::WriteRequest>>,
    /// Operations per received `WriteRequest` (puts + deletes + delete-ranges).
    write_batch_sizes: Mutex<Vec<usize>>,
    write_connections: AtomicU32,
    /// Keeps assignment streams open for the lifetime of the mock.
    assignment_senders: Mutex<Vec<mpsc::UnboundedSender<Result<proto::ShardAssignments, Status>>>>,
}

/// A running mock server. Dropping it stops the server.
pub(crate) struct MockOxia {
    pub(crate) address: String,
    state: Arc<State>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for MockOxia {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl MockOxia {
    pub(crate) async fn start(behavior: Behavior) -> MockOxia {
        let state = Arc::new(State {
            behavior: Mutex::new(behavior),
            write_requests: Mutex::new(Vec::new()),
            write_batch_sizes: Mutex::new(Vec::new()),
            write_connections: AtomicU32::new(0),
            assignment_senders: Mutex::new(Vec::new()),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock listener");
        let port = listener.local_addr().expect("mock local addr").port();
        let address = format!("127.0.0.1:{port}");

        let service = Service {
            state: state.clone(),
            advertised: address.clone(),
        };
        let incoming = futures::stream::unfold(listener, |listener| async move {
            let accepted = listener.accept().await.map(|(stream, _)| stream);
            Some((accepted, listener))
        });
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(OxiaClientServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .expect("mock server failed");
        });

        MockOxia {
            address,
            state,
            server,
        }
    }

    pub(crate) fn set_read_mode(&self, read: ReadMode) {
        self.state.behavior.lock().expect("behavior lock").read = read;
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

/// The record of `key`, as a get or a range scan finds it.
fn record(key: &str) -> proto::GetResponse {
    proto::GetResponse {
        status: proto::Status::Ok as i32,
        version: Some(ok_version()),
        value: Some(bytes::Bytes::from(key.to_string())),
        key: Some(key.to_string()),
        secondary_index_key: None,
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
}

type Rpc<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl OxiaService for Service {
    type GetShardAssignmentsStream = BoxStream<'static, Result<proto::ShardAssignments, Status>>;

    async fn get_shard_assignments(
        &self,
        request: Request<proto::ShardAssignmentsRequest>,
    ) -> Rpc<Self::GetShardAssignmentsStream> {
        let (scripted_error, shards, key_sorting) = {
            let behavior = self.state.behavior.lock().expect("behavior lock");
            (
                behavior.assignments_error,
                behavior.shards,
                behavior.key_sorting,
            )
        };
        if let Some(code) = scripted_error {
            return Err(Status::new(code, "scripted assignments failure"));
        }
        let namespace = request.into_inner().namespace;
        let assignments = proto::ShardAssignments {
            namespaces: [(
                namespace,
                proto::NamespaceShardsAssignment {
                    assignments: (0..shards)
                        .map(|shard| {
                            let (min_hash_inclusive, max_hash_inclusive) =
                                hash_range(shard, shards);
                            proto::ShardAssignment {
                                shard: shard.into(),
                                leader: self.advertised.clone(),
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
        };
        let (tx, rx) = mpsc::unbounded();
        tx.unbounded_send(Ok(assignments))
            .expect("first assignment");
        // Park the sender so the stream stays open, like a real server.
        self.state
            .assignment_senders
            .lock()
            .expect("senders lock")
            .push(tx);
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
                        .map(|_| match found {
                            Some(key) => record(key),
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
        let (responses, end_inclusive) = self.scan(request.shard, request.end_inclusive);
        let responses: Vec<_> = responses
            .into_iter()
            .map(|keys| {
                Ok(proto::RangeScanResponse {
                    records: keys.into_iter().map(record).collect(),
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
        _: Request<proto::NotificationsRequest>,
    ) -> Rpc<Self::GetNotificationsStream> {
        Err(Status::unimplemented("not scripted"))
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
    #[tokio::test]
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

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.put("count/first", "x").await }
        });
        // Let the first put dispatch alone, then queue six more behind it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut rest = tokio::task::JoinSet::new();
        for i in 0..6 {
            let client = client.clone();
            rest.spawn(async move { client.put(format!("count/{i}"), "x").await });
        }
        first.await.expect("join").expect("first put");
        while let Some(result) = rest.join_next().await {
            result.expect("join").expect("put");
        }

        assert_eq!(mock.write_batch_sizes(), vec![1, 3, 3]);
    }

    /// With the window saturated, accumulating batches seal when the next
    /// operation would exceed `batch_max_size`.
    #[tokio::test]
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

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.put("bytes/first", vec![0u8; 100]).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut rest = tokio::task::JoinSet::new();
        for i in 0..6 {
            let client = client.clone();
            rest.spawn(async move { client.put(format!("bytes/{i}"), vec![0u8; 100]).await });
        }
        first.await.expect("join").expect("first put");
        while let Some(result) = rest.join_next().await {
            result.expect("join").expect("put");
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

    /// No more write batches than `max_write_batches_in_flight` are ever
    /// outstanding on a shard.
    #[tokio::test]
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
        let mut puts = tokio::task::JoinSet::new();
        for i in 0..6 {
            let client = client.clone();
            puts.spawn(async move { client.put(format!("window/{i}"), "x").await });
            if i < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        // Give the batcher ample time to dispatch everything it is willing to.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            mock.write_batch_sizes().len(),
            2,
            "only the window's worth of batches may be dispatched"
        );

        // The server never answers, so every operation times out.
        while let Some(result) = puts.join_next().await {
            assert!(matches!(result.expect("join"), Err(OxiaError::Timeout)));
        }
    }

    /// A server that acknowledges a batch with missing per-operation
    /// responses produces a decode error, not a hang.
    #[tokio::test]
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
    #[tokio::test]
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
    #[tokio::test]
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
    #[tokio::test]
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
    #[tokio::test]
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
    /// (e.g. a secondary index name containing `/`) maps to `InvalidArgument`,
    /// not to a decode error.
    #[tokio::test]
    async fn invalid_argument_status_maps_to_invalid_argument() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::PutStatus(proto::Status::InvalidArgument as i32),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        let result = client
            .put("invalid/key", "x")
            .secondary_index("by/name", "x")
            .await;
        assert!(
            matches!(result, Err(OxiaError::InvalidArgument(_))),
            "got {result:?}"
        );
    }

    /// Per-operation proto statuses in read responses map to the typed
    /// errors, and a healthy read produces the served value.
    #[tokio::test]
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
    #[tokio::test]
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
    #[tokio::test]
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
    #[tokio::test]
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

    /// A floor or ceiling get without a partition key goes to every shard,
    /// and the client keeps the largest or smallest answer in the key sorting
    /// that the assignments report. A server that reports none keeps the
    /// legacy slash-aware order. The natural and hierarchical cases are the
    /// examples of oxia-db/oxia#1388, where the legacy order picks the other
    /// answer.
    #[tokio::test]
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
    #[tokio::test]
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

    /// A list or a range scan with `end_inclusive` fails with
    /// `EndInclusiveUnsupported` unless the leader of every shard includes
    /// the end of the range, which it says in all its responses, sending one
    /// at least: an older server ignores the flag and leaves the end out.
    /// The cases are those of the Go reference (`endInclusiveTests`).
    #[tokio::test]
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
}
