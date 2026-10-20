//! Multi-node cluster integration tests.
//!
//! `oxia standalone` puts every shard on one server, so these tests run a
//! real cluster in one container — an `oxia coordinator` plus multiple
//! `oxia server` (dataserver) processes on distinct ports — with shards
//! spread across the nodes. That exercises what standalone cannot: routing to
//! distinct leaders, cross-shard queries merged across servers, and — via the
//! coordinator's admin API — a **live shard split** while a client is
//! running.

use futures::{StreamExt, TryStreamExt};
use oxia::{ComparisonType, Notification, Notifications, OxiaClient, OxiaClientBuilder, OxiaError};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use testcontainers::core::ports::ContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const DEFAULT_OXIA_IMAGE: &str = "oxia/oxia";
const DEFAULT_OXIA_TAG: &str = "main";

struct Cluster {
    container: ContainerAsync<GenericImage>,
    /// The first server's public address — the client's bootstrap address.
    address: String,
}

/// Starts a one-container cluster: `servers` dataserver processes (public
/// ports 6648, 6658, … — each mapped to a pinned host port and advertised as
/// such) plus a coordinator with `initial_shards` shards spread across them.
async fn start_cluster(servers: usize, initial_shards: u32) -> Cluster {
    let image = std::env::var("OXIA_IMAGE").unwrap_or_else(|_| DEFAULT_OXIA_IMAGE.to_string());
    let tag = std::env::var("OXIA_TAG").unwrap_or_else(|_| DEFAULT_OXIA_TAG.to_string());

    // The advertised public addresses must be reachable from the host, so pin
    // one host port per server up front and bake them into the cluster config.
    let host_ports: Vec<u16> = (0..servers)
        .map(|_| {
            std::net::TcpListener::bind(("127.0.0.1", 0))
                .expect("bind probe port")
                .local_addr()
                .expect("probe port addr")
                .port()
        })
        .collect();

    let mut server_entries = String::new();
    let mut server_cmds = String::new();
    for (i, host_port) in host_ports.iter().enumerate() {
        let public = 6648 + i * 10;
        let internal = 6649 + i * 10;
        let metrics = 8080 + i * 10;
        server_entries.push_str(&format!(
            "  - {{ public: \"localhost:{host_port}\", internal: \"localhost:{internal}\" }}\n"
        ));
        server_cmds.push_str(&format!(
            "oxia server --public-addr 0.0.0.0:{public} --internal-addr 0.0.0.0:{internal} \
             --metrics-addr 0.0.0.0:{metrics} --wal-dir /tmp/wal-{i} --data-dir /tmp/db-{i} & "
        ));
    }
    let cluster_yaml = format!(
        "namespaces:\n  \
           - name: default\n    \
             initialShardCount: {initial_shards}\n    \
             replicationFactor: 1\n\
         servers:\n{server_entries}"
    );
    let script = format!(
        "{server_cmds}\
         oxia coordinator --conf /oxia/conf/cluster.yaml --internal-addr 0.0.0.0:7649 \
         --public-addr 0.0.0.0:6651 --metrics-addr 0.0.0.0:7080 & \
         wait"
    );

    let mut request = GenericImage::new(image, tag)
        .with_wait_for(WaitFor::message_on_stdout("Started Grpc server"))
        .with_copy_to("/oxia/conf/cluster.yaml", cluster_yaml.into_bytes())
        .with_cmd(vec!["bash".to_string(), "-c".to_string(), script]);
    for (i, host_port) in host_ports.iter().enumerate() {
        let public = 6648 + i * 10;
        request = request.with_mapped_port(*host_port, ContainerPort::Tcp(public as u16));
    }
    let container = request.start().await.expect("Failed to start Oxia cluster");

    // Wait for every server, not just the bootstrap one. A server rejects
    // all client requests, writes to the shards it leads included, with
    // "server not initialized yet" until the coordinator sends it the shard
    // assignments, and each server gets them at its own time: up to ~10 s
    // apart before oxia-db/oxia#1326. A server serves the assignments only
    // once it has them, so a client builds through it only once it is ready.
    for host_port in &host_ports {
        let probe = connect_with_retry(&format!("localhost:{host_port}")).await;
        probe.close().await.expect("close probe client");
    }

    Cluster {
        container,
        address: format!("localhost:{}", host_ports[0]),
    }
}

impl Cluster {
    /// Splits a shard through the coordinator's admin API, from inside the
    /// container. The CLI returns once the split has started; use
    /// [`Cluster::wait_until_removed`] to wait for it to complete.
    async fn split_shard(&self, shard: i64) -> String {
        let mut result = self
            .container
            .exec(testcontainers::core::ExecCommand::new([
                "oxia",
                "admin",
                "shard",
                "split",
                "--namespace",
                "default",
                "--shard",
                &shard.to_string(),
            ]))
            .await
            .expect("exec shard split");
        let stdout = result.stdout_to_vec().await.expect("split stdout");
        String::from_utf8_lossy(&stdout).to_string()
    }

    /// Waits until the coordinator no longer lists `shard` in the namespace
    /// status: a split parent is deleted once its children have taken over.
    async fn wait_until_removed(&self, shard: i64) {
        let entry = format!("\"{shard}\": {{");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let mut result = self
                .container
                .exec(testcontainers::core::ExecCommand::new([
                    "oxia",
                    "admin",
                    "namespace",
                    "get",
                    "default",
                    "--output",
                    "json",
                ]))
                .await
                .expect("exec namespace get");
            let stdout = result.stdout_to_vec().await.expect("namespace stdout");
            let status = String::from_utf8_lossy(&stdout);
            assert!(status.contains("\"shards\""), "unexpected status: {status}");
            if !status.contains(&entry) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "shard {shard} is still listed: {status}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Builds a client, retrying until the server at `address` serves the shard
/// assignments.
async fn connect_with_retry(address: &str) -> OxiaClient {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let result = OxiaClientBuilder::new()
            .service_address(address.to_string())
            .request_timeout(Duration::from_secs(10))
            .build()
            .await;
        match result {
            Ok(client) => return client,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "client could not connect to the cluster before the deadline: {e}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Routing and cross-shard queries against shards on different servers: every
/// key round-trips, inequality gets reduce across shards on distinct nodes,
/// and list / streaming range-scan come back in the global key order.
#[tokio::test]
async fn test_multi_node_routing_and_global_order() {
    let cluster = start_cluster(2, 4).await;
    let client = connect_with_retry(&cluster.address).await;

    // Enough keys to land on every shard (and therefore on both servers).
    let keys: Vec<String> = (0..60).map(|i| format!("multi/k{i:03}")).collect();
    for key in &keys {
        client
            .put(key.clone(), format!("value-{key}"))
            .await
            .unwrap();
    }
    for key in &keys {
        let got = client.get(key.clone()).await.unwrap();
        assert_eq!(
            got.value.as_deref(),
            Some(format!("value-{key}").as_bytes())
        );
    }

    // Cross-shard inequality gets: broadcast to every shard (on both nodes)
    // and reduced to the single global winner.
    let floor = client
        .get("multi/k010zzz")
        .comparison(ComparisonType::Floor)
        .await
        .unwrap();
    assert_eq!(floor.key, "multi/k010");
    let ceiling = client
        .get("multi/k010zzz")
        .comparison(ComparisonType::Ceiling)
        .await
        .unwrap();
    assert_eq!(ceiling.key, "multi/k011");
    let lower = client
        .get("multi/k000")
        .comparison(ComparisonType::Lower)
        .await;
    assert!(lower.is_err(), "nothing lies below the first key");
    let higher = client
        .get("multi/k059")
        .comparison(ComparisonType::Higher)
        .await;
    assert!(higher.is_err(), "nothing lies above the last key");

    // Global merge order: both the collected list and the streaming scan must
    // interleave the per-shard (per-server) streams into one sorted sequence.
    let listed = client.list("multi/", "multi/~").await.unwrap();
    assert_eq!(listed, keys);
    let mut scan = client
        .range_scan("multi/", "multi/~")
        .stream()
        .await
        .unwrap();
    let mut scanned = Vec::new();
    while let Some(record) = scan.next().await {
        scanned.push(record.unwrap().key);
    }
    assert_eq!(scanned, keys);

    // A range delete spanning every shard.
    client.delete_range("multi/", "multi/~").await.unwrap();
    let listed = client.list("multi/", "multi/~").await.unwrap();
    assert!(listed.is_empty());

    client.close().await.unwrap();
}

/// A live shard split under a running client: the parent shard is fenced and
/// deleted mid-session while a writer keeps writing; the client re-routes to
/// the child shards transparently, no operation fails, and data written
/// before the split stays readable through the children.
#[tokio::test]
async fn test_shard_split_reroutes_live_client() {
    let cluster = start_cluster(2, 1).await;
    let client = connect_with_retry(&cluster.address).await;

    for i in 0..20 {
        client
            .put(format!("split/before-{i:02}"), format!("v{i}"))
            .await
            .unwrap();
    }

    // Writer running across the split; every write must succeed.
    let writer = tokio::spawn({
        let client = client.clone();
        async move {
            let mut written = 0u32;
            for i in 0.. {
                client
                    .put(format!("split/during-{i:04}"), "x")
                    .await
                    .expect("write during split");
                written += 1;
                if written >= 200 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            written
        }
    });

    // Split the only shard while the writer runs. The children may even land
    // on different servers, forcing new connections.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let split_output = cluster.split_shard(0).await;
    assert!(
        split_output.contains("\"Error\":null"),
        "split must succeed, got: {split_output}"
    );

    let written = writer.await.expect("writer task");
    assert_eq!(written, 200, "no write may fail across the split");

    // Data from before the split is served by the child shards.
    for i in 0..20 {
        let got = client.get(format!("split/before-{i:02}")).await.unwrap();
        assert_eq!(got.value.as_deref(), Some(format!("v{i}").as_bytes()));
    }
    // And new writes land normally after the split.
    client.put("split/after", "done").await.unwrap();
    let got = client.get("split/after").await.unwrap();
    assert_eq!(got.value.as_deref(), Some(b"done".as_ref()));

    client.close().await.unwrap();
}

/// A key starting with `prefix` that a two-shard namespace places on its
/// first shard (`upper == false`) or on its second: the coordinator gives
/// each half of the hash space to one shard, and a key is routed by the low
/// 32 bits of its XXH3 hash.
fn key_in_half(prefix: &str, upper: bool) -> String {
    (0u32..)
        .map(|n| format!("{prefix}{n}"))
        .find(|key| (xxhash_rust::xxh3::xxh3_64(key.as_bytes()) as u32 >= 1 << 31) == upper)
        .expect("keys land in both halves")
}

/// One round of `test_delete_range_across_live_split`: a range to delete,
/// and the keys in it.
struct Round {
    start: String,
    end: String,
    /// Written before the round: one on shard 0, one on shard 1.
    before: [String; 2],
    /// Written on shard 1 during the round, once its copy of the delete range
    /// has been applied there.
    late: String,
}

impl Round {
    fn new(i: usize) -> Round {
        let start = format!("dr/{i:05}/");
        Round {
            end: format!("{start}~"),
            before: [
                key_in_half(&format!("{start}before-"), false),
                key_in_half(&format!("{start}before-"), true),
            ],
            late: key_in_half(&format!("{start}late-"), true),
            start,
        }
    }
}

async fn is_deleted(client: &OxiaClient, key: &str) -> bool {
    match client.get(key).await {
        Ok(_) => false,
        Err(OxiaError::KeyNotFound) => true,
        Err(e) => panic!("get {key}: {e}"),
    }
}

/// A delete range without a partition key, across a live split of one of
/// two shards. The split shard's copy goes to its children only: the other
/// shard applies the delete range once, so a key written there after that,
/// while the delete range is still pending on the splitting shard, is not
/// deleted by a second application. The keys written before the delete
/// range are gone from every shard, the children included.
#[tokio::test]
async fn test_delete_range_across_live_split() {
    const ROUNDS: usize = 1000;
    let cluster = start_cluster(2, 2).await;
    let client = connect_with_retry(&cluster.address).await;

    // Write every round's first keys up front: a round must be able to start
    // while shard 0 takes no writes, as the split completes.
    let rounds: Arc<Vec<Round>> = Arc::new((0..ROUNDS).map(Round::new).collect());
    futures::stream::iter(rounds.iter().flat_map(|round| &round.before))
        .map(|key| client.put(key.clone(), "before").into_future())
        .buffer_unordered(64)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    // Writes to shard 0 without pause, so that a write reaches the parent
    // while it rejects writes as the split completes. The write batcher then
    // holds the shard's writes back for its retry backoff, the copy of the
    // delete range in progress included, until they fail with `ShardMoved`
    // once the children have taken over.
    let writer = tokio::spawn({
        let client = client.clone();
        let stop = stop.clone();
        async move {
            let mut n = 0;
            while !stop.load(Ordering::SeqCst) {
                let key = key_in_half(&format!("load/{n}-"), false);
                client.put(key, "x").await.expect("write during the split");
                n += 1;
            }
        }
    });
    // Runs rounds until stopped, and returns how many it ran. Apart from its
    // delete range, a round only touches shard 1, so rounds keep starting
    // while shard 0 rejects writes.
    let prober = tokio::spawn({
        let client = client.clone();
        let rounds = rounds.clone();
        let stop = stop.clone();
        async move {
            for (i, round) in rounds.iter().enumerate() {
                if stop.load(Ordering::SeqCst) {
                    return i;
                }
                let mut delete = client
                    .delete_range(round.start.clone(), round.end.clone())
                    .into_future();
                // Polled once, the delete range has a copy queued for every
                // shard.
                assert!(futures::poll!(&mut delete).is_pending());
                // Once shard 1 has applied its copy (not merely queued it: a
                // server without FEATURE_ORDERED_WRITES applies a batch's puts
                // before its delete ranges), write a key there in the range,
                // while the copy for shard 0 may still be held back.
                let deadline = Instant::now() + Duration::from_secs(10);
                while !is_deleted(&client, &round.before[1]).await {
                    assert!(
                        Instant::now() < deadline,
                        "round {i}: shard 1 did not apply the delete range"
                    );
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                client
                    .put(round.late.clone(), "late")
                    .await
                    .expect("late put");
                delete.await.expect("delete range across the split");
                assert!(
                    !is_deleted(&client, &round.late).await,
                    "round {i}: shard 1 applied the delete range a second time"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("the split did not complete within {ROUNDS} rounds");
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let split_output = cluster.split_shard(0).await;
    assert!(
        split_output.contains("\"Error\":null"),
        "split must succeed, got: {split_output}"
    );
    cluster.wait_until_removed(0).await;
    // A few more rounds, against the children.
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, Ordering::SeqCst);
    writer.await.expect("writer");
    let ran = prober.await.expect("prober");

    // Every round deleted its range on shard 0, or on the children.
    for (i, round) in rounds[..ran].iter().enumerate() {
        assert!(
            is_deleted(&client, &round.before[0]).await,
            "round {i}: the delete range missed shard 0 or its children"
        );
    }

    client.close().await.unwrap();
}

/// Runs `scan`, a list or a range scan, until `stop` is set, and returns how
/// many times it ran. Each time, it returns `expected`, or fails with
/// `ShardMoved` if it opened a stream on the parent once the parent no
/// longer served: never with another error.
async fn scan_until_stopped<F, Fut>(
    stop: &AtomicBool,
    kind: &str,
    expected: &[String],
    mut scan: F,
) -> usize
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Vec<String>, OxiaError>>,
{
    let mut scans = 0;
    while !stop.load(Ordering::SeqCst) {
        match scan().await {
            Ok(keys) => assert_eq!(keys, expected, "{kind}"),
            Err(OxiaError::ShardMoved) => {}
            Err(e) => panic!("{kind} during the split: {e:?}"),
        }
        scans += 1;
    }
    scans
}

/// Runs `scan` again while it fails with `ShardMoved`, as a caller does to
/// read the new shards.
async fn retry_while_moved<F, Fut>(mut scan: F) -> Result<Vec<String>, OxiaError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Vec<String>, OxiaError>>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match scan().await {
            Err(OxiaError::ShardMoved) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            result => return result,
        }
    }
}

/// Reads across a live split of one of two shards, each kind in a loop of
/// its own: lists and range scans without a partition key, lists with a
/// partition key on the splitting shard, and gets. A get never fails: one
/// that reaches the parent once it no longer serves goes to the child that
/// owns the key once the client has the children's assignments. A list or
/// a range scan that opened a stream on the parent then fails with
/// `ShardMoved` instead, as in the Go client, rather than with another error
/// after the request timeout; run again, it reads the children.
#[tokio::test]
async fn test_reads_across_live_split() {
    let cluster = start_cluster(2, 2).await;
    let client = connect_with_retry(&cluster.address).await;

    let keys: Arc<Vec<String>> = Arc::new((0..40).map(|i| format!("reads/k{i:02}")).collect());
    for key in keys.iter() {
        client.put(key.clone(), key.clone()).await.unwrap();
    }
    // Records with a partition key that shard 0 owns.
    let partition_key = key_in_half("pk-", false);
    let partitioned: Arc<Vec<String>> =
        Arc::new((0..10).map(|i| format!("partitioned/k{i:02}")).collect());
    for key in partitioned.iter() {
        client
            .put(key.clone(), "x")
            .partition_key(partition_key.clone())
            .await
            .unwrap();
    }

    let list = {
        let client = client.clone();
        move || client.list("reads/", "reads/~").into_future()
    };
    let range_scan = {
        let client = client.clone();
        move || {
            let scan = client.range_scan("reads/", "reads/~");
            async move {
                let records = scan.await?;
                Ok::<_, OxiaError>(records.into_iter().map(|record| record.key).collect())
            }
        }
    };
    let partition_list = {
        let client = client.clone();
        move || {
            client
                .list("partitioned/", "partitioned/~")
                .partition_key(partition_key.clone())
                .into_future()
        }
    };

    let stop = Arc::new(AtomicBool::new(false));
    // Each reader runs until stopped, and returns how many reads it made.
    let lists = tokio::spawn({
        let (stop, keys, list) = (stop.clone(), keys.clone(), list.clone());
        async move { scan_until_stopped(&stop, "list", &keys, list).await }
    });
    let range_scans = tokio::spawn({
        let (stop, keys, range_scan) = (stop.clone(), keys.clone(), range_scan.clone());
        async move { scan_until_stopped(&stop, "range scan", &keys, range_scan).await }
    });
    let partition_lists = tokio::spawn({
        let (stop, partitioned, partition_list) =
            (stop.clone(), partitioned.clone(), partition_list.clone());
        async move {
            scan_until_stopped(
                &stop,
                "list with a partition key",
                &partitioned,
                partition_list,
            )
            .await
        }
    });
    let gets = tokio::spawn({
        let (client, keys, stop) = (client.clone(), keys.clone(), stop.clone());
        async move {
            let mut reads = 0;
            while !stop.load(Ordering::SeqCst) {
                let key = &keys[reads % keys.len()];
                let got = client.get(key.clone()).await.expect("get during the split");
                assert_eq!(got.value.as_deref(), Some(key.as_bytes()));
                reads += 1;
            }
            reads
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let split_output = cluster.split_shard(0).await;
    assert!(
        split_output.contains("\"Error\":null"),
        "split must succeed, got: {split_output}"
    );
    cluster.wait_until_removed(0).await;
    // A few more reads, from the children.
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, Ordering::SeqCst);
    for (kind, reader) in [
        ("lists", lists),
        ("range scans", range_scans),
        ("lists with a partition key", partition_lists),
        ("gets", gets),
    ] {
        let reads = reader.await.expect(kind);
        assert!(reads > 0, "no {kind} ran");
    }

    // Once the split has completed, the lists and range scans read the
    // children.
    assert_eq!(retry_while_moved(list).await.unwrap(), *keys);
    assert_eq!(retry_while_moved(range_scan).await.unwrap(), *keys);
    assert_eq!(
        retry_while_moved(partition_list).await.unwrap(),
        *partitioned
    );

    client.close().await.unwrap();
}

/// The keys of the next `count` notifications, all creations, sorted, once
/// no other arrives for a while: a duplicate would come after.
async fn created_keys(notifications: &mut Notifications, count: usize) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut keys = Vec::with_capacity(count);
    while keys.len() < count {
        let notification = tokio::time::timeout_at(deadline, notifications.recv())
            .await
            .unwrap_or_else(|_| panic!("{} of the {count} notifications arrived", keys.len()))
            .expect("the subscription is open");
        match notification {
            Notification::KeyCreated { key, .. } => keys.push(key),
            other => panic!("unexpected notification: {other:?}"),
        }
    }
    let extra = tokio::time::timeout(Duration::from_millis(500), notifications.recv()).await;
    assert!(extra.is_err(), "unexpected notification: {extra:?}");
    keys.sort();
    keys
}

/// Notifications across a live split of the only shard, by two
/// subscriptions opened before it: one with a buffer that never fills, which
/// reads the stream as the notifications come, and one that lags behind, as
/// nothing reads it until the split has completed and its buffer holds a
/// single notification. Each follows the split to the children, after the
/// last batch it received from the parent: both get the creation of every
/// key written before, during and after the split, once.
#[tokio::test]
async fn test_notifications_across_live_split() {
    let cluster = start_cluster(2, 1).await;
    let client = connect_with_retry(&cluster.address).await;
    let mut prompt = client.notifications().buffer_size(100_000).await.unwrap();
    let mut lagging = client.notifications().buffer_size(1).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn({
        let (client, stop) = (client.clone(), stop.clone());
        async move {
            let mut written = Vec::new();
            while !stop.load(Ordering::SeqCst) {
                let key = format!("notified/during-{:05}", written.len());
                client
                    .put(key.clone(), "x")
                    .await
                    .expect("write during the split");
                written.push(key);
            }
            written
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let split_output = cluster.split_shard(0).await;
    assert!(
        split_output.contains("\"Error\":null"),
        "split must succeed, got: {split_output}"
    );
    cluster.wait_until_removed(0).await;
    stop.store(true, Ordering::SeqCst);
    let mut written = writer.await.expect("writer");
    // Once the split has completed, the children take every write.
    for i in 0..20 {
        let key = format!("notified/after-{i:02}");
        client.put(key.clone(), "x").await.unwrap();
        written.push(key);
    }
    written.sort();

    assert_eq!(created_keys(&mut prompt, written.len()).await, written);
    assert_eq!(created_keys(&mut lagging, written.len()).await, written);

    client.close().await.unwrap();
}

/// Puts pipelined to the same keys across a live split of the only shard are
/// applied in the order they were issued: the modification count of each put
/// is higher than those of the puts to its key issued before it. When the
/// split completes, some of the puts are pending on the parent, and are
/// handed off to the children, while the puts issued once the client has the
/// new assignments go to the children directly.
///
/// Runs on a multi-thread runtime, as a pipelined writer would: the parent's
/// batcher hands its writes off while the writes issued meanwhile are queued
/// on other threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_split_keeps_the_write_order() {
    const KEYS: usize = 16;
    // Enough puts in flight for the writer to keep issuing them while the
    // ones pending on the parent are handed off.
    const IN_FLIGHT: usize = 1024;
    let cluster = start_cluster(2, 1).await;
    let client = connect_with_retry(&cluster.address).await;

    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn({
        let client = client.clone();
        let stop = stop.clone();
        async move {
            // `buffered` polls the puts first in the order they are issued:
            // the order they join their shard's batch in.
            let mut puts = futures::stream::iter(0..)
                .take_while(|_| futures::future::ready(!stop.load(Ordering::SeqCst)))
                .map(|seq: usize| {
                    let key = format!("order/key-{:02}", seq % KEYS);
                    let put = client.put(key.clone(), seq.to_string());
                    async move { (seq, key, put.await) }
                })
                .buffered(IN_FLIGHT);
            // The puts that succeeded, those that failed, and those applied
            // before a put issued earlier to their key.
            let mut succeeded = 0;
            let mut failed = Vec::new();
            let mut out_of_order = Vec::new();
            let mut last_applied: HashMap<String, (usize, i64)> = HashMap::new();
            while let Some((seq, key, result)) = puts.next().await {
                let put = match result {
                    Ok(put) => put,
                    Err(e) => {
                        failed.push(format!("put #{seq} to {key}: {e}"));
                        continue;
                    }
                };
                succeeded += 1;
                let count = put.version.modifications_count;
                if let Some(&(last_seq, last_count)) = last_applied.get(&key) {
                    if count <= last_count {
                        out_of_order.push(format!(
                            "{key}: put #{seq} applied as modification {count}, \
                             before put #{last_seq} (modification {last_count})"
                        ));
                    }
                }
                last_applied.insert(key, (seq, count));
            }
            (succeeded, failed, out_of_order)
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let split_output = cluster.split_shard(0).await;
    assert!(
        split_output.contains("\"Error\":null"),
        "split must succeed, got: {split_output}"
    );
    cluster.wait_until_removed(0).await;
    // Keep writing to the children for a while.
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.store(true, Ordering::SeqCst);

    let (succeeded, failed, out_of_order) = writer.await.expect("writer task");
    println!(
        "{succeeded} puts succeeded, {} failed, {} out of order",
        failed.len(),
        out_of_order.len()
    );
    assert!(
        out_of_order.is_empty(),
        "puts applied out of the order they were issued: {:#?}",
        &out_of_order[..out_of_order.len().min(20)]
    );
    assert!(
        failed.is_empty(),
        "puts failed across the split: {:#?}",
        &failed[..failed.len().min(20)]
    );
    assert!(succeeded > 0, "no put succeeded");

    client.close().await.unwrap();
}
