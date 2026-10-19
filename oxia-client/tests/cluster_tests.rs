//! Multi-node cluster integration tests.
//!
//! `oxia standalone` puts every shard on one server, so these tests run a
//! real cluster in one container — an `oxia coordinator` plus multiple
//! `oxia server` (dataserver) processes on distinct ports — with shards
//! spread across the nodes. That exercises what standalone cannot: routing to
//! distinct leaders, cross-shard queries merged across servers, and — via the
//! coordinator's admin API — a **live shard split** while a client is
//! running.

use oxia::{ComparisonType, OxiaClient, OxiaClientBuilder};
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
    /// container. The CLI is synchronous: when it returns, the split has
    /// completed (children elected, parent fenced and deleted).
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
/// and list / streaming range-scan come back in global slash-aware order.
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
    use futures::StreamExt;
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
