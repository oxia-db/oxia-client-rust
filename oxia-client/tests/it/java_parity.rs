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

//! Faithful ports of the Oxia **Java** client's integration tests
//! (`io.oxia.client.it.OxiaClientIT` / `NotificationIt`). Each test replays the
//! same keys, values, operation order, and assertions as its Java counterpart,
//! so any divergence in client-observable semantics between the two SDKs shows
//! up here. The server runs multi-shard so the list/range-scan/notification
//! paths exercise the same cross-shard behavior as the Java suite. (The Java
//! tests use 10 shards; we use fewer — the count does not change any API-call or
//! key sequence under test, only container weight and the number of per-shard
//! range-delete notifications a broadcast `delete_range` produces.)
//!
//! Idiomatic representation differences are adapted rather than treated as
//! divergences (same meaning, different shape):
//! - Java's `null` "not found" get  -> Rust `Err(OxiaError::KeyNotFound)`
//! - Java's excluded value (empty `byte[]`) -> Rust `value == None`
//! - Java's `delete` returning `boolean` -> Rust `Result<(), KeyNotFound>`
//!
//! Scenarios dominated by features this SDK does not expose are intentionally
//! not ported: `OxiaClientIT.test` (asserts the `oxia.client.ops` metric count
//! and `session_id`/`client_identifier` on `GetResult`) and
//! `testRangeScanCloseStopsIteration` (Java sync-iterator mechanics).

use oxia::{ComparisonType, Notification, OxiaClient, OxiaClientBuilder, OxiaError};
use std::time::Duration;
use testcontainers::core::ports::ContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::common;

const OXIA_PORT: u16 = 6648;
const SHARDS: u32 = 4;

/// Starts a multi-shard standalone Oxia server (the Java IT uses
/// `new OxiaContainer(...).withShards(10)`; see the module note on shard count).
async fn start_oxia() -> (Container<GenericImage>, String) {
    let container = common::oxia_image()
        .with_exposed_port(ContainerPort::Tcp(OXIA_PORT))
        .with_wait_for(WaitFor::message_on_stdout("Started Grpc server"))
        .with_cmd(vec![
            "oxia".to_string(),
            "standalone".to_string(),
            "--shards".to_string(),
            SHARDS.to_string(),
        ])
        .start()
        .expect("Failed to start Oxia container");

    let host_port = container
        .get_host_port_ipv4(OXIA_PORT)
        .expect("Failed to get host port");
    (container, format!("http://127.0.0.1:{}", host_port))
}

async fn new_client(address: &str) -> OxiaClient {
    OxiaClientBuilder::new()
        .service_address(address.to_string())
        .request_timeout(Duration::from_secs(10))
        .build()
        .await
        .unwrap()
}

/// A get reduced to `(key, value)`, or `None` for a not-found result — the Rust
/// analog of Java's `GetResult` / `null`.
async fn get_kv(client: &OxiaClient, key: &str, cmp: ComparisonType) -> Option<(String, Vec<u8>)> {
    match client.get(key).comparison(cmp).await {
        Ok(r) => Some((r.key, r.value.map(|b| b.to_vec()).unwrap_or_default())),
        Err(OxiaError::KeyNotFound) => None,
        Err(e) => panic!("get({key:?}, {cmp:?}) failed: {e}"),
    }
}

fn kv(key: &str, value: &str) -> Option<(String, Vec<u8>)> {
    Some((key.to_string(), value.as_bytes().to_vec()))
}

/// Port of `OxiaClientIT.testGetFloorCeiling`: an exhaustive comparison matrix
/// over present keys (a, c, d, e, g) and absent ones (b, f).
#[compio::test]
async fn test_get_floor_ceiling() {
    use ComparisonType::{Ceiling, Equal, Floor, Higher, Lower};
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for (k, v) in [("a", "0"), ("c", "2"), ("d", "3"), ("e", "4"), ("g", "6")] {
        client.put(k, v.as_bytes().to_vec()).await.unwrap();
    }

    // a (present)
    assert_eq!(get_kv(&client, "a", Equal).await, kv("a", "0"));
    assert_eq!(get_kv(&client, "a", Floor).await, kv("a", "0"));
    assert_eq!(get_kv(&client, "a", Ceiling).await, kv("a", "0"));
    assert_eq!(get_kv(&client, "a", Lower).await, None);
    assert_eq!(get_kv(&client, "a", Higher).await, kv("c", "2"));

    // b (absent)
    assert_eq!(get_kv(&client, "b", Equal).await, None);
    assert_eq!(get_kv(&client, "b", Floor).await, kv("a", "0"));
    assert_eq!(get_kv(&client, "b", Ceiling).await, kv("c", "2"));
    assert_eq!(get_kv(&client, "b", Lower).await, kv("a", "0"));
    assert_eq!(get_kv(&client, "b", Higher).await, kv("c", "2"));

    // c (present)
    assert_eq!(get_kv(&client, "c", Equal).await, kv("c", "2"));
    assert_eq!(get_kv(&client, "c", Floor).await, kv("c", "2"));
    assert_eq!(get_kv(&client, "c", Ceiling).await, kv("c", "2"));
    assert_eq!(get_kv(&client, "c", Lower).await, kv("a", "0"));
    assert_eq!(get_kv(&client, "c", Higher).await, kv("d", "3"));

    // d (present)
    assert_eq!(get_kv(&client, "d", Equal).await, kv("d", "3"));
    assert_eq!(get_kv(&client, "d", Floor).await, kv("d", "3"));
    assert_eq!(get_kv(&client, "d", Ceiling).await, kv("d", "3"));
    assert_eq!(get_kv(&client, "d", Lower).await, kv("c", "2"));
    assert_eq!(get_kv(&client, "d", Higher).await, kv("e", "4"));

    // e (present)
    assert_eq!(get_kv(&client, "e", Equal).await, kv("e", "4"));
    assert_eq!(get_kv(&client, "e", Floor).await, kv("e", "4"));
    assert_eq!(get_kv(&client, "e", Ceiling).await, kv("e", "4"));
    assert_eq!(get_kv(&client, "e", Lower).await, kv("d", "3"));
    assert_eq!(get_kv(&client, "e", Higher).await, kv("g", "6"));

    // f (absent)
    assert_eq!(get_kv(&client, "f", Equal).await, None);
    assert_eq!(get_kv(&client, "f", Floor).await, kv("e", "4"));
    assert_eq!(get_kv(&client, "f", Ceiling).await, kv("g", "6"));
    assert_eq!(get_kv(&client, "f", Lower).await, kv("e", "4"));
    assert_eq!(get_kv(&client, "f", Higher).await, kv("g", "6"));

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testPartitionKey`: a partition key pins every op to one
/// shard, and the wrong partition key resolves nothing.
#[compio::test]
async fn test_partition_key() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    client
        .put("pk_a", b"0".to_vec())
        .partition_key("x")
        .await
        .unwrap();

    // Without the partition key the record is on a different shard: not found.
    assert!(matches!(
        client.get("pk_a").await,
        Err(OxiaError::KeyNotFound)
    ));
    let got = client.get("pk_a").partition_key("x").await.unwrap();
    assert_eq!(got.key, "pk_a");
    assert_eq!(got.value.as_deref(), Some(b"0".as_ref()));

    for (k, v) in [
        ("pk_a", "0"),
        ("pk_b", "1"),
        ("pk_c", "2"),
        ("pk_d", "3"),
        ("pk_e", "4"),
        ("pk_f", "5"),
        ("pk_g", "6"),
    ] {
        client
            .put(k, v.as_bytes().to_vec())
            .partition_key("x")
            .await
            .unwrap();
    }

    assert_eq!(
        client.list("pk_a", "pk_d").await.unwrap(),
        vec!["pk_a", "pk_b", "pk_c"]
    );
    assert_eq!(
        client
            .list("pk_a", "pk_d")
            .partition_key("x")
            .await
            .unwrap(),
        vec!["pk_a", "pk_b", "pk_c"]
    );
    assert!(
        client
            .list("pk_a", "pk_d")
            .partition_key("wrong-partition-key")
            .await
            .unwrap()
            .is_empty()
    );

    // Java's `delete` returns a boolean; Rust returns Result<(), KeyNotFound>.
    assert!(matches!(
        client
            .delete("pk_g")
            .partition_key("wrong-partition-key")
            .await,
        Err(OxiaError::KeyNotFound)
    ));
    client.delete("pk_g").partition_key("x").await.unwrap();

    let higher = client
        .get("pk_a")
        .comparison(ComparisonType::Higher)
        .await
        .unwrap();
    assert_eq!(higher.key, "pk_b");
    assert_eq!(higher.value.as_deref(), Some(b"1".as_ref()));

    let higher = client
        .get("pk_a")
        .comparison(ComparisonType::Higher)
        .partition_key("x")
        .await
        .unwrap();
    assert_eq!(higher.key, "pk_b");
    assert_eq!(higher.value.as_deref(), Some(b"1".as_ref()));

    // The wrong partition key must not resolve to pk_b.
    let wrong = client
        .get("pk_a")
        .comparison(ComparisonType::Higher)
        .partition_key("another-wrong-partition-key")
        .await;
    assert!(
        matches!(&wrong, Err(OxiaError::KeyNotFound)) || matches!(&wrong, Ok(r) if r.key != "pk_b"),
        "wrong partition key must not resolve to pk_b, got {wrong:?}"
    );

    // A delete-range with the wrong partition key is a no-op.
    client
        .delete_range("pk_c", "pk_e")
        .partition_key("wrong-partition-key")
        .await
        .unwrap();
    assert_eq!(
        client.list("pk_c", "pk_f").await.unwrap(),
        vec!["pk_c", "pk_d", "pk_e"]
    );

    client
        .delete_range("pk_c", "pk_e")
        .partition_key("x")
        .await
        .unwrap();
    assert_eq!(client.list("pk_c", "pk_f").await.unwrap(), vec!["pk_e"]);

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testSequentialKeys`: invalid sequence-key options are
/// rejected client-side, and server-assigned keys are the base plus one
/// zero-padded 20-digit segment per delta.
///
/// The Java test's negative-delta case is not portable — `sequence_key_deltas`
/// takes `u64`, so negatives cannot be expressed. Java throws
/// `IllegalArgumentException` synchronously; this client returns
/// `Err(InvalidArgument)` when the builder is awaited.
#[compio::test]
async fn test_sequential_keys() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Sequential keys require a partition key.
    assert!(matches!(
        client
            .put("sk_a", b"0".to_vec())
            .sequence_key_deltas(vec![1])
            .await,
        Err(OxiaError::InvalidArgument(_))
    ));
    // A zero delta is rejected.
    assert!(matches!(
        client
            .put("sk_a", b"0".to_vec())
            .partition_key("x")
            .sequence_key_deltas(vec![0])
            .await,
        Err(OxiaError::InvalidArgument(_))
    ));
    // Sequential keys cannot be combined with an expected version.
    assert!(matches!(
        client
            .put("sk_a", b"0".to_vec())
            .partition_key("x")
            .sequence_key_deltas(vec![1])
            .expected_version_id(1)
            .await,
        Err(OxiaError::InvalidArgument(_))
    ));

    let r1 = client
        .put("sk_a", b"0".to_vec())
        .partition_key("x")
        .sequence_key_deltas(vec![1])
        .await
        .unwrap();
    assert_eq!(r1.key, format!("sk_a-{:020}", 1));

    let r2 = client
        .put("sk_a", b"1".to_vec())
        .partition_key("x")
        .sequence_key_deltas(vec![3])
        .await
        .unwrap();
    assert_eq!(r2.key, format!("sk_a-{:020}", 4));

    let r3 = client
        .put("sk_a", b"2".to_vec())
        .partition_key("x")
        .sequence_key_deltas(vec![1, 6])
        .await
        .unwrap();
    assert_eq!(r3.key, format!("sk_a-{:020}-{:020}", 5, 6));

    // The base key itself is never stored.
    assert!(matches!(
        client.get("sk_a").partition_key("x").await,
        Err(OxiaError::KeyNotFound)
    ));
    assert_eq!(
        client
            .get(&r1.key)
            .partition_key("x")
            .await
            .unwrap()
            .value
            .as_deref(),
        Some(b"0".as_ref())
    );
    assert_eq!(
        client
            .get(&r2.key)
            .partition_key("x")
            .await
            .unwrap()
            .value
            .as_deref(),
        Some(b"1".as_ref())
    );
    assert_eq!(
        client
            .get(&r3.key)
            .partition_key("x")
            .await
            .unwrap()
            .value
            .as_deref(),
        Some(b"2".as_ref())
    );

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testRangeScan`: a range scan returns the keys in range,
/// and re-scanning yields the same results.
#[compio::test]
async fn test_range_scan() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for (k, v) in [
        ("range-scan-a", "0"),
        ("range-scan-b", "1"),
        ("range-scan-c", "2"),
        ("range-scan-d", "3"),
        ("range-scan-e", "4"),
        ("range-scan-f", "5"),
        ("range-scan-g", "6"),
    ] {
        client.put(k, v.as_bytes().to_vec()).await.unwrap();
    }

    let scan = client
        .range_scan("range-scan-a", "range-scan-d")
        .await
        .unwrap();
    let keys: Vec<&str> = scan.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(keys, vec!["range-scan-a", "range-scan-b", "range-scan-c"]);

    let scan2 = client
        .range_scan("range-scan-a", "range-scan-d")
        .await
        .unwrap();
    let keys2: Vec<&str> = scan2.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(keys2, keys);

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testRangeScanWithPartitionKey`.
#[compio::test]
async fn test_range_scan_with_partition_key() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for (k, v) in [
        ("range-scan-pkey-a", "0"),
        ("range-scan-pkey-b", "1"),
        ("range-scan-pkey-c", "2"),
        ("range-scan-pkey-d", "3"),
        ("range-scan-pkey-e", "4"),
        ("range-scan-pkey-f", "5"),
        ("range-scan-pkey-g", "6"),
    ] {
        client
            .put(k, v.as_bytes().to_vec())
            .partition_key("x")
            .await
            .unwrap();
    }

    let scan = client
        .range_scan("range-scan-pkey-a", "range-scan-pkey-d")
        .partition_key("x")
        .await
        .unwrap();
    let keys: Vec<&str> = scan.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            "range-scan-pkey-a",
            "range-scan-pkey-b",
            "range-scan-pkey-c"
        ]
    );

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testSequenceBatching`: a burst of sequential puts, in
/// submission order, yields contiguous keys `idx-000..01 … idx-000..050`, each
/// carrying the value submitted with it.
#[compio::test]
async fn test_sequence_batching() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Submit all puts, then await them in submission order (join_all preserves
    // the result order), mirroring the Java loop over `resultList`.
    let futures: Vec<_> = (1..=50)
        .map(|i| {
            client
                .put("idx", format!("message-{i}").into_bytes())
                .partition_key("ids")
                .sequence_key_deltas(vec![1])
                .into_future()
        })
        .collect();
    let results = futures::future::join_all(futures).await;

    for (i, result) in results.into_iter().enumerate() {
        let put = result.unwrap();
        assert_eq!(put.key, format!("idx-{:020}", i + 1));
        let got = client.get(&put.key).partition_key("ids").await.unwrap();
        assert_eq!(
            got.value.as_deref(),
            Some(format!("message-{}", i + 1).as_bytes())
        );
    }

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testSecondaryIndex`: list/range-scan over a secondary
/// index return primary keys ordered by index value, and deletes update it.
#[compio::test]
async fn test_secondary_index() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for (k, idx) in [
        ("si-a", "0"),
        ("si-b", "1"),
        ("si-c", "2"),
        ("si-d", "3"),
        ("si-e", "4"),
        ("si-f", "5"),
        ("si-g", "6"),
    ] {
        client
            .put(k, idx.as_bytes().to_vec())
            .secondary_index("val", idx)
            .await
            .unwrap();
    }

    assert_eq!(
        client.list("1", "4").use_index("val").await.unwrap(),
        vec!["si-b", "si-c", "si-d"]
    );

    let scan = client.range_scan("2", "5").use_index("val").await.unwrap();
    let mut keys: Vec<&str> = scan.iter().map(|r| r.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["si-c", "si-d", "si-e"]);

    client.delete("si-b").await.unwrap();

    assert_eq!(
        client.list("0", "3").use_index("val").await.unwrap(),
        vec!["si-a", "si-c"]
    );
    let scan = client.range_scan("0", "3").use_index("val").await.unwrap();
    let mut keys: Vec<&str> = scan.iter().map(|r| r.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["si-a", "si-c"]);

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testGetIncludeValue`: excluding the value returns no
/// value (Java: empty `byte[]`; Rust: `None`) while still resolving the record.
#[compio::test]
async fn test_get_include_value() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let key = "stream";
    let mut keys = Vec::new();
    for _ in 0..2 {
        let r = client
            .put(key, b"some-value".to_vec())
            .partition_key(key)
            .sequence_key_deltas(vec![1])
            .await
            .unwrap();
        keys.push(r.key);
    }

    for sub_key in &keys {
        let with_value = client
            .get(sub_key)
            .partition_key(key)
            .include_value(true)
            .await
            .unwrap();
        assert!(with_value.value.is_some());

        let without_value = client
            .get(sub_key)
            .partition_key(key)
            .include_value(false)
            .await
            .unwrap();
        assert!(without_value.value.is_none());
    }

    let higher = client
        .get(&keys[0])
        .partition_key(key)
        .include_value(false)
        .comparison(ComparisonType::Higher)
        .await
        .unwrap();
    assert!(higher.value.is_none());
    assert_eq!(higher.key, keys[1]);

    let lower = client
        .get(&keys[1])
        .partition_key(key)
        .include_value(false)
        .comparison(ComparisonType::Lower)
        .await
        .unwrap();
    assert!(lower.value.is_none());
    assert_eq!(lower.key, keys[0]);

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testSecondaryIndexGet`: `get(...).use_index(...)`
/// resolves an index value to its primary record, honoring the comparison type.
#[compio::test]
async fn test_secondary_index_get() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let prefix = "si-get";
    // i in 1..10: primary key "<prefix>-<'a'+i>", index value "%03d".
    for i in 1..10u8 {
        let primary = format!("{prefix}-{}", (b'a' + i) as char);
        let value = format!("{i:03}");
        client
            .put(&primary, value.clone().into_bytes())
            .secondary_index("val-idx", &value)
            .await
            .unwrap();
    }

    // Resolves an index value to `(primary key, value)`, or None.
    async fn idx_get(
        client: &OxiaClient,
        index_value: &str,
        cmp: ComparisonType,
    ) -> Option<(String, Vec<u8>)> {
        match client
            .get(index_value)
            .use_index("val-idx")
            .comparison(cmp)
            .await
        {
            Ok(r) => Some((r.key, r.value.map(|b| b.to_vec()).unwrap_or_default())),
            Err(OxiaError::KeyNotFound) => None,
            Err(e) => panic!("indexed get({index_value:?}, {cmp:?}) failed: {e}"),
        }
    }
    use ComparisonType::{Ceiling, Equal, Higher, Lower};
    let b = format!("{prefix}-b");
    let e = format!("{prefix}-e");
    let f = format!("{prefix}-f");
    let g = format!("{prefix}-g");
    let i_ = format!("{prefix}-i");
    let j = format!("{prefix}-j");
    let c = format!("{prefix}-c");

    assert_eq!(idx_get(&client, "000", Equal).await, None);
    assert_eq!(
        idx_get(&client, "001", Equal).await,
        Some((b.clone(), b"001".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "005", Equal).await,
        Some((f.clone(), b"005".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "009", Equal).await,
        Some((j.clone(), b"009".to_vec()))
    );
    assert_eq!(idx_get(&client, "999", Equal).await, None);

    assert_eq!(idx_get(&client, "000", Lower).await, None);
    assert_eq!(idx_get(&client, "001", Lower).await, None);
    assert_eq!(
        idx_get(&client, "005", Lower).await,
        Some((e.clone(), b"004".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "009", Lower).await,
        Some((i_.clone(), b"008".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "999", Lower).await,
        Some((j.clone(), b"009".to_vec()))
    );

    assert_eq!(
        idx_get(&client, "000", Higher).await,
        Some((b.clone(), b"001".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "001", Higher).await,
        Some((c.clone(), b"002".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "005", Higher).await,
        Some((g.clone(), b"006".to_vec()))
    );
    assert_eq!(idx_get(&client, "009", Higher).await, None);
    assert_eq!(idx_get(&client, "999", Higher).await, None);

    assert_eq!(
        idx_get(&client, "000", Ceiling).await,
        Some((b.clone(), b"001".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "001", Ceiling).await,
        Some((b.clone(), b"001".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "005", Ceiling).await,
        Some((f.clone(), b"005".to_vec()))
    );
    assert_eq!(
        idx_get(&client, "009", Ceiling).await,
        Some((j.clone(), b"009".to_vec()))
    );
    assert_eq!(idx_get(&client, "999", Ceiling).await, None);

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testGetSequenceUpdates`: the stream reports the highest
/// sequence key; a fresh subscriber immediately sees the current latest, then
/// subsequent advances.
///
/// The Java case that a missing partition key throws `IllegalArgumentException`
/// is not portable — `sequence_updates` takes the partition key as a required
/// argument, so it cannot be called without one.
#[compio::test]
async fn test_get_sequence_updates() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let key = "su-key";
    let put_seq = || {
        client
            .put(key, b"0".to_vec())
            .partition_key("x")
            .sequence_key_deltas(vec![1])
    };

    let mut updates1 = client.sequence_updates(key, "x").await.unwrap();
    // No update before the first put.
    assert!(
        compio::time::timeout(Duration::from_secs(1), updates1.recv())
            .await
            .is_err(),
        "no sequence update expected before the first put"
    );

    let r1 = put_seq().await.unwrap();
    assert_eq!(r1.key, format!("{key}-{:020}", 1));
    let got = compio::time::timeout(Duration::from_secs(5), updates1.recv())
        .await
        .expect("update not delivered")
        .expect("stream closed");
    assert_eq!(got, r1.key);

    // Drop the first subscriber; further advances must not reach it.
    drop(updates1);
    let r2 = put_seq().await.unwrap();

    // A fresh subscriber immediately receives the current latest, then advances.
    let mut updates2 = client.sequence_updates(key, "x").await.unwrap();
    let latest = compio::time::timeout(Duration::from_secs(5), updates2.recv())
        .await
        .expect("current latest not delivered")
        .expect("stream closed");
    assert_eq!(latest, r2.key);

    let r3 = put_seq().await.unwrap();
    let r4 = put_seq().await.unwrap();
    // Advances may coalesce; read until the stream reaches the last key.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last = None;
    while last.as_deref() != Some(r4.key.as_str()) {
        match compio::time::timeout_at(deadline, updates2.recv()).await {
            Ok(Some(k)) => last = Some(k),
            _ => break,
        }
    }
    assert_eq!(last.as_deref(), Some(r4.key.as_str()));
    assert!(r3.key < r4.key, "sequence keys must advance monotonically");

    client.close().await.unwrap();
}

/// Port of `OxiaClientIT.testMaximumSize`: a 50MiB value goes through, in a
/// batch of its own, and one of the server's maximum message size (256MiB)
/// fails with the server's status (Java: `OxiaStatusException`).
#[compio::test]
async fn test_maximum_size() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let payload = vec![0u8; 50 * 1024 * 1024];
    client.put("max-size/50m", payload.clone()).await.unwrap();
    let got = client.get("max-size/50m").await.unwrap();
    assert_eq!(got.value.as_deref(), Some(payload.as_slice()));

    let result = client
        .put("max-size/256m", vec![0u8; 256 * 1024 * 1024])
        .await;
    assert!(
        matches!(
            &result,
            Err(OxiaError::Grpc {
                code: tonic::Code::ResourceExhausted,
                ..
            })
        ),
        "got {result:?}"
    );

    client.close().await.unwrap();
}

/// Port of `NotificationIt.testDeleteRange` (10 shards): a `delete_range`
/// surfaces as one `KeyRangeDeleted` notification per shard, each carrying the
/// requested bounds.
///
/// Unlike the Java test, no warm-up is needed: `notifications()` only returns
/// once every shard's subscription is live (this SDK's P1-13 behavior), so no
/// change can be missed.
#[compio::test]
async fn test_delete_range_notification() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let mut rx = client.notifications().await.unwrap();

    for i in 0..10 {
        client
            .put(i.to_string(), i.to_string().into_bytes())
            .await
            .unwrap();
    }

    client.delete_range("0", "100").await.unwrap();

    // A broadcast delete_range produces one KeyRangeDeleted per shard, each with
    // the requested bounds (the 10 creations above arrive first and are skipped).
    let expected = SHARDS as usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut range_deletes = 0;
    while range_deletes < expected {
        match compio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(Notification::KeyRangeDeleted {
                key,
                key_range_last,
            })) => {
                assert_eq!(key, "0");
                assert_eq!(key_range_last.as_deref(), Some("100"));
                range_deletes += 1;
            }
            Ok(Some(_)) => continue,
            Ok(None) => panic!("notification stream closed early"),
            Err(_) => panic!("only saw {range_deletes}/{expected} range-delete notifications"),
        }
    }

    client.close().await.unwrap();
}

/// Port of `OxiaClientFailFastIT.testWrongNamespace`: building a client for a
/// namespace the server does not serve fails fast (Java: `NamespaceNotFoundException`).
#[compio::test]
async fn test_wrong_namespace() {
    let (_container, address) = start_oxia().await;

    let build = OxiaClientBuilder::new()
        .service_address(address)
        .namespace("my-ns-does-not-exist")
        .request_timeout(Duration::from_secs(5))
        .build();

    match compio::time::timeout(Duration::from_secs(15), build).await {
        Ok(Ok(_)) => panic!("build unexpectedly succeeded for a nonexistent namespace"),
        Ok(Err(_)) => {} // failed fast with the namespace error
        Err(_) => panic!("build hung instead of failing fast on a bad namespace"),
    }
}
