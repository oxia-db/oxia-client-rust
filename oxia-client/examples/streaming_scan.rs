//! Streaming list and range-scan: iterate a key range of any size in
//! O(shards) memory.
//!
//! `list(..)` / `range_scan(..)` awaited directly collect the whole result
//! into a `Vec` — fine for bounded ranges. Chaining `.stream()` instead
//! returns an ordered async stream that pulls from the server only as fast as
//! it is consumed (buffering at most one pending item per shard), so a scan
//! over millions of records runs in constant memory. Items arrive in the
//! global key order of the namespace; the first error terminates the stream.

use futures::StreamExt;
use oxia::OxiaClient;
use tracing::info;
use tracing::level_filters::LevelFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_target(false)
        .init();

    let client = OxiaClient::connect("localhost:6648").await.unwrap();

    // Seed some records.
    for i in 0..500 {
        client
            .put(format!("scan/item-{i:04}"), format!("value-{i}"))
            .await
            .unwrap();
    }
    info!("Seeded 500 records");

    // Stream just the keys, in order, without materializing the whole range.
    let mut keys = client.list("scan/", "scan/~").stream().await.unwrap();
    let mut count = 0;
    while let Some(key) = keys.next().await {
        let key = key.unwrap();
        if count < 3 {
            info!("key: {key}");
        }
        count += 1;
    }
    info!("Streamed {count} keys");

    // Stream full records (keys and values). Dropping the stream early
    // cancels the underlying RPCs, so consuming only a prefix is cheap.
    let mut records = client.range_scan("scan/", "scan/~").stream().await.unwrap();
    let mut bytes: usize = 0;
    while let Some(record) = records.next().await {
        let record = record.unwrap();
        bytes += record.value.as_ref().map(|v| v.len()).unwrap_or(0);
    }
    info!("Streamed all records: {bytes} value bytes total");

    client.delete_range("scan/", "scan/~").await.unwrap();
    client.close().await.unwrap();
}
