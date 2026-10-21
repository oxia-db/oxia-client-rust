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

use oxia::OxiaClient;
use std::time::Duration;
use tracing::info;
use tracing::level_filters::LevelFilter;

#[compio::main]
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

    let partition_key = "my-partition";
    let base_key = "seq/events";

    // Subscribe to sequence updates for the key.
    let mut updates = client
        .sequence_updates(base_key, partition_key)
        .buffer_size(50)
        .await
        .unwrap();
    info!("Subscribed to sequence updates for key '{}'", base_key);

    // Spawn a task to listen for sequence updates
    let listener = compio::runtime::spawn(async move {
        let mut count = 0;
        while let Some(highest_key) = updates.recv().await {
            info!("Sequence update: highest_sequence_key={:?}", highest_key);
            count += 1;
            if count >= 5 {
                break;
            }
        }
        info!("Received {} sequence updates", count);
    });

    // Give the listener time to connect
    compio::time::sleep(Duration::from_millis(500)).await;

    // Put records with sequence key deltas - the server assigns sequential
    // suffixes and returns the generated key.
    for i in 1..=5 {
        let result = client
            .put(base_key, format!("event-{i}"))
            .partition_key(partition_key)
            .sequence_key_deltas([1])
            .await
            .unwrap();
        info!("Put sequence event {}: generated key={:?}", i, result.key);
    }

    // Wait for listener to process updates (with timeout)
    let _ = compio::time::timeout(Duration::from_secs(5), listener).await;

    // Cleanup
    client
        .delete_range("seq/", "seq/~")
        .partition_key(partition_key)
        .await
        .unwrap();
    info!("Cleaned up sequence keys.");

    client.close().await.unwrap();
}
