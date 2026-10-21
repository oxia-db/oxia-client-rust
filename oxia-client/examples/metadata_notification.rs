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

use oxia::{Notification, OxiaClient};
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

    // Subscribe to notifications
    let mut notifications = client.notifications().await.unwrap();
    info!("Subscribed to notifications, performing operations...");

    // Spawn a task to listen for notifications
    let listener = compio::runtime::spawn(async move {
        let mut count = 0;
        while let Some(notification) = notifications.recv().await {
            match &notification {
                Notification::KeyCreated { key, version_id } => {
                    info!("Notification: KeyCreated key={key:?} version_id={version_id:?}");
                }
                Notification::KeyModified { key, version_id } => {
                    info!("Notification: KeyModified key={key:?} version_id={version_id:?}");
                }
                Notification::KeyDeleted { key } => {
                    info!("Notification: KeyDeleted key={key:?}");
                }
                Notification::KeyRangeDeleted {
                    key,
                    key_range_last,
                } => {
                    info!(
                        "Notification: KeyRangeDeleted key={key:?} key_range_last={key_range_last:?}"
                    );
                }
                other => info!("Notification: {other}"),
            }
            count += 1;
            if count >= 5 {
                break;
            }
        }
        info!("Received {} notifications total", count);
    });

    // Give the notification listener time to connect
    compio::time::sleep(Duration::from_millis(500)).await;

    // Perform operations that will generate notifications
    client.put("notify/key1", "value1").await.unwrap();
    info!("Put notify/key1");

    client.put("notify/key1", "value2").await.unwrap();
    info!("Modified notify/key1");

    client.put("notify/key2", "value3").await.unwrap();
    info!("Put notify/key2");

    client.delete("notify/key1").await.unwrap();
    info!("Deleted notify/key1");

    client.delete_range("notify/", "notify/~").await.unwrap();
    info!("Delete range notify/*");

    // Wait for listener to process notifications (with timeout)
    let _ = compio::time::timeout(Duration::from_secs(5), listener).await;

    client.close().await.unwrap();
}
