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
    let payload = "payload";

    let put_response = client.put("key", payload).await.unwrap();
    println!(
        "put the value. key {:?} version {:?}",
        put_response.key, put_response.version
    );

    // A CAS put against the wrong version id fails.
    let failed = client.put("key", payload).expected_version_id(-1).await;
    println!("put with error: {:?}", failed.unwrap_err());

    // A CAS put against the current version id succeeds.
    let put_response = client
        .put("key", payload)
        .expected_version_id(put_response.version.version_id)
        .await
        .unwrap();
    println!(
        "put the value. key {:?} version {:?}",
        put_response.key, put_response.version
    );

    client.close().await.unwrap();
}
