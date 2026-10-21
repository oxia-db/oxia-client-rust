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
    let payload = "payload";

    // put ephemeral record
    let put_result = client.put("key1", payload).ephemeral().await.unwrap();
    info!(
        "put the ephemeral record. key {:?} value {:?} version {:?}",
        put_result.key, payload, put_result.version
    );
    // close the client (this closes the session, which deletes ephemeral records)
    client.close().await.unwrap();

    // create a new client and verify the ephemeral record is gone
    let client2 = OxiaClient::connect("localhost:6648").await.unwrap();
    let result = client2.get("key1").await;
    info!("get the value again. error: {:?}", result.unwrap_err());

    client2.close().await.unwrap();
}
