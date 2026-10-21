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
    let payload = "payload";

    for key in ["key1", "key2", "key3"] {
        let put_result = client.put(key, payload).await.unwrap();
        info!(
            "put the value. key {:?} value {:?} version {:?}",
            put_result.key, payload, put_result.version
        );
    }

    // list keys
    let keys = client.list("", "/").await.unwrap();
    info!("list the keys. keys {:?}", keys);

    // range-scan
    let records = client.range_scan("", "/").await.unwrap();
    info!("range_scan result: {:?}", records);

    // get key1
    let get_result = client.get("key1").await.unwrap();
    info!(
        "get the value. key {:?} value {:?} version {:?}",
        get_result.key, get_result.value, get_result.version
    );

    // delete key1
    client.delete("key1").await.unwrap();
    info!("deleted key1");
    let result = client.get("key1").await;
    info!("get the value again. error: {:?}", result.unwrap_err());

    // delete range
    client.delete_range("", "/").await.unwrap();
    info!("delete range keys.");
    let keys = client.list("", "/").await.unwrap();
    info!("list the keys. keys {:?}", keys);

    client.close().await.unwrap();
}
