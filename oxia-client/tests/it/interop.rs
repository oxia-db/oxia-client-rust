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

//! Cross-client interop tests: the Rust client and the reference Go client (the
//! `oxia` CLI shipped in the server image) must agree on how keys are routed to
//! shards. Both are exercised against the same multi-shard namespace.
//!
//! The routing-critical direction is "Rust writes, Go reads": the Go CLI does a
//! single-shard get, so if the two clients hashed keys differently, Go would
//! look on the wrong shard and fail to find records the Rust client wrote.

use oxia::{OxiaClient, OxiaClientBuilder};
use std::time::Duration;
use testcontainers::core::ports::ContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::core::{CmdWaitFor, ExecCommand};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::common;

const OXIA_PORT: u16 = 6648;
const SHARDS: u32 = 10;
const KEY_COUNT: usize = 24;

/// Starts a standalone Oxia server with `SHARDS` shards so keys actually spread
/// across shards and routing is exercised.
async fn start_oxia_multishard() -> (Container<GenericImage>, String) {
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

/// Runs `oxia client <args>` (the Go client) inside the container and returns
/// its trimmed stdout. Requires exit code 0, so a failed get/put panics.
async fn oxia_cli(container: &Container<GenericImage>, args: &[&str]) -> String {
    let mut cmd: Vec<String> = vec!["oxia".to_string(), "client".to_string()];
    cmd.extend(args.iter().map(|s| s.to_string()));
    let mut result = container
        .exec(ExecCommand::new(cmd).with_cmd_ready_condition(CmdWaitFor::exit_code(0)))
        .unwrap_or_else(|e| panic!("`oxia client {}` failed: {e}", args.join(" ")));
    let out = result.stdout_to_vec().expect("read exec stdout");
    String::from_utf8(out)
        .expect("exec stdout is not utf-8")
        .trim_end()
        .to_string()
}

/// Rust writes across shards; the Go CLI must find every record on the shard it
/// routes to. A routing-hash mismatch would make the Go get fail (KeyNotFound).
#[compio::test]
async fn rust_writes_go_reads_across_shards() {
    let (container, address) = start_oxia_multishard().await;
    let client = new_client(&address).await;

    for i in 0..KEY_COUNT {
        let key = format!("interop/rust-{i}");
        let value = format!("rust-value-{i}");
        client
            .put(key, value.into_bytes())
            .await
            .expect("rust put failed");
    }

    for i in 0..KEY_COUNT {
        let key = format!("interop/rust-{i}");
        let expected = format!("rust-value-{i}");
        let got = oxia_cli(&container, &["get", &key]).await;
        assert_eq!(got, expected, "Go CLI read the wrong value for {key}");
    }

    client.close().await.unwrap();
}

/// The Go CLI writes across shards; the Rust client must read every value back,
/// confirming value/proto round-tripping across clients.
#[compio::test]
async fn go_writes_rust_reads_across_shards() {
    let (container, address) = start_oxia_multishard().await;
    let client = new_client(&address).await;

    for i in 0..KEY_COUNT {
        let key = format!("interop/go-{i}");
        let value = format!("go-value-{i}");
        oxia_cli(&container, &["put", &key, &value]).await;
    }

    for i in 0..KEY_COUNT {
        let key = format!("interop/go-{i}");
        let expected = format!("go-value-{i}");
        let got = client.get(key.clone()).await.expect("rust get failed");
        assert_eq!(
            got.value.as_deref(),
            Some(expected.as_bytes()),
            "Rust read the wrong value for {key}"
        );
    }

    client.close().await.unwrap();
}
