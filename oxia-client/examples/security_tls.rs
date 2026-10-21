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

//! Connecting over TLS (requires the `tls` Cargo feature):
//!
//! ```shell
//! cargo run --features tls --example security_tls
//! ```
//!
//! Environment:
//! - `OXIA_ADDRESS`     — service address (default: `https://localhost:6648`;
//!   an `https://` or `tls://` address enables TLS with the system trust roots)
//! - `OXIA_CA_CERT`     — path to a PEM CA certificate to trust instead of the
//!   system roots (e.g. a self-signed cluster CA)
//! - `OXIA_CLIENT_CERT` / `OXIA_CLIENT_KEY` — paths to a PEM client
//!   certificate and key, for clusters requiring mutual TLS

use oxia::{OxiaClient, TlsOptions};
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

    let address =
        std::env::var("OXIA_ADDRESS").unwrap_or_else(|_| "https://localhost:6648".to_string());

    // Default: verify the server against the operating system's trust roots.
    let mut tls = TlsOptions::new();

    // Custom CA: trust the cluster's CA instead of the system roots.
    if let Ok(ca_path) = std::env::var("OXIA_CA_CERT") {
        let ca_pem = std::fs::read(&ca_path).expect("read CA certificate");
        tls = tls.trusted_ca_pem(ca_pem);
        info!("Trusting CA from {ca_path}");
    }

    // Mutual TLS: present a client certificate.
    if let (Ok(cert_path), Ok(key_path)) = (
        std::env::var("OXIA_CLIENT_CERT"),
        std::env::var("OXIA_CLIENT_KEY"),
    ) {
        let cert_pem = std::fs::read(&cert_path).expect("read client certificate");
        let key_pem = std::fs::read(&key_path).expect("read client key");
        tls = tls.identity_pem(cert_pem, key_pem);
        info!("Presenting client identity from {cert_path}");
    }

    let client = OxiaClient::builder()
        .service_address(address.clone())
        .tls(tls)
        .build()
        .await
        .unwrap();
    info!("Connected over TLS to {address}");

    let put_result = client
        .put("tls/hello", "encrypted in transit")
        .await
        .unwrap();
    info!("Put succeeded, version {:?}", put_result.version.version_id);
    let record = client.get("tls/hello").await.unwrap();
    info!(
        "Got {:?} = {:?}",
        record.key,
        record.value.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    client.close().await.unwrap();
}
