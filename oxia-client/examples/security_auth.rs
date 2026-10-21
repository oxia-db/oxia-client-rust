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

//! Bearer-token authentication.
//!
//! Every request carries an `authorization: Bearer <token>` header. Two ways
//! to supply the token:
//! - [`auth_token`](oxia::OxiaClientBuilder::auth_token): a static token.
//! - [`auth_token_provider`](oxia::OxiaClientBuilder::auth_token_provider): a
//!   closure called once per request, so rotated credentials are picked up
//!   transparently. It must return quickly and never block — refresh tokens
//!   elsewhere (e.g. a background task) and hand out the cached value here.
//!
//! Environment:
//! - `OXIA_ADDRESS` — service address (default: `localhost:6648`)
//! - `OXIA_TOKEN`   — the bearer token to present
//!
//! Tokens are readable by anyone who can observe the connection; combine
//! authentication with TLS (see the `security_tls` example) outside trusted
//! networks.

use oxia::OxiaClient;
use std::sync::{Arc, RwLock};
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

    let address = std::env::var("OXIA_ADDRESS").unwrap_or_else(|_| "localhost:6648".to_string());
    let token = std::env::var("OXIA_TOKEN").unwrap_or_default();

    // Simplest form: a static token.
    let client = OxiaClient::builder()
        .service_address(address.clone())
        .auth_token(token.clone())
        .build()
        .await
        .unwrap();
    info!("Connected with a static token");
    client.put("auth/static", "hello").await.unwrap();
    client.close().await.unwrap();

    // Rotating credentials: the provider is consulted for every request.
    // Here a shared slot stands in for whatever refreshes the token (an OAuth
    // client, a file watcher, ...) — the closure itself just reads the cache.
    let current_token = Arc::new(RwLock::new(token));
    let provider_view = current_token.clone();
    let client = OxiaClient::builder()
        .service_address(address)
        .auth_token_provider(move || provider_view.read().expect("token lock").clone())
        .build()
        .await
        .unwrap();
    info!("Connected with a token provider");

    client.put("auth/rotating", "hello").await.unwrap();
    // A refresher updating the slot is picked up by the very next request:
    *current_token.write().expect("token lock") = "refreshed-token".to_string();

    let record = client.get("auth/rotating").await;
    info!("Read after rotation: {record:?}");

    client.close().await.unwrap();
}
