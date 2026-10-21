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

use std::time::Duration;
use uuid::Uuid;

/// Configuration options for the Oxia client.
#[derive(Debug, Clone)]
pub struct OxiaClientOptions {
    /// The address of the Oxia service (e.g., "http://localhost:6648").
    pub service_address: String,
    /// The Oxia namespace to use (default: "default").
    pub namespace: String,
    /// Client identity string, used for ephemeral record tracking.
    pub identity: String,
    /// Maximum batch size in bytes (default: 128KB).
    pub batch_max_size: u32,
    /// Maximum number of requests per batch (default: 1000).
    pub max_requests_per_batch: u32,
    /// Write batches in flight per shard before the open batch waits
    /// (default: 4).
    pub max_write_batches_in_flight: u32,
    /// Read batches in flight per shard before the open batch waits
    /// (default: 4).
    pub max_read_batches_in_flight: u32,
    /// Session timeout for ephemeral records (default: 15s).
    pub session_timeout: Duration,
    /// Interval between session keep-alive heartbeats (default: `session_timeout / 10`).
    pub session_keep_alive: Duration,
    /// Timeout for individual requests (default: 30s).
    pub request_timeout: Duration,
    /// Token authentication; `None` sends no credentials (default).
    pub auth: Option<crate::auth::TokenAuth>,
    /// TLS settings; `None` connects in plaintext (default).
    #[cfg(feature = "tls")]
    pub tls: Option<crate::tls::TlsOptions>,
}

impl Default for OxiaClientOptions {
    fn default() -> Self {
        Self {
            service_address: String::from("http://127.0.0.1:6648"),
            namespace: String::from("default"),
            identity: Uuid::new_v4().to_string(),
            batch_max_size: 128 * 1024,
            max_requests_per_batch: 1000,
            max_write_batches_in_flight: 4,
            max_read_batches_in_flight: 4,
            session_timeout: Duration::from_secs(15),
            session_keep_alive: Duration::from_secs(15) / 10,
            request_timeout: Duration::from_secs(30),
            auth: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}
