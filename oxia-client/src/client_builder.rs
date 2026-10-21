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

//! Builder for configuring and creating an [`OxiaClient`].

use crate::address::ensure_protocol;
use crate::client::OxiaClient;
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use std::time::Duration;

/// Configures and creates an [`OxiaClient`].
///
/// | Option | Default |
/// |---|---|
/// | [`service_address`](OxiaClientBuilder::service_address) | `127.0.0.1:6648` |
/// | [`namespace`](OxiaClientBuilder::namespace) | `default` |
/// | [`identity`](OxiaClientBuilder::identity) | random UUID |
/// | [`batch_max_size`](OxiaClientBuilder::batch_max_size) | 128 KiB |
/// | [`max_requests_per_batch`](OxiaClientBuilder::max_requests_per_batch) | 1000 |
/// | [`max_write_batches_in_flight`](OxiaClientBuilder::max_write_batches_in_flight) | 4 |
/// | [`max_read_batches_in_flight`](OxiaClientBuilder::max_read_batches_in_flight) | 4 |
/// | [`session_timeout`](OxiaClientBuilder::session_timeout) | 15 s |
/// | [`session_keep_alive`](OxiaClientBuilder::session_keep_alive) | `session_timeout / 10` |
/// | [`request_timeout`](OxiaClientBuilder::request_timeout) | 30 s |
///
/// ```no_run
/// # async fn example() -> Result<(), oxia::OxiaError> {
/// use oxia::OxiaClient;
/// use std::time::Duration;
///
/// let client = OxiaClient::builder()
///     .service_address("localhost:6648")
///     .namespace("my-namespace")
///     .session_timeout(Duration::from_secs(30))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
#[must_use = "builders do nothing unless built"]
pub struct OxiaClientBuilder {
    service_address: Option<String>,
    namespace: Option<String>,
    identity: Option<String>,
    batch_max_size: Option<u32>,
    max_requests_per_batch: Option<u32>,
    max_write_batches_in_flight: Option<u32>,
    max_read_batches_in_flight: Option<u32>,
    session_timeout: Option<Duration>,
    session_keep_alive: Option<Duration>,
    request_timeout: Option<Duration>,
    auth: Option<crate::auth::TokenAuth>,
    #[cfg(feature = "otel")]
    meter: Option<nanometer::Meter>,
    #[cfg(feature = "tls")]
    tls: Option<crate::tls::TlsOptions>,
}

// The `auth` token provider (an `Arc<dyn TokenProvider>`) and the `otel`
// meter (around an `Arc<dyn InstrumentProvider>`) are not structurally
// unwind-safe, which would strip these auto traits from the builder — a
// spurious semver break, since neither reading a token nor recording a metric
// across an unwind boundary is unsound. Assert them back; the compile-time
// check in the tests module proves the builder stays unwind-safe in every
// feature configuration.
impl std::panic::UnwindSafe for OxiaClientBuilder {}
impl std::panic::RefUnwindSafe for OxiaClientBuilder {}

impl OxiaClientBuilder {
    /// Creates a builder with all options at their defaults.
    pub fn new() -> Self {
        OxiaClientBuilder::default()
    }

    /// The address of the Oxia service, as `host:port` (default:
    /// `127.0.0.1:6648`).
    pub fn service_address(mut self, service_address: impl Into<String>) -> Self {
        self.service_address = Some(ensure_protocol(service_address.into()));
        self
    }

    /// The Oxia namespace all operations apply to (default: `"default"`).
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// The client identity reported with ephemeral records (default: a random
    /// UUID).
    pub fn identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = Some(identity.into());
        self
    }

    /// How many write batches may be in flight per shard before operations
    /// accumulate in the open batch (default: 4).
    ///
    /// Batching is rate-adaptive: while fewer batches are outstanding on a
    /// shard's write stream, operations are dispatched almost immediately;
    /// once the window is exhausted, the open batch accumulates operations, so
    /// batch size automatically tracks the server's service rate. A batch
    /// that reaches [`batch_max_size`](Self::batch_max_size) or
    /// [`max_requests_per_batch`](Self::max_requests_per_batch) goes out at
    /// once, whatever the window. Must be at least 1.
    pub fn max_write_batches_in_flight(mut self, max: u32) -> Self {
        self.max_write_batches_in_flight = Some(max);
        self
    }

    /// How many read batches may be in flight per shard before gets
    /// accumulate in the open batch (default: 4). Works like
    /// [`max_write_batches_in_flight`](Self::max_write_batches_in_flight), for
    /// the read RPCs of a shard. Must be at least 1.
    pub fn max_read_batches_in_flight(mut self, max: u32) -> Self {
        self.max_read_batches_in_flight = Some(max);
        self
    }

    /// The maximum byte size of a batch (default: 128KiB), counting the keys
    /// and values of its writes. A write larger than this goes out in a
    /// batch of its own.
    pub fn batch_max_size(mut self, batch_max_size: u32) -> Self {
        self.batch_max_size = Some(batch_max_size);
        self
    }

    /// The maximum number of operations in a batch (default: 1000).
    pub fn max_requests_per_batch(mut self, max_requests_per_batch: u32) -> Self {
        self.max_requests_per_batch = Some(max_requests_per_batch);
        self
    }

    /// The session timeout for ephemeral records (default: 15s). Ephemeral
    /// records disappear if the client fails to heartbeat for this long.
    pub fn session_timeout(mut self, session_timeout: Duration) -> Self {
        self.session_timeout = Some(session_timeout);
        self
    }

    /// The interval between session keep-alive heartbeats (default:
    /// `session_timeout / 10`).
    pub fn session_keep_alive(mut self, session_keep_alive: Duration) -> Self {
        self.session_keep_alive = Some(session_keep_alive);
        self
    }

    /// The deadline applied to individual operations (default: 30s).
    pub fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }

    /// Authenticates every request with the given static bearer token
    /// (`authorization: Bearer <token>`). For tokens that rotate, use
    /// [`auth_token_provider`](Self::auth_token_provider).
    pub fn auth_token(self, token: impl Into<String>) -> Self {
        let token = token.into();
        self.auth_token_provider(move || token.clone())
    }

    /// Authenticates every request with bearer tokens from the given
    /// [`TokenProvider`](crate::TokenProvider), called once per request so
    /// rotated credentials are picked up transparently. Any
    /// `Fn() -> String + Send + Sync` closure works.
    pub fn auth_token_provider(mut self, provider: impl crate::TokenProvider + 'static) -> Self {
        self.auth = Some(crate::auth::TokenAuth(std::sync::Arc::new(provider)));
        self
    }

    /// Records client metrics through the given OpenTelemetry meter provider,
    /// under the scope named `oxia_client`. When unset, the global meter
    /// provider is used. Requires the `otel` feature.
    ///
    /// The client's metrics are [nanometer](https://docs.rs/nanometer)
    /// histograms, which export through a `nanometer::Producer` registered
    /// with the app's metric reader, not through the provider: see the `otel`
    /// feature in the crate docs.
    #[cfg(feature = "otel")]
    #[cfg_attr(docsrs, doc(cfg(feature = "otel")))]
    pub fn meter_provider(mut self, provider: &impl opentelemetry::metrics::MeterProvider) -> Self {
        self.meter = Some(crate::metrics::meter_from_provider(provider));
        self
    }

    /// Connects with TLS, using the given [`TlsOptions`](crate::TlsOptions).
    /// Requires the `tls` feature.
    ///
    /// TLS is also enabled — with default options, verifying against the
    /// system trust roots — by an `https://` (or `tls://`) service address.
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    pub fn tls(mut self, tls: crate::tls::TlsOptions) -> Self {
        self.tls = Some(tls);
        self
    }

    fn assemble_options(self) -> OxiaClientOptions {
        let mut options = OxiaClientOptions::default();
        if let Some(service_address) = self.service_address {
            options.service_address = service_address
        }
        if let Some(namespace) = self.namespace {
            options.namespace = namespace;
        }
        if let Some(identity) = self.identity {
            options.identity = identity;
        }
        if let Some(batch_max_size) = self.batch_max_size {
            options.batch_max_size = batch_max_size;
        }
        if let Some(max_requests_per_batch) = self.max_requests_per_batch {
            options.max_requests_per_batch = max_requests_per_batch;
        }
        if let Some(max) = self.max_write_batches_in_flight {
            options.max_write_batches_in_flight = max;
        }
        if let Some(max) = self.max_read_batches_in_flight {
            options.max_read_batches_in_flight = max;
        }
        if let Some(session_timeout) = self.session_timeout {
            options.session_timeout = session_timeout;
        }
        // Default the keep-alive interval to session_timeout/10, matching the
        // reference client, once the final session timeout is known.
        options.session_keep_alive = self
            .session_keep_alive
            .unwrap_or(options.session_timeout / 10);
        if let Some(request_timeout) = self.request_timeout {
            options.request_timeout = request_timeout;
        }
        options.auth = self.auth;
        // An https:// (or tls://, normalized to https:// on entry) service
        // address implies TLS with default options, matching the Go client's
        // `tls://` handling.
        #[cfg(feature = "tls")]
        {
            options.tls = self.tls.or_else(|| {
                options
                    .service_address
                    .starts_with("https://")
                    .then(crate::tls::TlsOptions::new)
            });
        }
        options
    }

    /// Connects to the cluster and returns the client.
    ///
    /// Must run inside a compio runtime, on the thread the client will be
    /// used on (see [Runtime](crate#runtime)); elsewhere it fails with
    /// [`OxiaError::InvalidArgument`].
    ///
    /// Fails fast — within the request timeout — when the cluster is
    /// unreachable, the namespace does not exist, or the first shard
    /// assignments cannot be obtained.
    pub async fn build(self) -> Result<OxiaClient, OxiaError> {
        if compio::runtime::Runtime::try_current().is_none() {
            return Err(OxiaError::InvalidArgument(
                "the client must be built inside a compio runtime".to_string(),
            ));
        }
        #[cfg(feature = "otel")]
        let metrics = crate::metrics::Metrics::new(self.meter.clone());
        #[cfg(not(feature = "otel"))]
        let metrics = crate::metrics::Metrics;
        let options = self.assemble_options();
        if options.max_write_batches_in_flight == 0 || options.max_read_batches_in_flight == 0 {
            return Err(OxiaError::InvalidArgument(
                "max batches in flight must be at least 1".to_string(),
            ));
        }
        // Without the `tls` feature an https:// dial would only fail deep in
        // the transport; reject it up front with an actionable message.
        #[cfg(not(feature = "tls"))]
        if options.service_address.starts_with("https://") {
            return Err(OxiaError::InvalidArgument(
                "TLS service address requires the `tls` Cargo feature".to_string(),
            ));
        }
        OxiaClient::new(options, metrics).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The builder must stay unwind-safe in every feature configuration; the
    // `otel` meter field must not silently strip these auto traits (see the
    // manual impls above). Compile-time only.
    const _: fn() = || {
        fn assert_unwind_safe<T: std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
        assert_unwind_safe::<OxiaClientBuilder>();
    };

    // The client is single-threaded, but its configuration is not: a builder
    // can be cloned into one thread per client, and errors can be sent
    // anywhere. Compile-time only.
    const _: fn() = || {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OxiaClientBuilder>();
        assert_send_sync::<OxiaError>();
    };

    #[test]
    fn keep_alive_defaults_to_a_tenth_of_the_session_timeout() {
        let options = OxiaClientBuilder::new()
            .session_timeout(Duration::from_secs(30))
            .assemble_options();
        assert_eq!(options.session_keep_alive, Duration::from_secs(3));
    }

    #[test]
    fn keep_alive_can_be_overridden() {
        let options = OxiaClientBuilder::new()
            .session_timeout(Duration::from_secs(30))
            .session_keep_alive(Duration::from_millis(500))
            .assemble_options();
        assert_eq!(options.session_keep_alive, Duration::from_millis(500));
    }

    #[test]
    fn default_keep_alive_tracks_the_default_session_timeout() {
        let options = OxiaClientBuilder::new().assemble_options();
        assert_eq!(options.session_keep_alive, options.session_timeout / 10);
    }
}
