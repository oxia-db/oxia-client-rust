//! Rust client SDK for [Oxia](https://github.com/oxia-db/oxia), a scalable
//! metadata store and coordination system.
//!
//! The entry point is [`OxiaClient`]. Every operation returns a request builder:
//! chain options onto it and `.await` it directly.
//!
//! The [Oxia documentation site](https://oxia-db.github.io/) carries the full
//! concept guide and side-by-side examples for every language SDK; this page is
//! the Rust quick reference and API index.
//!
//! ```no_run
//! use oxia::{ComparisonType, OxiaClient};
//!
//! # async fn example() -> Result<(), oxia::OxiaError> {
//! let client = OxiaClient::connect("localhost:6648").await?;
//!
//! // Plain put/get.
//! let res = client.put("greeting", "hello").await?;
//! let rec = client.get("greeting").await?;
//! assert_eq!(rec.value.as_deref(), Some(b"hello".as_ref()));
//!
//! // Options chain onto the builder.
//! client
//!     .put("config/a", "1")
//!     .expected_version_id(res.version.version_id)
//!     .await?;
//! client.put("locks/w1", "").ephemeral().await?;
//! let floor = client.get("config/z").comparison(ComparisonType::Floor).await?;
//!
//! // Range operations, as a whole result or as an ordered stream.
//! let keys = client.list("config/", "config/~").await?;
//! let mut scan = client.range_scan("config/", "config/~").stream().await?;
//! # let _ = (floor, keys, scan);
//! client.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Concepts
//!
//! Each concept links to its Rust entry point and to the matching guide on the
//! [Oxia docs site](https://oxia-db.github.io/), which covers it in depth and
//! shows the equivalent in every other SDK.
//!
//! - **Records & versioning** — key/value pairs carry a [`Version`] (version id
//!   and timestamps); conditional operations
//!   ([`expected_version_id`](PutBuilder::expected_version_id)) implement
//!   compare-and-swap.
//!   ([guide](https://oxia-db.github.io/docs/features/versioning))
//! - **Sharding** — keys route to shards by hash; the client tracks assignments
//!   and transparently re-routes operations across shard splits and merges.
//!   ([guide](https://oxia-db.github.io/docs/architecture/logical-architecture))
//! - **Sessions & ephemeral records** — [`ephemeral`](PutBuilder::ephemeral)
//!   records live as long as the client's session and are removed when it closes
//!   or expires; the session is kept alive and re-created transparently.
//!   ([guide](https://oxia-db.github.io/docs/features/ephemerals))
//! - **Partition keys** ([`partition_key`](PutBuilder::partition_key)) co-locate
//!   related records on one shard, enabling atomic multi-record batches.
//!   ([guide](https://oxia-db.github.io/docs/features/partition-keys))
//! - **Sequential keys** — the server appends a monotonic, zero-padded suffix
//!   per delta ([`sequence_key_deltas`](PutBuilder::sequence_key_deltas)); follow
//!   advances with [`sequence_updates`](OxiaClient::sequence_updates).
//!   ([guide](https://oxia-db.github.io/docs/features/sequence-keys))
//! - **Secondary indexes** ([`secondary_index`](PutBuilder::secondary_index))
//!   add alternate query paths for gets, lists and scans
//!   ([`use_index`](GetBuilder::use_index)).
//!   ([guide](https://oxia-db.github.io/docs/features/secondary-indexes))
//! - **Notifications** ([`notifications`](OxiaClient::notifications)) stream
//!   every change applied to the database.
//!   ([guide](https://oxia-db.github.io/docs/features/notifications))
//!
//! Keys use Oxia's slash-aware order (`/`-separated path segments), not plain
//! lexicographic order; range boundaries follow it.
//! ([guide](https://oxia-db.github.io/docs/features/oxia-key-sorting))
//!
//! # Options
//!
//! Configure the client with [`OxiaClientBuilder`]:
//! [`namespace`](OxiaClientBuilder::namespace),
//! [`request_timeout`](OxiaClientBuilder::request_timeout),
//! [`session_timeout`](OxiaClientBuilder::session_timeout) /
//! [`session_keep_alive`](OxiaClientBuilder::session_keep_alive),
//! [`identity`](OxiaClientBuilder::identity), token authentication
//! ([`auth_token`](OxiaClientBuilder::auth_token) /
//! [`auth_token_provider`](OxiaClientBuilder::auth_token_provider)), and the
//! batching limits
//! ([`batch_max_size`](OxiaClientBuilder::batch_max_size),
//! [`max_requests_per_batch`](OxiaClientBuilder::max_requests_per_batch),
//! [`max_write_batches_in_flight`](OxiaClientBuilder::max_write_batches_in_flight),
//! [`max_read_batches_in_flight`](OxiaClientBuilder::max_read_batches_in_flight)).
//! Every default is documented on the builder.
//!
//! Per-operation options chain onto the request builder before `.await`:
//!
//! | Operation | Options |
//! |-----------|---------|
//! | [`put`](OxiaClient::put) | [`expected_version_id`](PutBuilder::expected_version_id), [`expected_record_not_exists`](PutBuilder::expected_record_not_exists), [`partition_key`](PutBuilder::partition_key), [`sequence_key_deltas`](PutBuilder::sequence_key_deltas), [`secondary_index`](PutBuilder::secondary_index), [`ephemeral`](PutBuilder::ephemeral) |
//! | [`get`](OxiaClient::get) | [`comparison`](GetBuilder::comparison), [`partition_key`](GetBuilder::partition_key), [`include_value`](GetBuilder::include_value), [`use_index`](GetBuilder::use_index) |
//! | [`delete`](OxiaClient::delete) | [`expected_version_id`](DeleteBuilder::expected_version_id), [`partition_key`](DeleteBuilder::partition_key) |
//! | [`delete_range`](OxiaClient::delete_range) | [`partition_key`](DeleteRangeBuilder::partition_key) |
//! | [`list`](OxiaClient::list) | [`partition_key`](ListBuilder::partition_key), [`use_index`](ListBuilder::use_index), [`stream`](ListBuilder::stream) |
//! | [`range_scan`](OxiaClient::range_scan) | [`partition_key`](RangeScanBuilder::partition_key), [`use_index`](RangeScanBuilder::use_index), [`stream`](RangeScanBuilder::stream) |
//! | [`notifications`](OxiaClient::notifications) | [`buffer_size`](NotificationsBuilder::buffer_size) |
//! | [`sequence_updates`](OxiaClient::sequence_updates) | [`buffer_size`](SequenceUpdatesBuilder::buffer_size) |
//!
//! # Error handling
//!
//! Every operation returns [`OxiaError`]. Semantic outcomes callers commonly
//! match on are dedicated variants ([`OxiaError::KeyNotFound`],
//! [`OxiaError::UnexpectedVersionId`], …); transient infrastructure failures
//! answer `true` to [`OxiaError::is_retryable`] and are safe to retry —
//! idempotent operations unconditionally, non-idempotent ones (puts without a
//! version condition) with application-level judgment. Each operation on
//! [`OxiaClient`] documents the errors it can produce.
//!
//! The client retries retryable failures internally — re-routing via the
//! leader hints the server attaches to routing errors, re-hashing operations
//! onto the new shard when a shard is split or merged, with exponential
//! backoff, bounded by the request timeout — so the errors you observe are
//! post-retry. One consequence, shared with the reference clients: a write
//! whose batch failed *after* reaching the wire may be retried even though the
//! server already applied it, so unconditional writes have at-least-once
//! semantics under retries; version-conditioned writes are exactly-once.
//!
//! # Cancellation
//!
//! Dropping an operation's future stops waiting but does not recall an
//! operation already submitted to a batch; it may still execute on the
//! server. The `recv` methods on [`Notifications`] and [`SequenceUpdates`]
//! are cancel-safe.
//!
//! # Cargo features
//!
//! - **`otel`** *(off by default)* — records client metrics through
//!   [OpenTelemetry](https://opentelemetry.io/). With the feature enabled,
//!   per-operation latency and value sizes are reported under the `oxia_client`
//!   meter; attach a meter provider with [`OxiaClientBuilder`]'s
//!   `meter_provider` method, or let the client fall back to the global
//!   provider. Without it, metrics recording compiles to a zero-cost no-op.
//! - **`tls`** *(off by default)* — TLS connections via
//!   [rustls](https://github.com/rustls/rustls). Enabled by an `https://` (or
//!   `tls://`) service address, which verifies the server against the system
//!   trust roots; a custom CA, a client certificate (mutual TLS), or a
//!   domain-name override are configured with [`OxiaClientBuilder`]'s `tls`
//!   method (see `TlsOptions`).
//!
//! Token authentication needs no feature: it is always available through
//! [`auth_token`](OxiaClientBuilder::auth_token) /
//! [`auth_token_provider`](OxiaClientBuilder::auth_token_provider).

#![forbid(unsafe_code)]
// Render "Available on crate feature otel" badges on docs.rs (nightly-only
// attribute, enabled solely under the `docsrs` cfg that docs.rs sets).
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![allow(clippy::result_large_err)]
// A panic on wire data must never take down the caller's process: forbid
// `.unwrap()` in library code so every fallible result is handled. Unit tests
// (compiled with `cfg(test)`) are exempt, where unwrapping is the idiom.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

mod address;
mod auth;
mod batcher;
mod client;
mod client_builder;
mod client_options;
mod errors;
mod hash;
mod key;
mod metrics;
#[cfg(test)]
mod mock_server;
mod notification_manager;
mod operations;
mod provider_manager;
mod requests;
mod retry;
mod sequence_updates_manager;
mod server_error;
mod session_manager;
mod shard_manager;
mod streams;
#[cfg(feature = "tls")]
mod tls;
mod types;

#[allow(
    clippy::derive_partial_eq_without_eq,
    // `#[async_trait]` marks the generated server trait's methods
    // `#[must_use]`, and newer clippy also treats their boxed-future return
    // type as must-use.
    clippy::double_must_use,
    clippy::enum_variant_names,
    missing_docs,
    // The generated server stubs are used only by the cfg(test) mock server,
    // so they are dead code in non-test builds.
    dead_code
)]
pub(crate) mod proto {
    include!(concat!(env!("OUT_DIR"), "/io.oxia.proto.v1.rs"));
}

pub use bytes::Bytes;

pub use auth::TokenProvider;
pub use client::OxiaClient;
pub use client_builder::OxiaClientBuilder;
pub use errors::OxiaError;
pub use requests::{
    DeleteBuilder, DeleteRangeBuilder, GetBuilder, ListBuilder, NotificationsBuilder, PutBuilder,
    RangeScanBuilder, SequenceUpdatesBuilder,
};
pub use streams::{ListStream, Notifications, RangeScanStream, SequenceUpdates};
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub use tls::TlsOptions;
pub use types::{ComparisonType, GetResult, Notification, PutResult, Version};
