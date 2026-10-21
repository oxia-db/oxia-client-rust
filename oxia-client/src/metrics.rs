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

//! Optional OpenTelemetry metrics, mirroring the Go client's `oxia_client`
//! meter. Enabled with the `otel` Cargo feature; without it, [`Metrics`] is a
//! zero-cost no-op so recording costs nothing in the default build.
//!
//! Recorded through [nanometer](https://docs.rs/nanometer) handles, one per
//! series, created with the client: the client lives on one thread, and so do
//! its handles. Every instrument is a histogram, which nanometer exports
//! through its `Producer`.
//!
//! Instruments (scope `oxia_client`), each carrying `type` and `result`
//! (success / failure) attributes:
//! - `oxia_client_op` — operation latency, in seconds; `type` put / get /
//!   delete / delete_range.
//! - `oxia_client_op_value` — operation value size, in bytes (put / get).
//! - `oxia_client_batch_total` — batch latency from when the batch was
//!   created, in seconds; `type` write / read.
//! - `oxia_client_batch_exec` — batch latency from when the batch was first
//!   sent, in seconds.
//! - `oxia_client_batch_value` — the values of a batch, in bytes: those of the
//!   puts of a write batch, or of the gets' responses of a read batch.
//! - `oxia_client_batch_request` — the number of operations in a batch.

use std::time::Instant;

#[cfg(feature = "otel")]
pub(crate) use otel::Metrics;

#[cfg(not(feature = "otel"))]
pub(crate) use noop::Metrics;

/// The `type` attribute of the operation metrics.
#[derive(Clone, Copy)]
pub(crate) enum OpType {
    Put,
    Get,
    Delete,
    DeleteRange,
}

/// The `type` attribute of the batch metrics.
#[derive(Clone, Copy)]
pub(crate) enum BatchType {
    Write,
    Read,
}

/// What the batch metrics record about a batch when it completes.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
pub(crate) struct BatchStats {
    pub(crate) created: Instant,
    /// When the batch was first sent: `None` for a batch that failed before.
    pub(crate) sent: Option<Instant>,
    pub(crate) value_size: usize,
    pub(crate) requests: usize,
}

#[cfg(feature = "otel")]
mod otel {
    use super::{BatchStats, BatchType, OpType};
    use nanometer::{HistogramDef, LatencyDef, LocalHistogram, LocalLatency, Meter};
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::MeterProvider;
    use std::time::{Duration, Instant};

    const SCOPE: &str = "oxia_client";

    const OP_TIME: LatencyDef =
        LatencyDef::new("oxia_client_op").description("Latency of client operations");
    const OP_VALUE: HistogramDef =
        HistogramDef::bytes("oxia_client_op_value").description("Value size of client operations");
    const BATCH_TOTAL: LatencyDef = LatencyDef::new("oxia_client_batch_total")
        .description("Latency of client batches, from when they were created");
    const BATCH_EXEC: LatencyDef = LatencyDef::new("oxia_client_batch_exec")
        .description("Latency of client batches, from when they were first sent");
    const BATCH_VALUE: HistogramDef =
        HistogramDef::bytes("oxia_client_batch_value").description("Value size of client batches");
    const BATCH_REQUESTS: HistogramDef = HistogramDef::counts("oxia_client_batch_request")
        .unit("{request}")
        .description("Operations per client batch");

    /// The `type` of the operation metrics, indexed by [`OpType`].
    const OP_TYPES: [&str; 4] = ["put", "get", "delete", "delete_range"];
    /// The operations with a value: the first two of [`OP_TYPES`].
    const VALUE_OP_TYPES: [&str; 2] = ["put", "get"];
    /// The `type` of the batch metrics, indexed by [`BatchType`].
    const BATCH_TYPES: [&str; 2] = ["write", "read"];

    /// One handle per series of a metric, by `type` and then by `result`,
    /// indexed by whether the operation succeeded.
    fn series<T, const N: usize>(
        types: [&'static str; N],
        handle: impl Fn(&[KeyValue]) -> T,
    ) -> [[T; 2]; N] {
        types.map(|type_name| {
            [false, true].map(|ok| {
                handle(&[
                    KeyValue::new("type", type_name),
                    KeyValue::new("result", if ok { "success" } else { "failure" }),
                ])
            })
        })
    }

    /// Records the client's metrics under the `oxia_client` scope.
    ///
    /// `!Send`, like the client: its nanometer handles belong to the thread
    /// that created them.
    pub(crate) struct Metrics {
        op_time: [[LocalLatency; 2]; 4],
        op_value: [[LocalHistogram; 2]; 2],
        batch_total: [[LocalLatency; 2]; 2],
        batch_exec: [[LocalLatency; 2]; 2],
        batch_value: [[LocalHistogram; 2]; 2],
        batch_requests: [[LocalHistogram; 2]; 2],
    }

    impl Metrics {
        /// Creates the handles from `meter`, or from the global meter provider
        /// when none was configured (matching the Go client's default).
        pub(crate) fn new(meter: Option<Meter>) -> Self {
            let meter = meter
                .unwrap_or_else(|| Meter::new(&*opentelemetry::global::meter_provider(), SCOPE));
            Metrics {
                op_time: series(OP_TYPES, |a| meter.latency(OP_TIME, a)),
                op_value: series(VALUE_OP_TYPES, |a| meter.histogram(OP_VALUE, a)),
                batch_total: series(BATCH_TYPES, |a| meter.latency(BATCH_TOTAL, a)),
                batch_exec: series(BATCH_TYPES, |a| meter.latency(BATCH_EXEC, a)),
                batch_value: series(BATCH_TYPES, |a| meter.histogram(BATCH_VALUE, a)),
                batch_requests: series(BATCH_TYPES, |a| meter.histogram(BATCH_REQUESTS, a)),
            }
        }

        /// Records a completed operation: its `op_type`, latency, and outcome.
        /// `value_size` is recorded only when the operation carries a value
        /// (a put's value, or a get's response value).
        pub(crate) fn record_op(
            &self,
            op_type: OpType,
            latency: Duration,
            value_size: Option<usize>,
            ok: bool,
        ) {
            let op = op_type as usize;
            let result = ok as usize;
            self.op_time[op][result].record(latency);
            if let (Some(size), Some(value)) = (value_size, self.op_value.get(op)) {
                value[result].record(size as u64);
            }
        }

        /// Records a completed batch: its `batch_type`, stats, and outcome.
        pub(crate) fn record_batch(&self, batch_type: BatchType, stats: &BatchStats, ok: bool) {
            let batch = batch_type as usize;
            let result = ok as usize;
            let now = Instant::now();
            self.batch_total[batch][result].record(now - stats.created);
            if let Some(sent) = stats.sent {
                self.batch_exec[batch][result].record(now - sent);
            }
            self.batch_value[batch][result].record(stats.value_size as u64);
            self.batch_requests[batch][result].record(stats.requests as u64);
        }
    }

    impl Default for Metrics {
        /// Records through the global meter provider.
        fn default() -> Self {
            Metrics::new(None)
        }
    }

    /// The nanometer meter of the `oxia_client` scope, exporting through
    /// `provider`. Used by the client builder's `meter_provider`.
    pub(crate) fn meter_from_provider(provider: &impl MeterProvider) -> Meter {
        Meter::new(provider, SCOPE)
    }
}

#[cfg(feature = "otel")]
pub(crate) use otel::meter_from_provider;

#[cfg(not(feature = "otel"))]
mod noop {
    use super::{BatchStats, BatchType, OpType};
    use std::time::Duration;

    /// No-op stand-in for [`super::Metrics`] when the `otel` feature is off.
    #[derive(Default)]
    pub(crate) struct Metrics;

    impl Metrics {
        #[inline]
        pub(crate) fn record_op(
            &self,
            _op_type: OpType,
            _latency: Duration,
            _value_size: Option<usize>,
            _ok: bool,
        ) {
        }

        #[inline]
        pub(crate) fn record_batch(&self, _batch_type: BatchType, _stats: &BatchStats, _ok: bool) {}
    }
}
