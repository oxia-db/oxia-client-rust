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
//! Instruments (meter `oxia_client`), each carrying `type`
//! (put / get / delete / delete_range) and `result` (success / failure)
//! attributes:
//! - `oxia_client_op` — operation latency, in milliseconds.
//! - `oxia_client_op_value` — operation value size, in bytes.

#[cfg(feature = "otel")]
pub(crate) use otel::Metrics;

#[cfg(not(feature = "otel"))]
pub(crate) use noop::Metrics;

#[cfg(feature = "otel")]
mod otel {
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::{Histogram, Meter, MeterProvider};
    use std::time::Duration;

    /// Records the client's per-operation metrics under the `oxia_client`
    /// meter. Cheap to clone — the histograms are `Arc`-backed handles.
    #[derive(Clone)]
    pub(crate) struct Metrics {
        op_time: Histogram<f64>,
        op_value: Histogram<u64>,
    }

    impl Metrics {
        /// Builds the instruments from `meter`, or the global meter provider
        /// when none was configured (matching the Go client's default).
        pub(crate) fn new(meter: Option<Meter>) -> Self {
            let meter = meter.unwrap_or_else(|| opentelemetry::global::meter("oxia_client"));
            Metrics {
                op_time: meter
                    .f64_histogram("oxia_client_op")
                    .with_unit("ms")
                    .with_description("Latency of client operations")
                    .build(),
                op_value: meter
                    .u64_histogram("oxia_client_op_value")
                    .with_unit("By")
                    .with_description("Value size of client operations")
                    .build(),
            }
        }

        /// Records a completed operation: its `op_type`, latency, and outcome.
        /// `value_size` is recorded only when the operation carries a value
        /// (a put's value, or a get's response value).
        pub(crate) fn record_op(
            &self,
            op_type: &'static str,
            latency: Duration,
            value_size: Option<usize>,
            ok: bool,
        ) {
            let attrs = [
                KeyValue::new("type", op_type),
                KeyValue::new("result", if ok { "success" } else { "failure" }),
            ];
            self.op_time.record(latency.as_secs_f64() * 1000.0, &attrs);
            if let Some(size) = value_size {
                self.op_value.record(size as u64, &attrs);
            }
        }
    }

    /// Builds a [`Metrics`] from an OpenTelemetry meter provider, naming the
    /// meter `oxia_client`. Used by the client builder's `meter_provider`.
    pub(crate) fn meter_from_provider(provider: &impl MeterProvider) -> Meter {
        provider.meter("oxia_client")
    }
}

#[cfg(feature = "otel")]
pub(crate) use otel::meter_from_provider;

#[cfg(not(feature = "otel"))]
mod noop {
    use std::time::Duration;

    /// No-op stand-in for [`super::Metrics`] when the `otel` feature is off.
    #[derive(Clone, Default)]
    pub(crate) struct Metrics;

    impl Metrics {
        #[inline]
        pub(crate) fn record_op(
            &self,
            _op_type: &'static str,
            _latency: Duration,
            _value_size: Option<usize>,
            _ok: bool,
        ) {
        }
    }
}
