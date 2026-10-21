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

//! `oxia-perf` — a load-generation and latency-measurement CLI for the Oxia
//! Rust client, mirroring the perf tools shipped with the Go and Java SDKs.
//!
//! It drives a configurable mix of reads and writes at a target rate against a
//! set of keys, and reports throughput and latency percentiles every 10s (plus
//! an aggregate summary on exit). The client, like the load, runs on one
//! thread.

use clap::Parser;
use futures::future::{Either, select};
use hdrhistogram::Histogram;
use oxia::{Bytes, OxiaClient, OxiaError};
use rand::RngCore;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// mimalloc rather than the system allocator: the client allocates on every
/// operation.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// How often interval stats are printed.
const REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// How late a tick of the traffic generators may be and still catch up on
/// the schedule, as for tokio's interval.
const MISSED_TICK_TOLERANCE: Duration = Duration::from_millis(5);

#[derive(Parser, Debug)]
#[command(
    name = "oxia-perf",
    version,
    about = "Load-generation and latency benchmarking for the Oxia Rust client"
)]
struct Args {
    /// Oxia service address (host:port).
    #[arg(short = 'a', long, default_value = "localhost:6648")]
    service_address: String,

    /// Oxia namespace.
    #[arg(short = 'n', long, default_value = "default")]
    namespace: String,

    /// Total request rate, ops/s.
    #[arg(short = 'r', long, default_value_t = 100.0)]
    rate: f64,

    /// Percentage of requests that are reads (the rest are writes).
    #[arg(short = 'p', long = "read-write-percent", default_value_t = 80.0)]
    read_percentage: f64,

    /// Number of distinct keys the load is spread over.
    #[arg(short = 'k', long, default_value_t = 1000)]
    keys_cardinality: u32,

    /// Value size in bytes.
    #[arg(short = 's', long, default_value_t = 128)]
    value_size: usize,

    /// Generate a fresh random payload for every write.
    #[arg(long)]
    random_payload: bool,

    /// Maximum number of requests per batch.
    #[arg(long, default_value_t = 1000)]
    max_requests_per_batch: u32,

    /// Per-request timeout, in seconds.
    #[arg(long, default_value_t = 30.0)]
    request_timeout: f64,
}

#[compio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    if !(0.0..=100.0).contains(&args.read_percentage) {
        return Err("--read-write-percent must be between 0 and 100".into());
    }
    if args.keys_cardinality == 0 {
        return Err("--keys-cardinality must be greater than 0".into());
    }
    info!(?args, "starting oxia-perf");

    let client = OxiaClient::builder()
        .service_address(&args.service_address)
        .namespace(&args.namespace)
        .max_requests_per_batch(args.max_requests_per_batch)
        .request_timeout(Duration::from_secs_f64(args.request_timeout))
        .build()
        .await?;

    let keys: Arc<Vec<String>> = Arc::new(
        (0..args.keys_cardinality)
            .map(|i| format!("key-{i}"))
            .collect(),
    );
    let stats = Arc::new(Stats::new());

    let mut generators = Vec::new();
    let write_rate = args.rate * (100.0 - args.read_percentage) / 100.0;
    if write_rate > 0.0 {
        generators.push(compio::runtime::spawn(write_traffic(
            client.clone(),
            keys.clone(),
            stats.clone(),
            write_rate,
            args.value_size,
            args.random_payload,
        )));
    }
    let read_rate = args.rate * args.read_percentage / 100.0;
    if read_rate > 0.0 {
        generators.push(compio::runtime::spawn(read_traffic(
            client.clone(),
            keys.clone(),
            stats.clone(),
            read_rate,
        )));
    }

    let start = Instant::now();
    let mut ticker = compio::time::interval(REPORT_INTERVAL);
    ticker.tick().await; // discard the immediate first tick
    let mut ctrl_c = pin!(compio::signal::ctrl_c());
    loop {
        match select(pin!(ticker.tick()), ctrl_c.as_mut()).await {
            Either::Left(_) => stats.report_interval(),
            Either::Right(_) => {
                println!("\nInterrupted, shutting down…");
                break;
            }
        }
    }

    // Stop generating load before tearing the client down: dropping their
    // handles cancels the generators.
    drop(generators);
    stats.report_summary(start.elapsed());
    client.close().await?;
    Ok(())
}

async fn write_traffic(
    client: OxiaClient,
    keys: Arc<Vec<String>>,
    stats: Arc<Stats>,
    rate: f64,
    value_size: usize,
    random_payload: bool,
) {
    let fixed: Bytes = Bytes::from(vec![0u8; value_size]);
    let mut ticker = Ticker::new(Duration::from_secs_f64(1.0 / rate));
    loop {
        ticker.tick().await;
        let key = keys[rand::random_range(0..keys.len())].clone();
        let value: Bytes = if random_payload {
            let mut v = vec![0u8; value_size];
            rand::rng().fill_bytes(&mut v);
            v.into()
        } else {
            fixed.clone()
        };
        let client = client.clone();
        let stats = stats.clone();
        compio::runtime::spawn(async move {
            let start = Instant::now();
            match client.put(key.clone(), value).await {
                Ok(_) => stats.record_write(elapsed_micros(start)),
                // In flight while the client is closing on shutdown; not a failure.
                Err(OxiaError::Closed) => {}
                Err(err) => {
                    warn!(key = %key, %err, "write failed");
                    stats.record_failure();
                }
            }
        })
        .detach();
    }
}

async fn read_traffic(client: OxiaClient, keys: Arc<Vec<String>>, stats: Arc<Stats>, rate: f64) {
    let mut ticker = Ticker::new(Duration::from_secs_f64(1.0 / rate));
    loop {
        ticker.tick().await;
        let key = keys[rand::random_range(0..keys.len())].clone();
        let client = client.clone();
        let stats = stats.clone();
        compio::runtime::spawn(async move {
            let start = Instant::now();
            match client.get(key.clone()).await {
                // A miss is a normal outcome for a read, not a failure.
                Ok(_) | Err(OxiaError::KeyNotFound) => stats.record_read(elapsed_micros(start)),
                // In flight while the client is closing on shutdown; not a failure.
                Err(OxiaError::Closed) => {}
                Err(err) => {
                    warn!(key = %key, %err, "read failed");
                    stats.record_failure();
                }
            }
        })
        .detach();
    }
}

/// Ticks at a fixed period, as tokio's interval with
/// `MissedTickBehavior::Delay` does: a tick late by less than
/// [`MISSED_TICK_TOLERANCE`] catches up on the schedule, so rates above the
/// timer resolution are reached; after a later one, the next tick comes a
/// full period later rather than in a burst.
struct Ticker {
    period: Duration,
    deadline: Instant,
}

impl Ticker {
    fn new(period: Duration) -> Self {
        Ticker {
            period,
            deadline: Instant::now(),
        }
    }

    async fn tick(&mut self) {
        compio::time::sleep_until(self.deadline).await;
        let now = Instant::now();
        self.deadline = if now > self.deadline + MISSED_TICK_TOLERANCE {
            now + self.period
        } else {
            self.deadline + self.period
        };
    }
}

fn elapsed_micros(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}

/// Latency histograms and counters shared by the traffic generators and the
/// reporter. Interval histograms are reset each report and folded into the
/// running totals for the final summary.
struct Stats {
    write: Mutex<Histogram<u64>>,
    read: Mutex<Histogram<u64>>,
    write_total: Mutex<Histogram<u64>>,
    read_total: Mutex<Histogram<u64>>,
    failed: AtomicU64,
    failed_total: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        let hist = || Mutex::new(Histogram::<u64>::new(3).expect("valid histogram config"));
        Stats {
            write: hist(),
            read: hist(),
            write_total: hist(),
            read_total: hist(),
            failed: AtomicU64::new(0),
            failed_total: AtomicU64::new(0),
        }
    }

    fn record_write(&self, micros: u64) {
        record(&self.write, micros);
    }

    fn record_read(&self, micros: u64) {
        record(&self.read, micros);
    }

    fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.failed_total.fetch_add(1, Ordering::Relaxed);
    }

    fn report_interval(&self) {
        let secs = REPORT_INTERVAL.as_secs_f64();
        let (w_ops, w) = drain(&self.write, &self.write_total);
        let (r_ops, r) = drain(&self.read, &self.read_total);
        let failed = self.failed.swap(0, Ordering::Relaxed) as f64 / secs;
        let w_rate = w_ops as f64 / secs;
        let r_rate = r_ops as f64 / secs;
        println!(
            "Stats - Total ops: {:6.1} ops/s - Failed ops: {:6.1} ops/s\n\
             \x20       Write ops {:6.1} w/s  Latency ms: 50% {:5.1} - 95% {:5.1} - 99% {:5.1} - 99.9% {:5.1} - max {:6.1}\n\
             \x20       Read  ops {:6.1} r/s  Latency ms: 50% {:5.1} - 95% {:5.1} - 99% {:5.1} - 99.9% {:5.1} - max {:6.1}",
            w_rate + r_rate,
            failed,
            w_rate,
            w[0],
            w[1],
            w[2],
            w[3],
            w[4],
            r_rate,
            r[0],
            r[1],
            r[2],
            r[3],
            r[4],
        );
    }

    fn report_summary(&self, elapsed: Duration) {
        // Fold the last (partial) interval into the totals.
        fold(&self.write, &self.write_total);
        fold(&self.read, &self.read_total);
        let w = self.write_total.lock().expect("mutex");
        let r = self.read_total.lock().expect("mutex");
        let secs = elapsed.as_secs_f64().max(0.001);
        let total = w.len() + r.len();
        println!(
            "\nAggregated over {:.0}s: {} ops ({} writes, {} reads), {} failed — {:.1} ops/s\n\
             \x20  Write latency ms: p50 {:.1}  p95 {:.1}  p99 {:.1}  p99.9 {:.1}  max {:.1}\n\
             \x20  Read  latency ms: p50 {:.1}  p95 {:.1}  p99 {:.1}  p99.9 {:.1}  max {:.1}",
            secs,
            total,
            w.len(),
            r.len(),
            self.failed_total.load(Ordering::Relaxed),
            total as f64 / secs,
            ms(&w, 0.5),
            ms(&w, 0.95),
            ms(&w, 0.99),
            ms(&w, 0.999),
            max_ms(&w),
            ms(&r, 0.5),
            ms(&r, 0.95),
            ms(&r, 0.99),
            ms(&r, 0.999),
            max_ms(&r),
        );
    }
}

fn record(hist: &Mutex<Histogram<u64>>, micros: u64) {
    // hdrhistogram's lowest discernible value is 1; a sub-microsecond op clamps.
    let _ = hist.lock().expect("mutex").record(micros.max(1));
}

fn ms(hist: &Histogram<u64>, quantile: f64) -> f64 {
    hist.value_at_quantile(quantile) as f64 / 1000.0
}

fn max_ms(hist: &Histogram<u64>) -> f64 {
    hist.max() as f64 / 1000.0
}

/// Reads the interval histogram's op count and latency percentiles (ms), folds
/// it into the running total, and resets it for the next interval.
fn drain(interval: &Mutex<Histogram<u64>>, total: &Mutex<Histogram<u64>>) -> (u64, [f64; 5]) {
    let mut hist = interval.lock().expect("mutex");
    let count = hist.len();
    let percentiles = [
        ms(&hist, 0.5),
        ms(&hist, 0.95),
        ms(&hist, 0.99),
        ms(&hist, 0.999),
        max_ms(&hist),
    ];
    let _ = total.lock().expect("mutex").add(&*hist);
    hist.clear();
    (count, percentiles)
}

fn fold(interval: &Mutex<Histogram<u64>>, total: &Mutex<Histogram<u64>>) {
    let mut hist = interval.lock().expect("mutex");
    let _ = total.lock().expect("mutex").add(&*hist);
    hist.clear();
}
