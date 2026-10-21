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

//! Counts the heap allocations the client makes per `put` and per `get`.
//!
//! A counting global allocator wraps the system one. Each scenario warms up,
//! then divides the allocations made during its measured loop by the number
//! of operations. Runs against a live server
//! (`cargo run --release --example alloc_count`):
//!
//! - `OXIA_ADDRESS` (default `localhost:6648`)
//! - `ALLOC_OPS`: operations per measured loop (default 10000)
//! - `ALLOC_TRACE=1`: also capture a backtrace for every allocation of a short
//!   second run of each scenario and print the allocation sites, per op. Build
//!   with debug info (`CARGO_PROFILE_RELEASE_DEBUG=1
//!   CARGO_PROFILE_RELEASE_STRIP=false`) for symbols.

use oxia::{Bytes, OxiaClient};
use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::collections::HashMap;
use std::future::{Future, IntoFuture, poll_fn};
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::task::Poll;

/// Operations kept in flight by the concurrent scenarios.
const DEPTH: usize = 100;
const TRACE_OPS: usize = 200;
const VALUE: &[u8] = &[0u8; 128];

#[global_allocator]
static ALLOCATOR: Counting = Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static TRACING: AtomicBool = AtomicBool::new(false);
static TRACES: Mutex<Vec<(&str, usize, Backtrace)>> = Mutex::new(Vec::new());

thread_local! {
    /// Set while capturing a backtrace, whose own allocations aren't counted.
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

impl Counting {
    fn record(kind: &'static str, counter: &AtomicU64, bytes: usize) {
        if IN_HOOK.get() {
            return;
        }
        counter.fetch_add(1, Relaxed);
        BYTES.fetch_add(bytes as u64, Relaxed);
        if TRACING.load(Relaxed) {
            IN_HOOK.set(true);
            let backtrace = Backtrace::force_capture();
            TRACES.lock().unwrap().push((kind, bytes, backtrace));
            IN_HOOK.set(false);
        }
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::record("alloc", &ALLOCS, layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::record("alloc", &ALLOCS, layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::record("realloc", &REALLOCS, new_size.saturating_sub(layout.size()));
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[derive(Clone, Copy)]
struct Counts {
    allocs: u64,
    reallocs: u64,
    bytes: u64,
}

fn counts() -> Counts {
    Counts {
        allocs: ALLOCS.load(Relaxed),
        reallocs: REALLOCS.load(Relaxed),
        bytes: BYTES.load(Relaxed),
    }
}

#[compio::main]
async fn main() {
    let address = std::env::var("OXIA_ADDRESS").unwrap_or_else(|_| "localhost:6648".into());
    let ops: usize = std::env::var("ALLOC_OPS").map_or(10_000, |v| v.parse().unwrap());
    let trace = std::env::var("ALLOC_TRACE").is_ok_and(|v| v == "1");

    let client = OxiaClient::connect(&address).await.unwrap();
    let keys: Vec<String> = (0..1000).map(|i| format!("alloc-key-{i:04}")).collect();
    let value = Bytes::from_static(VALUE);

    // Each op passes the key as a `&str`: the client's `impl Into<String>`
    // makes one allocation for it, as any caller passing an owned key would.
    // `run_concurrent` unwraps (and drops) the results.
    let put = |i: usize| {
        client
            .put(keys[i % keys.len()].as_str(), value.clone())
            .into_future()
    };
    let get = |i: usize| client.get(keys[i % keys.len()].as_str()).into_future();

    // The keys must exist for the gets.
    run_concurrent(keys.len(), 1, put).await;

    println!(
        "{:<26} {:>8} {:>10} {:>11} {:>10}",
        "scenario", "ops", "allocs/op", "reallocs/op", "bytes/op"
    );
    for (name, depth) in [("put sequential", 1), ("put concurrent", DEPTH)] {
        let c = measure(ops, |n| run_concurrent(n, depth, put)).await;
        print_row(name, ops, c);
    }
    for (name, depth) in [("get sequential", 1), ("get concurrent", DEPTH)] {
        let c = measure(ops, |n| run_concurrent(n, depth, get)).await;
        print_row(name, ops, c);
    }

    if trace {
        for (name, depth, is_put) in [
            ("put sequential", 1, true),
            ("put concurrent", DEPTH, true),
            ("get sequential", 1, false),
            ("get concurrent", DEPTH, false),
        ] {
            TRACES.lock().unwrap().clear();
            TRACING.store(true, Relaxed);
            if is_put {
                run_concurrent(TRACE_OPS, depth, put).await;
            } else {
                run_concurrent(TRACE_OPS, depth, get).await;
            }
            TRACING.store(false, Relaxed);
            let traces = std::mem::take(&mut *TRACES.lock().unwrap());
            print_sites(name, traces);
        }
    }

    client.close().await.unwrap();
}

/// Warms the scenario up with `ops` operations, then counts the allocations
/// of `ops` more.
async fn measure<F, Fut>(ops: usize, run: F) -> Counts
where
    F: Fn(usize) -> Fut,
    Fut: Future<Output = ()>,
{
    run(ops).await;
    let before = counts();
    run(ops).await;
    let after = counts();
    Counts {
        allocs: after.allocs - before.allocs,
        reallocs: after.reallocs - before.reallocs,
        bytes: after.bytes - before.bytes,
    }
}

/// Runs `n` operations, `depth` at a time, polling them in place (no task
/// spawns and no futures-combinator allocations of its own).
async fn run_concurrent<F, Fut, T, E>(n: usize, depth: usize, op: F)
where
    F: Fn(usize) -> Fut,
    Fut: Future<Output = Result<T, E>> + Unpin,
    E: std::fmt::Debug,
{
    let mut in_flight: Vec<Option<Fut>> = Vec::with_capacity(depth);
    let mut i = 0;
    while i < n {
        in_flight.extend((i..(i + depth).min(n)).map(|j| Some(op(j))));
        i += in_flight.len();
        poll_fn(|cx| {
            let mut pending = false;
            for slot in in_flight.iter_mut() {
                if let Some(fut) = slot {
                    if let Poll::Ready(result) = Pin::new(fut).poll(cx) {
                        result.unwrap();
                        *slot = None;
                    } else {
                        pending = true;
                    }
                }
            }
            if pending {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        in_flight.clear();
    }
}

fn print_row(name: &str, ops: usize, c: Counts) {
    let ops_f = ops as f64;
    println!(
        "{:<26} {:>8} {:>10.2} {:>11.2} {:>10.1}",
        name,
        ops,
        c.allocs as f64 / ops_f,
        c.reallocs as f64 / ops_f,
        c.bytes as f64 / ops_f
    );
}

/// Frames of the allocator and of the backtrace capture, skipped to reach the
/// code that allocated.
fn is_machinery(frame: &str) -> bool {
    let frame = frame.trim_start_matches('<');
    [
        "std::backtrace",
        "alloc_count::Counting",
        "__rustc::",
        "__rust_",
        "alloc::alloc::",
        "alloc::raw_vec::",
        "core::alloc::",
    ]
    .iter()
    .any(|p| frame.starts_with(p))
}

/// The frame's function, without generic arguments and with its location's
/// file name.
fn describe((symbol, location): &(String, Option<String>)) -> String {
    let mut name = String::new();
    let mut depth = 0;
    let mut chars = symbol.chars().peekable();
    while let Some(c) = chars.next() {
        // Drop turbofish arguments (`::<...>`), keep `<Type as Trait>`.
        if depth > 0 {
            match c {
                '<' => depth += 1,
                '>' => depth -= 1,
                _ => {}
            }
        } else if c == ':' && name.ends_with(':') && chars.peek() == Some(&'<') {
            name.pop();
            chars.next();
            depth = 1;
        } else {
            name.push(c);
        }
    }
    let file = location
        .as_deref()
        .map(|l| l.rsplit('/').next().unwrap_or(l))
        .unwrap_or("?");
    format!("{name} ({file})")
}

fn in_crates(frame: &str, crates: &[&str]) -> bool {
    let frame = frame.trim_start_matches(['<', '&', '(', '[', '*']);
    // A method of a primitive (`<u8 as …>`, `<&str as …>`) belongs to std.
    let primitive = frame
        .split([' ', '>', ',', ';'])
        .next()
        .is_some_and(|ty| !ty.contains("::"));
    if primitive {
        return crates == STD;
    }
    crates.iter().any(|c| frame.starts_with(c))
}

const STD: &[&str] = &["alloc::", "core::", "std::"];
const RPC: &[&str] = &["hyper::", "h2::", "tonic::", "cyper_core::"];
const OXIA: &[&str] = &["oxia::"];

fn print_sites(name: &str, traces: Vec<(&str, usize, Backtrace)>) {
    // (kind, first library frame, first hyper/h2/tonic frame, first oxia
    // frame) -> (count, bytes)
    let mut sites: HashMap<(&str, String, String, String), (u64, u64)> = HashMap::new();
    for (kind, bytes, backtrace) in &traces {
        let text = backtrace.to_string();
        let mut frames: Vec<(String, Option<String>)> = Vec::new();
        for line in text.lines() {
            let trimmed = line.trim_start();
            if let Some(location) = trimmed.strip_prefix("at ") {
                if let Some(last) = frames.last_mut() {
                    last.1 = Some(location.to_string());
                }
            } else if let Some((index, symbol)) = trimmed.split_once(": ")
                && index.chars().all(|c| c.is_ascii_digit())
            {
                frames.push((symbol.to_string(), None));
            }
        }
        let frames: Vec<_> = frames
            .into_iter()
            .filter(|(f, _)| !is_machinery(f))
            .collect();
        let first = |crates: &[&str], exclude: bool| {
            frames
                .iter()
                .find(|(f, _)| in_crates(f, crates) != exclude)
                .map(describe)
                .unwrap_or_else(|| "-".into())
        };
        let key = (
            *kind,
            first(STD, true),
            first(RPC, false),
            first(OXIA, false),
        );
        let site = sites.entry(key).or_default();
        site.0 += 1;
        site.1 += *bytes as u64;
    }
    let mut sorted: Vec<_> = sites.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    println!(
        "\n=== {name}: {} allocations+reallocs over {TRACE_OPS} ops ({:.2}/op) ===",
        traces.len(),
        traces.len() as f64 / TRACE_OPS as f64
    );
    for ((kind, site, rpc, oxia), (count, bytes)) in sorted {
        println!(
            "{:>6.2}/op {:>8.0}B/op [{kind}] {site}\n             rpc:  {rpc}\n             oxia: {oxia}",
            count as f64 / TRACE_OPS as f64,
            bytes as f64 / TRACE_OPS as f64
        );
    }
}
