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

//! Sends the client's log events to a callback, so a wrapper SDK can put them
//! through its own runtime's logger.
//!
//! The client logs through [`tracing`], which a library must not configure
//! for the application embedding it: the dispatcher is process-global and
//! only one thing may own it. Here the application is the wrapper, so this
//! module lets it install a bridge that formats each event and hands it over.
//!
//! Unlike the completion callbacks, a log callback is invoked **synchronously
//! on whichever thread emitted the event**, not through the client's
//! dispatcher thread. The two are different kinds of thing: a completion
//! carries ownership of a result and has to arrive exactly once, so queueing
//! it is right, while a log line is droppable — and queueing those would
//! build an unbounded buffer during exactly the log storm you least want one
//! in. A wrapper does its own marshalling instead, which it has to do anyway:
//! Node cannot touch JS off the loop thread, Python needs the GIL, and only
//! the wrapper knows how much it is willing to buffer.
//!
//! So a log callback must not block, and must not call back into this
//! library.

use std::ffi::{c_char, c_void};
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU8, Ordering};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

use crate::error::{guard, guard_void, invalid, oxia_error_code, write_message};

/// How much to log. Ordered by verbosity, so each level admits the ones
/// before it.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum oxia_log_level {
    /// Nothing at all. What a zeroed argument asks for.
    OXIA_LOG_OFF = 0,
    /// Failures the library cannot handle itself. The client logs none
    /// today, so do not wait for these to appear.
    OXIA_LOG_ERROR = 1,
    /// Something went wrong and was retried or worked around: a failed
    /// background task, an expired session, a shard with no leader. Where
    /// the operational value is.
    OXIA_LOG_WARN = 2,
    /// Lifecycle: sessions, listeners and streams starting and stopping.
    OXIA_LOG_INFO = 3,
    /// Detail for diagnosing the client itself.
    OXIA_LOG_DEBUG = 4,
    /// Everything, including per-request detail. Verbose enough to affect
    /// throughput.
    OXIA_LOG_TRACE = 5,
}

impl oxia_log_level {
    fn of(level: &tracing::Level) -> oxia_log_level {
        match *level {
            tracing::Level::ERROR => oxia_log_level::OXIA_LOG_ERROR,
            tracing::Level::WARN => oxia_log_level::OXIA_LOG_WARN,
            tracing::Level::INFO => oxia_log_level::OXIA_LOG_INFO,
            tracing::Level::DEBUG => oxia_log_level::OXIA_LOG_DEBUG,
            tracing::Level::TRACE => oxia_log_level::OXIA_LOG_TRACE,
        }
    }

    fn filter(self) -> tracing::level_filters::LevelFilter {
        use tracing::level_filters::LevelFilter;
        match self {
            oxia_log_level::OXIA_LOG_OFF => LevelFilter::OFF,
            oxia_log_level::OXIA_LOG_ERROR => LevelFilter::ERROR,
            oxia_log_level::OXIA_LOG_WARN => LevelFilter::WARN,
            oxia_log_level::OXIA_LOG_INFO => LevelFilter::INFO,
            oxia_log_level::OXIA_LOG_DEBUG => LevelFilter::DEBUG,
            oxia_log_level::OXIA_LOG_TRACE => LevelFilter::TRACE,
        }
    }

    fn from_raw(raw: u8) -> oxia_log_level {
        match raw {
            1 => oxia_log_level::OXIA_LOG_ERROR,
            2 => oxia_log_level::OXIA_LOG_WARN,
            3 => oxia_log_level::OXIA_LOG_INFO,
            4 => oxia_log_level::OXIA_LOG_DEBUG,
            5 => oxia_log_level::OXIA_LOG_TRACE,
            _ => oxia_log_level::OXIA_LOG_OFF,
        }
    }
}

/// Receives one log event.
///
/// `target` is the module that emitted it, such as `oxia::session_manager`,
/// which maps onto the logger name most host logging frameworks want.
/// `target` and `message` are borrowed for the duration of the call: copy
/// what you keep.
pub type oxia_log_callback = extern "C" fn(
    level: oxia_log_level,
    target: *const c_char,
    message: *const c_char,
    ctx: *mut c_void,
);

/// Where events go, and the context to hand back. Both are opaque here; the
/// caller is told that the context must be usable from any thread.
#[derive(Clone, Copy)]
struct Sink {
    callback: oxia_log_callback,
    ctx: *mut c_void,
}

unsafe impl Send for Sink {}
unsafe impl Sync for Sink {}

/// The level to admit, so that it can be changed without reinstalling.
static MAX_LEVEL: AtomicU8 = AtomicU8::new(oxia_log_level::OXIA_LOG_OFF as u8);

/// The current sink, swappable: a caller may start with stderr and move to
/// its own logger later. The guard is dropped before the callback runs, so a
/// callback that somehow logs cannot deadlock against it.
static SINK: RwLock<Option<Sink>> = RwLock::new(None);

/// Whether this library owns the process's `tracing` dispatcher. Set on the
/// first install and never again: `set_global_default` only succeeds once.
static OWNS_DISPATCHER: OnceLock<bool> = OnceLock::new();

/// The bridge installed as the global subscriber. Stateless: everything it
/// needs is in the statics above, so the level and the sink can change
/// without reinstalling it.
struct Bridge;

impl Subscriber for Bridge {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        oxia_log_level::of(metadata.level()) as u8 <= MAX_LEVEL.load(Ordering::Relaxed)
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(oxia_log_level::from_raw(MAX_LEVEL.load(Ordering::Relaxed)).filter())
    }

    fn event(&self, event: &Event<'_>) {
        let metadata = event.metadata();
        let level = oxia_log_level::of(metadata.level());
        if level as u8 > MAX_LEVEL.load(Ordering::Relaxed) {
            return;
        }
        // Copy the sink out and release the lock before calling into the
        // caller's code, which may take arbitrarily long.
        let sink = match SINK.read() {
            Ok(sink) => *sink,
            Err(poisoned) => *poisoned.into_inner(),
        };
        let Some(sink) = sink else {
            return;
        };
        let mut text = Formatted::default();
        event.record(&mut text);
        let target = std::ffi::CString::new(metadata.target()).unwrap_or_default();
        let message = std::ffi::CString::new(text.0.replace('\0', "\\0")).unwrap_or_default();
        // A panic in a caller's log callback must not unwind into `tracing`,
        // which is emitting from the middle of the client's own work.
        let _ = std::panic::catch_unwind(|| {
            (sink.callback)(level, target.as_ptr(), message.as_ptr(), sink.ctx)
        });
    }

    // The client emits no spans, so there is nothing to track. These still
    // have to be answered, because the dispatcher is process-global and
    // another crate's spans reach it too; ignoring them loses that crate's
    // span context but never misbehaves.
    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        // Ids must be non-zero; one shared id is enough for spans that are
        // never entered or recorded.
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

/// Renders an event's fields the way `tracing`'s own formatter does: the
/// `message` field as the text, anything else appended as `key=value`.
#[derive(Default)]
struct Formatted(String);

impl Visit for Formatted {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
        } else {
            let _ = write!(self.0, " {}={}", field.name(), value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            // A `tracing` message is `fmt::Arguments`, whose Debug is its
            // Display, so this is the formatted line rather than a quoted
            // one.
            let _ = write!(self.0, "{value:?}");
        } else {
            let _ = write!(self.0, " {}={:?}", field.name(), value);
        }
    }
}

/// Installs `callback` as the destination for the client's log events, and
/// admits everything up to `max_level`.
///
/// Process-global, not per-client, and it affects every `tracing` event in
/// the process — including those of other Rust libraries linked into it,
/// because that is what owning the dispatcher means. Call it before creating
/// a client, so that the events from connecting are not missed.
///
/// Calling it again replaces the callback and the level, so a caller may
/// start with `oxia_log_to_stderr` and move to its own logger. It returns
/// `OXIA_ERR_ALREADY_SET` only when something *else* in the process already
/// owns `tracing` — a Rust host application with its own subscriber, say —
/// in which case this library will not take it away, and its events go
/// wherever that subscriber sends them.
///
/// The callback runs on whichever thread emitted the event, which includes
/// the client's own thread. It must not block, and must not call back into
/// this library.
///
/// # Safety
///
/// `ctx` is passed back untouched and must be usable from any thread.
/// `error_message` must be `NULL` or point at a writable `char *`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_set_log_callback(
    callback: Option<extern "C" fn(oxia_log_level, *const c_char, *const c_char, *mut c_void)>,
    max_level: oxia_log_level,
    ctx: *mut c_void,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let Some(callback) = callback else {
            return invalid(error_message, "callback is NULL");
        };
        install(Some(Sink { callback, ctx }), max_level, error_message)
    })
}

/// Sends the client's log events to standard error, one line each.
///
/// A shortcut for triage and for C and C++ programs that want no bridge of
/// their own; otherwise as `oxia_set_log_callback`, which it can be replaced
/// by.
///
/// # Safety
///
/// `error_message` must be `NULL` or point at a writable `char *`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_log_to_stderr(
    max_level: oxia_log_level,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        install(
            Some(Sink {
                callback: stderr_sink,
                ctx: std::ptr::null_mut(),
            }),
            max_level,
            error_message,
        )
    })
}

extern "C" fn stderr_sink(
    level: oxia_log_level,
    target: *const c_char,
    message: *const c_char,
    _ctx: *mut c_void,
) {
    let name = match level {
        oxia_log_level::OXIA_LOG_ERROR => "ERROR",
        oxia_log_level::OXIA_LOG_WARN => "WARN",
        oxia_log_level::OXIA_LOG_INFO => "INFO",
        oxia_log_level::OXIA_LOG_DEBUG => "DEBUG",
        oxia_log_level::OXIA_LOG_TRACE => "TRACE",
        oxia_log_level::OXIA_LOG_OFF => return,
    };
    // Both strings come from this library and are valid for the call.
    let target = unsafe { std::ffi::CStr::from_ptr(target) };
    let message = unsafe { std::ffi::CStr::from_ptr(message) };
    eprintln!(
        "{name:<5} {} {}",
        target.to_string_lossy(),
        message.to_string_lossy()
    );
}

/// Changes how much is logged, without touching the destination. Takes
/// effect immediately, and `OXIA_LOG_OFF` silences the bridge.
#[unsafe(no_mangle)]
pub extern "C" fn oxia_set_log_level(max_level: oxia_log_level) {
    guard_void(|| set_level(max_level));
}

/// Stops sending log events anywhere. The dispatcher stays installed — it
/// cannot be given back — but nothing is formatted or delivered.
#[unsafe(no_mangle)]
pub extern "C" fn oxia_clear_log_callback() {
    guard_void(|| {
        if let Ok(mut sink) = SINK.write() {
            *sink = None;
        }
        set_level(oxia_log_level::OXIA_LOG_OFF);
    });
}

/// Points `tracing` at this library's bridge, once per process, and sets the
/// sink and the level.
///
/// # Safety
///
/// As `oxia_set_log_callback`.
unsafe fn install(
    sink: Option<Sink>,
    max_level: oxia_log_level,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    // The sink and the level go in *before* the dispatcher, because
    // `set_global_default` reads `max_level_hint` as it installs and caches
    // the answer: installing while the level still said OFF would leave
    // `tracing` short-circuiting every macro before it ever asked `enabled`.
    match SINK.write() {
        Ok(mut current) => *current = sink,
        Err(poisoned) => *poisoned.into_inner() = sink,
    }
    set_level(max_level);
    let owns =
        *OWNS_DISPATCHER.get_or_init(|| tracing::subscriber::set_global_default(Bridge).is_ok());
    if !owns {
        MAX_LEVEL.store(oxia_log_level::OXIA_LOG_OFF as u8, Ordering::Relaxed);
        write_message(
            error_message,
            "another tracing subscriber already owns this process, so the \
             client's log events go there instead",
        );
        return oxia_error_code::OXIA_ERR_ALREADY_SET;
    }
    oxia_error_code::OXIA_OK
}

/// Stores the level and makes `tracing` notice it.
///
/// `tracing` caches both each callsite's interest and the global maximum
/// level, asking `max_level_hint` only when that cache is rebuilt — so a
/// level changed without this is a level that does nothing.
fn set_level(max_level: oxia_log_level) {
    MAX_LEVEL.store(max_level as u8, Ordering::Relaxed);
    tracing::callsite::rebuild_interest_cache();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_levels_are_ordered_by_verbosity() {
        assert!(oxia_log_level::OXIA_LOG_OFF < oxia_log_level::OXIA_LOG_ERROR);
        assert!(oxia_log_level::OXIA_LOG_ERROR < oxia_log_level::OXIA_LOG_WARN);
        assert!(oxia_log_level::OXIA_LOG_WARN < oxia_log_level::OXIA_LOG_INFO);
        assert!(oxia_log_level::OXIA_LOG_INFO < oxia_log_level::OXIA_LOG_DEBUG);
        assert!(oxia_log_level::OXIA_LOG_DEBUG < oxia_log_level::OXIA_LOG_TRACE);
    }

    #[test]
    fn a_zeroed_level_argument_is_off() {
        assert_eq!(oxia_log_level::OXIA_LOG_OFF as i32, 0);
        assert_eq!(oxia_log_level::from_raw(0), oxia_log_level::OXIA_LOG_OFF);
    }

    #[test]
    fn every_tracing_level_maps_across() {
        let pairs = [
            (tracing::Level::ERROR, oxia_log_level::OXIA_LOG_ERROR),
            (tracing::Level::WARN, oxia_log_level::OXIA_LOG_WARN),
            (tracing::Level::INFO, oxia_log_level::OXIA_LOG_INFO),
            (tracing::Level::DEBUG, oxia_log_level::OXIA_LOG_DEBUG),
            (tracing::Level::TRACE, oxia_log_level::OXIA_LOG_TRACE),
        ];
        for (level, expected) in pairs {
            assert_eq!(oxia_log_level::of(&level), expected);
            // And the round trip through the raw byte the static holds.
            assert_eq!(oxia_log_level::from_raw(expected as u8), expected);
        }
    }

    #[test]
    fn a_message_field_becomes_the_line_and_others_are_appended() {
        let mut formatted = Formatted::default();
        // `tracing` records the message through `record_debug` as
        // `fmt::Arguments`, whose Debug is the formatted text.
        formatted.record_debug(&field("message"), &format_args!("shard leader not found"));
        formatted.record_str(&field("shard"), "7");
        assert_eq!(formatted.0, "shard leader not found shard=7");
    }

    #[test]
    fn a_null_callback_is_rejected() {
        let mut message: *mut c_char = std::ptr::null_mut();
        let code = unsafe {
            oxia_set_log_callback(
                None,
                oxia_log_level::OXIA_LOG_WARN,
                std::ptr::null_mut(),
                &mut message,
            )
        };
        assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
        assert!(
            unsafe { std::ffi::CStr::from_ptr(message) }
                .to_string_lossy()
                .contains("callback")
        );
        unsafe { crate::oxia_string_free(message) };
    }

    #[test]
    fn setting_the_level_is_accepted_before_anything_is_installed() {
        // Harmless on its own: with no sink, nothing is delivered.
        oxia_set_log_level(oxia_log_level::OXIA_LOG_OFF);
        assert_eq!(
            MAX_LEVEL.load(Ordering::Relaxed),
            oxia_log_level::OXIA_LOG_OFF as u8
        );
    }

    // Whether a level actually reaches `tracing`'s own filter cannot be
    // asserted here: these tests never install the bridge as the global
    // subscriber, so `LevelFilter::current()` reports the default rather
    // than this module's level. `the_log_bridge_delivers_filters_and_can_be_
    // replaced` in `tests/it.rs` covers it where the bridge is installed,
    // by checking that OFF stops delivery and that raising the level
    // resumes it.

    /// Builds a `Field` to record against, which `tracing` only hands out
    /// through a callsite.
    fn field(name: &'static str) -> Field {
        struct Callsite;
        impl tracing::Callsite for Callsite {
            fn set_interest(&self, _interest: tracing::subscriber::Interest) {}
            fn metadata(&self) -> &Metadata<'_> {
                unreachable!("only the field set is used")
            }
        }
        // The field set has to outlive the `Field`, and a test's statics do.
        static NAMES: &[&str] = &["message", "shard"];
        static CALLSITE: Callsite = Callsite;
        static FIELDS: OnceLock<tracing::field::FieldSet> = OnceLock::new();
        let fields = FIELDS.get_or_init(|| {
            tracing::field::FieldSet::new(NAMES, tracing::callsite::Identifier(&CALLSITE))
        });
        fields.field(name).expect("a known field name")
    }
}
