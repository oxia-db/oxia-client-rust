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

//! The error code every fallible entry point returns, and the optional
//! message that carries the detail the code cannot.

use std::ffi::{CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};

use oxia::OxiaError;

/// The outcome of an operation.
///
/// Codes are stable: a new one is appended rather than renumbering the
/// others, and an error this version of the library does not know maps to
/// [`OXIA_ERR_OTHER`](oxia_error_code::OXIA_ERR_OTHER). Pass an
/// `error_message` out-parameter to recover the detail a code leaves out —
/// which argument was invalid, which gRPC status the server returned.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum oxia_error_code {
    /// The operation succeeded.
    OXIA_OK = 0,
    /// The requested key does not exist.
    OXIA_ERR_KEY_NOT_FOUND = 1,
    /// A conditional operation found a different version than it expected.
    OXIA_ERR_UNEXPECTED_VERSION_ID = 2,
    /// The session backing an ephemeral record has expired or been closed.
    OXIA_ERR_SESSION_EXPIRED = 3,
    /// Kept for compatibility; the server now reports an oversized write as a
    /// gRPC status.
    OXIA_ERR_REQUEST_TOO_LARGE = 4,
    /// An argument or option was invalid. The message says which.
    OXIA_ERR_INVALID_ARGUMENT = 5,
    /// No leader is available for the shard, e.g. during an election.
    /// Retryable.
    OXIA_ERR_LEADER_NOT_FOUND = 6,
    /// No shard owns the key yet, because assignments are still loading.
    /// Retryable.
    OXIA_ERR_NO_SHARD_FOR_KEY = 7,
    /// A connection was lost or could not be established. Retryable.
    OXIA_ERR_DISCONNECTED = 8,
    /// The operation did not finish before its deadline.
    OXIA_ERR_TIMEOUT = 9,
    /// The server returned a gRPC status. The message carries the code and
    /// the server's text.
    OXIA_ERR_GRPC = 10,
    /// A server response could not be decoded: a protocol mismatch or a
    /// server bug.
    OXIA_ERR_DECODE = 11,
    /// The client has been closed and can no longer be used.
    OXIA_ERR_CLOSED = 12,
    /// An error this version of the library does not recognize. The message
    /// carries its text.
    OXIA_ERR_OTHER = 13,
    /// A shard's leader does not support an inclusive end of range.
    OXIA_ERR_END_INCLUSIVE_UNSUPPORTED = 14,
    /// The shard was split or merged while the operation targeted it.
    /// Retryable.
    OXIA_ERR_SHARD_MOVED = 15,
    /// A bug in the library: a panic was caught at the boundary rather than
    /// being allowed to unwind into the caller, which would end the process.
    /// The message carries the panic text; please report it.
    OXIA_ERR_PANIC = 16,
}

impl From<&OxiaError> for oxia_error_code {
    fn from(error: &OxiaError) -> Self {
        match error {
            OxiaError::KeyNotFound => oxia_error_code::OXIA_ERR_KEY_NOT_FOUND,
            OxiaError::UnexpectedVersionId => oxia_error_code::OXIA_ERR_UNEXPECTED_VERSION_ID,
            OxiaError::SessionExpired => oxia_error_code::OXIA_ERR_SESSION_EXPIRED,
            OxiaError::RequestTooLarge => oxia_error_code::OXIA_ERR_REQUEST_TOO_LARGE,
            OxiaError::InvalidArgument(_) => oxia_error_code::OXIA_ERR_INVALID_ARGUMENT,
            OxiaError::EndInclusiveUnsupported => {
                oxia_error_code::OXIA_ERR_END_INCLUSIVE_UNSUPPORTED
            }
            OxiaError::LeaderNotFound { .. } => oxia_error_code::OXIA_ERR_LEADER_NOT_FOUND,
            OxiaError::NoShardForKey { .. } => oxia_error_code::OXIA_ERR_NO_SHARD_FOR_KEY,
            OxiaError::Disconnected(_) => oxia_error_code::OXIA_ERR_DISCONNECTED,
            OxiaError::Timeout => oxia_error_code::OXIA_ERR_TIMEOUT,
            OxiaError::Grpc { .. } => oxia_error_code::OXIA_ERR_GRPC,
            OxiaError::Decode(_) => oxia_error_code::OXIA_ERR_DECODE,
            OxiaError::ShardMoved => oxia_error_code::OXIA_ERR_SHARD_MOVED,
            OxiaError::Closed => oxia_error_code::OXIA_ERR_CLOSED,
            // `OxiaError` is #[non_exhaustive]; a variant added upstream
            // surfaces with its text rather than as a new code.
            _ => oxia_error_code::OXIA_ERR_OTHER,
        }
    }
}

/// Whether retrying an operation that failed with `code` might succeed.
///
/// Mirrors `OxiaError::is_retryable` in the Rust client and the retryable set
/// of the reference Go client. The client already retries internally within
/// each operation's timeout, so a retryable code means the whole operation
/// may be worth submitting again.
///
/// [`OXIA_ERR_GRPC`](oxia_error_code::OXIA_ERR_GRPC) answers `false`: whether
/// a gRPC status is retryable depends on the status code, which only the
/// message carries.
#[unsafe(no_mangle)]
pub extern "C" fn oxia_error_is_retryable(code: oxia_error_code) -> bool {
    matches!(
        code,
        oxia_error_code::OXIA_ERR_LEADER_NOT_FOUND
            | oxia_error_code::OXIA_ERR_NO_SHARD_FOR_KEY
            | oxia_error_code::OXIA_ERR_SHARD_MOVED
            | oxia_error_code::OXIA_ERR_DISCONNECTED
    )
}

/// Frees a message an `error_message` out-parameter was set to.
///
/// Does nothing when `message` is `NULL`, so it is safe to call
/// unconditionally.
///
/// # Safety
///
/// `message` must have been set by this library and not yet been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_string_free(message: *mut c_char) {
    if !message.is_null() {
        drop(CString::from_raw(message));
    }
}

/// Writes `text` to an `error_message` out-parameter, when the caller asked
/// for one.
///
/// # Safety
///
/// `out` must be `NULL` or point at a writable `char *`.
pub unsafe fn write_message(out: *mut *mut c_char, text: &str) {
    if out.is_null() {
        return;
    }
    // A message with an interior NUL cannot be a C string; the client never
    // produces one, but a server-supplied gRPC message could, so sanitize
    // rather than fail or panic.
    let message = match CString::new(text) {
        Ok(message) => message,
        Err(_) => CString::new(text.replace('\0', "\\0")).unwrap_or_default(),
    };
    *out = message.into_raw();
}

/// Reports a failure: the code to return, with its text in `out`.
///
/// # Safety
///
/// As [`write_message`].
pub unsafe fn fail(out: *mut *mut c_char, error: &OxiaError) -> oxia_error_code {
    write_message(out, &error.to_string());
    oxia_error_code::from(error)
}

/// Reports an argument the boundary itself rejected.
///
/// # Safety
///
/// As [`write_message`].
pub unsafe fn invalid(out: *mut *mut c_char, text: impl AsRef<str>) -> oxia_error_code {
    write_message(out, text.as_ref());
    oxia_error_code::OXIA_ERR_INVALID_ARGUMENT
}

/// Runs an entry point's body, turning a panic into
/// [`OXIA_ERR_PANIC`](oxia_error_code::OXIA_ERR_PANIC).
///
/// Every `extern "C"` function in this crate goes through here. A panic that
/// reached the boundary would otherwise abort the process, which for a
/// library driving another language's SDK means taking the host down with no
/// diagnostic.
///
/// # Safety
///
/// `out` must be `NULL` or point at a writable `char *`.
pub unsafe fn guard(
    out: *mut *mut c_char,
    body: impl FnOnce() -> oxia_error_code,
) -> oxia_error_code {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(code) => code,
        Err(payload) => {
            let text = panic_text(&*payload);
            write_message(out, &format!("panic in the Oxia client: {text}"));
            oxia_error_code::OXIA_ERR_PANIC
        }
    }
}

/// Runs a body that has nothing to report, swallowing a panic. Used by the
/// `void` entry points, where there is no code to return.
pub fn guard_void(body: impl FnOnce()) {
    let _ = catch_unwind(AssertUnwindSafe(body));
}

/// The text of a caught panic payload.
pub fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        return (*text).to_string();
    }
    if let Some(text) = payload.downcast_ref::<String>() {
        return text.clone();
    }
    "unknown panic".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::ptr;

    #[test]
    fn every_error_variant_maps_to_its_own_code() {
        let cases: Vec<(OxiaError, oxia_error_code)> = vec![
            (
                OxiaError::KeyNotFound,
                oxia_error_code::OXIA_ERR_KEY_NOT_FOUND,
            ),
            (
                OxiaError::UnexpectedVersionId,
                oxia_error_code::OXIA_ERR_UNEXPECTED_VERSION_ID,
            ),
            (
                OxiaError::SessionExpired,
                oxia_error_code::OXIA_ERR_SESSION_EXPIRED,
            ),
            (
                OxiaError::RequestTooLarge,
                oxia_error_code::OXIA_ERR_REQUEST_TOO_LARGE,
            ),
            (
                OxiaError::InvalidArgument("x".into()),
                oxia_error_code::OXIA_ERR_INVALID_ARGUMENT,
            ),
            (
                OxiaError::EndInclusiveUnsupported,
                oxia_error_code::OXIA_ERR_END_INCLUSIVE_UNSUPPORTED,
            ),
            (
                OxiaError::LeaderNotFound { shard: 1 },
                oxia_error_code::OXIA_ERR_LEADER_NOT_FOUND,
            ),
            (
                OxiaError::NoShardForKey { key: "k".into() },
                oxia_error_code::OXIA_ERR_NO_SHARD_FOR_KEY,
            ),
            (
                OxiaError::Disconnected("x".into()),
                oxia_error_code::OXIA_ERR_DISCONNECTED,
            ),
            (OxiaError::Timeout, oxia_error_code::OXIA_ERR_TIMEOUT),
            (
                OxiaError::Grpc {
                    code: tonic::Code::Internal,
                    message: "boom".into(),
                },
                oxia_error_code::OXIA_ERR_GRPC,
            ),
            (
                OxiaError::Decode("x".into()),
                oxia_error_code::OXIA_ERR_DECODE,
            ),
            (OxiaError::ShardMoved, oxia_error_code::OXIA_ERR_SHARD_MOVED),
            (OxiaError::Closed, oxia_error_code::OXIA_ERR_CLOSED),
        ];
        for (error, expected) in cases {
            assert_eq!(oxia_error_code::from(&error), expected, "{error}");
        }
    }

    #[test]
    fn the_retryable_set_matches_the_clients() {
        for error in [
            OxiaError::LeaderNotFound { shard: 1 },
            OxiaError::NoShardForKey { key: "k".into() },
            OxiaError::ShardMoved,
            OxiaError::Disconnected("x".into()),
        ] {
            assert!(error.is_retryable(), "{error}");
            assert!(
                oxia_error_is_retryable(oxia_error_code::from(&error)),
                "{error}"
            );
        }
        for error in [
            OxiaError::KeyNotFound,
            OxiaError::Timeout,
            OxiaError::Closed,
        ] {
            assert!(!error.is_retryable(), "{error}");
            assert!(
                !oxia_error_is_retryable(oxia_error_code::from(&error)),
                "{error}"
            );
        }
    }

    #[test]
    fn a_failure_carries_its_detail_in_the_message() {
        let mut message: *mut c_char = ptr::null_mut();
        let code = unsafe { fail(&mut message, &OxiaError::InvalidArgument("bad key".into())) };
        assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
        let text = unsafe { CStr::from_ptr(message) }.to_str().unwrap();
        assert!(text.contains("bad key"), "{text}");
        unsafe { oxia_string_free(message) };
    }

    #[test]
    fn a_caller_that_wants_no_message_passes_null() {
        let code = unsafe { fail(ptr::null_mut(), &OxiaError::Timeout) };
        assert_eq!(code, oxia_error_code::OXIA_ERR_TIMEOUT);
    }

    #[test]
    fn a_message_with_an_interior_nul_is_sanitized_not_dropped() {
        let mut message: *mut c_char = ptr::null_mut();
        unsafe { write_message(&mut message, "before\0after") };
        let text = unsafe { CStr::from_ptr(message) }.to_str().unwrap();
        assert_eq!(text, "before\\0after");
        unsafe { oxia_string_free(message) };
    }

    #[test]
    fn a_panic_becomes_a_code_instead_of_an_abort() {
        let mut message: *mut c_char = ptr::null_mut();
        let code = unsafe { guard(&mut message, || panic!("deliberate")) };
        assert_eq!(code, oxia_error_code::OXIA_ERR_PANIC);
        let text = unsafe { CStr::from_ptr(message) }.to_str().unwrap();
        assert!(text.contains("deliberate"), "{text}");
        unsafe { oxia_string_free(message) };
    }

    #[test]
    fn freeing_a_null_message_is_a_no_op() {
        unsafe { oxia_string_free(ptr::null_mut()) };
    }
}
