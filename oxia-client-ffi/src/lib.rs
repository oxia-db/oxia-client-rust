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

//! A C ABI over [`oxia-client`](https://docs.rs/oxia-client), for C and C++
//! applications and as the core another language's Oxia SDK can be built on.
//!
//! The generated header is `oxia_client_ffi.h`; see `README.md` for the
//! contract in prose and `examples/example.c` for a complete program. The
//! rules the whole surface follows:
//!
//! - **Bytes, not C strings.** Keys and values cross as a pointer and a
//!   length (`oxia_slice` in, `oxia_bytes` out), because an Oxia key is
//!   a protobuf `string` — UTF-8, but allowed to contain a NUL byte — and a
//!   value is arbitrary binary. Buffers handed back are NUL-terminated past
//!   their length as a convenience, so a key without interior NULs can be
//!   printed with `%s`.
//! - **Zero means default.** Every options struct is designed so that
//!   `= {0}` asks for the plain operation with the client's defaults. Only
//!   the fields a caller sets matter.
//! - **A code and an optional message.** Every fallible function returns an
//!   `oxia_error_code` and takes a final `char **error_message`. Pass
//!   `NULL` to ignore the detail; otherwise a non-`OXIA_OK` return sets it
//!   to a message the caller frees with `oxia_string_free`.
//! - **Out-parameters are only written on success.** They are set to `NULL`
//!   on every error path, so a caller can free unconditionally.
//! - **Nothing aborts.** Null pointers, non-UTF-8 keys and malformed slices
//!   come back as `OXIA_ERR_INVALID_ARGUMENT`; a panic inside the client is
//!   caught at the boundary and returned as `OXIA_ERR_PANIC` rather than
//!   being allowed to unwind into the caller, which would end the process.
//! - **One destructor.** `oxia_client_free` is the only call that
//!   invalidates a client, and it closes it gracefully first.
//!   `oxia_client_close` is idempotent and leaves the handle valid.
//!
//! # Threads
//!
//! A client may be used from any thread, and operations submitted from
//! several threads run concurrently and batch together. Each operation comes
//! in two forms:
//!
//! - the blocking form (`oxia_client_put`) parks the calling thread until the
//!   result arrives;
//! - the asynchronous form (`oxia_client_put_async`) returns as soon as the
//!   operation is accepted and calls a callback when it finishes. It cannot
//!   wait for room the way the blocking form does, so it is refused with
//!   `OXIA_ERR_QUEUE_FULL` while the client already holds
//!   `max_in_flight_bytes` of accepted operations.
//!
//! The subscriptions and the streaming reads are pulled rather than pushed:
//! see [`streams`] for why, and for the one-request-per-item protocol behind
//! them.
//!
//! A callback runs on a dispatcher thread of the client's, never on the
//! thread that submitted the operation and never on the thread the client
//! itself runs on. It is therefore free to block and to make blocking Oxia
//! calls. Callbacks are invoked one at a time, in the order the operations
//! completed, and each is invoked exactly once for every submission that
//! returned `OXIA_OK`. Do not call `oxia_client_free` from a callback: it
//! waits for the dispatcher to drain.

// The ABI's type names are the C ones, so that the generated header reads as
// C rather than as transliterated Rust.
#![allow(non_camel_case_types)]
// Every `unsafe fn` in this crate is a boundary function whose body *is* the
// unsafe operation — dereferencing the caller's pointers, reclaiming what it
// was handed — and whose contract is stated in its `# Safety` section.
// Marking each of those operations individually would wrap practically every
// line of the crate without saying anything the function's contract does not
// already say.
#![allow(unsafe_op_in_unsafe_fn)]

mod error;
mod mem;
mod ops;
mod results;
mod runtime;
mod streams;

use std::ffi::c_char;
use std::ptr;
use std::time::Duration;

use oxia::{OxiaClient as NativeOxiaClient, OxiaClientBuilder};

pub use error::{oxia_error_code, oxia_error_is_retryable, oxia_string_free};
pub use mem::{oxia_bytes, oxia_bytes_free, oxia_slice};
pub use ops::*;
pub use results::*;
pub use runtime::oxia_client;
pub use streams::*;

use error::{fail, guard, guard_void, invalid, write_message};
use mem::{optional_str, required_bytes, required_str};

/// The binary interface this header describes.
///
/// Bumped on every change that makes a caller built against an older header
/// incompatible with a newer library — a changed signature, a renumbered
/// code, a reordered struct, and also a *field appended to one of the
/// caller-allocated options structs*, because the library would then read
/// past what an older caller allocated.
///
/// A caller that loads this library separately from the header it was built
/// against — which is every wrapper SDK shipping a prebuilt binary — should
/// compare this against `oxia_abi_version` before its first call and
/// refuse to go on if they differ. Exact equality, not "at least": while
/// this ABI is pre-1.0 there is no compatibility window to rely on.
///
/// This is not the crate's version: see `oxia_version_string` for that,
/// which moves for every release whether the ABI changed or not.
pub const OXIA_ABI_VERSION: u32 = 2;

/// The `OXIA_ABI_VERSION` this library was built with.
///
/// ```c
/// if (oxia_abi_version() != OXIA_ABI_VERSION) {
///     fprintf(stderr, "liboxia_client_ffi is ABI %u, this build expects %u\n",
///             oxia_abi_version(), OXIA_ABI_VERSION);
///     return 1;
/// }
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn oxia_abi_version() -> u32 {
    OXIA_ABI_VERSION
}

/// How a client connects and behaves.
///
/// A zeroed struct with only `service_address` set asks for the client's
/// defaults throughout: every numeric field means "the default" when it is
/// 0, and every slice means "unset" when its `data` is `NULL`.
#[repr(C)]
pub struct oxia_client_options {
    /// The address of an Oxia service, such as `"localhost:6648"`. Required.
    /// An `https://` address needs the library to have been built with TLS.
    pub service_address: oxia_slice,
    /// The namespace to operate in. Unset means `default`.
    pub namespace: oxia_slice,
    /// The identity this client claims on its session, which ephemeral
    /// records record. Unset means a generated one.
    pub identity: oxia_slice,
    /// How long an operation may take before it fails with
    /// `OXIA_ERR_TIMEOUT`, in milliseconds.
    pub request_timeout_ms: u64,
    /// How long the server keeps this client's session alive without a
    /// keep-alive, in milliseconds.
    pub session_timeout_ms: u64,
    /// How often to send a session keep-alive, in milliseconds. The default
    /// is a tenth of the session timeout.
    pub session_keep_alive_ms: u64,
    /// The soft byte limit of a write batch.
    pub batch_max_size: u32,
    /// The most operations to put in one batch.
    pub max_requests_per_batch: u32,
    /// The most write batches to keep in flight per shard.
    pub max_write_batches_in_flight: u32,
    /// The most read batches to keep in flight per shard.
    pub max_read_batches_in_flight: u32,
    /// A bearer token to authenticate with, sent on every request. Unset
    /// means no authentication.
    pub auth_token: oxia_slice,
    /// Connect with TLS, verifying the server against the operating
    /// system's trust roots. Implied by any of the `tls_` fields below.
    pub tls: bool,
    /// PEM-encoded CA certificates to verify the server against, instead of
    /// the operating system's trust roots.
    pub tls_trusted_ca_pem: oxia_slice,
    /// A PEM-encoded certificate to present as this client's identity, for
    /// mutual TLS. Requires `tls_client_key_pem`.
    pub tls_client_cert_pem: oxia_slice,
    /// The PEM-encoded private key of `tls_client_cert_pem`.
    pub tls_client_key_pem: oxia_slice,
    /// The name to verify the server's certificate against, and to send as
    /// SNI, instead of the host in `service_address`.
    pub tls_domain_name: oxia_slice,
    /// The most bytes to hold for the operations in flight, keys and values
    /// together, before the client pushes back. The default is 64 MiB.
    ///
    /// It bounds both call forms, in the way each can be bounded. A blocking
    /// operation waits for room, so it simply takes longer; an asynchronous
    /// one is refused with `OXIA_ERR_QUEUE_FULL`, because it has to answer
    /// before the operation has been admitted. A refused submission does
    /// not call back, and room frees up as the callbacks of the accepted
    /// ones run.
    ///
    /// `0` asks for the default, as every numeric field here does, so there
    /// is no way to ask for no limit at all: set a very large value for
    /// that.
    pub max_in_flight_bytes: u64,
}

/// Turns the options into a builder, together with the byte budget the
/// asynchronous submissions are held to, or explains what was wrong with
/// them.
unsafe fn builder_from(
    options: *const oxia_client_options,
) -> Result<(OxiaClientBuilder, usize), String> {
    let options = options.as_ref().ok_or("options is NULL")?;
    let mut builder = NativeOxiaClient::builder()
        .service_address(required_str(options.service_address, "service_address")?.to_string());
    if let Some(namespace) = optional_str(options.namespace, "namespace")? {
        builder = builder.namespace(namespace.to_string());
    }
    if let Some(identity) = optional_str(options.identity, "identity")? {
        builder = builder.identity(identity.to_string());
    }
    if options.request_timeout_ms > 0 {
        builder = builder.request_timeout(Duration::from_millis(options.request_timeout_ms));
    }
    if options.session_timeout_ms > 0 {
        builder = builder.session_timeout(Duration::from_millis(options.session_timeout_ms));
    }
    if options.session_keep_alive_ms > 0 {
        builder = builder.session_keep_alive(Duration::from_millis(options.session_keep_alive_ms));
    }
    if options.batch_max_size > 0 {
        builder = builder.batch_max_size(options.batch_max_size);
    }
    if options.max_requests_per_batch > 0 {
        builder = builder.max_requests_per_batch(options.max_requests_per_batch);
    }
    if options.max_write_batches_in_flight > 0 {
        builder = builder.max_write_batches_in_flight(options.max_write_batches_in_flight);
    }
    if options.max_read_batches_in_flight > 0 {
        builder = builder.max_read_batches_in_flight(options.max_read_batches_in_flight);
    }
    let max_in_flight_bytes = if options.max_in_flight_bytes > 0 {
        builder = builder.max_in_flight_bytes(options.max_in_flight_bytes);
        options.max_in_flight_bytes
    } else {
        // `0` means the client's default here, not "no limit". The default
        // has to be resolved rather than left to the builder, because the
        // asynchronous submissions are held to the same number.
        oxia::DEFAULT_MAX_IN_FLIGHT_BYTES
    };
    if let Some(token) = optional_str(options.auth_token, "auth_token")? {
        builder = builder.auth_token(token.to_string());
    }
    builder = apply_tls(builder, options)?;
    Ok((
        builder,
        usize::try_from(max_in_flight_bytes).unwrap_or(usize::MAX),
    ))
}

#[cfg(feature = "tls")]
unsafe fn apply_tls(
    builder: OxiaClientBuilder,
    options: &oxia_client_options,
) -> Result<OxiaClientBuilder, String> {
    let ca = required_bytes(options.tls_trusted_ca_pem, "tls_trusted_ca_pem")?;
    let cert = required_bytes(options.tls_client_cert_pem, "tls_client_cert_pem")?;
    let key = required_bytes(options.tls_client_key_pem, "tls_client_key_pem")?;
    let domain = optional_str(options.tls_domain_name, "tls_domain_name")?;
    let configured = !ca.is_empty() || !cert.is_empty() || !key.is_empty() || domain.is_some();
    if !options.tls && !configured {
        return Ok(builder);
    }
    if cert.is_empty() != key.is_empty() {
        return Err("tls_client_cert_pem and tls_client_key_pem must be set together".to_string());
    }
    let mut tls = oxia::TlsOptions::new();
    if !ca.is_empty() {
        tls = tls.trusted_ca_pem(ca.to_vec());
    }
    if !cert.is_empty() {
        tls = tls.identity_pem(cert.to_vec(), key.to_vec());
    }
    if let Some(domain) = domain {
        tls = tls.domain_name(domain.to_string());
    }
    Ok(builder.tls(tls))
}

#[cfg(not(feature = "tls"))]
unsafe fn apply_tls(
    builder: OxiaClientBuilder,
    options: &oxia_client_options,
) -> Result<OxiaClientBuilder, String> {
    let configured = options.tls
        || !required_bytes(options.tls_trusted_ca_pem, "tls_trusted_ca_pem")?.is_empty()
        || !required_bytes(options.tls_client_cert_pem, "tls_client_cert_pem")?.is_empty()
        || !required_bytes(options.tls_client_key_pem, "tls_client_key_pem")?.is_empty()
        || optional_str(options.tls_domain_name, "tls_domain_name")?.is_some();
    if configured {
        return Err(
            "TLS was requested but this library was built without the `tls` feature".to_string(),
        );
    }
    Ok(builder)
}

/// Connects a client, blocking until it is ready to use.
///
/// Fails fast — within the request timeout — when the cluster is
/// unreachable, the namespace does not exist, or the first shard assignments
/// cannot be obtained. On `OXIA_OK`, `*client` owns a client the caller
/// destroys with `oxia_client_free`; on any error it is set to `NULL`.
///
/// # Safety
///
/// `options` must point at an initialized `oxia_client_options` whose
/// slices are valid for the call. `client` must point at a writable pointer,
/// and `error_message` must be `NULL` or point at one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_new(
    options: *const oxia_client_options,
    client: *mut *mut oxia_client,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        if !client.is_null() {
            *client = ptr::null_mut();
        } else {
            return invalid(error_message, "the client out-parameter is NULL");
        }
        let (builder, max_in_flight_bytes) = match builder_from(options) {
            Ok(built) => built,
            Err(text) => return invalid(error_message, text),
        };
        match oxia_client::start(builder, max_in_flight_bytes) {
            Ok(started) => {
                *client = Box::into_raw(Box::new(started));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Flushes the client's pending writes and shuts it down, blocking until it
/// has finished.
///
/// Idempotent, and the handle stays valid: a later operation on it fails
/// with `OXIA_ERR_CLOSED`, and `oxia_client_free` still has to be called
/// to release it. Closing explicitly is how a caller learns that the final
/// flush succeeded — `oxia_client_free` closes too, but has no way to
/// report a failure.
///
/// # Safety
///
/// `client` must be a client from `oxia_client_new` that has not been
/// freed. `error_message` must be `NULL` or point at a writable `char *`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_close(
    client: *mut oxia_client,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let Some(client) = client.as_ref() else {
            return invalid(error_message, "client is NULL");
        };
        match client.close() {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Destroys a client, closing it first if it is still open.
///
/// The only call that invalidates the handle, and safe to call after
/// `oxia_client_close`. Does nothing when `client` is `NULL`. Waits for
/// the operations in flight to settle and for every pending callback to run,
/// so it must not be called from a callback.
///
/// # Safety
///
/// `client` must be `NULL`, or a client from `oxia_client_new` that has
/// not been freed yet. No operation on it may be in progress on another
/// thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_free(client: *mut oxia_client) {
    guard_void(|| {
        if !client.is_null() {
            drop(Box::from_raw(client));
        }
    });
}

/// Writes this library's version, as `MAJOR.MINOR.PATCH`, to `out`.
///
/// Useful to a wrapper SDK that wants to report the core it is built on.
/// The caller frees the string with `oxia_string_free`.
///
/// # Safety
///
/// `out` must be `NULL` or point at a writable `char *`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_version_string(out: *mut *mut c_char) {
    guard_void(|| write_message(out, env!("CARGO_PKG_VERSION")));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    fn slice(text: &'static str) -> oxia_slice {
        oxia_slice {
            data: text.as_ptr(),
            len: text.len(),
        }
    }

    fn options() -> oxia_client_options {
        oxia_client_options {
            service_address: slice("localhost:6648"),
            namespace: oxia_slice::none(),
            identity: oxia_slice::none(),
            request_timeout_ms: 0,
            session_timeout_ms: 0,
            session_keep_alive_ms: 0,
            batch_max_size: 0,
            max_requests_per_batch: 0,
            max_write_batches_in_flight: 0,
            max_read_batches_in_flight: 0,
            auth_token: oxia_slice::none(),
            tls: false,
            tls_trusted_ca_pem: oxia_slice::none(),
            tls_client_cert_pem: oxia_slice::none(),
            tls_client_key_pem: oxia_slice::none(),
            tls_domain_name: oxia_slice::none(),
            max_in_flight_bytes: 0,
        }
    }

    #[test]
    fn the_minimal_options_build_a_client_builder() {
        assert!(unsafe { builder_from(&options()) }.is_ok());
    }

    #[test]
    fn a_zero_memory_cap_resolves_to_the_clients_own_default() {
        let (_, budget) = unsafe { builder_from(&options()) }.unwrap();
        assert_eq!(budget as u64, oxia::DEFAULT_MAX_IN_FLIGHT_BYTES);
    }

    #[test]
    fn a_memory_cap_is_the_budget_the_submissions_are_held_to() {
        let mut capped = options();
        capped.max_in_flight_bytes = 4096;
        let (_, budget) = unsafe { builder_from(&capped) }.unwrap();
        assert_eq!(budget, 4096);
    }

    #[test]
    fn null_options_are_rejected() {
        let err = unsafe { builder_from(ptr::null()) }.unwrap_err();
        assert!(err.contains("options"), "{err}");
    }

    #[test]
    fn a_missing_service_address_names_itself() {
        let mut bare = options();
        bare.service_address = oxia_slice::none();
        let err = unsafe { builder_from(&bare) }.unwrap_err();
        assert!(err.contains("service_address"), "{err}");
    }

    #[test]
    fn every_option_is_accepted_together() {
        let mut full = options();
        full.namespace = slice("ns");
        full.identity = slice("worker-1");
        full.request_timeout_ms = 1_000;
        full.session_timeout_ms = 30_000;
        full.session_keep_alive_ms = 3_000;
        full.batch_max_size = 1024;
        full.max_requests_per_batch = 16;
        full.max_write_batches_in_flight = 4;
        full.max_read_batches_in_flight = 4;
        full.auth_token = slice("token");
        full.max_in_flight_bytes = 8 * 1024 * 1024;
        assert!(unsafe { builder_from(&full) }.is_ok());
    }

    #[test]
    fn a_non_utf8_service_address_is_rejected_rather_than_panicking() {
        let raw = [b'h', 0xff];
        let mut bad = options();
        bad.service_address = oxia_slice {
            data: raw.as_ptr(),
            len: raw.len(),
        };
        let err = unsafe { builder_from(&bad) }.unwrap_err();
        assert!(err.contains("UTF-8"), "{err}");
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_client_certificate_without_its_key_is_rejected() {
        let mut half = options();
        half.tls_client_cert_pem = slice("-----BEGIN CERTIFICATE-----");
        let err = unsafe { builder_from(&half) }.unwrap_err();
        assert!(err.contains("must be set together"), "{err}");
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn asking_for_tls_without_the_feature_explains_itself() {
        let mut secure = options();
        secure.tls = true;
        let err = unsafe { builder_from(&secure) }.unwrap_err();
        assert!(err.contains("`tls` feature"), "{err}");
    }

    #[test]
    fn connecting_without_an_out_parameter_is_rejected() {
        let mut message: *mut c_char = ptr::null_mut();
        let code = unsafe { oxia_client_new(&options(), ptr::null_mut(), &mut message) };
        assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
        unsafe { oxia_string_free(message) };
    }

    #[test]
    fn closing_a_null_client_is_an_invalid_argument_not_a_crash() {
        let mut message: *mut c_char = ptr::null_mut();
        let code = unsafe { oxia_client_close(ptr::null_mut(), &mut message) };
        assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
        unsafe { oxia_string_free(message) };
    }

    #[test]
    fn freeing_a_null_client_is_a_no_op() {
        unsafe { oxia_client_free(ptr::null_mut()) };
    }

    #[test]
    fn the_abi_version_matches_the_constant_the_header_carries() {
        // The point of the pair: a wrapper compares the constant it compiled
        // against with what the library it loaded reports.
        assert_eq!(oxia_abi_version(), OXIA_ABI_VERSION);
    }

    #[test]
    fn the_version_string_is_the_crate_version() {
        let mut version: *mut c_char = ptr::null_mut();
        unsafe { oxia_version_string(&mut version) };
        let text = unsafe { CStr::from_ptr(version) }.to_str().unwrap();
        assert_eq!(text, env!("CARGO_PKG_VERSION"));
        unsafe { oxia_string_free(version) };
    }
}
