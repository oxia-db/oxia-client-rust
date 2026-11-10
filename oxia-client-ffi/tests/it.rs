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

//! The ABI, exercised through the same entry points a C caller uses, against
//! a real Oxia server.
//!
//! The crate is built as an `rlib` as well as a shared library so that these
//! tests can call the `extern "C"` functions directly: the arguments are the
//! same raw pointers, so a test here covers the same path a C program takes,
//! without a C toolchain in the loop. `examples/example.c` covers the header
//! itself.

#![allow(non_camel_case_types)]
// As in the crate itself: these tests are all boundary calls.
#![allow(unsafe_op_in_unsafe_fn)]

use std::ffi::{CStr, c_char, c_void};
use std::ptr;
use std::slice;
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Duration;

use oxia_client_ffi::*;
use testcontainers::core::ports::ContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const OXIA_PORT: u16 = 6648;

/// One standalone server for the whole binary: these tests keep to their own
/// key prefixes, so they do not need a server each.
fn address() -> &'static str {
    static SERVER: OnceLock<(Container<GenericImage>, String)> = OnceLock::new();
    &SERVER
        .get_or_init(|| {
            // `nightly-beta` is built daily from Oxia's main branch, as in
            // oxia-client's own tests; CI pre-pulls it.
            let image = std::env::var("OXIA_IMAGE").unwrap_or_else(|_| "oxia/oxia".to_string());
            let tag = std::env::var("OXIA_TAG").unwrap_or_else(|_| "nightly-beta".to_string());
            let container = GenericImage::new(image, tag)
                .with_exposed_port(ContainerPort::Tcp(OXIA_PORT))
                .with_wait_for(WaitFor::message_on_stdout("Started Grpc server"))
                .with_cmd(["oxia", "standalone"])
                .start()
                .expect("failed to start the Oxia container");
            let mut port = container.get_host_port_ipv4(OXIA_PORT);
            for _ in 0..20 {
                if port.is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
                port = container.get_host_port_ipv4(OXIA_PORT);
            }
            let address = format!("http://127.0.0.1:{}", port.expect("no host port"));
            (container, address)
        })
        .1
}

// ===========================================================================
// Helpers
// ===========================================================================

/// What `= {0}` means in C. Every options struct is designed so that this is
/// the plain operation with the client's defaults, which these tests rely on
/// exactly as a C caller would.
fn zeroed<T>() -> T {
    unsafe { std::mem::zeroed() }
}

fn slice_of(bytes: &[u8]) -> oxia_slice {
    oxia_slice {
        data: bytes.as_ptr(),
        len: bytes.len(),
    }
}

/// The items of an array the ABI handed back; empty when it holds none.
unsafe fn items<T>(data: *const T, len: usize) -> &'static [T] {
    if data.is_null() || len == 0 {
        &[]
    } else {
        slice::from_raw_parts(data, len)
    }
}

/// The bytes of an owned buffer, or `None` when the field is absent.
unsafe fn owned(buffer: oxia_bytes) -> Option<Vec<u8>> {
    if buffer.data.is_null() {
        None
    } else {
        Some(slice::from_raw_parts(buffer.data, buffer.len).to_vec())
    }
}

/// Takes an error message out-parameter, freeing it.
unsafe fn take_message(message: *mut c_char) -> String {
    assert!(!message.is_null(), "the error carried no message");
    let text = CStr::from_ptr(message).to_string_lossy().into_owned();
    oxia_string_free(message);
    text
}

/// An open client against the shared server, with a short request timeout so
/// a mistake fails fast.
fn client() -> *mut oxia_client {
    let address = address().as_bytes();
    let mut options: oxia_client_options = zeroed();
    options.service_address = slice_of(address);
    options.request_timeout_ms = 10_000;
    let mut client: *mut oxia_client = ptr::null_mut();
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_new(&options, &mut client, &mut message) };
    assert_eq!(
        code,
        oxia_error_code::OXIA_OK,
        "connect failed: {}",
        unsafe { take_message(message) }
    );
    assert!(!client.is_null());
    client
}

/// Stores a value, asserting that it worked, and returns its version id.
fn put(client: *const oxia_client, key: &[u8], value: &[u8]) -> i64 {
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(key);
    options.value = slice_of(value);
    let mut result: *mut oxia_put_result = ptr::null_mut();
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_put(client, &options, &mut result, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_OK, "put failed: {}", unsafe {
        take_message(message)
    });
    let version_id = unsafe { (*result).version.version_id };
    unsafe { oxia_put_result_free(result) };
    version_id
}

/// Reads a key with the default options.
fn get(client: *const oxia_client, key: &[u8]) -> Result<*mut oxia_get_result, oxia_error_code> {
    let mut options: oxia_get_options = zeroed();
    options.key = slice_of(key);
    let mut result: *mut oxia_get_result = ptr::null_mut();
    let code = unsafe { oxia_client_get(client, &options, &mut result, ptr::null_mut()) };
    if code == oxia_error_code::OXIA_OK {
        Ok(result)
    } else {
        assert!(
            result.is_null(),
            "a failed get must clear its out-parameter"
        );
        Err(code)
    }
}

// ===========================================================================
// The crashes the previous ABI had
// ===========================================================================

#[test]
fn a_non_utf8_key_is_rejected_instead_of_aborting() {
    let client = client();
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(&[b'k', 0xff, 0xfe]);
    options.value = slice_of(b"x");
    let mut result: *mut oxia_put_result = ptr::null_mut();
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_put(client, &options, &mut result, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
    assert!(result.is_null());
    let text = unsafe { take_message(message) };
    assert!(text.contains("UTF-8"), "{text}");
    unsafe { oxia_client_free(client) };
}

#[test]
fn null_arguments_are_rejected_instead_of_segfaulting() {
    let client = client();
    let mut message: *mut c_char = ptr::null_mut();

    // No client.
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"null/a");
    let mut result: *mut oxia_put_result = ptr::null_mut();
    let code = unsafe { oxia_client_put(ptr::null(), &options, &mut result, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
    assert!(unsafe { take_message(message) }.contains("client"));

    // No options.
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_put(client, ptr::null(), &mut result, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
    assert!(unsafe { take_message(message) }.contains("options"));

    // No out-parameter.
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_put(client, &options, ptr::null_mut(), &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
    assert!(unsafe { take_message(message) }.contains("out-parameter"));

    // A key whose pointer is null but whose length is not.
    let mut message: *mut c_char = ptr::null_mut();
    let mut bogus: oxia_put_options = zeroed();
    bogus.key = oxia_slice {
        data: ptr::null(),
        len: 7,
    };
    let code = unsafe { oxia_client_put(client, &bogus, &mut result, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
    assert!(unsafe { take_message(message) }.contains("key"));

    unsafe { oxia_client_free(client) };
}

#[test]
fn an_empty_value_may_be_passed_as_a_null_pointer() {
    let client = client();
    // The natural way to say "no bytes" in C, and undefined behaviour in the
    // previous ABI.
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"empty/null-pointer");
    options.value = oxia_slice {
        data: ptr::null(),
        len: 0,
    };
    let mut result: *mut oxia_put_result = ptr::null_mut();
    let code = unsafe { oxia_client_put(client, &options, &mut result, ptr::null_mut()) };
    assert_eq!(code, oxia_error_code::OXIA_OK);
    unsafe { oxia_put_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn closing_then_freeing_a_client_is_the_supported_order() {
    let client = client();
    put(client, b"close/a", b"v");
    let mut message: *mut c_char = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_close(client, &mut message) },
        oxia_error_code::OXIA_OK
    );
    // Idempotent.
    assert_eq!(
        unsafe { oxia_client_close(client, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    // The handle is still valid, and operations on it report that it closed.
    assert_eq!(
        get(client, b"close/a"),
        Err(oxia_error_code::OXIA_ERR_CLOSED)
    );
    // And freeing afterwards is not a double free.
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_failed_connection_clears_the_client_out_parameter() {
    let mut options: oxia_client_options = zeroed();
    options.service_address = slice_of(b"127.0.0.1:1");
    options.request_timeout_ms = 2_000;
    // A sentinel, as an uninitialized local would hold.
    let mut client = 0xdead_beef_usize as *mut oxia_client;
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_new(&options, &mut client, &mut message) };
    assert_ne!(code, oxia_error_code::OXIA_OK);
    assert!(client.is_null(), "a failed connect must clear the handle");
    assert!(!unsafe { take_message(message) }.is_empty());
}

// ===========================================================================
// Data that the previous ABI could not represent
// ===========================================================================

#[test]
fn a_key_holding_a_nul_byte_round_trips() {
    let client = client();
    let key = b"nul/a\0b";
    put(client, key, b"value");

    let result = get(client, key).expect("get");
    let read_key = unsafe { owned((*result).key) }.unwrap();
    assert_eq!(read_key, key, "the key came back truncated at the NUL");
    unsafe { oxia_get_result_free(result) };

    // And it is listed with its full length.
    let mut options: oxia_range_options = zeroed();
    options.min_key_inclusive = slice_of(b"nul/");
    options.max_key_exclusive = slice_of(b"nul/~");
    let mut list: *mut oxia_key_list = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_list(client, &options, &mut list, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    let keys = unsafe { items((*list).keys, (*list).len) };
    let listed: Vec<Vec<u8>> = keys.iter().map(|k| unsafe { owned(*k) }.unwrap()).collect();
    assert!(listed.contains(&key.to_vec()), "{listed:?}");
    unsafe { oxia_key_list_free(list) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_binary_value_round_trips_byte_for_byte() {
    let client = client();
    let value: Vec<u8> = (0u8..=255).collect();
    put(client, b"binary/a", &value);
    let result = get(client, b"binary/a").expect("get");
    assert!(unsafe { (*result).has_value });
    assert_eq!(unsafe { owned((*result).value) }.unwrap(), value);
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn has_value_tells_a_value_apart_from_one_that_was_not_asked_for() {
    let client = client();
    put(client, b"has-value/empty", b"v");

    // A value that is there.
    let result = get(client, b"has-value/empty").expect("get");
    assert!(unsafe { (*result).has_value });
    assert_eq!(unsafe { owned((*result).value) }.unwrap(), b"v");
    unsafe { oxia_get_result_free(result) };

    // Asked for without its value.
    let mut options: oxia_get_options = zeroed();
    options.key = slice_of(b"has-value/empty");
    options.exclude_value = true;
    let mut result: *mut oxia_get_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_get(client, &options, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    assert!(!unsafe { (*result).has_value });
    assert!(unsafe { (*result).value.data.is_null() });
    assert!(unsafe { (*result).version.version_id } >= 0);
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

/// A stored empty value is not distinguishable from no value at all, because
/// the server does not report one: it leaves the field out of the response
/// exactly as it does for a record that carries no value. Worth pinning down,
/// since it is the kind of difference a conformance suite across SDKs trips
/// over.
#[test]
fn a_stored_empty_value_comes_back_as_no_value() {
    let client = client();
    put(client, b"empty/stored", b"");
    let result = get(client, b"empty/stored").expect("get");
    assert!(!unsafe { (*result).has_value });
    assert_eq!(unsafe { (*result).value.len }, 0);
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn an_owned_key_is_nul_terminated_for_printf() {
    let client = client();
    put(client, b"printf/a", b"v");
    let result = get(client, b"printf/a").expect("get");
    let key = unsafe { (*result).key };
    // One past the length, as `%s` would read.
    assert_eq!(unsafe { *key.data.add(key.len) }, 0);
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_version_crosses_over_whole() {
    let client = client();
    put(client, b"version/a", b"first");
    put(client, b"version/a", b"second");
    let result = get(client, b"version/a").expect("get");
    let version = unsafe { &(*result).version };
    assert!(version.version_id >= 0);
    assert_eq!(
        version.modifications_count, 1,
        "two writes, one modification"
    );
    assert!(version.created_timestamp > 0);
    assert!(version.modified_timestamp >= version.created_timestamp);
    assert!(!version.is_ephemeral);
    assert_eq!(version.session_id, 0);
    // The server reports the writer's identity on every record, not only on
    // ephemeral ones; this client did not set one, so it is the generated id.
    assert!(
        !unsafe { owned(version.client_identity) }
            .unwrap()
            .is_empty()
    );
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

// ===========================================================================
// Options
// ===========================================================================

#[test]
fn a_conditional_put_enforces_the_expected_version() {
    let client = client();
    let version_id = put(client, b"cas/a", b"first");

    let mut stale: oxia_put_options = zeroed();
    stale.key = slice_of(b"cas/a");
    stale.value = slice_of(b"second");
    stale.has_expected_version_id = true;
    stale.expected_version_id = version_id + 1;
    let mut result: *mut oxia_put_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_put(client, &stale, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_ERR_UNEXPECTED_VERSION_ID
    );
    assert!(result.is_null());

    let mut current = stale;
    current.expected_version_id = version_id;
    assert_eq!(
        unsafe { oxia_client_put(client, &current, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    unsafe { oxia_put_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_put_can_require_that_the_record_is_new() {
    let client = client();
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"exclusive/a");
    options.value = slice_of(b"v");
    options.expected_record_not_exists = true;
    let mut result: *mut oxia_put_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_put(client, &options, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    unsafe { oxia_put_result_free(result) };
    // The second attempt finds it there.
    assert_eq!(
        unsafe { oxia_client_put(client, &options, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_ERR_UNEXPECTED_VERSION_ID
    );
    unsafe { oxia_client_free(client) };
}

#[test]
fn an_ephemeral_record_reports_its_session_and_identity() {
    let address = address().as_bytes();
    let mut client_options: oxia_client_options = zeroed();
    client_options.service_address = slice_of(address);
    client_options.identity = slice_of(b"ffi-test-worker");
    client_options.request_timeout_ms = 10_000;
    let mut client: *mut oxia_client = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_new(&client_options, &mut client, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );

    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"ephemeral/a");
    options.value = slice_of(b"v");
    options.ephemeral = true;
    let mut result: *mut oxia_put_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_put(client, &options, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    unsafe { oxia_put_result_free(result) };

    let result = get(client, b"ephemeral/a").expect("get");
    let version = unsafe { &(*result).version };
    assert!(version.is_ephemeral);
    assert_ne!(version.session_id, 0);
    assert_eq!(
        unsafe { owned(version.client_identity) }.unwrap(),
        b"ffi-test-worker"
    );
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_partition_key_routes_a_record_and_reads_it_back() {
    let client = client();
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"partition/a");
    options.value = slice_of(b"v");
    options.partition_key = slice_of(b"tenant-1");
    let mut result: *mut oxia_put_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_put(client, &options, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    unsafe { oxia_put_result_free(result) };

    let mut read: oxia_get_options = zeroed();
    read.key = slice_of(b"partition/a");
    read.partition_key = slice_of(b"tenant-1");
    let mut result: *mut oxia_get_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_get(client, &read, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    assert_eq!(unsafe { owned((*result).value) }.unwrap(), b"v");
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn sequence_key_deltas_make_the_server_generate_the_key() {
    let client = client();
    let deltas: [u64; 1] = [1];
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"sequence/log");
    options.value = slice_of(b"entry");
    options.partition_key = slice_of(b"sequence/log");
    options.sequence_key_deltas = deltas.as_ptr();
    options.sequence_key_deltas_len = deltas.len();

    let mut first: *mut oxia_put_result = ptr::null_mut();
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_put(client, &options, &mut first, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_OK, "{}", unsafe {
        take_message(message)
    });
    let first_key = unsafe { owned((*first).key) }.unwrap();
    assert_ne!(first_key, b"sequence/log", "the server appends a suffix");
    unsafe { oxia_put_result_free(first) };

    let mut second: *mut oxia_put_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_put(client, &options, &mut second, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    let second_key = unsafe { owned((*second).key) }.unwrap();
    assert!(second_key > first_key, "the suffix advances monotonically");
    unsafe { oxia_put_result_free(second) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_secondary_index_gives_a_get_another_way_in() {
    let client = client();
    let index = oxia_secondary_index {
        index_name: slice_of(b"by-colour"),
        secondary_key: slice_of(b"green"),
    };
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"index/a");
    options.value = slice_of(b"v");
    options.secondary_indexes = &index;
    options.secondary_indexes_len = 1;
    let mut result: *mut oxia_put_result = ptr::null_mut();
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_put(client, &options, &mut result, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_OK, "{}", unsafe {
        take_message(message)
    });
    unsafe { oxia_put_result_free(result) };

    let mut read: oxia_get_options = zeroed();
    read.key = slice_of(b"green");
    read.use_index = slice_of(b"by-colour");
    let mut result: *mut oxia_get_result = ptr::null_mut();
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_get(client, &read, &mut result, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_OK, "{}", unsafe {
        take_message(message)
    });
    assert_eq!(unsafe { owned((*result).key) }.unwrap(), b"index/a");
    assert_eq!(
        unsafe { owned((*result).secondary_index_key) }.unwrap(),
        b"green"
    );
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_comparison_selects_the_neighbouring_record() {
    let client = client();
    put(client, b"compare/a", b"1");
    put(client, b"compare/c", b"3");

    let mut options: oxia_get_options = zeroed();
    options.key = slice_of(b"compare/b");
    options.comparison = oxia_comparison_type::OXIA_COMPARISON_FLOOR;
    let mut result: *mut oxia_get_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_get(client, &options, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    assert_eq!(unsafe { owned((*result).key) }.unwrap(), b"compare/a");
    unsafe { oxia_get_result_free(result) };

    options.comparison = oxia_comparison_type::OXIA_COMPARISON_CEILING;
    let mut result: *mut oxia_get_result = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_get(client, &options, &mut result, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    assert_eq!(unsafe { owned((*result).key) }.unwrap(), b"compare/c");
    unsafe { oxia_get_result_free(result) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_list_can_include_the_end_of_its_range() {
    let client = client();
    put(client, b"inclusive/a", b"1");
    put(client, b"inclusive/b", b"2");

    let mut options: oxia_range_options = zeroed();
    options.min_key_inclusive = slice_of(b"inclusive/a");
    options.max_key_exclusive = slice_of(b"inclusive/b");
    let mut list: *mut oxia_key_list = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_list(client, &options, &mut list, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    assert_eq!(unsafe { (*list).len }, 1, "the end is excluded by default");
    unsafe { oxia_key_list_free(list) };

    options.end_inclusive = true;
    let mut list: *mut oxia_key_list = ptr::null_mut();
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_list(client, &options, &mut list, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_OK, "{}", unsafe {
        take_message(message)
    });
    assert_eq!(unsafe { (*list).len }, 2);
    unsafe { oxia_key_list_free(list) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn an_empty_range_lists_nothing_rather_than_failing() {
    let client = client();
    let mut options: oxia_range_options = zeroed();
    options.min_key_inclusive = slice_of(b"nothing/");
    options.max_key_exclusive = slice_of(b"nothing/~");
    let mut list: *mut oxia_key_list = ptr::null_mut();
    assert_eq!(
        unsafe { oxia_client_list(client, &options, &mut list, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    assert_eq!(unsafe { (*list).len }, 0);
    assert!(unsafe { (*list).keys.is_null() });
    unsafe { oxia_key_list_free(list) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_range_scan_returns_whole_records_in_order() {
    let client = client();
    put(client, b"scan/a", b"1");
    put(client, b"scan/b", b"2");

    let mut options: oxia_range_options = zeroed();
    options.min_key_inclusive = slice_of(b"scan/");
    options.max_key_exclusive = slice_of(b"scan/~");
    let mut records: *mut oxia_record_list = ptr::null_mut();
    let mut message: *mut c_char = ptr::null_mut();
    let code = unsafe { oxia_client_range_scan(client, &options, &mut records, &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_OK, "{}", unsafe {
        take_message(message)
    });
    let found = unsafe { items((*records).records, (*records).len) };
    assert_eq!(found.len(), 2);
    assert_eq!(unsafe { owned(found[0].key) }.unwrap(), b"scan/a");
    assert_eq!(unsafe { owned(found[0].value) }.unwrap(), b"1");
    assert_eq!(unsafe { owned(found[1].key) }.unwrap(), b"scan/b");
    unsafe { oxia_record_list_free(records) };
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_conditional_delete_enforces_the_expected_version() {
    let client = client();
    let version_id = put(client, b"delete/a", b"v");

    let mut options: oxia_delete_options = zeroed();
    options.key = slice_of(b"delete/a");
    options.has_expected_version_id = true;
    options.expected_version_id = version_id + 1;
    assert_eq!(
        unsafe { oxia_client_delete(client, &options, ptr::null_mut()) },
        oxia_error_code::OXIA_ERR_UNEXPECTED_VERSION_ID
    );

    options.expected_version_id = version_id;
    assert_eq!(
        unsafe { oxia_client_delete(client, &options, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    assert_eq!(
        get(client, b"delete/a"),
        Err(oxia_error_code::OXIA_ERR_KEY_NOT_FOUND)
    );
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_range_delete_clears_every_key_in_it() {
    let client = client();
    put(client, b"wipe/a", b"1");
    put(client, b"wipe/b", b"2");

    let mut options: oxia_delete_range_options = zeroed();
    options.min_key_inclusive = slice_of(b"wipe/");
    options.max_key_exclusive = slice_of(b"wipe/~");
    assert_eq!(
        unsafe { oxia_client_delete_range(client, &options, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );
    assert_eq!(
        get(client, b"wipe/a"),
        Err(oxia_error_code::OXIA_ERR_KEY_NOT_FOUND)
    );
    assert_eq!(
        get(client, b"wipe/b"),
        Err(oxia_error_code::OXIA_ERR_KEY_NOT_FOUND)
    );
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_missing_key_is_reported_without_a_result() {
    let client = client();
    assert_eq!(
        get(client, b"missing/nope"),
        Err(oxia_error_code::OXIA_ERR_KEY_NOT_FOUND)
    );
    assert!(!oxia_error_is_retryable(
        oxia_error_code::OXIA_ERR_KEY_NOT_FOUND
    ));
    unsafe { oxia_client_free(client) };
}

// ===========================================================================
// The asynchronous form
// ===========================================================================

/// One callback's arguments, as far as a test cares about them.
type Delivered = (oxia_error_code, Option<Vec<u8>>, Option<String>);

/// Collects what the callbacks were handed, and lets a test wait for them.
#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<Delivered>>,
    arrived: Condvar,
}

impl Recorder {
    fn record(&self, code: oxia_error_code, key: Option<Vec<u8>>, message: Option<String>) {
        self.seen.lock().unwrap().push((code, key, message));
        self.arrived.notify_all();
    }

    fn wait_for(&self, count: usize) -> Vec<Delivered> {
        let mut seen = self.seen.lock().unwrap();
        while seen.len() < count {
            let (next, timeout) = self
                .arrived
                .wait_timeout(seen, Duration::from_secs(30))
                .unwrap();
            seen = next;
            assert!(
                !timeout.timed_out(),
                "only {} callbacks arrived",
                seen.len()
            );
        }
        seen.clone()
    }
}

unsafe fn message_of(message: *const c_char) -> Option<String> {
    if message.is_null() {
        None
    } else {
        Some(CStr::from_ptr(message).to_string_lossy().into_owned())
    }
}

extern "C" fn on_put(
    code: oxia_error_code,
    message: *const c_char,
    result: *const oxia_put_result,
    ctx: *mut c_void,
) {
    let recorder = unsafe { &*(ctx as *const Recorder) };
    let key = if result.is_null() {
        None
    } else {
        unsafe { owned((*result).key) }
    };
    recorder.record(code, key, unsafe { message_of(message) });
}

extern "C" fn on_status(code: oxia_error_code, message: *const c_char, ctx: *mut c_void) {
    let recorder = unsafe { &*(ctx as *const Recorder) };
    recorder.record(code, None, unsafe { message_of(message) });
}

#[test]
fn an_async_put_calls_back_once_with_its_result() {
    let client = client();
    let recorder = Recorder::default();
    let ctx = &recorder as *const Recorder as *mut c_void;

    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"async/a");
    options.value = slice_of(b"v");
    assert_eq!(
        unsafe { oxia_client_put_async(client, &options, Some(on_put), ctx, ptr::null_mut()) },
        oxia_error_code::OXIA_OK
    );

    let seen = recorder.wait_for(1);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, oxia_error_code::OXIA_OK);
    assert_eq!(seen[0].1.as_deref(), Some(b"async/a".as_ref()));
    assert_eq!(seen[0].2, None, "a success carries no message");
    unsafe { oxia_client_free(client) };
}

#[test]
fn an_async_failure_carries_its_code_and_message() {
    let client = client();
    let recorder = Recorder::default();
    let ctx = &recorder as *const Recorder as *mut c_void;

    let mut options: oxia_delete_options = zeroed();
    options.key = slice_of(b"async/missing");
    assert_eq!(
        unsafe {
            oxia_client_delete_async(client, &options, Some(on_status), ctx, ptr::null_mut())
        },
        oxia_error_code::OXIA_OK
    );

    let seen = recorder.wait_for(1);
    assert_eq!(seen[0].0, oxia_error_code::OXIA_ERR_KEY_NOT_FOUND);
    assert!(seen[0].2.is_some(), "a failure carries a message");
    unsafe { oxia_client_free(client) };
}

#[test]
fn many_async_puts_each_call_back_exactly_once() {
    let client = client();
    let recorder = Recorder::default();
    let ctx = &recorder as *const Recorder as *mut c_void;

    let keys: Vec<Vec<u8>> = (0..64)
        .map(|index| format!("async/many/{index:03}").into_bytes())
        .collect();
    for key in &keys {
        let mut options: oxia_put_options = zeroed();
        options.key = slice_of(key);
        options.value = slice_of(b"v");
        assert_eq!(
            unsafe { oxia_client_put_async(client, &options, Some(on_put), ctx, ptr::null_mut()) },
            oxia_error_code::OXIA_OK
        );
    }

    let seen = recorder.wait_for(keys.len());
    assert_eq!(seen.len(), keys.len(), "exactly one callback each");
    assert!(
        seen.iter()
            .all(|(code, _, _)| *code == oxia_error_code::OXIA_OK)
    );
    let mut returned: Vec<Vec<u8>> = seen
        .iter()
        .map(|(_, key, _)| key.clone().unwrap())
        .collect();
    returned.sort();
    let mut expected = keys.clone();
    expected.sort();
    assert_eq!(returned, expected);
    unsafe { oxia_client_free(client) };
}

#[test]
fn a_null_callback_is_rejected_before_anything_is_submitted() {
    let client = client();
    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"async/no-callback");
    options.value = slice_of(b"v");
    let mut message: *mut c_char = ptr::null_mut();
    let code =
        unsafe { oxia_client_put_async(client, &options, None, ptr::null_mut(), &mut message) };
    assert_eq!(code, oxia_error_code::OXIA_ERR_INVALID_ARGUMENT);
    assert!(unsafe { take_message(message) }.contains("callback"));
    unsafe { oxia_client_free(client) };
}

/// What a callback that bridges into another runtime may well do: block, and
/// call back into the client. It runs on the dispatcher thread, not on the
/// client's, so neither stalls nor deadlocks.
struct Reentrant {
    client: *mut oxia_client,
    recorder: Recorder,
}

unsafe impl Sync for Reentrant {}

extern "C" fn on_put_then_read(
    code: oxia_error_code,
    _message: *const c_char,
    _result: *const oxia_put_result,
    ctx: *mut c_void,
) {
    let state = unsafe { &*(ctx as *const Reentrant) };
    assert_eq!(code, oxia_error_code::OXIA_OK);
    std::thread::sleep(Duration::from_millis(50));
    let read = get(state.client, b"reentrant/a").expect("a blocking get from a callback");
    let value = unsafe { owned((*read).value) };
    unsafe { oxia_get_result_free(read) };
    state.recorder.record(code, value, None);
}

#[test]
fn a_callback_may_block_and_call_back_into_the_client() {
    let client = client();
    let state = Reentrant {
        client,
        recorder: Recorder::default(),
    };
    let ctx = &state as *const Reentrant as *mut c_void;

    let mut options: oxia_put_options = zeroed();
    options.key = slice_of(b"reentrant/a");
    options.value = slice_of(b"from-the-callback");
    assert_eq!(
        unsafe {
            oxia_client_put_async(
                client,
                &options,
                Some(on_put_then_read),
                ctx,
                ptr::null_mut(),
            )
        },
        oxia_error_code::OXIA_OK
    );

    let seen = state.recorder.wait_for(1);
    assert_eq!(seen[0].1.as_deref(), Some(b"from-the-callback".as_ref()));
    unsafe { oxia_client_free(client) };
}

// ===========================================================================
// Threads
// ===========================================================================

#[test]
fn one_client_serves_many_threads_at_once() {
    let client = client();
    let shared = client as usize;
    let threads: Vec<_> = (0..8)
        .map(|worker| {
            std::thread::spawn(move || {
                let client = shared as *mut oxia_client;
                for index in 0..25 {
                    let key = format!("threads/{worker}/{index}").into_bytes();
                    put(client, &key, b"v");
                    let result = get(client, &key).expect("get");
                    assert_eq!(unsafe { owned((*result).value) }.unwrap(), b"v");
                    unsafe { oxia_get_result_free(result) };
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("a worker thread failed");
    }
    unsafe { oxia_client_free(client) };
}
