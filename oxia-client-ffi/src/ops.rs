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

//! The operations, each in a blocking and an asynchronous form.
//!
//! Every operation takes its arguments in an options struct. A zeroed struct
//! asks for the plain operation with the client's defaults, so a caller sets
//! only the fields it cares about:
//!
//! ```c
//! oxia_put_options options = {0};
//! options.key = (oxia_slice){ (const uint8_t *)"greeting", 8 };
//! options.value = (oxia_slice){ (const uint8_t *)"hello", 5 };
//! ```

use std::ffi::{CString, c_char, c_void};
use std::ptr;

use oxia::{ComparisonType, GetResult, OxiaClient as NativeOxiaClient, OxiaError, PutResult};

use crate::error::{fail, guard, invalid, oxia_error_code};
use crate::mem::{optional_str, oxia_slice, required_array, required_bytes, required_str};
use crate::results::{oxia_get_result, oxia_key_list, oxia_put_result, oxia_record_list};
use crate::runtime::oxia_client;

// ===========================================================================
// Callback plumbing
// ===========================================================================

/// A caller's callback together with the context pointer to hand back.
struct Callback<F> {
    function: F,
    ctx: *mut c_void,
}

// The context is opaque to this library: it travels from the thread that
// submitted the operation to the dispatcher thread that calls back, and the
// caller is told as much.
unsafe impl<F: Send> Send for Callback<F> {}

/// The shape of every callback that carries a result.
type ResultCallback<R> = extern "C" fn(oxia_error_code, *const c_char, *const R, *mut c_void);

/// The shape of the callbacks for the operations that return nothing.
type StatusCallback = extern "C" fn(oxia_error_code, *const c_char, *mut c_void);

// The invocations go through these methods rather than reading
// `callback.function` and `callback.ctx` at the call site, because a closure
// that touches the fields captures them one by one and so captures a bare
// `*mut c_void`, which is not `Send`. Calling a method captures the wrapper
// whole, which is.
impl<R> Callback<ResultCallback<R>> {
    /// Hands over a result, borrowed for the duration of the call.
    fn succeeded(&self, result: &R) {
        (self.function)(oxia_error_code::OXIA_OK, ptr::null(), result, self.ctx);
    }

    fn failed(&self, code: oxia_error_code, message: *const c_char) {
        (self.function)(code, message, ptr::null(), self.ctx);
    }
}

impl Callback<StatusCallback> {
    fn succeeded(&self) {
        (self.function)(oxia_error_code::OXIA_OK, ptr::null(), self.ctx);
    }

    fn failed(&self, code: oxia_error_code, message: *const c_char) {
        (self.function)(code, message, self.ctx);
    }
}

/// Builds the closure the dispatcher runs for an operation that has a result.
///
/// The result and the message are borrowed for the duration of the call: the
/// result is freed as soon as the callback returns, and the `CString` lives
/// until the end of this closure.
fn on_result<T, R>(
    callback: Callback<ResultCallback<R>>,
    convert: fn(&T) -> R,
    release: unsafe fn(R),
) -> impl FnOnce(Result<T, OxiaError>) + Send + 'static
where
    T: Send + 'static,
    R: 'static,
{
    move |outcome| match outcome {
        Ok(value) => {
            let result = convert(&value);
            callback.succeeded(&result);
            unsafe { release(result) };
        }
        Err(error) => {
            let message = c_string(&error.to_string());
            callback.failed(oxia_error_code::from(&error), message.as_ptr());
        }
    }
}

/// Builds the closure the dispatcher runs for an operation with no result.
fn on_status(
    callback: Callback<StatusCallback>,
) -> impl FnOnce(Result<(), OxiaError>) + Send + 'static {
    move |outcome| match outcome {
        Ok(()) => callback.succeeded(),
        Err(error) => {
            let message = c_string(&error.to_string());
            callback.failed(oxia_error_code::from(&error), message.as_ptr());
        }
    }
}

/// A message as a C string, with any interior NUL escaped so it survives.
fn c_string(text: &str) -> CString {
    CString::new(text)
        .or_else(|_| CString::new(text.replace('\0', "\\0")))
        .unwrap_or_default()
}

// ===========================================================================
// Options
// ===========================================================================

/// An additional index a put writes the record under.
#[repr(C)]
pub struct oxia_secondary_index {
    /// The name of the index. Must not contain `/`.
    pub index_name: oxia_slice,
    /// The key to index the record under.
    pub secondary_key: oxia_slice,
}

/// The arguments of a put.
#[repr(C)]
pub struct oxia_put_options {
    /// The key to write. Required, and must be valid UTF-8.
    pub key: oxia_slice,
    /// The value to write. May be empty.
    pub value: oxia_slice,
    /// Compare-and-swap: the write only applies if the stored version
    /// matches. Read only when `has_expected_version_id` is true.
    pub expected_version_id: i64,
    /// Whether `expected_version_id` applies.
    pub has_expected_version_id: bool,
    /// The write only applies if the record does not exist yet.
    pub expected_record_not_exists: bool,
    /// Bind the record to this client's session, so it is deleted when the
    /// session closes or expires.
    pub ephemeral: bool,
    /// Co-locate the record on the shard this key hashes to; `data` `NULL`
    /// to route by the record's own key.
    pub partition_key: oxia_slice,
    /// Per-delta monotonic suffixes the server appends to the key; `NULL`
    /// for a plain key.
    pub sequence_key_deltas: *const u64,
    /// How many deltas `sequence_key_deltas` holds.
    pub sequence_key_deltas_len: usize,
    /// Secondary indexes to add the record to; `NULL` for none.
    pub secondary_indexes: *const oxia_secondary_index,
    /// How many entries `secondary_indexes` holds.
    pub secondary_indexes_len: usize,
}

/// Which record a get selects relative to the requested key.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum oxia_comparison_type {
    /// The stored key must equal the requested key.
    OXIA_COMPARISON_EQUAL = 0,
    /// The highest key `<=` the requested key.
    OXIA_COMPARISON_FLOOR = 1,
    /// The lowest key `>=` the requested key.
    OXIA_COMPARISON_CEILING = 2,
    /// The highest key `<` the requested key.
    OXIA_COMPARISON_LOWER = 3,
    /// The lowest key `>` the requested key.
    OXIA_COMPARISON_HIGHER = 4,
}

impl From<oxia_comparison_type> for ComparisonType {
    fn from(comparison: oxia_comparison_type) -> Self {
        match comparison {
            oxia_comparison_type::OXIA_COMPARISON_EQUAL => ComparisonType::Equal,
            oxia_comparison_type::OXIA_COMPARISON_FLOOR => ComparisonType::Floor,
            oxia_comparison_type::OXIA_COMPARISON_CEILING => ComparisonType::Ceiling,
            oxia_comparison_type::OXIA_COMPARISON_LOWER => ComparisonType::Lower,
            oxia_comparison_type::OXIA_COMPARISON_HIGHER => ComparisonType::Higher,
        }
    }
}

/// The arguments of a get.
#[repr(C)]
pub struct oxia_get_options {
    /// The key to read. Required, and must be valid UTF-8.
    pub key: oxia_slice,
    /// Which record to select; zero is an exact match.
    pub comparison: oxia_comparison_type,
    /// Return only the version, without the value. Zero — the default —
    /// returns the value.
    pub exclude_value: bool,
    /// Read from the shard this key hashes to; `data` `NULL` to route by the
    /// requested key.
    pub partition_key: oxia_slice,
    /// Resolve the key through this secondary index instead of the primary
    /// one; `data` `NULL` for the primary.
    pub use_index: oxia_slice,
}

/// The arguments of a delete.
#[repr(C)]
pub struct oxia_delete_options {
    /// The key to delete. Required, and must be valid UTF-8.
    pub key: oxia_slice,
    /// Compare-and-swap: the delete only applies if the stored version
    /// matches. Read only when `has_expected_version_id` is true.
    pub expected_version_id: i64,
    /// Whether `expected_version_id` applies.
    pub has_expected_version_id: bool,
    /// Route to the shard this key hashes to; `data` `NULL` to route by the
    /// record's own key.
    pub partition_key: oxia_slice,
}

/// The arguments of a delete over a range of keys.
#[repr(C)]
pub struct oxia_delete_range_options {
    /// The first key of the range, included. Required.
    pub min_key_inclusive: oxia_slice,
    /// The key the range stops before, excluded. Required.
    pub max_key_exclusive: oxia_slice,
    /// Delete only within the shard this key hashes to; `data` `NULL` to
    /// cover every shard.
    pub partition_key: oxia_slice,
}

/// The arguments of a list or a range scan, which take the same options.
#[repr(C)]
pub struct oxia_range_options {
    /// The first key of the range, included. Required.
    pub min_key_inclusive: oxia_slice,
    /// The key the range stops at. Excluded unless `end_inclusive`.
    pub max_key_exclusive: oxia_slice,
    /// Read only the shard this key hashes to; `data` `NULL` to merge every
    /// shard.
    pub partition_key: oxia_slice,
    /// Walk this secondary index instead of the primary one; `data` `NULL`
    /// for the primary.
    pub use_index: oxia_slice,
    /// Include `max_key_exclusive` itself. Fails with
    /// `OXIA_ERR_END_INCLUSIVE_UNSUPPORTED` against a shard whose leader is
    /// too old to support it.
    pub end_inclusive: bool,
    /// Include the keys Oxia keeps for its own bookkeeping.
    pub include_internal_keys: bool,
}

// ===========================================================================
// Parsed requests
//
// The native request builders borrow the client and are not `Send`, so they
// have to be built on the client's thread. Each entry point validates its
// options into one of these owned, `Send` requests, which the client's thread
// then turns into a builder.
// ===========================================================================

struct PutRequest {
    key: String,
    value: Vec<u8>,
    expected_version_id: Option<i64>,
    expected_record_not_exists: bool,
    ephemeral: bool,
    partition_key: Option<String>,
    sequence_key_deltas: Vec<u64>,
    secondary_indexes: Vec<(String, String)>,
}

impl PutRequest {
    unsafe fn parse(options: *const oxia_put_options) -> Result<PutRequest, String> {
        let options = options.as_ref().ok_or("options is NULL")?;
        let mut secondary_indexes = Vec::new();
        for index in required_array(
            options.secondary_indexes,
            options.secondary_indexes_len,
            "secondary_indexes",
        )? {
            secondary_indexes.push((
                required_str(index.index_name, "secondary_indexes[].index_name")?.to_string(),
                required_str(index.secondary_key, "secondary_indexes[].secondary_key")?.to_string(),
            ));
        }
        Ok(PutRequest {
            key: required_str(options.key, "key")?.to_string(),
            value: required_bytes(options.value, "value")?.to_vec(),
            expected_version_id: options
                .has_expected_version_id
                .then_some(options.expected_version_id),
            expected_record_not_exists: options.expected_record_not_exists,
            ephemeral: options.ephemeral,
            partition_key: optional_str(options.partition_key, "partition_key")?
                .map(str::to_string),
            sequence_key_deltas: required_array(
                options.sequence_key_deltas,
                options.sequence_key_deltas_len,
                "sequence_key_deltas",
            )?
            .to_vec(),
            secondary_indexes,
        })
    }

    async fn execute(self, client: NativeOxiaClient) -> Result<PutResult, OxiaError> {
        let mut put = client.put(self.key, self.value);
        if let Some(version_id) = self.expected_version_id {
            put = put.expected_version_id(version_id);
        }
        if self.expected_record_not_exists {
            put = put.expected_record_not_exists();
        }
        if self.ephemeral {
            put = put.ephemeral();
        }
        if let Some(partition_key) = self.partition_key {
            put = put.partition_key(partition_key);
        }
        if !self.sequence_key_deltas.is_empty() {
            put = put.sequence_key_deltas(self.sequence_key_deltas);
        }
        for (index_name, secondary_key) in self.secondary_indexes {
            put = put.secondary_index(index_name, secondary_key);
        }
        put.await
    }
}

struct GetRequest {
    key: String,
    comparison: ComparisonType,
    include_value: bool,
    partition_key: Option<String>,
    use_index: Option<String>,
}

impl GetRequest {
    unsafe fn parse(options: *const oxia_get_options) -> Result<GetRequest, String> {
        let options = options.as_ref().ok_or("options is NULL")?;
        Ok(GetRequest {
            key: required_str(options.key, "key")?.to_string(),
            comparison: options.comparison.into(),
            include_value: !options.exclude_value,
            partition_key: optional_str(options.partition_key, "partition_key")?
                .map(str::to_string),
            use_index: optional_str(options.use_index, "use_index")?.map(str::to_string),
        })
    }

    async fn execute(self, client: NativeOxiaClient) -> Result<GetResult, OxiaError> {
        let mut get = client.get(self.key).comparison(self.comparison);
        if !self.include_value {
            get = get.include_value(false);
        }
        if let Some(partition_key) = self.partition_key {
            get = get.partition_key(partition_key);
        }
        if let Some(index_name) = self.use_index {
            get = get.use_index(index_name);
        }
        get.await
    }
}

struct DeleteRequest {
    key: String,
    expected_version_id: Option<i64>,
    partition_key: Option<String>,
}

impl DeleteRequest {
    unsafe fn parse(options: *const oxia_delete_options) -> Result<DeleteRequest, String> {
        let options = options.as_ref().ok_or("options is NULL")?;
        Ok(DeleteRequest {
            key: required_str(options.key, "key")?.to_string(),
            expected_version_id: options
                .has_expected_version_id
                .then_some(options.expected_version_id),
            partition_key: optional_str(options.partition_key, "partition_key")?
                .map(str::to_string),
        })
    }

    async fn execute(self, client: NativeOxiaClient) -> Result<(), OxiaError> {
        let mut delete = client.delete(self.key);
        if let Some(version_id) = self.expected_version_id {
            delete = delete.expected_version_id(version_id);
        }
        if let Some(partition_key) = self.partition_key {
            delete = delete.partition_key(partition_key);
        }
        delete.await
    }
}

struct DeleteRangeRequest {
    min_key_inclusive: String,
    max_key_exclusive: String,
    partition_key: Option<String>,
}

impl DeleteRangeRequest {
    unsafe fn parse(
        options: *const oxia_delete_range_options,
    ) -> Result<DeleteRangeRequest, String> {
        let options = options.as_ref().ok_or("options is NULL")?;
        Ok(DeleteRangeRequest {
            min_key_inclusive: required_str(options.min_key_inclusive, "min_key_inclusive")?
                .to_string(),
            max_key_exclusive: required_str(options.max_key_exclusive, "max_key_exclusive")?
                .to_string(),
            partition_key: optional_str(options.partition_key, "partition_key")?
                .map(str::to_string),
        })
    }

    async fn execute(self, client: NativeOxiaClient) -> Result<(), OxiaError> {
        let mut delete = client.delete_range(self.min_key_inclusive, self.max_key_exclusive);
        if let Some(partition_key) = self.partition_key {
            delete = delete.partition_key(partition_key);
        }
        delete.await
    }
}

struct RangeRequest {
    min_key_inclusive: String,
    max_key_exclusive: String,
    partition_key: Option<String>,
    use_index: Option<String>,
    end_inclusive: bool,
    include_internal_keys: bool,
}

impl RangeRequest {
    unsafe fn parse(options: *const oxia_range_options) -> Result<RangeRequest, String> {
        let options = options.as_ref().ok_or("options is NULL")?;
        Ok(RangeRequest {
            min_key_inclusive: required_str(options.min_key_inclusive, "min_key_inclusive")?
                .to_string(),
            max_key_exclusive: required_str(options.max_key_exclusive, "max_key_exclusive")?
                .to_string(),
            partition_key: optional_str(options.partition_key, "partition_key")?
                .map(str::to_string),
            use_index: optional_str(options.use_index, "use_index")?.map(str::to_string),
            end_inclusive: options.end_inclusive,
            include_internal_keys: options.include_internal_keys,
        })
    }

    async fn list(self, client: NativeOxiaClient) -> Result<Vec<String>, OxiaError> {
        let mut list = client
            .list(self.min_key_inclusive, self.max_key_exclusive)
            .include_internal_keys(self.include_internal_keys);
        if let Some(partition_key) = self.partition_key {
            list = list.partition_key(partition_key);
        }
        if let Some(index_name) = self.use_index {
            list = list.use_index(index_name);
        }
        if self.end_inclusive {
            list = list.end_inclusive();
        }
        list.await
    }

    async fn range_scan(self, client: NativeOxiaClient) -> Result<Vec<GetResult>, OxiaError> {
        let mut scan = client
            .range_scan(self.min_key_inclusive, self.max_key_exclusive)
            .include_internal_keys(self.include_internal_keys);
        if let Some(partition_key) = self.partition_key {
            scan = scan.partition_key(partition_key);
        }
        if let Some(index_name) = self.use_index {
            scan = scan.use_index(index_name);
        }
        if self.end_inclusive {
            scan = scan.end_inclusive();
        }
        scan.await
    }
}

// ===========================================================================
// Shared entry-point scaffolding
// ===========================================================================

/// Checks the handle and the out-parameter every blocking operation needs,
/// clearing the out-parameter so that it is `NULL` on every error path.
///
/// # Safety
///
/// `result` must be `NULL` or point at a writable pointer.
unsafe fn blocking_preamble<'a, R>(
    client: *const oxia_client,
    result: *mut *mut R,
    error_message: *mut *mut c_char,
) -> Result<&'a oxia_client, oxia_error_code> {
    if !result.is_null() {
        *result = ptr::null_mut();
    }
    let Some(client) = client.as_ref() else {
        return Err(invalid(error_message, "client is NULL"));
    };
    if result.is_null() {
        return Err(invalid(error_message, "the result out-parameter is NULL"));
    }
    Ok(client)
}

/// Checks the handle and the callback every asynchronous operation needs.
unsafe fn async_preamble<'a, F>(
    client: *const oxia_client,
    callback: Option<F>,
    error_message: *mut *mut c_char,
) -> Result<(&'a oxia_client, F), oxia_error_code> {
    let Some(client) = client.as_ref() else {
        return Err(invalid(error_message, "client is NULL"));
    };
    let Some(callback) = callback else {
        return Err(invalid(error_message, "callback is NULL"));
    };
    Ok((client, callback))
}

// ===========================================================================
// put
// ===========================================================================

/// Called once when a put submitted with `oxia_client_put_async` finishes.
pub type oxia_put_callback =
    extern "C" fn(oxia_error_code, *const c_char, *const oxia_put_result, *mut c_void);

/// Writes a record, blocking until the server has acknowledged it.
///
/// On `OXIA_OK`, `*result` owns a result the caller frees with
/// `oxia_put_result_free`; on any error it is set to `NULL`.
///
/// # Safety
///
/// `client` must be an open client. `options` must point at an initialized
/// options struct whose slices stay valid for the call. `result` must point
/// at a writable pointer, and `error_message` must be `NULL` or point at
/// one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_put(
    client: *const oxia_client,
    options: *const oxia_put_options,
    result: *mut *mut oxia_put_result,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let client = match blocking_preamble(client, result, error_message) {
            Ok(client) => client,
            Err(code) => return code,
        };
        let request = match PutRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        match client.run(move |native| request.execute(native)) {
            Ok(put) => {
                *result = Box::into_raw(Box::new(oxia_put_result::new(&put)));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Submits a put and returns at once; `callback` runs when it finishes.
///
/// Returns `OXIA_OK` once the operation has been accepted, which is when
/// `callback` is guaranteed to run exactly once. Any other code means it was
/// rejected and `callback` will not run.
///
/// # Safety
///
/// As `oxia_client_put`, except that the options are read before this
/// returns and need not outlive it. `ctx` is passed back untouched and must
/// be usable from the dispatcher thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_put_async(
    client: *const oxia_client,
    options: *const oxia_put_options,
    callback: Option<
        extern "C" fn(oxia_error_code, *const c_char, *const oxia_put_result, *mut c_void),
    >,
    ctx: *mut c_void,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let (client, function) = match async_preamble(client, callback, error_message) {
            Ok(pair) => pair,
            Err(code) => return code,
        };
        let request = match PutRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        let deliver = on_result(
            Callback { function, ctx },
            oxia_put_result::new,
            oxia_put_result::free,
        );
        match client.submit(move |native| request.execute(native), deliver) {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}

// ===========================================================================
// get
// ===========================================================================

/// Called once when a get submitted with `oxia_client_get_async` finishes.
pub type oxia_get_callback =
    extern "C" fn(oxia_error_code, *const c_char, *const oxia_get_result, *mut c_void);

/// Reads a record, blocking until the server has answered.
///
/// A missing record is `OXIA_ERR_KEY_NOT_FOUND`, not an empty result. On
/// `OXIA_OK`, `*result` owns a result the caller frees with
/// `oxia_get_result_free`; on any error it is set to `NULL`.
///
/// # Safety
///
/// As `oxia_client_put`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_get(
    client: *const oxia_client,
    options: *const oxia_get_options,
    result: *mut *mut oxia_get_result,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let client = match blocking_preamble(client, result, error_message) {
            Ok(client) => client,
            Err(code) => return code,
        };
        let request = match GetRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        match client.run(move |native| request.execute(native)) {
            Ok(record) => {
                *result = Box::into_raw(Box::new(oxia_get_result::new(&record)));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Submits a get and returns at once; `callback` runs when it finishes.
///
/// # Safety
///
/// As `oxia_client_put_async`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_get_async(
    client: *const oxia_client,
    options: *const oxia_get_options,
    callback: Option<
        extern "C" fn(oxia_error_code, *const c_char, *const oxia_get_result, *mut c_void),
    >,
    ctx: *mut c_void,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let (client, function) = match async_preamble(client, callback, error_message) {
            Ok(pair) => pair,
            Err(code) => return code,
        };
        let request = match GetRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        let deliver = on_result(
            Callback { function, ctx },
            oxia_get_result::new,
            oxia_get_result::free,
        );
        match client.submit(move |native| request.execute(native), deliver) {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}

// ===========================================================================
// delete and delete_range
// ===========================================================================

/// Called once when an operation with no result finishes.
pub type oxia_status_callback = extern "C" fn(oxia_error_code, *const c_char, *mut c_void);

/// Deletes a record, blocking until the server has acknowledged it.
///
/// Deleting a key that does not exist is `OXIA_ERR_KEY_NOT_FOUND`.
///
/// # Safety
///
/// `client` must be an open client, `options` must point at an initialized
/// options struct, and `error_message` must be `NULL` or point at a writable
/// `char *`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_delete(
    client: *const oxia_client,
    options: *const oxia_delete_options,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let Some(client) = client.as_ref() else {
            return invalid(error_message, "client is NULL");
        };
        let request = match DeleteRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        match client.run(move |native| request.execute(native)) {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Submits a delete and returns at once; `callback` runs when it finishes.
///
/// # Safety
///
/// As `oxia_client_put_async`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_delete_async(
    client: *const oxia_client,
    options: *const oxia_delete_options,
    callback: Option<extern "C" fn(oxia_error_code, *const c_char, *mut c_void)>,
    ctx: *mut c_void,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let (client, function) = match async_preamble(client, callback, error_message) {
            Ok(pair) => pair,
            Err(code) => return code,
        };
        let request = match DeleteRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        let deliver = on_status(Callback { function, ctx });
        match client.submit(move |native| request.execute(native), deliver) {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Deletes every record in a range, blocking until the server has
/// acknowledged it. An empty range succeeds.
///
/// # Safety
///
/// As `oxia_client_delete`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_delete_range(
    client: *const oxia_client,
    options: *const oxia_delete_range_options,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let Some(client) = client.as_ref() else {
            return invalid(error_message, "client is NULL");
        };
        let request = match DeleteRangeRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        match client.run(move |native| request.execute(native)) {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Submits a range delete and returns at once; `callback` runs when it
/// finishes.
///
/// # Safety
///
/// As `oxia_client_put_async`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_delete_range_async(
    client: *const oxia_client,
    options: *const oxia_delete_range_options,
    callback: Option<extern "C" fn(oxia_error_code, *const c_char, *mut c_void)>,
    ctx: *mut c_void,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let (client, function) = match async_preamble(client, callback, error_message) {
            Ok(pair) => pair,
            Err(code) => return code,
        };
        let request = match DeleteRangeRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        let deliver = on_status(Callback { function, ctx });
        match client.submit(move |native| request.execute(native), deliver) {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}

// ===========================================================================
// list and range_scan
// ===========================================================================

/// Called once when a list submitted with `oxia_client_list_async` finishes.
pub type oxia_list_callback =
    extern "C" fn(oxia_error_code, *const c_char, *const oxia_key_list, *mut c_void);

/// Called once when a scan submitted with `oxia_client_range_scan_async`
/// finishes.
pub type oxia_range_scan_callback =
    extern "C" fn(oxia_error_code, *const c_char, *const oxia_record_list, *mut c_void);

/// Lists the keys in a range, merged across shards in the key order of the
/// namespace. An empty range is `OXIA_OK` with a list of length 0.
///
/// On `OXIA_OK`, `*result` owns a list the caller frees with
/// `oxia_key_list_free`; on any error it is set to `NULL`.
///
/// # Safety
///
/// As `oxia_client_put`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_list(
    client: *const oxia_client,
    options: *const oxia_range_options,
    result: *mut *mut oxia_key_list,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let client = match blocking_preamble(client, result, error_message) {
            Ok(client) => client,
            Err(code) => return code,
        };
        let request = match RangeRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        match client.run(move |native| request.list(native)) {
            Ok(keys) => {
                *result = Box::into_raw(Box::new(oxia_key_list::new(&keys)));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Submits a list and returns at once; `callback` runs when it finishes.
///
/// # Safety
///
/// As `oxia_client_put_async`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_list_async(
    client: *const oxia_client,
    options: *const oxia_range_options,
    callback: Option<
        extern "C" fn(oxia_error_code, *const c_char, *const oxia_key_list, *mut c_void),
    >,
    ctx: *mut c_void,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let (client, function) = match async_preamble(client, callback, error_message) {
            Ok(pair) => pair,
            Err(code) => return code,
        };
        let request = match RangeRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        let deliver = on_result(
            Callback { function, ctx },
            |keys: &Vec<String>| oxia_key_list::new(keys),
            oxia_key_list::free,
        );
        match client.submit(move |native| request.list(native), deliver) {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Reads every record in a range, merged across shards in the key order of
/// the namespace.
///
/// On `OXIA_OK`, `*result` owns a list the caller frees with
/// `oxia_record_list_free`; on any error it is set to `NULL`.
///
/// # Safety
///
/// As `oxia_client_put`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_range_scan(
    client: *const oxia_client,
    options: *const oxia_range_options,
    result: *mut *mut oxia_record_list,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let client = match blocking_preamble(client, result, error_message) {
            Ok(client) => client,
            Err(code) => return code,
        };
        let request = match RangeRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        match client.run(move |native| request.range_scan(native)) {
            Ok(records) => {
                *result = Box::into_raw(Box::new(oxia_record_list::new(&records)));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Submits a range scan and returns at once; `callback` runs when it
/// finishes.
///
/// # Safety
///
/// As `oxia_client_put_async`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_range_scan_async(
    client: *const oxia_client,
    options: *const oxia_range_options,
    callback: Option<
        extern "C" fn(oxia_error_code, *const c_char, *const oxia_record_list, *mut c_void),
    >,
    ctx: *mut c_void,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        let (client, function) = match async_preamble(client, callback, error_message) {
            Ok(pair) => pair,
            Err(code) => return code,
        };
        let request = match RangeRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        let deliver = on_result(
            Callback { function, ctx },
            |records: &Vec<GetResult>| oxia_record_list::new(records),
            oxia_record_list::free,
        );
        match client.submit(move |native| request.range_scan(native), deliver) {
            Ok(()) => oxia_error_code::OXIA_OK,
            Err(error) => fail(error_message, &error),
        }
    })
}
