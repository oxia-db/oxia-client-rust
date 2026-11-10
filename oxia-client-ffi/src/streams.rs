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

//! The subscriptions and the streaming reads.
//!
//! These are pulled, not pushed. A handle's `next` asks the client's thread
//! for one item and blocks for the reply; nothing is taken from the native
//! stream until a caller asks for it. That is deliberate: a pushed stream
//! would need somewhere to put items a slow consumer has not taken yet, and
//! an unbounded queue there would lose the backpressure the native
//! subscription already applies through its `buffer_size`. Pulling makes the
//! consumer the only thing that sets the pace, and keeps at most one item in
//! flight inside this library.
//!
//! The protocol between a handle and the task serving it on the client's
//! thread is one request per item:
//!
//! 1. `next` sends a request, unless one is already outstanding.
//! 2. The serving task takes one item from the native stream and sends it
//!    back.
//! 3. `next` returns it, or `OXIA_ERR_TIMEOUT` if it did not arrive in time —
//!    leaving the request outstanding, so the item is delivered to the next
//!    call rather than being dropped.
//!
//! A stream is finished by its own end, by an error, by `close`, or by the
//! client being freed — which has to stop these tasks explicitly, since each
//! one is otherwise parked on its handle's next request and compio's
//! `block_on` does not return while a task it spawned is still alive. `close`
//! only drops the cancellation sender, so it never blocks and is safe to call
//! from another thread while a consumer sits in `next`: the serving task
//! stops, which disconnects the item channel, which wakes that consumer with
//! `OXIA_STREAM_END`.

use std::ffi::c_char;
use std::sync::Mutex;
use std::sync::mpsc as sync_mpsc;
use std::time::Duration;

use futures::channel::{mpsc, oneshot};
use futures::{Stream, StreamExt};
use oxia::{GetResult, Notification, OxiaError};
use tokio::sync::watch;

use crate::error::{fail, guard, guard_void, invalid, oxia_error_code};
use crate::mem::{optional_bytes, oxia_bytes, oxia_slice, required_str};
use crate::ops::{RangeRequest, oxia_range_options};
use crate::results::oxia_get_result;
use crate::runtime::oxia_client;

// ===========================================================================
// The pull protocol
// ===========================================================================

/// One answer to one request.
pub(crate) enum StreamItem<T> {
    Item(T),
    Error(OxiaError),
    /// The native stream finished on its own.
    End,
}

/// What a `next` found.
pub(crate) enum Next<T> {
    Item(T),
    Error(OxiaError),
    /// No item within the timeout; the request stays outstanding.
    TimedOut,
    /// Finished, by its own end, an error already reported, or `close`.
    End,
}

/// Serves one item per request from `stream`, on the client's thread, until
/// the handle is dropped or closed or the stream ends.
pub(crate) async fn serve<T, S, Fut>(
    open: Fut,
    ready: sync_mpsc::Sender<Result<(), OxiaError>>,
    mut requests: mpsc::UnboundedReceiver<()>,
    items: sync_mpsc::Sender<StreamItem<T>>,
    mut cancelled: oneshot::Receiver<()>,
    mut shutdown: watch::Receiver<bool>,
) where
    Fut: Future<Output = Result<S, OxiaError>>,
    S: Stream<Item = Result<T, OxiaError>>,
{
    let stream = match open.await {
        Ok(stream) => {
            // Nobody waiting for the handle any more: give up before
            // subscribing to anything.
            if ready.send(Ok(())).is_err() {
                return;
            }
            stream
        }
        Err(err) => {
            let _ = ready.send(Err(err));
            return;
        }
    };
    let mut stream = Box::pin(stream);
    'serving: loop {
        tokio::select! {
            request = requests.next() => {
                // `None` means the handle was dropped.
                if request.is_none() {
                    break 'serving;
                }
                tokio::select! {
                    item = stream.next() => {
                        let answer = match item {
                            Some(Ok(value)) => StreamItem::Item(value),
                            Some(Err(err)) => StreamItem::Error(err),
                            None => StreamItem::End,
                        };
                        // An error or an end is the last thing this stream
                        // has to say.
                        let last = !matches!(answer, StreamItem::Item(_));
                        if items.send(answer).is_err() || last {
                            break 'serving;
                        }
                    }
                    _ = &mut cancelled => break 'serving,
                    _ = shutdown.changed() => break 'serving,
                }
            }
            _ = &mut cancelled => break 'serving,
            _ = shutdown.changed() => break 'serving,
        }
    }
    // Dropping `stream` ends the subscription, and dropping `items`
    // disconnects whoever is blocked in `next`.
}

/// The consumer's end of the protocol. Behind a mutex, because `next` is the
/// only thing that touches it and two threads calling `next` at once would
/// otherwise race on `outstanding`.
struct Consumer<T> {
    requests: mpsc::UnboundedSender<()>,
    items: sync_mpsc::Receiver<StreamItem<T>>,
    /// Whether a request is already waiting to be answered.
    outstanding: bool,
}

/// A stream a foreign thread pulls from.
pub struct StreamHandle<T> {
    consumer: Mutex<Consumer<T>>,
    /// Dropping the sender cancels the serving task. Behind its own mutex so
    /// that `close` never waits on a consumer that is blocked in `next`.
    cancel: Mutex<Option<oneshot::Sender<()>>>,
}

impl<T> StreamHandle<T> {
    pub(crate) fn new(
        requests: mpsc::UnboundedSender<()>,
        items: sync_mpsc::Receiver<StreamItem<T>>,
        cancel: oneshot::Sender<()>,
    ) -> StreamHandle<T> {
        StreamHandle {
            consumer: Mutex::new(Consumer {
                requests,
                items,
                outstanding: false,
            }),
            cancel: Mutex::new(Some(cancel)),
        }
    }

    /// Asks for the next item and waits for it. `timeout` of `None` waits
    /// indefinitely.
    fn next(&self, timeout: Option<Duration>) -> Next<T> {
        // A panic in another caller must not make the stream unusable; the
        // data behind the mutex is still consistent.
        let mut consumer = self
            .consumer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !consumer.outstanding {
            if consumer.requests.unbounded_send(()).is_err() {
                return Next::End;
            }
            consumer.outstanding = true;
        }
        let answer = match timeout {
            None => consumer
                .items
                .recv()
                .map_err(|_| sync_mpsc::RecvTimeoutError::Disconnected),
            Some(timeout) => consumer.items.recv_timeout(timeout),
        };
        match answer {
            Ok(item) => {
                consumer.outstanding = false;
                match item {
                    StreamItem::Item(value) => Next::Item(value),
                    StreamItem::Error(err) => Next::Error(err),
                    StreamItem::End => Next::End,
                }
            }
            // The request stays outstanding, so the item it is waiting for
            // reaches the next call instead of being lost.
            Err(sync_mpsc::RecvTimeoutError::Timeout) => Next::TimedOut,
            Err(sync_mpsc::RecvTimeoutError::Disconnected) => Next::End,
        }
    }

    /// Stops the stream. Idempotent, and safe from any thread.
    fn close(&self) {
        let mut cancel = self
            .cancel
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(cancel.take());
    }
}

/// Turns a `timeout_ms` argument into a duration, where 0 means "wait".
fn timeout_of(timeout_ms: u64) -> Option<Duration> {
    (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms))
}

// ===========================================================================
// Notifications
// ===========================================================================

/// What happened to a record.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum oxia_notification_type {
    /// A record was created. `version_id` is set.
    OXIA_NOTIFICATION_KEY_CREATED = 0,
    /// A record was modified. `version_id` is set.
    OXIA_NOTIFICATION_KEY_MODIFIED = 1,
    /// A record was deleted.
    OXIA_NOTIFICATION_KEY_DELETED = 2,
    /// A range of records was deleted. `key` is the first key of the range
    /// and `key_range_last` the key it stops before.
    OXIA_NOTIFICATION_KEY_RANGE_DELETED = 3,
    /// The server's retention deleted notifications before this subscription
    /// could read them, so any record may have changed without a
    /// notification for it. No key is carried: a consumer keeping state from
    /// the notifications has to re-read it.
    OXIA_NOTIFICATION_MISSED = 4,
    /// A notification this version of the library does not recognize.
    /// Reported rather than dropped, so a consumer knows something happened.
    OXIA_NOTIFICATION_UNKNOWN = 5,
}

/// One change to the database.
#[repr(C)]
pub struct oxia_notification {
    /// What happened.
    pub type_: oxia_notification_type,
    /// The key it happened to; `data` is `NULL` for
    /// `OXIA_NOTIFICATION_MISSED`.
    pub key: oxia_bytes,
    /// Whether `version_id` is set, which it is for a creation and a
    /// modification.
    pub has_version_id: bool,
    /// The version the record now has.
    pub version_id: i64,
    /// The end of the deleted range, for
    /// `OXIA_NOTIFICATION_KEY_RANGE_DELETED`; `data` is `NULL` otherwise, and
    /// also when the range runs to the end of the keyspace.
    pub key_range_last: oxia_bytes,
}

impl oxia_notification {
    fn new(notification: &Notification) -> oxia_notification {
        let (type_, key, version_id, key_range_last) = match notification {
            Notification::KeyCreated { key, version_id } => (
                oxia_notification_type::OXIA_NOTIFICATION_KEY_CREATED,
                Some(key.as_str()),
                *version_id,
                None,
            ),
            Notification::KeyModified { key, version_id } => (
                oxia_notification_type::OXIA_NOTIFICATION_KEY_MODIFIED,
                Some(key.as_str()),
                *version_id,
                None,
            ),
            Notification::KeyDeleted { key } => (
                oxia_notification_type::OXIA_NOTIFICATION_KEY_DELETED,
                Some(key.as_str()),
                None,
                None,
            ),
            Notification::KeyRangeDeleted {
                key,
                key_range_last,
            } => (
                oxia_notification_type::OXIA_NOTIFICATION_KEY_RANGE_DELETED,
                Some(key.as_str()),
                None,
                key_range_last.as_deref(),
            ),
            Notification::NotificationsMissed => (
                oxia_notification_type::OXIA_NOTIFICATION_MISSED,
                None,
                None,
                None,
            ),
            // `Notification` is #[non_exhaustive]: surface a variant added
            // upstream instead of silently skipping it.
            _ => (
                oxia_notification_type::OXIA_NOTIFICATION_UNKNOWN,
                None,
                None,
                None,
            ),
        };
        oxia_notification {
            type_,
            key: optional_bytes(key),
            has_version_id: version_id.is_some(),
            version_id: version_id.unwrap_or(0),
            key_range_last: optional_bytes(key_range_last),
        }
    }

    unsafe fn free(self) {
        self.key.free();
        self.key_range_last.free();
    }
}

/// A subscription to every change in the namespace.
///
/// Opaque to C: created by `oxia_client_notifications`, finished with
/// `oxia_notifications_close` and released with `oxia_notifications_free`.
pub struct oxia_notifications(StreamHandle<Notification>);

/// How a notifications subscription behaves.
#[repr(C)]
pub struct oxia_notifications_options {
    /// How many notifications the client buffers before it applies
    /// backpressure to the server. 0 selects the client's default.
    pub buffer_size: usize,
}

/// Subscribes to every change applied to the namespace.
///
/// A subscription starts from the moment it is opened; changes made before
/// are not replayed. On `OXIA_OK`, `*subscription` owns a handle the caller
/// releases with `oxia_notifications_free`; on any error it is set to `NULL`.
///
/// # Safety
///
/// `client` must be an open client, `options` must point at an initialized
/// options struct, `subscription` must point at a writable pointer, and
/// `error_message` must be `NULL` or point at one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_notifications(
    client: *const oxia_client,
    options: *const oxia_notifications_options,
    subscription: *mut *mut oxia_notifications,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        if subscription.is_null() {
            return invalid(error_message, "the subscription out-parameter is NULL");
        }
        *subscription = std::ptr::null_mut();
        let Some(client) = client.as_ref() else {
            return invalid(error_message, "client is NULL");
        };
        let Some(options) = options.as_ref() else {
            return invalid(error_message, "options is NULL");
        };
        let buffer_size = options.buffer_size;
        let opened = client.open_stream(move |native| async move {
            let mut builder = native.notifications();
            if buffer_size > 0 {
                builder = builder.buffer_size(buffer_size);
            }
            builder.await.map(|stream| stream.map(Ok))
        });
        match opened {
            Ok(handle) => {
                *subscription = Box::into_raw(Box::new(oxia_notifications(handle)));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Waits for the next notification.
///
/// Returns `OXIA_OK` with `*notification` set, which the caller frees with
/// `oxia_notification_free`; `OXIA_ERR_TIMEOUT` when `timeout_ms` passed with
/// nothing to report, which is not an error and leaves the subscription
/// usable; or `OXIA_STREAM_END` once the subscription is finished. A
/// `timeout_ms` of 0 waits indefinitely.
///
/// One thread at a time per subscription: a second caller blocks until the
/// first returns.
///
/// # Safety
///
/// `subscription` must be a handle that has not been freed, `notification`
/// must point at a writable pointer, and `error_message` must be `NULL` or
/// point at one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_notifications_next(
    subscription: *mut oxia_notifications,
    timeout_ms: u64,
    notification: *mut *mut oxia_notification,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        if notification.is_null() {
            return invalid(error_message, "the notification out-parameter is NULL");
        }
        *notification = std::ptr::null_mut();
        let Some(subscription) = subscription.as_ref() else {
            return invalid(error_message, "subscription is NULL");
        };
        match subscription.0.next(timeout_of(timeout_ms)) {
            Next::Item(item) => {
                *notification = Box::into_raw(Box::new(oxia_notification::new(&item)));
                oxia_error_code::OXIA_OK
            }
            Next::Error(error) => fail(error_message, &error),
            Next::TimedOut => oxia_error_code::OXIA_ERR_TIMEOUT,
            Next::End => oxia_error_code::OXIA_STREAM_END,
        }
    })
}

/// Stops a subscription.
///
/// Idempotent, safe to call from any thread, and safe to call while another
/// thread is blocked in `oxia_notifications_next` — that call returns
/// `OXIA_STREAM_END`. The handle stays valid and must still be released with
/// `oxia_notifications_free`.
///
/// # Safety
///
/// `subscription` must be `NULL`, or a handle that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_notifications_close(subscription: *mut oxia_notifications) {
    guard_void(|| {
        if let Some(subscription) = subscription.as_ref() {
            subscription.0.close();
        }
    });
}

/// Releases a subscription, stopping it first if it is still running.
///
/// Does nothing when `subscription` is `NULL`. No call on the handle may be
/// in progress on another thread: use `oxia_notifications_close` to end a
/// blocked `oxia_notifications_next` first, and free the handle once that
/// call has returned.
///
/// # Safety
///
/// `subscription` must be `NULL`, or a handle that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_notifications_free(subscription: *mut oxia_notifications) {
    guard_void(|| {
        if !subscription.is_null() {
            drop(Box::from_raw(subscription));
        }
    });
}

/// Frees a notification. Does nothing when `notification` is `NULL`.
///
/// # Safety
///
/// `notification` must have come from `oxia_notifications_next` and must not
/// have been freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_notification_free(notification: *mut oxia_notification) {
    guard_void(|| {
        if !notification.is_null() {
            Box::from_raw(notification).free();
        }
    });
}

// ===========================================================================
// Key streams: sequence updates, and a streamed list
// ===========================================================================

/// A stream of keys.
///
/// Opaque to C. Produced by `oxia_client_sequence_updates`, which never ends
/// on its own, and by `oxia_client_list_stream`, which ends when the range is
/// exhausted. Finished with `oxia_key_stream_close` and released with
/// `oxia_key_stream_free`.
pub struct oxia_key_stream(StreamHandle<String>);

/// Which sequence to follow.
#[repr(C)]
pub struct oxia_sequence_updates_options {
    /// The key whose sequence to follow, without the generated suffix.
    /// Required.
    pub key: oxia_slice,
    /// The partition key the sequence is written under. Required: a sequence
    /// lives on one shard.
    pub partition_key: oxia_slice,
    /// How many updates the client buffers before it applies backpressure to
    /// the server. 0 selects the client's default.
    pub buffer_size: usize,
}

/// Follows the highest key of a sequence as the server appends to it.
///
/// On `OXIA_OK`, `*stream` owns a handle the caller releases with
/// `oxia_key_stream_free`; on any error it is set to `NULL`.
///
/// # Safety
///
/// As `oxia_client_notifications`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_sequence_updates(
    client: *const oxia_client,
    options: *const oxia_sequence_updates_options,
    stream: *mut *mut oxia_key_stream,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        if stream.is_null() {
            return invalid(error_message, "the stream out-parameter is NULL");
        }
        *stream = std::ptr::null_mut();
        let Some(client) = client.as_ref() else {
            return invalid(error_message, "client is NULL");
        };
        let Some(options) = options.as_ref() else {
            return invalid(error_message, "options is NULL");
        };
        let key = match required_str(options.key, "key") {
            Ok(key) => key.to_string(),
            Err(text) => return invalid(error_message, text),
        };
        let partition_key = match required_str(options.partition_key, "partition_key") {
            Ok(partition_key) => partition_key.to_string(),
            Err(text) => return invalid(error_message, text),
        };
        let buffer_size = options.buffer_size;
        let opened = client.open_stream(move |native| async move {
            let mut builder = native.sequence_updates(key, partition_key);
            if buffer_size > 0 {
                builder = builder.buffer_size(buffer_size);
            }
            builder.await.map(|stream| stream.map(Ok))
        });
        match opened {
            Ok(handle) => {
                *stream = Box::into_raw(Box::new(oxia_key_stream(handle)));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Lists the keys in a range as a stream, instead of collecting them like
/// `oxia_client_list`.
///
/// Keys arrive in the key order of the namespace, merged across shards. Use
/// this for a range too large to hold in memory at once; it costs a round
/// trip to the client's thread per key, so `oxia_client_list` is faster for a
/// range that fits.
///
/// # Safety
///
/// As `oxia_client_notifications`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_list_stream(
    client: *const oxia_client,
    options: *const oxia_range_options,
    stream: *mut *mut oxia_key_stream,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        if stream.is_null() {
            return invalid(error_message, "the stream out-parameter is NULL");
        }
        *stream = std::ptr::null_mut();
        let Some(client) = client.as_ref() else {
            return invalid(error_message, "client is NULL");
        };
        let request = match RangeRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        match client.open_stream(move |native| request.list_stream(native)) {
            Ok(handle) => {
                *stream = Box::into_raw(Box::new(oxia_key_stream(handle)));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Waits for the next key.
///
/// Returns `OXIA_OK` with `*key` set, which the caller frees with
/// `oxia_bytes_free`; `OXIA_ERR_TIMEOUT` when `timeout_ms` passed with
/// nothing to report; or `OXIA_STREAM_END` once the stream is finished. A
/// `timeout_ms` of 0 waits indefinitely.
///
/// One thread at a time per stream.
///
/// # Safety
///
/// `stream` must be a handle that has not been freed, `key` must point at a
/// writable `oxia_bytes`, and `error_message` must be `NULL` or point at a
/// writable `char *`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_key_stream_next(
    stream: *mut oxia_key_stream,
    timeout_ms: u64,
    key: *mut oxia_bytes,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        if key.is_null() {
            return invalid(error_message, "the key out-parameter is NULL");
        }
        *key = oxia_bytes::none();
        let Some(stream) = stream.as_ref() else {
            return invalid(error_message, "stream is NULL");
        };
        match stream.0.next(timeout_of(timeout_ms)) {
            Next::Item(item) => {
                *key = oxia_bytes::copy_from(item.as_bytes());
                oxia_error_code::OXIA_OK
            }
            Next::Error(error) => fail(error_message, &error),
            Next::TimedOut => oxia_error_code::OXIA_ERR_TIMEOUT,
            Next::End => oxia_error_code::OXIA_STREAM_END,
        }
    })
}

/// Stops a key stream. As `oxia_notifications_close`.
///
/// # Safety
///
/// `stream` must be `NULL`, or a handle that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_key_stream_close(stream: *mut oxia_key_stream) {
    guard_void(|| {
        if let Some(stream) = stream.as_ref() {
            stream.0.close();
        }
    });
}

/// Releases a key stream. As `oxia_notifications_free`.
///
/// # Safety
///
/// `stream` must be `NULL`, or a handle that has not been freed, with no call
/// on it in progress on another thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_key_stream_free(stream: *mut oxia_key_stream) {
    guard_void(|| {
        if !stream.is_null() {
            drop(Box::from_raw(stream));
        }
    });
}

// ===========================================================================
// Record streams: a streamed range scan
// ===========================================================================

/// A stream of records, from `oxia_client_range_scan_stream`.
///
/// Opaque to C. Finished with `oxia_record_stream_close` and released with
/// `oxia_record_stream_free`.
pub struct oxia_record_stream(StreamHandle<GetResult>);

/// Reads every record in a range as a stream, instead of collecting them
/// like `oxia_client_range_scan`.
///
/// Records arrive in the key order of the namespace, merged across shards.
/// Use this for a range too large to hold in memory at once; it costs a round
/// trip to the client's thread per record.
///
/// # Safety
///
/// As `oxia_client_notifications`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_client_range_scan_stream(
    client: *const oxia_client,
    options: *const oxia_range_options,
    stream: *mut *mut oxia_record_stream,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        if stream.is_null() {
            return invalid(error_message, "the stream out-parameter is NULL");
        }
        *stream = std::ptr::null_mut();
        let Some(client) = client.as_ref() else {
            return invalid(error_message, "client is NULL");
        };
        let request = match RangeRequest::parse(options) {
            Ok(request) => request,
            Err(text) => return invalid(error_message, text),
        };
        match client.open_stream(move |native| request.range_scan_stream(native)) {
            Ok(handle) => {
                *stream = Box::into_raw(Box::new(oxia_record_stream(handle)));
                oxia_error_code::OXIA_OK
            }
            Err(error) => fail(error_message, &error),
        }
    })
}

/// Waits for the next record.
///
/// Returns `OXIA_OK` with `*record` set, which the caller frees with
/// `oxia_get_result_free`; `OXIA_ERR_TIMEOUT` when `timeout_ms` passed with
/// nothing to report; or `OXIA_STREAM_END` once the stream is finished. A
/// `timeout_ms` of 0 waits indefinitely.
///
/// One thread at a time per stream.
///
/// # Safety
///
/// `stream` must be a handle that has not been freed, `record` must point at
/// a writable pointer, and `error_message` must be `NULL` or point at a
/// writable `char *`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_record_stream_next(
    stream: *mut oxia_record_stream,
    timeout_ms: u64,
    record: *mut *mut oxia_get_result,
    error_message: *mut *mut c_char,
) -> oxia_error_code {
    guard(error_message, || {
        if record.is_null() {
            return invalid(error_message, "the record out-parameter is NULL");
        }
        *record = std::ptr::null_mut();
        let Some(stream) = stream.as_ref() else {
            return invalid(error_message, "stream is NULL");
        };
        match stream.0.next(timeout_of(timeout_ms)) {
            Next::Item(item) => {
                *record = Box::into_raw(Box::new(oxia_get_result::new(&item)));
                oxia_error_code::OXIA_OK
            }
            Next::Error(error) => fail(error_message, &error),
            Next::TimedOut => oxia_error_code::OXIA_ERR_TIMEOUT,
            Next::End => oxia_error_code::OXIA_STREAM_END,
        }
    })
}

/// Stops a record stream. As `oxia_notifications_close`.
///
/// # Safety
///
/// `stream` must be `NULL`, or a handle that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_record_stream_close(stream: *mut oxia_record_stream) {
    guard_void(|| {
        if let Some(stream) = stream.as_ref() {
            stream.0.close();
        }
    });
}

/// Releases a record stream. As `oxia_notifications_free`.
///
/// # Safety
///
/// `stream` must be `NULL`, or a handle that has not been freed, with no call
/// on it in progress on another thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_record_stream_free(stream: *mut oxia_record_stream) {
    guard_void(|| {
        if !stream.is_null() {
            drop(Box::from_raw(stream));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    #[test]
    fn a_zero_timeout_waits_indefinitely() {
        assert_eq!(timeout_of(0), None);
        assert_eq!(timeout_of(25), Some(Duration::from_millis(25)));
    }

    #[test]
    fn null_handles_are_rejected_rather_than_dereferenced() {
        let mut message: *mut c_char = ptr::null_mut();
        let mut notification: *mut oxia_notification = ptr::null_mut();
        assert_eq!(
            unsafe { oxia_notifications_next(ptr::null_mut(), 0, &mut notification, &mut message) },
            oxia_error_code::OXIA_ERR_INVALID_ARGUMENT
        );
        assert!(notification.is_null());
        unsafe { crate::oxia_string_free(message) };

        let mut key = oxia_bytes::none();
        assert_eq!(
            unsafe { oxia_key_stream_next(ptr::null_mut(), 0, &mut key, ptr::null_mut()) },
            oxia_error_code::OXIA_ERR_INVALID_ARGUMENT
        );
        let mut record: *mut oxia_get_result = ptr::null_mut();
        assert_eq!(
            unsafe { oxia_record_stream_next(ptr::null_mut(), 0, &mut record, ptr::null_mut()) },
            oxia_error_code::OXIA_ERR_INVALID_ARGUMENT
        );
    }

    #[test]
    fn a_null_out_parameter_is_rejected() {
        let mut message: *mut c_char = ptr::null_mut();
        assert_eq!(
            unsafe { oxia_notifications_next(ptr::null_mut(), 0, ptr::null_mut(), &mut message) },
            oxia_error_code::OXIA_ERR_INVALID_ARGUMENT
        );
        assert!(
            unsafe { std::ffi::CStr::from_ptr(message) }
                .to_string_lossy()
                .contains("out-parameter")
        );
        unsafe { crate::oxia_string_free(message) };
    }

    #[test]
    fn closing_and_freeing_null_handles_is_a_no_op() {
        unsafe {
            oxia_notifications_close(ptr::null_mut());
            oxia_notifications_free(ptr::null_mut());
            oxia_key_stream_close(ptr::null_mut());
            oxia_key_stream_free(ptr::null_mut());
            oxia_record_stream_close(ptr::null_mut());
            oxia_record_stream_free(ptr::null_mut());
            oxia_notification_free(ptr::null_mut());
        }
    }

    #[test]
    fn a_missed_notification_carries_no_key() {
        let converted = oxia_notification::new(&Notification::NotificationsMissed);
        assert_eq!(
            converted.type_,
            oxia_notification_type::OXIA_NOTIFICATION_MISSED
        );
        assert!(converted.key.data.is_null());
        assert!(!converted.has_version_id);
        unsafe { converted.free() };
    }

    #[test]
    fn a_creation_carries_its_key_and_version() {
        let converted = oxia_notification::new(&Notification::KeyCreated {
            key: "a/b".to_string(),
            version_id: Some(7),
        });
        assert_eq!(
            converted.type_,
            oxia_notification_type::OXIA_NOTIFICATION_KEY_CREATED
        );
        assert_eq!(converted.key.len, 3);
        assert!(converted.has_version_id);
        assert_eq!(converted.version_id, 7);
        assert!(converted.key_range_last.data.is_null());
        unsafe { converted.free() };
    }

    #[test]
    fn a_range_deletion_carries_both_ends() {
        let converted = oxia_notification::new(&Notification::KeyRangeDeleted {
            key: "a/".to_string(),
            key_range_last: Some("a/z".to_string()),
        });
        assert_eq!(
            converted.type_,
            oxia_notification_type::OXIA_NOTIFICATION_KEY_RANGE_DELETED
        );
        assert_eq!(converted.key_range_last.len, 3);
        unsafe { converted.free() };
    }

    #[test]
    fn a_deletion_reports_no_version() {
        let converted = oxia_notification::new(&Notification::KeyDeleted {
            key: "a/b".to_string(),
        });
        assert_eq!(
            converted.type_,
            oxia_notification_type::OXIA_NOTIFICATION_KEY_DELETED
        );
        assert!(!converted.has_version_id);
        assert_eq!(converted.version_id, 0);
        unsafe { converted.free() };
    }
}
