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

#![allow(clippy::not_unsafe_ptr_arg_deref)]
use futures::channel::mpsc;
use futures::future::LocalBoxFuture;
use futures::StreamExt;
use std::ffi::{c_char, CStr, CString};
use std::future::Future;
use std::slice;
use std::thread::JoinHandle;

use oxia::OxiaClient as NativeOxiaClient;
use oxia::{OxiaClientBuilder, OxiaError};

/// A call to run on the client's thread.
type Job = Box<dyn FnOnce(NativeOxiaClient) -> LocalBoxFuture<'static, ()> + Send>;

/// A client and the thread it runs on. The native client is single-threaded:
/// it lives on a thread of its own, in a compio runtime, and every call runs
/// there while the calling thread waits for its result. Calls from several
/// threads run concurrently, and batch together.
pub struct OxiaClient {
    jobs: mpsc::UnboundedSender<Job>,
    worker: Option<JoinHandle<()>>,
}

impl OxiaClient {
    /// Builds the client on a new thread, and returns once it is connected.
    fn start(builder: OxiaClientBuilder) -> Result<OxiaClient, OxiaError> {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (jobs, mut job_rx) = mpsc::unbounded::<Job>();
        let worker = std::thread::spawn(move || {
            let runtime = match compio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(err) => {
                    let _ = ready_tx.send(Err(OxiaError::Disconnected(err.to_string())));
                    return;
                }
            };
            runtime.block_on(async move {
                let client = match builder.build().await {
                    Ok(client) => client,
                    Err(err) => {
                        let _ = ready_tx.send(Err(err));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));
                while let Some(job) = job_rx.next().await {
                    compio::runtime::spawn(job(client.clone())).detach();
                }
            });
        });
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(OxiaClient {
                jobs,
                worker: Some(worker),
            }),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(OxiaError::Closed),
        }
    }

    /// Runs `op` on the client's thread and waits for its result.
    fn run<T, F, Fut>(&self, op: F) -> Result<T, OxiaError>
    where
        T: Send + 'static,
        F: FnOnce(NativeOxiaClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, OxiaError>> + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        let job: Job = Box::new(move |client| {
            Box::pin(async move {
                let _ = tx.send(op(client).await);
            })
        });
        if self.jobs.unbounded_send(job).is_err() {
            return Err(OxiaError::Closed);
        }
        // The job is dropped unanswered if the client's thread has stopped.
        rx.recv().unwrap_or(Err(OxiaError::Closed))
    }
}

impl Drop for OxiaClient {
    fn drop(&mut self) {
        // Stops the client's thread once the calls in progress have returned,
        // tearing down the client.
        self.jobs.close_channel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[repr(C)]
pub struct COxiaClientOptions {
    pub service_address: *const c_char,
    pub namespace: *const c_char,
}

#[repr(C)]
pub struct COxiaPutResult {
    pub key: *mut c_char,
    pub version_id: i64,
}

#[repr(C)]
pub struct COxiaGetResult {
    pub key: *mut c_char,
    pub value: *mut u8,
    pub value_len: usize,
    pub version_id: i64,
}

#[repr(i32)]
#[derive(Debug, PartialEq)]
pub enum COxiaError {
    Ok = 0,
    KeyNotFound = 1,
    UnexpectedVersionId = 2,
    SessionExpired = 3,
    RequestTooLarge = 4,
    InvalidArgument = 5,
    LeaderNotFound = 6,
    NoShardForKey = 7,
    Disconnected = 8,
    Timeout = 9,
    Grpc = 10,
    Decode = 11,
    Closed = 12,
    Other = 13,
}

impl From<OxiaError> for COxiaError {
    fn from(error: OxiaError) -> Self {
        match error {
            OxiaError::KeyNotFound => COxiaError::KeyNotFound,
            OxiaError::UnexpectedVersionId => COxiaError::UnexpectedVersionId,
            OxiaError::SessionExpired => COxiaError::SessionExpired,
            OxiaError::RequestTooLarge => COxiaError::RequestTooLarge,
            OxiaError::InvalidArgument(_) => COxiaError::InvalidArgument,
            OxiaError::LeaderNotFound { .. } => COxiaError::LeaderNotFound,
            OxiaError::NoShardForKey { .. } => COxiaError::NoShardForKey,
            OxiaError::Disconnected(_) => COxiaError::Disconnected,
            OxiaError::Timeout => COxiaError::Timeout,
            OxiaError::Grpc { .. } => COxiaError::Grpc,
            OxiaError::Decode(_) => COxiaError::Decode,
            OxiaError::Closed => COxiaError::Closed,
            // `OxiaError` is #[non_exhaustive]; map any future variant here.
            _ => COxiaError::Other,
        }
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_new(
    options: COxiaClientOptions,
    client_ptr: *mut *mut OxiaClient,
) -> COxiaError {
    let service_address = unsafe {
        CStr::from_ptr(options.service_address)
            .to_str()
            .unwrap()
            .to_string()
    };
    let namespace = unsafe {
        CStr::from_ptr(options.namespace)
            .to_str()
            .unwrap()
            .to_string()
    };

    let res = OxiaClient::start(
        NativeOxiaClient::builder()
            .service_address(service_address)
            .namespace(namespace),
    );

    match res {
        Ok(client) => {
            unsafe {
                *client_ptr = Box::into_raw(Box::new(client));
            }
            COxiaError::Ok
        }
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_free(client: *mut OxiaClient) {
    if !client.is_null() {
        unsafe {
            let _ = Box::from_raw(client);
        };
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_put(
    client: *const OxiaClient,
    key: *const c_char,
    value: *const u8,
    value_len: usize,
    result_ptr: *mut *mut COxiaPutResult,
) -> COxiaError {
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap().to_string() };
    let value = unsafe { slice::from_raw_parts(value, value_len).to_vec() };
    let rust_client = unsafe { &*client };
    let result = rust_client.run(move |client| async move { client.put(key, value).await });

    match result {
        Ok(put_result) => {
            let c_result = COxiaPutResult {
                key: CString::new(put_result.key).unwrap().into_raw(),
                version_id: put_result.version.version_id,
            };
            unsafe {
                *result_ptr = Box::into_raw(Box::new(c_result));
            }
            COxiaError::Ok
        }
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_get(
    client: *const OxiaClient,
    key: *const c_char,
    result_ptr: *mut *mut COxiaGetResult,
) -> COxiaError {
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap().to_string() };
    let rust_client = unsafe { &*client };
    let result = rust_client.run(move |client| async move { client.get(key).await });

    match result {
        Ok(get_result) => {
            let key_str = CString::new(get_result.key).unwrap();
            let c_result = match get_result.value {
                Some(value) => {
                    // Copy out of the shared `Bytes` so the C side owns a plain
                    // allocation it can free with oxia_get_result_free.
                    let owned = value.to_vec();
                    let value_len = owned.len();
                    let value_ptr = owned.as_ptr() as *mut u8;
                    std::mem::forget(owned);
                    COxiaGetResult {
                        key: key_str.into_raw(),
                        value: value_ptr,
                        value_len,
                        version_id: get_result.version.version_id,
                    }
                }
                None => COxiaGetResult {
                    key: key_str.into_raw(),
                    value: std::ptr::null_mut(),
                    value_len: 0,
                    version_id: get_result.version.version_id,
                },
            };
            unsafe {
                *result_ptr = Box::into_raw(Box::new(c_result));
            }
            COxiaError::Ok
        }
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_shutdown(client: *mut OxiaClient) -> COxiaError {
    let rust_client = unsafe { Box::from_raw(client) };
    let res = rust_client.run(|client| async move { client.close().await });

    match res {
        Ok(_) => COxiaError::Ok,
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_put_result_free(result: *mut COxiaPutResult) {
    if !result.is_null() {
        let box_result = unsafe { Box::from_raw(result) };
        if !box_result.key.is_null() {
            unsafe {
                let _ = CString::from_raw(box_result.key);
            };
        }
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_delete(client: *const OxiaClient, key: *const c_char) -> COxiaError {
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap().to_string() };
    let rust_client = unsafe { &*client };
    let result = rust_client.run(move |client| async move { client.delete(key).await });

    match result {
        Ok(_) => COxiaError::Ok,
        Err(err) => COxiaError::from(err),
    }
}

#[repr(C)]
pub struct COxiaListResult {
    pub keys: *mut *mut c_char,
    pub keys_len: usize,
}

#[no_mangle]
pub extern "C" fn oxia_client_list(
    client: *const OxiaClient,
    min_key_inclusive: *const c_char,
    max_key_exclusive: *const c_char,
    result_ptr: *mut *mut COxiaListResult,
) -> COxiaError {
    let min_key = unsafe {
        CStr::from_ptr(min_key_inclusive)
            .to_str()
            .unwrap()
            .to_string()
    };
    let max_key = unsafe {
        CStr::from_ptr(max_key_exclusive)
            .to_str()
            .unwrap()
            .to_string()
    };
    let rust_client = unsafe { &*client };
    let result = rust_client.run(move |client| async move { client.list(min_key, max_key).await });

    match result {
        Ok(keys) => {
            let c_keys: Vec<*mut c_char> = keys
                .into_iter()
                .map(|k| CString::new(k).unwrap().into_raw())
                .collect();
            let keys_len = c_keys.len();
            let keys_ptr = c_keys.as_ptr() as *mut *mut c_char;
            std::mem::forget(c_keys);
            let c_result = COxiaListResult {
                keys: keys_ptr,
                keys_len,
            };
            unsafe {
                *result_ptr = Box::into_raw(Box::new(c_result));
            }
            COxiaError::Ok
        }
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_list_result_free(result: *mut COxiaListResult) {
    if !result.is_null() {
        let box_result = unsafe { Box::from_raw(result) };
        if !box_result.keys.is_null() {
            let keys = unsafe {
                Vec::from_raw_parts(box_result.keys, box_result.keys_len, box_result.keys_len)
            };
            for key in keys {
                if !key.is_null() {
                    unsafe {
                        let _ = CString::from_raw(key);
                    }
                }
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn oxia_client_delete_range(
    client: *const OxiaClient,
    min_key_inclusive: *const c_char,
    max_key_exclusive: *const c_char,
) -> COxiaError {
    let min_key = unsafe {
        CStr::from_ptr(min_key_inclusive)
            .to_str()
            .unwrap()
            .to_string()
    };
    let max_key = unsafe {
        CStr::from_ptr(max_key_exclusive)
            .to_str()
            .unwrap()
            .to_string()
    };
    let rust_client = unsafe { &*client };
    let result =
        rust_client.run(move |client| async move { client.delete_range(min_key, max_key).await });

    match result {
        Ok(_) => COxiaError::Ok,
        Err(err) => COxiaError::from(err),
    }
}

#[no_mangle]
pub extern "C" fn oxia_get_result_free(result: *mut COxiaGetResult) {
    if !result.is_null() {
        let box_result = unsafe { Box::from_raw(result) };
        if !box_result.key.is_null() {
            unsafe {
                let _ = CString::from_raw(box_result.key);
            };
        }
        if !box_result.value.is_null() {
            unsafe {
                let _ = Vec::from_raw_parts(
                    box_result.value,
                    box_result.value_len,
                    box_result.value_len,
                );
            }
        }
    }
}
