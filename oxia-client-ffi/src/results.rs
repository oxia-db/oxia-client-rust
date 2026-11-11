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

//! What the operations hand back, and the functions that free it.
//!
//! A blocking operation writes an owned result through its out-parameter,
//! which the caller frees with the matching `_free`. An asynchronous
//! operation passes the same structure to its callback by const pointer,
//! borrowed for the duration of the call: the library frees it afterwards,
//! so a callback that needs the data past its return must copy it.

use oxia::{GetResult, PutResult, Version};

use crate::error::guard_void;
use crate::mem::{from_array, into_array, optional_bytes, oxia_bytes};

/// The version metadata of a record.
#[repr(C)]
pub struct oxia_version {
    /// Changes on every modification; the value to pass as
    /// `expected_version_id` for a compare-and-swap.
    pub version_id: i64,
    /// How many times the record has been modified since it was created.
    pub modifications_count: i64,
    /// When the record was created, in milliseconds since the epoch.
    pub created_timestamp: u64,
    /// When the record was last modified, in milliseconds since the epoch.
    pub modified_timestamp: u64,
    /// Whether the record is ephemeral, i.e. bound to a client session.
    pub is_ephemeral: bool,
    /// The owning session, meaningful only when `is_ephemeral` is true.
    pub session_id: i64,
    /// The identity of the client that last wrote the record — the one it
    /// set as `identity`, or a generated id. Recorded for ephemeral records
    /// only; `data` is `NULL` for every other record.
    pub client_identity: oxia_bytes,
}

impl oxia_version {
    pub(crate) fn new(version: &Version) -> oxia_version {
        oxia_version {
            version_id: version.version_id,
            modifications_count: version.modifications_count,
            created_timestamp: version.created_timestamp,
            modified_timestamp: version.modified_timestamp,
            is_ephemeral: version.is_ephemeral(),
            session_id: version.session_id.unwrap_or(0),
            client_identity: optional_bytes(version.client_identity.as_deref()),
        }
    }

    unsafe fn free(self) {
        self.client_identity.free();
    }
}

/// What a put stored.
#[repr(C)]
pub struct oxia_put_result {
    /// The key written. Differs from the requested key when the server
    /// generated it from sequence key deltas.
    pub key: oxia_bytes,
    /// The version of the record just written.
    pub version: oxia_version,
}

impl oxia_put_result {
    pub(crate) fn new(result: &PutResult) -> oxia_put_result {
        oxia_put_result {
            key: oxia_bytes::copy_from(result.key.as_bytes()),
            version: oxia_version::new(&result.version),
        }
    }

    pub(crate) unsafe fn free(self) {
        self.key.free();
        self.version.free();
    }
}

/// A record a get or a range scan returned.
#[repr(C)]
pub struct oxia_get_result {
    /// The key of the record. Differs from the requested key for a
    /// non-`EQUAL` comparison, such as a floor or ceiling query.
    pub key: oxia_bytes,
    /// Whether the response carried a value. False when the query asked not
    /// to have one (`exclude_value`), and also when the record carries no
    /// value.
    ///
    /// A record stored with an empty value reads back as `has_value` false:
    /// the server leaves the field out of the response for an empty value
    /// just as it does for no value, so the two cannot be told apart here.
    pub has_value: bool,
    /// The value, when `has_value` is true.
    pub value: oxia_bytes,
    /// The version metadata of the record.
    pub version: oxia_version,
    /// The matching secondary-index key when the query used an index;
    /// `data` is `NULL` otherwise.
    pub secondary_index_key: oxia_bytes,
}

impl oxia_get_result {
    pub(crate) fn new(result: &GetResult) -> oxia_get_result {
        oxia_get_result {
            key: oxia_bytes::copy_from(result.key.as_bytes()),
            has_value: result.value.is_some(),
            value: match &result.value {
                // Copied out of the shared `Bytes` so the caller owns a plain
                // allocation it can free on its own.
                Some(value) => oxia_bytes::copy_from(value),
                None => oxia_bytes::none(),
            },
            version: oxia_version::new(&result.version),
            secondary_index_key: optional_bytes(result.secondary_index_key.as_deref()),
        }
    }

    pub(crate) unsafe fn free(self) {
        self.key.free();
        self.value.free();
        self.version.free();
        self.secondary_index_key.free();
    }
}

/// The keys a list returned, in the key order of the namespace.
#[repr(C)]
pub struct oxia_key_list {
    /// The keys; `NULL` when `len` is 0.
    pub keys: *mut oxia_bytes,
    /// How many keys there are.
    pub len: usize,
}

impl oxia_key_list {
    pub(crate) fn new(keys: &[String]) -> oxia_key_list {
        let (keys, len) = into_array(
            keys.iter()
                .map(|key| oxia_bytes::copy_from(key.as_bytes()))
                .collect(),
        );
        oxia_key_list { keys, len }
    }

    pub(crate) unsafe fn free(self) {
        for key in from_array(self.keys, self.len) {
            key.free();
        }
    }
}

/// The records a range scan returned, in the key order of the namespace.
#[repr(C)]
pub struct oxia_record_list {
    /// The records; `NULL` when `len` is 0.
    pub records: *mut oxia_get_result,
    /// How many records there are.
    pub len: usize,
}

impl oxia_record_list {
    pub(crate) fn new(records: &[GetResult]) -> oxia_record_list {
        let (records, len) = into_array(records.iter().map(oxia_get_result::new).collect());
        oxia_record_list { records, len }
    }

    pub(crate) unsafe fn free(self) {
        for record in from_array(self.records, self.len) {
            record.free();
        }
    }
}

/// Frees a result from `oxia_client_put`. Does nothing when `result` is
/// `NULL`.
///
/// # Safety
///
/// `result` must have come from `oxia_client_put` and must not have been
/// freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_put_result_free(result: *mut oxia_put_result) {
    guard_void(|| {
        if !result.is_null() {
            Box::from_raw(result).free();
        }
    });
}

/// Frees a result from `oxia_client_get`. Does nothing when `result` is
/// `NULL`.
///
/// # Safety
///
/// `result` must have come from `oxia_client_get` and must not have been
/// freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_get_result_free(result: *mut oxia_get_result) {
    guard_void(|| {
        if !result.is_null() {
            Box::from_raw(result).free();
        }
    });
}

/// Frees a result from `oxia_client_list`. Does nothing when `result` is
/// `NULL`.
///
/// # Safety
///
/// `result` must have come from `oxia_client_list` and must not have been
/// freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_key_list_free(result: *mut oxia_key_list) {
    guard_void(|| {
        if !result.is_null() {
            Box::from_raw(result).free();
        }
    });
}

/// Frees a result from `oxia_client_range_scan`. Does nothing when `result`
/// is `NULL`.
///
/// # Safety
///
/// `result` must have come from `oxia_client_range_scan` and must not have
/// been freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxia_record_list_free(result: *mut oxia_record_list) {
    guard_void(|| {
        if !result.is_null() {
            Box::from_raw(result).free();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;
    use std::slice;

    // `Version`, `PutResult` and `GetResult` are #[non_exhaustive], so they
    // cannot be built outside `oxia-client`: the conversions that read them
    // are covered against a real server in `tests/it.rs`, which is where the
    // interesting cases (an empty versus an absent value, a key holding a
    // NUL byte) actually come from.

    #[test]
    fn an_empty_key_list_carries_no_pointer() {
        let list = oxia_key_list::new(&[]);
        assert_eq!(list.len, 0);
        assert!(list.keys.is_null());
        unsafe { list.free() };
    }

    #[test]
    fn a_key_list_keeps_its_order_and_nul_terminates_each_key() {
        let list = oxia_key_list::new(&["a".to_string(), "bb".to_string()]);
        assert_eq!(list.len, 2);
        let keys = unsafe { slice::from_raw_parts(list.keys, list.len) };
        assert_eq!(keys[0].len, 1);
        assert_eq!(unsafe { slice::from_raw_parts(keys[0].data, 2) }, b"a\0");
        assert_eq!(keys[1].len, 2);
        assert_eq!(unsafe { slice::from_raw_parts(keys[1].data, 3) }, b"bb\0");
        unsafe { list.free() };
    }

    #[test]
    fn a_key_holding_a_nul_byte_keeps_its_length() {
        let list = oxia_key_list::new(&["a\0b".to_string()]);
        let keys = unsafe { slice::from_raw_parts(list.keys, list.len) };
        assert_eq!(keys[0].len, 3);
        assert_eq!(unsafe { slice::from_raw_parts(keys[0].data, 3) }, b"a\0b");
        unsafe { list.free() };
    }

    #[test]
    fn freeing_a_null_result_is_a_no_op() {
        unsafe {
            oxia_put_result_free(ptr::null_mut());
            oxia_get_result_free(ptr::null_mut());
            oxia_key_list_free(ptr::null_mut());
            oxia_record_list_free(ptr::null_mut());
        }
    }
}
