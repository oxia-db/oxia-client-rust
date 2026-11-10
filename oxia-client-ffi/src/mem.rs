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

//! The two byte-buffer types the ABI passes keys and values in, and the
//! conversions between them and Rust.

use std::ptr;
use std::slice;

/// Bytes borrowed from the caller, as a pointer and a length.
///
/// Keys and values cross the boundary as bytes, not as NUL-terminated
/// strings: an Oxia key is a protobuf `string`, so it is UTF-8 but may
/// contain a NUL byte, and a value is arbitrary binary.
///
/// `data` may be `NULL` only when `len` is 0. For the options that take an
/// optional string, a `NULL` `data` means "unset", while a non-`NULL` `data`
/// with `len` 0 means the empty string.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct oxia_slice {
    /// The first byte, or `NULL`.
    pub data: *const u8,
    /// The number of bytes.
    pub len: usize,
}

impl oxia_slice {
    /// An unset slice: what `memset`-ing an options struct to zero leaves.
    pub const fn none() -> oxia_slice {
        oxia_slice {
            data: ptr::null(),
            len: 0,
        }
    }
}

/// Bytes owned by the caller, freed by the `_free` function of the result
/// that carries them.
///
/// `data` points at `len` bytes followed by a NUL terminator that is not
/// counted in `len`, so a key or value without interior NUL bytes can be
/// passed straight to `printf("%s")`. Use `len` whenever it may contain
/// them. A `NULL` `data` means the field is absent.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct oxia_bytes {
    /// The first byte, or `NULL` when the field is absent.
    pub data: *mut u8,
    /// The number of bytes, excluding the NUL terminator.
    pub len: usize,
}

impl oxia_bytes {
    /// An absent field.
    pub const fn none() -> oxia_bytes {
        oxia_bytes {
            data: ptr::null_mut(),
            len: 0,
        }
    }

    /// Copies `src` into a fresh allocation owned by the caller.
    pub fn copy_from(src: &[u8]) -> oxia_bytes {
        // One allocation of `len + 1`, NUL-terminated. `with_capacity`
        // followed by exactly that many pushes leaves capacity == length, so
        // `into_boxed_slice` does not reallocate and the pointer can be
        // reclaimed as a `Box<[u8]>` of a known length.
        let mut buf = Vec::with_capacity(src.len() + 1);
        buf.extend_from_slice(src);
        buf.push(0);
        let data = Box::into_raw(buf.into_boxed_slice()) as *mut u8;
        oxia_bytes {
            data,
            len: src.len(),
        }
    }

    /// Frees what [`oxia_bytes::copy_from`] allocated.
    ///
    /// # Safety
    ///
    /// `self` must have come from [`oxia_bytes::copy_from`] and must not have
    /// been freed already.
    pub unsafe fn free(self) {
        if !self.data.is_null() {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                self.data,
                self.len + 1,
            )));
        }
    }
}

/// Copies an optional string, as an absent [`oxia_bytes`] when it is `None`.
pub fn optional_bytes(src: Option<&str>) -> oxia_bytes {
    match src {
        Some(s) => oxia_bytes::copy_from(s.as_bytes()),
        None => oxia_bytes::none(),
    }
}

/// Borrows the bytes of a required slice.
///
/// # Safety
///
/// `data` must point at `len` initialized bytes for the duration of `'a`.
pub unsafe fn required_bytes<'a>(s: oxia_slice, what: &str) -> Result<&'a [u8], String> {
    if s.data.is_null() {
        if s.len == 0 {
            // A zero-length slice is legitimately empty; `from_raw_parts`
            // would be undefined behaviour on a null pointer even so.
            return Ok(&[]);
        }
        return Err(format!("{what}: data is NULL but len is {}", s.len));
    }
    Ok(slice::from_raw_parts(s.data, s.len))
}

/// Borrows a required slice as the UTF-8 string the wire format needs.
///
/// # Safety
///
/// As [`required_bytes`].
pub unsafe fn required_str<'a>(s: oxia_slice, what: &str) -> Result<&'a str, String> {
    let bytes = required_bytes(s, what)?;
    str::from_utf8(bytes).map_err(|err| format!("{what}: not valid UTF-8 ({err})"))
}

/// Borrows an optional slice, `None` when `data` is `NULL`.
///
/// # Safety
///
/// As [`required_bytes`].
pub unsafe fn optional_str<'a>(s: oxia_slice, what: &str) -> Result<Option<&'a str>, String> {
    if s.data.is_null() {
        if s.len != 0 {
            return Err(format!("{what}: data is NULL but len is {}", s.len));
        }
        return Ok(None);
    }
    Ok(Some(required_str(s, what)?))
}

/// Borrows an array the caller passed as a pointer and a length.
///
/// # Safety
///
/// `data` must point at `len` initialized `T`s for the duration of `'a`.
pub unsafe fn required_array<'a, T>(
    data: *const T,
    len: usize,
    what: &str,
) -> Result<&'a [T], String> {
    if data.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(format!("{what}: array is NULL but its length is {len}"));
    }
    Ok(slice::from_raw_parts(data, len))
}

/// Hands an array of `T` to the caller as a pointer and a length.
pub fn into_array<T>(items: Vec<T>) -> (*mut T, usize) {
    let len = items.len();
    if len == 0 {
        return (ptr::null_mut(), 0);
    }
    (Box::into_raw(items.into_boxed_slice()) as *mut T, len)
}

/// Reclaims what [`into_array`] handed out, as a `Vec` that owns its items.
///
/// # Safety
///
/// `data` and `len` must come from [`into_array`] and must not have been
/// freed already.
pub unsafe fn from_array<T>(data: *mut T, len: usize) -> Vec<T> {
    if data.is_null() || len == 0 {
        return Vec::new();
    }
    Box::from_raw(ptr::slice_from_raw_parts_mut(data, len)).into_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_bytes_are_nul_terminated_and_keep_interior_nuls() {
        let bytes = oxia_bytes::copy_from(b"a\0b");
        assert_eq!(bytes.len, 3);
        let with_terminator = unsafe { slice::from_raw_parts(bytes.data, bytes.len + 1) };
        assert_eq!(with_terminator, b"a\0b\0");
        unsafe { bytes.free() };
    }

    #[test]
    fn an_empty_owned_buffer_is_still_a_valid_c_string() {
        let bytes = oxia_bytes::copy_from(b"");
        assert_eq!(bytes.len, 0);
        assert!(!bytes.data.is_null());
        assert_eq!(unsafe { *bytes.data }, 0);
        unsafe { bytes.free() };
    }

    #[test]
    fn a_null_slice_of_length_zero_borrows_as_empty() {
        let empty = unsafe { required_bytes(oxia_slice::none(), "key") }.unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn a_null_slice_with_a_length_is_rejected() {
        let bogus = oxia_slice {
            data: ptr::null(),
            len: 4,
        };
        let err = unsafe { required_bytes(bogus, "value") }.unwrap_err();
        assert!(err.contains("value"), "{err}");
    }

    #[test]
    fn a_non_utf8_key_is_rejected_rather_than_panicking() {
        let raw = [b't', 0xff, 0xfe];
        let slice = oxia_slice {
            data: raw.as_ptr(),
            len: raw.len(),
        };
        let err = unsafe { required_str(slice, "key") }.unwrap_err();
        assert!(err.contains("UTF-8"), "{err}");
    }

    #[test]
    fn an_unset_optional_string_differs_from_an_empty_one() {
        assert_eq!(
            unsafe { optional_str(oxia_slice::none(), "partition_key") }.unwrap(),
            None
        );
        let empty = oxia_slice {
            data: b"".as_ptr(),
            len: 0,
        };
        assert_eq!(
            unsafe { optional_str(empty, "partition_key") }.unwrap(),
            Some("")
        );
    }

    #[test]
    fn arrays_round_trip_through_the_boundary() {
        let (data, len) = into_array(vec![1u64, 2, 3]);
        assert_eq!(
            unsafe { required_array(data, len, "deltas") }.unwrap(),
            [1, 2, 3]
        );
        assert_eq!(unsafe { from_array(data, len) }, vec![1, 2, 3]);
    }

    #[test]
    fn an_empty_array_is_handed_over_as_null() {
        let (data, len) = into_array(Vec::<u64>::new());
        assert!(data.is_null());
        assert_eq!(len, 0);
        assert!(unsafe { from_array(data, len) }.is_empty());
    }
}
