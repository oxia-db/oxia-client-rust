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

//! Key-to-shard hashing.
//!
//! Oxia routes a key to a shard by hashing it and locating the shard whose
//! hash range contains the result. The hash must match the server and every
//! other Oxia client SDK exactly, otherwise a key written by one client is
//! looked up on the wrong shard by another.

use crate::proto::ShardKeyRouter;
use tracing::warn;

/// Hashes `key` with the algorithm Oxia uses for shard routing: the low 32
/// bits of the 64-bit XXH3 digest (seed 0).
///
/// This mirrors the Go reference implementation in `common/hash/hash.go`,
/// `uint32(xxh3.HashString(key))`, which is what the coordinator uses to
/// compute shard hash-range boundaries.
pub(crate) fn xxh3_32(key: &str) -> u32 {
    xxhash_rust::xxh3::xxh3_64(key.as_bytes()) as u32
}

/// Hashes `key` using the algorithm advertised by the namespace's
/// [`ShardKeyRouter`].
///
/// `XXHASH3` is the only routing algorithm Oxia defines. `UNKNOWN` (sent by
/// servers that predate the field) is treated as `XXHASH3` for compatibility,
/// matching the Go client, which always routes with XXH3. Any value we do not
/// recognize is logged and falls back to `XXHASH3` — the only algorithm that
/// has ever existed — rather than silently routing with a mismatched hash.
pub(crate) fn shard_key_hash(router: i32, key: &str) -> u32 {
    match ShardKeyRouter::try_from(router) {
        Ok(ShardKeyRouter::Xxhash3) | Ok(ShardKeyRouter::Unknown) => xxh3_32(key),
        Err(_) => {
            warn!("Unrecognized shard key router {router}; routing with XXHASH3");
            xxh3_32(key)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Vectors taken verbatim from the Go reference test
    /// (`common/hash/hash_test.go`). If these diverge, keys placed by this
    /// client land on different shards than every other Oxia client.
    #[test]
    fn matches_upstream_vectors() {
        assert_eq!(xxh3_32("foo"), 125730186);
        assert_eq!(xxh3_32("bar"), 2687685474);
        assert_eq!(xxh3_32("baz"), 862947621);
    }

    #[test]
    fn router_dispatch_selects_xxhash3() {
        // The declared XXHASH3 router.
        assert_eq!(
            shard_key_hash(ShardKeyRouter::Xxhash3 as i32, "foo"),
            xxh3_32("foo")
        );
        // UNKNOWN (older servers that do not set the field) → XXHASH3.
        assert_eq!(
            shard_key_hash(ShardKeyRouter::Unknown as i32, "foo"),
            xxh3_32("foo")
        );
        // An unrecognized future router falls back to XXHASH3.
        assert_eq!(shard_key_hash(999, "foo"), xxh3_32("foo"));
    }
}
