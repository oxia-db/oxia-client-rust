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

//! Decoding of Oxia's rich gRPC error details.
//!
//! The server attaches a `google.rpc.ErrorInfo` (domain `oxia.io`) to error
//! statuses, carrying a machine-readable `reason` and, for routing errors, a
//! **leader hint** (`shard` / `leader` metadata) pointing at the current
//! leader before the client's shard-assignment watcher has caught up. This
//! mirrors the reference Go client's `constant.FromGrpcError`.

use crate::errors::OxiaError;
use tonic::Status;
use tonic_types::StatusExt;

const OXIA_ERROR_DOMAIN: &str = "oxia.io";
const METADATA_SHARD: &str = "shard";
const METADATA_LEADER: &str = "leader";
const REASON_SESSION_NOT_FOUND: &str = "SESSION_NOT_FOUND";
const REASON_SHARD_NOT_FOUND: &str = "SHARD_NOT_FOUND";

/// The address of a shard's current leader, extracted from an error's details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeaderHint {
    pub(crate) shard: i64,
    pub(crate) leader: String,
}

impl LeaderHint {
    /// The hinted address, if this hint is for `shard`.
    pub(crate) fn address_for(&self, shard: i64) -> Option<&str> {
        (self.shard == shard).then_some(self.leader.as_str())
    }
}

/// A gRPC status decoded into a typed error plus an optional leader hint.
pub(crate) struct DecodedStatus {
    pub(crate) error: OxiaError,
    pub(crate) leader_hint: Option<LeaderHint>,
}

impl From<OxiaError> for DecodedStatus {
    fn from(error: OxiaError) -> Self {
        DecodedStatus {
            error,
            leader_hint: None,
        }
    }
}

/// Decodes `status`, honoring Oxia's `ErrorInfo` details when present.
pub(crate) fn decode_status(status: Status) -> DecodedStatus {
    let details = status.get_error_details();
    let Some(info) = details.error_info() else {
        return DecodedStatus {
            error: OxiaError::from(status),
            leader_hint: None,
        };
    };
    if info.domain != OXIA_ERROR_DOMAIN {
        return DecodedStatus {
            error: OxiaError::from(status),
            leader_hint: None,
        };
    }

    let leader_hint = match (
        info.metadata.get(METADATA_LEADER),
        info.metadata.get(METADATA_SHARD),
    ) {
        (Some(leader), Some(shard)) if !leader.is_empty() => {
            shard.parse().ok().map(|shard| LeaderHint {
                shard,
                leader: leader.clone(),
            })
        }
        _ => None,
    };

    let error = match info.reason.as_str() {
        REASON_SESSION_NOT_FOUND => OxiaError::SessionExpired,
        // A split/merged shard: the operation re-routes to the new shard.
        REASON_SHARD_NOT_FOUND => OxiaError::ShardMoved,
        _ => OxiaError::from(status),
    };

    DecodedStatus { error, leader_hint }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tonic::Code;
    use tonic_types::ErrorDetails;

    fn oxia_status(code: Code, reason: &str, metadata: HashMap<String, String>) -> Status {
        let mut details = ErrorDetails::new();
        details.set_error_info(reason, OXIA_ERROR_DOMAIN, metadata);
        Status::with_error_details(code, "test", details)
    }

    #[test]
    fn plain_status_decodes_by_code() {
        let decoded = decode_status(Status::unavailable("down"));
        assert!(matches!(
            decoded.error,
            OxiaError::Grpc {
                code: Code::Unavailable,
                ..
            }
        ));
        assert!(decoded.error.is_retryable());
        assert!(decoded.leader_hint.is_none());
    }

    #[test]
    fn session_not_found_reason_maps_to_session_expired() {
        let status = oxia_status(Code::NotFound, REASON_SESSION_NOT_FOUND, HashMap::new());
        let decoded = decode_status(status);
        assert!(matches!(decoded.error, OxiaError::SessionExpired));
    }

    #[test]
    fn leader_hint_is_extracted() {
        let metadata = HashMap::from([
            ("shard".to_string(), "7".to_string()),
            ("leader".to_string(), "server-2:6648".to_string()),
        ]);
        let status = oxia_status(Code::Aborted, "NODE_IS_NOT_LEADER", metadata);
        let decoded = decode_status(status);
        assert!(decoded.error.is_retryable());
        let hint = decoded.leader_hint.expect("hint");
        assert_eq!(hint.shard, 7);
        assert_eq!(hint.leader, "server-2:6648");
        assert_eq!(hint.address_for(7), Some("server-2:6648"));
        assert_eq!(hint.address_for(8), None);
    }

    #[test]
    fn shard_not_found_reason_maps_to_shard_moved() {
        let status = oxia_status(Code::NotFound, "SHARD_NOT_FOUND", HashMap::new());
        let decoded = decode_status(status);
        assert!(matches!(decoded.error, OxiaError::ShardMoved));
        assert!(decoded.error.is_retryable());
    }

    #[test]
    fn foreign_domain_details_are_ignored() {
        let mut details = ErrorDetails::new();
        details.set_error_info("SESSION_NOT_FOUND", "example.com", HashMap::new());
        let status = Status::with_error_details(Code::NotFound, "test", details);
        let decoded = decode_status(status);
        assert!(matches!(decoded.error, OxiaError::Grpc { .. }));
        assert!(decoded.leader_hint.is_none());
    }

    #[test]
    fn incomplete_hint_metadata_is_ignored() {
        let metadata = HashMap::from([("leader".to_string(), "server-2:6648".to_string())]);
        let status = oxia_status(Code::Aborted, "NODE_IS_NOT_LEADER", metadata);
        assert!(decode_status(status).leader_hint.is_none());
    }
}
