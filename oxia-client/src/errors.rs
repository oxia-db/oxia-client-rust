use thiserror::Error;

/// Errors returned by the Oxia client.
///
/// The variants fall into three groups:
/// - **Semantic results** callers commonly match on ([`KeyNotFound`],
///   [`UnexpectedVersionId`], [`SessionExpired`], [`RequestTooLarge`],
///   [`InvalidArgument`], [`EndInclusiveUnsupported`]). These are never
///   retryable.
/// - **Transient failures** that the client's background loops (and, in a later
///   phase, its operation-level retries) may retry: [`LeaderNotFound`],
///   [`NoShardForKey`], [`Disconnected`], and some [`Grpc`] codes. See
///   [`OxiaError::is_retryable`].
/// - **Terminal failures**: [`Timeout`], [`Decode`], [`Closed`], and the
///   remaining [`Grpc`] codes.
///
/// [`KeyNotFound`]: OxiaError::KeyNotFound
/// [`UnexpectedVersionId`]: OxiaError::UnexpectedVersionId
/// [`SessionExpired`]: OxiaError::SessionExpired
/// [`RequestTooLarge`]: OxiaError::RequestTooLarge
/// [`InvalidArgument`]: OxiaError::InvalidArgument
/// [`EndInclusiveUnsupported`]: OxiaError::EndInclusiveUnsupported
/// [`LeaderNotFound`]: OxiaError::LeaderNotFound
/// [`NoShardForKey`]: OxiaError::NoShardForKey
/// [`Disconnected`]: OxiaError::Disconnected
/// [`Grpc`]: OxiaError::Grpc
/// [`Timeout`]: OxiaError::Timeout
/// [`Decode`]: OxiaError::Decode
/// [`Closed`]: OxiaError::Closed
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum OxiaError {
    /// The requested key does not exist.
    #[error("key not found")]
    KeyNotFound,

    /// A conditional operation failed because the stored version did not match
    /// the expected one.
    #[error("unexpected version id")]
    UnexpectedVersionId,

    /// The session backing an ephemeral record has expired or been closed.
    #[error("session no longer exists")]
    SessionExpired,

    /// A single request, or a batch that cannot be split further, exceeds the
    /// server's maximum message size.
    #[error("request is too large")]
    RequestTooLarge,

    /// An option or argument passed to an operation was invalid.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A [`list`](crate::OxiaClient::list) or
    /// [`range_scan`](crate::OxiaClient::range_scan) with
    /// [`end_inclusive`](crate::ListBuilder::end_inclusive) reached a shard
    /// whose leader left the end of the range out: its server is older, or
    /// not all the servers of the shard support it yet.
    #[error("the shard does not support an inclusive end of range")]
    EndInclusiveUnsupported,

    /// No leader is currently available for the shard (e.g. a leader election is
    /// in progress). Retryable.
    #[error("no leader available for shard {shard}")]
    LeaderNotFound {
        /// The shard without a known leader.
        shard: i64,
    },

    /// No shard currently owns the key, because shard assignments have not been
    /// loaded yet or are momentarily incomplete. Retryable.
    #[error("no shard owns key {key:?} (assignments not ready)")]
    NoShardForKey {
        /// The key that could not be routed.
        key: String,
    },

    /// The connection to a server was lost or could not be established, or an
    /// in-flight request's worker went away. Retryable.
    #[error("disconnected: {0}")]
    Disconnected(String),

    /// An operation did not complete before its deadline.
    #[error("request timed out")]
    Timeout,

    /// The server returned a gRPC status. Whether it is retryable depends on the
    /// code (see [`OxiaError::is_retryable`]).
    #[error("grpc status {code:?}: {message}")]
    Grpc {
        /// The gRPC status code.
        code: tonic::Code,
        /// The gRPC status message.
        message: String,
    },

    /// A server response could not be decoded — a required field was missing, or
    /// the response did not match the request. Indicates a protocol mismatch or
    /// a server bug.
    #[error("failed to decode server response: {0}")]
    Decode(String),

    /// A shard was split or merged while an operation targeted it. The client
    /// re-routes such operations to the new shard automatically, so this is not
    /// normally observed, except from a [`list`](crate::OxiaClient::list) or a
    /// [`range_scan`](crate::OxiaClient::range_scan) that was opening its
    /// streams, as in the Go client. Retryable (by re-routing).
    #[error("shard was split or merged")]
    ShardMoved,

    /// The client has been shut down and can no longer be used.
    #[error("client is closed")]
    Closed,
}

impl OxiaError {
    /// Whether retrying the operation that produced this error might succeed.
    ///
    /// Mirrors the retryable set of the reference Go client: connection loss,
    /// missing shard leaders / assignments, and the `UNAVAILABLE` / `ABORTED` /
    /// `CANCELLED` gRPC codes (the last being a shutting-down server's own
    /// cancellation propagating). Semantic results (e.g.
    /// [`KeyNotFound`](OxiaError::KeyNotFound)) and terminal failures are not
    /// retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            OxiaError::LeaderNotFound { .. }
            | OxiaError::NoShardForKey { .. }
            | OxiaError::ShardMoved
            | OxiaError::Disconnected(_) => true,
            OxiaError::Grpc { code, .. } => {
                // `Cancelled` here is the *server's* context cancellation
                // propagating while it shuts down or drops a connection — the
                // client never cancels these RPCs itself — so like the other
                // two it signals a server that may come right back.
                matches!(
                    code,
                    tonic::Code::Unavailable | tonic::Code::Aborted | tonic::Code::Cancelled
                )
            }
            _ => false,
        }
    }
}

impl From<tonic::Status> for OxiaError {
    fn from(status: tonic::Status) -> Self {
        // tonic reports client-side transport failures (broken connection,
        // h2 errors) as code `Unknown` with a local error source, unlike
        // grpc-go which uses `Unavailable` for the same events. A status the
        // *server* sent has no local source. Map the transport case to
        // `Disconnected` so it classifies as retryable, like in Go.
        if status.code() == tonic::Code::Unknown && std::error::Error::source(&status).is_some() {
            return OxiaError::Disconnected(format!("transport error: {}", status.message()));
        }
        OxiaError::Grpc {
            code: status.code(),
            message: status.message().to_string(),
        }
    }
}

impl From<tonic::transport::Error> for OxiaError {
    fn from(err: tonic::transport::Error) -> Self {
        OxiaError::Disconnected(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification() {
        let msg = String::new();
        assert!(OxiaError::LeaderNotFound { shard: 1 }.is_retryable());
        assert!(OxiaError::NoShardForKey { key: "k".into() }.is_retryable());
        assert!(OxiaError::Disconnected("x".into()).is_retryable());
        assert!(
            OxiaError::Grpc {
                code: tonic::Code::Unavailable,
                message: msg.clone(),
            }
            .is_retryable()
        );
        assert!(
            OxiaError::Grpc {
                code: tonic::Code::Aborted,
                message: msg.clone(),
            }
            .is_retryable()
        );
        assert!(
            OxiaError::Grpc {
                code: tonic::Code::Cancelled,
                message: msg.clone(),
            }
            .is_retryable()
        );

        assert!(!OxiaError::KeyNotFound.is_retryable());
        assert!(!OxiaError::UnexpectedVersionId.is_retryable());
        assert!(!OxiaError::SessionExpired.is_retryable());
        assert!(!OxiaError::RequestTooLarge.is_retryable());
        assert!(!OxiaError::EndInclusiveUnsupported.is_retryable());
        assert!(OxiaError::ShardMoved.is_retryable());
        assert!(!OxiaError::Timeout.is_retryable());
        assert!(!OxiaError::Closed.is_retryable());
        assert!(
            !OxiaError::Grpc {
                code: tonic::Code::NotFound,
                message: msg,
            }
            .is_retryable()
        );
    }

    #[test]
    fn from_tonic_status_maps_to_grpc() {
        let err = OxiaError::from(tonic::Status::unavailable("down"));
        assert!(matches!(
            err,
            OxiaError::Grpc {
                code: tonic::Code::Unavailable,
                ..
            }
        ));
        assert!(err.is_retryable());
    }
}
