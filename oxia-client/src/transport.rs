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

//! The gRPC transport: the tonic-generated client over hyper's HTTP/2 client,
//! running on the compio runtime of the client's thread.
//!
//! It stands in for tonic's own transport, which runs on tokio, and does what
//! the client relied on it for: dialing with TCP_NODELAY and a connect
//! timeout, TLS with HTTP/2 negotiated through ALPN, HTTP/2 keep-alive,
//! enforcing request deadlines, and mapping transport failures to the gRPC
//! statuses tonic reports for them.

use crate::auth::{AuthInterceptor, TokenAuth};
use crate::errors::OxiaError;
use crate::proto::oxia_client_client::OxiaClientClient;
use cyper_core::{CompioExecutor, CompioTimer, HyperStream};
use futures::future::LocalBoxFuture;
use http_body_util::BodyExt;
use http_body_util::combinators::MapErr;
use hyper::body::Incoming;
use hyper::client::conn::http2;
use std::error::Error;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::codegen::Service;
use tonic::service::interceptor::InterceptedService;
use tonic::{Code, Status};
use tracing::debug;

/// The transport every RPC flows through: a connection wrapped with the auth
/// interceptor (a no-op when no token provider is configured).
pub(crate) type AuthChannel = InterceptedService<GrpcChannel, AuthInterceptor>;

/// HTTP/2 keep-alive settings, matching the reference client. The server enforces
/// a minimum client ping interval of 5s and permits pings without an active
/// stream, so a 10s interval is safe. Keep-alive lets a silently-dead connection
/// (e.g. a dropped NAT mapping) be detected instead of leaving the long-lived
/// streams — assignments, notifications, sequence updates, the write stream —
/// hanging forever.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(3);

/// What every connection is dialed with.
#[derive(Clone)]
pub(crate) struct DialOptions {
    pub(crate) connect_timeout: Duration,
    pub(crate) auth: Option<TokenAuth>,
    #[cfg(feature = "tls")]
    pub(crate) tls: Option<crate::tls::TlsConfig>,
}

/// A connection to a server, and the client that issues RPCs on it.
#[derive(Clone)]
pub(crate) struct Provider {
    pub(crate) client: OxiaClientClient<AuthChannel>,
    channel: GrpcChannel,
}

impl Provider {
    /// Whether the connection has closed: the server went away or the
    /// connection failed. RPCs on it fail; dial again instead.
    pub(crate) fn is_closed(&self) -> bool {
        self.channel.send.is_closed()
    }
}

/// Connects to `address` (`http://host:port`, or `https://host:port` for
/// TLS), within the connect timeout.
pub(crate) async fn connect(address: &str, options: &DialOptions) -> Result<Provider, OxiaError> {
    let disconnected = |err: String| OxiaError::Disconnected(format!("{address}: {err}"));
    let origin: http::Uri = address
        .parse()
        .map_err(|err| disconnected(format!("invalid address: {err}")))?;
    let tls = match origin.scheme_str() {
        Some("http") => false,
        Some("https") => true,
        _ => {
            return Err(disconnected(
                "the address must be http:// or https://".to_string(),
            ));
        }
    };
    // `Uri::host` keeps the brackets of an IPv6 address.
    let host = origin
        .host()
        .ok_or_else(|| disconnected("the address has no host".to_string()))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let port = origin.port_u16().unwrap_or(if tls { 443 } else { 80 });
    let dial = async {
        let stream = compio::net::TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|err| disconnected(err.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|err| disconnected(err.to_string()))?;
        let stream = if tls {
            secure(&host, stream, options).await.map_err(disconnected)?
        } else {
            HyperStream::new_plain(stream)
        };
        let (send, connection) = http2::Builder::new(CompioExecutor)
            .timer(CompioTimer)
            .keep_alive_interval(KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .handshake(stream)
            .await
            .map_err(|err| disconnected(err.to_string()))?;
        // The connection runs until it fails, or until every handle of it is
        // dropped and its streams are done.
        let peer = address.to_string();
        compio::runtime::spawn(async move {
            if let Err(err) = connection.await {
                debug!(address = peer, error = %err, "connection closed");
            }
        })
        .detach();
        Ok::<_, OxiaError>(GrpcChannel { send })
    };
    let channel = compio::time::timeout(options.connect_timeout, dial)
        .await
        .map_err(|_| disconnected("timed out connecting".to_string()))??;
    let client = OxiaClientClient::with_origin(
        InterceptedService::new(channel.clone(), AuthInterceptor::new(options.auth.clone())),
        origin,
    );
    Ok(Provider { client, channel })
}

#[cfg(feature = "tls")]
async fn secure(
    host: &str,
    stream: compio::net::TcpStream,
    options: &DialOptions,
) -> Result<HyperStream<compio::net::TcpStream>, String> {
    match &options.tls {
        Some(tls) => tls.connect(host, stream).await,
        None => Err("TLS is not configured".to_string()),
    }
}

#[cfg(not(feature = "tls"))]
async fn secure(
    _host: &str,
    _stream: compio::net::TcpStream,
    _options: &DialOptions,
) -> Result<HyperStream<compio::net::TcpStream>, String> {
    Err("TLS requires the `tls` Cargo feature".to_string())
}

/// A response body whose errors are the statuses tonic reports for them.
type ResponseBody = MapErr<Incoming, fn(hyper::Error) -> Status>;

/// The service the generated client calls: one HTTP/2 connection.
#[derive(Clone)]
pub(crate) struct GrpcChannel {
    send: http2::SendRequest<tonic::body::Body>,
}

impl Service<http::Request<tonic::body::Body>> for GrpcChannel {
    type Response = http::Response<ResponseBody>;
    type Error = Status;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Status>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        // Always ready: on a closed connection, sending the request fails
        // instead, as a transport failure. The generated client would turn a
        // readiness failure into an `Unknown` status, which is not retryable.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let timeout = grpc_timeout(request.headers());
        let response = self.send.send_request(request);
        Box::pin(async move {
            let response = match timeout {
                Some(timeout) => compio::time::timeout(timeout, response)
                    .await
                    .map_err(|_| timeout_expired())?,
                None => response.await,
            };
            let response = response.map_err(status_from_hyper)?;
            let to_status: fn(hyper::Error) -> Status = status_from_hyper;
            Ok(response.map(|body| body.map_err(to_status)))
        })
    }
}

/// The deadline of a request (`tonic::Request::set_timeout`), which the
/// client enforces too, as tonic's own transport does: the response headers
/// must arrive within it. A malformed header is ignored, as tonic ignores it.
fn grpc_timeout(headers: &http::HeaderMap) -> Option<Duration> {
    let value = headers.get("grpc-timeout")?.to_str().ok()?;
    if value.is_empty() {
        return None;
    }
    let (amount, unit) = value.split_at(value.len() - 1);
    // The gRPC spec allows at most 8 digits.
    if amount.len() > 8 {
        return None;
    }
    let amount: u64 = amount.parse().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(amount * 60 * 60)),
        "M" => Some(Duration::from_secs(amount * 60)),
        "S" => Some(Duration::from_secs(amount)),
        "m" => Some(Duration::from_millis(amount)),
        "u" => Some(Duration::from_micros(amount)),
        "n" => Some(Duration::from_nanos(amount)),
        _ => None,
    }
}

/// The status of a request past its deadline, as tonic reports it.
fn timeout_expired() -> Status {
    let expired = tonic::TimeoutExpired(());
    let mut status = Status::cancelled(expired.to_string());
    status.set_source(Arc::new(expired));
    status
}

/// The status of a transport failure, as tonic's own transport reports it:
/// tonic maps hyper and h2 errors to statuses only in its tokio-based
/// transport, which the client does not use. An `Unknown` status with the
/// error as its source becomes [`OxiaError::Disconnected`].
fn status_from_hyper(err: hyper::Error) -> Status {
    let mut status = if err.is_timeout() {
        // A keep-alive ping that got no answer: per the gRPC spec, the calls
        // fail with UNAVAILABLE.
        Status::unavailable(err.to_string())
    } else if err.is_canceled() {
        Status::cancelled(err.to_string())
    } else if let Some(h2) = err.source().and_then(|e| e.downcast_ref::<h2::Error>()) {
        Status::new(code_from_h2(h2), format!("h2 protocol error: {err}"))
    } else {
        Status::unknown(err.to_string())
    };
    status.set_source(Arc::new(err));
    status
}

/// The status code of an h2 error, from the gRPC HTTP/2 spec
/// (<https://github.com/grpc/grpc/blob/3977c30/doc/PROTOCOL-HTTP2.md#errors>),
/// as tonic maps it.
fn code_from_h2(err: &h2::Error) -> Code {
    match err.reason() {
        Some(h2::Reason::NO_ERROR)
        | Some(h2::Reason::PROTOCOL_ERROR)
        | Some(h2::Reason::INTERNAL_ERROR)
        | Some(h2::Reason::FLOW_CONTROL_ERROR)
        | Some(h2::Reason::SETTINGS_TIMEOUT)
        | Some(h2::Reason::COMPRESSION_ERROR)
        | Some(h2::Reason::CONNECT_ERROR) => Code::Internal,
        Some(h2::Reason::REFUSED_STREAM) => Code::Unavailable,
        Some(h2::Reason::CANCEL) => Code::Cancelled,
        Some(h2::Reason::ENHANCE_YOUR_CALM) => Code::ResourceExhausted,
        Some(h2::Reason::INADEQUATE_SECURITY) => Code::PermissionDenied,
        _ => Code::Unknown,
    }
}
