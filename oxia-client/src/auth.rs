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

//! Token authentication: attaches an `authorization: Bearer <token>` header to
//! every request, matching the reference Go client.

use std::sync::Arc;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;

/// Supplies the bearer token attached to every request the client sends.
///
/// Configured with
/// [`OxiaClientBuilder::auth_token_provider`](crate::OxiaClientBuilder::auth_token_provider)
/// (or [`auth_token`](crate::OxiaClientBuilder::auth_token) for a static
/// token). The provider is called on every RPC — including the long-lived
/// streams when they (re)connect — so expiring credentials are picked up
/// transparently. It must return quickly and never block: fetch and cache
/// refreshed tokens elsewhere (e.g. a background task).
///
/// Any `Fn() -> String + Send + Sync` closure is a `TokenProvider`:
///
/// ```no_run
/// # async fn example() -> Result<(), oxia::OxiaError> {
/// use oxia::OxiaClient;
///
/// let client = OxiaClient::builder()
///     .service_address("localhost:6648")
///     .auth_token_provider(|| std::env::var("OXIA_TOKEN").unwrap_or_default())
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Bearer tokens are readable by anyone who can observe the connection;
/// combine authentication with TLS (the `tls` feature) outside trusted
/// networks.
pub trait TokenProvider: Send + Sync {
    /// Returns the token to attach to the next request.
    fn token(&self) -> String;
}

impl<F> TokenProvider for F
where
    F: Fn() -> String + Send + Sync,
{
    fn token(&self) -> String {
        self()
    }
}

/// The configured token provider, as stored in the client options. Debug never
/// prints token material.
#[derive(Clone)]
pub(crate) struct TokenAuth(pub(crate) Arc<dyn TokenProvider>);

impl std::fmt::Debug for TokenAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TokenAuth")
    }
}

/// Interceptor applied to every channel; attaches the `authorization` header
/// when a token provider is configured, and is a no-op otherwise.
#[derive(Clone)]
pub(crate) struct AuthInterceptor(Option<TokenAuth>);

impl AuthInterceptor {
    pub(crate) fn new(auth: Option<TokenAuth>) -> AuthInterceptor {
        AuthInterceptor(auth)
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(TokenAuth(provider)) = &self.0 {
            let header = format!("Bearer {}", provider.token());
            let value: MetadataValue<_> = header.parse().map_err(|_| {
                Status::invalid_argument("auth token contains invalid header characters")
            })?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attaches_bearer_header() {
        let auth = TokenAuth(Arc::new(|| "my-token".to_string()));
        let mut interceptor = AuthInterceptor::new(Some(auth));
        let request = interceptor.call(tonic::Request::new(())).unwrap();
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer my-token"
        );
    }

    #[test]
    fn no_provider_is_a_no_op() {
        let mut interceptor = AuthInterceptor::new(None);
        let request = interceptor.call(tonic::Request::new(())).unwrap();
        assert!(request.metadata().get("authorization").is_none());
    }

    #[test]
    fn invalid_token_characters_are_rejected() {
        let auth = TokenAuth(Arc::new(|| "bad\ntoken".to_string()));
        let mut interceptor = AuthInterceptor::new(Some(auth));
        assert!(interceptor.call(tonic::Request::new(())).is_err());
    }

    #[test]
    fn provider_is_called_per_request() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let counter = calls.clone();
        let auth = TokenAuth(Arc::new(move || {
            format!("t{}", counter.fetch_add(1, Ordering::SeqCst))
        }));
        let mut interceptor = AuthInterceptor::new(Some(auth));
        let first = interceptor.call(tonic::Request::new(())).unwrap();
        let second = interceptor.call(tonic::Request::new(())).unwrap();
        assert_eq!(first.metadata().get("authorization").unwrap(), "Bearer t0");
        assert_eq!(second.metadata().get("authorization").unwrap(), "Bearer t1");
    }
}
