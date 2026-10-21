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

//! TLS configuration for connecting to a TLS-enabled Oxia cluster. Enabled
//! with the `tls` Cargo feature.

use tonic::transport::{Certificate, ClientTlsConfig, Identity};

/// TLS settings for the client's connections, passed to
/// [`OxiaClientBuilder::tls`](crate::OxiaClientBuilder::tls).
///
/// The default configuration ([`TlsOptions::new`]) verifies the server
/// certificate against the operating system's trusted root store. Options
/// mirror the reference Go client's TLS configuration:
///
/// - [`trusted_ca_pem`](TlsOptions::trusted_ca_pem) — trust a custom CA
///   *instead of* the system roots (e.g. a self-signed cluster CA).
/// - [`identity_pem`](TlsOptions::identity_pem) — present a client
///   certificate (mutual TLS).
/// - [`domain_name`](TlsOptions::domain_name) — override the name the server
///   certificate is verified against (and the SNI sent), when the dialed
///   address differs from the certificate's subject.
///
/// ```no_run
/// # async fn example() -> Result<(), oxia::OxiaError> {
/// use oxia::{OxiaClient, TlsOptions};
///
/// let ca_pem = std::fs::read("ca.crt").expect("read CA certificate");
/// let client = OxiaClient::builder()
///     .service_address("https://oxia.example.com:6648")
///     .tls(TlsOptions::new().trusted_ca_pem(ca_pem))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
#[must_use = "TlsOptions must be passed to OxiaClientBuilder::tls"]
pub struct TlsOptions {
    ca_pem: Option<Vec<u8>>,
    identity_pem: Option<(Vec<u8>, Vec<u8>)>,
    domain_name: Option<String>,
}

impl TlsOptions {
    /// TLS with default settings: the server certificate is verified against
    /// the operating system's trusted root store.
    pub fn new() -> Self {
        TlsOptions::default()
    }

    /// Trusts the given PEM-encoded CA certificate(s) for verifying the
    /// server, *instead of* the operating system's root store.
    pub fn trusted_ca_pem(mut self, ca_pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(ca_pem.into());
        self
    }

    /// Presents the given PEM-encoded certificate and private key as the
    /// client's identity (mutual TLS).
    pub fn identity_pem(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.identity_pem = Some((cert_pem.into(), key_pem.into()));
        self
    }

    /// Verifies the server certificate against this name (and sends it as
    /// SNI) instead of the host in the dialed address.
    pub fn domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = Some(domain_name.into());
        self
    }

    /// Builds the tonic TLS configuration applied to every connection.
    pub(crate) fn to_client_tls_config(&self) -> ClientTlsConfig {
        let mut config = ClientTlsConfig::new();
        config = match &self.ca_pem {
            Some(pem) => config.ca_certificate(Certificate::from_pem(pem)),
            None => config.with_native_roots(),
        };
        if let Some((cert, key)) = &self.identity_pem {
            config = config.identity(Identity::from_pem(cert, key));
        }
        if let Some(domain) = &self.domain_name {
            config = config.domain_name(domain);
        }
        config
    }
}

// Manual Debug: never print certificate or private-key material.
impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsOptions")
            .field("custom_ca", &self.ca_pem.is_some())
            .field("client_identity", &self.identity_pem.is_some())
            .field("domain_name", &self.domain_name)
            .finish()
    }
}
