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

use crate::errors::OxiaError;
use compio::tls::TlsConnector;
use compio::tls::rustls::crypto::{self, CryptoProvider};
use compio::tls::rustls::pki_types::pem::PemObject;
use compio::tls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use compio::tls::rustls::{ClientConfig, RootCertStore};
use std::sync::Arc;

/// The ALPN protocol id of HTTP/2, which the server must negotiate.
const ALPN_H2: &[u8] = b"h2";

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

    /// Builds the TLS configuration applied to every connection, as tonic's
    /// own TLS transport did: the process-default crypto provider if one is
    /// installed, else ring; HTTP/2 negotiated through ALPN.
    pub(crate) fn to_tls_config(&self) -> Result<TlsConfig, OxiaError> {
        let provider = CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(crypto::ring::default_provider()));
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|err| invalid_tls("protocol versions", err))?;
        let mut roots = RootCertStore::empty();
        match &self.ca_pem {
            Some(pem) => {
                let certs = CertificateDer::pem_slice_iter(pem)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| invalid_tls("CA certificate", err))?;
                roots.add_parsable_certificates(certs);
            }
            None => {
                let native = rustls_native_certs::load_native_certs();
                if !native.errors.is_empty() {
                    tracing::debug!(
                        "errors occurred when loading native certs: {:?}",
                        native.errors
                    );
                }
                if native.certs.is_empty() {
                    return Err(OxiaError::InvalidArgument(
                        "no native root CA certificates found".to_string(),
                    ));
                }
                roots.add_parsable_certificates(native.certs);
            }
        }
        let builder = builder.with_root_certificates(roots);
        let mut config = match &self.identity_pem {
            Some((cert, key)) => {
                let chain = CertificateDer::pem_slice_iter(cert)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| invalid_tls("client certificate", err))?;
                let key = PrivateKeyDer::from_pem_slice(key)
                    .map_err(|err| invalid_tls("client private key", err))?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|err| invalid_tls("client identity", err))?
            }
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols.push(ALPN_H2.to_vec());
        if let Some(domain) = &self.domain_name {
            ServerName::try_from(domain.as_str()).map_err(|err| invalid_tls("domain name", err))?;
        }
        Ok(TlsConfig {
            connector: TlsConnector::from(Arc::new(config)),
            domain_name: self.domain_name.clone(),
        })
    }
}

fn invalid_tls(what: &str, err: impl std::fmt::Display) -> OxiaError {
    OxiaError::InvalidArgument(format!("invalid TLS {what}: {err}"))
}

/// The TLS settings of every connection, built from [`TlsOptions`].
#[derive(Clone)]
pub(crate) struct TlsConfig {
    connector: TlsConnector,
    domain_name: Option<String>,
}

impl TlsConfig {
    /// Runs the TLS handshake on `stream`, a connection to `host`, verifying
    /// the server certificate against `host`, or the configured domain name,
    /// and requiring the server to negotiate HTTP/2.
    pub(crate) async fn connect(
        &self,
        host: &str,
        stream: compio::net::TcpStream,
    ) -> Result<cyper_core::HyperStream<compio::net::TcpStream>, String> {
        let domain = self.domain_name.as_deref().unwrap_or(host);
        let stream = self
            .connector
            .connect(domain, stream)
            .await
            .map_err(|err| format!("TLS handshake failed: {err}"))?;
        let stream = cyper_core::HyperStream::new_tls(stream);
        if stream.negotiated_alpn().as_deref() != Some(ALPN_H2) {
            return Err("HTTP/2 was not negotiated".to_string());
        }
        Ok(stream)
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
