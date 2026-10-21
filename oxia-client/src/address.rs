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

/// Normalizes a user- or server-supplied address into a dialable URI.
/// `tls://` (the scheme used by the reference Go client) is an alias for
/// `https://`; a bare `host:port` defaults to `http://`.
pub(crate) fn ensure_protocol(raw_address: String) -> String {
    if let Some(rest) = raw_address.strip_prefix("tls://") {
        return format!("https://{}", rest);
    }
    if raw_address.starts_with("http://") || raw_address.starts_with("https://") {
        return raw_address;
    }
    format!("http://{}", raw_address)
}

/// Rewrites an `http://` address to `https://`. Shard-assignment addresses
/// arrive without a scheme and default to `http://`; when the client is
/// configured for TLS every connection must dial `https://` instead.
#[cfg(feature = "tls")]
pub(crate) fn force_https(address: String) -> String {
    match address.strip_prefix("http://") {
        Some(rest) => format!("https://{}", rest),
        None => address,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_addresses_default_to_http() {
        assert_eq!(
            ensure_protocol("localhost:6648".to_string()),
            "http://localhost:6648"
        );
    }

    #[test]
    fn explicit_schemes_are_preserved() {
        assert_eq!(
            ensure_protocol("http://localhost:6648".to_string()),
            "http://localhost:6648"
        );
        assert_eq!(
            ensure_protocol("https://localhost:6648".to_string()),
            "https://localhost:6648"
        );
    }

    #[test]
    fn tls_scheme_is_an_alias_for_https() {
        assert_eq!(
            ensure_protocol("tls://localhost:6648".to_string()),
            "https://localhost:6648"
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn force_https_rewrites_only_http() {
        assert_eq!(
            force_https("http://localhost:6648".to_string()),
            "https://localhost:6648"
        );
        assert_eq!(
            force_https("https://localhost:6648".to_string()),
            "https://localhost:6648"
        );
    }
}
