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

use crate::auth::TokenAuth;
use crate::errors::OxiaError;
use crate::proto::oxia_client_client::OxiaClientClient;
use crate::transport::{self, DialOptions, Provider};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::OnceCell;

pub(crate) use crate::transport::AuthChannel;

pub struct ProviderManager {
    providers: RefCell<HashMap<String, Rc<OnceCell<Provider>>>>,
    options: DialOptions,
}

impl ProviderManager {
    pub async fn get_provider(
        &self,
        address: String,
    ) -> Result<OxiaClientClient<AuthChannel>, OxiaError> {
        // Take the cell out of the map: the map must not stay borrowed while
        // connecting, as every other lookup needs it. The lookups of an
        // address that is being connected to wait for the same connection.
        let cell = {
            let mut providers = self.providers.borrow_mut();
            let cell = providers.entry(address.clone()).or_default();
            // A connection that has closed is not reused, as tonic's channel
            // did not: dial the server again.
            if cell.get().is_some_and(Provider::is_closed) {
                *cell = Rc::default();
            }
            cell.clone()
        };
        // With TLS configured, every dial — the bootstrap address and the
        // scheme-less shard-leader addresses that defaulted to http:// — must
        // go over https://.
        #[cfg(feature = "tls")]
        let address = if self.options.tls.is_some() {
            crate::address::force_https(address)
        } else {
            address
        };
        let provider = cell
            .get_or_try_init(|| transport::connect(&address, &self.options))
            .await?;
        Ok(provider.client.clone())
    }

    pub fn new(connect_timeout: Duration) -> ProviderManager {
        ProviderManager {
            providers: RefCell::new(HashMap::new()),
            options: DialOptions {
                connect_timeout,
                auth: None,
                #[cfg(feature = "tls")]
                tls: None,
            },
        }
    }

    /// Authenticates every connection this manager creates with tokens from
    /// the given provider.
    pub fn with_auth(mut self, auth: Option<TokenAuth>) -> ProviderManager {
        self.options.auth = auth;
        self
    }

    /// Applies a TLS configuration to every connection this manager creates.
    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, tls: Option<crate::tls::TlsConfig>) -> ProviderManager {
        self.options.tls = tls;
        self
    }

    /// Drops all cached connections.
    pub fn clear(&self) {
        self.providers.borrow_mut().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two lookups of an address with no connection yet, on the client's
    /// thread: the second must wait for the connection the first is making,
    /// without the map staying borrowed across the connect, which would fail
    /// the second lookup.
    #[test]
    fn concurrent_lookups_while_connecting_do_not_block_the_thread() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = compio::runtime::Runtime::new().expect("runtime");
            runtime.block_on(async {
                // Accepts connections but never speaks HTTP/2.
                let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
                let address = format!("http://{}", listener.local_addr().expect("address"));
                let manager = ProviderManager::new(Duration::from_millis(500));
                let _ = futures::future::join(
                    manager.get_provider(address.clone()),
                    manager.get_provider(address),
                )
                .await;
            });
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the lookups must complete");
    }
}
