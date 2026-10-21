//! End-to-end TLS tests (feature `tls`).
//!
//! `oxia standalone` cannot serve TLS, so these tests run a minimal real
//! cluster inside one container: an `oxia server` (dataserver) with TLS on its
//! public port, plus an `oxia coordinator` — internal traffic stays plaintext.
//! Certificates are generated per test with a throwaway CA, and the cluster
//! config advertises the host-mapped public port so the shard-assignment
//! addresses the client re-dials are reachable (and covered by the server
//! certificate's `localhost` SAN).
#![cfg(feature = "tls")]

use oxia::{OxiaClient, OxiaClientBuilder, TlsOptions};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use std::time::{Duration, Instant};
use testcontainers::core::ports::ContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

mod common;

const OXIA_PORT: u16 = 6648;

struct TestCerts {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

/// A throwaway CA, a server certificate for `localhost`/`127.0.0.1`, and a
/// client certificate (for mutual TLS), all freshly generated.
fn generate_certs() -> TestCerts {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "oxia-test-ca");
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let server_cert =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap()
            .signed_by(&server_key, &ca_cert, &ca_key)
            .unwrap();

    let client_key = KeyPair::generate().unwrap();
    let client_cert = CertificateParams::new(vec!["oxia-test-client".to_string()])
        .unwrap()
        .signed_by(&client_key, &ca_cert, &ca_key)
        .unwrap();

    TestCerts {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

struct TlsCluster {
    _container: ContainerAsync<GenericImage>,
    address: String,
}

/// Starts the one-container TLS cluster. With `client_auth`, the server also
/// requires and verifies a client certificate (mutual TLS).
async fn start_tls_oxia(certs: &TestCerts, client_auth: bool) -> TlsCluster {
    // The advertised public address must be reachable from the host, so pin
    // the host port up front and bake it into the cluster config.
    let host_port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind probe port")
        .local_addr()
        .expect("probe port addr")
        .port();

    let cluster_yaml = format!(
        "namespaces:\n  \
           - name: default\n    \
             initialShardCount: 2\n    \
             replicationFactor: 1\n\
         servers:\n  \
           - {{ public: \"localhost:{host_port}\", internal: \"localhost:6649\" }}\n"
    );

    let mut server_cmd = String::from(
        "oxia server --public-addr 0.0.0.0:6648 --internal-addr 0.0.0.0:6649 \
         --metrics-addr 0.0.0.0:8080 \
         --tls-cert-file /oxia/tls/server.crt --tls-key-file /oxia/tls/server.key \
         --wal-dir /tmp/wal --data-dir /tmp/db",
    );
    if client_auth {
        server_cmd.push_str(" --tls-trusted-ca-file /oxia/tls/ca.crt --tls-client-auth");
    }
    let script = format!(
        "{server_cmd} & \
         oxia coordinator --conf /oxia/tls/cluster.yaml --internal-addr 0.0.0.0:6652 \
         --public-addr 0.0.0.0:6651 --metrics-addr 0.0.0.0:8081 & \
         wait"
    );

    let container = common::oxia_image()
        .with_wait_for(WaitFor::message_on_stdout("Started Grpc server"))
        .with_mapped_port(host_port, ContainerPort::Tcp(OXIA_PORT))
        .with_copy_to("/oxia/tls/ca.crt", certs.ca_pem.clone().into_bytes())
        .with_copy_to(
            "/oxia/tls/server.crt",
            certs.server_cert_pem.clone().into_bytes(),
        )
        .with_copy_to(
            "/oxia/tls/server.key",
            certs.server_key_pem.clone().into_bytes(),
        )
        .with_copy_to("/oxia/tls/cluster.yaml", cluster_yaml.into_bytes())
        .with_cmd(vec!["bash".to_string(), "-c".to_string(), script])
        .start()
        .await
        .expect("Failed to start TLS Oxia cluster");

    TlsCluster {
        _container: container,
        address: format!("https://localhost:{host_port}"),
    }
}

/// Builds a client, retrying while the cluster elects its shard leaders.
async fn connect_with_retry(address: &str, tls: TlsOptions) -> OxiaClient {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let result = OxiaClientBuilder::new()
            .service_address(address.to_string())
            .request_timeout(Duration::from_secs(5))
            .tls(tls.clone())
            .build()
            .await;
        match result {
            Ok(client) => return client,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "client could not connect over TLS before the deadline: {e}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// TLS end to end: handshake with a custom CA, operations across shards (the
/// re-dialed shard-assignment addresses must also go over TLS), and rejection
/// of a client that does not trust the cluster's CA.
#[tokio::test]
async fn test_tls_end_to_end() {
    let certs = generate_certs();
    let cluster = start_tls_oxia(&certs, false).await;

    let tls = TlsOptions::new().trusted_ca_pem(certs.ca_pem.clone());
    let client = connect_with_retry(&cluster.address, tls).await;

    // Spread keys over both shards; every op crosses the TLS connection the
    // client re-established from the advertised assignment addresses.
    for i in 0..10 {
        client
            .put(format!("tls/key-{i}"), format!("value-{i}").into_bytes())
            .await
            .unwrap();
    }
    for i in 0..10 {
        let got = client.get(format!("tls/key-{i}")).await.unwrap();
        assert_eq!(got.value.as_deref(), Some(format!("value-{i}").as_bytes()));
    }
    let keys = client.list("tls/", "tls/~").await.unwrap();
    assert_eq!(keys.len(), 10);
    client.delete("tls/key-0").await.unwrap();

    // A client that does not trust the test CA must fail certificate
    // verification (the system roots do not include it).
    let untrusted = OxiaClientBuilder::new()
        .service_address(cluster.address.clone())
        .request_timeout(Duration::from_secs(3))
        .tls(TlsOptions::new())
        .build()
        .await;
    assert!(
        untrusted.is_err(),
        "a client without the cluster CA must be rejected"
    );

    client.close().await.unwrap();
}

/// Mutual TLS: the server requires a client certificate; a client presenting
/// one works end to end, a client without one is rejected.
#[tokio::test]
async fn test_tls_mutual_auth() {
    let certs = generate_certs();
    let cluster = start_tls_oxia(&certs, true).await;

    let tls = TlsOptions::new()
        .trusted_ca_pem(certs.ca_pem.clone())
        .identity_pem(certs.client_cert_pem.clone(), certs.client_key_pem.clone());
    let client = connect_with_retry(&cluster.address, tls).await;

    client.put("mtls/key", b"secret".to_vec()).await.unwrap();
    let got = client.get("mtls/key").await.unwrap();
    assert_eq!(got.value.as_deref(), Some(b"secret".as_ref()));

    // Without a client certificate the handshake must be rejected.
    let no_identity = OxiaClientBuilder::new()
        .service_address(cluster.address.clone())
        .request_timeout(Duration::from_secs(3))
        .tls(TlsOptions::new().trusted_ca_pem(certs.ca_pem.clone()))
        .build()
        .await;
    assert!(
        no_identity.is_err(),
        "a client without a certificate must be rejected by mutual TLS"
    );

    client.close().await.unwrap();
}
