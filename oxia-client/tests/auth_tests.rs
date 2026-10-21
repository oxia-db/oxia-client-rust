//! End-to-end token-authentication tests.
//!
//! `oxia standalone` has no auth flags, so — like the TLS suite — these tests
//! run a minimal real cluster in one container: an `oxia server` with the
//! `oidc` auth provider on its public port, plus an `oxia coordinator`
//! (internal traffic is not authenticated). The provider is configured with a
//! static public-key file, so it performs no OIDC discovery; tokens are JWTs
//! signed locally with the matching fixture key
//! (`testdata/jwt_test_key.pem`, a throwaway key generated for these tests).

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use oxia::{OxiaClient, OxiaClientBuilder};
use serde::Serialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use testcontainers::core::ports::ContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const OXIA_PORT: u16 = 6648;
const DEFAULT_OXIA_IMAGE: &str = "oxia/oxia";
const DEFAULT_OXIA_TAG: &str = "nightly-beta";

const ISSUER: &str = "https://oxia-test-issuer";
const AUDIENCE: &str = "oxia-test";
const JWT_PRIVATE_KEY_PEM: &str = include_str!("testdata/jwt_test_key.pem");
const JWT_PUBLIC_KEY_PEM: &str = include_str!("testdata/jwt_test_pub.pem");

#[derive(Serialize)]
struct Claims {
    iss: &'static str,
    sub: &'static str,
    aud: &'static str,
    iat: u64,
    exp: u64,
}

/// Signs a JWT the server's oidc provider will validate against the fixture
/// public key. `validity` may be short to exercise token rotation.
fn sign_token(aud: &'static str, validity: Duration) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs();
    let claims = Claims {
        iss: ISSUER,
        sub: "test-user",
        aud,
        iat: now,
        exp: now + validity.as_secs(),
    };
    jsonwebtoken::encode(
        &Header::new(Algorithm::RS256),
        &claims,
        &EncodingKey::from_rsa_pem(JWT_PRIVATE_KEY_PEM.as_bytes()).expect("fixture key parses"),
    )
    .expect("sign JWT")
}

struct AuthCluster {
    _container: ContainerAsync<GenericImage>,
    address: String,
}

/// Starts the one-container cluster with token authentication required on the
/// dataserver's public port.
async fn start_auth_oxia() -> AuthCluster {
    let image = std::env::var("OXIA_IMAGE").unwrap_or_else(|_| DEFAULT_OXIA_IMAGE.to_string());
    let tag = std::env::var("OXIA_TAG").unwrap_or_else(|_| DEFAULT_OXIA_TAG.to_string());

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

    let auth_params = format!(
        "{{\"issuers\":{{\"{ISSUER}\":{{\"allowedAudiences\":\"{AUDIENCE}\",\
         \"staticKeyFile\":\"/oxia/auth/jwt_pub.pem\"}}}}}}"
    );
    let script = format!(
        "oxia server --public-addr 0.0.0.0:6648 --internal-addr 0.0.0.0:6649 \
         --metrics-addr 0.0.0.0:8080 \
         --auth-provider-name oidc --auth-provider-params '{auth_params}' \
         --wal-dir /tmp/wal --data-dir /tmp/db & \
         oxia coordinator --conf /oxia/auth/cluster.yaml --internal-addr 0.0.0.0:6652 \
         --public-addr 0.0.0.0:6651 --metrics-addr 0.0.0.0:8081 & \
         wait"
    );

    let container = GenericImage::new(image, tag)
        .with_wait_for(WaitFor::message_on_stdout("Started Grpc server"))
        .with_mapped_port(host_port, ContainerPort::Tcp(OXIA_PORT))
        .with_copy_to(
            "/oxia/auth/jwt_pub.pem",
            JWT_PUBLIC_KEY_PEM.as_bytes().to_vec(),
        )
        .with_copy_to("/oxia/auth/cluster.yaml", cluster_yaml.into_bytes())
        .with_cmd(vec!["bash".to_string(), "-c".to_string(), script])
        .start()
        .await
        .expect("Failed to start auth-enabled Oxia cluster");

    AuthCluster {
        _container: container,
        address: format!("localhost:{host_port}"),
    }
}

/// Builds an authenticated client, retrying while the cluster elects its
/// shard leaders.
async fn connect_with_retry(
    address: &str,
    configure: impl Fn(OxiaClientBuilder) -> OxiaClientBuilder,
) -> OxiaClient {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let builder = OxiaClientBuilder::new()
            .service_address(address.to_string())
            .request_timeout(Duration::from_secs(5));
        match configure(builder).build().await {
            Ok(client) => return client,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "authenticated client could not connect before the deadline: {e}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Static-token auth end to end: an authenticated client operates across
/// shards (unary and stream RPCs all carry the header), while clients with no
/// token or a token for the wrong audience are rejected.
#[tokio::test]
async fn test_auth_token_end_to_end() {
    let cluster = start_auth_oxia().await;

    let token = sign_token(AUDIENCE, Duration::from_secs(3600));
    let client = connect_with_retry(&cluster.address, |b| b.auth_token(token.clone())).await;

    // Spread keys over both shards; puts ride the write stream, gets the read
    // path — both must carry the authorization header.
    for i in 0..10 {
        client
            .put(format!("auth/key-{i}"), format!("value-{i}").into_bytes())
            .await
            .unwrap();
    }
    for i in 0..10 {
        let got = client.get(format!("auth/key-{i}")).await.unwrap();
        assert_eq!(got.value.as_deref(), Some(format!("value-{i}").as_bytes()));
    }
    let keys = client.list("auth/", "auth/~").await.unwrap();
    assert_eq!(keys.len(), 10);
    client.delete("auth/key-0").await.unwrap();

    // No token: the server rejects every RPC with Unauthenticated, a
    // permanent error, so the client fails fast.
    let no_token = OxiaClientBuilder::new()
        .service_address(cluster.address.clone())
        .request_timeout(Duration::from_secs(3))
        .build()
        .await;
    assert!(
        no_token.is_err(),
        "a client without a token must be rejected"
    );

    // Valid signature but an audience the server does not allow.
    let wrong_audience = OxiaClientBuilder::new()
        .service_address(cluster.address.clone())
        .request_timeout(Duration::from_secs(3))
        .auth_token(sign_token("some-other-service", Duration::from_secs(3600)))
        .build()
        .await;
    assert!(
        wrong_audience.is_err(),
        "a token for a different audience must be rejected"
    );

    client.close().await.unwrap();
}

/// Token rotation: the provider is consulted per RPC, so a client whose
/// tokens expire quickly keeps working as long as the provider mints fresh
/// ones.
#[tokio::test]
async fn test_auth_token_provider_rotation() {
    let cluster = start_auth_oxia().await;

    // Each token lives 2 seconds; the provider signs a fresh one per request.
    let client = connect_with_retry(&cluster.address, |b| {
        b.auth_token_provider(|| sign_token(AUDIENCE, Duration::from_secs(2)))
    })
    .await;

    client.put("rotate/key", b"v1".to_vec()).await.unwrap();

    // Let the token used above expire; new RPCs must succeed because the
    // provider is called again for each of them.
    tokio::time::sleep(Duration::from_secs(3)).await;

    client.put("rotate/key", b"v2".to_vec()).await.unwrap();
    let got = client.get("rotate/key").await.unwrap();
    assert_eq!(got.value.as_deref(), Some(b"v2".as_ref()));

    client.close().await.unwrap();
}
