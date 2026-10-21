# Examples

Every example runs against a local Oxia at `localhost:6648` (unless noted):

```shell
docker run -p 6648:6648 oxia/oxia:nightly-beta oxia standalone
cargo run --example basic_kv
```

| Example | Shows |
|---|---|
| [basic_kv](basic_kv.rs) | Put, get, list, range-scan, delete, delete-range |
| [basic_versioning](basic_versioning.rs) | Versions and conditional writes (compare-and-swap) |
| [basic_complex_query](basic_complex_query.rs) | Floor / ceiling / lower / higher gets |
| [basic_secondary](basic_secondary.rs) | Secondary indexes |
| [metadata_ephemeral](metadata_ephemeral.rs) | Ephemeral records and sessions |
| [metadata_notification](metadata_notification.rs) | The namespace change feed |
| [streaming_sequence](streaming_sequence.rs) | Sequential keys and sequence updates |
| [streaming_scan](streaming_scan.rs) | Streaming list / range-scan in O(shards) memory |
| [retry_patterns](retry_patterns.rs) | Application-level retries: `is_retryable`, CAS loops, semantic outcomes |
| [graceful_shutdown](graceful_shutdown.rs) | Stop producers → drain in-flight ops → `close()` |
| [security_auth](security_auth.rs) | Bearer-token authentication (static + rotating provider) |
| [security_tls](security_tls.rs) | TLS: system roots, custom CA, mutual TLS — `cargo run --features tls --example security_tls` |
