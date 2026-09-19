# oxia-client-rust

[![Crates.io][crates-badge]][crates-url]
[![Docs.rs][docs-badge]][docs-url]
[![CI][ci-badge]][ci-url]
[![MSRV][msrv-badge]][msrv-url]
[![License][license-badge]][license-url]
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Foxia-db%2Foxia-client-rust.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Foxia-db%2Foxia-client-rust?ref=badge_shield)

[crates-badge]: https://img.shields.io/crates/v/oxia-client.svg
[crates-url]: https://crates.io/crates/oxia-client
[docs-badge]: https://img.shields.io/docsrs/oxia-client
[docs-url]: https://docs.rs/oxia-client
[ci-badge]: https://github.com/oxia-db/oxia-client-rust/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/oxia-db/oxia-client-rust/actions/workflows/ci.yml
[msrv-badge]: https://img.shields.io/badge/MSRV-1.85-blue.svg
[msrv-url]: https://github.com/oxia-db/oxia-client-rust
[license-badge]: https://img.shields.io/crates/l/oxia-client.svg
[license-url]: ./LICENSE

The native Rust client SDK for [Oxia](https://github.com/oxia-db/oxia), a distributed
key-value and metadata store. Built on the [Tokio](https://tokio.rs) asynchronous runtime,
it offers an idiomatic, comprehensive API — plus an optional C FFI for use from other
languages.

## Quickstart

```toml
[dependencies]
oxia-client = "0.1"
```

```rust
use oxia::{ComparisonType, OxiaClient};

#[tokio::main]
async fn main() -> Result<(), oxia::OxiaError> {
    let client = OxiaClient::connect("localhost:6648").await?;

    client.put("greeting", "hello").await?;
    let record = client.get("greeting").await?;
    assert_eq!(record.value.as_deref(), Some(b"hello".as_ref()));

    client.close().await?;
    Ok(())
}
```

Every operation returns a builder you chain options onto and `.await`:

```rust
client.put("locks/w1", "").ephemeral().await?;
let floor = client.get("config/z").comparison(ComparisonType::Floor).await?;
let keys = client.list("users/", "users/~").await?;
```

See the [API documentation](https://docs.rs/oxia-client) for the full guide, and the
runnable [examples](./oxia-client/examples).

## Features

Data-plane feature parity with the reference [Go](https://oxia-db.github.io/docs/clients/go)
and [Java](https://oxia-db.github.io/docs/clients/java) clients:

| Feature | Rust | Go | Java |
|---|:--:|:--:|:--:|
| Put / Get / Delete / Delete-range | ✅ | ✅ | ✅ |
| Conditional writes (compare-and-swap) | ✅ | ✅ | ✅ |
| Partition keys | ✅ | ✅ | ✅ |
| Sequential keys | ✅ | ✅ | ✅ |
| Secondary indexes | ✅ | ✅ | ✅ |
| Ephemeral records & sessions | ✅ | ✅ | ✅ |
| Notifications | ✅ | ✅ | ✅ |
| Sequence updates | ✅ | ✅ | ✅ |
| Floor / Ceiling / Lower / Higher get | ✅ | ✅ | ✅ |
| List / Range scan (streaming) | ✅ | ✅ | ✅ |
| Adaptive request batching | ✅ | ✅ | ✅ |
| Automatic retries & shard re-routing | ✅ | ✅ | ✅ |
| TLS | ✅¹ | ✅ | ✅ |
| Token authentication | ✅ | ✅ | ✅ |
| OpenTelemetry metrics | ✅¹ | ✅ | ✅ |

✅ supported

¹ Behind an off-by-default Cargo feature (`tls` and `otel`, respectively).

## Compatibility

The client speaks Oxia's `io.oxia.proto.v1` gRPC protocol and requires a server that serves
it (current Oxia releases; CI runs against the `oxia/oxia:main` image). It does **not** support
the legacy `io.streamnative.oxia.proto` service.

| `oxia-client` | Oxia server | Rust toolchain |
|---|---|---|
| 0.1.x | serves `io.oxia.proto.v1` (recent releases) | 1.85+ (edition 2024) |

## Crates

- **[oxia-client](./oxia-client)** — the core async Rust client (published to crates.io as
  `oxia-client`, imported as `use oxia::…`).
- **[oxia-client-ffi](./oxia-client-ffi)** — a C Foreign Function Interface over `oxia-client`,
  exposing it to C/C++ applications.
- **[oxia-perf](./oxia-perf)** — a load-generation and latency-measurement CLI (like the Go and
  Java SDKs' perf tools): drives a configurable read/write mix at a target rate and reports
  throughput and latency percentiles. Run `cargo run -p oxia-perf -- --help`.

## Documentation

- **API reference:** [docs.rs/oxia-client](https://docs.rs/oxia-client)
- **Concept guide & per-SDK examples:** [Oxia documentation](https://oxia-db.github.io/docs/clients/rust)

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md) for how to build, test, and
submit changes, and [RELEASING.md](./RELEASING.md) for the release process. To report a security
issue, see [SECURITY.md](./SECURITY.md).

## License

Licensed under the [Apache License 2.0](./LICENSE).


[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Foxia-db%2Foxia-client-rust.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Foxia-db%2Foxia-client-rust?ref=badge_large)