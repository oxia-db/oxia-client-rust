# Contributing to oxia-client-rust

Thanks for contributing! This repository is the native Rust client SDK for
[Oxia](https://github.com/oxia-db/oxia). Please be kind and respectful to the community —
see the Oxia [Code of Conduct](https://github.com/oxia-db/oxia/blob/main/CODE_OF_CONDUCT.md).

## Prerequisites

- **Rust 1.99+** (edition 2024) — install via [rustup](https://rustup.rs). 1.99 is the MSRV
  (the floor of the [nanometer](https://crates.io/crates/nanometer) metrics library); please
  don't use features that raise it.
- **CMake** — required by a native build dependency. This repo pins it with
  [mise](https://mise.jdx.dev): run `mise install` in the repo root, or install CMake yourself.
- **Docker** — the integration and interop tests start a real Oxia server in a container
  (`oxia/oxia:nightly-beta`, built daily from Oxia's main branch). Docker must be running to
  execute them.

No `protoc` is required: the gRPC code is generated at build time from the `.proto`
sources with the pure-Rust [`protox`](https://crates.io/crates/protox) compiler.

## Building and testing

```shell
cargo build --workspace
cargo fmt --all -- --check                               # formatting
cargo clippy --workspace --all-targets -- -D warnings    # lints — warnings are errors
cargo test --workspace --lib --doc                       # fast unit + doc tests (no Docker)
cargo test --workspace                                   # everything, incl. Docker-backed tests
```

The Docker-backed tests pull `oxia/oxia:nightly-beta`. On a cold machine, pre-pull it to
avoid a first-run delay and a concurrent-pull race. The tests never re-pull an image that is
already present, so pull it again to pick up the latest daily build:

```shell
docker pull oxia/oxia:nightly-beta
```

Build the API docs locally with:

```shell
cargo doc -p oxia-client --no-deps --open
```

CI runs `fmt`, `clippy -D warnings`, `build`, the full test suite, and
[`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks); all must pass.

## Submitting changes

1. Fork the repository and create a feature branch off `main`.
2. Keep changes focused and match the surrounding style. `cargo fmt` and
   `cargo clippy --all-targets -- -D warnings` must be clean.
3. Add tests for new behavior (unit tests, or integration tests under
   `oxia-client/tests/` for server-observable behavior).
4. **Sign off your commits.** This project requires a
   [Developer Certificate of Origin](https://developercertificate.org/) sign-off — commit with
   `git commit -s` (adds a `Signed-off-by` trailer). The DCO check will fail otherwise.
5. Open a Pull Request with a clear description. Make sure CI is green.

Have a question or an idea? Start a
[discussion](https://github.com/oxia-db/oxia/discussions). All contributions are licensed under
the Apache License 2.0.
