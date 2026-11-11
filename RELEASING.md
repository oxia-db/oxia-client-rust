# Release process

`oxia-client` is released to [crates.io](https://crates.io/crates/oxia-client) and announced as
[GitHub Releases](https://github.com/oxia-db/oxia-client-rust/releases). Releases are automated
with [release-plz](https://release-plz.dev).

**Release notes live in GitHub Releases** — this repository intentionally keeps no in-repo
`CHANGELOG.md`, and the notes are written by hand (see step 3 below). release-plz picks the next
version from the commit history, so land changes with
[Conventional Commits](https://www.conventionalcommits.org) messages. While the crate is at 0.x, a
breaking change (`feat!:` / `BREAKING CHANGE:`) bumps the minor version and any other change the
patch; from 1.0, `feat:` bumps the minor, `fix:` the patch, and a breaking change the major.

## How it works

The [`Release-plz`](./.github/workflows/release-plz.yml) workflow runs on every push to `main`:

1. **Release PR.** release-plz keeps a PR open that bumps `oxia-client`'s version according to the
   conventional commits merged since the last release. Review it like any other PR.
2. **Publish.** Merging the release PR makes the workflow `cargo publish` `oxia-client`, push a
   `vX.Y.Z` tag, and create a GitHub Release. The workflow publishes on any push to `main` whose
   `oxia-client` version is not on crates.io yet, so a version bumped by hand is released by the
   next merge, with no release PR.
3. **Write the release notes.** With `changelog_update = false`, release-plz creates the GitHub
   Release with an empty body, named `vX.Y.Z`. Edit it to add the notes (highlights, breaking
   changes, migration notes) and the `oxia-client X.Y.Z` title of the earlier releases:

   ```shell
   gh release edit vX.Y.Z --title "oxia-client X.Y.Z" --notes-file notes.md
   ```

4. **Attach the FFI binaries.** A final job builds `oxia-client-ffi` for every supported platform
   and uploads the archives to the release that was just cut — see
   [`ffi-binaries.yml`](./.github/workflows/ffi-binaries.yml). It is a job of the *same* workflow
   run rather than a workflow triggered by the release, for the reason noted below: a release
   created with `GITHUB_TOKEN` does not start new workflow runs, so an `on: release` workflow
   would never fire.

Only `oxia-client` is released to crates.io. The `oxia-client-ffi` shim and the `oxia-perf` tool
have `release = false` in [`release-plz.toml`](./release-plz.toml), so release-plz never bumps,
publishes, tags, or creates a GitHub Release for them (`oxia-perf` also has `publish = false`).

`oxia-client-ffi` is nonetheless *distributed*, as the archives attached to each release: it is
the core other language SDKs are driven from, and requiring every wrapper author to have a Rust
toolchain and a matching MSRV is the friction that stops wrappers being written. The archives are
versioned by the release tag, not by the crate's own version, and each one records the ABI version
of the header it ships so a caller can tell which ABI it speaks without unpacking it.

## Required setup (one-time)

- **`CRATE_TOKEN` repository secret** — a crates.io API token with the **publish-update** scope
  (plus **publish-new** for a brand-new crate name), scoped to include `oxia-client`. It is passed
  to release-plz as `CARGO_REGISTRY_TOKEN`; a token lacking these scopes fails publishing with
  `403 Forbidden`.
- **Repository setting** — under *Settings → Actions → General → Workflow permissions*, enable
  *"Allow GitHub Actions to create and approve pull requests"* so the release PR can be opened.
- The workflow uses the built-in `GITHUB_TOKEN`. Because PRs opened by `GITHUB_TOKEN` do not
  trigger other workflows, the release PR will not run CI on its own; to get CI on it, supply a
  [Personal Access Token or GitHub App token](https://release-plz.dev/docs/github/token) as the
  `release-pr` job's `GITHUB_TOKEN`.

## Versioning

The crate follows [Semantic Versioning](https://semver.org). `ci.yml` runs
[`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) against the latest
published release to catch accidental breaking changes; release-plz's own semver check is disabled
in `release-plz.toml` to avoid duplicating it.
