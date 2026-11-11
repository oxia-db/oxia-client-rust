# Release process

`oxia-client` is released to [crates.io](https://crates.io/crates/oxia-client) and announced as
[GitHub Releases](https://github.com/oxia-db/oxia-client-rust/releases).

**A release is a tag.** Push `vX.Y.Z` and the [`Release`](./.github/workflows/release.yml) workflow
does the rest. There is no version bump to land first and no release PR to merge.

```shell
git switch main && git pull
git tag -s v0.3.0 -m "oxia-client 0.3.0"
git push origin v0.3.0
```

Then [write the release notes](#writing-the-notes), which is the one step that is not automated.

To try it first, run the `Release` workflow from the Actions tab ("Run workflow") and give it the tag
you intend to push. The tag does not have to exist. Everything runs — the version is stamped, the
bump is checked, the crate is packaged and verified, and every platform's archive is built — except
the three things that cannot be taken back: the publish is a dry run, no GitHub Release is created,
and the archives are left as artifacts of the run for you to download and inspect.

## Where the version lives

In the tag, and nowhere else. `oxia-client/Cargo.toml` carries `version = "0.0.0-dev"`, which is
never published: the workflow stamps the tag's version into the manifest before it builds or
publishes anything, and does not commit it. The stamping is
[`stamp-version.py`](./.github/scripts/stamp-version.py), shared by the publish job and the FFI
build so that the crate, the archives and the library's own `oxia_version_string()` cannot
disagree about what they are.

A consequence worth knowing: a library built from a checkout reports `0.0.0-dev`. That is
deliberate — a development build should not claim to be a release — and it is what you will see
locally, in the archives attached to a pull request, and in anything built from a branch.

You choose the number, so choose it against what changed:
[Semantic Versioning](https://semver.org), and while the crate is at 0.x a breaking change bumps
the minor and anything else the patch. The workflow runs
[`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) against the last
published release with the tag's version in place, so a tag whose bump is too small for the API
change fails there — before anything reaches crates.io.

## What the workflow does

1. **Refuses a tag that is not on `main`.** Releases come from `main`, which `ci.yml` has already
   tested, so the release run does not repeat the full suite.
2. **Stamps the version** from the tag, rejecting anything that is not `vMAJOR.MINOR.PATCH` (a
   pre-release suffix such as `v0.3.0-rc.1` is allowed).
3. **Checks the bump** with `cargo-semver-checks`, as above.
4. **Publishes to crates.io**, unless that version is already there — so a re-run is safe.
5. **Creates the GitHub Release** at the tag, titled `oxia-client X.Y.Z`, with an empty body. An
   existing release's notes are never overwritten.
6. **Attaches the FFI binaries**: [`ffi-binaries.yml`](./.github/workflows/ffi-binaries.yml) builds
   `oxia-client-ffi` for every supported platform and uploads the archives to the release.

If a step fails, delete the tag (`git push --delete origin vX.Y.Z`), fix the problem, and tag
again. Nothing is published until step 4, and crates.io cannot be undone past it: a version can be
yanked, never deleted or replaced. A manual run does steps 2, 3 and 6 and stops short of every
irreversible one, so it is the cheap way to find out that step 1 or 2 would have refused the tag.

## Writing the notes

The release is created with an empty body, named `vX.Y.Z`. Edit it to add the highlights, the
breaking changes and any migration notes, and give it the `oxia-client X.Y.Z` title the earlier
releases use:

```shell
gh release edit vX.Y.Z --title "oxia-client X.Y.Z" --notes-file notes.md
```

There is intentionally no in-repo `CHANGELOG.md`.

## The FFI archives

Only `oxia-client` goes to crates.io. `oxia-client-ffi` and `oxia-perf` are not published:
`oxia-perf` is a workspace tool, and the FFI shim is distributed as the archives attached to each
release instead.

That is deliberate. The shim is the core other language SDKs are driven from, and requiring every
wrapper author to have a Rust toolchain and a matching MSRV is the friction that stops wrappers
being written. The archives are named after the release tag, and each one records the ABI version
of the header it ships so a caller can tell which ABI it speaks without unpacking it.

The FFI crate's own `version` in `Cargo.toml` is an implementation detail that nothing reports; see
the versioning section of [`oxia-client-ffi/README.md`](./oxia-client-ffi/README.md) for the two
numbers that are reported and what each is for.

## Required setup (one-time)

- **`CRATE_TOKEN` repository secret** — a crates.io API token with the **publish-update** scope
  (plus **publish-new** for a brand-new crate name), scoped to include `oxia-client`. It is passed
  to `cargo publish` as `CARGO_REGISTRY_TOKEN`; a token lacking these scopes fails publishing with
  `403 Forbidden`.
- Nothing else. In particular this needs no *"Allow GitHub Actions to create and approve pull
  requests"* permission, because nothing opens a pull request, and no Personal Access Token,
  because a tag pushed by a person starts the run.
