# Contributing to Cufflink

Thanks for your interest. Bug reports, fixes, docs, and well-scoped features are
all welcome. This document covers how to get a change from your machine into
`main`.

## Before you start

- **Bugs**: open an issue with a reproduction. If you already have a fix, a PR
  with the failing test is even better.
- **Features or behavior changes**: open an issue first so we can agree on the
  approach before you invest time. Cufflink is a platform other services depend
  on, so anything touching the manifest format, host functions, route security,
  or auth needs a design conversation.
- **Security issues**: never open a public issue. See [SECURITY.md](SECURITY.md).

## Development setup

```bash
# Toolchain is pinned by rust-toolchain.toml (rustup picks it up automatically)
rustup show

# Local services: PostgreSQL, NATS, Keycloak
docker compose up -d postgres nats keycloak

# Build and test the whole workspace
cargo build --workspace
cargo test --workspace
```

[docs/setup.md](docs/setup.md) has the full walkthrough, and
[docs/building-on-cufflink.md](docs/building-on-cufflink.md) explains the
platform's moving parts.

## Keep it general

Cufflink is a general-purpose platform, and everything in this repository is
public. Do not name a real company, customer, deployment or private ticket in
code, tests, docs or commit messages, and do not copy schemas, specs or tests
from a system built on Cufflink. Test behaviour with invented fixtures, and
describe what the platform does rather than who needed it.

## Making changes

1. Fork the repo and create a branch from `main`
   (`feat/short-description`, `fix/short-description`).
2. Make your change. Keep PRs focused: one logical change per PR.
3. Add or update tests. New host functions, CLI commands, and route behavior
   need coverage; bug fixes should include a regression test.
4. Update docs under `docs/` and the relevant crate's rustdoc when you change
   user-facing behavior.
5. Run the same checks CI runs:

   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```

6. Open a PR against `main`. Fill in the template; explain *why*, not just what.

CI must be green and any review conversations resolved before merge.

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(platform): add per-tenant rate limits
fix(cli): keep .next/cache out of the web artifact
docs(fn): document generate_uuid
```

Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `ci`, `chore`, `style`.
Scope is the crate or area (`platform`, `cli`, `fn`, `sdk`, `web-runner`,
`worker`, `db`, `auth`). The body should explain the reasoning and any
trade-offs; a future reader should understand the change from the message alone.

## Versioning and releases

Published crates (`cufflink-types`, `cufflink-sdk-macros`, `cufflink`,
`cufflink-migrate`, `cufflink-pgcodec`, `cufflink-fn`, `cufflink-cli`,
`cufflink-nats`, `cufflink-native`) follow semver. Don't bump versions in a feature PR;
maintainers cut releases by tagging `v*`, which publishes to crates.io and
pushes Docker images.

## License

A contribution is licensed under the license of the part of the repository it
changes: FSL-1.1-ALv2 for the platform and server components, MIT OR
Apache-2.0 for the published crates and `examples/` (see the
[README](README.md#license)). By submitting a contribution you also grant the
licensor a perpetual, worldwide, royalty-free, irrevocable license to use,
modify, sublicense and distribute it under any terms, including in commercial
products and services.

## Code of conduct

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
