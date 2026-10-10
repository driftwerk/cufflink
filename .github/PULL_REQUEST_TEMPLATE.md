## What

<!-- One or two sentences: what changes and which crate/area. -->

## Why

<!-- The problem or motivation. Link the issue if there is one: Closes #123 -->

## How

<!-- Anything a reviewer needs to know about the approach or trade-offs. -->

## Checklist

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] Tests added or updated for the change
- [ ] Docs updated (`docs/`, rustdoc) if user-facing behavior changed
- [ ] No behavior change to manifests, host functions, or auth without prior discussion
