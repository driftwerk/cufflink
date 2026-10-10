# Working in this repository

Cufflink is a public, general-purpose platform. Everything committed here is
published: source, tests, fixtures, docs, commit messages, branch names, and
pull request titles and descriptions.

## Nothing customer-specific, ever

- Never write the name of a real company, product, customer, tenant, domain,
  cloud project, bucket or person that uses or runs Cufflink. Never write an
  identifier from a private tracker (ticket keys, review or audit codes).
- Never copy or adapt material from a consumer's codebase or infrastructure:
  schemas, migrations, API specs, route lists, handlers, tests, dashboards,
  configuration. A renamed copy is still a copy.
- Never explain a change by who needed it or what happened to them. Say what
  the platform does and what the test proves. No "as X's services do", no
  rollout steps for a particular deployment, no incident stories.
- Test a behaviour with an invented fixture. The examples in this repository
  use the tenants `acme`, `globex`, `harbor` and `orchard`, hosts under
  `example.com`, and an invented library domain (`loan-service`,
  `catalog-service`); reuse those rather than inventing more.
- If you delegate work to another agent or tool, repeat these rules in the
  brief. They do not carry over on their own.

Before every commit and before opening a pull request, read the diff and the
message once more for anything that points outside this repository.

## Conventions

- Commits are authored with the maintainer's public identity as configured
  in git; do not override `user.name` or `user.email`.
- `cargo fmt`, `cargo clippy --all-targets -- -D warnings` and
  `cargo test --workspace` must pass. Integration tests start their own
  Postgres, NATS and Redis when the `CUFFLINK_TEST_*` variables are unset.
- Web tooling uses `pnpm`.
- Releases are cut by tag; do not change crate versions outside a release.
