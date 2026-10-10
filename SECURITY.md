# Security Policy

Cufflink is a multi-tenant platform that runs untrusted WASM handlers, terminates
authentication, and fronts a shared database. Security reports are taken seriously
and handled privately until a fix is available.

## Reporting a vulnerability

**Please do not open a public issue for security problems.**

Report privately through GitHub's vulnerability reporting:

1. Go to https://github.com/driftwerk/cufflink/security/advisories/new
2. Describe the issue, the affected component (platform, web-runner, worker, CLI,
   SDK, `cufflink-fn`), a minimal reproduction if you have one, and the impact
   as you understand it.

You will get an acknowledgement within 3 business days. We'll keep you updated
as the report is triaged, fixed, and released, and credit you in the advisory
unless you'd rather not be named.

## Scope

In scope:

- Tenant isolation bypasses (reading or writing another tenant's data, routes,
  events, or secrets)
- Authentication or authorization bypasses (Keycloak integration, `auth_required`
  tables, `permission_area` checks, route security postures, deploy authority)
- WASM sandbox escapes or host-function abuse from a guest handler
- Injection via manifests, table definitions, or dynamic CRUD routes
- Secret exposure (vault, staging secrets, tokens in logs or audit events)
- Denial-of-service that a single unprivileged tenant or unauthenticated caller
  can trigger against the shared platform

Out of scope:

- Vulnerabilities in a deployment's own handler code or web app
- Issues that require an already-compromised platform host or database
- Findings in third-party dependencies with no demonstrated impact on Cufflink
  (please report those upstream; a `cargo audit` advisory alone is not a report)

## Supported versions

Fixes land on `main` and ship in the next release. Only the latest published
version of each crate and image receives security fixes; deployments should track
the latest `v*` tag.

## Disclosure

We aim to publish a fix and advisory within 90 days of a confirmed report, sooner
for issues that are actively exploitable. Coordinated disclosure is appreciated.
