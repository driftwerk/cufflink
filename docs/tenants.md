# Tenants on an External Identity Provider

A tenant's users do not have to live in the platform's realm. The tenant
row names the realm its tokens come from, and the platform validates those
tokens against that realm's JWKS.

**The realm must be served by the Keycloak at the platform's
`KEYCLOAK_URL`.** Every Bearer token, on `/svc/...` and on the API alike, is
validated against keys fetched from `{KEYCLOAK_URL}/realms/{realm}`,
whatever the tenant's `keycloak_url` says. The tenant's own `keycloak_url`
is read in one place, the `cufflink login` exchange (`POST
/api/auth/device/token`). A tenant registered with a Keycloak at another
address can therefore mint CLI keys, and its users' Bearer tokens are
refused ([issuer vs. JWKS URL](authentication.md#4-issuer-vs-jwks-url)).

```bash
cufflink tenants create --name Acme --slug acme \
  --tenant-keycloak-url https://sso.acme.example \
  --tenant-keycloak-realm acme
cufflink tenants update acme \
  --keycloak-issuer https://sso.acme.example \
  --service-account-clients service-account-crm,service-account-reports
```

- `--tenant-keycloak-url` is where the platform fetches the realm's JWKS
  for the `cufflink login` exchange, and nothing else. It must be set, with
  the realm, for the tenant's Bearer tokens to be accepted at all: a tenant
  without it is not matched to its realm. The admin API (user search, logout)
  is called at `KEYCLOAK_URL` with the tenant's ops credentials.
- `--keycloak-issuer` is the base of the `iss` claim tokens carry
  (`{issuer}/realms/{realm}`) when it differs from the URL the platform uses,
  for example a browser-facing address in front of an internal one. Tokens
  from another issuer are refused.
- `--service-account-clients` lists the tenant's client-credentials clients
  (matched on `azp`) whose tokens bypass per-user permission checks. Under the
  tenant's `service_accounts=enforce` policy these are the only ones.
- `audience` ([policies](policies.md)) restricts which clients' tokens are
  accepted at all.

Users are then granted Cufflink roles by subject (`POST
/api/tenants/{id}/roles/assign`), and scopes on those roles can use the
external realm's own claims, e.g. `books:view@field(branch_id=branch_id)`
with an `branch_id` mapper on the realm ([scoped
permissions](scoped-permissions.md)).
