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

## Tenant buckets

A bucket a tenant brings with it, with keys of its own choosing, is
registered to that tenant by a platform admin:

```bash
cufflink tenants buckets set harbor example-documents --key-prefix leads/
printf '%s' "$SECRET" | cufflink tenants buckets set harbor example-assets \
  --acl public-read --access-key-id AKEXAMPLE --secret-access-key-stdin
cufflink tenants buckets list harbor
cufflink tenants buckets delete harbor example-documents
cufflink tenants buckets release example-documents
```

| Method | Path | |
|--------|------|-|
| GET | `/api/tenants/{slug}/buckets` | The tenant's buckets, retired ones included, with each credential's access key id and never its secret |
| PUT | `/api/tenants/{slug}/buckets/{bucket}` | Registers the bucket, or replaces its rules and credential (`key_prefixes`, `acls`, `credential`) |
| DELETE | `/api/tenants/{slug}/buckets/{bucket}` | Retires it: refused to every guest, still claimed |
| POST | `/api/buckets/{bucket}/release` | Frees a bucket retired for at least two minutes |

Only the tenant's own guests reach the bucket, with exact keys under its
prefixes and the canned ACLs it lists, whatever the tenant's
`storage_scope`. Deleting the tenant retires its buckets. The rules, and
what each refusal means, are in
[tenant buckets](tenant-isolation.md#tenant-buckets).
