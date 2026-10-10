# Partner Keys

Partner keys let an external system (a book supplier, an integration partner)
call a tenant's `api_key` routes. They are separate from the deploy-authority
API keys of the [authentication](authentication.md) docs, so a partner key can
never reach the control plane.

Requires a platform that advertises `partner_keys`.

## Format and storage

```
cfp_<prefix>_<secret>
cfp_0AbC3dEf9GhI_5k2J…   (12-character prefix, 256-bit secret, both base 62)
```

The platform stores the prefix (to find the key) and the SHA-256 of the
secret; the raw key is shown once, when it is created. A key has:

- `scopes`: one or more `resource[:action]` names in lowercase letters,
  digits and `_ . -` (`books:ingest`, `receipts:*`). `resource:*` grants every
  action on the resource; a route requiring a scope accepts a key holding
  that scope or its resource wildcard.
- an expiry, 1 to 365 days (default 90);
- `revoked_at`, set by a revocation, and `last_used_at`, recorded at most once a
  minute.

## Managing keys

Callers need deploy authority, or the `partner_keys:manage` permission in the
tenant, and may only manage their own tenant's keys.

| Request | Answer |
|---------|--------|
| `POST /api/tenants/{slug}/partner-keys` `{"name", "scopes", "expires_in_days"}` | `201 {"id", "key", "prefix", "scopes", "expires_at"}` |
| `GET /api/tenants/{slug}/partner-keys` | `{"keys": [...]}`, never the secret or its hash |
| `DELETE /api/tenants/{slug}/partner-keys/{id}` | `200 {"revoked": true}`; `404` when already revoked or unknown |

```bash
cufflink keys partner create acme-books --scope books:ingest --expires-in-days 30
cufflink keys partner list
cufflink keys partner revoke <id>
# --tenant <slug> overrides Cufflink.toml
```

Creates and revocations are logged and written to the audit log with the
real subject.

## Verification

A route with the `api_key` posture reads `X-Api-Key` only. The key's prefix
finds it (each replica caches keys for 30 seconds, unknown prefixes too, and
clears the cache on `cufflink.partner_keys.changed`, which every create and
revoke publishes); then the secret's hash is compared in constant time, and the
key must be unrevoked, unexpired, of the route's tenant and hold a scope
covering the route's. Every refusal is the same `401 {"error": "Invalid API
key"}`; the reason is only counted:

- `cufflink_partner_key_rejected_total{reason}` — `malformed`, `unknown`,
  `revoked`, `expired`, `tenant`, `scope`;
- `cufflink_partner_keys_active` — keys in force.

The guest of an `api_key` route sees `"api_key": {"id", "name", "scopes"}` and
no `auth`.

A route takes partner keys with the `api_key` posture, naming the scope it
requires (`resource[:action]`; a granted `resource:*` covers every action):

```rust
("POST", "/ingest-book/{branch}", "ingest_book", api_key("books:ingest")),
```

Its guest reads the key as `req.api_key()` (`id`, `name`, `scopes`) and is
never passed the `X-Api-Key` header. Bearer tokens and deploy API keys are
not accepted on these routes. The OpenAPI document marks them with an
`api_key` scheme (`in: header`, `name: X-Api-Key`).
