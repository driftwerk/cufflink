# Binding a database role before a service's first deploy

## Problem

Under `db_roles: enforce` a service whose schema already belongs to a
database role (an [adopted](../schema-ownership.md#adopting-a-schema-that-exists)
schema, say) could not be onboarded in one pass:

- Its first deploy has no binding, so the platform tries to give it a
  role of its own, which provisioning refuses: the schema belongs to
  another role (`schema X belongs to Y, not to the platform`). That reason
  was only logged.
- A first deploy that runs DDL (a SQL migration, `on_migrate`) was then
  refused by its DDL session, which found no verified binding (503, "has
  no database role"). One that runs none was activated, and every request
  to the service answered 503 until it was bound.
- `cufflink services db-role set` answered 404 for a service the platform
  had not recorded, and only a deploy recorded one.

What worked was to deploy once, which records the service, bind it, then
deploy again. That leaves a failed or unservable first version in the
service's history.

## Decision

`db-role set` may record the service itself, before any deploy, when it is
asked to:

```sh
printf '%s' "$ROLE_PASSWORD" | cufflink services db-role set harbor catalog-service \
  --register --schema catalog_service --role catalog_service_user --password-stdin
```

- `register` (`--register`) records a service the tenant has not deployed,
  with the name given and `schema` (`--schema`, by default the name with
  `-` as `_`, as a manifest without `schema_name` has), then binds it as
  for any service. Without `register`, an unknown service is a 404 as
  before, so a mistyped name never records a service.
- A registration names its role (`--role`). A service with no binding
  whose schema the platform may own is given a role of its own at its
  first deploy already; only a role that exists needs binding first.
- The name and schema are checked as a deploy checks them: a valid service
  name, a valid, unreserved schema no other service of the tenant has.
- `register` with a service that exists binds it as before, and is refused
  when `schema` names another schema than the one recorded. A set can be
  sent again until it succeeds.
- The deploy lock of the service is held from the registration to the end
  of the binding, so no deploy of it runs in between. When the binding is
  refused, a service the request recorded is removed again, so a failed
  set leaves nothing behind.
- The service's first deploy finds the service and its verified binding.
  It runs its DDL and SQL as the bound role, as any later deploy does, and
  `after_deploy` verifies the binding again against the served tables of
  the version it activated.

### Under enforce, a first version needs a verified role

Under `enforce` no statement of a service runs without a verified
binding, so a first version that would run without one is not activated.
Before a service's first version, the platform provisions its role as it
did; when provisioning fails or leaves the binding unverified, the deploy
is now refused (422) with provisioning's reason, which for a schema of
another owner names the owner and the `db-role set` that binds it. A
redeploy of a service that runs is not refused this way, so a deploy that
mends a service is never blocked by it; under `warn` the failure is logged
as before. A plan does not provision, so it does not show the refusal.

The served tables are not judged before the first deploy: a version's
tables may be made by its own DDL, so verification reads the tables of
the version that runs, and `after_deploy` verifies the binding again once
the first version is active, as after any deploy.

### An active service that has never run may be deployed inactive

A recorded service has a status, and a deploy keeps it: an active one
refuses `--inactive` (a deploy never stops a running service), and an
inactive one stays inactive until it is enabled. A registration records
the service active, as a first deploy without `--inactive` does, so a
plain first deploy starts it. An active service with no deployment runs
nothing a deploy could stop, so its first deploy may now ask for
`--inactive`, and a plan answers the status that deploy would set; the
status is settled under the service's deploy lock. An inactive service
that never ran stays inactive, as before: a first `--inactive` deploy that
failed is not started by the deploy that follows it.

A deploy whose schema differs from the one recorded is refused, as for a
deployed service; the message now says the schema is fixed once the
service is recorded, which a registration also does.

## Security

- Binding stays a platform administrator's act (`PlatformAdmin`, as every
  `db-role` route). A tenant's deployer can neither record a binding nor
  name a role: the manifest carries no role.
- What a registration hands a deployer is what binding a recorded service
  always did: the next deploy of that service name in that tenant runs as
  the role. A binding is checked as it is set (the role exists, logs in
  with the password given, is not a superuser, the platform's role or the
  server's administrator, reaches its schema, and carries no other
  service), so a registration cannot bind what a binding of a deployed
  service could not.
- `enforce`'s guarantee is unchanged: a session of the service runs as its
  role once the binding is verified, and is refused while it is not.
- Audit: the set is recorded as `db_role.set`, whose details now carry
  `registered` (whether the request recorded the service) and `schema`,
  beside the role. The verification it runs is recorded as
  `db_role.verify`, as before.
- A registration nothing deploys stays a service with no deployment and a
  verified binding. `cufflink services delete` removes it and its binding,
  and leaves the role it was bound to as it is: the platform changes no
  role it did not make.

## Alternatives not taken

- **A role named in the manifest or at deploy.** Manifests are written and
  deployed by a tenant's deployers, not by platform administrators, so a
  manifest naming a role would let a deployer pick any role in the tenant
  database. A manifest also has no place for an adopted role's password.
- **A pending binding consumed by the first deploy.** Its password would
  have to be sealed before the service has an id, while a binding's
  password is sealed for its service's id. It would add a sealed table of
  its own to every reseal, verify and storage mode, and a second way for a
  binding to exist. Recording the service reuses the binding, its sealing,
  `show`, `verify` and `rotate`, and the `enforce` gate as they are.
- **Taking the binding from the schema's owner at the first deploy.** The
  platform does not know the owner's password, and taking a schema's owner
  as a service's role without an administrator saying so would let a
  deployer reach whatever that role owns.

## Tests

- Unit: the status a deploy admits for an active service that has never
  run; the request a set reads; the CLI's body with and without
  `--register` and `--schema`.
- Integration (`platform/tests/db_roles_bindings.rs`), under `enforce`:
  - one pass: register and bind, then the first deploy succeeds and its
    statements run as the bound role;
  - the old way still works: a first deploy refused, then a binding, then
    a deploy;
  - an unknown service without `register` is a 404; a registration without
    a role, with an invalid or taken schema, or whose binding is refused,
    leaves no service behind;
  - a first deploy that declares another schema than the registration is
    refused;
  - a registered service planned, and deployed `--inactive`;
  - deleting a registered service that never ran leaves its role as it
    was.
