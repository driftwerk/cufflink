# Scoped Permissions

A permission grant can carry a **scope** that decides *which rows* it reaches:
the caller's own, the rows matching one of the caller's token claims, or all
of them. A table opts in with `scoped_by`; every other table behaves exactly
as it did before scopes existed.

Requires a platform that advertises `scoped_permissions` (see
[versions](versions.md)); a manifest with a scoped table demands it, so an older
platform refuses the deploy instead of ignoring the scope.

## Grants

Only the suffix after `@` has a grammar. The part before it is compared
exactly as always.

```
grant := base [ "@" scope ]
scope := "own" | "all" | "field(" column "=" claim ")"
column := [a-z_][a-z0-9_]*          claim := [A-Za-z0-9_.:-]+
```

| Role string | On a `scoped_by` table | Everywhere else |
|-------------|------------------------|-----------------|
| `books:view` | the caller's own rows (`@own`) | `books:view`, as before |
| `books:view@own` | the caller's own rows | `books:view` |
| `books:view@field(branch_id=branch_id)` | rows whose `branch_id` is the caller's `branch_id` claim | `books:view` |
| `books:*@all` | every row, every operation | `books:*` |
| `books:view@everyone` | nothing (malformed) | nothing |

- A well-formed scoped grant also grants the unscoped permission, so adding
  `@all` to a staff role never removes access anywhere.
- A malformed suffix grants nothing. Role writes (`POST`/`PUT
  /api/tenants/{id}/roles`) and a manifest's `authorization.default_roles`
  refuse it with 422 naming the string; strings without `@` are never
  inspected.
- A `@field` claim is read from the token's claims (plus `sub`, `email` and
  `preferred_username`): the claim of exactly that name, else the dotted path
  through nested claims, so `@field(branch_id=branch.id)` reads
  `{"branch": {"id": …}}` (Keycloak nests a mapper whose name contains dots).
  `realm_access` is not among the claims. A string or number is one value, an
  array its elements; a missing claim matches **no** rows, never all of them.
  A `@field` grant naming a column the table does not have grants nothing
  there (logged once).

> **A scope only narrows `scoped_by` tables.** Everywhere else a scoped grant
> acts as its unscoped permission: `books:view@field(…)` reads *every* row of
> a `books`-area table without `scoped_by`, and passes a guest's
> `auth.can("books", "view")` and a `permission(...)` route posture. Opt the
> tables in with `scoped_by` *before* assigning narrowed roles, and have custom
> routes narrow with `Auth::scope_for` or a `scoped(...)` posture rather than
> gating on `Auth::can`.

## Tables opt in

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "books", auth_required, owner_field = "user_id", scoped_by = "books")]
pub struct Book {
    #[key]
    pub id: Uuid,
    pub user_id: String,
    pub branch_id: String,
    pub title: String,
}
```

- `scoped_by` names the permission area whose grants' scopes apply; it must be
  declared in `authorization.areas`.
- `owner_field` is required: it is the column `@own` reaches.
- `permission_area` defaults to the `scoped_by` area; if both are set they
  must be equal.
- `owner_self_service` keeps working: an owner with no grant at all still
  reaches their own rows.
- No column may be named `scope`: `?scope=` is how a caller narrows.

A scope decides which rows a caller reaches. Which columns of those rows it
is answered, and may set, is a grant of its own: see
[column permissions](column-permissions.md).

## The effective scope

For each request the caller's grants for `{area}:{operation}` are combined:

1. A service account, or any matching `@all` grant: **all rows**.
2. Otherwise the union (OR) of each matching grant's rows: `@own` (and
   unscoped grants) the caller's rows, `@field` the claim's rows.
3. No matching grant: the caller's own rows on an `owner_self_service` table,
   otherwise **403** `Missing permission: {area}:{operation}`.

`?scope=own` narrows a request to the caller's own rows (within whatever else
the grants allow); `?scope=all` insists on all rows and answers **403**
`scope=all not granted for {area}:{operation}` unless the caller has them. A
scope never widens.

| Operation | On a scoped table |
|-----------|-------------------|
| list, GraphQL | only reachable rows are listed and counted |
| get | a row outside the scope is **404** (no existence oracle) |
| create | `@all`: as sent. A lone `@own`: the owner column is set to the caller. A lone single-valued `@field`: its column is filled when absent, **403** when the body names another value. Otherwise the row must fall inside the scope, else **403**. The row as stored is checked again, and refused (**403**, nothing written) if it lies outside the scope |
| update | a row outside the scope is **404**; the updated row must still be inside the caller's scope, else **403** and nothing changes (re-sending the unchanged owner or claim value is fine) |
| delete | a row outside the scope is **404**; the response body is unchanged |
| batch | each operation is resolved as above, a delete of a row outside the scope included (**404**); any refusal fails the whole batch and nothing is written |
| SSE `/{table}/stream` | events whose row is outside the scope are not delivered |

Every value a write to a scoped table sets is bound as its column's type, as
under `?filter_mode=v2`: a value that is not one (`"5x"` for an integer) is
**400** naming the column rather than coerced, so the value the scope was
checked against is the value stored.

Hooks on a scoped table receive the effective scope as `"scope"` in their
envelope, and the rows they are shown as `existing` are scoped too. Audit
events carry `"scope": "all" | "own" | "field"`. Both keys appear only for
scoped tables.

The effective scope is serialised for guests as:

```json
{"kind": "all"}
{"kind": "rows", "predicates": [
  {"owner": {"column": "user_id", "subject": "…"}},
  {"claim": {"column": "branch_id", "values": ["branch-a"]}},
  {"and": [{"owner": {…}}, {"or": [{"claim": {…}}]}]}
]}
```

Predicates are OR-ed; `and` appears only after `?scope=own` narrowed a
field scope.

## Guests

`auth.permissions` keeps its exact strings: a scoped grant appears as its base
(`books:view@all` → `books:view`), so an SDK comparing strings keeps working.
The originals are in `auth.scoped_permissions`, present only when the caller
has any. `cufflink-fn` reads them with:

```rust
use cufflink_fn::GrantScope;

match auth.scope_for("books", "view") {
    Some(GrantScope::All) => { /* every row */ }
    Some(GrantScope::Field { column, claim }) => { /* rows where column = auth.claim(&claim) */ }
    Some(GrantScope::Own) => { /* the caller's rows */ }
    None => { /* no grant */ }
}
```

`scope_for` returns the widest matching grant (all, then the first field
grant, then own). It is a single grant, where the platform's CRUD routes reach
the rows of every `@own` and `@field` grant together, so a guest applying it
never sees more than the platform would. `auth.claim` resolves dotted claim
names as the platform does.

## Custom routes

```rust
("GET", "/assets", "list_assets", scoped("assets", "view", owner = "customer_id")),
```

A `scoped` route admits an authenticated caller with a grant on
`{area}:{operation}` and hands the guest the rows it reaches, computed as for
a table with `owner` as the `@own` column (without one, `@own` grants reach
nothing). No grant answers 403; a service account reaches everything. The
guest keeps `auth` and gets `scope` — `{"kind": "all"}` or `{"kind": "rows",
"predicates": […]}` in the form above — as `req.scope()`. With `db-v2` it
renders as a condition with its values bound after the query's own:

```rust
let mut params = vec![Param::from(status)];
let condition = req.scope().map_or("FALSE".to_string(), |scope| scope.to_sql(&mut params));
let rows = sql::query(
    &format!("SELECT * FROM assets WHERE status = $1 AND {condition}"),
    &params,
);
```

The area must be declared in `authorization.areas`.

The values arrive typed as the columns of the tables with `scoped_by` set to
the area declare them (a claim `"7"` against an integer column is `7`); a value that
is not of the column's type is dropped, so it reaches no row, and an empty
scope renders `FALSE`, as in CRUD. A column no such table declares keeps its
values as the claims gave them, and the query binds them as the column's
type, so a value that cannot be one fails the query.

## Metrics and logs

- `cufflink_scope_decisions_total{table, op, scope}` — `scope` is `all`, `rows`,
  `denied`, `widen` or `invalid`.
- `target: "cufflink::scope"` debug line per decision; a throttled warning for a
  malformed grant or a field grant naming an unknown column.

## Mapping roles

| Role | Grant |
|------|-------|
| Global staff | `books:*@all` |
| Branch staff | `books:*@field(branch_id=branch_id)` (claim from a Keycloak mapper) |
| Self | `books:*` or `books:*@own` |

Adopting scopes on an existing service: add `@all` to staff roles first (safe:
it changes nothing until a table is scoped), then deploy `scoped_by` on the
owner tables one service at a time.
