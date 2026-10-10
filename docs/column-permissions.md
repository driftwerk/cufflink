# Column Permissions

A column can ask a grant of the caller: to **read** its values through the
data routes, to **write** them, or both. A caller without the grant is
answered rows without the column and is refused a write that sets it; every
other column, and every table without such a column, behaves exactly as
before.

It is what lets one table serve both the people a row is about and the
people who work on it: the owner of a row reaches the row, and not the
notes, identifiers and state the staff keep on it.

Requires a platform that advertises `column_permissions` (see
[versions](versions.md)); a manifest with a restricted column demands it, so
an older platform refuses the deploy instead of serving the column to
everyone.

## Declaring

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(
    name = "listings",
    auth_required,
    permission_area = "listings",
    owner_field = "customer_id",
    owner_self_service
)]
pub struct Listing {
    #[key]
    pub id: Uuid,
    pub customer_id: Uuid,
    pub title: String,
    #[restricted(read = "internal", write = "internal")]
    pub ops_notes: Option<String>,
    #[restricted(write = "manage")]
    #[default("draft")]
    pub status: String,
}
```

```rust
authorization: {
    areas: [("listings", ["view", "create", "edit", "delete", "internal", "manage"])],
    default_roles: [
        ("staff", "Staff", ["listings:view@all", "listings:edit@all", "listings:internal"]),
        ("manager", "Manager", ["listings:*@all"]),
    ],
}
```

- `read` and `write` each name an **operation of the table's permission
  area** (`scoped_by`, else `permission_area`): `read = "internal"` on a
  `listings` table asks `listings:internal`. Either may be left out: a
  column with only `write` is read by every caller of the table, one with
  only `read` is written by every caller that may write the row.
- A grant is matched as every permission is: `listings:internal`,
  `listings:*` and `*` all grant it. A service account is asked nothing.
- **Owning the row grants neither.** On an `owner_self_service` table the
  owner reaches their rows without a grant, and their restricted columns
  only with one.
- **A scope does not narrow a column grant.** `listings:internal@own` grants
  the column on every row the caller reaches, as `listings:internal` does:
  which rows are reached is the row operation's grant alone
  ([scoped permissions](scoped-permissions.md)).

A deploy is refused (the macro refuses the first seven at compile time) when:

| The declaration | Why |
|-----------------|-----|
| names neither `read` nor `write`, or an operation that is not an identifier | it asks nothing a role could grant |
| is on a table without `permission_area` or `scoped_by` | there is no area for the operation to be of |
| is on the primary key, the `owner_field`, or a `#[timestamp]` column | every caller addresses, scopes and orders rows by them |
| restricts `read` of a column named `created_at` | listings are ordered by it for every caller |
| restricts `write` of a column that is neither nullable nor has a default | a caller without the grant could create no row |
| restricts `read` of a column the table's declared `search` names, or of any column where the search reads a `search_vector` the platform does not keep | the declared search matches it for every caller |
| restricts `read` of a unique column (`#[unique]`, or a unique index over it) without restricting `write` | a caller that may write it learns the values other rows hold from the conflicts |
| names an area `authorization.areas` does not declare, or an operation the area does not list | a misspelt operation would withhold the column from everyone |
| is on a [private table](schema-ownership.md#private-tables) | no data route serves it |

## What a caller without the grant gets

| | A column the caller does not read | A column the caller does not write |
|---|---|---|
| list, get, GraphQL | absent from every row | as any column |
| the row a create, update or batch answers | absent | as stored |
| `?select=`, `?sort=`, `?column=value`, a GraphQL argument | as a name the table never had: ignored | as any column |
| `?filter=`, a GraphQL field | as a name the table never had: a v1 filter ignores it, `?filter_mode=v2` answers **400** `unknown field`, GraphQL **400** `Unknown field` | as any column |
| `?search=`, `?fts=` on a table without a declared `search` | not searched | as any column |
| SSE `/{table}/stream` | absent from every event's `data`; the caller is sent only CRUD events (`action`, `service`, `table`, `tenant_id` and the row's columns it reads), and nothing else a service publishes on the table's subject | as any column |
| create, update, batch | as any column | **403** `Missing permission: listings:manage (to set 'status')`, and nothing is written; in a batch the whole batch is refused |

The names of restricted columns are not secret: the OpenAPI spec and the
GraphQL schema list them, with what they ask (below). Their values are.

A write is checked on the body the caller sent, before `before_create` or
`before_update` runs: a hook is the service's own code, and what it puts in
`data` is written whatever the caller may set. That is how a column the
caller cannot set is still given a value on their behalf.

Rows are still reached through a column the caller does not read: a
`@field(team_id=team)` grant scopes by `team_id` whether or not the caller
is answered it. Do not restrict `write` of a column a `@field` grant scopes
by: a caller whose claim holds several values names the column in a create,
and would be refused.

Three things a restriction does not do:

- **A wildcard grants columns too.** `listings:*`, at any scope, is every
  operation of the area, a restricted column's among them. A role meant for
  the people a row is about lists its operations (`listings:view@own`,
  `listings:edit@own`) and never the wildcard.
- **A constraint still answers.** A `check(...)` that compares a column the
  caller writes with one it does not read tells the caller, by the 422 it
  answers, how the two compare. Keep such a rule in a hook, or restrict the
  written column too.
- **A stream keeps the view it opened with.** A grant given or taken away
  reaches an open `/{table}/stream` when the caller reconnects, as its rows
  do.

## What is not narrowed

The platform narrows what the **data routes** answer. Everything a service's
own code is handed holds the whole row, as it must to do its work:

- hook envelopes (`data`, `existing`, `result`);
- CRUD events on `dw.{tenant}.{service}.{table}.*`, and with them
  subscriptions, webhooks and the outbox;
- audit records (which `audit_redact` redacts, for every reader);
- the service's SQL, `cufflink sql`, backups and exports.

A custom route answers what its handler writes. One that reads a table with
restricted columns applies the same rule with `auth.can(area, operation)`.

An [SSE bridge](routing.md#server-sent-events) without a reducer forwards
each message as it is. A deploy refuses one whose subject carries the events
of one of the service's own tables that withholds a column
(`dw.{tenant}.{service}.{table}.>`, or a wildcard over it): declare a
reducer, which decides what each caller is sent. A bridge over **another**
service's table events is not checked at deploy; give it a reducer too.

## OpenAPI and GraphQL

Each restricted column carries what it asks in the spec, and a column some
callers are not answered is not `required` of a response:

```json
"ops_notes": {"type": "string", "nullable": true,
              "x-cufflink-access": {"read": "listings:internal", "write": "listings:internal"}}
```

`GET /graphql/schema` marks such a field `# read with listings:internal` and
types it nullable.

## Metrics and logs

- `cufflink_column_decisions_total{table, columns}` — `columns` is `all` or
  `withheld`; counted only for tables with a restricted column.
- `target: "cufflink::columns"` debug line per decision.

## Adopting it on a table in use

1. Upgrade the platform, then build the service with an SDK that knows
   `#[restricted]`.
2. Declare the operation in `authorization.areas` and grant it in
   `default_roles` to the roles that work on the rows. A deploy adds a
   default role's new permissions to the tenant's role of that name, so the
   grant and the restriction may ride one deploy; a role the manifest does
   not seed is granted it through `PUT /api/tenants/{id}/roles` first, or
   it loses the column the moment the restriction deploys.
3. Restrict the columns. Clients that send a restricted column in a write
   body are refused from then on; stop sending it first.

Rolling the platform back below this release serves every restricted column
to every caller of its table again: roll the services back first
([rolling a platform back](versions.md#rolling-a-platform-back)).
