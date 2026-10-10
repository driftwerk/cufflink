# Typed Routes, OpenAPI and Clients

## The spec

`GET /svc/{tenant}/{service}/_meta/openapi.json`, `cufflink openapi [--local]`
and `generate-client` serve one of two documents:

- An **undocumented** service gets the spec it always had: absolute
  `/svc/{tenant}/{service}/…` paths, custom routes as `/_fn/{handler}`.
- A service with an `openapi: { … }` block or any documented route gets an
  OpenAPI 3.1 document relative to its `servers` (by default
  `{origin}/svc/{tenant}/{service}` with both as variables): strict routes at
  their own path (others at `/_fn/{handler}`), CRUD at `/{table}` and
  `/{table}/{id}`, nullable columns as `type: [T, "null"]`, each operation's
  security from its posture (`bearer_auth`, or `api_key` for partner-key
  routes; none for public routes; internal routes left out).

Both documents describe the tables of `tables` alone. A
[private table](schema-ownership.md#private-tables) has no path, no schema
and no operation id, so a documented type may have the name its row schema
would have had (`Note` beside a private `notes`).

## Documenting routes

```toml
cufflink = { version = "…", features = ["typed-routes"] }
schemars = "1"
```

```rust
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
pub struct ApiUpdateShelfRequest { pub name: String, pub capacity: Option<i32> }

cufflink::service! {
    name: "catalog-service",
    tables: [BookRecord],
    mode: wasm,
    routing: strict,
    openapi: {
        version: "1.4.0",
        servers: [("https://api.example.com/api", "Production")],
        tags: [("Catalog Service", "Catalog Service API")],
        schemas: [PageParams],
    },
    custom_routes: [
        ("PUT", "/books/shelves/{shelf_id}", "update_shelf", perm("shelves", "edit"),
            path = ShelfPath, request = ApiUpdateShelfRequest, tags = ["shelves"],
            summary = "Update a shelf",
            responses = [(200, ApiShelfResponse, "Shelf updated"), (400, ApiErrorResponse),
                         (401), (403), (404, ApiErrorResponse), (500, ApiErrorResponse)]),
        ("GET", "/receipts/{receipt_id}/download", "download_receipt_pdf", perm("receipts", "view"),
            responses = [(200, bytes("application/pdf"), "PDF"), (404, ApiErrorResponse)]),
        ("POST", "/templates/import", "import_templates", perm("templates", "edit"),
            multipart, request = multipart_form),
    ],
}
```

| Keyword | |
|---------|---|
| `request = T`, `(T, "mime")`, `multipart_form` | a required body; `T` is JSON |
| `responses = [...]`, `response = (...)` | `(status)`, `(status, "description")`, `(status, T)`, `(status, T, "description")`, `(status, bytes("mime"))`, `(status, (T, "mime"))`; a response without a description gets the status's reason phrase |
| `path = T`, `query = T` | a struct whose fields become the path or query parameters (`Option` fields optional); `path` must name exactly the route's parameters |
| `operation_id`, `summary`, `description`, `tags = [...]`, `deprecated`, `security = [...]` | the operation id defaults to the handler name, the tags to the service name, the security to the posture's |

The `openapi` block takes `title`, `version`, `description`, `servers`,
`tags`, `security_scheme` (default `bearer_auth`) and `schemas`, types to
include as components though no route names them.

Schemas come from [schemars](https://docs.rs/schemars) (JSON Schema 2020-12
under `#/components/schemas`). Two different types under one schema name —
schemars names `Page<Book>` and `Page<Receipt>` both `Page` — stop
`--emit-manifest` with an error asking for `#[schemars(rename = "…")]`, as do
duplicate operation ids, a query or path field that is an object, and a type
named like a table's schema. The deploy validator checks the same.

## Generating a client

```bash
cufflink generate-client --local --output clients/typescript
cufflink generate-client --local --generator openapi-generator \
    --npm-name @acme/catalog-service-client --output clients/typescript
```

- `--generator builtin` (the default) writes `client.ts` and `index.ts`: the
  client it always wrote for an undocumented service, and for a documented
  one a typed client with one method per operation (typed path parameters,
  query object, body and result). `--class-name` names the class
  (`CufflinkClient` by default) and `--esm` sets `"type": "module"` in
  `package.json`.
- `--generator openapi-generator` runs openapi-generator's `typescript-fetch`
  through `npx` (Node and a Java runtime needed) with `supportsES6`,
  `nullSafeAdditionalProps`, camelCase properties and enums and
  `removeEnumValuePrefix`; `--npm-name` sets `npmName` and
  `--additional-properties K=V,…` adds or replaces properties. The generator
  version is pinned, 7.21.0 unless `--generator-version` says otherwise,
  through an `openapitools.json` written to the output directory.
- A path parameter named `data` or `query` is the `dataParam` or
  `queryParam` argument of a built-in client method, which keeps `data` and
  `query` for the body and query object.
