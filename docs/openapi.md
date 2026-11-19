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

Each table has three schemas: the row (`Notes`), which requires every
column a caller is answered, nullable ones as `null`; the create body
(`NotesCreate`), which requires the non-null columns without a default,
except the owner column the platform fills; and the update body
(`NotesUpdate`), which requires nothing, since `PUT` sets only the columns
it is sent. A `JSONB` column is any JSON value.

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
- Both built-in clients take `ClientOptions` after the base URL: `fetch`,
  the function that performs the requests (one that adds credentials or
  retries, say), and `headers`, a record or a function called per request.
  A plain headers record is still accepted. A non-2xx response throws a
  `CufflinkError` with `status` and the parsed `body`; its message is the
  body's `error` or `error_message` when there is one.
- A parameter's schema examples move to the Parameter object, where
  Swagger UI shows them: one example becomes its `example`, two or more its
  `examples` map, keyed `"0"`, `"1"`, … with each `{"value": …}`. Clients are
  unchanged by it, and so are `deploy_hash` and `schema_hash`.

## Public documents

A service on `routing: strict` can publish named, unauthenticated API
documents, each a subset of its spec, under mounts it chooses:

```rust
openapi: {
    title: "catalog-service",
    docs: [
        {
            name: "public-api",
            mount: "/docs/public-api",
            include_tags: ["Catalog", "Orders"],
            title: "Catalog Public API",
            version: "1.0.0",
            description: catalog_docs::public_api_description(),
            contact: { name: "API Support", email: "support@example.com" },
            servers: [("https://api.example.com", "Production")],
            tags: [("Catalog", "Products and prices")],
            path_prefix: "/api",
            api_key: { header: "Authorization", description: "The key issued to you" },
            cors: mirror,
        },
    ],
},
```

`name`, `mount`, `include_tags`, `title` and `version` are required;
`description` takes any expression whose value implements `ToString`. A
manifest declaring documents requires the platform feature `public_docs`.

**What is selected.** A document holds the operations of the service's
server-relative spec tagged with any of `include_tags` (internal routes never),
the schemas they reach through `$ref`, and the security schemes they name.
Its `info`, `tags` and `servers` are its own. Its servers are the first
non-empty of its own `servers`, the `openapi` block's `servers`, and the
service's base. `path_prefix` is put before every path; it describes a
gateway's public layout, so it needs the document's own `servers`, which
address that gateway. `api_key` presents the `api_key` scheme in another
header, for a key a gateway checks; a document that selects a route whose
key the platform checks itself (the `api_key` posture, in `X-Api-Key`)
cannot declare it.

**What the deploy refuses**, naming the document: documents on
`routing: legacy`; a name outside `[a-z][a-z0-9-]{0,62}` or used twice; an
empty `include_tags`, or a tag that selects no published operation; a
mount that is not literal segments, starts with a reserved segment or a
table's name, overlaps another document's mount, or has a route under it
(the bare mount and the mount plus `/` included); an `api_key.header` that is
not a header name; a malformed `path_prefix`; an empty server URL, title or
version; and a `cors` mode the platform does not know.

**The answers**, under `/svc/{tenant}/{service}{mount}`, with no token:

| Request | Answer |
|---|---|
| `GET`/`HEAD …/openapi.json` | `200 application/json`, the document |
| `GET`/`HEAD …/swagger-ui/` and `…/swagger-ui/index.html` | `200`, Swagger UI's page |
| `GET`/`HEAD …/swagger-ui/<file>` | `200`, one of the files that page loads |
| `GET`/`HEAD …/swagger-ui` | `303`, `location: swagger-ui/` |
| any other method on those paths but `OPTIONS` | `405`, `allow: GET,HEAD` |
| anything else under the mount | the platform's `404` |

Every link is relative (the page loads `../openapi.json`), so the same
bytes work at the platform's own path and behind a gateway that publishes
the mount elsewhere. A service's [error profile](routing.md#error-profile)
renders the `404` and `405`.

**Caching.** Each `200` carries a strong `etag` (the SHA-256 of the bytes)
and `cache-control: no-cache`; an `If-None-Match` that matches answers
`304`. A document is built once per deployed version, outside the request
path. A document that cannot be built (only a stored manifest that never
passed the deploy's checks can fail) answers `404` and is counted in
`cufflink_public_docs_build_errors_total`; the service and its other
documents keep answering.

**CORS**, per document:
- `restricted` (the default): the platform's own policy
  (`CORS_ALLOWED_ORIGINS`), as for every route. A browser page on another
  origin cannot load the document.
- `any`: `access-control-allow-origin: *`, never credentials; a preflight
  allows `GET,HEAD` and any header.
- `mirror`: what tower-http's `CorsLayer::very_permissive()` sends: the
  request's `Origin` echoed, `access-control-allow-credentials: true`, a
  `vary` on the request's origin, method and headers, and a preflight's
  method and headers echoed. Documents set and need no cookie, so a
  credentialed read reveals no more than an anonymous one.

Only paths under that document's mount are affected; the service's routes
keep the platform's policy. A platform that meets a mode it does not know
(a rollback) serves the document as `restricted` and logs a warning.

**Swagger UI** is `swagger-ui-dist` 5.17.14 (Apache-2.0), embedded in the
platform binary: `index.html`, `swagger-ui.css`, `index.css`,
`swagger-ui-bundle.js`, `swagger-ui-standalone-preset.js` and the two
favicons, unmodified, plus the platform's own `swagger-initializer.js`.
Nothing is loaded from a CDN, and no other file of the package is served.
`_meta/docs` loads the same files from `_meta/docs/{file}`, which needs no
token. `platform/assets/swagger-ui/README.md` names the source and how to
move to another version.

**From the CLI.** `cufflink openapi --doc public-api --local` prints the
document built from the local manifest; without `--local` it fetches it from
the deployed service's mount, as its readers do. An unknown name lists the
declared ones. `generate-client` always uses the whole spec.
