# Custom Routes: Routing, Requests and Responses

Everything on this page is opt-in. A service that declares none of it is
served exactly as before: every custom route by handler name under
`/svc/{tenant}/{service}/_fn/{handler}`, the guest request and response JSON
unchanged.

A route is `(method, path, handler, posture, keyword, …)`: the posture is
the fourth element and every keyword on this page (`job(...)`,
`forward_auth`, `body_limit`, `timeout`, `multipart`, `sse(...)` and the
OpenAPI keywords) follows it, each at most once, in any order.

## Strict routing

```rust
cufflink::service! {
    name: "catalog-service",
    tables: [BookRecord],
    mode: wasm,
    routing: strict,
    custom_routes: [
        ("GET", "/books/{book_id}", "get_book", perm("books", "view")),
        ("PUT", "/books/{book_id}/due-date", "set_due_date", perm("books", "edit")),
        ("GET", "/files/{*rest}", "get_file", auth),
    ],
}
```

With `routing: strict` each route is also served at its own path below the
service, `/svc/{tenant}/{service}/books/7/due-date`:

- A segment is a literal (`[A-Za-z0-9._~-]+`), a parameter `{name}` (or the
  older `:name`, `[a-z_][a-z0-9_]*`), or a trailing catch-all `{*rest}`.
  `/` alone is the service root. The macro rejects a malformed path at
  compile time, the deploy validator again.
- Routes are matched per method; `HEAD` is served by `GET` (the guest sees
  `GET`, the body is dropped), except on an event stream. A path with
  routes for other methods answers `405` with `Allow`, which on a table's
  CRUD path also lists the strict routes declared there; a path with no
  route answers `404 {"error": "Not found"}`. A trailing slash is
  significant.
- Parameters are percent-decoded (`{"error"}` 400 when not UTF-8) and reach
  the guest as `path_params`, `[[name, value], …]` in path order;
  `req.path_param("book_id")`, `req.path_params()`.
- `_fn/{handler}` keeps working for every route (with no path parameters),
  so existing clients need no change. `_meta`, `_jobs` and `graphql` stay the
  platform's, and a table's CRUD paths stay reachable: a route may not take
  a request CRUD serves for one of the service's tables (`/{table}`,
  `/{table}/{id}`, `/{table}/stream`, `/{table}/batch`), and two routes may
  not match the same requests.
- A [private table](schema-ownership.md#private-tables) has no CRUD paths,
  so a route may take them: a service with `notes` in `private_tables`
  declares `("GET", "/notes/{note_id}", …)`. A method no route takes there
  is the `405` with `Allow` of the routes alone.

## Route limits

```rust
("POST", "/templates/import", "import_templates", perm("templates", "edit"),
    multipart, body_limit = 10_485_760, timeout = 60),
```

- The body of every custom route is read only **after** the caller is
  authorized, within the route's `body_limit` (bytes, at most 64 MiB;
  4 MiB by default). A larger body answers `413`.
- `timeout` (1 to 120 seconds; 30 by default) is the route's execution
  budget, on a worker too. A `job(...)` route keeps its own timeout.
- `multipart` (POST, PUT, PATCH) parses a `multipart/form-data` body into
  `req.multipart()` parts (name, filename, content type, bytes; at most 100
  parts, each within the limit). A malformed form answers `400`.
- `audit_redact = ["card.number", "lines[].sku"]` names what of the body no
  audit record of a call holds, as a table's `audit_redact` names what of
  its rows none does ([policies](policies.md#durable-audit-and-webhook-deliveries)).
  The guest is given the body whole. Needs a platform advertising
  `audit_redaction`.

## The request

A strict route, or a route using any keyword on this page, additionally
receives:

| Key | SDK | |
|-----|-----|---|
| `path`, `path_params` | `path()`, `path_param(name)`, `path_params()` | strict routes only |
| `query_string`, `query` | `query_string()`, `query_all(name)`, `query_first(name)`, `query::<T>()` | every value of every key; `body.params` keeps its single value |
| `client_ip` | `client_ip()` | behind the platform's trusted proxies (`RATE_LIMIT_CLIENT_IP_SOURCE`) |
| `multipart` | `multipart()` | `multipart` routes |
| `caller` | `caller()` | [`internal` routes](service-calls.md#internal-routes) |
| `api_key` | `api_key()` | [`api_key` routes](partner-keys.md) |
| `scope` | `scope()` | [`scoped` routes](scoped-permissions.md#custom-routes) |

`req.header(name)` is case-insensitive.

## Responses (`cufflink-fn` feature `response-v2`)

Available on any platform: `Response::created`, `status_json`,
`error_message(status, msg)` (`{"error_message": …}`) and
`validation_errors(&map)` (422 `{"validation_errors": {field: [msgs]}}`).

With `response-v2`, which needs a platform advertising `response_v2` and a
CLI of the same release:

```rust
Response::no_content()                                   // 204, empty
Response::attachment("books.xlsx", XLSX, bytes)          // Content-Disposition: attachment; filename="books.xlsx"
Response::inline("receipt.pdf", "application/pdf", bytes)
Response::plain("pong")                                  // text/plain; charset=utf-8
Response::redirect(Redirect::SeeOther, "/books/7")  // 301, 302, 303, 307 or 308 via Redirect
Response::bytes("text/csv", csv).header("cache-control", "no-store")
Response::s3_object(S3Object { bucket, key, content_type: None, disposition: Some(Disposition::Attachment(name)) })
```

- `header(name, value)` adds a header; a later value replaces an earlier one.
  The platform drops (and logs, and counts in
  `cufflink_guest_header_dropped_total`) headers it owns: connection and
  framing headers, `content-length`, `set-cookie`, `access-control-*`,
  `strict-transport-security`, `x-request-id`.
- The status is 200–599 (`Response::redirect` takes one of 301, 302, 303,
  307 and 308 as a `Redirect`); 204 and 304 responses have no body. An
  invalid status or header is the guest's bug and answers
  `500 {"error": "WASM execution error: invalid response …"}`.
- A non-ASCII filename adds an RFC 5987 `filename*`.
- `s3_object` streams a stored object without it passing through the guest,
  with the guest's own storage permissions (a denied key is a 500), its
  `Content-Length`, and the object's content type unless one is given. A
  missing object answers 404.
- Hooks (`before_*`/`after_*`, `on_migrate`, subscriptions) read the
  response as they always have; a v2 envelope there is plain JSON.

## Error profile

```rust
errors: { key: "error_message" },
```

The platform's own error responses for the service (authorization, routing,
limits, guest failures, CRUD) carry their message under the key:
`{"error_message": "Not found"}`. The guest's own responses are never
rewritten, and services without a profile keep `{"error": …}`.

## Server-sent events

```rust
("GET", "/receipts/book/{book_id}/progress", "receipt_progress", perm("receipts", "view"),
    sse(subject = "dw.{tenant}.receipts.pdf.progress.{book_id}", reducer = "receipt_progress_reduce")),
```

- The subject starts `dw.{tenant}.` and then names its events with a
  literal token: not a wildcard or a path parameter, and not `logs`, the
  tenant's audit log, which is never bridged. Every other `{name}` is a path
  parameter of the (strict) route, and its value must be one subject token
  (no `.`, `*`, `>` or whitespace; 400 otherwise). `*` and `>` wildcards are
  allowed after the naming token.
- A subject that names another of the tenant's services
  (`dw.{tenant}.billing.…`, where CRUD events are published) is refused with
  403 unless one of the manifest's `subscriptions` covers it: a stream
  exposes only what its service already consumes.
- A stream without a reducer forwards whole rows. A deploy refuses one
  over the events of one of the service's own tables that withholds a
  column from some callers ([column permissions](column-permissions.md));
  a reducer decides what each caller is sent.
- On a `public` or undeclared posture the stream reaches anonymous callers,
  so the route opts in with `sse(..., anonymous = true)`; the macro and the
  deploy refuse it otherwise. An anonymous stream needs the caller's address
  (its cap is per address).
- A `HEAD` never opens a stream: it answers 405 with the methods the path
  serves.
- The platform subscribes, and takes a slot of the principal's stream cap
  (`SSE_MAX_STREAMS_PER_PRINCIPAL`, 429 beyond it), **before** the handler
  runs, so what is published while the handler builds its snapshot is not
  lost (up to 1000 messages).
- The handler returns `Response::sse(SseSpec { initial, state, keep_alive_secs,
  ping, timeout_secs, headers })`. The stream sends `initial`, then each
  message: through the reducer when declared, otherwise as a data-only event
  with the payload.
- A reducer is an exported handler run per message in order (5 s each):

  ```rust
  handler!(receipt_progress_reduce, |req: Request| sse::reduce(req, |state, message| Reduction {
      events: vec![SseEvent::data(message.data)],
      state,
      close: false,
  }));
  ```

  It must be idempotent for messages the snapshot already reflects. Its
  state is capped at 64 KiB; a failing reduction drops the message.
- The stream ends on disconnect, `timeout_secs` (default 300, at most 3600),
  a reduction with `close`, or shutdown, with no closing event. Any other
  response from the handler is returned as usual and frees the slot.
- A reducer runs where the route's handler runs (on a worker when it is
  worker-routed) and sees the route's `auth`, `posture` and `caller`.
- Metrics: `cufflink_sse_bridge_streams_active`,
  `cufflink_sse_bridge_events_total{result}`,
  `cufflink_sse_bridge_closed_total{reason}`,
  `cufflink_sse_rejected_total{reason}`.

## Postures

Besides `public`, `auth`, `perm` and `owner`:

- `internal("service", …)` or `internal("*")`: only the tenant's own
  services, through [service calls](service-calls.md#internal-routes).
- `api_key("resource:action")`: only [partner keys](partner-keys.md).
- `scoped("area", "op")` or `scoped("area", "op", owner = "column")`: an
  authenticated caller with the rows their grants reach
  ([scoped permissions](scoped-permissions.md#custom-routes)).

Each is derived into `required_platform_features`
([versions](versions.md)).
