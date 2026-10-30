# Observability

Every binary logs JSON on stdout and serves Prometheus metrics; with `OTEL_EXPORTER_OTLP_ENDPOINT` set it also exports traces and logs over OTLP. What it exports follows one contract, so that a tenant's service is a service of its own in Loki, Tempo, Prometheus and Grafana, whichever binary ran it. This page is that contract, the settings that turn it on, the monitoring configuration that implements it (`infra/monitoring/`), the probes, and the metrics reference.

## The contract

| What | As exported (OTLP) | After the collector | Loki | Tempo (TraceQL) | Span metrics (Prometheus) | Cufflink's own metrics |
|------|--------------------|---------------------|------|-----------------|---------------------------|------------------------|
| Tenant | `cufflink.tenant` | `service.namespace` | label `service_namespace` | `resource.service.namespace` | `service_namespace` | `tenant` |
| Service | `cufflink.service` | `service.name` | label `service_name` | `resource.service.name` | `service` | `service` |
| Deployed version | `cufflink.version` (`v<n>`) | `service.version` | metadata `service_version` | `resource.service.version` | `service_version` | `cufflink_service_info{version, commit}` |
| Handler | `cufflink.handler`, and the [span name](#span-and-route-names) | unchanged | metadata `cufflink_handler` | `span.cufflink.handler`, `name` | `span_name` | `handler`, `route` |
| Kind of execution | `cufflink.kind` | unchanged | metadata `cufflink_kind` | `span.cufflink.kind` | `cufflink_kind` | `kind` |
| Binary | resource `cufflink.component` | unchanged | label `cufflink_component` | `resource.cufflink.component` | — | the scrape's `job` |
| Environment | resource `deployment.environment.name`, from `OTEL_RESOURCE_ATTRIBUTES` | unchanged | label `deployment_environment_name` | resource | — | what the scrape configuration adds |
| Trace | trace id and span id on spans and on log records | unchanged | metadata `trace_id`, `span_id` | the trace | exemplar `traceID` | — |

"After the collector", Loki, Tempo and the span metrics are what the [canonical monitoring assets](#the-canonical-monitoring-assets) make of the export. The first two columns and the last hold for any install.

What a binary does outside any tenant's service (startup, deploys, the management API, a request that resolves no service) stays under the binary's own `service.name`, `cufflink-<component>`, with no `service.namespace`. That is what tells the platform's telemetry from a tenant's.

## Whose telemetry it is

### The process

Every span and log record a binary exports carries its process's resource:

| Resource attribute | Value |
|--------------------|-------|
| `service.name` | `cufflink-platform`, `cufflink-worker`, `cufflink-web-runner` or `cufflink-ext-runner` |
| `cufflink.component` | `platform`, `worker`, `web-runner` or `ext-runner` |
| `cufflink.tenant` | the tenant of a runner that serves one (`WEB_TENANT`, `EXT_TENANT`); absent otherwise |

`OTEL_RESOURCE_ATTRIBUTES` adds to these (`deployment.environment.name=prod`, for one). It cannot replace the three above.

### The scope

A span can declare a **scope**: the tenant it works for and, within the tenant, the service, the deployed version, the handler and the kind of execution. The scope is five span attributes:

| Attribute | Value |
|-----------|-------|
| `cufflink.tenant` | the tenant's slug |
| `cufflink.service` | the service's name |
| `cufflink.version` | the deployment's version, as `v<n>` |
| `cufflink.handler` | the handler's name |
| `cufflink.kind` | `route`, `crud`, `hook`, `subscription`, `schedule`, `job`, `worker_route`, `sse_reducer`, `on_migrate` or `child` |

A scope may name less than all five: one that names a tenant alone carries `cufflink.tenant` only.

- **Spans inherit.** Every span opened under a span with a scope is exported with that scope, field by field: a field the span declares itself wins, the rest come from its parent. A span reads its parent's scope once, when it is opened, so a scope recorded on a span later reaches only the spans opened after that.
- **Log records carry it.** A log record emitted under any of those spans is exported with the same five attributes. An attribute of that name the event sets itself is kept.
- **Outside a scope nothing is added.** Startup, deploys and the management API export as before, under the binary's `service.name`.

### Where a scope opens

| What runs | Span | Scope | `cufflink.kind` |
|-----------|------|-------|-----------------|
| A request to a custom route (`_fn/{handler}` or its declared path) | `request` | the service, its version, the route's handler | `route` |
| A request to a table, GraphQL or a table's event stream | `request` | the service and its version | `crud` |
| A request to `_meta/*` | `request` | the service and its version | none |
| A poll or retry of a job (`/svc/{tenant}/_jobs/{id}`) | `request` | the tenant alone | none |
| A guest handler, wherever it runs | `wasm_handler` | the service, the version of the module, the handler | `route`, `hook`, `subscription`, `schedule`, `sse_reducer`, `on_migrate`, `job` or `worker_route` |
| A subscription message, a schedule fire | `subscription`, `schedule` | the service, its version, the handler | `subscription`, `schedule` |
| A job a worker runs for a route, an after-hook, a long-running job | `worker_job` | the service, the job's version, the handler | `worker_route`, `hook`, `job` |
| A backup or restore job | `backup_job` | the service | `job` |
| A pass of a tenant's outbox relay that has rows to relay | `outbox_relay` | the tenant alone | none |
| What a runner's child prints (a web app, a native service) | `child_output` | the service and its version | `child` |

The Span column is the span's name on stdout (`span.name`). Tempo and the span metrics show its exported name: a request's [route name](#span-and-route-names); `{tenant}/{service}/{handler}` for a subscription message and a schedule fire; and `worker/sync`, `worker/async` or `worker/long` for a worker's job, whose handler is in `cufflink.handler`. The other spans are exported under the name in the table.

Everything a handler does (its queries, fetches, publishes, service and extension calls) and every line it logs, the guest's `[wasm]` lines included, is under one of these spans and so carries its scope. A service call another service's guest makes is its callee's from the callee's `request` span on, in the caller's trace: the call carries the caller's `traceparent`, over HTTP and in process alike, and a `traceparent` or `tracestate` a guest sets on a call itself is dropped.

**A scope is recorded only once it is resolved.** A request takes its scope where the registry answers for its tenant and service, and a worker's job where what the job claims of itself matches the deployment the platform recorded. A request that resolves no service and a job that is refused carry no scope: they stay the platform's and the worker's own.

### Span and route names

A resolved request is named `{tenant}/{service}/{leaf}`: the handler under `_fn`, the first segment of a declared route's path (`:id` when it is a parameter), the table (or `graphql`), `_meta/{leaf}`; a job poll is `{tenant}/_jobs`. The same name is the `route` label of `cufflink_http_request_duration_seconds`.

**Nothing under `/svc` is named from its path.** Until a handler resolves it, such a request is `METHOD unmatched`, and it stays so when:

- its tenant or service is not registered, or its `_fn` handler is not a declared route;
- it is answered before a handler runs: the rate limiter's 429, load shedding's 503, a body over the limit (413), a CORS preflight. The status is still on the series.

A request for a table the service does not serve is `{tenant}/{service}/:id`. So neither span names nor the `route` label grow with what a scanner sends: their values are bounded by the services deployed and what those declare.

### Traces on log records

A log record exported over OTLP carries the trace id and the span id of the span it was emitted in, so a log line links to its trace. Under a parent propagated from another process (a `traceparent` header, a NATS message's headers) that is the caller's trace id. A record emitted outside any span carries neither.

### One trace per message

A subscription message, a schedule fire and a worker job each run under a span that continues the publisher's trace when the message carries one, and starts a trace of its own when it does not. Messages a pump handles are therefore separate traces, not one trace that lasts as long as the pump. A backup job and a pass of the outbox relay are traces of their own too, and a relay that finds nothing due exports no span.

### stdout

The JSON lines keep their shape: `timestamp`, `level`, `fields`, `target`, `span` and `spans`, and every event field. Two things differ from v0.29:

- the `span` and `spans` entries of a span that declares a scope include its `cufflink.*` fields;
- the lines of a subscription message, a schedule fire or a worker job no longer list the pump's `{"name":"task",…}` entry first in `spans`: the message's span is the first entry.

stdout carries everything, whatever is exported. The OTLP log export leaves out the diagnostics of the export path itself (events of `opentelemetry*`, `h2`, `hyper`, `hyper_util`, `reqwest`, `rustls` and `tonic`), which would otherwise be exported by the code they describe.

### What the collector makes of it

A binary exports a tenant's spans and lines under its own resource, with the scope as attributes. The canonical collector (`infra/monitoring/otel-collector-config.yaml`) regroups them: a span or log record that carries `cufflink.service` leaves the binary's resource for one of its own, on which

- `service.namespace` is the tenant (`cufflink.tenant`),
- `service.name` is the service (`cufflink.service`),
- `service.version` is the version (`cufflink.version`), when there is one,

and the three `cufflink.*` attributes they came from are removed. `cufflink.handler`, `cufflink.kind` and the binary's `cufflink.component` stay as they are. A record scoped to a tenant alone (the outbox relay, a job poll) is not regrouped as a service: it stays under the binary's `service.name`, with `cufflink.tenant` beside it as a resource attribute (`resource.cufflink.tenant` in TraceQL, the metadata `cufflink_tenant` in Loki), so no service appears in a tenant's list that the tenant did not deploy.

A collector without this mapping forwards the scope as plain attributes, and every line and span stays under `cufflink-<component>`, as before v0.30.0.

### Labels

What each store keeps of a record that went through the canonical collector:

| Store | Indexed, or a label | Beside the record |
|-------|---------------------|-------------------|
| Loki | the stream labels `service_name`, `service_namespace`, `cufflink_component`, `deployment_environment_name`, and no others | structured metadata: `detected_level`, `service_version`, `cufflink_handler`, `cufflink_kind`, `trace_id`, `span_id` and every other attribute of the record. The line is the message alone |
| Tempo | `resource.service.namespace`, `resource.service.name`, `resource.service.version`, `resource.cufflink.component` | `span.cufflink.handler`, `span.cufflink.kind` and the span's own attributes |
| Prometheus, [span metrics](#span-metrics) | `service_namespace`, `service`, `service_version`, `span_name`, `span_kind`, `status_code`, `cufflink_kind`, and `source="tempo"` from Tempo's generator | an exemplar `traceID` on the latency buckets |
| Prometheus, [Cufflink's metrics](#metrics) | each metric's own labels (`tenant`, `service`, `handler`, `kind`, …), the scrape's `job` (`cufflink-platform`, `cufflink-worker`) and `instance`. The external label `cluster` is added only to what this Prometheus sends on (remote write, federation, alerts), not to what is queried in it | — |

A Loki stream is therefore one service of one tenant as one kind of binary ran it in one environment, not each process that ran it: the pod and the instance are not labels.

## Settings

| Setting | Read by | Values | Default |
|---------|---------|--------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | platform, worker, web-runner, ext-runner | the collector's OTLP/HTTP address, e.g. `http://otel-collector:4318` | unset: nothing is exported |
| `OTEL_RESOURCE_ATTRIBUTES` | the same four | `key=value,…`; name the environment with `deployment.environment.name=<env>` | unset |
| `RUST_LOG` | the same four | a `tracing` filter | `info,cufflink_<component>=debug` (`info,cufflink_platform=debug`, …) |
| `LOKI_URL` | platform | the Loki that [`cufflink logs`](#logs-cufflink-logs) reads | unset: the command is refused |
| `LOKI_LABEL_MODEL` | platform | `legacy`, `scoped` ([below](#loki_label_model)) | `legacy` |
| `CUFFLINK_CHILD_TELEMETRY` | web-runner, ext-runner | `legacy`, `scoped` ([children's telemetry](deployment.md#childrens-telemetry)) | `legacy` |
| `CUFFLINK_CHILD_OUTPUT_LEVEL` | web-runner, ext-runner | `debug`, `info` ([children's telemetry](deployment.md#childrens-telemetry)) | `debug` |

Without `OTEL_EXPORTER_OTLP_ENDPOINT` a binary logs JSON on stdout and exports nothing. With it, traces and logs go out over OTLP/HTTP beside stdout. Worker spans for request/reply jobs and after-hooks are parented on the dispatching request's trace, so one trace follows a request from the platform onto the worker; long jobs start their own trace. Metrics are **not** pushed over OTLP by any component: Prometheus scrapes them ([metrics](#metrics)).

The last three settings each default to what every release before v0.30.0 did. Their values are read without regard to case or surrounding spaces, and a value that is neither of the two stops the binary at start, naming the variable and what it accepts:

```
LOKI_LABEL_MODEL has an invalid value "otlp": 'otlp' is not one of legacy, scoped
```

- **`LOKI_LABEL_MODEL`** says how the Loki behind `LOKI_URL` was fed, and so how the platform asks it for a service's lines. It changes nothing a binary exports.
- **`CUFFLINK_CHILD_TELEMETRY=scoped`** makes a runner's child that has an OpenTelemetry SDK report as its service: the runner sets the child's `OTEL_SERVICE_NAME` to the service and puts `service.namespace`, `service.version` and `cufflink.component` into its `OTEL_RESOURCE_ATTRIBUTES`. Under `legacy` the child receives the runner's own values.
- **`CUFFLINK_CHILD_OUTPUT_LEVEL=info`** logs what children print at `info`. At `debug` the default filter leaves those lines out, of stdout and of the export alike. The lines are logged inside a `child_output` span of the child whatever the two settings say, so with `info` a native service's output lands under its service without the service doing anything.

With the canonical assets, set `LOKI_LABEL_MODEL=scoped` on the platform and `CUFFLINK_CHILD_TELEMETRY=scoped` and `CUFFLINK_CHILD_OUTPUT_LEVEL=info` on the runners, as `docker-compose.yml` does, after reading [who can write a tenant's lines](#who-can-write-a-tenants-lines).

## Logs: `cufflink logs`

`cufflink logs [--follow]` is served by the platform's `/api/services/{id}/logs` endpoint (`/logs/tail` when following), which queries Loki's `query_range` API. It requires `LOKI_URL` to be set on the **platform** (the compose stack sets `LOKI_URL: http://loki:3100`); without it the command fails with `LOKI_URL not configured`. Logs only reach Loki when the binaries export them over OTLP, so both `OTEL_EXPORTER_OTLP_ENDPOINT` and `LOKI_URL` are needed for the command to return anything. The caller needs deploy authority in the service's tenant; the tenant in the query is the caller's own, never an argument.

`--since <duration>`, `--from` and `--to` (RFC 3339) pick the range (the last hour when none is given), `--search <text>` and `--level <level>` filter, `-n`/`--limit` caps the lines (100, at most 5000) and `--output <file>` saves them. `--follow` alone streams new lines; with a range it is an ordinary query.

### `LOKI_LABEL_MODEL`

How the platform finds a service's lines depends on how the Loki behind `LOKI_URL` was fed. `LOKI_LABEL_MODEL` (platform) says which; any other value stops the platform at start.

| Value | The query | Needs |
|-------|-----------|-------|
| `legacy` (default) | `{service_name="cufflink-platform"}`, filtered on the JSON **body** of each line for the service, the tenant, the severity and the search text: the query of every earlier release, byte for byte | A collector that still writes JSON bodies with the deprecated `loki` exporter (`loki.format: json`). Only the platform's own lines are found: a handler a worker ran is not |
| `scoped` | `{service_namespace="<tenant>", service_name="<service>"}`, then `\| detected_level="<level>"` and `\|= "<search>"` | A collector that maps the [scope](#the-scope) to `service.namespace` and `service.name`, and a Loki that takes OTLP with `service.namespace` as an index label. Lines are found whichever binary logged them, a worker's included |

The two settings are a pair: `scoped` against a Loki that was fed the legacy way finds nothing, and so does `legacy` against one fed over OTLP. The canonical collector no longer writes the legacy shape, so a platform beside it sets `scoped`.

Under `scoped` a line is the message alone, so each entry of `GET /api/services/{id}/logs` and of `/logs/tail` gains two optional members beside `timestamp` and `line`, and the CLI shows them:

- `level`: Loki's `detected_level`;
- `fields`: those of `cufflink_handler`, `cufflink_kind` and `trace_id` that Loki holds as the line's structured metadata; left out when it holds none of them.

Under `legacy` an entry is `timestamp` and `line`, as before. Under `scoped` the answer of `GET /api/services/{id}/logs` also carries `"label_model": "scoped"` beside `logs`; a `legacy` answer is byte for byte what it was.

Under both models the entries of an answer are oldest first, whichever requests they belong to, and a followed stream sends each line once. The query is sent with `direction=forward` and the limit, so `-n 100` over a range that holds more lines returns the oldest 100 of the range, not the latest: narrow the range (`--since`, `--from`) to read the latest.

The CLI prints an entry as the time, the level in brackets and the message, then the fields as `name=value`. Under `legacy` it reads the level, the target and the fields `tenant`, `service`, `table` and `operation` from the JSON line itself; a line that is not JSON is printed as it is.

`cufflink logs --output <file>` keeps what the terminal line was made from, in one form for the whole file. Under `legacy` that is each line as it is, a JSON record that says everything of itself; under `scoped` it is each entry as the API gave it, one JSON object a line with `timestamp`, `line` and, where Loki holds them, `level` and `fields`. The CLI reads the form from the answer's `label_model`, not from the entries, so an entry Loki holds nothing of beside its line is saved as JSON like the others. A followed stream is not saved.

Under `scoped` the answer is every line logged inside the service's scope, whichever binary logged it: the guest's own lines, and also what the platform or a worker logged while running the service (a refused statement, a denied egress, a failed publish). Under `legacy` it was the lines that named the tenant and service as fields. A tenant's deployers therefore read more of the platform's side of their service than before.

### Who can write a tenant's lines

`cufflink logs` returns what Loki holds, and Loki holds what the collector was sent. Neither model checks who sent a line:

- under `scoped`, the command returns whatever Loki holds under the tenant's `service_namespace` and the service's `service_name`;
- under `legacy`, it returns every line of the stream `service_name="cufflink-platform"` whose text holds `"service":"<service>"` and `"tenant":"<tenant>"`. What a guest logs cannot forge that through the platform's own records, where its text is a JSON string and its quotes are escaped. A record sent to the collector as `cufflink-platform` with those two attributes is returned like the platform's own.

**So anything that can reach the collector can write lines that appear in another tenant's `cufflink logs`, and on its dashboards, under either model.** A collector only receives, so nothing is read through it; a Loki, Tempo or Prometheus that can be reached is read, and written to, directly: none of the three asks who is calling (Loki's `auth_enabled: false`, Tempo's own OTLP receivers, Prometheus's `--web.enable-remote-write-receiver`), and a line pushed to Loki goes past every collector, the pinning one included. Two kinds of tenant code can reach what the binaries' networks reach:

1. **A runner's children.** A web app or a native service is the tenant's own process. It reaches whatever its runner's network reaches, whatever its environment says, and it is given the runner's `OTEL_EXPORTER_OTLP_ENDPOINT` under both values of `CUFFLINK_CHILD_TELEMETRY`. The shipped `docker-compose.yml` puts the one collector on the runner network: it is a single developer's stack.
2. **Guests, through `http::*`.** The tenant policy `egress` defaults to `warn`, which records a request to a private or internal address and lets it through ([policies](policies.md)). `enforce` refuses only a target that resolves to a private or otherwise internal address (loopback, RFC 1918, link-local, CGNAT, ULA and the like): until a tenant's `egress` is `enforce`, with no `allow_hosts` entry that names them, its guests can post to the collector, and read from and write to Loki, Tempo and Prometheus, on such addresses wherever the platform or the worker that runs them can; and one that answers on a public address (a managed OTLP gateway, a load balancer, a VPC range outside RFC 1918) they reach under either mode, with no violation recorded.

Therefore, wherever more than one tenant's code runs:

- **Before setting `LOKI_LABEL_MODEL=scoped` with native services or web apps on a shared collector, give each tenant's runners a collector of their own that pins the tenant on everything it forwards, and keep the shared collector off the runner network. This is required, not advice**: its configuration is `infra/monitoring/otel-collector-tenant.yaml`, described under [a runner's collector](deployment.md#a-runners-collector).
- That collector sets its tenant as `cufflink.tenant` and `service.namespace` on every resource, removes both names everywhere below it, and forwards nothing as `cufflink-platform` or `cufflink-worker`. What a runner's child sends is therefore stored under the runner's tenant whatever it claims, the runner's own lines with it, and is in no line the `legacy` query reads either.
- Set `egress` to `enforce` for every tenant ([tenant isolation](tenant-isolation.md#egress-memory-binding-and-redis)), and keep the collector, Loki, Tempo and Prometheus out of every `allow_hosts`. A runner's collector does nothing about guests, which run inside the platform and the worker.
- Keep the collector and the three stores on private addresses, or behind authentication: the `egress` policy does nothing for a host with a public address. Keep the collector's ports off any network a client can reach. The compose stack publishes 4317 and 4318 on the host.

With that in place, and the collector and the stores on internal addresses only, nothing of another tenant's is in a tenant's `cufflink logs`.

## `cufflink_service_info`

`cufflink_service_info{tenant, service, version, commit, mode}` is 1 for each service a platform replica has registered, which is each active service. It joins a tenant's span metrics to what is deployed:

- `version` is `v<n>`, the value of `service_version` on the service's span metrics and logs; `commit` is the commit the deploy named, and empty when it named none;
- it is written from the registry at each scrape, so a service that is disabled or deleted, and the version a redeploy replaced, are gone from the next scrape. A rollback, and a deploy that is skipped because nothing changed but names a newer commit, change the line too;
- each replica prints its own registry: query `max by (tenant, service, version, commit) (cufflink_service_info)`. A version or commit that was not there five minutes before marks a deploy, which is how the service dashboard draws its annotations;
- the platform alone serves it. A worker has no registry.

## The canonical monitoring assets

`infra/monitoring/` holds the configuration the compose stack runs and a deployment vendors as it is: each file works with the others, and the CI job `observability` proves them together on the two collectors, Loki, Tempo and Prometheus that `docker-compose.yml` names, with a platform and a worker built from the same commit (`platform/tests/observability.rs`, `.github/scripts/observability-stack.sh`).

| File | What it sets |
|------|--------------|
| `otel-collector-config.yaml` | A span or log record that carries a service's [scope](#the-scope) leaves the binary's resource for one named as the tenant's service: `service.namespace` is the tenant, `service.name` the service, `service.version` the version ([above](#what-the-collector-makes-of-it)). A record scoped to a tenant alone stays the binary's, with `cufflink.tenant` beside it. Traces go to Tempo and logs to Loki, both over OTLP. There is no metrics pipeline |
| `otel-collector-tenant.yaml` | [A runner's collector](deployment.md#a-runners-collector), for where more than one tenant's code runs: everything it receives is forwarded to the collector above as the telemetry of the one tenant its `CUFFLINK_TENANT` names. The compose stack declares it under the profile `tenant-collector` and does not start it |
| `loki-config.yaml` | OTLP ingestion with the stream labels `service_name`, `service_namespace`, `cufflink_component` and `deployment_environment_name`; everything else a line carries (`detected_level`, `service_version`, `cufflink_handler`, `cufflink_kind`, `trace_id`, `span_id`) is structured metadata. Volume, level detection and patterns are on, which Logs Drilldown needs; lines are kept 30 days |
| `tempo.yaml` | Span metrics and service graphs written to Prometheus with exemplars, and local blocks for TraceQL metrics, which Traces Drilldown needs; traces are kept 7 days |
| `prometheus.yml` | Scrapes of the platform (`platform:8080`) and the worker (`wasm-worker:8081`), under the external label `cluster: cufflink`: set the labels that name your own environment. Prometheus is started with `--web.enable-remote-write-receiver` and `--enable-feature=exemplar-storage`, for what Tempo writes |
| `grafana-datasources.yaml` | The datasources under the fixed uids `prometheus`, `loki` and `tempo`, linked to each other: a log line to its trace, a trace to its lines and its metrics, an exemplar to its trace |
| `grafana-dashboards.yaml`, `dashboards/` | The dashboards below, one Grafana folder for each directory, not editable in Grafana |

Set `LOKI_LABEL_MODEL=scoped` on a platform whose Loki is fed this way ([above](#loki_label_model)), and name the environment with `OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=<env>` on every binary.

The versions the files are proven on are those `docker-compose.yml` names: `otel/opentelemetry-collector-contrib` 0.128.0, `grafana/loki` 3.3.2, `grafana/tempo` 2.7.0, `prom/prometheus` v2.48.0 and `grafana/grafana` 12.1.0.

### The compose stack

| Component | Purpose | Host port (compose) |
|-----------|---------|---------------------|
| OpenTelemetry Collector | Receives traces and logs via OTLP | 4317 (gRPC), 4318 (HTTP) |
| Grafana Tempo | Distributed tracing backend | not published (internal `tempo:3200`) |
| Grafana Loki | Log aggregation | not published (internal `loki:3100`) |
| Prometheus | Metrics collection | 9090 |
| Grafana | Visualization dashboards | 3001 (login `admin` / `admin`) |

Note that host port **3100 is the web-runner**, not Loki. Loki and Tempo are reachable only from inside the compose network, which is where the platform (`LOKI_URL`) and Grafana talk to them. The stack sets `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_RESOURCE_ATTRIBUTES: deployment.environment.name=dev` on the platform, the worker and both runners, `LOKI_LABEL_MODEL: scoped` on the platform, and `CUFFLINK_CHILD_TELEMETRY: scoped` and `CUFFLINK_CHILD_OUTPUT_LEVEL: info` on the runners.

### Vendoring

A deployment does not edit these files into its own: it copies `infra/monitoring/` as it is at the tag of the Cufflink release it runs, into a directory of its own repository that holds nothing else, and keeps its own configuration beside that directory.

- **Copy at the release tag, and check it in CI.** A script that fetches the directory at the tag (a `sync-cufflink-monitoring` script, say) should also have a check mode that fails when the vendored bytes differ from the tag's, so that a change is made in Cufflink and arrives with a release, never by an edit in place.
- **What is yours stays beside it.** The collector, Loki, Tempo and Prometheus files name the compose stack's hosts (`tempo:4317`, `loki:3100`, `prometheus:9090`, `platform:8080`, `wasm-worker:8081`, and `otel-collector:4318` in a runner's collector), local storage and one external label. Give your stores those names on the monitoring network, or generate your files from the vendored ones; put your scrape targets, external labels, storage and alert rules in files of your own.
- **Dashboards are loaded from the vendored directory** by a file provider like `grafana-dashboards.yaml` (`foldersFromFilesStructure`, so `Tenants/` and `Platform/` are the Grafana folders; `allowUiUpdates: false`). Your own dashboards go in another directory under another provider. They can rely on the datasource uids `prometheus`, `loki` and `tempo` and on the label names of [the contract](#the-contract).
- **The settings go with the files.** A platform whose Loki is fed by the vendored collector sets `LOKI_LABEL_MODEL=scoped`. Bring in the collector and Loki first, then Grafana, then the platform's setting: lines Loki took before the change keep their old labels for as long as it keeps them, so the dashboards and `cufflink logs` show what was logged since.

### Span metrics

Tempo derives these from the spans it receives, so a tenant's service has request metrics without counting anything itself:

| Metric | Labels |
|--------|--------|
| `traces_spanmetrics_calls_total`, `traces_spanmetrics_latency` (histogram) | `service_namespace` (the tenant), `service`, `service_version`, `span_name`, `span_kind`, `status_code`, `cufflink_kind` |
| `traces_service_graph_request_total` and its latency histograms | `client`, `server`, `service_namespace` |

The spans a service is entered by are those of kind `SPAN_KIND_SERVER` (a request) and `SPAN_KIND_CONSUMER` (a message, a schedule's fire, a worker's job); their `span_name` is the [route name](#span-and-route-names) for a request, `{tenant}/{service}/{handler}` for a message and a schedule's fire, and `worker/sync`, `worker/async` or `worker/long` for a worker's job (its handler is `cufflink_handler`, not a `span_name`). The latency histogram's buckets (`traces_spanmetrics_latency_bucket`) carry exemplars, each the `traceID` of a request; the Prometheus datasource links that label to Tempo. Spans that carry no service scope are counted under their binary (`service="cufflink-platform"`, no `service_namespace`).

### Dashboards

| Dashboard | uid | Folder | Shows |
|-----------|-----|--------|-------|
| Tenant services | `cufflink-tenant` | Tenants | One row for each service of the chosen tenant: version and commit, requests a second, error ratio, p95, error logs, dead-lettered subscription messages and consumer lag, with links to its logs, its traces and its own dashboard; below it a row for each service with its requests, logs and traces |
| Service | `cufflink-service` | Tenants | One service: requests, errors and latency by handler, executions by kind, what Cufflink reports of its subscriptions, consumers and schedules, the tenant's whole dead-letter queue and outbox, deploys as annotations, logs and traces |
| Cufflink platform | `cufflink-platform` | Platform | The binaries themselves: readiness, HTTP, WASM slots, database pools, registry, deploys, policy violations, messaging as the binaries count it, the worker, the runners, shutdown, and the logs outside any service |

These replace the two dashboards of earlier releases (`cufflink-overview`, `cufflink-tenant-services`), which are gone.

Every query of a Tenants dashboard names the tenant (`service_namespace="$tenant"`, or `tenant="$tenant"` on Cufflink's own metrics), so two tenants with a service of the same name never share a panel; a test of the files refuses a query that does not (`testkit/tests/monitoring_assets.rs`). Metrics labelled with a service and no tenant (`cufflink_custom_route_duration_seconds`, `cufflink_router_wasm_binary_missing_total`) are on the platform dashboard alone.

The tenant and service lists are the tenants and services that logged within the dashboard's time range: widen the range when one is not listed. Preview tenants (`preview-…`) are listed once "Preview tenants" is set to `include`.

Tempo's service graph draws a node for each service name and labels its series with the tenant (`service_namespace`), so two tenants' services of the same name are one node there until the graph is filtered by `service_namespace`: it is a view across tenants, for whoever operates the platform.

## Probes

| Endpoint | Binary | Meaning |
|----------|--------|---------|
| `GET /health` | platform (`:8080`), worker (`:8081`), web-runner (`:3100`), ext-runner (`:3200`) | Liveness: 200 while the listener is open |
| `GET /ready` | platform | 200 `state: ready` once migrations and the registry have loaded and the database answers; 503 `starting`/`migrated` before that, and `shutting_down` from the shutdown signal until exit |
| `GET /ready` | worker | 200 `state: ready` once the tenant catalog has loaded (which needs the platform's migrations; until then the worker takes no jobs), its three JetStream consumers are up and the platform database answers within 2 s; 503 `starting` or `shutting_down` otherwise. The body also reports `catalog` (`loading`/`loaded`), `db`, `nats`, `redis`, `consumers {up, expected}` and `version` |

Point readiness probes at `/ready` and liveness probes at `/health`; the web-runner's and the ext-runner's readiness is `/_cufflink/ready`. At a shutdown the worker keeps its probe listener open until the drain is over, so `/ready` reports `shutting_down` and `/health` stays 200 throughout. The platform answers `/ready` with 503 `shutting_down` for `SHUTDOWN_UNREADY_DELAY_SECS` (default 0, for installs without a preStop hook) and then stops accepting connections while in-flight requests finish.

### Shutdown

On SIGTERM or SIGINT each binary runs a fixed sequence of phases within `SHUTDOWN_GRACE_SECS` (45 s): the platform `http` → `tasks` → `nats` → `telemetry`; the worker `tasks` → `http` → `nats` → `telemetry`; the web-runner `http` → `children` → `tasks` → `nats` → `telemetry`; the ext-runner `http` → `reconcile` → `calls` → `children` → `tasks` → `nats` → `telemetry`. The `nats` and `telemetry` flushes are capped at 5 s each. Every phase's duration is recorded in `cufflink_shutdown_phase_seconds{phase}` and logged; the process exits 1 when the HTTP drain or the tracked tasks (for the ext-runner, also its calls or children) overran the grace.

## Per-service introspection

Every deployed service automatically exposes three unauthenticated endpoints under a reserved `_meta` namespace — no service-author code required.

| URL | Purpose |
|-----|---------|
| `GET /svc/{tenant}/{service}/_meta/health` | JSON: `name`, `version`, `commit_hash`, `status`, `tenant`, `table_count`, `deployed_at`, `now`. Use for monitoring probes and uptime checks. |
| `GET /svc/{tenant}/{service}/_meta/openapi.json` | Auto-generated OpenAPI 3.0 spec for the service's CRUD + custom routes. |
| `GET /svc/{tenant}/{service}/_meta/docs` | Swagger UI HTML — opens the sibling `openapi.json` in an interactive explorer. |

Example:

```bash
curl -s https://api.example.com/svc/acme/audit-service/_meta/health | jq .
# {
#   "name": "audit-service",
#   "tenant": "acme",
#   "version": 2,
#   "commit_hash": "cfe69d4",
#   "status": "active",
#   "table_count": 1,
#   "deployed_at": "2026-04-26T08:02:17.249447+00:00",
#   "now": "2026-04-26T09:15:33.012001+00:00"
# }
```

These routes return 404 for unknown `(tenant, service)` pairs and don't require auth, so deploy-verification scripts and external probes can hit them directly.

## Metrics

The platform serves Prometheus metrics on `/metrics` (`:8080`) and each worker on `:8081/metrics` (`WORKER_HTTP_PORT`). The web runner serves its own on `/_cufflink/metrics` (`:3100`), new in R2, only with `Authorization: Bearer $WEB_STATUS_TOKEN` and not at all without the variable; a scrape of it needs the token. The extension runner does the same on its port (`RUNNER_PORT`, 3200) with `EXT_STATUS_TOKEN`. Prometheus (`infra/monitoring/prometheus.yml`) scrapes `platform:8080` and `wasm-worker:8081` every 15 seconds, and neither runner: add a scrape with its token for the runners' series. The NATS server's own metrics are not collected: its monitoring port serves JSON, which Prometheus reads only through an exporter.

Histogram buckets: `*_seconds` metrics use 5 ms, 10 ms, 25 ms, 50 ms, 100 ms, 250 ms, 500 ms, 1 s, 2.5 s, 5 s, 10 s, 30 s, 60 s, 120 s, 300 s, 900 s; `*_bytes` metrics use 1 KiB, 16 KiB, 256 KiB, 1 MiB, 4 MiB, 16 MiB, 64 MiB, 256 MiB, 1 GiB.

### Requests, CRUD and deploys (platform)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_http_request_duration_seconds` | histogram | `method` (the nine standard methods or `other`), `route`, `status_class` | Request latency; `route` is the request's [span name](#span-and-route-names), never a raw path |
| `cufflink_http_requests_in_flight` | gauge | — | Requests being served |
| `cufflink_crud_requests_total` | counter | `tenant`, `service`, `operation` (`list`, `get`, `create`, `update`, `delete`, `batch`), `table` | CRUD requests for a table a registered service serves; a request for another service or table is not counted |
| `cufflink_services_total` | gauge | — | Active services (refreshed on each scrape) |
| `cufflink_service_info` | gauge | `tenant`, `service`, `version` (`v<n>`), `commit` (empty when the deploy named none), `mode` | Always 1: one series for each service this replica has registered, which is each active service, naming the version and commit it runs ([above](#cufflink_service_info)) |
| `cufflink_deployments_total` | counter | `result` (`attempted`, `success`, `failed`, `rejected`, `skipped`), `mode` | Deployment outcomes |
| `cufflink_deployment_wasm_missing_total` | counter | — | Deploys of a handler-bearing service that arrived without a WASM binary |
| `cufflink_deploy_import_rejected_total` | counter | — | Deploys refused (422) because the module imports a host function this platform does not provide |
| `cufflink_deploy_lock_dropped_total` | counter | — | Deploy locks released by closing their session (an error, a cancelled request or a failed unlock) rather than by an unlock |
| `cufflink_deploy_jobs` | gauge | `status` (`queued`, `running`, `succeeded`, `failed`) | Deploys that run beyond their request (`POST /api/services/deploy?async=true`), by state, as this replica last counted them |
| `cufflink_sql_migrations_applied_total` | counter | `tenant`, `service` | SQL migrations deploys applied, those of a deploy that failed after them included |
| `cufflink_sql_migration_duration_seconds` | histogram | — | How long the SQL migrations of a deploy took, for deploys that applied any |
| `cufflink_schema_guard_findings_total` | counter | `kind` (`satisfied`, `drift`, `dependent`) | What the schema guard of a deploy found: changes the database held already, places where it contradicts the manifest, and objects outside the manifest that depend on what the deploy would drop |
| `cufflink_router_wasm_binary_missing_total` | counter | `service` | Custom route invoked but no loadable WASM binary was found |
| `cufflink_route_posture_undeclared_total` | counter | `service` | Custom route executed without a declared posture (legacy, unenforced) |
| `cufflink_rate_limited_total` | counter | — | Requests rejected by the rate limiter (when `RATE_LIMIT_ENABLED` is true, or unset under the hardened HTTP profile) |
| `cufflink_webhooks_delivered_total` / `cufflink_webhooks_failed_total` | counter | `event` | Webhook deliveries and failures of the dispatcher, which delivers for the tenants that enforce no outbox (`cufflink_webhook_deliveries_total` counts the others') |
| `cufflink_jwks_stale_served_total` | counter | `realm` | Tokens validated against a cached JWKS because Keycloak could not be reached (`KEYCLOAK_JWKS_MAX_STALE_SECS`) |
| `cufflink_http_load_shed_total` | counter | — | Requests answered 503 `server busy` because `HTTP_MAX_CONCURRENT_REQUESTS` were already in flight |
| `cufflink_crud_db_errors_total` | counter | `kind`, `status` | Database errors on data routes by SQLSTATE kind and the status answered (409, 422, 400, 503 or 500) |
| `cufflink_crud_filter_mode_total` | counter | `mode` (`v1`, `v2`) | List requests by filter mode |
| `cufflink_scope_decisions_total` | counter | `table`, `op`, `scope` (`all`, `rows`, `denied`, `widen`, `invalid`) | Row-scope decisions on scoped tables and routes |
| `cufflink_column_decisions_total` | counter | `table`, `columns` (`all`, `withheld`) | Column decisions on tables with [column permissions](column-permissions.md): whether the caller reads and writes every column |
| `cufflink_custom_route_duration_seconds` | histogram | `service`, `route` (the declared path, or the handler through `_fn`), `status_class` | Custom-route latency |
| `cufflink_strict_route_matches_total` | counter | `result` (`matched`, `passthrough`, `no_route`, `method_not_allowed`) | Strict-routing decisions |
| `cufflink_request_body_rejected_total` | counter | `reason` (`too_large`, `multipart`) | Custom-route bodies refused: over the route's limit, or not the multipart form the route takes |
| `cufflink_guest_response_v2_total` | counter | `body` (`empty`, `json`, `bytes`, `s3`, `sse`) | Response-v2 envelopes rendered |
| `cufflink_guest_header_dropped_total` | counter | `name` | Response headers a guest set that the platform owns, dropped |
| `cufflink_s3_stream_bytes_total` | counter | — | Bytes of stored objects streamed to clients by `s3_object` responses |
| `cufflink_sse_streams_active` | gauge | — | Event streams open, CRUD and bridged |
| `cufflink_sse_rejected_total` | counter | `reason` (`cap`, `subject`) | Streams refused: the principal's cap reached, or a subject naming another service's events its manifest does not subscribe to |
| `cufflink_sse_bridge_streams_active` | gauge | — | Bridged (`sse(...)`) streams open |
| `cufflink_sse_bridge_events_total` | counter | `result` (`forwarded`, `reduced`, `dropped`, `reducer_error`) | Messages a bridged stream handled |
| `cufflink_sse_bridge_closed_total` | counter | `reason` (`client`, `timeout`, `reducer`, `shutdown`, `overflow`, `subscription_ended`) | Why bridged streams ended |
| `cufflink_service_identity_rejected_total` | counter | `reason` (`missing`, `format`, `unknown_key`, `signature`, `expired`, `not_yet_valid`, `lifetime`, `tenant`, `service`, `method`, `path`, `depth`, `caller`, `budget`) | Service identities refused on internal routes, spent budgets refused anywhere, and invalid identities ignored on other routes |
| `cufflink_api_key_rejected_total` | counter | `reason` | Tenant API keys refused (unknown or expired) |
| `cufflink_api_keys_revoked_total` | counter | `reason` | Tenant API keys revoked |
| `cufflink_partner_key_rejected_total` | counter | `reason` (`malformed`, `unknown`, `revoked`, `expired`, `tenant`, `scope`) | Partner keys refused |
| `cufflink_partner_keys_active` | gauge | — | Partner keys neither revoked nor expired (refreshed periodically) |
| `cufflink_platform_admin_calls_total` | counter | `route` | Calls made with platform-admin authority |
| `cufflink_perm_cache_invalidations_total` | counter | `scope` | Permission-cache entries invalidated after a role change |
| `cufflink_policy_violation_total` | counter | `key`, `tenant`, `outcome` (`warned`, `denied`), `reason` | Tenant policy violations |
| `cufflink_route_credentials_stripped_total` | counter | `tenant`, `service` | Custom-route requests whose caller credentials were withheld from a guest without `forward_auth` under an enforced `tenant_binding` |
| `cufflink_policy_reload_total` | counter | `result` | Tenant catalog and policy reloads (the worker too) |
| `cufflink_webhook_dispatch_denied_total` | counter | `reason` | Webhook deliveries refused, e.g. an event whose tenant does not match its subject |
| `cufflink_wasm_hash_mismatch_total` | counter | `service_id` | Modules refused because storage holds bytes other than the deployment's hash (the worker too) |

### WASM execution (platform and worker)

`kind` is `route`, `hook`, `subscription`, `on_migrate`, `job`, `worker_route` or `sse_reducer`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_wasm_handler_duration_seconds` | histogram | `kind`, `result` (`ok`, `error`, `timeout`, `trap`, `memory_limit`, `shutdown`, `cancelled`) | Guest execution time and outcome |
| `cufflink_wasm_inflight` | gauge | `kind` | Guests executing |
| `cufflink_wasm_queue_wait_seconds` | histogram | `kind` | Wait for an execution slot (`WASM_MAX_CONCURRENT`) |
| `cufflink_wasm_admission_refused_total` | counter | `tenant`, `service`, `class` (`interactive`), `reason` (`route_concurrency`), `mode` (`enforce`) | Invocations answered `503` without running, because their route's `max_concurrent` stayed full for `WASM_ADMISSION_WAIT_MS`; logged at most once a minute per tenant, service and reason under target `cufflink::wasm::budget` |
| `cufflink_wasm_memory_peak_bytes` | histogram | `kind` | Peak linear memory per execution |
| `cufflink_wasm_response_bytes` | histogram | `kind` | Response size per execution |
| `cufflink_wasm_limit_events_total` | counter | `limit` (`memory`, `length`, `response`, `table`), `mode` (`warn`, `enforce`) | Guest limits hit |
| `cufflink_wasm_module_cache_entries` | gauge | — | Compiled modules cached (platform only; set at scrape) |
| `cufflink_wasm_module_compile_seconds` | histogram | `source` (`deploy`, `s3`) | Module compilation time |
| `cufflink_wasm_module_load_seconds` | histogram | `source` (`cwasm`, `compile`) | Time to load a module a replica had not cached: from its precompiled artefact, or compiled from the `.wasm` |
| `cufflink_wasm_artifact_mismatch_total` | counter | — | Precompiled artefacts refused because their bytes do not match the recorded hash |
| `cufflink_db_v2_calls_total` | counter | `op`, `result` (`ok` or the error kind) | Guest `db-v2` calls |
| `cufflink_db_v2_call_duration_seconds` | histogram | `op` | Guest `db-v2` call time |
| `cufflink_db_tx_open` | gauge | — | Guest interactive transactions open |
| `cufflink_db_tx_duration_seconds` | histogram | `outcome` (`commit`, `rollback`, `abandoned`) | Guest transaction lifetime |
| `cufflink_db_tx_abandoned_total` | counter | `reason` (`trap`, `deadline`, `shutdown`, `leaked`) | Transactions the guest left open, rolled back by the platform |
| `cufflink_cache_calls_total` | counter | `op` (`get`, `mget`, `set`, `set_nx`, `del`, `incr`, `expire`, or `invalid` for a call that is not one), `result` (`ok` or `unavailable`, `timeout`, `invalid_key`, `invalid_ttl`, `too_large`, `protocol`) | Guest namespaced-cache calls |
| `cufflink_cache_call_duration_seconds` | histogram | `op` (as above) | Guest cache call time |
| `cufflink_service_calls_total` | counter | `caller`, `target` (`-` before the target is known), `result` (`ok`, `http_4xx`, `http_5xx`, `timeout`, `depth`, `unknown_service`, `transport`, `denied`) | Guest service calls |
| `cufflink_service_call_duration_seconds` | histogram | — | Guest service-call time |
| `cufflink_ext_calls_total` | counter | `caller`, `extension` (`-` for a call refused as `invalid` or `not_declared`, so a guest cannot add label values), `result` (`ok`, `invalid`, `not_declared`, `not_found`, `denied`, `too_large`, `timeout`, `unavailable`, `failed`) | Guest `ext_call` calls, each counted once by how it ended |
| `cufflink_ext_call_duration_seconds` | histogram | `extension` (as above) | Guest `ext_call` time, refusals included |
| `cufflink_egress_denied_total` | counter | `reason` (`invalid_url`, `scheme`, `metadata`, `unresolvable`, `body_too_large`) | Outbound guest requests the egress guard refused |
| `cufflink_nats_reply_ignored_total` | counter | — | `Cufflink-Reply` headers naming no platform inbox, ignored |
| `cufflink_db_guest_statement_duration_seconds` | histogram | `op` (`query`, `execute`, `execute_all`, `read_only`) | Guest database statements |
| `cufflink_nats_publish_unscoped_total` | counter | — | Guest `nats::publish`/`request` allowed only because the manifest predates `publishes` enforcement |
| `cufflink_nats_publish_denied_total` | counter | `reason` (`policy`, `reserved`, `claimed`, `invalid`) | Guest `nats::publish`/`request` denied: by the manifest's `publishes` allowlist, as a platform or another tenant's `dw.` subject, as a subject another tenant claims, or as not a subject (a wildcard, an empty token, over 1024 bytes). A v2 publish on what is not a subject is counted in `cufflink_nats_guest_publish_total{result="invalid"}` instead |
| `cufflink_nats_guest_publish_total` | counter | `mode` (`jetstream`, `core`), `result` (`ack`, `duplicate`, `sent`, `denied`, `no_stream`, `timeout`, `unavailable`, `invalid`, `too_large`) | Guest `nats::publish_with`/`publish_json`/`publish_core` calls (feature `nats-publish-v2`), each counted once by how it ended. A `denied` one is also in `cufflink_nats_publish_denied_total` (which says why) and an `unavailable` one in `cufflink_nats_publish_failed_total{kind="guest_v2"}`: sum within a family, never across |
| `cufflink_host_http_v2_total` | counter | `result` (`ok`, `denied`, `timeout`, `too_large`, `error`) | Guest `http::fetch_bytes`/`multipart` calls (feature `http-v2`) |
| `cufflink_host_storage_v2_total` | counter | `op` (`upload`, `head`), `result` (`ok`, `not_found`, `denied`, `too_large`, `not_configured`, `timeout`, `backend`) | Guest `storage::upload`/`head` calls (feature `storage-v2`) |

### Messaging, registry and lifecycle

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_subscription_messages_total` | counter | `tenant`, `service`, `handler`, `result` (`ok`, `error`, `invalid`, `nak`, `retry`, `dead_letter`, `terminal`) | Subscription messages by outcome; `error` includes a handler the shutdown stopped (its message is acknowledged); `nak` counts messages handed back at shutdown. Under `ack: on_success`, `retry` is a message delivered again, `dead_letter` one given up on and kept, `terminal` one given up on without a dead letter |
| `cufflink_subscription_handler_duration_seconds` | histogram | `kind` (`subscription`, `schedule`, `job`, `hook`, `backup`, `webhook`) | How long a consumer's handler took with a message (platform and worker) |
| `cufflink_pump_inflight` | gauge | `kind` | Messages being handled by the consumers of that kind (platform and worker) |
| `cufflink_pump_heartbeats_total` | counter | `kind` | Times a message still being handled had its `ack_wait` restarted |
| `cufflink_dlq_messages_total` | counter | `tenant`, `kind` (`sub`, `sched`, `job`, `hook`, `webhook`, `outbox`) | Messages stored in the dead-letter queue (`CUFFLINK_DLQ`) |
| `cufflink_dlq_publish_failed_total` | counter | `kind` | Dead letters the queue could not take; the message is handed back and stays in its stream |
| `cufflink_platform_stream_unavailable_total` | counter | `stream` | Attempts to create a platform stream (`CUFFLINK_DLQ`, `CUFFLINK_TICKS`, …) that failed; it is retried with backoff (platform and worker) |
| `cufflink_outbox_rows_written_total` | counter | `tenant`, `kind` (`event`, `audit`, `webhook`) | Rows written to a tenant's outbox (`outbox: enforce`) |
| `cufflink_outbox_published_total` | counter | `tenant`, `route` (`jetstream`, `core`, `reject`, `unauthenticated`), `result` (`ok`, `error`, `rejected`, `unauthenticated`) | Outbox rows the relay published, by where they went |
| `cufflink_outbox_dead_lettered_total` | counter | `tenant`, `reason` (`rejected`, `unauthenticated`, `exhausted`) | Outbox rows given up to the dead-letter queue: a subject the tenant may not publish, a message the server cannot carry, an audit or webhook row the platform did not write, or `OUTBOX_MAX_ATTEMPTS` failures |
| `cufflink_outbox_rows_unauthenticated_total` | counter | `tenant`, `kind` (`audit`, `webhook`) | Audit and webhook rows of an outbox whose MAC did not hold: written by something other than the platform (a guest's SQL), or altered since. Any is worth an alert |
| `cufflink_outbox_dead_letter_truncated_total` | counter | `tenant` | Dead letters of outbox rows the queue could not take whole (a payload the server cannot carry), stored with a reference to the row in place of the payload |
| `cufflink_outbox_relay_leader` | gauge | `tenant` | 1 on the replica that relays the tenant's outbox, 0 on the others |
| `cufflink_audit_records_total` | counter | `tenant`, `action_class` (`write`, `read`, `custom`) | Audit records made: of a row written, of a read of a `sensitive` table (made only where the tenant's records are durable), of a custom route called |
| `cufflink_audit_reads_unrecorded_total` | counter | `tenant`, `reason` (`sampled`, `coalesced`) | Reads of `sensitive` tables with no record of their own: not of the reads sampled, or counted into the next record of their kind |
| `cufflink_audit_publish_failed_total` | counter | `sink` (`legacy`, `durable`, `outbox`) | Audit records that were dropped: of a batch on `dw.{tenant}.logs`, on the stream of durable records (`CUFFLINK_AUDIT_DURABLE_ALL`), or of a custom route that could not be written to the outbox |
| `cufflink_webhook_deliveries_total` | counter | `result` (`delivered`, `retry`, `failed`, `dropped`, `deferred`) | Attempts to deliver the webhooks of tenants that enforce an outbox: taken by the receiver, to be made again, given up (refused, or the eighth), not made (the webhook was deleted or switched off meanwhile), or waiting for its webhook or tenant to have room on the replica (no attempt) |
| `cufflink_outbox_depth` / `cufflink_outbox_oldest_age_seconds` | gauge | `tenant` | Rows a tenant's outbox holds and the age of the oldest, measured by the leader every 30 s (a replica with relays lists the streams as often) |
| `cufflink_consumer_loops` | gauge | — | Subscription consumer loops running on the replica (set at scrape) |
| `cufflink_consumer_ops_total` | counter | `op` (`keep`, `create`, `update`, `handover`, `delete`, `release`) | What deploys did to durables |
| `cufflink_consumer_reconcile_runs_total` | counter | `result` (`ok`, `error`) | Reconciliations of a replica's consumer loops with the registry |
| `cufflink_consumer_reconcile_failures` | gauge | `tenant`, `service` | 1 while a service's loops could not be reconciled |
| `cufflink_consumer_missing` | gauge | `tenant`, `service`, `durable` | 1 while a durable a bound subscription names is not on its stream (the reconciler never creates it) |
| `cufflink_consumer_pending` / `cufflink_consumer_ack_pending` | gauge | `tenant`, `service`, `handler`, `durable` | Messages a durable has yet to deliver, and delivered but not yet acknowledged (set by the reconciler); what was reported of a removed service is forgotten |
| `cufflink_consumer_stream_unreadable_total` | counter | `stream` | Reconciliations that could not read a consumed stream; the services that consume it fail their pass, and no other |
| `cufflink_stream_ops_total` | counter | `op` (`create`, `update`, `keep`) | What deploys did to the streams manifests declare |
| `cufflink_stream_replicas_below_target` | gauge | `stream` | 1 while a consumed stream has fewer replicas than the platform's target |
| `cufflink_schedule_fires_published_total` | counter | `tenant`, `service`, `schedule`, `catch_up` (`true`, `false`) | Schedule fires published, once across replicas; a catch-up also logs `schedule_catch_up` |
| `cufflink_schedule_runs_total` | counter | `tenant`, `service`, `schedule`, `result` (`ok`, `error`, `coalesced`, `enqueued`) | Fires handled: run, not run under `overlap: skip` (`coalesced`), or enqueued as a job |
| `cufflink_schedule_lag_seconds` | histogram | — | How late a fire started after it was due |
| `cufflink_nats_publish_failed_total` | counter | `kind` (`core`, `jetstream`, `guest_v2`), `reason` (`timeout`, `error`) | Bounded publishes that failed |
| `cufflink_registry_version` | gauge | — | Registry reload generation (also in `/ready`) |
| `cufflink_registry_reload_total` | counter | `result` (`ok`, `error`) | Registry reloads, and refreshes of one service after its deploy, rollback, module upload or enabling. A refresh is discarded and read again when anything in the replica's registry changed under it, another service's refresh included, and gives up after three such reads. So several deploys or status changes landing on one replica at once can count an `error` on a healthy platform: each has committed, and the notification it sends makes the replica reload. Alert on `error` that persists, or that comes with `cufflink_registry_load_errors_total` or database errors, not on single counts during deploys |
| `cufflink_registry_load_errors_total` | counter | — | Service rows skipped because their manifest no longer decodes |
| `cufflink_ready` | gauge | `state` (`starting`, `migrated`, `ready`, `shutting_down`) | 1 for the platform's current readiness state, 0 for the others |
| `cufflink_migrations_pending` | gauge | — | Control-plane migrations not yet applied |
| `cufflink_db_pool_max` / `cufflink_db_pool_size` / `cufflink_db_pool_available` / `cufflink_db_pool_waiting` | gauge | `pool` (`platform`, `tenant:{slug}`, `login:{tenant}:{service}`) | Connection-pool state (set at scrape; platform and worker). A `login:` pool is the one a service logs in through as its own role (`db_roles: enforce`) |
| `cufflink_db_role_fallback_total` | counter | `tenant`, `service` | Statements the service's role was refused that ran again as the platform (`db_roles: warn`); each is also a `cufflink_policy_violation_total{key="db_roles",reason="role_refused"}` and, once a minute for a service, a `db_role_fallback` log event with Postgres's message |
| `cufflink_db_role_provision_failures_total` | counter | `tenant`, `service` | Deploys around which the service's role could not be made or brought in order |
| `cufflink_db_role_verify_unanswered_total` | counter | `tenant`, `service` | Verifications that could not try a login as the role (server unreached, no connection to give): the binding keeps what was known of it, and a `db_role_verify_unanswered` log event names what was not answered |
| `cufflink_db_role_lock_dropped_total` | counter | — | Provisioning locks of a tenant database whose holder was dropped mid-work; the session is closed, and the lock with it |
| `cufflink_db_role_retire_failures_total` | counter | — | Deleted services whose managed role could not be made `NOLOGIN` (logged with the statement to run by hand) |
| `cufflink_secret_operations_total` | counter | `op` (`seal`, `open`, `reseal`), `result` (`ok`, `error`, `stale`) | Stored secrets sealed, opened and sealed again for another row; `stale` is an `open` that found the sealed value older than a plaintext an earlier release wrote, and read the plaintext |
| `cufflink_secret_plaintext_rows` | gauge | `table` | Under `encrypted` storage, at startup: rows that still hold a secret's plaintext beside its sealed value, until `reseal --operation encrypt` |
| `cufflink_shutdown_phase_seconds` | histogram | `phase` (`http`, `tasks`, `reconcile`, `calls`, `children`, `nats`, `telemetry`) | Duration of each shutdown phase (every binary; each phase is also logged, as the platform's listener has closed by the later ones) |

### Worker

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_worker_jobs_total` | counter | `kind` (`sync`, `async`, `long`), `result` | Jobs finished: `sync` `ok`/`error`; `async` `ok`/`error`/`interrupted`, counted per delivery (a failed after-hook is delivered up to three times, then dead-lettered); `long` `ok`/`failed`/`crashed`/`interrupted`/`retry`/`lease_lost` (`interrupted` = handed back at shutdown, `retry` = failed with another attempt to follow) |
| `cufflink_worker_inflight` | gauge | `kind` | Jobs executing |
| `cufflink_worker_job_identity_mismatch_total` | counter | — | Jobs refused because the identity they claim is not their deployment's |
| `cufflink_worker_reply_refused_total` | counter | — | Request/reply jobs refused, and not run, because their reply subject is not a `_INBOX.` inbox |
| `cufflink_job_transitions_total` | counter | `to` (`running`, `completed`, `retry`, `failed`, `released`) | Long jobs' state changes, counted by the worker, its reclaimer included; a `retry` asked of the platform (`POST …/_jobs/{id}/retry`) is counted by the platform |
| `cufflink_job_fence_lost_total` | counter | `op` (`execute`, `complete`, `retry`, `fail`, `release`) | Writes of an attempt the fence refused because the job changed hands; logged as `job_fence_lost` |
| `cufflink_job_lease_lost_total` | counter | — | Jobs whose worker could not extend the lease and gave them up |
| `cufflink_job_reclaimed_total` | counter | `reason` (`unpublished`, `lease_expired`, `exhausted`) | Jobs the reclaimer published again, or failed when out of attempts |
| `cufflink_job_backlog` | gauge | `tenant` | Jobs pending or running per tenant (set by the reclaimer) |

### Web runner

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_web_runner_child_exits_total` | counter | `reason` | App processes that exited, and why |

### Extension runner

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_runner_api_requests_total` | counter | `endpoint` (`native`, `binary`, `environment`, `web`, `bundle`, `web_environment`), `result` (`ok`, `unauthorized`, `not_found`, `invalid`, `error`) | Exported by the platform: answers of its runner API to extension runners and tenant-scoped web runners: a refused runner key, and anything asked outside the key's tenant (`not_found`), included |
| `cufflink_ext_runner_children` | gauge | `state` (`ready`, `starting`, `crash_looping`, `refused`) | Native services' deployments by state: running and relaying calls; being started or waiting out a restart's backoff; not started again until their next deploy, for failing right after they start or for serving other extensions than their manifest provides |
| `cufflink_ext_runner_restarts_total` | counter | `reason` (`exit`, `signal`, `killed`, `unready`, `crash_loop`) | Children that failed and are started again: exited by themselves or on a signal, killed after three failed health probes, or never answered ready; `crash_loop` counts the failure after which the service is left stopped |
| `cufflink_ext_runner_calls_total` | counter | `result` (`ok`, `failed`, `unavailable`, `timeout`, `too_large`, `invalid`, `late`, `no_reply`) | Calls relayed: answered with a value or with the extension's own error; `unavailable`, `timeout` or `too_large` when the child answered that reserved code, could not be reached, or replied more than NATS carries, or the runner stopped relaying while the call waited for a slot; refused as malformed; not answered because the caller's deadline passed or the call had no reply subject |
| `cufflink_ext_runner_call_duration_seconds` | histogram | — | From taking a call to answering it |

Metrics that carry `tenant` and `service` can be filtered to a tenant's service; a metric with `service` and no `tenant` cannot tell two tenants' services of one name apart. Label cardinality is bounded by the deployed services, handlers and subscriptions: nothing is labelled from a path segment that did not resolve to one of them ([span and route names](#span-and-route-names)).

## Known limits

- **A child's lines link to a trace of ten seconds of its output.** `child_output` is a span for one window of the line budget of one output stream of a child: it opens with the window's first line and is exported when the window ends, ten seconds later at most, or when the child exits. A line a child prints carries that span's trace id, so its trace link books nowhere for those seconds, and then to the up to 100 lines the stream logged in that window, with the count of those the budget left out. It is not a trace of the request the child was serving. The line itself, under its service, is there at once.
- **No traces from inside a native service.** `cufflink-native` exports nothing itself: a native service's output reaches Loki through its runner, and the caller's `ext.call` span, under the calling service's scope, is what a trace shows of it. A child with an OpenTelemetry SDK of its own (a web app, say) exports what that SDK sends.
- **Tempo's service graph merges services of one name.** Two tenants' services of the same name are one node until the graph is filtered by `service_namespace` ([dashboards](#dashboards)).
- **NATS server metrics are not collected** by the canonical stack: the server's monitoring port (8222) serves JSON, which Prometheus reads only through an exporter. What the binaries count of their messaging is in [the metrics below](#messaging-registry-and-lifecycle).
- **The collector has no metrics pipeline.** Nothing Cufflink runs exports metrics over OTLP, and the canonical collector takes none. Prometheus scrapes Cufflink's metrics, and Tempo writes the span metrics.
- **Exemplars are on `traces_spanmetrics_latency_bucket`**, under the label `traceID` (Tempo 2.7 names it), and not on `traces_spanmetrics_calls_total`.
- **Requests answered before they resolve are `METHOD unmatched`**, a real service's included: a 429, a 503 from load shedding, a 413 and a CORS preflight are not attributed to a tenant in span metrics or in the `route` label ([span and route names](#span-and-route-names)).
- **The runners' own metrics are not scraped** by `prometheus.yml`; they need their status tokens ([metrics](#metrics)). The panels of the platform dashboard that read them stay empty until a scrape is added.
- **Dashboards list what logged.** A tenant or service that logged nothing in the dashboard's range is not in its lists.
- **Lines from before the switch keep their labels.** After a Loki starts being fed over OTLP, `cufflink logs` under `scoped` and the dashboards show what was logged since.
- **Who reaches the collector can write as anyone** ([who can write a tenant's lines](#who-can-write-a-tenants-lines)).
