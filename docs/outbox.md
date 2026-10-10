# The Outbox, Durable Audit and Webhook Deliveries

Needs a platform that advertises `outbox` (the policy and the relay) and
`audit_redaction` (`audit_redact` and `sensitive` tables) in
`GET /api/version` → `supports`.

By default a data write commits its row and then publishes its event
(`dw.{tenant}.{service}.{table}.{created|updated|deleted}`) and its audit
record (`dw.{tenant}.logs`); a crash or a NATS outage between the two loses
the message. The tenant policy `outbox: enforce` closes that gap. The rules
the policy follows, the durable record's fields and the delivery rules of
webhooks are in [policies](policies.md#durable-audit-and-webhook-deliveries);
this page is how to turn it on and run it.

## What changes under `outbox: enforce`

- A CRUD create, update, delete or batch is one transaction that writes the
  row and, in the tenant database's outbox (`_cufflink.outbox`), what it
  publishes: the event, the audit record, the durable audit record and a
  delivery for each webhook registered for the event. A write that commits
  publishes; one that rolls back publishes nothing.
- A relay publishes the outbox in the order it was written, one leader per
  tenant across the replicas, with a `Nats-Msg-Id` on every message so a
  stream drops a repeat. Subjects and payloads are those of `off`.
- A message larger than the NATS server carries is refused at write time
  (413 naming the limit) instead of blocking the outbox.
- Audit records are also kept on the stream `CUFFLINK_AUDIT`
  (`cufflink.audit.{tenant}.{service}`), and webhooks are delivered by the
  platform with retries until the receiver takes them.

There is no `warn` mode: a shadow write would publish twice.

## Turning it on

```sh
cufflink tenants policy set harbor outbox enforce
```

Setting the policy first creates the schema `_cufflink` in the tenant's
database (the outbox, its lease and the enqueue functions), so the
platform's database role needs `CREATE` on that database; when Postgres
refuses, the policy is not stored and the answer (422) carries Postgres's
message. Every replica also gives the schema, within a minute, to each
tenant whose platform default is `enforce`. A write that finds no schema
yet is answered 503 with `Retry-After`.

Turning the policy back to `off` is safe at any time: the relay keeps
publishing what was written under `enforce` until the outbox is empty, even
across restarts, and then stops.

## Running it

- Relay settings: `OUTBOX_RELAY_BATCH`, `OUTBOX_RELAY_POLL_MS`,
  `OUTBOX_PUBLISH_CONCURRENCY` (`1` for strict one-by-one order),
  `OUTBOX_MAX_ATTEMPTS`; the audit stream's `CUFFLINK_AUDIT_MAX_AGE_DAYS` and
  `CUFFLINK_AUDIT_MAX_BYTES`; `WEBHOOK_DELIVERY_CONCURRENCY`
  ([setup](setup.md#platform-configuration)).
- Watch `cufflink_outbox_depth` and `cufflink_outbox_oldest_age_seconds`
  (a growing age is a stuck relay or an unreachable NATS),
  `cufflink_outbox_relay_leader` (one replica per tenant) and
  `cufflink_outbox_dead_lettered_total` ([observability](observability.md)).
- A row that keeps failing is retried, after 1 s doubling to 5 min, ahead of
  every row written after it, then moved to the dead-letter queue as
  `cufflink.dlq.{tenant}.{service}.outbox.{kind}`, where `cufflink dlq`
  lists, replays and drops it. A row whose subject the tenant may not
  publish is dead-lettered at once (`outbox.rejected`).
- Behind a transaction pooler (`TENANT_DB_POOLER=pgbouncer_transaction`),
  which carries no `LISTEN`, the relay polls instead of being woken.

## Enqueueing from a guest

A guest that writes its own rows in SQL can publish from the same
transaction, with the same guarantee the data routes have, by calling the
function the platform's own writes go through under a service's role:

```sql
_cufflink.outbox_enqueue_own(
    p_kind TEXT[], p_subject TEXT[], p_payload BYTEA[], p_headers JSONB[], p_msg_id UUID[])
```

The five arrays are read side by side, one row for each position. It needs
the tenant under `outbox: enforce`, which is when the relay runs, and
under `db_roles` (`warn` or `enforce`) with a verified binding, so that
the guest's SQL runs as its service's role
([database roles](db-roles-and-secrets.md)).

- **The service is not an argument.** The function takes it from the role
  that calls it, by the map the platform keeps in `_cufflink.service_roles`.
  A role mapped to no service, the platform's own included (which is what a
  guest's SQL runs as with `db_roles` off), is refused with SQLSTATE 42501
  (`role … enqueues for no service`). A service's role may not call
  `outbox_enqueue_many`, nor touch the outbox's tables.
- **`kind` is `event` for a guest.** The column also takes `audit` and
  `webhook`, but those rows are the platform's: each carries a MAC only the
  platform can make, and the relay dead-letters one without it as
  `unauthenticated`, counting it in
  `cufflink_outbox_rows_unauthenticated_total`.
- **`subject` must be one the tenant may publish on**: under
  `dw.{tenant}.`, or a subject the tenant claims
  ([streams](subscriptions.md)). The function stores what it is given; it is
  the relay that checks, when the row is due, and it dead-letters a row
  whose subject is outside the tenant's namespace, or is not a subject, at
  once (`rejected`). So does a message larger than the server carries.
- **`msg_id` is the row's `Nats-Msg-Id`**, under which it is published
  however often the relay has to try, so a stream drops a repeat inside
  its duplicate window. Derive it from what the message is about when the
  same event may be enqueued twice; a random one is enough otherwise.
- **`headers`** is a JSON array of `[name, value]` pairs of strings
  (`'[]'` for none). An entry of another shape is left out, as is every
  header whose name begins `Nats-` or `Cufflink-` (`Nats-Msg-Id`,
  `Nats-Rollup` and `Cufflink-Reply` are not a row's to set) or that names
  `Nats-Msg-Id` in its name or value.
- **`payload`** is the message's bytes as they are published.

`db-v2` binds no `bytea[]` or `jsonb[]` parameter, so build the arrays in
SQL. One row:

```rust
sql::transaction(|tx| {
    tx.execute("UPDATE notes SET archived = true WHERE id = $1::text::uuid", &params![id])?;
    tx.query(
        "SELECT _cufflink.outbox_enqueue_own(
             ARRAY['event'], ARRAY[$1::text], ARRAY[$2::bytea],
             ARRAY[$3::jsonb], ARRAY[$4::text::uuid])",
        &params![subject, payload_bytes, serde_json::json!([]), msg_id],
    )?;
    Ok(())
})?;
```

The row commits or rolls back with the transaction, and is published after
it commits, in the order the outbox was written.

## Audit records

`CUFFLINK_AUDIT_DURABLE_ALL=true` keeps the durable records of every
tenant, published once a write is committed for tenants without an outbox.

A table can keep values out of every audit record and have its reads
recorded:

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(
    name = "clients",
    audit_redact = ["national_insurance", "bank_*"],
    sensitive,
)]
pub struct Client { /* … */ }
```

- `audit_redact` — columns whose values both audit records replace with
  `"[redacted]"`, by name or, with a final `*`, by prefix. Events and
  webhook deliveries still carry the row.
- `sensitive` — listings, reads, GraphQL queries and SSE streams of the
  table each make a durable record naming the rows read (at most 100) and
  the columns filtered by, never the values; an update's record keeps the
  row as it was. A read that cannot be recorded is refused.

An audit record made with an API key minted for a user names that user.

## Webhook deliveries

For a tenant under `enforce`, a webhook is sent as before with
`X-Cufflink-Delivery` added (the same id at every attempt, for the
receiver to deduplicate). A 408, 429, 5xx or unreachable receiver is tried
again, after 10 s doubling to an hour or after a `Retry-After` of an hour
or less, eight times in all; any other answer, and an address the tenant's
`egress` policy refuses, ends it. See what became of them:

```sh
curl -H "Authorization: Bearer $TOKEN" \
  "$CUFFLINK_URL/api/services/$SERVICE_ID/webhooks/$WEBHOOK_ID/deliveries?limit=50"
```

Deliveries given up are in the dead-letter queue as
`cufflink.dlq.{tenant}.{service}.webhook.{webhook_id}`.
