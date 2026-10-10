# Subscriptions, Streams and Schedules

What a service declares to be handed messages, the streams it needs, what
it publishes, and the handlers the platform runs on a timetable. The
basics of a subscription (subjects, the request a handler receives,
`publishes`) are in the [WASM guide](wasm-guide.md#nats-subscriptions);
this page is what each declaration can say and what the platform does with
it. With nothing but `(subject, handler, wasm)` declared, a subscription
behaves as it did in v0.27.0.

| Declaration | Platform feature |
|-------------|------------------|
| any delivery keyword below, `consumers: scoped` | `nats_consumer_config` |
| `streams: [...]` | `streams` |
| `schedules: [...]` | `schedules` (`jobs_v2` too for a `job` with `retry_on` or `backoff`) |
| `cufflink-fn` feature `nats-publish-v2` | `nats_publish_v2` |
| `cufflink-fn` feature `delivery-control` | `delivery_control` |

A platform without the feature refuses the deploy (422) instead of running
the service without it ([versions](versions.md)).

## Delivery

Keywords follow the positional items of a subscription:

```rust
subscriptions: [
    ("books.due_date.set", "on_book_event", wasm, "BOOKS",
     ack = on_success, max_deliver = 3, backoff = [10, 60], ack_wait = 30,
     concurrency = 4, timeout = 120),
],
```

| Keyword | Default | Meaning |
|---------|---------|---------|
| `ack` | `always` | `always` acknowledges the message whatever the handler did. `on_success` acknowledges it only when the handler succeeds (below) |
| `max_deliver` | unlimited; `5` under `ack = on_success` | Deliveries before the message is given up on (1 to 100, or `-1` for no limit) |
| `backoff` | 5 s doubling to 300 s; for a bound durable (`durable = "…"`), its `ack_wait` | Delay before each redelivery in seconds, the last repeating (up to 10 steps) |
| `ack_wait` | `CONSUMER_DEFAULT_ACK_WAIT_SECS` (30) | Seconds the server waits for a settlement (5 to 3600); a handler still running has it restarted every third of it |
| `max_ack_pending` | the server's | Messages delivered and not yet settled (1 to 100000) |
| `batch` | the platform's | Messages fetched at once (1 to 256) |
| `concurrency` | `1` | Messages handled at once per replica (1 to 64) |
| `deliver` | `all` | Where a new durable starts: `all`, `new`, `last`, `last_per_subject` |
| `filters` | — | Subjects beside the subscription's own the durable filters on |
| `dead_letter` | `true` under `on_success`, else `false` | Whether a message given up on is kept in the dead-letter queue |
| `durable` | — | Bind, or create, exactly this durable; the platform never deletes it |
| `timeout` | `60` | The handler's budget in seconds (1 to 300) |

Under `ack = on_success` what the handler answers settles the message:

| Handler | Message |
|---------|---------|
| 2xx | acknowledged |
| 5xx, a trap, an error | delivered again after `backoff`; dead-lettered after `max_deliver` |
| 4xx | not delivered again; dead-lettered |
| `Response::retry_after(delay)` (`delivery-control`) | delivered again after `delay`, whatever `backoff` says, and after an hour at most |
| `Response::terminal(reason)` (`delivery-control`) | not delivered again; dead-lettered with `reason` |
| stopped by a shutdown | handed back at once |

Without a declared `backoff`, a failed message of a bound durable comes back
after the durable's `ack_wait`, which is when it came back before the
platform consumed the durable; any other is delayed 5 s, then twice as
long each time, up to 300 s.

Under `ack = always` every answer is a success, as it always was, and
`retry_after`/`terminal` change nothing. To an HTTP caller of the same
handler `retry_after` is a 503 and `terminal` a 422.

### What the handler is told

`req.subscription()` describes the delivery (`None` on a platform without
`nats_message_meta`):

```rust
if let Some(sub) = req.subscription() {
    let once = format!("{}:{}", sub.stream(), sub.sequence());  // the same across redeliveries
    if sub.is_redelivery() {
        log::warn(&format!("{} delivered {} times", sub.subject(), sub.attempt()));
    }
}
```

It also has `consumer()`, `pending()`, `published_at()`, `header(name)` and
`msg_id()`.

### Durable names

A durable is named `{service}_{handler}` unless the service declares
`consumers: scoped`, which names each one for the tenant, the service, the
handler and what it consumes, so two tenants or two subscriptions of a
handler never share one. Changing the naming, a subscription's subject or
its stream hands the durable over on the next deploy: the new one starts
where the old one's acknowledgements end, and the old one is deleted (on a
`work_queue` stream the old one is deleted first and the new one delivers
what it left).
`durable = "..."` binds a durable someone else manages; the platform never
creates it again once it exists and never deletes it. A deploy is refused
(422) when the durable is one another service of the platform consumes, or
one the platform manages for another tenant or service; on a stream the
platform shares between tenants (`CUFFLINK_TICKS`) a subscription binds its
own legacy durable alone.

Deploys create, update and hand over durables. Every replica runs the loops
of every subscription, reconciling them with the registry whenever it
changes and every `CONSUMER_RECONCILE_INTERVAL_SECS`; a replica recreates a
managed durable the server lost, and reports a bound one that is missing
(`cufflink_consumer_missing`). `cufflink consumers ls` lists the durables
the manifest wants, what the server has of each, and the handovers on
record.

A disabled service (`cufflink services disable`) is in no replica's
registry, so its loops are stopped and stay stopped: a deploy, a rollback
or a module upload of it plans and carries out its durables as for any
service and starts no loop, on any replica, and its schedules do not fire.
Its durables keep their place meanwhile, and its loops start from them when
it is enabled ([active and inactive
services](deployment.md#active-and-inactive-services)). A service that
takes over a durable another consumer still pulls from is first deployed
with [`cufflink deploy --inactive`](deployment.md#deploying-a-service-inactive),
which binds the durable and starts no loop on it.

What becomes of the messages a loop holds when it stops depends on why it
stops:

| The loop stops because | Fetched, not yet started | Being handled |
|------------------------|--------------------------|---------------|
| the platform shuts down | handed back at once, for another replica to take | the handler goes on. If it finishes, its answer settles the message as ever. If the shutdown stops it first, the message is handed back at once under `ack = on_success` and acknowledged under `ack = always` (counted as `error`) |
| its durable is no longer consumed here: the service was disabled or deleted, or a deploy dropped the subscription | handed back at once | **the handler runs to its end**, and its answer settles the message. Nothing is cut short, so a durable is drained only once its running handlers have answered |
| a deploy replaced it with a loop on the same durable | handled by the old loop for up to a second, not handed back; the new loop fetches the rest | runs to its end |

A message handed back uses one of its deliveries, which `max_deliver`
counts under `ack = on_success`; that is why a replaced loop does not hand
back. Messages handed back are counted as
`cufflink_subscription_messages_total{result="nak"}`.

### The dead-letter queue

A message given up on is kept in `CUFFLINK_DLQ` for
`CUFFLINK_DLQ_MAX_AGE_DAYS` (14) with where it came from and why, as are
failed [jobs](wasm-guide.md#jobs-v2-retries-and-dead-letters) and schedule
fires.

```bash
cufflink dlq ls [--service notes] [-n 50]   # the newest letters
cufflink dlq replay <sequence>              # publish it again where it came from, then drop it
cufflink dlq drop <sequence>
```

A tenant sees only its own letters
(`GET /api/tenants/{slug}/dlq`, `POST .../dlq/{sequence}/replay`,
`DELETE .../dlq/{sequence}`).

## Streams

A service declares the JetStream streams it needs; the deploy creates each,
or extends the one that exists:

```rust
streams: [
    ("NOTES", ["notes.>", "audit.logs"], limits, max_age = 2592000, duplicate_window = 120),
],
```

The positional items are the name, the subjects and the retention
(`limits`, `work_queue`, `interest`); the keywords are `max_age` and
`duplicate_window` (seconds), `max_msgs`, `max_bytes`, `storage` (`file`,
`memory`), `discard` (`old`, `new`) and `description`. A deploy never
deletes a stream, removes a subject from one or lowers a limit below the
server's (lower it on the server first). One stream is declared by one
service of a tenant.

A stream's name and every subject outside `dw.{tenant}.>` must be claimed
by the tenant, which a platform administrator grants
(`cufflink tenants nats-claims set <slug> --subject 'notes.>' --stream NOTES`,
or `PUT /api/tenants/{slug}/nats-claims`). A subject another tenant claims
cannot be subscribed to or published on.

## Publishing with acknowledgements

`nats::publish` is fire-and-forget. With the `nats-publish-v2` feature a
handler learns whether a stream stored the message:

```rust
use cufflink_fn::nats::{self, PublishOptions};

let ack = nats::publish_json(
    "notes.created",
    &note,
    &PublishOptions::new().msg_id(&format!("note:{}", note.id)),
)?;
if ack.duplicate { /* stored before, under this id */ }
```

`publish_with(subject, &bytes, &opts)` is the same for bytes;
`publish_core(subject, &bytes, &headers)` sends to whoever listens now
without waiting for a stream. A subject no stream captures fails with
`NoStream` rather than being lost. `msg_id` makes a retried publish store
once inside the stream's duplicate window: derive it from what the message
is for, such as the delivery a subscription handler is handling. Headers
the server or the platform set (`Nats-*`, `Cufflink-*`) are refused, and so,
beside a `msg_id`, is a header whose name or value spells `Nats-Msg-Id`:
NATS servers before 2.12.4 take a header to be where its name first
appears, and would miss the id. The `publishes` allowlist and namespace
claims apply as for `nats::publish`.

## Schedules

The platform runs a handler on a timetable, once per fire across every
replica:

```rust
schedules: [
    ("overdue_reminders", every(60), "overdue_reminders"),
    ("nightly_report", cron("0 2 * * *"), "nightly_report",
     timezone = "Europe/London", catch_up = none, job(timeout = 900)),
],
```

| Item | Meaning |
|------|---------|
| name | `[a-z0-9_]`, at most 64, unique in the service |
| `every(secs)` | every `secs` seconds (at least 10), at multiples of `secs` since the Unix epoch |
| `cron("...")` | five fields, or six with seconds first; read in `timezone` (an IANA name, UTC by default) |
| `overlap` | `skip` (default): a fire while the previous one still runs is not run; `allow`: fires may run at once (four at most) |
| `catch_up` | what runs of the fires an outage missed: `latest` (default) the newest, `none` nothing, `all` the oldest `SCHEDULE_CATCH_UP_CAP` (10) |
| `job` / `job(...)` | run each fire as a [job](wasm-guide.md#long-running-jobs) on a worker, enqueued once per fire, with the job options of a route |
| `timeout` | the inline handler's budget in seconds (1 to 300, default 60) |

A fire that starts more than `SCHEDULE_ON_TIME_GRACE_SECS` (60) after it
was due is a catch-up. Under `overlap: skip` fires that pile up behind one
still running are coalesced: the newest runs and the others are counted as
`coalesced`; a caught-up fire always runs. A fire that fails is delivered
again after 10 s, then 30 s, unless a newer fire arrives first, and is dead-lettered
after its third delivery. With `job`, `overlap: skip` enqueues nothing while
a job of an earlier fire is unfinished.

The handler is a plain handler that `req.schedule()` describes:

```rust
handler!(overdue_reminders, |req: Request| {
    let Some(fire) = req.schedule() else { return Response::error("not a schedule") };
    log::info(&format!("{} due {} (late: {})", fire.name(), fire.fire_at(), fire.is_catch_up()));
    // ...
    Response::empty()
});
```

## Metrics

`cufflink_subscription_messages_total`, `cufflink_consumer_*`,
`cufflink_stream_*`, `cufflink_dlq_*`, `cufflink_nats_guest_publish_total`
and `cufflink_schedule_*` are described in
[observability](observability.md#messaging-registry-and-lifecycle).
