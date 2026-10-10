# A Platform on Your Machine: `cufflink dev`

`cufflink dev up` starts a whole platform in Docker (PostgreSQL, NATS,
Redis, object storage, Keycloak, the platform and the worker) with a tenant,
an API key and two users already there, so a service can be deployed and
called without a login or a checkout of Cufflink.

```bash
cufflink dev doctor                  # Docker, Compose, free ports, images (--with-web-runner, --with-ext-runner too)
cufflink dev up                      # start, wait for /ready (120 s), register `dev`
cufflink dev deploy --watch          # deploy the project, and again on every change
curl -H "Authorization: Bearer $(cufflink dev token --user alice)" \
  http://localhost:8080/svc/dev/todo-service/todos
cufflink dev logs platform -f
cufflink dev down                    # --volumes removes the data too
```

## What is started

| Service | Address on this machine | Notes |
|---------|------------------------|-------|
| platform | http://localhost:8080 | tenant `dev` |
| worker | *(none)* | WASM jobs and after-hooks |
| Keycloak | http://localhost:8180 | realm `cufflink-dev`; admin console `admin` / `admin` (local development only) |
| PostgreSQL | localhost:5432 | `cufflink` / `cufflink` (local development only) |
| NATS | localhost:4222 | JetStream enabled, no authentication |
| Redis, object storage (RustFS) | *(not published)* | bucket `cufflink` |
| web-runner | http://localhost:3100 | only with `--with-web-runner` |
| ext-runner | http://localhost:3200 (`/health`, `/_cufflink/ready`) | only with `--with-ext-runner`, which also starts the platform with `CUFFLINK_EXTENSIONS=true` ([extensions](extensions.md)) |

Every port is published on `127.0.0.1` alone: the stack cannot be reached
from another machine. **Every credential of the stack is for this local
development stack only** (the Postgres, Keycloak, object-storage and user
passwords above and in `compose.yml` are public, in the CLI's source):
never reuse one on a platform anyone else can reach.

The files Compose runs are the CLI's own, written to `~/.cufflink/dev/`
by every `up` (`compose.yml`, `compose.mode.yml`, the realm, and `run/`,
where the platform writes the key). Changes to them are overwritten.

## Images

| Flag | Images |
|------|--------|
| *(none)* | `kingsleyh/cufflink-platform:<tag>`, `-worker`, `-web-runner`, `-ext-runner`: the images CI published of the CLI's own commit (`sha-<commit>`, the tag a released CLI is built with); `latest` for a CLI built locally, which `up` warns of. They are built for `linux/amd64`, emulated on Apple Silicon |
| `--platform-image <ref>`, `--worker-image <ref>`, `--web-runner-image <ref>`, `--ext-runner-image <ref>` | the image named, run as it was built (a native `arm64` image runs natively); for the others the published one |
| `--from-source <repo>` | built from the checkout's `Dockerfile.platform`, `Dockerfile.worker`, `Dockerfile.web-runner` and `Dockerfile.ext-runner`, for this machine |

The platform image must be of a release that reads
`CUFFLINK_DEV_BOOTSTRAP`; `up` says so when the platform is ready and
wrote no key, and prints the version the platform says it is. Postgres,
NATS, Redis, Keycloak, RustFS and the AWS CLI that makes the bucket are
pinned to a version each.

## Native services and extensions

`up --with-ext-runner` runs the extension runner as the `dev` tenant's and
lets the platform take extensions. The platform mints the tenant's runner
key at start and writes it to the stack's `run` directory, where the runner
reads it (`CUFFLINK_RUNNER_KEY_FILE`); the runner holds no database, secrets
or storage credentials. A native service is deployed with the environment's
own key, as any service is; registering a NATS provider is a platform
admin's: mint a platform API key as `alice` for it.

```bash
cufflink dev up --with-ext-runner
cufflink dev deploy                  # in the native service's project
export CUFFLINK_PLATFORM_API_KEY=$(curl -s -X POST http://localhost:8080/api/platform/api-keys \
  -H "Authorization: Bearer $(cufflink dev token --user alice)" \
  -H "Content-Type: application/json" -d '{"name": "dev"}' | jq -r .key)
cufflink --env dev ext list --tenant dev
```

The CLI builds a native service for `x86_64` Linux, the platform CI builds
the published runner for: `--from-source` on an `arm64` machine builds an
`arm64` runner, which starts those binaries only where Docker runs `amd64`
programs under emulation. Keycloak keeps the realm it imported
while its container lives: after upgrading the CLI, `cufflink dev down`
and `up` import the realm with this role.

## The environment `dev`

A project has an environment `dev` without declaring one: the stack's
platform, tenant `dev`, and its Keycloak realm. `up` stores the key the
platform wrote in `~/.config/cufflink/credentials.json`, so every command
works against the stack with `--env dev`:

```bash
cufflink --env dev status
cufflink --env dev sql "SELECT count(*) FROM todos"
```

A project that declares `[environments.dev]` in its `Cufflink.toml` keeps
its own.

## Users

The passwords are the local development stack's alone.

| User | Password | Realm roles | May |
|------|----------|-------------|-----|
| `alice` | `alice` | `cufflink-deploy`, `cufflink-platform-admin` | deploy (native services included), manage roles, call routes; as a platform admin (which registering NATS providers and minting runner keys still need), mint a platform API key |
| `bob` | `bob` | *(none)* | call routes |

`cufflink dev token --user <name>` asks Keycloak for an access token with
the password grant (`--password` for a user you added). The tokens are
the realm's own, so every posture is enforced as it is in production:
give `bob` a permission with `cufflink --env dev roles assign` to see a
`perm(...)` route admit him.

## `deploy --watch`

`cufflink dev deploy` is `cufflink --env dev deploy`, and refuses a
project whose own `[environments.dev]` is another platform than the
stack's (`http://localhost:8080`, `http://127.0.0.1:8080` and
`http://[::1]:8080` are the stack's). With `--watch` it
deploys once and then again whenever `src/`, `migrations/`, `Cargo.toml`
or `Cufflink.toml` change, 500 ms after the last change (or 5 s after the
first, while changes keep coming). A deploy that
fails is printed and the watch goes on. A change made while a deploy
runs makes the next one.

## `doctor`

Checks that Docker and its Compose plugin answer, that the ports of the
services the stack runs are free (nothing answers on them over IPv4 or
IPv6, or the stack holds them, when it is up), and that every image is
present, can be pulled, or is one `up --from-source` builds. The web-runner
is checked with `--with-web-runner` and the ext-runner with
`--with-ext-runner`, as `up` runs them. It reads the stack
`up` last wrote, else the default one, and exits non-zero when a check
fails.

## The bootstrap

The platform of the stack is started with

```
CUFFLINK_DEV_BOOTSTRAP=tenant=dev,realm=cufflink-dev,key_file=/run/cufflink/dev-key
```

Before it reports ready, a platform given that variable:

1. creates the tenant, with `KEYCLOAK_URL` as its Keycloak and the realm
   named, and its database, unless they are there;
2. keeps the key in `key_file` when it is an active key of the tenant,
   else mints one (`CUFFLINK_API_KEY_TTL_DAYS`) and writes it there, mode
   `0600`.

A tenant of that slug with another realm, or a key file that cannot be
read or written, ends the startup with an error. **A platform refuses to
start while the variable is set unless `RUN_ENV=development` is set
explicitly**: an unset `RUN_ENV`, `staging` and `production` are all
refused, since the bootstrap creates a tenant and leaves its key in a file. Every part is required;
`key_file` is an absolute path.
