# Web bundles: build once, deploy the bytes, and say when the checks were skipped

Proposed. Covers issues #61 (build once and deploy the artifact), #66 (an
opt-in flag to skip the in-deploy checks) and #67 (leave `.next/cache` out of
the artifact). Section 14 holds the questions for the maintainer, each with
a proposed default. Section 8 says how this fits the web deploy lifecycle
design (#62, #63, #64), which is written in parallel.

## Outcome

- `cufflink build --bundle out/site.bundle` builds a web app into one file,
  as it already does for a WASM service, and sends nothing. `cufflink deploy
  --bundle out/site.bundle --env production` deploys that file and builds
  nothing. The SHA-256 the build prints is the hash the platform records,
  the hash `cufflink deployments list` shows and the hash the web runner
  names the app's directory by: one value from the build to the running
  process.
- A platform that already holds a bundle is not sent it again: a second
  deploy of the same file is a lookup by hash and a deploy request, seconds
  rather than minutes.
- A bundle records the names of the build-time values it was built with and
  a digest of their values. A deploy to an environment whose values differ
  is refused before anything is sent, so the bytes a release verified are
  never shipped where they would read another environment's settings.
- `typecheck`, `lint` and `test` still run before every web build by
  default, on a developer's machine as in CI. Only `--skip-checks`, given
  explicitly, skips them. Nothing skips them implicitly: no variable, no
  configuration key, no CI detection.
- Every web version records whether its checks ran or were skipped, and the
  deployment history and the audit log show it. A tenant policy can refuse a
  skip unless the deploy key is one the tenant lists.
- `.next/cache` and `node_modules/.cache` are never in a bundle, by a rule of
  the bundle's own writer that a test reads back from the archive, rather
  than by a flag passed to whichever `tar` the machine has.
- Deploys of WASM, CRUD and native services see no change.

## 1. What the platform does today

Read from `cli/src`, `platform/src`, `web-runner/src` and `docs/` on `main`
at v0.37.1.

### 1.1 One command builds and deploys

`cufflink deploy` in a web project (`[service].mode = "web"`) runs
`deploy_web` (`cli/src/commands/deploy.rs:1426-1570`), in this order:

1. It runs the package's `typecheck`, `lint` and `test` scripts, each only
   when `package.json` declares it (`deploy.rs:1437-1448`), through the
   detected package manager (`run_npm_script`, `deploy.rs:1601-1611`). A
   failing script stops the deploy before anything is sent. There is no way
   to skip them.
2. It registers a version: `POST /api/services/deploy` with the manifest
   `{name, mode: "web", version: <package.json version>, tables: []}`
   (`deploy.rs:1450-1462`). It sends no `cli_version` or `api_version`,
   asks the platform for no version or features, and hard-codes
   `allow_destructive: false`.
3. It applies the environment's config and secrets (`maybe_sync_configs`,
   `deploy.rs:1497`).
4. It reads the service's configs back from the platform
   (`fetch_service_configs`, `deploy.rs:1831-1892`) and lays the values
   `Cufflink.toml` declares for the environment over them, vault secrets
   included (`resolve_local_build_envs`, `deploy.rs:1780-1792`, and
   `resolve_local_envs`, `cli/src/commands/config_cmd.rs:618-626`). Every
   one of these values becomes an environment variable of `pnpm run build`
   (`deploy.rs:1518-1527`), which also inherits the whole environment of
   the shell that ran the CLI. `fetch_service_configs` answers an empty list
   on any failure (a refused request, a body that is not JSON), so a build
   can go ahead without the platform's values and nothing says so.
5. It packs `.next`, `node_modules`, `package.json` and `public` with the
   system's `tar czfh - --exclude=.next/cache …` (`create_web_tarball` and
   `web_tarball_args`, `deploy.rs:1644-1695`). In a pnpm workspace,
   `node_modules` is a pruned copy made by `pnpm --filter <name> deploy
   --prod` into a temporary directory; when that fails, the CLI prints the
   error and packs the nearest `node_modules` that holds `next` instead
   (`resolve_node_modules`, `deploy.rs:1697-1707`), which carries the
   development dependencies and the workspace's links.
6. It uploads the archive: `POST /api/services/{id}/web`
   (`platform/src/api/web.rs:19-103`).

### 1.2 The archive

- **What it holds** is decided by the host's `tar`. `h` follows every
  symbolic link, so the archive holds only directories and regular files.
  The exclusion of `.next/cache` has been there since the first commit, and
  `web_tarball_args_exclude_the_build_cache` (`deploy.rs:3727`) checks the
  argument list. Nothing reads an archive back to check that the cache is
  not in it. Whether the pattern matches, under symbolic links followed by
  `h`, depends on whether the host has GNU tar or bsdtar, each with its own
  matching rules. `node_modules/.cache`, where bundler plugins keep their
  caches, is always packed.
- **Its bytes** are not reproducible. Entry order is the file system's;
  every entry carries the file's modification time, owner and group ids and
  names; the gzip header is whatever the host's tar writes. Packing the same
  tree twice gives two hashes.
- **Its size** is bounded by the platform's artifact body limit, 250 MiB
  (`ARTIFACT_BODY_LIMIT`, `platform/src/app/layers.rs:16`, applied at
  `layers.rs:225`). The upload is buffered whole (`axum::body::Bytes`,
  `web.rs:23`) and copied once more into the storage call (`web.rs:49`).
  The runner bounds what an archive extracts to with
  `WEB_ARTIFACT_MAX_ENTRIES` (200 000) and `WEB_ARTIFACT_MAX_BYTES`
  (2 GiB), and refuses entries and links that leave the app directory
  (`web-runner/src/artifact.rs:21-72`).

### 1.3 Storage, versions and the runner

- The upload hashes the body (`web.rs:41-43`), stores it at
  `web/{service_id}/v{version}.tar.gz` (`artifact_key`, `web.rs:15-17`) for
  the service's current version, writes the key, hash and size on that
  version's row (`web.rs:53-69`) and publishes `deploy.web.<tenant>.<service>`
  (`web.rs:71-87`). The key names a version, not content: an upload under a
  version that already has an artifact replaces it.
- A web deploy is skipped when the manifest hash and the code hash both match
  the running version (`should_skip_deploy`,
  `platform/src/deployer/pipeline.rs:134-141`). For a web app the code hash
  is the module hash (`code_hash`, `pipeline.rs:121-130`), which a web app
  never has. A web deploy whose `package.json` version did not change is
  therefore skipped, and the CLI then uploads the new archive over the
  running version's object (#63).
- A web version is activated before its artifact exists
  (`activate_in_runner`, `pipeline.rs:232-261`); the runner reads only a
  current version with `artifact_s3_key IS NOT NULL`
  (`web-runner/src/source.rs:226-238`). A rollback of a web app activates a
  version with no artifact (`pipeline.rs:457-505`) (#62).
- The runner reads the key and the hash from the version's row, or from the
  runner API, which looks the key up by service and hash
  (`platform/src/api/runner.rs:170-188`); it never builds a key itself. Each
  bundle is unpacked once into `APPS_DIR/{tenant}/{service}/{service id}-{sha256}`
  (`docs/deployment.md`, "Bundles" under "Web runner").
- Deleting a version returns only a native service's object key
  (`delete_by_version`, `platform/src/repo/deployments.rs:339-353`), so a deleted or pruned web
  version's object stays in storage. Deleting a service deletes every web
  key its rows name (`platform/src/api/services.rs:251-309`).
- `web/` is one of the platform's reserved key prefixes that no guest may
  write (`PLATFORM_KEY_PREFIXES`,
  `platform/src/wasm_runtime/storage_scope.rs:10`).

### 1.4 Bundles exist, for WASM services only

`cufflink build --bundle <file>` writes a WASM or CRUD service as one
gzip-compressed JSON file (`cli/src/bundle.rs`, `cli/src/commands/build.rs`)
and `cufflink deploy --bundle <file>` deploys it without its sources
(`run_bundle`, `deploy.rs:411-449`). The file is written whole or not at all
(`write_whole`, `bundle.rs:59-67`); the gzip header carries no time and the
fields have one order, so one input gives one file
(`two_writes_of_one_input_are_byte_identical`, `bundle.rs`). A web project is
refused by both `bundle_of_sources` and `Package::of`: `bundles carry WASM
services; deploy web services from source` (`deploy.rs:558-588`,
`docs/deployment.md:566-568`).

### 1.5 Who deployed, and how

A deploy runs as an `AuthUser` whose `credential()` is either a key, by id and
name, or a token (`platform/src/auth/extractor.rs:409-417`). `cufflink
login` mints a tenant API key named `cli-{username}`
(`docs/authentication.md`), so a developer's deploys and a pipeline's both
come with an API key; only the key's id tells them apart. A deploy that
creates a version writes an audit entry `deploy`
(`platform/src/api/deployments.rs:535-546`); a skipped deploy writes none.
`DeployRequest` refuses unknown fields (`#[serde(deny_unknown_fields)]`,
`deployments.rs:33-53`, `deploy_request_rejects_unknown_fields`), so a new
field reaches only a platform that advertises it.

## 2. Design

### 2.1 A web bundle is the archive itself

A web bundle is the gzip-compressed tar archive that the platform stores and
the runner unpacks, byte for byte. Its first entry is
`.cufflink/bundle.json`, a small JSON document that says what the archive
is. No wrapper is put around the archive, and the payload is not
base64-encoded in JSON as a WASM bundle's module is: an archive may be
hundreds of megabytes, and an upload body equal to the file makes the file's
SHA-256 the platform's `artifact_hash` with nothing to re-derive.

```json
{
  "web_bundle": 1,
  "service": "site",
  "mode": "web",
  "manifest": { "name": "site", "mode": "web", "version": "1.4.0", "tables": [] },
  "commit_hash": "4f2a9c1d8e…",
  "cli_version": "0.38.0",
  "mtime": 1762646400,
  "node_modules": "pruned",
  "build_env": {
    "environment": "staging",
    "names": ["NEXT_PUBLIC_API_URL", "NEXT_PUBLIC_SITE_NAME"],
    "digest": "1c9e…"
  },
  "checks": { "skipped": false, "scripts": ["typecheck", "lint", "test"] },
  "toolchain": { "package_manager": "pnpm", "node": "v22.11.0", "next": "15.0.3" }
}
```

| Field | Meaning |
|---|---|
| `web_bundle` | The format number. This note defines format 1 |
| `service`, `mode`, `manifest` | The service and exactly the manifest a deploy sends; `service` and `mode` must equal the manifest's |
| `commit_hash` | The commit the bundle was built at (`--commit-hash`, else `git rev-parse HEAD`), or `null` with the warning WASM bundles print |
| `cli_version` | The CLI that wrote it |
| `mtime` | The modification time every entry carries (section 2.2) |
| `node_modules` | `pruned` (made by `pnpm deploy --prod`) or `nearest` (the nearest `node_modules` holding `next`) |
| `build_env` | The environment whose values the build was given, their names, and one digest of the names and values (section 2.4) |
| `checks` | Which of `typecheck`, `lint`, `test` the package declares, and whether they ran (and passed) or were skipped (section 2.5) |
| `toolchain` | For people reading a deploy: the package manager, `node --version` and the `next` package's version. Nothing compares them |

The entry is read by the CLI before it deploys a bundle and by the platform
when it takes an upload; both read only the first entry and stop. The runner
unpacks it into the app's directory with the rest. `next start` serves only
`public/` and `.next/static`, so the file is never served, and it tells
anyone looking at an app's directory on a runner what it was built from. The
runner's entry rules (`artifact.rs:21-72`) admit it unchanged.

`.cufflink/` is reserved at the root of a bundle: a project that has its own
`.cufflink` directory under `public/` is unaffected (that is
`public/.cufflink`), and the writer never takes a project file at the root.

**Recognising a bundle.** `cufflink deploy --bundle` opens the gzip stream
and looks at the first bytes inside: `{` is a WASM bundle (format 1 of
`bundle.rs`), a tar header (`ustar` at offset 257) is a web bundle. Anything
else is `it is not a bundle (…)`, as today. A CLI that predates this note
reads a web bundle as `it is not a bundle (expected value at line 1
column 1)`, which names no newer format; the release notes say so (section
12).

### 2.2 The writer, and what makes its bytes reproducible

The archive is written in process, with the `tar` crate the web runner
already uses to read it, by a new module `cli/src/web_bundle.rs`. The host's
`tar` is no longer run.

- **Order.** Entries are written depth-first with each directory's entries
  sorted by their names' bytes, after `.cufflink/bundle.json`. A
  directory's entry precedes its contents.
- **Headers.** GNU format headers (long names need them). Owner and group
  ids 0, owner and group names empty. Directories and files with any
  execute bit are `0755`, every other file `0644`; the writer never writes a
  set-id or sticky bit.
- **Time.** Every entry carries the same modification time: the
  `SOURCE_DATE_EPOCH` variable when it is set (the reproducible-builds
  convention), and otherwise the time the build started, recorded as
  `mtime`. A fixed `0` would be simpler and is rejected: Next.js serves
  `public/` with `Last-Modified` and a weak ETag made of the file's size and
  modification time, so with one time for every release a changed file of
  the same size would keep its ETag and browsers would keep the old one.
  One time per build gives every release new validators, and
  `SOURCE_DATE_EPOCH` gives a pipeline identical bytes from identical trees.
- **Compression.** `GzBuilder::new().mtime(0)`, no file name, the default
  level, as `bundle.rs` writes a WASM bundle.
- **Links.** Every symbolic link is followed, as `tar h` does today, so the
  archive holds only directories and regular files and the runner's link
  rules never fire on a bundle the CLI wrote. Preserving links would make
  pnpm's layouts smaller, but would also change how Node resolves modules
  (`--preserve-symlinks` behaviour against real paths) for every app at
  once; it is left out (section 13). A link that leads back into a
  directory it is under is refused: `node_modules/x links to an ancestor of
  itself`. A link to nothing is refused with its path.
- **Other files.** A socket, FIFO or device file is refused with its path.
- **Writing.** The archive is written to `<file>.<pid>.partial` beside the
  destination and renamed into place, as `bundle.rs` does, so a failed build
  leaves no file and a file that was there stays as it was. The SHA-256 and
  size are computed while writing, not by reading the file again.

Whether two runs of `next build` on one tree give one tree is Next.js's
business (its build id, its manifests), not the bundle's: the writer gives
one archive from one tree and one `mtime`, and the hash names the bytes of
the build that made them. That is what #61 needs. A release process ships
the bytes it verified because it ships the file, not because a rebuild would
reproduce it.

The writer counts the entries and the bytes they extract to while it writes
and prints both. It does not refuse past the runner's caps, which each
runner sets for itself; the runner's refusal names its setting
(`WEB_ARTIFACT_MAX_ENTRIES`, `WEB_ARTIFACT_MAX_BYTES`).

### 2.3 What a bundle holds (#67)

The roots are those of today's archive: `.next`, `node_modules`,
`package.json` and, when there is one, `public`. Paths are excluded by one
constant, anchored at the archive's root:

```rust
pub(crate) const EXCLUDED: [&str; 2] = [".next/cache", "node_modules/.cache"];

fn excluded(path: &Path) -> bool {
    EXCLUDED.iter().any(|excluded| path == Path::new(excluded))
}
```

An excluded directory is not descended into. The rule is the writer's own,
so it holds on every host, and anchoring keeps it from touching a package
that ships a directory called `cache` deeper in `node_modules`.
`.next/cache` holds the build's incremental and image caches, which `next
start` rebuilds as needed. `node_modules/.cache` holds bundler plugins'
caches, which nothing reads at run time. `.next/trace`, the build's trace
file, is kept for now (question 5).

`node_modules` comes from where it comes from today, with one change: when
`pnpm deploy --prod` fails in a workspace, `cufflink build --bundle` refuses
instead of packing the nearest `node_modules`:

```
Cannot build the bundle out/site.bundle: `pnpm --filter site deploy --prod` failed, and a
bundle is built only from a pruned node_modules (the nearest one carries development
dependencies and links into this workspace):
<pnpm's output>
```

A deploy from source keeps today's fallback and records `"node_modules":
"nearest"`, so the deployment history shows such a version (question 4). The
temporary directory `pnpm deploy` writes is a `tempfile::TempDir` held until
the archive is written and dropped after, on every path; today's code keeps
it (`tempdir()?.keep()`) and removes it only when the archive was written.

### 2.4 Build-time values

The obstacle to building once is not the archive. A Next.js build inlines
`NEXT_PUBLIC_*` values into the browser's code and may read any other value
the build is given, so a build made with staging's values is a staging build.
The design keeps that true and makes it visible, instead of pretending that
any bundle can go anywhere.

**Which values a build is given.** Today, every value the platform holds for
the service and every value `Cufflink.toml` declares for the environment,
vault secrets included. A new, optional key narrows it:

```toml
[web]
build_env = ["NEXT_PUBLIC_API_URL", "NEXT_PUBLIC_SITE_NAME"]
```

- Absent, a build is given every value, as today.
- Present, a build is given exactly those names. A name with no value is
  refused before anything is built: `[web].build_env lists
  NEXT_PUBLIC_SITE_NAME, and neither [environments.production] in
  Cufflink.toml nor the platform's configs for site have it`. An empty list
  gives the build no values, which makes every bundle portable.
- A value outside the list still reaches the app at run time: the runner
  passes every config to the process when it starts it, as it does today.

**What a bundle records.** The environment's name, the sorted names, and one
digest of names and values together:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BuildEnv {
    pub environment: Option<String>,
    pub names: BTreeSet<String>,
    pub digest: String,
}

impl BuildEnv {
    pub(crate) fn of(environment: Option<String>, values: &BTreeMap<String, String>) -> Self {
        let framed = values.iter().fold(Vec::new(), |mut framed, (name, value)| {
            [name.as_bytes(), value.as_bytes()].iter().for_each(|part| {
                framed.extend_from_slice(&(part.len() as u64).to_be_bytes());
                framed.extend_from_slice(part);
            });
            framed
        });
        Self {
            environment,
            names: values.keys().cloned().collect(),
            digest: crate::digest::sha256_hex(&framed),
        }
    }

    pub(crate) fn mismatch(&self, target: &Self) -> Option<Mismatch> {
        (self.digest != target.digest).then(|| Mismatch {
            only_target: target.names.difference(&self.names).cloned().collect(),
            only_bundle: self.names.difference(&target.names).cloned().collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Mismatch {
    pub only_target: Vec<String>,
    pub only_bundle: Vec<String>,
}
```

A mismatch with both lists empty has the same names and other values.

No value is recorded, and no value is hashed alone: one digest covers every
value, so a bundle never carries a per-secret hash that could be tested a
guess at a time. Whoever holds a bundle holds whatever the build compiled
into it already; the digest adds a test for the values the build did not
use, which `[web].build_env` removes, and the documentation says to keep
secrets out of build-time values altogether.

**Where the values come from at build time.** `cufflink build --bundle`
sends nothing, as for a WASM bundle: it is given the values `Cufflink.toml`
declares for the environment (`--env`, else `default_env`), vault secrets
included, narrowed by `[web].build_env`. It does not read the platform's
configs (question 1). A deploy from source reads them, as today, and builds
with the platform's values under the local ones.

**What a deploy compares.** Before it uploads, a deploy works out the values
the target would build with, by the same function a deploy from source uses
(the platform's configs for the service, the local values laid over them,
narrowed by `[web].build_env`), and compares that `BuildEnv` with the
bundle's. Names that differ are named. When the names are the same, the
values differ, and the refusal says so without saying which, since a bundle
records one digest:

```
Cannot deploy the bundle out/site.bundle: it was built with other build-time values than
production would build it with:
  NEXT_PUBLIC_FLAGS is set for production and was not when the bundle was built
Build it for production (`cufflink build --bundle … --env production`), or give
production the values it was built with; values only the running app needs belong out of
[web].build_env
```

or, with the same names, `  NEXT_PUBLIC_API_URL, NEXT_PUBLIC_SITE_NAME are
set for both, and at least one of their values differs`.

There is no flag that deploys past the comparison (question 2). A bundle is
for every environment whose build-time values are its own; an app that
wants one bundle for staging and production reads what differs at run time
and lists nothing else in `[web].build_env`.

**Reading the platform's configs.** `fetch_service_configs` stops answering
an empty list on a failure. A service the platform does not have has no
configs (an empty list); any other failure stops the build or the deploy
before anything is built or sent: `Cannot read site's configs on staging
(…): the build would be given only the values Cufflink.toml declares`.

**The shell's environment.** The build still inherits the shell's
environment, which nothing can record. The CLI warns for each inherited
variable named `NEXT_PUBLIC_*` that is not among the values it gives the
build: `warning: NEXT_PUBLIC_DEBUG is set in this shell and is not one of
the build's values; the bundle's record does not cover it`.

### 2.5 The checks (#66)

**Default.** Before every web build, from source or into a bundle, the CLI
runs the package's `typecheck`, `lint` and `test` scripts, each when
`package.json` declares it, as today. A failing script stops the build and
nothing is sent or written.

**The flag.** `--skip-checks`, on `cufflink deploy` (a web app from source)
and on `cufflink build --bundle` (a web app). Nothing else turns the checks
off: no environment variable, no `Cufflink.toml` key, no detection of CI,
and no platform setting. The CLI prints, on stderr, before it builds:

```
warning: --skip-checks: typecheck, lint and test are not run; the deploy records that they
were skipped
```

**Where the flag is refused.**

| Given | Refusal |
|---|---|
| `deploy --bundle <file> --skip-checks` | usage error (exit 2), from clap: the flag conflicts with `--bundle`. A bundle's checks ran or were skipped when it was built, and it records which |
| `--skip-checks` in a project that is not a web app | `--skip-checks applies to web apps, which run their package's typecheck, lint and test scripts before they build; wasm services run none` (exit 1, before anything is built) |
| `--skip-checks` against a platform that does not advertise `deploy_checks` | `--skip-checks needs a platform that records skipped checks ('deploy_checks'); platform … does not. Upgrade the platform or deploy without the flag.` (exit 1, before anything is built or sent) |
| a bundle built with `--skip-checks`, deployed to a platform without `deploy_checks` | `Cannot deploy the bundle out/site.bundle: its checks were skipped, and platform … cannot record that ('deploy_checks')` |

The last two keep the owner's rule that a skip is always recorded: a skip
never reaches a platform that would drop the record.

**What is recorded.** The bundle's `checks`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Script { Typecheck, Lint, Test }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checks {
    pub skipped: bool,
    pub scripts: BTreeSet<Script>,
}
```

`scripts` is what the package declares. With `skipped: false` each of them
ran and passed; with `skipped: true` none ran. A package that declares none
records `{"skipped": false, "scripts": []}`, which the history shows as
`none`, not as `ran`. The type lives in `cufflink-types` so the CLI and the
platform read one shape.

The platform takes the record from the bundle's first entry when the bundle
is uploaded (section 2.6), never from a separate request field, so a bundle
and its record cannot be paired otherwise. A version deployed through the
old upload route, or by a CLI that predates this note, has no record and is
shown as not reported, never as `ran`.

**What the record is.** The checks run on the deployer's machine, and the
record is the deployer's statement about them. The platform cannot verify
that a script ran, or that it is the script the repository holds; a
modified CLI can skip the checks and write `skipped: false`. The record and
the policy (section 5) govern the honest path: they make a skip visible and
let a tenant restrict who may ask for one. They are not a security boundary,
and the documentation says so in those words.

### 2.6 The platform stores bundles by content

**Where.** `web/{tenant_id}/bundles/{sha256}.tar.gz`, under the reserved
`web/` prefix. The key names content, so an object under it is never
replaced, and it names a tenant rather than a service because a bundle is
uploaded before its version is registered (#63), when a new service has no
id yet. The bundle's own `service` binds it to one service of that tenant.

**What the platform knows of a bundle.** A new table, in the platform
migration `V9__web_bundles.sql`:

```sql
CREATE TABLE web_bundles (
    tenant_id   uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    sha256      text        NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    service     text        NOT NULL,
    size_bytes  bigint      NOT NULL CHECK (size_bytes > 0),
    s3_key      text        NOT NULL,
    meta        jsonb       NOT NULL,
    uploaded_by text        NOT NULL,
    uploaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, sha256)
);
```

`meta` is the bundle's first entry less its `manifest`, so `checks` and
`build_env` are read where versions are listed. A version keeps naming its
object as today, by `artifact_s3_key`, `artifact_hash` and `artifact_size`
on its row; the listing joins `web_bundles` on `(tenant_id, artifact_hash)`.

**Upload.** `PUT /api/web/bundles/{sha256}` with the bundle as the body
(`application/gzip`), under `DeployAuth`, inside the artifact body limit:

1. The path's hash must be 64 lower-case hex digits (`is_sha256`), else 400.
2. The body's SHA-256 must be the path's, else 422: `the body's SHA-256 is
   <found>, and the path names <given>`.
3. The first entry must be `.cufflink/bundle.json`, at most 64 KiB, a format
   the platform reads, of mode `web`, naming a valid service name, with a
   well-formed `checks`. Otherwise 422 naming what is wrong (`it is web
   bundle format 2, and this platform reads format 1`, `its first entry is
   .next/BUILD_ID, not .cufflink/bundle.json`).
4. Under `deploy_checks: enforce`, a bundle whose checks were skipped is
   refused (403) before it is stored when the uploading credential may not
   skip (section 5), so a refused deploy leaves no object behind.
5. The object is stored and the row inserted (`ON CONFLICT DO NOTHING`).
   A bundle already held is not stored again, and the answer is the same:
   `200 {"sha256", "size_bytes", "service", "stored": false}`; a new one is
   `201` with `"stored": true`.

The body is taken as `Bytes` and handed to storage as `Vec::from(body)`,
without today's second copy. The platform reads only the first entry; it
does not walk the archive, which the CLI did before it sent it and the runner
does when it unpacks it.

**Lookup.** `HEAD /api/web/bundles/{sha256}` answers 200 with `Content-Length`
the bundle's size when the tenant holds it, else 404. The CLI asks before it
uploads, which is what makes a second deploy of a file seconds long.

**Deploy by hash.** `POST /api/services/deploy` takes a new field,
`web_bundle: "<sha256>"`, for a web manifest only (422 for any other mode).
The platform refuses, before it claims the deploy lock:

- a hash the tenant holds no bundle under: 409 `no web bundle <sha256> is
  uploaded for this tenant; upload it first (PUT /api/web/bundles/<sha256>)`;
- a bundle of another service: 422 `web bundle <sha256> is site-admin's, and
  this deploy is of site`;
- a bundle whose `manifest` is not the request's: 422 `web bundle <sha256>
  was built with another manifest than this deploy sends`.

What happens next is #63's: the version is registered and activated with the
bundle's key, hash and size in one transaction, a deploy whose manifest and
bundle hash both equal the running version's is skipped, and
`deploy.web.<tenant>.<service>` is published once the version is active
(section 8 lists what this note relies on).

**Rollback.** A rollback of a web app copies the target version's
`artifact_s3_key`, `artifact_hash` and `artifact_size` to the new version and
publishes `deploy.web` (#62). With objects named by content this is a copy of
three columns; no object is copied.

**Deleting.** Every path that removes a version or a service deletes an
object only when no version row of any service still names it
(`records_artifact`, `deployments.rs:374-386`), and removes its
`web_bundles` row with it:

- deleting a version (`delete_by_version` returns the web key as well as the
  native one, which also ends the leak in section 1.3);
- pruning versions (the same path);
- deleting a service (today's loop over `artifact_keys`, now guarded);
- deleting a tenant (the cascade removes the rows; the tenant's prefix
  `web/{tenant_id}/` is deleted with `delete_prefix`).

A bundle uploaded and never deployed is deleted at the tenant's next upload
once it is more than 24 hours old and no version names it: one query over
`web_bundles` for that tenant, then a delete per object. No background job
is added.

### 2.7 Config travels with the version

Today the CLI registers a version, applies config, builds and uploads, so a
web app's process starts after its config was applied. Once a version is
registered only after its bundle exists (#63), a new service's first version
would be active, and its process started, before the CLI could apply its
config (the config routes take a service id that does not exist until the
deploy creates it).

The deploy request therefore takes the environment's config with it, for a
web manifest with `web_bundle`: a field `config` holding exactly the body of
`PUT /api/services/{id}/config/apply` (`ApplyConfigRequest`,
`platform/src/api/config.rs:42-48`) with `prune: false` and `dry_run:
false`. The platform applies it in the transaction that activates the
version, by the code the apply route runs, so the runner never sees the new
version without its config or the new config with the old version. The
deploy's answer carries the apply's diff, printed as `  Applied config: …`
as today. The deploy job's stored `request` (`Admitted::request`,
`deployments.rs:183-196`) never holds `config`: it holds the names it
applied.

This is the one place where this note and #63's touch the same code; if
#63's note settles the order another way that keeps the invariant in section
8, this field goes.

### 2.8 What the deploy record shows

- `GET /api/services/{id}/deployments` adds to each web version `"bundle":
  {"checks": …, "build_env": {"environment", "names", "digest"},
  "node_modules": …, "commit_hash": …}` from `web_bundles.meta`, or `null`
  for a version uploaded the old way.
- `cufflink deployments list` adds a column `CHECKS`: `ran`, `none`,
  `SKIPPED`, or `-` (not reported, or not a web app), and a column `BUNDLE`
  with the short hash.
- The deploy's own output adds `  Checks:  typecheck, lint, test passed` or
  `  Checks:  skipped (--skip-checks)`.
- The audit entry `deploy` adds `checks` and `bundle` to its details, beside
  the credential that `audit::record` writes. A skipped deploy that carries
  `web_bundle` writes an audit entry `deploy.skipped` with the same details,
  so a skip of the checks is recorded even when the bytes were already
  running.

## 3. CLI

### 3.1 `cufflink build`

```
cufflink build --bundle <FILE> [--commit-hash <SHA>] [--skip-checks] [--env <ENV>]
```

Help text:

```
--bundle <FILE>     Write the service as a bundle: one file `cufflink deploy --bundle`
                    deploys. For WASM and CRUD services and web apps; not for native
                    services. A web app's bundle is built with the values Cufflink.toml
                    declares for --env (or default_env), and is refused by a deploy to an
                    environment whose build-time values differ
--commit-hash <SHA> Git commit hash the bundle is of (auto-detected if omitted)
--skip-checks       Web apps only: do not run the package's typecheck, lint and test
                    scripts before the build. The bundle, and every deploy of it, records
                    that they were skipped. Nothing else skips them
```

A web build prints the checks and build lines it prints today, then:

```
Wrote out/site.bundle (50331648 bytes)
  Service:   site (mode: web)
  Commit:    4f2a9c1d8e…
  Build env: staging (NEXT_PUBLIC_API_URL, NEXT_PUBLIC_SITE_NAME; digest 1c9e…)
  Checks:    typecheck, lint, test passed
  Entries:   18204 (extracts to 212418560 bytes)
  SHA-256:   9e8d7c6b5a…
```

`bundled` and `bundle_of_sources` admit `ServiceMode::Web`; the refusal of a
native or container service is unchanged.

### 3.2 `cufflink deploy`

```
--skip-checks   Web apps only: do not run the package's typecheck, lint and test
                scripts before the build. The deploy records that they were skipped.
                Not with --bundle, whose checks were run or skipped when it was built
```

A web deploy goes through the same `Destination::reached` as a WASM deploy:
it asks the platform's version and features, prints the negotiated line,
sends `cli_version` and `api_version`, and checks every flag against the
features before anything is built.

From source, against a platform with `web_bundles`:

1. Read the project, the vault and the platform's configs for the service
   (section 2.4); refuse on a failure.
2. Run the checks, unless `--skip-checks`.
3. Build, write the bundle to a temporary directory.
4. `HEAD` the bundle; `PUT` it when the platform does not hold it.
5. `POST /api/services/deploy` with `web_bundle` and `config`.

With `--bundle <file>`:

1. Read the whole file: a whole gzip stream with nothing after it, a whole
   tar archive, a first entry the CLI reads, as a WASM bundle is read; the
   refusals are those of the WASM table in `docs/deployment.md` with
   `web bundle` in place of `bundle`. Compute the file's SHA-256.
2. Refuse a bundle of another service than `Cufflink.toml`'s `[service].name`
   (`foreign_project`, as for a WASM bundle).
3. Compare build-time values (section 2.4).
4. Steps 4 and 5 above.

```
Deploying site (mode: web) from the bundle out/site.bundle (built by cufflink v0.38.0)...
  Commit:    4f2a9c1d8e…
  Build env: staging, as production would build it
  Checks:    typecheck, lint, test passed
  SHA-256:   9e8d7c6b5a…
  platform v0.38.0 (features …, web_bundles, deploy_checks)
  The platform already holds this bundle
```

### 3.3 Code shape

- `cli/src/web_bundle.rs`: `Meta`, `BuildEnv`, `Checks` (from
  `cufflink-types`), `Roots`, `NodeModules`, the writer (`fn write(roots:
  &Roots, meta: &Meta, path: &Path) -> eyre::Result<Written>`), the reader
  (`fn read(path: &Path) -> eyre::Result<(Meta, crate::bundle::File)>`) and
  `fn first_entry(bytes: &[u8]) -> eyre::Result<Meta>`.
- `cli/src/bundle.rs` gains `enum Read { Wasm(Bundle), Web(web_bundle::Meta) }`
  decided by the first bytes inside the gzip stream; `run_bundle` matches on
  it.
- `deploy_web` becomes three functions with one job each: `web_bundle_of_sources`
  (checks, build env, build, write), `uploaded` (HEAD, PUT) and `deployed`
  (the request). `cufflink build --bundle` calls the first; a deploy from
  source calls all three; `deploy --bundle` calls the last two.
- `run_npm_script`, `detect_package_manager` and `format_command_output`
  are kept. `create_web_tarball` and `web_tarball_args` are removed.

## 4. API

| Method | Path | New | Body / answer |
|---|---|---|---|
| `HEAD` | `/api/web/bundles/{sha256}` | yes | 200 with `Content-Length`, or 404 |
| `PUT` | `/api/web/bundles/{sha256}` | yes | the bundle; 201 or 200 `{"sha256", "size_bytes", "service", "stored"}`; 400, 403, 413, 422 as in section 2.6 |
| `POST` | `/api/services/deploy` | fields | `web_bundle` (sha256), `config` (`ApplyConfigRequest`), web manifests only |
| `GET` | `/api/services/{id}/deployments` | field | `bundle` per version |
| `POST` | `/api/services/{id}/web` | unchanged | kept for CLIs that predate `web_bundles`: stores under the version's key as today, records no `meta`. Deprecated in the release notes |
| `GET` | `/api/services/{id}/web/status` | field | `bundle` as in the listing |

Both new routes are under the control routes' artifact body limit and its
concurrency limit, as the old upload is.

## 5. The `deploy_checks` policy

A new policy key, in the shape of every other (`docs/policies.md`):

| Key | Default | Config (default) | What it governs |
|---|---|---|---|
| `deploy_checks` | `off` | `{"skip_allowed_keys": []}` | Which credentials may deploy a web bundle whose checks were skipped. `off`: any. `warn`: any; a skip by a credential not listed is recorded (`skip_by_unlisted_key`, or `skip_by_token` for a token, which has no key id). `enforce`: refused with 403 at upload and at deploy |

```rust
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeployChecksConfig {
    #[serde(default)]
    pub skip_allowed_keys: BTreeSet<Uuid>,
}

pub(crate) fn skip_violation(
    checks: Option<&Checks>,
    credential: &Credential,
    config: &DeployChecksConfig,
) -> Option<&'static str> {
    checks.filter(|checks| checks.skipped).and_then(|_| match credential {
        Credential::Key { id, .. } if config.skip_allowed_keys.contains(id) => None,
        Credential::Key { .. } => Some("skip_by_unlisted_key"),
        Credential::Token => Some("skip_by_token"),
    })
}
```

- Keys are named by id, because `cufflink login` gives every developer an API
  key too: the id, from `cufflink keys list`, is what tells a pipeline's key
  from a developer's.
- A version with no record (an old CLI) is never a violation: such a CLI
  always ran the checks before it built.
- `enforce` with an empty list is valid: nobody may skip.
- The refusal: `the deploy_checks policy of tenant harbor does not let key
  cli-dev1 (7b1e…) deploy a bundle whose checks were skipped; a platform
  admin lists the keys that may in skip_allowed_keys`.
- The policy is evaluated with the uploading credential at upload and with
  the deploying credential at deploy, so a bundle uploaded by a key that may
  skip is still refused to one that may not.

```sh
cufflink tenants policy set harbor deploy_checks enforce \
  --config '{"skip_allowed_keys":["3f6c0d2a-6a1b-4f0e-9a51-0c9e2f8b7d14"]}'
```

## 6. Features and compatibility

Two platform features, one per platform PR:

| Feature | Provides |
|---|---|
| `web_bundles` | `HEAD`/`PUT /api/web/bundles/{sha256}`, `web_bundle` and `config` on a web deploy, versions keyed by bundle (#63), rollback carrying the bundle (#62), the `bundle` field of the listing |
| `deploy_checks` | `checks` read from a bundle and recorded, shown in the listing and audit log; the `deploy_checks` policy |

`deploy_checks` is advertised only with `web_bundles`. The feature tables in
`docs/versions.md` change in the PR that adds each feature, since a platform
test compares them with `supports`.

| CLI | Platform | What happens |
|---|---|---|
| new | `web_bundles` and `deploy_checks` | Sections 2 and 3 |
| new | `web_bundles` only | As above; `--skip-checks`, and a bundle whose checks were skipped, are refused (section 2.5); the listing shows no checks |
| new | neither | A deploy from source, or of a bundle whose checks ran, goes the old way: register, apply config, upload the same bytes with `POST /api/services/{id}/web`. The CLI prints `platform … predates web_bundles: registering, then uploading`. The hash the platform records is the bundle's, since the bytes are. `--skip-checks` is refused |
| older than this | any | Unchanged: the old flow, the old route, no record (`-` in the listing). Its archive is made by the host's `tar` |
| older than this | any, given a web bundle | `deploy --bundle <web bundle>`: `it is not a bundle (…)`. Build it again with the newer CLI, or deploy from source |

**Rolling a platform back** below the release that adds `web_bundles`:

| What it added | Before rolling the platform back |
|---|---|
| Versions whose objects are under `web/{tenant_id}/bundles/` | Nothing: the older runner and runner API read the key from the version's row, whatever it is |
| `web_bundles` rows and the `deploy_checks` policy | Nothing: the older platform reads neither. A tenant's `deploy_checks` row stays and applies again after the next upgrade |
| Deleting a service on the older platform | Its loop deletes every key the service's rows name, so a bundle object is deleted with its service as before |
| The database | `V9__web_bundles.sql` adds a table only; the older platform ignores it |

## 7. Failure handling

| Failure | What is left | What the operator sees |
|---|---|---|
| A check fails | Nothing written or sent | `Linting failed:` and the script's output (unchanged) |
| The build fails | No bundle file; a file that was there stays | `Build failed:` and its output (unchanged) |
| The platform's configs cannot be read | Nothing built or sent | `Cannot read site's configs on staging (…)` |
| A `[web].build_env` name has no value | Nothing built | Section 2.4 |
| `pnpm deploy` fails, building a bundle | No bundle | Section 2.3 |
| A link loops, dangles or is special | No bundle | The path and why |
| The build-time values differ from the target's | Nothing sent | Section 2.4 |
| The upload is cut off or refused | Nothing registered; at most a stored object no version names, deleted after 24 h | The status and the platform's words; running the deploy again uploads again |
| The upload is too large (413) | Nothing stored | `the platform takes bundles up to its artifact body limit; this one is 312 MiB` |
| The upload succeeds and the deploy request fails | A stored bundle no version names, deleted after 24 h unless deployed | The deploy's refusal; running it again finds the bundle (`HEAD` 200) and sends only the request |
| The bundle went between `HEAD` and the deploy (409) | Nothing registered | The CLI uploads once more and sends the request again; a second 409 is the error |
| The policy refuses a skip | Nothing stored or registered | Section 5 |
| The runner cannot unpack the bundle | The running app stays as it was (unchanged) | The runner's log names the entry and the cap |

A bundle the platform holds is never replaced or changed, so no failure at
any step alters what a running version serves.

## 8. How this fits the web deploy lifecycle design

The lifecycle design (#62 rollback that switches, #63 transactional deploys
and versions by content, #64 draining the replaced process) and this one are
written in parallel. This note assumes #63 makes a web version's identity its
manifest and its bundle's hash, and registers a version only once its bundle
is stored. The two meet in the deploy request and in storage, and both hold
these invariants:

1. A bundle is stored, under a key that names its content, before any version
   names it. An object under such a key is never replaced.
2. A web deploy names its bundle by SHA-256, and that hash is the uploaded
   file's, the row's `artifact_hash` and the runner's directory name.
3. A deploy whose manifest and bundle hash equal the running version's is
   skipped; any other registers a new version, in one transaction with its
   bundle columns and its config.
4. A rollback carries the target version's bundle columns.
5. The runner starts a version only with the config its deploy applied.
6. A bundle's metadata is read by the platform at upload and only there.

This note owns the bundle format, the writer, the build-time values, the
checks and their record, the upload and lookup routes, the policy, and the
`web_bundle` and `config` fields. #63's note owns registration, the skip
rule and when `deploy.web` is published; #62's the rollback; #64's the
runner's swap. If #63's note lands first with its own upload route or field
names that hold the invariants, this note takes them and drops its own; the
feature name `web_bundles` stands for whichever ships them.

## 9. Alternatives considered

### 9.1 A wrapper file around the archive

A tar holding `bundle.json` and `app.tar.gz`, or gzip JSON with the archive
in base64, as a WASM bundle carries its module. Either makes the uploaded
bytes differ from the file, so the hash the build prints is not the hash the
platform and runner use, and base64 adds a third to a file that can be
hundreds of megabytes. Rejected.

### 9.2 One bundle for every environment, values injected at start

Leaving every value out of the build and having the runner inject them would
make every bundle portable, but `NEXT_PUBLIC_*` values are compiled into
the browser's code by Next.js, not read at start. Making it work needs the
app's cooperation; `[web].build_env = []` is how an app that cooperates says
so, and the comparison keeps the others honest. Rejected as the only model.

### 9.3 Per-value digests

They would let a refusal name each differing value, at the cost of one hash
per value, secrets included, each testable a guess at a time. Rejected for
one digest and named names.

### 9.4 The checks attested by the platform

Running the scripts on the platform would make the record verifiable, but the
platform does not run tenants' builds and has no Node toolchain. The record
stays a statement by the deployer, documented as such.

### 9.5 `--skip-checks` read from `CI=true` or a config key

Contrary to the owner's rule that nothing skips the checks implicitly.
Rejected.

### 9.6 A background sweeper for unused uploads

A job would delete them on time rather than at the next upload, at the cost
of a scheduled task per platform. At the expected volume, one deploy at a
time per service, the next upload comes soon enough. Rejected for now.

## 10. Tests

Named as the repository names its tests: a sentence of what is proved.

**CLI, `cli/src/web_bundle.rs`** (unit, temporary directories, no Node):

- `a_bundle_never_holds_the_build_or_bundler_caches`: a tree with
  `.next/cache/images/x`, `node_modules/.cache/babel/y` and
  `node_modules/pkg/cache/z`; the archive read back with `tar::Archive`
  lacks the first two and holds the third.
- `the_first_entry_is_the_bundle_record_and_is_read_alone`: the record is
  entry 0 and `first_entry` reads it from a prefix of the file.
- `two_writes_of_one_tree_and_one_time_are_byte_identical`: same tree,
  same `mtime`, two files, one SHA-256; the gzip header's time is zero.
- `entries_are_sorted_and_carry_no_owner_and_one_time`: order, ids, names,
  `mtime`, modes `0755`/`0644`.
- `a_link_is_followed_and_a_loop_or_dangling_link_is_refused`.
- `a_socket_or_fifo_is_refused_with_its_path` (Unix).
- `a_failed_write_leaves_no_file_and_keeps_the_one_there`.
- `a_build_env_digest_covers_names_and_values_and_records_no_value`: the
  serialised `BuildEnv` contains no value; changing a value or moving bytes
  between a name and a value changes the digest.
- `a_mismatch_names_the_names_and_says_the_values_differ`.
- `the_written_hash_is_the_hash_of_the_file`.

**CLI, `cli/src/bundle.rs` and `deploy.rs`**:

- `a_bundle_is_recognised_by_what_its_gzip_stream_holds`: JSON is WASM, a
  tar header is web, anything else is `it is not a bundle`.
- `a_web_bundle_that_is_truncated_or_has_trailing_bytes_is_refused`.
- `a_web_bundle_of_another_service_is_refused_before_anything_is_sent`.
- `skip_checks_conflicts_with_bundle` (clap, exit 2).
- `skip_checks_is_refused_outside_a_web_app`.
- `skip_checks_is_refused_by_a_platform_that_cannot_record_it` and
  `a_bundle_whose_checks_were_skipped_is_refused_by_such_a_platform`.
- `no_variable_or_setting_skips_the_checks`: with `CI=true` and every
  `CUFFLINK_*` variable set, a build without the flag runs the scripts (a
  package whose scripts write marker files).
- `a_failed_config_read_stops_the_build` and
  `a_service_the_platform_lacks_has_no_configs`, against the stub platform
  (`cli/src/commands/stub.rs`).
- `a_bundle_the_platform_holds_is_not_uploaded_again`: stub answers `HEAD`
  200; no `PUT` is made.
- `a_missing_bundle_is_uploaded_once_more_after_a_409`.
- `an_older_platform_is_deployed_the_old_way_with_the_same_bytes`: stub
  without `web_bundles`; the body of `POST /api/services/{id}/web` is the
  bundle file.
- `a_web_deploy_negotiates_like_any_other`: the request carries
  `cli_version` and `api_version`.

**Platform** (integration, own Postgres and storage, invented tenants `acme`
and `globex`, service `site`):

- `an_upload_is_stored_once_by_its_hash_and_answered_the_same_again`.
- `an_upload_whose_body_is_not_its_hash_is_refused`.
- `an_upload_without_a_bundle_record_first_is_refused`.
- `a_deploy_names_only_a_bundle_its_tenant_uploaded`: `globex` cannot
  deploy `acme`'s hash (409, as if absent).
- `a_bundle_of_another_service_or_manifest_is_refused`.
- `a_deploy_of_the_running_bundle_is_skipped_and_audited` (with #63).
- `a_rollback_serves_the_earlier_bundle` (with #62).
- `config_and_version_change_together`: a deploy whose config apply fails
  activates nothing.
- `an_object_is_deleted_only_when_no_version_names_it`: version delete,
  prune, service delete.
- `an_unused_upload_is_deleted_after_a_day_at_the_next_upload`.
- `the_listing_shows_the_checks_of_each_version_and_none_for_old_uploads`.
- `deploy_checks_refuses_a_skip_by_an_unlisted_key_only_when_enforced`:
  `off`, `warn` (counter and log, allowed), `enforce` (403 at upload and at
  deploy), a listed key allowed, a token refused.
- `a_version_with_no_record_is_never_a_violation`.
- `the_deploy_checks_policy_config_refuses_unknown_fields`.
- `bundle_keys_are_reserved_from_guests`: `storage_scope`'s reserved-key
  test gains the new key.
- `supports_matches_the_feature_tables` (existing) passes with both features.

**Web runner** (existing harness):

- `a_bundle_with_its_record_unpacks_and_starts`: the `.cufflink/` entry is
  admitted and the app starts.

## 11. PR plan

Each PR passes `cargo fmt`, `cargo clippy --all-targets -- -D warnings` and
`cargo test --workspace` on its own, and changes the documentation of what it
changes.

1. **CLI: the bundle writer (#67).** `cli/src/web_bundle.rs` writer and its
   tests; a deploy from source packs with it instead of `tar`; the exclusion
   constant; the `TempDir` fix. No format record yet, no API change; the
   uploaded archive is otherwise what it was.
2. **Types and platform: `web_bundles`.** `Checks`, `Script` and the record
   type in `cufflink-types`; `V9__web_bundles.sql`; the two routes; the
   `web_bundle` and `config` fields; guarded deletes and the sweep; the
   listing field; the feature and its `versions.md` row. Written with #63's
   registration, or after it.
3. **Platform: `deploy_checks`.** The record in the listing, the audit
   details, `deploy.skipped`, the policy key, its validation and metric; the
   feature and its row.
4. **CLI: web bundles.** The record entry, `BuildEnv`, `[web].build_env`,
   the failing config read, `cufflink build --bundle` for web apps,
   `deploy --bundle` of a web bundle, the deploy from source through upload
   and deploy by hash, the fallback for older platforms, `Destination` for
   web deploys, the new list columns.
5. **CLI: `--skip-checks` (#66).** The flag on both commands, its
   refusals, the warning, the record.
6. **Docs.** `docs/deployment.md` (bundles for web apps, build-time values,
   the checks, the record), `docs/policies.md` (`deploy_checks`), release
   notes.

PRs 2 and 3 can be reviewed together with 4 and 5 against a local platform;
1 stands alone.

## 12. Documentation and release notes

`docs/deployment.md` gains, under "Bundles", a subsection for web apps: the
format, what it holds and leaves out, build-time values and the comparison,
the checks and the flag, the record, and the refusals table. The sentence
"`cufflink build --bundle` refuses … a web app" goes. `docs/policies.md`
gains the key's row. `docs/versions.md` gains:

- **Features added in v0.38.0:** `web_bundles`, `deploy_checks`.
- **Changes in v0.38.0:**
  - Web apps build into bundles: `cufflink build --bundle` and `cufflink
    deploy --bundle`; the build's SHA-256 is the deployed hash; a held
    bundle is not uploaded again.
  - A bundle records the names and a digest of its build-time values, and a
    deploy to an environment whose values differ is refused. New
    `[web].build_env`.
  - A failure to read the platform's configs now stops a web build.
  - `--skip-checks` on `deploy` and `build`; checks still run by default;
    every version records whether they ran; the `deploy_checks` policy.
  - Bundles never hold `.next/cache` or `node_modules/.cache`, on any host;
    archives are written by the CLI, not the host's `tar`, with one order,
    no owners and one time per build (`SOURCE_DATE_EPOCH` when set).
  - Web objects are stored by content under `web/{tenant}/bundles/`;
    deleted and pruned web versions' objects are deleted.
  - `POST /api/services/{id}/web` is deprecated; CLIs before v0.38.0 use it.
  - A web bundle cannot be deployed by a CLI before v0.38.0 (`it is not a
    bundle`).
- **Rolling a platform back from v0.38.0:** the table in section 6.

## 13. Not in this change

- Preserving symbolic links in bundles, which would shrink pnpm layouts.
- `--skip-checks` for `cufflink workspace deploy` and `deploy-package`.
- `--plan`, `--allow-destructive` and the other flags web deploys ignore
  today; they stay ignored.
- Signing bundles. The hash names the bytes; who built them is the deploy
  key's to say.
- Deploying a bundle the platform holds by its hash alone, without the
  file. Within one platform that is a rollback (#62).
- The process swap and draining (#64).

## 14. Questions for the maintainer

1. **May `cufflink build --bundle` read the platform's configs?** It would
   need credentials and `--env`, and would make a build depend on the
   platform. *Default: no. A build is given what `Cufflink.toml` declares;
   the deploy compares against the platform's values and names any it
   lacks.*
2. **May a deploy go past a build-time values mismatch?** A flag such as
   `--accept-build-env` would ship bytes built for another environment.
   *Default: no flag. Build for that environment, or narrow
   `[web].build_env`.*
3. **What does an absent `[web].build_env` mean?** *Default: every value,
   as today, for deploys from source and bundles alike; bundles are then
   portable only between environments with identical values, and the
   documentation recommends declaring the list.*
4. **Should a bundle refuse the nearest-`node_modules` fallback?** *Default:
   `build --bundle` refuses; a deploy from source keeps the fallback and
   records `nearest`.*
5. **Should `.next/trace` be left out too?** It is the build's trace and
   `next start` does not read it. *Default: keep it in this change; leaving
   it out is one more entry in `EXCLUDED` once confirmed against the Next.js
   versions in use.*
6. **The policy's default mode.** *Default: `off` (any credential may skip,
   and every skip is recorded); a tenant opts in to `warn` or `enforce`.*
7. **Config in the deploy request.** It makes config and version change
   together and lets a new service start with its config once registration
   waits for the bundle. *Default: yes, `config` on the web deploy request,
   settled with #63's note.*
8. **How long does the old upload route stay?** *Default: deprecated in
   v0.38.0, removed no earlier than two minor releases later, with the
   release notes naming the CLI version that stops using it.*
9. **Audit skipped deploys?** A deploy that changes nothing writes no audit
   entry today. *Default: a skipped web deploy with a bundle writes
   `deploy.skipped`, so a skip of the checks is always recorded; other
   modes are unchanged.*
