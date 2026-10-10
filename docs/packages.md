# Packages

Cufflink packages are installable bundles of services and frontends that can be deployed into any cufflink environment. The examples below use a fictional `my-package` published at `github:example-org/my-cufflink-package`.

## Installing a Package

`cufflink install` copies the package with `rsync`, so `rsync` must be on your `PATH`.

From GitHub:

```bash
cufflink install github:example-org/my-cufflink-package
```

From a specific version:

```bash
cufflink install github:example-org/my-cufflink-package@v1.0.0
```

From a local path:

```bash
cufflink install ~/dev/my-cufflink-package
```

Packages are installed into a `cufflink-packages/` directory relative to the current working directory.

## Reinstalling

If the package is already installed, cufflink asks `Package 'my-package' already installed. Overwrite? [y/N]` and then preserves:

- **`Cufflink.toml`** — your environment-specific configuration (API URLs, tenant, Keycloak config)
- **`secrets/`** — your encrypted secrets vault

All other files are updated from the source. Files that no longer exist in the source are removed.

## Package Structure

A package is a directory containing a `cufflink-package.toml`:

```toml
[package]
name = "my-package"
description = "Example package"
version = "0.1.0"

[[services]]
name = "my-api"
path = "services/my-api"

[[frontends]]
name = "my-frontend"
path = "apps/my-frontend"

[config]
required = [
    "PLATFORM_API_URL",
    "OAUTH_CLIENT_ID",
    "OAUTH_CLIENT_SECRET",
]
```

A package can contain multiple `[[services]]` (Rust/WASM) and `[[frontends]]` (web apps). `[config].required` is a list of configuration keys the package expects to be set before deploy; `cufflink install` prints them so you know what to provide with `cufflink config set`.

## Deploying a Package

```bash
cufflink deploy-package my-package
```

Skip specific components:

```bash
cufflink deploy-package my-package --skip my-frontend
```

## pnpm Workspace Detection

If the package is installed inside a pnpm monorepo, `cufflink install` checks whether `cufflink-packages/*` is listed in `pnpm-workspace.yaml`. If not, it prints a warning with the glob to add, because `pnpm install` won't install the package's dependencies until you do. The check is a warning only; it does not block install or deploy.
