use crate::config::PlatformConfig;
use crate::readiness::{Gate, WarmupOutcome};
use crate::shutdown::{self, Shutdown};
pub mod dev_bootstrap;
pub mod layers;
pub(crate) mod routes;
mod startup;

pub use startup::{until_ready, Abort, Permanence};

use crate::{api, auth, nats_manager, redis_conn, storage};
use crate::{repo, tenant_db, wasm_runtime, webhook_dispatcher, AppState, Assembly};
use axum::{routing::get, Router};
use cufflink_types::ServiceMode;
use futures_util::StreamExt;
use metrics_exporter_prometheus::PrometheusHandle;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{error, info, warn, Level};

const PLATFORM_POOL_SIZE: usize = 16;
const WARMUP_CONCURRENCY: usize = 4;

pub struct Platform {
    pub state: Arc<AppState>,
    pub router: Router,
    nats_reconnected: Arc<Notify>,
    migrations: repo::migrations::Target,
}

pub async fn build(
    config: PlatformConfig,
    shutdown: Shutdown,
    metrics_handle: PrometheusHandle,
    vars: impl Fn(&str) -> Option<String>,
) -> eyre::Result<Platform> {
    let object_storage =
        cufflink_storage::optional_from_vars(vars)?.map(storage::S3Storage::from_inner);
    assemble(config, shutdown, metrics_handle, object_storage).await
}

pub async fn assemble(
    config: PlatformConfig,
    shutdown: Shutdown,
    metrics_handle: PrometheusHandle,
    object_storage: Option<storage::S3Storage>,
) -> eyre::Result<Platform> {
    config.http.check_serving()?;
    let migrations = repo::migrations::Target {
        config: cufflink_db::parse_config(&config.database_url)?,
        settings: cufflink_db::PoolSettings::from_env(PLATFORM_POOL_SIZE)?,
    };
    let platform_db =
        cufflink_db::build_pool_with(migrations.config.clone(), migrations.settings.clone())?;
    let tenant_pools = tenant_db::TenantPoolManager::new(
        config.tenant_database_url(),
        config.max_tenant_pools,
        config.max_conns_per_tenant,
    )?
    .with_pooler(config.tenant_db_pooler)
    .with_direct_url(&config.database_url)?;

    let (nats, nats_reconnected) = crate::nats_conn::connect_with_reconnect_signal(
        &config.nats_url,
        "cufflink-platform",
        &config.nats_auth,
    )
    .await?;
    info!(
        "NATS client started for {} (connects in the background)",
        crate::nats_conn::redact_urls(&config.nats_url)
    );

    match &object_storage {
        Some(store) => info!(
            "Object storage configured (backend: {}, bucket: {})",
            store.backend_kind(),
            store.primary_bucket(),
        ),
        None => info!("Object storage not configured, using local filesystem"),
    }

    let wasm_engine = match wasm_runtime::WasmEngine::new(config.wasm.clone(), shutdown.clone()) {
        Ok(engine) => {
            if let Some(store) = &object_storage {
                engine.set_storage(store.clone()).await;
            }
            engine.set_artifact_index(platform_db.clone());
            info!("WASM runtime initialized (wasmtime)");
            Some(engine)
        }
        Err(e) => {
            warn!("WASM runtime unavailable: {}", e);
            None
        }
    };

    let redis = match config.redis_url.as_deref() {
        Some(url) => match redis_conn::RedisHandle::connect_in_background(url) {
            Ok(handle) => {
                info!("Redis configured; connecting in the background");
                handle
            }
            Err(e) => {
                warn!("Invalid Redis URL, running without Redis: {}", e);
                redis_conn::RedisHandle::disabled()
            }
        },
        None => {
            info!("Redis not configured, using in-memory rate limiter");
            redis_conn::RedisHandle::disabled()
        }
    };

    let keycloak_auth = (!config.keycloak_url.is_empty()).then(|| {
        auth::KeycloakAuth::new(
            &config.keycloak_url,
            &config.keycloak_realm,
            config.keycloak_issuer.as_deref(),
            config.jwks_max_stale,
        )
    });

    if !config.worker_routes.is_empty() {
        info!("Worker routes configured: {:?}", config.worker_routes);
    }

    let service_identity = auth::service_identity::ServiceIdentityKeys::from_setting(
        config.service_identity_keys.as_deref(),
    )?;
    let state = Arc::new(AppState::new(Assembly {
        config,
        platform_db,
        tenant_pools,
        nats,
        wasm_engine,
        metrics_handle,
        storage: object_storage,
        keycloak_auth,
        redis,
        shutdown,
        service_identity,
    }));
    state
        .svc_service
        .install(routes::svc_service(state.clone()))?;
    let router = router(state.clone());
    Ok(Platform {
        state,
        router,
        nats_reconnected,
        migrations,
    })
}

pub fn router(state: Arc<AppState>) -> Router {
    let routes = routes::control::routes().fallback_service(routes::svc_service(state.clone()));
    layers::outer(routes, &state)
        .merge(
            Router::new()
                .route("/health", get(api::health::health))
                .route("/ready", get(api::health::ready)),
        )
        .with_state(state)
}

pub fn spawn_bootstrap(
    state: Arc<AppState>,
    nats_reconnected: Arc<Notify>,
    migrations: repo::migrations::Target,
) -> JoinHandle<eyre::Result<()>> {
    let shutdown = state.shutdown.clone();
    shutdown.spawn("bootstrap", async move {
        let steps = async {
            until_ready(
                &state.shutdown,
                Level::WARN,
                "platform database",
                || async {
                    let client = state.platform_db.get().await?;
                    client.query_one("SELECT 1", &[]).await?;
                    Ok::<_, eyre::Report>(())
                },
            )
            .await?;
            info!("Connected to PostgreSQL (platform DB)");

            metrics::gauge!("cufflink_migrations_pending").set(1.0);
            until_ready(&state.shutdown, Level::ERROR, "platform migrations", || {
                repo::migrations::run(&migrations)
            })
            .await?;
            metrics::gauge!("cufflink_migrations_pending").set(0.0);
            state.readiness.mark(Gate::MigrationsDone);
            info!("Platform migrations complete");

            until_ready(&state.shutdown, Level::ERROR, "secret storage", || {
                state.secrets.check_startup()
            })
            .await?;
            info!(
                mode = state.config.secret_storage.as_str(),
                "Secret storage checked"
            );

            let expired = until_ready(&state.shutdown, Level::ERROR, "API key expiry", || {
                repo::api_keys::expire_unbounded(
                    &state.platform_db,
                    repo::api_keys::KeyClass::Tenant,
                    state.config.api_key_ttl,
                )
            })
            .await?;
            if expired > 0 {
                info!(
                    keys = expired,
                    ttl_days = state.config.api_key_ttl.num_days(),
                    "Gave tenant API keys without an expiry one"
                );
            }
            if let Some(ttl) = state.config.admin_api_key_ttl {
                let expired = until_ready(&state.shutdown, Level::ERROR, "admin API key expiry", || {
                    repo::api_keys::expire_unbounded(
                        &state.platform_db,
                        repo::api_keys::KeyClass::PlatformAdmin,
                        ttl,
                    )
                })
                .await?;
                if expired > 0 {
                    warn!(
                        keys = expired,
                        ttl_days = ttl.num_days(),
                        "Gave platform-admin API keys without an expiry one; rotate them with `cufflink auth keys rotate --admin` (CUFFLINK_ADMIN_API_KEY_TTL_DAYS=0 keeps them unbounded)"
                    );
                }
            }

            until_ready(&state.shutdown, Level::ERROR, "tenant catalog", || {
                state.tenants.reload()
            })
            .await?;
            info!("Tenant catalog loaded");
            if !state.tenant_sessions.resets_session_state() {
                let rows = until_ready(&state.shutdown, Level::ERROR, "tenant policies", || {
                    repo::tenant_policies::list_all(&state.platform_db)
                })
                .await?;
                if crate::db_roles::asks_for_roles(&state.config.policy_defaults, &rows) {
                    return Err(Abort::Fatal(eyre::eyre!(
                        crate::db_roles::NEEDS_CLEAN_RECYCLE
                    )));
                }
            }
            if let Ok(rows) = repo::tenant_policies::list_all(&state.platform_db).await {
                for var in state.config.deprecated_service_account_env(&rows) {
                    warn!("{var} is deprecated: a tenant enforces its own service_account_clients, where the variable no longer applies; it will be removed");
                }
            }

            if let Some(dev) = &state.config.dev_bootstrap {
                until_ready(&state.shutdown, Level::ERROR, "development bootstrap", || {
                    dev_bootstrap::run(&state, dev)
                })
                .await?;
            }

            until_ready(&state.shutdown, Level::ERROR, "service registry", || {
                state.registry.load_all()
            })
            .await?;
            state.readiness.mark(Gate::RegistryLoaded);
            info!(
                "Loaded {} services from registry",
                state.registry.count().await
            );
            Ok::<_, Abort>(())
        };
        let outcome = match state.config.startup_deadline {
            Some(deadline) => tokio::time::timeout(deadline, steps)
                .await
                .unwrap_or_else(|_| {
                    Err(Abort::Fatal(eyre::eyre!(
                        "startup did not complete within {deadline:?} (STARTUP_DEADLINE_SECS)"
                    )))
                }),
            None => steps.await,
        };
        match outcome {
            Ok(()) => {}
            Err(Abort::Shutdown) => {
                info!("Bootstrap abandoned: shutdown triggered");
                return Ok(());
            }
            Err(Abort::Fatal(error)) => return Err(error),
        }

        state.tenants.start_sync_listener(&state.shutdown);
        state
            .partner_keys
            .start_invalidation_listener(state.nats.clone(), &state.shutdown);
        api::partner_keys::spawn_active_gauge(state.platform_db.clone(), &state.shutdown);
        nats_manager::tick::start_publisher(state.nats.clone(), &state.shutdown);
        crate::webhooks::retention::start(state.clone());
        nats_manager::bootstrap::spawn(state.clone(), nats_reconnected);
        state.registry.start_sync(
            &state.shutdown,
            state.wasm_engine.clone(),
            state.config.registry_reload_interval,
        );
        nats_manager::deploy_fanout::start_listener(
            state.nats.clone(),
            state.platform_db.clone(),
            state.wasm_engine.clone(),
            &state.shutdown,
        );
        webhook_dispatcher::start(state.clone());
        crate::deployer::job::start_sweep(&state);
        state.webhooks.index.start_sync(
            &state.shutdown,
            state.nats.clone(),
            state.platform_db.clone(),
        );
        crate::webhooks::delivery::start(state.clone());
        crate::outbox::bootstrap::start(state.clone());
        crate::outbox::relay::start(state.clone());
        check_snapshots(&state).await;
        check_schemas(&state).await;
        warm_modules(&state).await;
        Ok(())
    })
}

async fn check_snapshots(state: &Arc<AppState>) {
    for svc in state.registry.all_services().await {
        let snapshot =
            repo::deployments::get_schema_snapshot(&state.platform_db, svc.service_id, svc.version)
                .await
                .and_then(|json| repo::deployments::parse_snapshot(&svc.name, svc.version, json));
        if let Err(e) = snapshot {
            error!(
                service = %svc.name,
                tenant = %svc.tenant_slug,
                version = svc.version,
                error = %e,
                "STARTUP CHECK FAILED: schema snapshot"
            );
        }
    }
}

async fn check_schemas(state: &Arc<AppState>) {
    match repo::services::sharing_a_schema(&state.platform_db).await {
        Ok(shared) => shared
            .into_iter()
            .for_each(|(tenant_id, schema, services)| {
                error!(
                    %tenant_id,
                    %schema,
                    ?services,
                    index = repo::services::SCHEMA_INDEX,
                    "STARTUP CHECK FAILED: services of one tenant share a schema, so the index \
                     that keeps schemas unique was not created"
                );
            }),
        Err(e) => warn!(error = %e, "Could not check that every service has a schema of its own"),
    }
}

async fn warm_modules(state: &Arc<AppState>) {
    let Some(engine) = state.wasm_engine.as_ref() else {
        return;
    };
    let services: Vec<_> = state
        .registry
        .all_services()
        .await
        .into_iter()
        .filter(|svc| match svc.manifest.mode {
            ServiceMode::Wasm => true,
            ServiceMode::Crud | ServiceMode::Container | ServiceMode::Web | ServiceMode::Native => {
                false
            }
        })
        .collect();
    state.readiness.set_pending_modules(services.len());
    let outcomes = futures_util::stream::iter(services)
        .map(|svc| async move {
            let loaded = engine
                .get_module(svc.service_id, svc.version, svc.wasm_hash.as_deref())
                .await
                .is_some();
            let outcome = if loaded {
                WarmupOutcome::Warmed
            } else {
                error!(
                    service = %svc.name,
                    tenant = %svc.tenant_slug,
                    version = svc.version,
                    "STARTUP CHECK FAILED: WASM module not retrievable — this version cannot serve handler routes"
                );
                WarmupOutcome::Broken
            };
            state.readiness.record_module(outcome);
            outcome
        })
        .buffer_unordered(WARMUP_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    let broken = outcomes
        .iter()
        .filter(|o| **o == WarmupOutcome::Broken)
        .count();
    let ok = outcomes.len() - broken;
    if broken == 0 {
        info!("Startup WASM invariant check: all {ok} active WASM service versions are loadable");
    } else {
        error!(
            "Startup WASM invariant check: {ok} OK, {broken} BROKEN — handler routes for broken versions will 500"
        );
    }
}

pub async fn serve(platform: Platform, listener: TcpListener) -> eyre::Result<()> {
    let Platform {
        state,
        router,
        nats_reconnected,
        migrations,
    } = platform;
    let shutdown = state.shutdown.clone();
    let bootstrap = spawn_bootstrap(state.clone(), nats_reconnected, migrations);
    let mut startup_failed = None;

    let server = axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(unready_then_close(state.clone()))
    .into_future();
    tokio::pin!(server);

    let ended = tokio::select! {
        biased;
        _ = shutdown::wait_for_signal() => {
            info!("Shutdown signal received");
            shutdown.trigger();
            None
        }
        _ = shutdown.cancelled() => None,
        error = startup::bootstrap_failure(bootstrap) => {
            error!(error = %error, "Startup failed; shutting down");
            startup_failed = Some(error);
            shutdown.trigger();
            None
        }
        result = &mut server => Some(result),
    };
    let http = match ended {
        Some(result) if !shutdown.is_triggered() => return result.map_err(Into::into),
        Some(result) => shutdown::phase(&shutdown, "http", std::future::ready(result)).await,
        None => shutdown::phase(&shutdown, "http", &mut server).await,
    };
    state.subscription_tasks.stop_all();
    let tasks = shutdown::phase(&shutdown, "tasks", async {
        let loops = state
            .subscription_tasks
            .join_all(shutdown.remaining())
            .await;
        loops.merge(shutdown.drain_tasks().await)
    })
    .await;
    if let Some(Err(e)) = &http {
        warn!(error = %e, "HTTP server ended with an error");
    }
    let finished = shutdown::finish(
        &shutdown,
        &[
            ("http", http.is_some()),
            ("tasks", shutdown::drained(tasks)),
        ],
        crate::nats_conn::flush_and_drain(&state.nats),
    )
    .await;
    startup_failed.map_or(finished, Err)
}

async fn unready_then_close(state: Arc<AppState>) {
    state.shutdown.cancelled().await;
    state.readiness.mark_shutting_down();
    tokio::time::sleep(state.config.shutdown_unready_delay).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn programmatic_trigger_runs_every_shutdown_phase() {
        let recorder = cufflink_telemetry::metrics_builder()
            .expect("builder")
            .build_recorder();
        let handle = recorder.handle();
        let shutdown = Shutdown::new(Duration::from_secs(3));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let config = PlatformConfig::for_tests(
            "postgres://cufflink:cufflink@127.0.0.1:1/cufflink",
            "nats://127.0.0.1:1",
        );
        let platform = build(
            config,
            shutdown.clone(),
            handle.clone(),
            cufflink_settings::process_var,
        )
        .await
        .expect("build");

        let trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trigger.trigger();
        });
        let runtime = tokio::runtime::Handle::current();
        let outcome = metrics::with_local_recorder(&recorder, || {
            tokio::task::block_in_place(|| runtime.block_on(serve(platform, listener)))
        });
        assert!(outcome.is_ok(), "{:?}", outcome.err());
        let rendered = handle.render();
        for phase in ["http", "tasks", "nats", "telemetry"] {
            assert!(
                rendered.contains(&format!("phase=\"{phase}\"")),
                "phase {phase} missing from {rendered}"
            );
        }
    }
}
