#[tokio::test]
async fn legacy_db_host_abi_is_unchanged() {
    let engine = crate::wasm_runtime::WasmEngine::new(
        crate::wasm_runtime::WasmLimits::default(),
        crate::shutdown::Shutdown::new(std::time::Duration::from_secs(1)),
    )
    .expect("engine");
    engine
        .set_storage(crate::storage::S3Storage::from_inner(
            cufflink_storage::in_memory("b"),
        ))
        .await;
    let module = wat::parse_str(
        r#"(module
             (import "env" "db_query" (func (param i32 i32) (result i32)))
             (import "env" "db_execute" (func (param i32 i32) (result i32)))
             (import "env" "db_query_read_only" (func (param i32 i32) (result i32)))
             (import "env" "db_execute_all" (func (param i32 i32) (result i32)))
             (memory (export "memory") 1))"#,
    )
    .expect("wat");
    engine
        .load_module(uuid::Uuid::new_v4(), 1, &module)
        .await
        .expect("a guest built against the legacy db ABI still links");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_failure_codes_hold_next_to_db_call() {
    use crate::wasm_runtime::{Invocation, InvocationKind, WasmEngine, WasmLimits};
    use std::time::Duration;

    let engine = WasmEngine::new(
        WasmLimits::default(),
        crate::shutdown::Shutdown::new(Duration::from_secs(1)),
    )
    .expect("engine");
    engine
        .set_storage(crate::storage::S3Storage::from_inner(
            cufflink_storage::in_memory("b"),
        ))
        .await;
    let module = wat::parse_str(
        r#"(module
             (import "env" "db_query" (func $query (param i32 i32) (result i32)))
             (import "env" "db_execute" (func $execute (param i32 i32) (result i32)))
             (import "env" "db_call" (func $call (param i32 i32) (result i32)))
             (memory (export "memory") 1)
             (data (i32.const 0) "SELECT 1")
             (data (i32.const 16) "{\"op\":\"begin\"}")
             (func (export "alloc") (param i32) (result i32) (i32.const 1024))
             (func $code (param $at i32) (param $rc i32)
               (i32.store8 (local.get $at) (i32.add (i32.const 49) (local.get $rc))))
             (func (export "run") (param i32 i32) (result i32)
               (i32.store (i32.const 512) (i32.const 3))
               (call $code (i32.const 516) (call $query (i32.const 0) (i32.const 8)))
               (call $code (i32.const 517) (call $execute (i32.const 0) (i32.const 8)))
               (call $code (i32.const 518) (call $call (i32.const 16) (i32.const 14)))
               (i32.const 512)))"#,
    )
    .expect("wat");
    let id = uuid::Uuid::new_v4();
    engine.load_module(id, 1, &module).await.expect("links");
    let compiled = engine.get_module(id, 1, None).await.expect("cached");
    let state = crate::AppState::minimal(engine.clone()).await;
    let codes = engine
        .run(
            state,
            Invocation {
                compiled,
                handler: "run".into(),
                request_json: "{}".into(),
                identity: crate::registry::ServiceIdentity {
                    tenant_id: uuid::Uuid::nil(),
                    tenant_slug: "acme".into(),
                    service_id: uuid::Uuid::nil(),
                    schema: "contract".into(),
                    service_name: "contract".into(),
                },
                publishes: Some(vec![]),
                extensions: Vec::new(),
                kind: InvocationKind::Job,
                scope_kind: None,
                budget: Duration::from_secs(15),
                cancel: None,
            },
        )
        .await
        .expect("runs");
    assert_eq!(codes, "001", "db_query -1, db_execute -1, db_call 0");
}
