use super::{block_on_bounded, read_guest_string, wrap, HostState};
use crate::registry::ServiceIdentity;
use crate::router::jobs::{enqueue_job, AuthSnapshot, JobIdentity, JobRequest};
use cufflink_types::host::request::GuestRequest;
use cufflink_types::JobConfig;
use serde_json::Value;
use std::collections::HashSet;
use wasmtime::{Caller, Linker};

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "jobs_enqueue",
        |mut caller: Caller<'_, HostState>,
         handler_ptr: i32,
         handler_len: i32,
         payload_ptr: i32,
         payload_len: i32,
         timeout_secs: i32,
         max_attempts: i32|
         -> i32 {
            let handler = host_try!(read_guest_string(
                &mut caller,
                "jobs_enqueue",
                handler_ptr,
                handler_len
            ));
            if handler.is_empty() {
                return -1;
            }
            let payload = serde_json::from_str::<Value>(&host_try!(read_guest_string(
                &mut caller,
                "jobs_enqueue",
                payload_ptr,
                payload_len
            )))
            .unwrap_or(Value::Null);

            let config = JobConfig {
                timeout_secs: u32::try_from(timeout_secs)
                    .ok()
                    .filter(|n| *n > 0)
                    .unwrap_or(JobConfig::DEFAULT_TIMEOUT_SECS),
                max_attempts: u32::try_from(max_attempts)
                    .ok()
                    .filter(|n| *n > 0)
                    .unwrap_or(JobConfig::DEFAULT_MAX_ATTEMPTS),
                ..Default::default()
            };

            let (
                tenant_id,
                tenant_slug,
                service_id,
                service_slug,
                service_version,
                auth,
                app_state,
            ) = {
                let hs = caller.data();
                (
                    hs.identity.tenant_id,
                    hs.identity.tenant_slug.clone(),
                    hs.identity.service_id,
                    hs.identity.service_name.clone(),
                    hs.service_version,
                    hs.auth.clone(),
                    hs.app_state.clone(),
                )
            };

            let request_json = match crate::router::guest_json(&enqueued_request(
                &caller.data().identity,
                &handler,
                payload,
                auth.as_ref(),
            )) {
                Ok(request) => request,
                Err(e) => {
                    tracing::error!("[wasm] jobs_enqueue error: {}", e);
                    return -1;
                }
            };

            let ident = JobIdentity {
                tenant_id,
                tenant_slug: &tenant_slug,
                service_id,
                service_slug: &service_slug,
                service_version,
            };

            let result = block_on_bounded(&caller.data().bounds(), async {
                enqueue_job(
                    &app_state,
                    &ident,
                    JobRequest::new(&handler, &request_json, auth.as_ref(), &config),
                )
                .await
            });

            match result {
                Ok(job_id) => {
                    caller.data_mut().host_response = Some(job_id.to_string());
                    0
                }
                Err(e) => {
                    tracing::error!("[wasm] jobs_enqueue error: {}", e);
                    -1
                }
            }
        },
    )?;

    Ok(())
}

fn enqueued_request(
    identity: &ServiceIdentity,
    handler: &str,
    payload: Value,
    auth: Option<&AuthSnapshot>,
) -> GuestRequest {
    GuestRequest {
        method: "POST".to_string(),
        handler: handler.to_string(),
        body: payload,
        tenant: identity.tenant_slug.clone(),
        service: identity.service_name.clone(),
        auth: Some(auth.map_or(Value::Null, |auth| auth.claims.clone())),
        ..GuestRequest::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_enqueued_job_runs_as_the_principal_that_enqueued_it() {
        let identity = ServiceIdentity {
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "acme".into(),
            service_id: uuid::Uuid::nil(),
            service_name: "stock-service".into(),
            schema: "stock_service".into(),
        };
        let auth = AuthSnapshot {
            subject: "user-1".into(),
            claims: json!({"sub": "user-1"}),
        };
        let request = |auth| {
            enqueued_request(
                &identity,
                "handle_run_report",
                json!({"id": "item-1"}),
                auth,
            )
            .to_value()
            .expect("request")
        };
        assert_eq!(
            request(Some(&auth)),
            json!({
                "method": "POST",
                "handler": "handle_run_report",
                "body": {"id": "item-1"},
                "tenant": "acme",
                "service": "stock-service",
                "auth": {"sub": "user-1"},
            })
        );
        assert_eq!(request(None)["auth"], Value::Null);
        assert_eq!(
            crate::router::guest_text(&enqueued_request(
                &identity,
                "handle_run_report",
                json!({"id": "item-1"}),
                Some(&auth)
            ))
            .expect("text"),
            r#"{"auth":{"sub":"user-1"},"body":{"id":"item-1"},"handler":"handle_run_report","method":"POST","service":"stock-service","tenant":"acme"}"#,
            "the bytes an enqueued job always read"
        );
    }
}
