use crate::config::CliConfig;
use serde_json::Value;
use std::process::Command;

pub struct RouteAudit {
    pub method: String,
    pub path: String,
    pub handler: String,
    pub label: String,
    pub anonymous: bool,
}

pub fn classify_route(route: &Value) -> RouteAudit {
    let str_at = |key: &str| route[key].as_str().unwrap_or("?").to_string();
    let (label, anonymous) = match route.get("posture") {
        None | Some(Value::Null) => ("undeclared".to_string(), true),
        Some(p) => match p["kind"].as_str() {
            Some("public") => (
                format!("public ({})", p["reason"].as_str().unwrap_or("")),
                true,
            ),
            Some("authenticated") => ("authenticated".to_string(), false),
            Some("permission") => (
                format!(
                    "permission {}:{}",
                    p["area"].as_str().unwrap_or("?"),
                    p["operation"].as_str().unwrap_or("?")
                ),
                false,
            ),
            Some("owner") => (
                format!("owner({})", p["field"].as_str().unwrap_or("?")),
                false,
            ),
            _ => ("unknown".to_string(), true),
        },
    };
    RouteAudit {
        method: str_at("method"),
        path: str_at("path"),
        handler: str_at("handler"),
        label,
        anonymous,
    }
}

pub fn audit_manifest(manifest: &Value) -> Vec<RouteAudit> {
    manifest["custom_routes"]
        .as_array()
        .map(|routes| routes.iter().map(classify_route).collect())
        .unwrap_or_default()
}

fn print_routes(service: &str, audits: &[RouteAudit]) {
    if audits.is_empty() {
        println!("{service}: no custom routes");
        return;
    }
    println!("{service}:");
    for a in audits {
        let flag = if a.anonymous { " ⚠ anonymous" } else { "" };
        println!(
            "  {:<6} {:<38} {:<26} {:<22}{}",
            a.method, a.path, a.handler, a.label, flag
        );
    }
}

pub async fn run(all_services: bool, audit: bool, env: Option<&str>) -> eyre::Result<()> {
    let anonymous = if all_services {
        run_deployed(env).await?
    } else {
        run_local()?
    };

    if audit && anonymous > 0 {
        eyre::bail!("{anonymous} anonymous or undeclared route(s) found");
    }
    Ok(())
}

fn run_local() -> eyre::Result<usize> {
    let output = Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;
    if !output.status.success() {
        eyre::bail!("Failed to build service. Run from a cufflink service directory.");
    }
    let manifest: Value = serde_json::from_slice(&output.stdout)?;
    let service = manifest["name"].as_str().unwrap_or("service").to_string();
    let audits = audit_manifest(&manifest);
    let anonymous = audits.iter().filter(|a| a.anonymous).count();
    print_routes(&service, &audits);
    Ok(anonymous)
}

async fn run_deployed(env: Option<&str>) -> eyre::Result<usize> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {name}\n");
    }
    let client = config.http_client();

    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services", config.api_url),
        )
        .send()
        .await?;
    if !resp.status().is_success() {
        eyre::bail!("Failed to list services: {}", resp.status());
    }
    let body: Value = resp.json().await?;
    let services = body["services"].as_array().cloned().unwrap_or_default();

    let mut total_anonymous = 0usize;
    for svc in &services {
        let (Some(id), Some(name)) = (svc["id"].as_str(), svc["name"].as_str()) else {
            continue;
        };
        let detail = config
            .auth_request(
                &client,
                reqwest::Method::GET,
                &format!("{}/api/services/{}", config.api_url, id),
            )
            .send()
            .await?;
        if !detail.status().is_success() {
            println!("{name}: unable to fetch manifest ({})", detail.status());
            continue;
        }
        let detail_body: Value = detail.json().await?;
        let audits = audit_manifest(&detail_body["current_manifest"]);
        total_anonymous += audits.iter().filter(|a| a.anonymous).count();
        print_routes(name, &audits);
    }
    Ok(total_anonymous)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_each_posture() {
        let public = classify_route(&json!({
            "method": "POST", "path": "/webhook", "handler": "h",
            "posture": {"kind": "public", "reason": "stripe"}
        }));
        assert!(public.anonymous);
        assert!(public.label.contains("public"));

        let perm = classify_route(&json!({
            "method": "GET", "path": "/x", "handler": "h",
            "posture": {"kind": "permission", "area": "items", "operation": "edit"}
        }));
        assert!(!perm.anonymous);
        assert_eq!(perm.label, "permission items:edit");

        let owner = classify_route(&json!({
            "method": "GET", "path": "/x", "handler": "h",
            "posture": {"kind": "owner", "field": "customer_id"}
        }));
        assert!(!owner.anonymous);
        assert_eq!(owner.label, "owner(customer_id)");

        let auth = classify_route(&json!({
            "method": "GET", "path": "/x", "handler": "h",
            "posture": {"kind": "authenticated"}
        }));
        assert!(!auth.anonymous);
    }

    #[test]
    fn missing_posture_is_undeclared_and_flagged() {
        let a = classify_route(&json!({"method": "GET", "path": "/x", "handler": "h"}));
        assert_eq!(a.label, "undeclared");
        assert!(a.anonymous);
    }

    #[test]
    fn audit_counts_only_anonymous() {
        let manifest = json!({
            "custom_routes": [
                {"method": "GET", "path": "/a", "handler": "a", "posture": {"kind": "authenticated"}},
                {"method": "POST", "path": "/b", "handler": "b", "posture": {"kind": "public", "reason": "wh"}},
                {"method": "GET", "path": "/c", "handler": "c"}
            ]
        });
        let audits = audit_manifest(&manifest);
        assert_eq!(audits.len(), 3);
        assert_eq!(audits.iter().filter(|a| a.anonymous).count(), 2);
    }
}
