use cufflink_types::slug::{is_valid_service_name, is_valid_tenant_slug};
use serde_json::Value;

pub fn target(payload: &Value) -> Result<(String, String), &'static str> {
    let tenant = payload["tenant_slug"]
        .as_str()
        .filter(|s| is_valid_tenant_slug(s))
        .ok_or("invalid_tenant_slug")?;
    let service = payload["service_name"]
        .as_str()
        .filter(|s| is_valid_service_name(s))
        .ok_or("invalid_service_name")?;
    Ok((tenant.to_string(), service.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_platform_identifiers() {
        let event = json!({"tenant_slug": "preview-pr-7", "service_name": "portal", "version": 3});
        assert_eq!(target(&event), Ok(("preview-pr-7".into(), "portal".into())));
    }

    #[test]
    fn refuses_path_escapes() {
        for (event, reason) in [
            (
                json!({"tenant_slug": "../..", "service_name": "x"}),
                "invalid_tenant_slug",
            ),
            (json!({"service_name": "x"}), "invalid_tenant_slug"),
            (
                json!({"tenant_slug": "t", "service_name": "../etc"}),
                "invalid_service_name",
            ),
            (
                json!({"tenant_slug": "t", "service_name": "a/b"}),
                "invalid_service_name",
            ),
        ] {
            assert_eq!(target(&event), Err(reason), "{event}");
        }
    }
}
