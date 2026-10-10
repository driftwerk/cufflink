use super::RegisteredService;
use cufflink_telemetry::Scope;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceIdentity {
    pub tenant_id: Uuid,
    pub tenant_slug: String,
    pub service_id: Uuid,
    pub service_name: String,
    pub schema: String,
}

impl ServiceIdentity {
    pub fn scope(&self, version: impl Into<Option<i32>>) -> Scope {
        Scope::service(
            self.tenant_slug.as_str(),
            self.service_name.as_str(),
            version,
        )
    }
}

impl RegisteredService {
    pub fn scope(&self) -> Scope {
        Scope::service(self.tenant_slug.as_str(), self.name.as_str(), self.version)
    }

    pub fn identity(&self) -> ServiceIdentity {
        ServiceIdentity {
            tenant_id: self.tenant_id,
            tenant_slug: self.tenant_slug.clone(),
            service_id: self.service_id,
            service_name: self.name.clone(),
            schema: self.manifest.schema(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registered(name: &str, schema_name: Option<&str>) -> RegisteredService {
        RegisteredService {
            service_id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
            tenant_slug: "acme".into(),
            name: name.into(),
            manifest: cufflink_types::ServiceManifest {
                name: name.into(),
                schema_name: schema_name.map(str::to_string),
                ..Default::default()
            },
            version: 3,
            wasm_hash: None,
            commit_hash: None,
        }
    }

    #[test]
    fn a_service_that_names_no_schema_keeps_the_one_it_always_had() {
        for (name, schema) in [
            ("stock-service", "stock_service"),
            ("reports", "reports"),
            ("a-b-c", "a_b_c"),
        ] {
            let service = registered(name, None);
            assert_eq!(
                service.identity(),
                ServiceIdentity {
                    tenant_id: service.tenant_id,
                    tenant_slug: "acme".into(),
                    service_id: service.service_id,
                    service_name: name.into(),
                    schema: schema.into(),
                }
            );
        }
    }

    #[test]
    fn a_services_scope_is_its_tenant_name_and_version() {
        let service = registered("notes", None);
        let scope = Scope::service("acme", "notes", 3);
        assert_eq!(service.scope(), scope);
        assert_eq!(service.identity().scope(3), scope);
        assert_eq!(
            service.identity().scope(None),
            Scope::service("acme", "notes", None)
        );
    }

    #[test]
    fn a_declared_schema_is_the_services_schema() {
        assert_eq!(
            registered("loans-service", Some("loan_service"))
                .identity()
                .schema,
            "loan_service"
        );
    }
}
