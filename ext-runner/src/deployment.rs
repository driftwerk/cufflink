use cufflink_supervisor::runner_api::RunnerApi;
use cufflink_telemetry::Scope;
use cufflink_types::runner::{is_sha256, NativeService, NativeSet};
use cufflink_types::slug::is_valid_service_name;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deployment {
    pub service_id: Uuid,
    pub tenant: String,
    pub service: String,
    pub version: i32,
    pub sha256: String,
    pub size: u64,
    pub extensions: BTreeSet<String>,
}

impl Deployment {
    pub fn scope(&self) -> Scope {
        Scope::service(self.tenant.as_str(), self.service.as_str(), self.version)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Desired {
    pub run: BTreeMap<Uuid, Arc<Deployment>>,
    pub unreadable: BTreeMap<Uuid, (String, String)>,
}

pub async fn desired(platform: &RunnerApi) -> eyre::Result<Desired> {
    Ok(desired_of(platform.native().await?))
}

fn desired_of(set: NativeSet) -> Desired {
    let tenant = set.tenant;
    set.services
        .into_iter()
        .fold(Desired::default(), |mut desired, service| {
            let (id, names) = (service.service_id, (tenant.clone(), service.service_name.clone()));
            match decode(service, &tenant) {
                Ok(deployment) => {
                    desired.run.insert(id, Arc::new(deployment));
                }
                Err(reason) => {
                    tracing::error!(tenant = %names.0, service = %names.1, %reason, "A native deployment cannot be run; whatever runs of the service keeps running");
                    desired.unreadable.insert(id, names);
                }
            }
            desired
        })
}

fn decode(service: NativeService, tenant: &str) -> Result<Deployment, String> {
    if !is_valid_service_name(&service.service_name) {
        return Err("its service name is not one".into());
    }
    let sha256 = service
        .sha256
        .filter(|hash| is_sha256(hash))
        .ok_or("it records no binary with a SHA-256")?;
    let size = service.size.ok_or("it records no binary size")?;
    let extensions: BTreeSet<String> = service.extensions.into_iter().collect();
    if extensions.is_empty() {
        return Err("its manifest provides no extension".into());
    }
    Ok(Deployment {
        service_id: service.service_id,
        tenant: tenant.to_string(),
        service: service.service_name,
        version: service.version,
        sha256,
        size,
        extensions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn service() -> NativeService {
        NativeService {
            service_id: Uuid::nil(),
            service_name: "ext-pdf".into(),
            version: 4,
            sha256: Some(HASH.into()),
            size: Some(9),
            extensions: vec!["pdf.render".into(), "pdf.merge".into()],
        }
    }

    fn set(tenant: &str, services: Vec<NativeService>) -> NativeSet {
        NativeSet {
            tenant: tenant.into(),
            services,
        }
    }

    #[test]
    fn a_service_is_the_deployment_it_describes() {
        assert_eq!(
            decode(service(), "acme"),
            Ok(Deployment {
                service_id: Uuid::nil(),
                tenant: "acme".into(),
                service: "ext-pdf".into(),
                version: 4,
                sha256: HASH.into(),
                size: 9,
                extensions: ["pdf.merge", "pdf.render"].map(String::from).into(),
            })
        );
    }

    #[test]
    fn what_cannot_be_run_keeps_what_runs_of_it() {
        let unreadable = NativeService {
            service_id: Uuid::from_u128(3),
            sha256: None,
            ..service()
        };
        let desired = desired_of(set("acme", vec![service(), unreadable]));
        assert_eq!(
            desired.run.keys().copied().collect::<Vec<_>>(),
            [Uuid::nil()]
        );
        assert_eq!(
            desired.unreadable,
            [(
                Uuid::from_u128(3),
                ("acme".to_string(), "ext-pdf".to_string())
            )]
            .into()
        );
    }

    #[test]
    fn a_service_that_cannot_be_run_says_why() {
        for (service, reason) in [
            (
                NativeService {
                    service_name: "ext/pdf".into(),
                    ..service()
                },
                "its service name is not one",
            ),
            (
                NativeService {
                    service_name: "../ext".into(),
                    ..service()
                },
                "its service name is not one",
            ),
            (
                NativeService {
                    sha256: None,
                    ..service()
                },
                "it records no binary with a SHA-256",
            ),
            (
                NativeService {
                    sha256: Some(format!("../{}", &HASH[3..])),
                    ..service()
                },
                "it records no binary with a SHA-256",
            ),
            (
                NativeService {
                    sha256: Some(HASH.to_uppercase()),
                    ..service()
                },
                "it records no binary with a SHA-256",
            ),
            (
                NativeService {
                    size: None,
                    ..service()
                },
                "it records no binary size",
            ),
            (
                NativeService {
                    extensions: vec![],
                    ..service()
                },
                "its manifest provides no extension",
            ),
        ] {
            assert_eq!(decode(service, "acme"), Err(reason.to_string()));
        }
    }
}
