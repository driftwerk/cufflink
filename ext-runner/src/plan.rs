use crate::deployment::{Deployment, Desired};
use crate::restart::Hold;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Start(Arc<Deployment>),
    Replace(Arc<Deployment>),
    Stop(Uuid),
}

pub fn plan(
    desired: &Desired,
    running: &BTreeMap<Uuid, Arc<Deployment>>,
    holds: &HashMap<Uuid, Hold>,
    now: Instant,
) -> Vec<Action> {
    let held = |deployment: &Deployment| {
        holds
            .get(&deployment.service_id)
            .is_some_and(|hold| hold.holds(deployment, now))
    };
    let stops = running
        .keys()
        .filter(|id| !desired.run.contains_key(id) && !desired.unreadable.contains_key(id))
        .map(|id| Action::Stop(*id));
    let runs = desired
        .run
        .iter()
        .filter(|(_, deployment)| !held(deployment))
        .filter_map(|(id, deployment)| match running.get(id) {
            Some(child) if child == deployment => None,
            Some(_) => Some(Action::Replace(deployment.clone())),
            None => Some(Action::Start(deployment.clone())),
        });
    stops.chain(runs).collect()
}

pub fn forgotten(holds: &HashMap<Uuid, Hold>, desired: &Desired) -> Vec<Uuid> {
    holds
        .iter()
        .filter(|(id, hold)| {
            !desired.unreadable.contains_key(id) && desired.run.get(id) != Some(&hold.deployment)
        })
        .map(|(id, _)| *id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restart::{Failure, Wait};
    use std::time::Duration;

    fn deployment(service: u128, version: i32) -> Arc<Deployment> {
        Arc::new(Deployment {
            service_id: Uuid::from_u128(service),
            tenant: "acme".into(),
            service: format!("ext-{service}"),
            version,
            sha256: format!("{version:064x}"),
            size: 1,
            extensions: [format!("ext.s{service}")].into(),
        })
    }

    fn by_id(deployments: &[Arc<Deployment>]) -> BTreeMap<Uuid, Arc<Deployment>> {
        deployments
            .iter()
            .map(|deployment| (deployment.service_id, deployment.clone()))
            .collect()
    }

    fn desired(run: Vec<Arc<Deployment>>, unreadable: &[u128]) -> Desired {
        Desired {
            run: by_id(&run),
            unreadable: unreadable
                .iter()
                .map(|id| (Uuid::from_u128(*id), ("acme".into(), format!("ext-{id}"))))
                .collect(),
        }
    }

    type Case = (
        &'static str,
        Desired,
        Vec<Arc<Deployment>>,
        HashMap<Uuid, Hold>,
        Vec<Action>,
    );

    fn held(holds: &[(Arc<Deployment>, Failure)], now: Instant) -> HashMap<Uuid, Hold> {
        holds
            .iter()
            .map(|(deployment, failure)| {
                (
                    deployment.service_id,
                    Hold::after(None, deployment.clone(), *failure, now),
                )
            })
            .collect()
    }

    #[test]
    fn the_plan_converges_running_on_desired() {
        let now = Instant::now();
        let (a1, a2, b1, c1, d1) = (
            deployment(1, 1),
            deployment(1, 2),
            deployment(2, 1),
            deployment(3, 1),
            deployment(4, 1),
        );
        let changed_extensions = Arc::new(Deployment {
            extensions: ["ext.other".to_string()].into(),
            ..(*b1).clone()
        });
        let rolled_back = Arc::new(Deployment {
            version: 3,
            ..(*a1).clone()
        });
        let cases: Vec<Case> = vec![
            (
                "nothing wanted, nothing runs",
                desired(vec![], &[]),
                vec![],
                HashMap::new(),
                vec![],
            ),
            (
                "a new service starts",
                desired(vec![a1.clone()], &[]),
                vec![],
                HashMap::new(),
                vec![Action::Start(a1.clone())],
            ),
            (
                "what runs as desired is left alone",
                desired(vec![a1.clone()], &[]),
                vec![a1.clone()],
                HashMap::new(),
                vec![],
            ),
            (
                "a new version replaces the running one",
                desired(vec![a2.clone()], &[]),
                vec![a1.clone()],
                HashMap::new(),
                vec![Action::Replace(a2.clone())],
            ),
            (
                "a version of the same binary replaces it too",
                desired(vec![rolled_back.clone()], &[]),
                vec![a1.clone()],
                HashMap::new(),
                vec![Action::Replace(rolled_back.clone())],
            ),
            (
                "other extensions replace it",
                desired(vec![changed_extensions.clone()], &[]),
                vec![b1.clone()],
                HashMap::new(),
                vec![Action::Replace(changed_extensions.clone())],
            ),
            (
                "a service no longer desired stops",
                desired(vec![a1.clone()], &[]),
                vec![a1.clone(), b1.clone()],
                HashMap::new(),
                vec![Action::Stop(b1.service_id)],
            ),
            (
                "an unreadable deployment keeps its child",
                desired(vec![], &[2]),
                vec![b1.clone()],
                HashMap::new(),
                vec![],
            ),
            (
                "a held deployment waits, the old child keeps running",
                desired(vec![a2.clone(), c1.clone()], &[]),
                vec![a1.clone()],
                held(
                    &[
                        (a2.clone(), Failure::Refused),
                        (c1.clone(), Failure::Unready),
                    ],
                    now,
                ),
                vec![],
            ),
            (
                "a hold of another deployment does not hold",
                desired(vec![a2.clone()], &[]),
                vec![],
                held(&[(a1.clone(), Failure::Refused)], now),
                vec![Action::Start(a2.clone())],
            ),
            (
                "stops come first, then everything else at once",
                desired(vec![a2.clone(), c1.clone(), d1.clone()], &[]),
                vec![a1.clone(), b1.clone(), d1.clone()],
                HashMap::new(),
                vec![
                    Action::Stop(b1.service_id),
                    Action::Replace(a2.clone()),
                    Action::Start(c1.clone()),
                ],
            ),
        ];
        for (case, desired, running, holds, actions) in cases {
            assert_eq!(
                plan(&desired, &by_id(&running), &holds, now),
                actions,
                "{case}"
            );
        }
    }

    #[test]
    fn a_backoff_that_ended_starts_the_deployment() {
        let now = Instant::now();
        let a1 = deployment(1, 1);
        let holds = held(&[(a1.clone(), Failure::Unready)], now);
        let desired = desired(vec![a1.clone()], &[]);
        assert_eq!(plan(&desired, &BTreeMap::new(), &holds, now), vec![]);
        let later = now + Duration::from_secs(2);
        assert_eq!(
            plan(&desired, &BTreeMap::new(), &holds, later),
            vec![Action::Start(a1)]
        );
    }

    #[test]
    fn a_deploy_or_a_removal_forgets_a_hold() {
        let now = Instant::now();
        let (a1, a2, b1, c1) = (
            deployment(1, 1),
            deployment(1, 2),
            deployment(2, 1),
            deployment(3, 1),
        );
        let holds = held(
            &[
                (a1.clone(), Failure::Refused),
                (b1.clone(), Failure::Refused),
                (c1.clone(), Failure::Refused),
            ],
            now,
        );
        assert!(holds.values().all(|hold| hold.wait == Wait::Refused));
        let mut ended = forgotten(&holds, &desired(vec![a2, b1], &[3]));
        ended.sort();
        assert_eq!(
            ended,
            [a1.service_id],
            "a redeployed service's hold ends; the one still wanted or unreadable stays"
        );
        assert_eq!(
            forgotten(&holds, &desired(vec![], &[])).len(),
            3,
            "removed services are forgotten"
        );
    }
}
