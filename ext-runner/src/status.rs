use crate::deployment::Deployment;
use crate::restart::{Hold, Wait};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Ready,
    Starting,
    CrashLooping,
    Refused,
}

impl State {
    pub const GAUGED: [State; 4] = [
        State::Ready,
        State::Starting,
        State::CrashLooping,
        State::Refused,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Starting => "starting",
            Self::CrashLooping => "crash_looping",
            Self::Refused => "refused",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    pub deployment: Arc<Deployment>,
    pub state: State,
    pub pid: Option<u32>,
}

impl View {
    pub fn json(&self) -> Value {
        let deployment = &self.deployment;
        json!({
            "tenant": deployment.tenant,
            "service": deployment.service,
            "version": deployment.version,
            "sha": &deployment.sha256[..12],
            "state": self.state.as_str(),
            "extensions": deployment.extensions,
            "pid": self.pid,
        })
    }
}

pub fn views(
    running: Vec<(Arc<Deployment>, Option<u32>)>,
    starting: &BTreeMap<Uuid, Arc<Deployment>>,
    holds: &HashMap<Uuid, Hold>,
) -> Vec<View> {
    let ready = running.into_iter().map(|(deployment, pid)| View {
        deployment,
        state: State::Ready,
        pid,
    });
    let starting = starting.values().map(|deployment| View {
        deployment: deployment.clone(),
        state: State::Starting,
        pid: None,
    });
    let held = holds.values().map(|hold| View {
        deployment: hold.deployment.clone(),
        state: match hold.wait {
            Wait::Until(_) => State::Starting,
            Wait::CrashLooping => State::CrashLooping,
            Wait::Refused => State::Refused,
        },
        pid: None,
    });
    let mut views =
        ready
            .chain(starting)
            .chain(held)
            .fold(Vec::<View>::new(), |mut views, view| {
                if views.iter().all(|seen| seen.deployment != view.deployment) {
                    views.push(view);
                }
                views
            });
    views.sort_by(|a, b| {
        let (a, b) = (&a.deployment, &b.deployment);
        (&a.tenant, &a.service, a.version).cmp(&(&b.tenant, &b.service, b.version))
    });
    views
}

pub fn counts(views: &[View]) -> [(State, usize); 4] {
    State::GAUGED.map(|state| (state, views.iter().filter(|v| v.state == state).count()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restart::tests::deployment;
    use crate::restart::Failure;
    use std::time::Duration;
    use tokio::time::Instant;

    fn other(id: u128, service: &str) -> Arc<Deployment> {
        Arc::new(Deployment {
            service_id: Uuid::from_u128(id),
            service: service.into(),
            ..(*deployment(1)).clone()
        })
    }

    #[test]
    fn each_deployment_shows_once_in_its_most_telling_state() {
        let now = Instant::now();
        let quick = Failure::Exited {
            uptime: Duration::from_secs(1),
        };
        let (running, next) = (deployment(1), deployment(2));
        let crashing = other(100, "ext-crash");
        let refused = other(101, "ext-refused");
        let crash_loop = [quick, quick, quick]
            .into_iter()
            .fold(None::<Hold>, |previous, failure| {
                Some(Hold::after(
                    previous.as_ref(),
                    crashing.clone(),
                    failure,
                    now,
                ))
            })
            .expect("held");
        let holds = HashMap::from([
            (
                running.service_id,
                Hold::after(None, running.clone(), Failure::Unready, now),
            ),
            (crashing.service_id, crash_loop),
            (
                refused.service_id,
                Hold::after(None, refused.clone(), Failure::Refused, now),
            ),
        ]);
        let starting = BTreeMap::from([(next.service_id, next.clone())]);
        let views = views(vec![(running.clone(), Some(42))], &starting, &holds);
        assert_eq!(
            views
                .iter()
                .map(|v| (
                    v.deployment.service.as_str(),
                    v.deployment.version,
                    v.state,
                    v.pid
                ))
                .collect::<Vec<_>>(),
            [
                ("ext-crash", 1, State::CrashLooping, None),
                ("ext-pdf", 1, State::Ready, Some(42)),
                ("ext-pdf", 2, State::Starting, None),
                ("ext-refused", 1, State::Refused, None),
            ]
        );
        assert_eq!(
            counts(&views),
            [
                (State::Ready, 1),
                (State::Starting, 1),
                (State::CrashLooping, 1),
                (State::Refused, 1)
            ]
        );
        assert_eq!(
            views[1].json(),
            json!({
                "tenant": "acme",
                "service": "ext-pdf",
                "version": 1,
                "sha": "000000000000",
                "state": "ready",
                "extensions": ["pdf.render"],
                "pid": 42,
            })
        );
    }
}
