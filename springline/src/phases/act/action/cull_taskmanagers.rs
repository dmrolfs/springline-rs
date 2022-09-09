use crate::kubernetes::FlinkComponent;
use crate::phases::act;
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::{ActError, ActErrorDisposition, ScaleActionPlan};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use either::{Left, Right};
use itertools::Itertools;
use k8s_openapi::api::core::v1::Pod;
use kube::ResourceExt;
use proctor::AppData;
use rand::Rng;
use std::marker::PhantomData;
use tracing::Instrument;
use crate::math;

pub const ACTION_LABEL: &str = "cull_taskmanagers";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CullTaskmanagers<P> {
    marker: PhantomData<P>,
}

impl<P> Default for CullTaskmanagers<P> {
    fn default() -> Self {
        Self { marker: PhantomData }
    }
}

#[async_trait]
impl<P> ScaleAction for CullTaskmanagers<P>
where
    P: AppData + ScaleActionPlan,
{
    type Plan = P;

    fn label(&self) -> &str {
        ACTION_LABEL
    }

    fn check_preconditions(&self, session: &ActionSession) -> Result<(), ActError> {
        session
            .savepoints
            .as_ref()
            .map(|_| ())
            .ok_or_else(|| ActError::ActionPrecondition {
                action: self.label().to_string(),
                reason: format!(
                    "no savepoint taken before culling, suggests possibleactive jobs: {:?}",
                    session.active_jobs
                ),
            })
    }

    #[tracing::instrument(level = "info", name = "CullTaskmanagers::execute", skip(self, plan))]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        let nr_to_kill = if plan.target_replicas() < plan.current_replicas() {
            plan.current_replicas() - plan.target_replicas()
        } else {
            let nr_taskmanagers = math::try_u64_to_f64(plan.current_replicas() as u64)
                .map_err(|err| ActError::Stage(err.into()))?;
            let max_culled = math::try_f64_to_u32(0.2 * nr_taskmanagers) as usize;
            rand::thread_rng().gen_range(1..=max_culled)
        };

        // focus on culling oldest taskmanagers
        let tms: Vec<(Pod, DateTime<Utc>)> = session
            .kube
            .list_pods(FlinkComponent::TaskManager)
            .await?
            .into_iter()
            .filter_map(|p: Pod| {
                p.metadata
                    .creation_timestamp
                    .as_ref()
                    .map(|t| &t.0)
                    .cloned()
                    .map(|ts| (p, ts))
            })
            .sorted_by_key(|(_, ts)| ts.timestamp())
            .take(nr_to_kill)
            .collect();

        for (tm, created_ts) in tms {
            let tm_name = tm.name_any();

            let culled = session.kube
                .delete_pod(&tm_name)
                .instrument(tracing::info_span!("culling taskmanager", taskmanager_name=%tm_name, %created_ts))
                .await;
            match culled {
                Ok(Left(_)) => tracing::info!("culling taskamanger[{tm_name}] is in progress"),
                Ok(Right(status)) => {
                    tracing::info!(?status, "culling taskmanager[{tm_name}] completed")
                },
                Err(err) => {
                    let track = format!("{}::delete_pod", self.label());
                    tracing::warn!(%created_ts, error=?err, %track, "failed to cull taskmanager: {tm_name} - ignoring.");
                    act::track_act_errors(&track, Some(&err), ActErrorDisposition::Ignored, plan);
                },
            }
        }

        Ok(())
    }
}
