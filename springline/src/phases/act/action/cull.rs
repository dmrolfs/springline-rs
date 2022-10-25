use crate::kubernetes::FlinkComponent;
use crate::model::NrReplicas;
use crate::phases::act;
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::{ActError, ActErrorDisposition};
use crate::phases::plan::ScaleActionPlan;
use crate::{math, Env};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use either::{Left, Right};
use itertools::Itertools;
use k8s_openapi::api::core::v1::Pod;
use kube::ResourceExt;
use proctor::AppData;
use std::marker::PhantomData;
use tracing::Instrument;

pub const ACTION_LABEL: &str = "cull_taskmanagers";

#[derive(Debug, Clone, PartialEq)]
pub struct CullTaskmanagers<P> {
    cull_ratio: f64,
    marker: PhantomData<P>,
}

impl<P> CullTaskmanagers<P> {
    pub fn new(cull_ratio: f64) -> Self {
        let cull_ratio = f64::max(0.0, f64::min(1.0, cull_ratio));
        Self { cull_ratio, marker: PhantomData }
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

    fn check_preconditions(&self, session: &Env<ActionSession>) -> Result<(), ActError> {
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
        &mut self, plan: &'s Self::Plan, session: &'s mut Env<ActionSession>,
    ) -> Result<(), ActError> {
        let surplus = if plan.target_replicas() < plan.current_replicas() {
            plan.current_replicas() - plan.target_replicas()
        } else {
            NrReplicas::NONE
        };

        let nr_taskmanagers = plan.current_replicas();
        let core_culled = NrReplicas::new(math::try_f64_to_u32(
            self.cull_ratio * nr_taskmanagers.as_f64(),
        ));

        let nr_to_cull = surplus + core_culled;
        let nr_to_cull = NrReplicas::min(plan.current_replicas(), nr_to_cull);
        tracing::debug!(
            %nr_to_cull, %surplus, %core_culled,
            "calculated number of taskmanagers to cull before restart."
        );

        // if plan.current_replicas() <= nr_to_cull {
        //     self.do_complete_culling(plan, session).await
        // } else {
        self.do_individual_culling(nr_to_cull, plan, session).await
        // }
    }
}

impl<P> CullTaskmanagers<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", skip(self, plan, session))]
    async fn do_individual_culling<'s>(
        &mut self, nr_to_cull: NrReplicas, plan: &'s <Self as ScaleAction>::Plan,
        session: &'s mut Env<ActionSession>,
    ) -> Result<(), ActError> {
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
            .take(math::saturating_u32_to_usize(nr_to_cull.into()))
            .collect();

        tracing::debug!(
            %nr_to_cull,
            culling_taskmanagers=?tms.iter().map(|(p, ts)| format!("{}::@::{ts}", p.name_any())).collect::<Vec<_>>(),
            "identified list of taskmanagers to cull before restart."
        );

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

        session.nr_target_replicas = Some(plan.target_replicas());
        Ok(())
    }

    // #[tracing::instrument(level = "info", skip(self, _plan, session))]
    // async fn do_complete_culling<'s>(
    //     &mut self, _plan: &'s <Self as ScaleAction>::Plan, session: &'s mut Env<ActionSession>,
    // ) -> Result<(), ActError> {
    //     let patched_scale = session
    //         .kube
    //         .taskmanager()
    //         .deploy
    //         .patch_scale(0, &session.correlation)
    //         .instrument(tracing::info_span!(
    //             "complete_culling::patch_replicas",
    //             correlation=?session.correlation,
    //         ))
    //         .await?;
    //
    //     tracing::debug!(
    //         ?patched_scale,
    //         "complete culling patched taskmanager replicas"
    //     );
    //
    //     session.nr_target_replicas = Some(0);
    //     Ok(())
    // }
}
