use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use proctor::{AppData, Correlation};
use tokio::time::Instant;
use tracing_futures::Instrument;

use super::{ActionSession, ScaleAction};
use crate::kubernetes::{self, FlinkComponent, KubernetesContext, KubernetesError};
use crate::model::NrReplicas;
use crate::phases::act;
use crate::phases::act::{ActError, ActErrorDisposition};
use crate::phases::plan::{ScaleActionPlan, ScaleDirection};
use crate::{math, Env};

pub const ACTION_LABEL: &str = "patch_replicas";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchReplicas<P> {
    pub taskmanager_register_timeout: Duration,
    pub flink_polling_interval: Duration,
    marker: PhantomData<P>,
}

impl<P> PatchReplicas<P> {
    pub const fn from_settings(
        taskmanager_register_timeout: Duration, flink_polling_interval: Duration,
    ) -> Self {
        Self {
            taskmanager_register_timeout,
            flink_polling_interval,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P> ScaleAction for PatchReplicas<P>
where
    P: AppData + ScaleActionPlan + Correlation,
{
    type Plan = P;

    fn label(&self) -> &str {
        ACTION_LABEL
    }

    fn check_preconditions(&self, _session: &Env<ActionSession>) -> Result<(), ActError> {
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "PatchReplicas::execute", skip(self, plan))]
    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut Env<ActionSession>,
    ) -> Result<(), ActError> {
        let nr_target_replicas = plan.target_replicas();
        let correlation = session.correlation().relabel();

        let original_nr_replicas =
            session.kube.taskmanager().deploy.get_scale(&correlation).await?;
        tracing::info!(%nr_target_replicas, ?original_nr_replicas, "patching to scale taskmanager replicas");

        let nr_patched_replicas: Result<Option<NrReplicas>, KubernetesError> = session
            .kube
            .taskmanager()
            .deploy
            .patch_scale(nr_target_replicas, &correlation)
            .instrument(tracing::info_span!(
                "kubernetes::patch_replicas",
                ?correlation, %nr_target_replicas, ?original_nr_replicas
            ))
            .await;
        let _patched = match nr_patched_replicas {
            Err(err) => self.handle_error_on_patch_scale(err, plan, session).await,
            patched => patched,
        }?;

        let _done = match self.block_until_satisfied(plan, &session.kube).await {
            Err(err) => self.handle_error_on_patch_block(err, plan, session).await,
            done => done,
        }?;

        Ok(())
    }
}

impl<P> PatchReplicas<P>
where
    P: AppData + ScaleActionPlan + Correlation,
{
    #[tracing::instrument(
        level = "info",
        name = "patch_replicas::block_until_satisfied",
        skip(plan, kube)
    )]
    async fn block_until_satisfied(
        &self, plan: &<Self as ScaleAction>::Plan, kube: &KubernetesContext,
    ) -> Result<(), ActError> {
        let correlation = plan.correlation();
        let target_nr_task_managers = plan.target_replicas();
        let mut pods_by_status = None;
        // TODO: convert to kube::runtime watcher - see pod watch example.

        let api_constraints = kube.api_constraints();
        tracing::debug!(
            ?correlation,
            "budgeting {:?} to kubernetes for patch replicas with {:?} polling interval.",
            api_constraints.api_timeout,
            api_constraints.polling_interval
        );

        let mut action_satisfied = false;
        let start = Instant::now();
        while start.elapsed() < api_constraints.api_timeout {
            let (statuses, is_satisfied) =
                Self::do_assess_patch_completion(plan, kube, start).await?;
            pods_by_status.replace(statuses);
            action_satisfied = is_satisfied;
            if action_satisfied {
                break;
            }
            tokio::time::sleep(api_constraints.polling_interval).await;
        }

        if !action_satisfied {
            let status_counts: HashMap<String, usize> = pods_by_status
                .unwrap_or_default()
                .into_iter()
                .map(|(status, pods)| (status, pods.len()))
                .collect();

            return Err(ActError::Timeout(
                kube.api_constraints().api_timeout,
                format!(
                    "Timed out waiting for patch replicas to complete: nr_target={} :: status_counts={:?} \
                     [correlation={}]",
                    target_nr_task_managers, status_counts, correlation,
                ),
            ));
        }

        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        name = "patch_replicas::assess_patch_completion",
        skip()
    )]
    async fn do_assess_patch_completion(
        plan: &<Self as ScaleAction>::Plan, kube: &KubernetesContext, start: Instant,
    ) -> Result<(HashMap<String, Vec<Pod>>, bool), KubernetesError> {
        let correlation = plan.correlation();
        let target_nr_task_managers = plan.target_replicas();
        let kube_constraints = kube.api_constraints();
        let task_managers = kube.list_pods(FlinkComponent::TaskManager).await?;
        let pods_by_status = KubernetesContext::group_pods_by_status(&task_managers);

        let (nr_running, pod_status_counts) = Self::do_count_running_pods(&pods_by_status);

        let budget_remaining = kube_constraints.api_timeout - Instant::now().duration_since(start);

        let (is_satisfied, note) = if nr_running == target_nr_task_managers {
            (true, "patch replicas confirmed")
        } else if nr_running < target_nr_task_managers && plan.direction() == ScaleDirection::Up {
            (false, "expecting taskmanagers to be added")
        } else if target_nr_task_managers < nr_running && plan.direction() == ScaleDirection::Down {
            (false, "expecting taskmanagers to be removed")
        } else {
            (
                false,
                "unexpected number of taskmanagers. Consider adjusting `action.taskmanager.label_selector` \
                setting to more specifically match taskmanager pods associated with the targets flink cluster. \
                Will continue monitoring in case it settles to expected levels.",
            )
        };

        tracing::info!(
            ?correlation,
            ?pod_status_counts,
            scale_direction=%plan.direction(),
            %note,
            "patch_replicas: kube found {} taskmanagers in cluster: {} running of {} requested - budget remaining: \
             {:?}",
            task_managers.len(),
            nr_running,
            target_nr_task_managers,
            budget_remaining
        );

        Ok((pods_by_status, is_satisfied))
    }

    fn do_count_running_pods(
        pods_by_status: &HashMap<String, Vec<Pod>>,
    ) -> (NrReplicas, HashMap<String, NrReplicas>) {
        let pod_status_counts: HashMap<String, NrReplicas> = pods_by_status
            .iter()
            .map(|(status, pods)| {
                (
                    status.clone(),
                    NrReplicas::new(math::saturating_usize_to_u32(pods.len())),
                )
            })
            .collect();

        let nr_running = pods_by_status
            .get(kubernetes::RUNNING_STATUS)
            .map(|pods| NrReplicas::new(math::saturating_usize_to_u32(pods.len())))
            .unwrap_or(NrReplicas::NONE);

        (nr_running, pod_status_counts)
    }

    #[tracing::instrument(level = "warn", skip(self, plan, session))]
    async fn handle_error_on_patch_scale<'s>(
        &self, error: KubernetesError, plan: &'s P, session: &'s mut Env<ActionSession>,
    ) -> Result<Option<NrReplicas>, KubernetesError> {
        let track = format!("{}::patch_scale", self.label());
        tracing::error!(
            ?error, ?plan, ?session, correlation=%session.correlation(), %track,
            "error on patch replicas scale"
        );
        act::track_act_errors(&track, Some(&error), ActErrorDisposition::Failed, plan);
        Err(error)
    }

    #[tracing::instrument(level = "warn", skip(self, plan, session))]
    async fn handle_error_on_patch_block<'s>(
        &self, error: ActError, plan: &'s P, session: &'s mut Env<ActionSession>,
    ) -> Result<(), ActError> {
        let track = format!("{}::patch_block", self.label());
        match error {
            ActError::Timeout(duration, ref message) => {
                tracing::error!(
                    ?plan, ?session, patch_replicas_budget=?duration, correlation=%session.correlation(), %track,
                    "{message} - moving on to next action."
                );
                act::track_act_errors(&track, Some(&error), ActErrorDisposition::Ignored, plan);
                Ok(())
            },
            other => {
                tracing::error!(
                    error=?other, ?plan, ?session, correlation=%session.correlation(), %track,
                    "patch_block error"
                );
                act::track_act_errors(&track, Some(&other), ActErrorDisposition::Failed, plan);
                Err(other)
            },
        }
    }
}
