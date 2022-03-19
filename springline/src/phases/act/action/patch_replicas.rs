use super::{ActionSession, ScaleAction};
use crate::kubernetes::{self, FlinkComponent, KubernetesContext};
use crate::phases::act;
use crate::phases::act::CorrelationId;
use crate::phases::act::{ActError, ScaleActionPlan};
use crate::phases::plan::ScalePlan;
use crate::settings::Settings;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;
use tracing_futures::Instrument;

pub const ACTION_LABEL: &str = "patch_replicas";

#[derive(Debug)]
pub struct PatchReplicas {
    pub settle_duration: Duration,
}

impl PatchReplicas {
    pub const fn from_settings(settings: &Settings) -> Self {
        Self {
            settle_duration: settings.kubernetes.patch_settle_duration,
        }
    }
}

#[async_trait]
impl ScaleAction for PatchReplicas {
    type In = ScalePlan;

    #[tracing::instrument(level = "info", name = "PatchReplicas::execute", skip(self))]
    async fn execute<'s>(&self, plan: &'s Self::In, session: &'s mut ActionSession) -> Result<(), ActError> {
        let timer = act::start_scale_action_timer(session.cluster_label(), ACTION_LABEL);

        let correlation = session.correlation();
        let nr_target_taskmanagers = plan.target_replicas();

        let scale_span = tracing::info_span!("kubernetes::get_scale", ?correlation);
        let original_nr_taskmanager_replicas = session
            .kube
            .taskmanager()
            .deploy
            .get_scale(&correlation)
            .instrument(scale_span)
            .await?;
        tracing::info!(%nr_target_taskmanagers, ?original_nr_taskmanager_replicas, "patching to scale taskmanager replicas");

        let patch_span = tracing::info_span!("kubernetes::patch_replicas", correlation = ?correlation, %nr_target_taskmanagers, ?original_nr_taskmanager_replicas);
        let patched_nr_taskmanager_replicas = session
            .kube
            .taskmanager()
            .deploy
            .patch_scale(nr_target_taskmanagers, &correlation)
            .instrument(patch_span)
            .await?;
        tracing::info!(
            %nr_target_taskmanagers, ?original_nr_taskmanager_replicas, ?patched_nr_taskmanager_replicas,
            "patched taskmanager replicas"
        );

        if let Err(error) = self.block_until_satisfied(plan, &session.kube).await {
            match error {
                ActError::Timeout(duration, message) => tracing::error!(
                    patch_replicas_budget=?duration,
                    "{message} - moving on to next action."
                ),
                err => return Err(err),
            }
        }

        session.mark_duration(ACTION_LABEL, Duration::from_secs_f64(timer.stop_and_record()));
        Ok(())
    }
}

impl PatchReplicas {
    #[tracing::instrument(level = "debug", name = "patch_replicase::block_until_satisfied", skip(kube))]
    async fn block_until_satisfied(
        &self, plan: &<Self as ScaleAction>::In, kube: &KubernetesContext,
    ) -> Result<(), ActError> {
        let correlation = plan.correlation();
        let target_nr_task_managers = plan.target_replicas();
        let start = Instant::now();
        let mut pods_by_status = None;
        //TODO: convert to kube::runtime watcher - see pod watch example.

        let api_constraints = kube.api_constraints();
        tracing::debug!(
            ?correlation,
            "budgeting {:?} to kubernetes for patch replicas with {:?} polling interval.",
            api_constraints.api_timeout,
            api_constraints.polling_interval
        );

        let mut action_satisfied = false;
        while Instant::now().duration_since(start) < api_constraints.api_timeout {
            let task_managers = kube.list_pods(FlinkComponent::TaskManager).await?;
            pods_by_status = Some(Self::group_pods_by_status(&task_managers));

            let (nr_running, pod_status_counts) = Self::do_count_running_pods(pods_by_status.as_ref());

            let budget_remaining = api_constraints.api_timeout - Instant::now().duration_since(start);
            tracing::info!(
                ?correlation, ?pod_status_counts,
                "patch_replicas: kube found {} taskmanagers in cluster: {} running of {} requested - budget remaining: {:?}",
                task_managers.len(), nr_running, target_nr_task_managers, budget_remaining
            );

            if nr_running == target_nr_task_managers {
                action_satisfied = true;
                break;
            }

            tokio::time::sleep(api_constraints.polling_interval).await;
        }

        if !action_satisfied {
            let status_counts: HashMap<String, usize> = pods_by_status
                .unwrap_or_else(HashMap::new)
                .into_iter()
                .map(|(status, pods)| (status, pods.len()))
                .collect();

            return Err(ActError::Timeout(
                kube.api_constraints().api_timeout,
                format!(
                    "Timed out waiting for patch replicas to complete: nr_target={} :: status_counts={:?} [correlation={}]",
                    target_nr_task_managers, status_counts, correlation,
                ),
            ));
        }

        self.do_let_patch_settle(correlation).await;

        Ok(())
    }

    async fn do_let_patch_settle(&self, correlation: &CorrelationId) {
        let settle_span = tracing::debug_span!(
        "letting the cluster settle to minimize flink restart failures",
            settle_duration=?self.settle_duration,
            correlation = ?correlation,
        );
        tokio::time::sleep(self.settle_duration).instrument(settle_span).await;
    }

    fn do_count_running_pods(pods_by_status: Option<&HashMap<String, Vec<Pod>>>) -> (usize, HashMap<String, usize>) {
        if pods_by_status.is_none() {
            return (0, HashMap::new());
        }

        let pod_status_counts: HashMap<String, usize> = pods_by_status
            .as_ref()
            .unwrap()
            .iter()
            .map(|(status, pods)| (status.clone(), pods.len()))
            .collect();

        let nr_running = pods_by_status
            .as_ref()
            .and_then(|ps| ps.get(kubernetes::RUNNING_STATUS).map(|pods| pods.len()))
            .unwrap_or(0);

        (nr_running, pod_status_counts)
    }

    fn group_pods_by_status(pods: &[Pod]) -> HashMap<String, Vec<Pod>> {
        let mut result = HashMap::new();

        for p in pods {
            let status = p
                .status
                .as_ref()
                .and_then(|ps| ps.phase.clone())
                .unwrap_or_else(|| kubernetes::UNKNOWN_STATUS.to_string());

            let pods_entry = result.entry(status).or_insert_with(Vec::new);
            pods_entry.push(p.clone());
        }

        result
    }
}
