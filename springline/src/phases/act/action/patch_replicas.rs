use super::{ActionSession, ScaleAction};
use crate::kubernetes::{self, FlinkComponent, KubernetesContext};
use crate::model::CorrelationId;
use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;
use crate::phases::plan::ScalePlan;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

pub const ACTION_LABEL: &str = "patch_replicas";

#[derive(Debug)]
pub struct PatchReplicas;
// pub cluster_label: String,
// pub kube: KubernetesContext,
// pub label_selector: String,
// pub deploy_resource: KubernetesDeployResource,
// pub api_constraints: KubernetesApiConstraints,
// pub taskmanagers: TaskmanagerContext,
// marker: std::marker::PhantomData<P>,
// }

// impl PatchReplicas<P> {
//     pub fn new(cluster_label: &str, kube: KubernetesContext) -> Self {
//         // let label_selector = scale_target.label_selector.clone();
//         // let deploy_resource = scale_target.deploy_resource.clone();
//         // let api_constraints = scale_target.kubernetes_api;
//
//         // let taskmanagers = TaskmanagerContext {
//         //     deploy: DeployApi::from_kubernetes_resource(&scale_target.deploy_resource, kube.clone()),
//         //     pods: Api::default_namespaced(kube),
//         //     params: ListParams {
//         //         label_selector: Some(scale_target.label_selector.clone()),
//         //         timeout: Some(api_constraints.api_timeout.as_secs() as u32),
//         //         ..Default::default()
//         //     },
//         // };
//
//         let marker = std::marker::PhantomData;
//         Self {
//             cluster_label: cluster_label.to_string(),
//             kube,
//             // label_selector,
//             // deploy_resource,
//             // api_constraints,
//             // taskmanagers,
//             marker,
//         }
//     }
// }

#[async_trait]
impl ScaleAction for PatchReplicas
// where
//     P: AppData + ScaleActionPlan,
{
    type In = ScalePlan;
    // type Plan = GovernanceOutcome;

    #[tracing::instrument(level = "info", name = "PatchReplicas::execute", skip(self))]
    async fn execute<'s>(&self, plan: &'s Self::In, session: &'s mut ActionSession) -> Result<(), ActError> {
        let timer = start_flink_taskmanager_patch_replicas_timer(session.cluster_label());

        // async fn execute(&mut self, plan: &P) -> Result<Duration, ActError> {
        let correlation = session.correlation();
        let nr_target_taskmanagers = plan.target_replicas();

        let original_nr_taskmanager_replicas = session.kube.taskmanager().deploy.get_scale(&correlation).await?;
        tracing::info!(%nr_target_taskmanagers, ?original_nr_taskmanager_replicas, "patching to scale taskmanager replicas");

        let patched_nr_taskmanager_replicas = session
            .kube
            .taskmanager()
            .deploy
            .patch_scale(nr_target_taskmanagers, &correlation)
            .await?;
        tracing::info!(
            %nr_target_taskmanagers, ?original_nr_taskmanager_replicas, ?patched_nr_taskmanager_replicas,
            "patched taskmanager replicas"
        );

        Self::block_until_satisfied(&session.kube, plan).await?;

        session.mark_duration(ACTION_LABEL, Duration::from_secs_f64(timer.stop_and_record()));
        Ok(())
    }
}

impl PatchReplicas
// where
//     P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", skip(kube))]
    async fn block_until_satisfied(kube: &KubernetesContext, plan: &<Self as ScaleAction>::In) -> Result<(), ActError> {
        let correlation = plan.correlation();
        let target_nr_task_managers = plan.target_replicas();
        let start = Instant::now();
        let mut action_satisfied = false;
        let mut pods_by_status = None;
        //TODO: convert to kube::runtime watcher - see pod watch example.

        let api_constraints = kube.api_constraints();
        tracing::warn!(
            %correlation,
            "DMR: budgeting {:?} to kubernetes for patch replicas with {:?} polling interval.",
            api_constraints.api_timeout, api_constraints.polling_interval
        );

        while Instant::now().duration_since(start) < api_constraints.api_timeout {
            let task_managers = kube.list_pods(FlinkComponent::TaskManager).await?;
            pods_by_status = Some(Self::group_pods_by_status(task_managers));

            let nr_running = Self::do_count_running_pods(pods_by_status.as_ref(), target_nr_task_managers, correlation);

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

        Ok(())
    }

    fn do_count_running_pods(
        pods_by_status: Option<&HashMap<String, Vec<Pod>>>, target_nr_task_managers: usize, correlation: &CorrelationId,
    ) -> usize {
        if pods_by_status.is_none() {
            return 0;
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

        tracing::warn!(
            %correlation, ?pod_status_counts, %nr_running, %target_nr_task_managers,
            "DMR: PATCH_REPLICAS: pods by status"
        );

        nr_running
    }

    fn group_pods_by_status(pods: Vec<Pod>) -> HashMap<String, Vec<Pod>> {
        let mut result = HashMap::new();

        for p in pods.into_iter() {
            let status = p
                .status
                .as_ref()
                .and_then(|ps| ps.phase.clone())
                .unwrap_or_else(|| kubernetes::UNKNOWN_STATUS.to_string());

            let pods = result.entry(status).or_insert_with(Vec::new);
            pods.push(p);
        }

        result
    }
}

#[inline]
fn start_flink_taskmanager_patch_replicas_timer(cluster_label: &str) -> HistogramTimer {
    FLINK_TASKMANAGER_PATCH_REPLICAS_TIME
        .with_label_values(&[cluster_label])
        .start_timer()
}

pub static FLINK_TASKMANAGER_PATCH_REPLICAS_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_taskmanager_patch_replicas_time_seconds",
            "Time spent patching taskmanager replicas and confirming they are running",
        )
        .buckets(vec![10., 15., 20., 30., 40., 50., 100., 250., 500., 750., 1000.]),
        &["label"],
    )
    .expect("failed creating flink_taskmanager_patch_replicas_time_seconds histogram metric")
});
