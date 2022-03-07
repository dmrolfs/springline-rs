use crate::kubernetes;
use crate::kubernetes::DeployApi;
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::kubernetes::TaskmanagerContext;
use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::{self, ActError};
use crate::settings::{self, KubernetesApiConstraints, KubernetesDeployResource};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client};
use once_cell::sync::Lazy;
use proctor::AppData;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

pub const ACTION_LABEL: &str = "patch_replicas";

#[derive(Debug, Clone)]
pub struct PatchReplicas<P> {
    pub cluster_label: String,
    pub label_selector: String,
    pub deploy_resource: KubernetesDeployResource,
    pub api_constraints: KubernetesApiConstraints,
    pub taskmanagers: TaskmanagerContext,
    marker: std::marker::PhantomData<P>,
}

impl<P> PatchReplicas<P> {
    pub fn new(cluster_label: &str, scale_target: &settings::TaskmanagerContext, kube: &Client) -> Self {
        let label_selector = scale_target.label_selector.clone();
        let deploy_resource = scale_target.deploy_resource.clone();
        let api_constraints = scale_target.kubernetes_api;
        let taskmanagers = TaskmanagerContext {
            deploy: DeployApi::from_kubernetes_resource(&scale_target.deploy_resource, kube),
            pods: Api::default_namespaced(kube.clone()),
            params: ListParams {
                label_selector: Some(scale_target.label_selector.clone()),
                timeout: Some(api_constraints.api_timeout.as_secs() as u32),
                ..Default::default()
            },
        };

        let marker = std::marker::PhantomData;
        Self {
            cluster_label: cluster_label.to_string(),
            label_selector,
            deploy_resource,
            api_constraints,
            taskmanagers,
            marker,
        }
    }
}

#[async_trait]
impl<P> ScaleAction<P> for PatchReplicas<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", name = "PatchReplicas::execute", skip(self))]
    async fn execute(&mut self, session: ActionSession<P>) -> Result<ActionSession<P>, (ActError, ActionSession<P>)> {
        let timer = start_flink_taskmanager_patch_replicas_timer(self.cluster_label.as_str());

        // async fn execute(&mut self, plan: &P) -> Result<Duration, ActError> {
        let correlation = session.correlation();
        let nr_target_taskmanagers = session.plan.target_replicas();

        let original_nr_taskmanager_replicas = self
            .taskmanagers
            .deploy
            .get_scale(&correlation)
            .await
            .map_err(|err| (err.into(), session.clone()))?;
        tracing::info!(%nr_target_taskmanagers, ?original_nr_taskmanager_replicas, "patching to scale taskmanager replicas");

        let patched_nr_taskmanager_replicas = self
            .taskmanagers
            .deploy
            .patch_scale(nr_target_taskmanagers, &correlation)
            .await
            .map_err(|err| (err.into(), session.clone()))?;
        tracing::info!(
            %nr_target_taskmanagers, ?original_nr_taskmanager_replicas, ?patched_nr_taskmanager_replicas,
            "patched taskmanager replicas"
        );

        self.block_until_satisfied(&session.plan)
            .await
            .map_err(|err| (err, session.clone()))?;

        let patch_duration = Duration::from_secs_f64(timer.stop_and_record());
        Ok(session.with_duration(ACTION_LABEL, patch_duration))
    }
}

impl<P> PatchReplicas<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", skip(self))]
    async fn block_until_satisfied(&self, plan: &P) -> Result<(), ActError> {
        let target_nr_task_managers = plan.target_replicas();
        let start = Instant::now();
        let mut action_satisfied = false;
        let mut pods_by_status = None;
        //TODO: convert to kube::runtime watcher - see pod watch example.
        while Instant::now().duration_since(start) < self.api_constraints.api_timeout {
            let taskmanagers = self.taskmanagers.list_pods().await?;
            pods_by_status = Some(Self::group_pods_by_status(taskmanagers));
            let running = pods_by_status
                .as_ref()
                .and_then(|ps| ps.get(kubernetes::RUNNING_STATUS).map(|pods| pods.len()))
                .unwrap_or(0);

            if running == target_nr_task_managers {
                action_satisfied = true;
                break;
            }

            tokio::time::sleep(self.api_constraints.polling_interval).await;
        }

        if !action_satisfied {
            let status_counts: HashMap<String, usize> = pods_by_status
                .unwrap_or_else(HashMap::new)
                .into_iter()
                .map(|(status, pods)| (status, pods.len()))
                .collect();

            return Err(ActError::Timeout(
                self.api_constraints.api_timeout,
                format!(
                    "Timed out waiting for patch replicas to complete: nr_target={} :: status_counts={:?}",
                    target_nr_task_managers, status_counts,
                ),
            ));
        }

        Ok(())
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
