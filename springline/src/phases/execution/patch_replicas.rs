use std::error::Error;
use std::fmt;
use std::sync::Arc;

use super::protocol;
use crate::phases::execution::ExecutionEvent;
use crate::phases::governance::GovernanceOutcome;
use crate::phases::MetricCatalog;
use crate::settings::{ExecutionSettings, KubernetesWorkloadResource};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use either::{Either, Left, Right};
use k8s_openapi::api::apps::v1::StatefulSet;
use pretty_snowflake::Id;
use proctor::elements::Timestamp;
use proctor::error::{MetricLabel, ProctorError};
use proctor::graph::stage::Stage;
use proctor::graph::{Inlet, Port, SinkShape, PORT_DATA};
use proctor::{AppData, ProctorResult, SharedString};
use serde_json::json;
use thiserror::Error;
use tokio::sync::broadcast;

const STAGE_NAME: &str = "execute_scaling";

#[derive(Debug, Error)]
pub enum ExecutionPhaseError {
    #[error("failure in kubernetes client: {0}")]
    Kube(#[from] kube::Error),

    #[error("failure occurred in the PatchReplicas inlet port: {0}")]
    Port(#[from] proctor::error::PortError),

    #[error("failure occurred while processing data in the PatchReplicas stage: {0}")]
    Stage(#[from] anyhow::Error),
}

impl MetricLabel for ExecutionPhaseError {
    fn slug(&self) -> SharedString {
        SharedString::Borrowed("execution")
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::Kube(_) => Left("kubernetes".into()),
            Self::Port(e) => Right(Box::new(e)),
            Self::Stage(_) => Left("stage".into()),
        }
    }
}

pub trait ExecutionScalePlan {
    fn correlation(&self) -> Id<MetricCatalog>;
    fn recv_timestamp(&self) -> Timestamp;
    fn current_replicas(&self) -> usize;
    fn target_replicas(&self) -> usize;
}

impl ExecutionScalePlan for GovernanceOutcome {
    fn correlation(&self) -> Id<MetricCatalog> {
        self.correlation_id.clone()
    }

    fn recv_timestamp(&self) -> Timestamp {
        self.recv_timestamp
    }

    fn current_replicas(&self) -> usize {
        self.current_nr_task_managers as usize
    }

    fn target_replicas(&self) -> usize {
        self.target_nr_task_managers as usize
    }
}

// #[derive(Debug)]
pub struct PatchReplicas<In> {
    kube: kube::Client,
    workload_resource: KubernetesWorkloadResource,
    inlet: Inlet<In>,
    pub tx_execution_monitor: broadcast::Sender<Arc<protocol::ExecutionEvent<In>>>,
}

impl<In> PatchReplicas<In> {
    #[tracing::instrument(level = "info", skip(kube))]
    pub fn new(kube: kube::Client, exec_settings: &ExecutionSettings) -> Self {
        let workload_resource = exec_settings.k8s_workload_resource.clone();
        let (tx_execution_monitor, _) = broadcast::channel(num_cpus::get() * 2);

        Self {
            kube,
            workload_resource,
            inlet: Inlet::new(STAGE_NAME, PORT_DATA),
            tx_execution_monitor,
        }
    }
}

impl<In> fmt::Debug for PatchReplicas<In> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PatchReplicas")
            .field("workload_resource", &self.workload_resource)
            .field("inlet", &self.inlet)
            .finish()
    }
}

impl<In> proctor::graph::stage::WithMonitor for PatchReplicas<In> {
    type Receiver = protocol::ExecutionMonitor<In>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_execution_monitor.subscribe()
    }
}

impl<In> SinkShape for PatchReplicas<In> {
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In> Stage for PatchReplicas<In>
where
    In: AppData + ExecutionScalePlan,
{
    #[inline]
    fn name(&self) -> SharedString {
        SharedString::Borrowed(STAGE_NAME)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run patch replicas", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }
}

impl<In> PatchReplicas<In>
where
    In: AppData + ExecutionScalePlan,
{
    #[inline]
    async fn do_check(&self) -> Result<(), ExecutionPhaseError> {
        self.inlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), ExecutionPhaseError> {
        let workload_resource = &self.workload_resource;
        let mut inlet = self.inlet.clone();
        let stateful_set: kube::Api<StatefulSet> = kube::Api::default_namespaced(self.kube.clone());

        while let Some(plan) = inlet.recv().await {
            let _timer = proctor::graph::stage::start_stage_eval_time(STAGE_NAME);

            match Self::patch_replicas_for(&plan, &stateful_set, workload_resource).await {
                Ok(_) => {
                    tracing::warn!(scale_plan=?plan, "EXECUTE SCALE PLAN: task manager replicas patched!");
                    self.notify_execution_succeeded(plan)
                },
                Err(err) => {
                    tracing::error!(error=?err, ?plan, "failed to patch replicas for plan");
                    self.notify_execution_failed(plan, err);
                },
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    fn notify_execution_succeeded(&self, plan: In) {
        let correlation = plan.correlation();
        let recv_timestamp = plan.recv_timestamp();
        match self
            .tx_execution_monitor
            .send(Arc::new(ExecutionEvent::PlanExecuted(plan)))
        {
            Ok(recipients) => {
                tracing::info!(?correlation, %recv_timestamp, "published PlanExecuted to {} recipients", recipients);
            },
            Err(err) => {
                tracing::error!(error=?err, ?correlation, %recv_timestamp, "failed to publish PlanExecuted event.");
            },
        }
    }

    #[tracing::instrument(level = "info", skip(self, error))]
    fn notify_execution_failed<E>(&self, plan: In, error: E)
    where
        E: Error + MetricLabel,
    {
        let correlation = plan.correlation();
        let recv_timestamp = plan.recv_timestamp();
        let label = error.label();
        match self.tx_execution_monitor.send(Arc::new(ExecutionEvent::PlanFailed {
            plan,
            error_metric_label: label.into(),
        })) {
            Ok(recipients) => {
                tracing::info!(?correlation, %recv_timestamp, execution_error=?error, "published PlanFailed to {} recipients", recipients);
            },
            Err(err) => {
                tracing::error!(?correlation, %recv_timestamp, execution_error=?error, publish_error=?err, "failed to publish PlanFailed event.");
            },
        }
    }

    #[tracing::instrument(level = "info", skip(stateful_set))]
    async fn patch_replicas_for(
        plan: &In, stateful_set: &kube::Api<StatefulSet>, workload_resource: &KubernetesWorkloadResource,
    ) -> Result<(), ExecutionPhaseError> {
        let name = workload_resource.get_name();
        let scale = stateful_set.get_scale(name).await?;
        tracing::info!("scale recv for {}: {:?}", name, scale.spec);

        let patch_params = kube::api::PatchParams::default();
        let fs = json!({ "spec": { "replicas": plan.target_replicas() }});
        let patched_scale = stateful_set
            .patch_scale(name, &patch_params, &kube::api::Patch::Merge(&fs))
            .await?;
        tracing::info!("Patched scale for {}: {:?}", name, patched_scale.spec);
        Ok(())
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), ExecutionPhaseError> {
        tracing::trace!("closing patch replicas execution phase inlet.");
        self.inlet.close().await;
        Ok(())
    }
}
