use std::error::Error;
use std::fmt;
use std::sync::Arc;

use super::{action, protocol};
use crate::phases::act::action::{ActionSession, ScaleAction};
use crate::phases::act::{ActError, ActEvent};
use crate::phases::governance::GovernanceOutcome;
use crate::phases::MetricCatalog;
use crate::settings::{ActionSettings, KubernetesDeployResource, TaskmanagerContext};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use k8s_openapi::api::apps::v1::StatefulSet;
use pretty_snowflake::Id;
use proctor::elements::Timestamp;
use proctor::error::{MetricLabel, ProctorError};
use proctor::graph::stage::Stage;
use proctor::graph::{Inlet, Port, SinkShape, PORT_DATA};
use proctor::{AppData, ProctorResult, SharedString};
use serde_json::json;
use tokio::sync::broadcast;
use tracing::Instrument;

const STAGE_NAME: &str = "execute_scaling";

pub trait ScaleActionPlan {
    fn correlation(&self) -> Id<MetricCatalog>;
    fn recv_timestamp(&self) -> Timestamp;
    fn current_replicas(&self) -> usize;
    fn target_replicas(&self) -> usize;
}

impl ScaleActionPlan for GovernanceOutcome {
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
pub struct ScaleActuator<In> {
    kube: kube::Client,
    action: Box<dyn ScaleAction<In>>,
    context: TaskmanagerContext,
    inlet: Inlet<In>,
    pub tx_action_monitor: broadcast::Sender<Arc<protocol::ActEvent<In>>>,
}

impl<In> ScaleActuator<In>
where
    In: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "info", skip(kube))]
    pub fn new(kube: kube::Client, settings: &ActionSettings) -> Self {
        let context = settings.taskmanager.clone();
        let (tx_action_monitor, _) = broadcast::channel(num_cpus::get() * 2);
        let action = action::make_action(&kube, settings);

        Self {
            kube,
            action,
            context,
            inlet: Inlet::new(STAGE_NAME, PORT_DATA),
            tx_action_monitor,
        }
    }
}

impl<In> fmt::Debug for ScaleActuator<In> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScaleActuator")
            .field("action", &self.action)
            .field("context", &self.context)
            .field("inlet", &self.inlet)
            .finish()
    }
}

impl<In> proctor::graph::stage::WithMonitor for ScaleActuator<In> {
    type Receiver = protocol::ActMonitor<In>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_action_monitor.subscribe()
    }
}

impl<In> SinkShape for ScaleActuator<In> {
    type In = In;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In> Stage for ScaleActuator<In>
where
    In: AppData + ScaleActionPlan,
{
    #[inline]
    fn name(&self) -> SharedString {
        SharedString::Borrowed(STAGE_NAME)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await.map_err(|err| ProctorError::Phase(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run patch replicas", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await.map_err(|err| ProctorError::Phase(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await.map_err(|err| ProctorError::Phase(err.into()))?;
        Ok(())
    }
}

impl<In> ScaleActuator<In>
where
    In: AppData + ScaleActionPlan,
{
    #[inline]
    async fn do_check(&self) -> Result<(), ActError> {
        self.inlet.check_attachment().await?;
        Ok(())
    }

    #[inline]
    async fn do_run(&mut self) -> Result<(), ActError> {
        // let workload_resource = &self.workload_resource;
        let mut inlet = self.inlet.clone();
        // let stateful_set: kube::Api<StatefulSet> = kube::Api::default_namespaced(self.kube.clone());

        while let Some(plan) = inlet.recv().await {
            let _timer = proctor::graph::stage::start_stage_eval_time(STAGE_NAME);
            let session = ActionSession::new(plan);
            match self.action.execute(session).await {
                Ok(ctx) => {
                    self.notify_action_succeeded(ctx);
                },
                Err((err, ctx)) => {
                    tracing::error!(error=?err, context=?ctx, "failure in scale action - dropping.");
                    self.notify_action_failed(ctx, err);
                },
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    fn notify_action_succeeded(&self, session: ActionSession<In>) {
        let correlation = session.correlation();
        let recv_timestamp = session.plan.recv_timestamp();
        match self.tx_action_monitor.send(Arc::new(ActEvent::PlanExecuted(session.plan))) {
            Ok(recipients) => {
                tracing::info!(?correlation, %recv_timestamp, "published PlanExecuted to {} recipients", recipients);
            },
            Err(err) => {
                tracing::error!(error=?err, ?correlation, %recv_timestamp, "failed to publish PlanExecuted event.");
            },
        }
    }

    #[tracing::instrument(level = "info", skip(self, error))]
    fn notify_action_failed<E>(&self, session: ActionSession<In>, error: E)
    where
        E: Error + MetricLabel,
    {
        let correlation = session.correlation();
        let recv_timestamp = session.plan.recv_timestamp();
        let label = error.label();
        match self.tx_action_monitor.send(Arc::new(ActEvent::PlanFailed {
            plan: session.plan,
            error_metric_label: label.into(),
        })) {
            Ok(recipients) => {
                tracing::info!(?correlation, %recv_timestamp, action_error=?error, "published PlanFailed to {} recipients", recipients);
            },
            Err(err) => {
                tracing::error!(?correlation, %recv_timestamp, action_error=?error, publish_error=?err, "failed to publish PlanFailed event.");
            },
        }
    }

    #[tracing::instrument(level = "info", skip(stateful_set))]
    async fn patch_replicas_for(
        plan: &In, stateful_set: &kube::Api<StatefulSet>, workload_resource: &KubernetesDeployResource,
    ) -> Result<(), ActError> {
        fn convert_kube_error(error: kube::Error) -> ActError {
            match error {
                kube::Error::Api(resp) => resp.into(),
                err => err.into(),
            }
        }

        let name = workload_resource.get_name();
        let target_nr_task_managers = plan.target_replicas();

        let k8s_get_scale_span = tracing::info_span!(
            "Kubernetes Admin Server",
            phase=%"act",
            action=%"get_scale",
            correlation=%plan.correlation()
        );

        let original_scale = stateful_set
            .get_scale(name)
            .instrument(k8s_get_scale_span)
            .await
            .map_err(convert_kube_error)?;
        tracing::info!("scale recv for {}: {:?}", name, original_scale.spec);

        let k8s_patch_scale_span = tracing::info_span!(
            "Kubernetes Admin Server",
            phase=%"act",
            action=%"patch_scale",
            correlation=%plan.correlation()
        );

        let patch_params = kube::api::PatchParams::default();
        let fs = json!({ "spec": { "replicas": target_nr_task_managers }});
        let patched_scale = stateful_set
            .patch_scale(name, &patch_params, &kube::api::Patch::Merge(&fs))
            .instrument(k8s_patch_scale_span)
            .await
            .map_err(convert_kube_error)?;

        tracing::info!("Patched scale for {}: {:?}", name, patched_scale.spec);
        if let Some(original_nr_task_managers) = original_scale.spec.clone().and_then(|s| s.replicas) {
            if target_nr_task_managers == original_nr_task_managers as usize {
                tracing::warn!(
                    %target_nr_task_managers,
                    "patched scale when current cluster size equals scale plan to be executed."
                );
            }
        }
        Ok(())
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), ActError> {
        tracing::trace!("closing patch replicas act phase inlet.");
        self.inlet.close().await;
        Ok(())
    }
}
