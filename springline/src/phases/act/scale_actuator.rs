use std::error::Error;
use std::fmt;
use std::sync::Arc;

use super::action::{self, ActionSession, CompositeAction, ScaleAction};
use super::{protocol, ActError, ActEvent, ScaleActionPlan};
use crate::settings::Settings;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;

use crate::flink::FlinkContext;
use crate::kubernetes::KubernetesContext;
use crate::phases::plan::ScalePlan;
use proctor::error::{MetricLabel, ProctorError};
use proctor::graph::stage::Stage;
use proctor::graph::{Inlet, Port, SinkShape, PORT_DATA};
use proctor::{AppData, ProctorResult, SharedString};
use tokio::sync::broadcast;

const STAGE_NAME: &str = "execute_scaling";

pub struct ScaleActuator<In> {
    kube: KubernetesContext,
    flink: FlinkContext,
    action: Box<dyn ScaleAction<In = In>>,
    inlet: Inlet<In>,
    pub tx_action_monitor: broadcast::Sender<Arc<protocol::ActEvent<In>>>,
}

impl ScaleActuator<ScalePlan> {
    #[tracing::instrument(level = "info", skip(kube))]
    pub fn new(kube: KubernetesContext, flink: FlinkContext, settings: &Settings) -> Self {
        let flink_action_settings = &settings.action.flink;

        let composite: CompositeAction<ScalePlan> = action::CompositeAction::default()
            .add_action_step(action::PrepareData)
            .add_action_step(action::TriggerSavepoint::from_settings(flink_action_settings))
            .add_action_step(action::PatchReplicas)
            .add_action_step(action::RestartJobs::from_settings(flink_action_settings));

        let (tx_action_monitor, _) = broadcast::channel(num_cpus::get() * 2);

        Self {
            kube,
            flink,
            action: Box::new(composite),
            inlet: Inlet::new(STAGE_NAME, PORT_DATA),
            tx_action_monitor,
        }
    }
}

impl<In> fmt::Debug for ScaleActuator<In> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScaleActuator")
            .field("action", &self.action)
            .field("kube", &self.kube)
            .field("flink", &self.flink)
            // .field("context", &self.context)
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

    #[tracing::instrument(level = "info", name = "run act scale actuator", skip(self))]
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
        let mut inlet = self.inlet.clone();

        while let Some(plan) = inlet.recv().await {
            self.notify_action_started(plan.clone());
            let _timer = proctor::graph::stage::start_stage_eval_time(STAGE_NAME);
            let mut session = ActionSession::new(plan.correlation().clone(), self.kube.clone(), self.flink.clone());
            match self.action.execute(&plan, &mut session).await {
                Ok(_) => {
                    self.notify_action_succeeded(plan, session);
                },
                Err(err) => {
                    tracing::error!(error=?err, ?session, "failure in scale action - dropping.");
                    self.notify_action_failed(plan, session, err);
                },
            }
        }

        Ok(())
    }

    #[tracing::instrument(
        level = "info",
        skip(self, plan),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp())
    )]
    fn notify_action_started(&self, plan: In) {
        tracing::info!(?plan, "scale action started");
        match self.tx_action_monitor.send(Arc::new(ActEvent::PlanActionStarted(plan))) {
            Ok(nr_recipients) => tracing::info!("published PlanActionStarted event to {nr_recipients} recipients."),
            Err(err) => tracing::error!(error=?err, "failed to publish PlanActionStarted event."),
        }
    }

    #[tracing::instrument(
        level = "info",
        skip(self, plan, session),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp())
    )]
    fn notify_action_succeeded(&self, plan: In, session: ActionSession) {
        let event = Arc::new(ActEvent::PlanExecuted { plan, durations: session.durations.clone() });

        match self.tx_action_monitor.send(event) {
            Ok(nr_recipients) => tracing::info!(
                action_durations=?session.durations,
                "published PlanExecuted event to {nr_recipients} recipients."
            ),
            Err(err) => tracing::error!(error=?err, "failed to publish PlanExecuted event."),
        }
    }

    #[tracing::instrument(
        level = "info",
        skip(self, plan, session, error),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp(), error_label=%error.label(),)
    )]
    fn notify_action_failed<E>(&self, plan: In, session: ActionSession, error: E)
    where
        E: Error + MetricLabel,
    {
        let label = error.label();
        match self.tx_action_monitor.send(Arc::new(ActEvent::PlanFailed {
            plan,
            error_metric_label: label.into(),
        })) {
            Ok(recipients) => tracing::info!(
                action_durations=?session.durations, action_error=?error,
                "published PlanFailed to {} recipients", recipients
            ),
            Err(err) => tracing::error!(
                action_error=?error, publish_error=?err, "failed to publish PlanFailed event."
            ),
        }
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), ActError> {
        tracing::trace!("closing patch replicas act phase inlet.");
        self.inlet.close().await;
        Ok(())
    }
}
