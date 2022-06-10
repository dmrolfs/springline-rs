use std::error::Error;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use proctor::error::{MetricLabel, ProctorError};
use proctor::graph::stage::Stage;
use proctor::graph::{Inlet, Port, SinkShape, PORT_DATA};
use proctor::{AppData, ProctorResult};
use tokio::sync::broadcast;
use tracing::Instrument;

use super::action::{self, ActionSession, CompositeAction, ScaleAction};
use super::{protocol, ActError, ActEvent, ScaleActionPlan};
use crate::flink::FlinkContext;
use crate::kubernetes::KubernetesContext;
use crate::phases::plan::ScalePlan;
use crate::settings::Settings;

const STAGE_NAME: &str = "execute_scaling";

pub struct ScaleActuator<In> {
    kube: KubernetesContext,
    flink: FlinkContext,
    action: Box<dyn ScaleAction<In = In>>,
    inlet: Inlet<In>,
    pub tx_action_monitor: broadcast::Sender<Arc<protocol::ActEvent<In>>>,
}

impl ScaleActuator<ScalePlan> {
    #[tracing::instrument(level = "trace", skip(kube))]
    pub fn new(kube: KubernetesContext, flink: FlinkContext, settings: &Settings) -> Self {
        let flink_action_settings = &settings.action.flink;

        let composite: CompositeAction<ScalePlan> = action::CompositeAction::default()
            .add_action_step(action::PrepareData)
            .add_action_step(action::TriggerSavepoint::from_settings(flink_action_settings))
            .add_action_step(action::PatchReplicas::from_settings(settings))
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
    fn name(&self) -> &str {
        STAGE_NAME
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await.map_err(|err| ProctorError::Phase(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run act scale actuator", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await.map_err(|err| ProctorError::Phase(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
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
        let rescaling_lock = tokio::sync::Mutex::new(false);

        while let Some(plan) = inlet.recv().await {
            let mut is_rescaling = rescaling_lock.lock().await;
            if *is_rescaling {
                tracing::info!("prior rescale in progress, skipping.");
                continue;
            }

            *is_rescaling = true;
            self.notify_action_started(plan.clone());
            let _timer = proctor::graph::stage::start_stage_eval_time(STAGE_NAME);
            let mut session = ActionSession::new(plan.correlation().clone(), self.kube.clone(), self.flink.clone());

            let outcome = match self.action.check_preconditions(&session) {
                Ok(_) => {
                    self.action
                        .execute(&plan, &mut session)
                        .instrument(tracing::info_span!("act::execute_actuator", ?plan))
                        .await
                },
                Err(err) => Err(err),
            };

            match outcome {
                Ok(_) => self.notify_action_succeeded(plan, session),
                Err(err) => {
                    tracing::error!(error=?err, ?session, "failure in scale action.");
                    self.notify_action_failed(plan, session, err);
                },
            }

            *is_rescaling = false;
        }

        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, plan),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp())
    )]
    fn notify_action_started(&self, plan: In) {
        tracing::info!(?plan, "rescale action started");
        match self.tx_action_monitor.send(Arc::new(ActEvent::PlanActionStarted(plan))) {
            Ok(nr_recipients) => tracing::debug!("published PlanActionStarted event to {nr_recipients} recipients."),
            Err(err) => tracing::warn!(error=?err, "failed to publish PlanActionStarted event."),
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, plan, session),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp())
    )]
    fn notify_action_succeeded(&self, plan: In, session: ActionSession) {
        tracing::info!(?plan, ?session, "rescale action succeeded");
        let event = Arc::new(ActEvent::PlanExecuted { plan, durations: session.durations.clone() });

        match self.tx_action_monitor.send(event) {
            Ok(nr_recipients) => tracing::debug!(
                action_durations=?session.durations,
                "published PlanExecuted event to {nr_recipients} recipients."
            ),
            Err(err) => tracing::warn!(error=?err, "failed to publish PlanExecuted event."),
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, plan, session, error),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp(), error_label=%error.label(),)
    )]
    fn notify_action_failed<E>(&self, plan: In, session: ActionSession, error: E)
    where
        E: Error + MetricLabel,
    {
        tracing::warn!(?error, ?plan, ?session, "rescale action failed");
        let error_metric_label = error.label();
        match self
            .tx_action_monitor
            .send(Arc::new(ActEvent::PlanFailed { plan, error_metric_label }))
        {
            Ok(recipients) => tracing::debug!(
                action_durations=?session.durations, action_error=?error,
                "published PlanFailed to {} recipients", recipients
            ),
            Err(err) => tracing::warn!(
                action_error=?error, publish_error=?err, "failed to publish PlanFailed event."
            ),
        }
    }

    #[inline]
    async fn do_close(mut self: Box<Self>) -> Result<(), ActError> {
        tracing::info!("closing patch replicas act phase inlet.");
        self.inlet.close().await;
        Ok(())
    }
}
