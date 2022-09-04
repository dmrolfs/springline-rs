use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use proctor::error::{MetricLabel, ProctorError};
use proctor::graph::stage::Stage;
use proctor::graph::{Inlet, Port, SinkShape, PORT_DATA};
use proctor::{AppData, ProctorResult};
use tokio::sync::broadcast;
use tracing::Instrument;

use super::action::{self, ActionSession, ScaleAction};
use super::{protocol, ActError, ActEvent, ScaleActionPlan};
use crate::flink::FlinkContext;
use crate::kubernetes::KubernetesContext;
use crate::phases::plan::ScaleDirection;
use crate::settings::{FlinkActionSettings, Settings};

const STAGE_NAME: &str = "execute_scaling";

#[derive(Debug, PartialEq)]
struct MakeActionParameters {
    pub taskmanager_register_timeout: Duration,
    pub flink_action_settings: FlinkActionSettings,
}

pub struct ScaleActuator<P> {
    kube: KubernetesContext,
    flink: FlinkContext,
    parameters: MakeActionParameters,
    // action: Box<dyn ScaleAction<Plan = P>>,
    inlet: Inlet<P>,
    pub tx_action_monitor: broadcast::Sender<Arc<protocol::ActEvent<P>>>,
}

impl<P> ScaleActuator<P>
where
    P: AppData + ScaleActionPlan,
{
    #[tracing::instrument(level = "trace", name = "ScaleActuator::new", skip(kube))]
    pub fn new(kube: KubernetesContext, flink: FlinkContext, settings: &Settings) -> Self {
        let (tx_action_monitor, _) = broadcast::channel(num_cpus::get());
        let parameters = MakeActionParameters {
            taskmanager_register_timeout: settings.kubernetes.patch_settle_timeout,
            flink_action_settings: settings.action.flink.clone(),
        };

        Self {
            kube,
            flink,
            parameters,
            inlet: Inlet::new(STAGE_NAME, PORT_DATA),
            tx_action_monitor,
        }
    }
}

impl<P> fmt::Debug for ScaleActuator<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScaleActuator")
            .field("make_action_parameters", &self.parameters)
            .field("kube", &self.kube)
            .field("flink", &self.flink)
            .field("inlet", &self.inlet)
            .finish()
    }
}

impl<P> proctor::graph::stage::WithMonitor for ScaleActuator<P> {
    type Receiver = protocol::ActMonitor<P>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_action_monitor.subscribe()
    }
}

impl<P> SinkShape for ScaleActuator<P> {
    type In = P;

    #[inline]
    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<P> Stage for ScaleActuator<P>
where
    P: AppData + ScaleActionPlan,
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

impl<P> ScaleActuator<P>
where
    P: AppData + ScaleActionPlan,
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
            let action = Self::make_rescale_action_plan(&plan, &self.parameters);
            let mut is_rescaling = rescaling_lock.lock().await;
            if *is_rescaling {
                tracing::info!("prior rescale in progress, skipping.");
                continue;
            }

            *is_rescaling = true;
            self.notify_action_started(&plan);
            let _timer = proctor::graph::stage::start_stage_eval_time(STAGE_NAME);
            let mut session = ActionSession::new(
                plan.correlation().clone(),
                self.kube.clone(),
                self.flink.clone(),
            );

            let outcome = match action.check_preconditions(&session) {
                Ok(_) => {
                    action
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

    #[tracing::instrument(level = "debug")]
    fn make_rescale_action_plan(
        plan: &P, parameters: &MakeActionParameters,
    ) -> Box<dyn ScaleAction<Plan = P>> {
        match plan.direction() {
            ScaleDirection::Up if plan.target_job_parallelism() <= plan.current_replicas() => {
                // intentionally still want to follow up rescale plan to prevent expanding parallelism
                // onto TMs destined to stop.
                Self::do_make_rescale_up_action(plan, parameters)
            },
            ScaleDirection::Up => Self::do_make_rescale_up_action(plan, parameters),
            ScaleDirection::Down => Self::do_make_rescale_down_action(plan, parameters),
            ScaleDirection::None => Box::new(action::NoAction::default()),
        }
    }

    #[tracing::instrument(level = "debug", skip(_plan, parameters))]
    fn do_make_rescale_up_action(
        _plan: &P, parameters: &MakeActionParameters,
    ) -> Box<dyn ScaleAction<Plan = P>> {
        let action = action::CompositeAction::default()
            .add_action_step(action::PrepareData::default())
            .add_action_step(action::PatchReplicas::from_settings(
                parameters.taskmanager_register_timeout,
                parameters.flink_action_settings.polling_interval,
            ))
            .add_action_step(action::CancelWithSavepoint::from_settings(
                &parameters.flink_action_settings,
            ))
            .add_action_step(action::RestartJobs::from_settings(
                &parameters.flink_action_settings,
            ));

        Box::new(action)
    }

    #[tracing::instrument(level = "debug", skip(_plan, parameters))]
    fn do_make_rescale_down_action(
        _plan: &P, parameters: &MakeActionParameters,
    ) -> Box<dyn ScaleAction<Plan = P>> {
        let action = action::CompositeAction::default()
            .add_action_step(action::PrepareData::default())
            .add_action_step(action::CancelWithSavepoint::from_settings(
                &parameters.flink_action_settings,
            ))
            .add_action_step(action::PatchReplicas::from_settings(
                parameters.taskmanager_register_timeout,
                parameters.flink_action_settings.polling_interval,
            ))
            .add_action_step(action::RestartJobs::from_settings(
                &parameters.flink_action_settings,
            ));

        Box::new(action)
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, plan),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp())
    )]
    fn notify_action_started(&self, plan: &P) {
        tracing::info!(?plan, "rescale action started");
        match self
            .tx_action_monitor
            .send(Arc::new(ActEvent::PlanActionStarted(plan.clone())))
        {
            Ok(nr_recipients) => {
                tracing::debug!("published PlanActionStarted event to {nr_recipients} recipients.")
            },
            Err(err) => tracing::warn!(error=?err, "failed to publish PlanActionStarted event."),
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, plan, session),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp())
    )]
    fn notify_action_succeeded(&self, plan: P, session: ActionSession) {
        tracing::info!(?plan, ?session, "rescale action succeeded");
        let event = Arc::new(ActEvent::PlanExecuted { plan, outcomes: session.history.clone() });

        match self.tx_action_monitor.send(event) {
            Ok(nr_recipients) => tracing::debug!(
                action_outcomes=?session.history,
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
    fn notify_action_failed<E>(&self, plan: P, session: ActionSession, error: E)
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
                history=?session.history, action_error=?error,
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
