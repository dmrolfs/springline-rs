use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::Env;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use proctor::error::{MetricLabel, ProctorError};
use proctor::graph::stage::Stage;
use proctor::graph::{Inlet, Port, SinkShape, PORT_DATA};
use proctor::{AppData, Correlation, IntoEnvelope, ProctorResult, ReceivedAt};
use tokio::sync::broadcast;
use tracing::Instrument;

use super::action::{self, ActionSession, ScaleAction};
use super::{protocol, ActError, ActEvent};
use crate::flink::FlinkContext;
use crate::kubernetes::{KubernetesApiConstraints, KubernetesContext};
use crate::phases::act::action::ActionStatus;
use crate::phases::act::ACTION_TOTAL_DURATION;
use crate::phases::plan::{ScaleActionPlan, ScaleDirection};
use crate::settings::{FlinkActionSettings, Settings};

const STAGE_NAME: &str = "execute_scaling";

#[derive(Debug, Clone, PartialEq)]
struct MakeActionParameters {
    pub cull_ratio: Option<f64>,
    pub taskmanager_register_timeout: Duration,
    pub flink_action_settings: FlinkActionSettings,
    pub kubernetes_action_settings: KubernetesApiConstraints,
}

pub struct ScaleActuator<P>
where
    P: IntoEnvelope,
{
    kube: KubernetesContext,
    flink: FlinkContext,
    parameters: MakeActionParameters,
    // action: Box<dyn ScaleAction<Plan = P>>,
    inlet: Inlet<P>,
    pub tx_action_monitor: broadcast::Sender<Arc<ActEvent<P>>>,
}

impl<P> ScaleActuator<P>
where
    P: AppData + ScaleActionPlan + IntoEnvelope,
{
    #[tracing::instrument(level = "trace", name = "ScaleActuator::new", skip(kube))]
    pub fn new(kube: KubernetesContext, flink: FlinkContext, settings: &Settings) -> Self {
        let (tx_action_monitor, _) = broadcast::channel(num_cpus::get());
        let parameters = MakeActionParameters {
            cull_ratio: settings.action.taskmanager.cull_ratio,
            taskmanager_register_timeout: settings.kubernetes.patch_settle_timeout,
            flink_action_settings: settings.action.flink.clone(),
            kubernetes_action_settings: settings.action.taskmanager.kubernetes_api,
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

impl<P> fmt::Debug for ScaleActuator<P>
where
    P: IntoEnvelope,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScaleActuator")
            .field("make_action_parameters", &self.parameters)
            .field("kube", &self.kube)
            .field("flink", &self.flink)
            .field("inlet", &self.inlet)
            .finish()
    }
}

impl<P> proctor::graph::stage::WithMonitor for ScaleActuator<P>
where
    P: IntoEnvelope,
{
    type Receiver = protocol::ActMonitor<P>;

    fn rx_monitor(&self) -> Self::Receiver {
        self.tx_action_monitor.subscribe()
    }
}

impl<P> SinkShape for ScaleActuator<P>
where
    P: IntoEnvelope,
{
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
    P: AppData + ScaleActionPlan + Correlation + ReceivedAt + IntoEnvelope,
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
    P: AppData + ScaleActionPlan + Correlation + ReceivedAt + IntoEnvelope,
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
            let action_timer = super::start_rescale_timer(ACTION_TOTAL_DURATION);

            let mut action = Self::make_rescale_action_plan(&plan, &self.parameters);
            let mut is_rescaling = rescaling_lock.lock().await;
            if *is_rescaling {
                tracing::info!("prior rescale in progress, skipping.");
                continue;
            }

            *is_rescaling = true;
            self.notify_action_started(&plan);
            let plan_metadata = plan.metadata();
            let mut session = Env::from_parts(
                plan_metadata.relabel(),
                ActionSession::new(self.kube.clone(), self.flink.clone()),
            );

            let _timer = proctor::graph::stage::start_stage_eval_time(STAGE_NAME);
            let outcome = match action.check_preconditions(&session) {
                Ok(_) => {
                    action
                        .execute(&plan, &mut session)
                        .instrument(tracing::info_span!("act::execute_actuator", ?plan))
                        .await
                },
                Err(err) => Err(err),
            };

            let action_duration = Duration::from_secs_f64(action_timer.stop_and_record());
            match outcome {
                Ok(_) => {
                    let mut total_status = ActionStatus::Success;
                    for outcome in session.history.iter() {
                        match outcome.status {
                            ActionStatus::Failure => {
                                total_status = ActionStatus::Failure;
                                break;
                            },
                            ActionStatus::Recovered => {
                                // set to recovered, but still look for failure. Later success doesn't redeem.
                                total_status = ActionStatus::Recovered;
                            },
                            ActionStatus::Success => (),
                        }
                    }

                    session.mark_completion(
                        ACTION_TOTAL_DURATION,
                        total_status,
                        action_duration,
                        action.is_leaf(),
                    );
                    self.notify_action_succeeded(plan, session);
                },
                Err(err) => {
                    tracing::error!(error=?err, ?session, "failure in scale action.");
                    session.mark_completion(
                        ACTION_TOTAL_DURATION,
                        ActionStatus::Failure,
                        action_duration,
                        action.is_leaf(),
                    );
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
            ScaleDirection::Up
                if plan.target_job_parallelism().as_u32() <= plan.current_replicas().as_u32() =>
            {
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
        let mut action = action::CompositeAction::new("rescale_up")
            .add_action_step(action::PrepareData::from_settings(
                &parameters.flink_action_settings,
            ))
            .add_action_step(action::CancelWithSavepoint::from_settings(
                &parameters.flink_action_settings,
            ))
            .add_action_step(action::PatchReplicas::from_settings(
                parameters.taskmanager_register_timeout,
                parameters.flink_action_settings.polling_interval,
            ));
        // .add_action_step(flink_settle.with_sub_label("after_rescale"))

        if let Some(cull_ratio) = parameters.cull_ratio {
            let culling = action::CompositeAction::new("culling")
                .add_action_step(action::CullTaskmanagers::new(cull_ratio))
                .add_action_step(
                    action::KubernetesSettlement::from_settings(
                        parameters.kubernetes_action_settings.api_timeout,
                        parameters.kubernetes_action_settings.polling_interval,
                    )
                    .with_sub_label("after_culling_taskmanagers"),
                );

            action = action.add_action_step(culling);
            // cannot affect active tms during savepoint
        }

        let restart = action::CompositeAction::new("restart")
            .add_action_step(
                action::FlinkSettlement::from_settings(
                    parameters.taskmanager_register_timeout,
                    parameters.flink_action_settings.polling_interval,
                )
                .with_sub_label("before_restart"),
            )
            .add_action_step(action::RestartJobs::from_settings(
                &parameters.flink_action_settings,
            ));

        action = action.add_action_step(restart);

        Box::new(action)
    }

    #[tracing::instrument(level = "debug", skip(_plan, parameters))]
    fn do_make_rescale_down_action(
        _plan: &P, parameters: &MakeActionParameters,
    ) -> Box<dyn ScaleAction<Plan = P>> {
        let mut action = action::CompositeAction::new("rescale_down")
            .add_action_step(action::PrepareData::from_settings(
                &parameters.flink_action_settings,
            ))
            .add_action_step(action::CancelWithSavepoint::from_settings(
                &parameters.flink_action_settings,
            ))
            .add_action_step(action::PatchReplicas::from_settings(
                parameters.taskmanager_register_timeout,
                parameters.flink_action_settings.polling_interval,
            ));

        if let Some(cull_ratio) = parameters.cull_ratio {
            let culling = action::CompositeAction::new("culling")
                .add_action_step(action::CullTaskmanagers::new(cull_ratio))
                .add_action_step(
                    action::KubernetesSettlement::from_settings(
                        parameters.kubernetes_action_settings.api_timeout,
                        parameters.kubernetes_action_settings.polling_interval,
                    )
                    .with_sub_label("after_culling_taskmanagers"),
                );

            action = action.add_action_step(culling);
        }

        // maybe add a settle step here?
        let restart = action::CompositeAction::new("restart")
            .add_action_step(
                action::FlinkSettlement::from_settings(
                    parameters.taskmanager_register_timeout,
                    parameters.flink_action_settings.polling_interval,
                )
                .with_sub_label("before_restart"),
            )
            .add_action_step(action::RestartJobs::from_settings(
                &parameters.flink_action_settings,
            ));

        action = action.add_action_step(restart);

        Box::new(action)
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, plan),
        fields(correlation=%plan.correlation(), recv_timestamp=%plan.recv_timestamp())
    )]
    fn notify_action_started(&self, plan: &P) {
        tracing::info!(?plan, "rescale action started");
        let plan_event = ActEvent::PlanActionStarted(plan.clone());
        match self.tx_action_monitor.send(Arc::new(plan_event)) {
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
    fn notify_action_succeeded(&self, plan: P, session: Env<ActionSession>) {
        tracing::info!(?plan, ?session, "rescale action succeeded");
        let event = ActEvent::PlanExecuted { plan, outcomes: session.history.clone() };
        match self.tx_action_monitor.send(Arc::new(event)) {
            Ok(nr_recipients) => tracing::debug!(
                action_outcomes=?session.history.iter().map(|o| o.to_string()).collect::<Vec<_>>(),
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
    fn notify_action_failed<E>(&self, plan: P, session: Env<ActionSession>, error: E)
    where
        E: Error + MetricLabel,
    {
        tracing::warn!(?error, ?plan, ?session, "rescale action failed");
        let error_metric_label = error.label();
        let plan_event = ActEvent::PlanFailed { plan, error_metric_label };
        match self.tx_action_monitor.send(Arc::new(plan_event)) {
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
