use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use enumflags2::BitFlags;
use once_cell::sync::Lazy;
use proctor::elements::{RecordsPerSecond, Telemetry, Timestamp, FORMAT};
use proctor::graph::stage::{ActorSourceApi, ActorSourceCmd};
use proctor::phases::plan::{PlanEvent, PlanMonitor};
use proctor::phases::sense::{ClearinghouseApi, ClearinghouseCmd};
use proctor::Correlation;
use prometheus::{IntCounter, IntGauge};

use crate::engine::service::{EngineCmd, EngineServiceApi, Health};
use crate::engine::{PhaseFlag, PhaseFlags};
use crate::flink;
use crate::phases::act::{ActEvent, ActMonitor, ACT_PHASE_ERRORS, ACT_SCALE_ACTION_COUNT, PIPELINE_CYCLE_TIME};
use crate::phases::decision::{DecisionContext, DecisionEvent, DecisionMonitor, DecisionResult, ScaleDirection};
use crate::phases::eligibility::{EligibilityContext, EligibilityEvent, EligibilityMonitor};
use crate::phases::governance::{GovernanceContext, GovernanceEvent, GovernanceMonitor, GovernanceOutcome};
use crate::phases::plan::{FlinkPlanningEvent, FlinkPlanningMonitor, PlanningStrategy, ScalePlan};
use crate::phases::{decision, WindowApi, WindowCmd};

#[derive(Debug, Clone, Copy, PartialEq)]
struct PlanningFeedback {
    pub forecasted_timestamp: Timestamp,
    pub forecasted_records_in_per_sec: RecordsPerSecond,
}

impl From<PlanningFeedback> for Telemetry {
    fn from(feedback: PlanningFeedback) -> Self {
        let mut telemetry = Self::new();

        telemetry.insert(
            flink::MC_FLOW__FORECASTED_TIMESTAMP.to_string(),
            feedback.forecasted_timestamp.as_secs_f64().into(),
        );

        telemetry.insert(
            flink::MC_FLOW__FORECASTED_RECORDS_IN_PER_SEC.to_string(),
            feedback.forecasted_records_in_per_sec.into(),
        );

        telemetry
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ActionFeedback {
    pub is_rescaling: bool,
    pub last_deployment: Option<DateTime<Utc>>,
    pub rescale_restart: Option<Duration>,
}

impl From<ActionFeedback> for Telemetry {
    fn from(feedback: ActionFeedback) -> Self {
        let mut telemetry = Self::new();

        telemetry.insert(
            crate::phases::eligibility::CLUSTER__IS_RESCALING.to_string(),
            feedback.is_rescaling.into(),
        );

        if let Some(last_deployment) = feedback.last_deployment {
            let last_deployment_rep = format!("{}", last_deployment.format(FORMAT));
            telemetry.insert(
                crate::phases::eligibility::CLUSTER__LAST_DEPLOYMENT.to_string(),
                last_deployment_rep.into(),
            );
        }

        if let Some(rescale_restart) = feedback.rescale_restart {
            match i64::try_from(rescale_restart.as_secs()) {
                Ok(rescale_restart_secs) => {
                    telemetry.insert(
                        crate::phases::plan::PLANNING__RESCALE_RESTART.to_string(),
                        rescale_restart_secs.into(),
                    );
                },
                Err(err) => {
                    tracing::warn!(error=?err, "failed to convert rescale restart duration to seconds - not publishing");
                },
            }
        }

        telemetry
    }
}

impl From<ScaleDirection> for i64 {
    fn from(direction: ScaleDirection) -> Self {
        match direction {
            ScaleDirection::None => 0,
            ScaleDirection::Down => -1,
            ScaleDirection::Up => 1,
        }
    }
}

#[derive(Debug)]
pub struct Monitor {
    pub rx_eligibility_monitor: EligibilityMonitor,
    pub rx_decision_monitor: DecisionMonitor,
    pub rx_plan_monitor: PlanMonitor<PlanningStrategy>,
    pub rx_flink_planning_monitor: FlinkPlanningMonitor,
    pub rx_governance_monitor: GovernanceMonitor,
    pub rx_action_monitor: ActMonitor<GovernanceOutcome>,
    pub tx_feedback: Option<ActorSourceApi<Telemetry>>,
    pub tx_engine: EngineServiceApi,
    pub tx_clearinghouse_api: ClearinghouseApi,
    pub tx_collect_window_api: WindowApi,
}

impl Monitor {
    #[tracing::instrument(level = "trace", skip(self), name = "monitor phase events")]
    pub async fn run(mut self) {
        let mut ready_phases = PhaseFlags::default();
        Self::mark_ready_phases(&mut ready_phases);

        loop {
            tokio::select! {
                Ok(e) = self.rx_eligibility_monitor.recv() => self.handle_eligibility_event(e, &mut ready_phases).await,
                Ok(e) = self.rx_decision_monitor.recv() => self.handle_decision_event(e, &mut ready_phases).await,
                Ok(e) = self.rx_plan_monitor.recv() => self.handle_plan_event(e, &mut ready_phases).await,
                Ok(e) = self.rx_flink_planning_monitor.recv() => self.handle_flink_planning_event(e).await,
                Ok(e) = self.rx_governance_monitor.recv() => self.handle_governance_event(e, &mut ready_phases).await,
                Ok(e) = self.rx_action_monitor.recv() => self.handle_action_event(e, &mut ready_phases).await,
                else => break,
            }

            if !ready_phases.is_all() {
                tracing::info!(
                    ?ready_phases,
                    "Phases first-load partially complete: waiting:{:?}",
                    !ready_phases
                )
            }
        }
    }

    async fn update_health(&self, ready_phases: &PhaseFlags) {
        if let Err(err) = EngineCmd::update_health(&self.tx_engine, Health::Ready(*ready_phases)).await {
            tracing::warn!(error=?err, "failed to update engine health");
        }
    }

    #[tracing::instrument(level = "trace")]
    fn mark_ready_phases(loaded: &mut BitFlags<PhaseFlag>) {
        use proctor::phases::sense::SubscriptionRequirements;

        vec![
            (PhaseFlag::Eligibility, EligibilityContext::required_fields().is_empty()),
            (PhaseFlag::Decision, DecisionContext::required_fields().is_empty()),
            (PhaseFlag::Governance, GovernanceContext::required_fields().is_empty()),
        ]
        .into_iter()
        .for_each(|(phase, is_ready)| {
            if is_ready {
                tracing::info!(?phase, "springline {} phase starts as ready for data.", phase);
                loaded.toggle(phase);
            }
        })
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_eligibility_event(&self, event: Arc<EligibilityEvent>, loaded: &mut BitFlags<PhaseFlag>) {
        match &*event {
            EligibilityEvent::ItemPassed(item, query_result) => {
                tracing::debug!(?event, ?query_result, correlation=%item.correlation(), "data item passed eligibility");
                ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(true as i64)
            },
            EligibilityEvent::ItemBlocked(item, query_result) => {
                tracing::info!(?event, ?query_result, correlation=%item.correlation(), "data item blocked in eligibility");
                ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(false as i64)
            },
            EligibilityEvent::ContextChanged(Some(ctx)) if !loaded.contains(PhaseFlag::Eligibility) => {
                tracing::info!(?event, correlation=%ctx.correlation(),"Eligibility Phase initial context loaded!");
                loaded.toggle(PhaseFlag::Eligibility);
                self.update_health(loaded).await;
            },
            EligibilityEvent::ContextChanged(context) => {
                tracing::info!(?context, correlation=?context.as_ref().map(|c| c.correlation().to_string()),"Eligibility Phase context changed");
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_decision_event(&self, event: Arc<DecisionEvent>, loaded: &mut BitFlags<PhaseFlag>) {
        match &*event {
            DecisionEvent::ItemPassed(item, query_result) => {
                tracing::info!(?event, ?query_result, correlation=%item.correlation(), "data item passed scaling decision");
                match decision::get_direction_and_reason(query_result) {
                    Ok((direction, _reason)) => DECISION_RESCALE_DECISION.set(direction.into()),
                    Err(error) => {
                        tracing::error!(
                            ?error,
                            "failed to get scale decision direction and reason from query results"
                        );
                        DECISION_RESCALE_DECISION.set(ScaleDirection::None.into());
                    },
                }
            },
            DecisionEvent::ItemBlocked(item, query_result) => {
                tracing::debug!(?event, ?query_result, correlation=%item.correlation(), "data item blocked by scaling decision");
                DECISION_RESCALE_DECISION.set(ScaleDirection::None.into());
            },
            DecisionEvent::ContextChanged(Some(ctx)) if !loaded.contains(PhaseFlag::Decision) => {
                tracing::info!(?event, correlation=%ctx.correlation(), "Decision Phase initial context loaded!");
                loaded.toggle(PhaseFlag::Decision);
                self.update_health(loaded).await;
            },
            DecisionEvent::ContextChanged(context) => {
                tracing::info!(?context, correlation=?context.as_ref().map(|c| c.correlation().to_string()), "Decision Phase context changed");
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_plan_event(&self, event: Arc<PlanEvent<PlanningStrategy>>, loaded: &mut BitFlags<PhaseFlag>) {
        match &*event {
            PlanEvent::DecisionPlanned(decision, plan) => match decision {
                DecisionResult::ScaleUp(_) | DecisionResult::ScaleDown(_) => {
                    tracing::info!(?event, correlation=%decision.item().correlation(), "planning for scaling decision");
                    DECISION_PLAN_CURRENT_NR_TASK_MANAGERS.set(plan.current_nr_task_managers as i64);
                    PLAN_TARGET_NR_TASK_MANAGERS.set(plan.target_nr_task_managers as i64);
                },
                _no_action => {
                    tracing::debug!(?event, correlation=%decision.item().correlation(), "no planning action by decision");
                },
            },
            PlanEvent::DecisionIgnored(decision) => {
                tracing::debug!(?event, correlation=%decision.item().correlation(), "planning is ignoring decision result.");
            },

            PlanEvent::ContextChanged(context) if !loaded.contains(PhaseFlag::Plan) => {
                tracing::info!(?event, ?context, correlation=%context.correlation(), "Plan Phase initial context loaded!");
                loaded.toggle(PhaseFlag::Plan);
                self.update_health(loaded).await;
            },
            PlanEvent::ContextChanged(context) => {
                tracing::info!(?context, correlation=%context.correlation(), "Flink Planning context changed.");
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_flink_planning_event(&self, event: Arc<FlinkPlanningEvent>) {
        match &*event {
            FlinkPlanningEvent::ObservationAdded { observation, next_forecast } => {
                tracing::debug!(?observation, correlation=?observation.correlation_id, "observation added to planning");
                PLAN_OBSERVATION_COUNT.inc();

                if let Some((tx, (forecast_ts, forecast_val))) = self.tx_feedback.as_ref().zip(*next_forecast) {
                    let feedback = PlanningFeedback {
                        forecasted_timestamp: forecast_ts,
                        forecasted_records_in_per_sec: forecast_val,
                    };

                    tracing::debug!(
                        %forecast_ts, %forecast_val, correlation=?observation.correlation_id,
                        "planning feedback of next forecast back into springline system"
                    );
                    if let Err(err) = ActorSourceCmd::push(tx, feedback.into()).await {
                        tracing::error!(
                            error=?err, %forecast_ts, %forecast_val, correlation=?observation.correlation_id,
                            "failed to feed forecasted workflow back into system."
                        )
                    }
                }
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_governance_event(&self, event: Arc<GovernanceEvent>, loaded: &mut BitFlags<PhaseFlag>) {
        match &*event {
            GovernanceEvent::ItemPassed(item, query_result) => {
                tracing::info!(?event, ?query_result, correlation=%item.correlation(), "data item passed governance");
                GOVERNANCE_PLAN_ACCEPTED.set(true as i64)
            },
            GovernanceEvent::ItemBlocked(item, query_result) => {
                tracing::info!(?event, ?query_result, correlation=%item.correlation(), "data item blocked in governance");
                GOVERNANCE_PLAN_ACCEPTED.set(false as i64)
            },
            GovernanceEvent::ContextChanged(Some(ctx)) if !loaded.contains(PhaseFlag::Governance) => {
                tracing::info!(?event, correlation=%ctx.correlation(), "Governance Phase initial context loaded!");
                loaded.toggle(PhaseFlag::Governance);
                self.update_health(loaded).await;
            },
            GovernanceEvent::ContextChanged(context) => {
                tracing::info!(?context, correlation=?context.as_ref().map(|c| c.correlation().to_string()), "Governance context changed.");
            },
        }
    }

    #[tracing::instrument(level="trace", skip(self, _loaded), fields(correlation=%event.correlation()))]
    async fn handle_action_event(&self, event: Arc<ActEvent<GovernanceOutcome>>, _loaded: &mut BitFlags<PhaseFlag>) {
        let now = Timestamp::now();

        let action_feedback = match &*event {
            ActEvent::PlanActionStarted(plan) => Self::do_handle_rescale_started(plan),
            ActEvent::PlanExecuted { plan, durations } => self.do_handle_rescale_executed(plan, durations, now).await,
            ActEvent::PlanFailed { plan, error_metric_label } => {
                Self::do_handle_rescale_failed(plan, error_metric_label, now)
            },
        };

        if let Some(ref tx) = self.tx_feedback {
            tracing::trace!(?action_feedback, "feedback springline per scale action");
            if let Err(err) = ActorSourceCmd::push(tx, action_feedback.into()).await {
                tracing::error!(error=?err, "failed to send scale deployment notification from monitor -- may impact future eligibility determination.");
            }
        }
    }

    fn do_handle_rescale_started(plan: &ScalePlan) -> ActionFeedback {
        tracing::info!(?plan, correlation=%plan.correlation(), "rescale action started");
        ActionFeedback { is_rescaling: true, ..ActionFeedback::default() }
    }

    async fn do_handle_rescale_executed(
        &self, plan: &ScalePlan, durations: &HashMap<String, Duration>, now: Timestamp,
    ) -> ActionFeedback {
        tracing::info!(%now, ?plan, correlation=%plan.correlation(), ?durations, "rescale executed");
        ACT_SCALE_ACTION_COUNT
            .with_label_values(&[
                plan.current_nr_task_managers.to_string().as_str(),
                plan.target_nr_task_managers.to_string().as_str(),
            ])
            .inc();

        let start = plan.recv_timestamp;
        let cycle_time_seconds = now.as_secs_f64() - start.as_secs_f64();
        PIPELINE_CYCLE_TIME.observe(cycle_time_seconds);

        if let Err(err) = ClearinghouseCmd::clear(&self.tx_clearinghouse_api).await {
            tracing::warn!(error=?err, correlation=%plan.correlation(), "failed to clear clearinghouse on rescaling.");
        }

        if let Err(err) = WindowCmd::clear(&self.tx_collect_window_api).await {
            tracing::warn!(error=?err, correlation=%plan.correlation(), "failed to clear window on rescaling.");
        }

        ActionFeedback {
            is_rescaling: false,
            last_deployment: Some(now.as_utc()),
            rescale_restart: durations.get(crate::phases::act::ACTION_TOTAL_DURATION).copied(),
        }
    }

    fn do_handle_rescale_failed(plan: &ScalePlan, error_metric_label: &str, _now: Timestamp) -> ActionFeedback {
        tracing::warn!(%error_metric_label, ?plan, correlation=%plan.correlation(), "rescale action during act phase failed.");
        ACT_PHASE_ERRORS
            .with_label_values(&[
                plan.current_nr_task_managers.to_string().as_str(),
                plan.target_nr_task_managers.to_string().as_str(),
                error_metric_label,
            ])
            .inc();

        ActionFeedback { is_rescaling: false, ..ActionFeedback::default() }
    }
}

pub static ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_is_eligible_for_scaling",
        "Is the cluster in a state deemed eligible for scaling",
    )
    .expect("failed creating eligibility_is_eligible_for_scaling metric")
});

// pub static DECISION_SHOULD_PLAN_FOR_SCALING: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "decision_should_plan_for_scaling",
//         "Should plan for scaling the cluster",
//     )
//     .expect("failed creating decision_should_plan_for_scaling metric")
// });

pub static DECISION_RESCALE_DECISION: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "decision_rescale_decision",
        "Decision on rescaling the cluster: -1 = rescale down, 0 = no action, 1 = rescale up",
    )
    .expect("failed creating decision_rescale_decision metric")
});

pub static PLAN_OBSERVATION_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("plan_observation_count", "Number of observations made for planning.")
        .expect("failed creating plan_observation_count metric")
});

pub static DECISION_PLAN_CURRENT_NR_TASK_MANAGERS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "decision_plan_current_nr_task_managers",
        "Number of task managers currently known to Decision and Planning",
    )
    .expect("failed creating decision_plan_current_nr_task_managers metric")
});

pub static PLAN_TARGET_NR_TASK_MANAGERS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "plan_target_nr_task_managers",
        "Number of task managers targeted by Planning",
    )
    .expect("failed creating plan_target_nr_task_managers metric")
});

pub static GOVERNANCE_PLAN_ACCEPTED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "governance_plan_accepted",
        "Has Springline governance accepted the last scaling plan presented",
    )
    .expect("failed creating governance_plan_accepted metric")
});
