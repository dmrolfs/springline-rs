use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use enumflags2::{bitflags, BitFlags};
use once_cell::sync::Lazy;
use proctor::elements::{RecordsPerSecond, Telemetry, Timestamp, FORMAT};
use proctor::graph::stage::{ActorSourceApi, ActorSourceCmd};
use proctor::phases::plan::{PlanEvent, PlanMonitor};
use prometheus::{IntCounter, IntGauge};

use crate::model;
use crate::phases::act::{ActEvent, ActMonitor, ACT_PHASE_ERRORS, ACT_SCALE_ACTION_COUNT, PIPELINE_CYCLE_TIME};
use crate::phases::decision::{DecisionContext, DecisionEvent, DecisionMonitor, DecisionResult};
use crate::phases::eligibility::{EligibilityContext, EligibilityEvent, EligibilityMonitor};
use crate::phases::governance::{GovernanceContext, GovernanceEvent, GovernanceMonitor, GovernanceOutcome};
use crate::phases::plan::{FlinkPlanningEvent, FlinkPlanningMonitor, PlanningStrategy, ScalePlan};

#[derive(Debug, Clone, Copy, PartialEq)]
struct PlanningFeedback {
    pub forecasted_timestamp: Timestamp,
    pub forecasted_records_in_per_sec: RecordsPerSecond,
}

impl From<PlanningFeedback> for Telemetry {
    fn from(feedback: PlanningFeedback) -> Self {
        let mut telemetry = Self::new();

        telemetry.insert(
            model::MC_FLOW__FORECASTED_TIMESTAMP.to_string(),
            feedback.forecasted_timestamp.as_f64().into(),
        );

        telemetry.insert(
            model::MC_FLOW__FORECASTED_RECORDS_IN_PER_SEC.to_string(),
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

#[bitflags(default = Sense | Plan | Act)]
#[repr(u8)]
#[derive(Debug, Display, Copy, Clone, PartialEq)]
enum ReadyPhases {
    Sense = 1 << 0,
    Eligibility = 1 << 1,
    Decision = 1 << 2,
    Plan = 1 << 3,
    Governance = 1 << 4,
    Act = 1 << 5,
}

#[derive(Debug)]
pub struct Monitor {
    rx_eligibility_monitor: EligibilityMonitor,
    rx_decision_monitor: DecisionMonitor,
    rx_plan_monitor: PlanMonitor<PlanningStrategy>,
    rx_flink_planning_monitor: FlinkPlanningMonitor,
    rx_governance_monitor: GovernanceMonitor,
    rx_action_monitor: Option<ActMonitor<GovernanceOutcome>>,
    tx_feedback: Option<ActorSourceApi<Telemetry>>,
}

// impl std::fmt::Debug for Monitor {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("Monitor")
//             .field("rx_eligibility", &self.rx_eligibility_monitor)
//             .field("rx_decision", &self.rx_decision_monitor)
//             .field("rx_plan", &self.rx_plan_monitor)
//             .field("rx_flink_planning", &self.rx_flink_planning_monitor)
//             .field("rx_governance", &self.rx_governance_monitor)
//             .field("rx_action", &self.rx_action_monitor)
//             .field("tx_feedback", &self.tx_feedback)
//             .finish()
//     }
// }

impl Monitor {
    pub fn new(
        rx_eligibility_monitor: EligibilityMonitor, rx_decision_monitor: DecisionMonitor,
        rx_plan_monitor: PlanMonitor<PlanningStrategy>, rx_flink_planning_monitor: FlinkPlanningMonitor,
        rx_governance_monitor: GovernanceMonitor, rx_action_monitor: Option<ActMonitor<GovernanceOutcome>>,
        tx_feedback: Option<ActorSourceApi<Telemetry>>,
    ) -> Self {
        Self {
            rx_eligibility_monitor,
            rx_decision_monitor,
            rx_plan_monitor,
            rx_flink_planning_monitor,
            rx_governance_monitor,
            rx_action_monitor,
            tx_feedback,
        }
    }

    #[tracing::instrument(level = "trace", skip(self), name = "monitor phase events")]
    pub async fn run(mut self) {
        let mut loaded = BitFlags::<ReadyPhases>::default();
        Self::mark_ready_phases(&mut loaded);

        let mut rx_action = self.rx_action_monitor.unwrap_or_else(|| {
            let (_, rx_dummy) = tokio::sync::broadcast::channel(0);
            rx_dummy
        });

        let tx_feedback = self.tx_feedback.as_ref();

        loop {
            let span = tracing::trace_span!("monitor event cycle");
            let _span_guard = span.enter();

            // WORK HERE To add act events and context fields -- see notebook
            tokio::select! {
                Ok(e) = self.rx_eligibility_monitor.recv() => Self::handle_eligibility_event(e, &mut loaded),
                Ok(e) = self.rx_decision_monitor.recv() => Self::handle_decision_event(e, &mut loaded),
                Ok(e) = self.rx_plan_monitor.recv() => Self::handle_plan_event(e, &mut loaded),
                Ok(e) = self.rx_flink_planning_monitor.recv() => Self::handle_flink_planning_event(e, tx_feedback).await,
                Ok(e) = self.rx_governance_monitor.recv() => Self::handle_governance_event(e, &mut loaded),
                Ok(e) = rx_action.recv() => Self::handle_action_event(e, tx_feedback, &mut loaded).await,
                else => {
                    tracing::info!("springline monitor stopping...");
                    break;
                }
            }

            if !loaded.is_all() {
                tracing::info!(?loaded, "Phases first-load partially complete: waiting:{:?}", !loaded)
            }
        }
    }

    #[tracing::instrument(level = "trace")]
    fn mark_ready_phases(loaded: &mut BitFlags<ReadyPhases>) {
        use proctor::phases::sense::SubscriptionRequirements;

        vec![
            (
                ReadyPhases::Eligibility,
                EligibilityContext::required_fields().is_empty(),
            ),
            (ReadyPhases::Decision, DecisionContext::required_fields().is_empty()),
            (ReadyPhases::Governance, GovernanceContext::required_fields().is_empty()),
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
    fn handle_eligibility_event(event: Arc<EligibilityEvent>, loaded: &mut BitFlags<ReadyPhases>) {
        match &*event {
            EligibilityEvent::ItemPassed(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item passed eligibility");
                ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(true as i64)
            },
            EligibilityEvent::ItemBlocked(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item blocked in eligibility");
                ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(false as i64)
            },
            EligibilityEvent::ContextChanged(Some(_ctx)) if !loaded.contains(ReadyPhases::Eligibility) => {
                tracing::info!(?event, "Eligibility Phase initial context loaded!");
                loaded.toggle(ReadyPhases::Eligibility);
            },
            EligibilityEvent::ContextChanged(context) => {
                tracing::debug!(?context, "Eligibility Phase context changed");
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_decision_event(event: Arc<DecisionEvent>, loaded: &mut BitFlags<ReadyPhases>) {
        match &*event {
            DecisionEvent::ItemPassed(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item passed scaling decision");
                DECISION_SHOULD_PLAN_FOR_SCALING.set(true as i64)
            },
            DecisionEvent::ItemBlocked(_item, query_result) => {
                tracing::debug!(?event, ?query_result, "data item blocked by scaling decision");
                DECISION_SHOULD_PLAN_FOR_SCALING.set(false as i64)
            },
            DecisionEvent::ContextChanged(Some(_ctx)) if !loaded.contains(ReadyPhases::Decision) => {
                tracing::info!(?event, "Decision Phase initial context loaded!");
                loaded.toggle(ReadyPhases::Decision);
            },
            DecisionEvent::ContextChanged(context) => {
                tracing::debug!(?context, "Decision Phase context changed");
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_plan_event(event: Arc<PlanEvent<PlanningStrategy>>, loaded: &mut BitFlags<ReadyPhases>) {
        match &*event {
            PlanEvent::DecisionPlanned(decision, plan) => match decision {
                DecisionResult::ScaleUp(_) | DecisionResult::ScaleDown(_) => {
                    tracing::debug!(?event, correlation=?decision.item().correlation_id, "planning for scaling decision");
                    DECISION_PLAN_CURRENT_NR_TASK_MANAGERS.set(plan.current_nr_task_managers as i64);
                    PLAN_TARGET_NR_TASK_MANAGERS.set(plan.target_nr_task_managers as i64);
                },
                _no_action => {
                    tracing::debug!(?event, correlation=?decision.item().correlation_id, "no planning action by decision");
                },
            },
            PlanEvent::DecisionIgnored(decision) => {
                tracing::debug!(?event, correlation=?decision.item().correlation_id, "planning is ignoring decision result.");
            },

            PlanEvent::ContextChanged(context) if !loaded.contains(ReadyPhases::Plan) => {
                tracing::info!(?event, ?context, "Plan Phase initial context loaded!");
                loaded.toggle(ReadyPhases::Plan);
            },
            PlanEvent::ContextChanged(context) => {
                tracing::debug!(?context, "Flink Planning context changed.");
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_flink_planning_event(
        event: Arc<FlinkPlanningEvent>, tx_feedback: Option<&ActorSourceApi<Telemetry>>,
    ) {
        match &*event {
            FlinkPlanningEvent::ObservationAdded { observation, next_forecast } => {
                tracing::debug!(?observation, correlation=?observation.correlation_id, "observation added to planning");
                PLAN_OBSERVATION_COUNT.inc();

                if let Some((tx, (forecast_ts, forecast_val))) = tx_feedback.zip(*next_forecast) {
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
    fn handle_governance_event(event: Arc<GovernanceEvent>, loaded: &mut BitFlags<ReadyPhases>) {
        match &*event {
            GovernanceEvent::ItemPassed(_item, query_result) => {
                tracing::debug!(?event, ?query_result, "data item passed governance");
                GOVERNANCE_PLAN_ACCEPTED.set(true as i64)
            },
            GovernanceEvent::ItemBlocked(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item blocked in governance");
                GOVERNANCE_PLAN_ACCEPTED.set(false as i64)
            },
            GovernanceEvent::ContextChanged(Some(_ctx)) if !loaded.contains(ReadyPhases::Governance) => {
                tracing::info!(?event, "Governance Phase initial context loaded!");
                loaded.toggle(ReadyPhases::Governance);
            },
            GovernanceEvent::ContextChanged(context) => {
                tracing::debug!(?context, "Governance context changed.");
            },
        }
    }

    #[tracing::instrument(level="info", skip(tx_feedback), fields(correlation=%event.correlation()))]
    async fn handle_action_event(
        event: Arc<ActEvent<GovernanceOutcome>>, tx_feedback: Option<&ActorSourceApi<Telemetry>>,
        _loaded: &mut BitFlags<ReadyPhases>,
    ) {
        let now = Timestamp::now();

        let action_feedback = match &*event {
            ActEvent::PlanActionStarted(plan) => Self::do_handle_plan_started(plan),
            ActEvent::PlanExecuted { plan, durations } => Self::do_handle_plan_executed(plan, durations, now),
            ActEvent::PlanFailed { plan, error_metric_label } => Self::do_handle_plan_failed(plan, error_metric_label),
        };

        if let Some(tx) = tx_feedback {
            tracing::debug!(?action_feedback, "feedback springline from scale action");
            if let Err(err) = ActorSourceCmd::push(tx, action_feedback.into()).await {
                tracing::error!(error=?err, "failed to send scale deployment notification from monitor -- may impact future eligibility determination.");
            }
        }
    }

    fn do_handle_plan_started(plan: &ScalePlan) -> ActionFeedback {
        tracing::debug!(?plan, correlation=?plan.correlation_id, "plan action started");
        ActionFeedback { is_rescaling: true, ..ActionFeedback::default() }
    }

    fn do_handle_plan_executed(
        plan: &ScalePlan, durations: &HashMap<String, Duration>, now: Timestamp,
    ) -> ActionFeedback {
        tracing::info!(%now, ?plan, correlation=?plan.correlation_id, ?durations, "plan executed");
        ACT_SCALE_ACTION_COUNT
            .with_label_values(&[
                plan.current_nr_task_managers.to_string().as_str(),
                plan.target_nr_task_managers.to_string().as_str(),
            ])
            .inc();

        let start = plan.recv_timestamp;
        let cycle_time_seconds = now.as_f64() - start.as_f64();
        PIPELINE_CYCLE_TIME.observe(cycle_time_seconds);

        ActionFeedback {
            is_rescaling: false,
            last_deployment: Some(now.as_utc()),
            rescale_restart: durations.get(crate::phases::act::ACTION_TOTAL_DURATION).copied(),
        }
    }

    fn do_handle_plan_failed(plan: &ScalePlan, error_metric_label: &str) -> ActionFeedback {
        tracing::warn!(%error_metric_label, ?plan, "plan action during act phase failed.");
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

pub(crate) static ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "eligibility_is_eligible_for_scaling",
        "Is the cluster in a state deemed eligible for scaling",
    )
    .expect("failed creating eligibility_is_eligible_for_scaling metric")
});

pub(crate) static DECISION_SHOULD_PLAN_FOR_SCALING: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "decision_should_plan_for_scaling",
        "Should plan for scaling the cluster",
    )
    .expect("failed creating decision_should_plan_for_scaling metric")
});

pub(crate) static PLAN_OBSERVATION_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::new("plan_observation_count", "Number of observations made for planning.")
        .expect("failed creating plan_observation_count metric")
});

pub(crate) static DECISION_PLAN_CURRENT_NR_TASK_MANAGERS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "decision_plan_current_nr_task_managers",
        "Number of task managers currently known to Decision and Planning",
    )
    .expect("failed creating decision_plan_current_nr_task_managers metric")
});

pub(crate) static PLAN_TARGET_NR_TASK_MANAGERS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "plan_target_nr_task_managers",
        "Number of task managers targeted by Planning",
    )
    .expect("failed creating plan_target_nr_task_managers metric")
});

pub(crate) static GOVERNANCE_PLAN_ACCEPTED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new(
        "governance_plan_accepted",
        "Has Springline governance accepted the last scaling plan presented",
    )
    .expect("failed creating governance_plan_accepted metric")
});
