use crate::engine::service::{EngineCmd, EngineServiceApi, Health};
use crate::engine::{PhaseFlag, PhaseFlags};
use crate::flink;
use crate::phases::act::{
    ActErrorDisposition, ActEvent, ActMonitor, ActionOutcome, ACTION_TOTAL_DURATION,
    ACT_IS_RESCALING, ACT_RESCALE_ACTION_COUNT, PHASE_ACT_ERRORS,
};
use crate::phases::decision::{
    DecisionContext, DecisionEvent, DecisionMonitor, DecisionResult, ScaleDirection,
};
use crate::phases::eligibility::{EligibilityContext, EligibilityEvent, EligibilityMonitor};
use crate::phases::governance::{GovernanceContext, GovernanceEvent, GovernanceMonitor};
use crate::phases::plan::{
    self, FlinkPlanningEvent, FlinkPlanningMonitor, PlanningOutcome, PlanningOutcomeT,
    PlanningStrategy,
};
use crate::phases::{decision, WindowApi, WindowCmd};
use chrono::{DateTime, Utc};
use enumflags2::BitFlags;
use once_cell::sync::Lazy;
use proctor::elements::telemetry::TableType;
use proctor::elements::{RecordsPerSecond, Telemetry, Timestamp, FORMAT};
use proctor::graph::stage::{ActorSourceApi, ActorSourceCmd};
use proctor::phases::plan::{PlanEvent, PlanMonitor};
use proctor::phases::sense::{ClearinghouseApi, ClearinghouseCmd};
use proctor::Correlation;
use prometheus::core::{AtomicU64, GenericGauge};
use prometheus::{IntCounter, IntGauge, Opts};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;

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
    pub rescale_restart: HashMap<ScaleDirection, Duration>,
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

        if !feedback.rescale_restart.is_empty() {
            let mut restart_table: TableType =
                HashMap::with_capacity(feedback.rescale_restart.len());
            for (d, r) in feedback.rescale_restart {
                restart_table.insert(d.to_string(), r.as_secs_f64().into());
            }

            telemetry.insert(
                plan::PLANNING__RESCALE_RESTART.to_string(),
                restart_table.into(),
            );
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
    pub rx_governance_monitor: GovernanceMonitor<PlanningOutcomeT>,
    pub rx_action_monitor: ActMonitor<PlanningOutcome>,
    pub tx_feedback: Option<ActorSourceApi<Telemetry>>,
    pub tx_engine: EngineServiceApi,
    pub tx_clearinghouse_api: ClearinghouseApi,
    pub tx_collect_window_api: WindowApi,
    pub decision_window: u32,
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
        if let Err(err) =
            EngineCmd::update_health(&self.tx_engine, Health::Ready(*ready_phases)).await
        {
            tracing::warn!(error=?err, "failed to update engine health");
        }
    }

    #[tracing::instrument(level = "trace")]
    fn mark_ready_phases(loaded: &mut BitFlags<PhaseFlag>) {
        use proctor::phases::sense::SubscriptionRequirements;

        vec![
            (
                PhaseFlag::Eligibility,
                EligibilityContext::required_fields().is_empty(),
            ),
            (
                PhaseFlag::Decision,
                DecisionContext::required_fields().is_empty(),
            ),
            (
                PhaseFlag::Governance,
                GovernanceContext::required_fields().is_empty(),
            ),
        ]
        .into_iter()
        .for_each(|(phase, is_ready)| {
            if is_ready {
                tracing::info!(
                    ?phase,
                    "springline {} phase starts as ready for data.",
                    phase
                );
                loaded.toggle(phase);
            }
        })
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_eligibility_event(
        &self, event: Arc<EligibilityEvent>, loaded: &mut BitFlags<PhaseFlag>,
    ) {
        match event.as_ref() {
            EligibilityEvent::ItemPassed(item, query_result) => {
                tracing::debug!(?event, ?query_result, correlation=%item.correlation(), "eligibility outcome: data item passed eligibility");
                ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(true as i64)
            },
            EligibilityEvent::ItemBlocked(item, query_result) => {
                tracing::info!(?event, ?query_result, correlation=%item.correlation(), "eligibility outcome: data item blocked in eligibility");
                ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(false as i64)
            },
            EligibilityEvent::ContextChanged(Some(ctx))
                if !loaded.contains(PhaseFlag::Eligibility) =>
            {
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
    async fn handle_decision_event(
        &self, event: Arc<DecisionEvent>, loaded: &mut BitFlags<PhaseFlag>,
    ) {
        match event.as_ref() {
            DecisionEvent::ItemPassed(item, query_result) => {
                decision::log_outcome_with_common_criteria(
                    "PASSED",
                    item,
                    Some(query_result),
                    self.decision_window,
                );

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
                decision::log_outcome_with_common_criteria(
                    "blocked",
                    item,
                    query_result.as_ref(),
                    self.decision_window,
                );
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
    async fn handle_plan_event(
        &self, event: Arc<PlanEvent<PlanningStrategy>>, loaded: &mut BitFlags<PhaseFlag>,
    ) {
        match event.as_ref() {
            PlanEvent::DecisionPlanned(decision, plan) => match decision.as_ref() {
                DecisionResult::ScaleUp(_) | DecisionResult::ScaleDown(_) => {
                    tracing::info!(
                        ?event,
                        ?decision,
                        "plan outcome: planning for scaling decision"
                    );
                    PLAN_TARGET_NR_TASK_MANAGERS.set(plan.target_nr_taskmanagers.into());
                },
                _no_action => {
                    tracing::debug!(?event, "plan outcome: no planning action by decision")
                },
            },
            PlanEvent::DecisionIgnored(decision) => {
                tracing::debug!(
                    ?event,
                    ?decision,
                    "plan outcome: planning is ignoring decision result."
                );
            },

            PlanEvent::ContextChanged(context) if !loaded.contains(PhaseFlag::Plan) => {
                tracing::info!(?event, ?context, "Plan Phase initial context loaded!");
                loaded.toggle(PhaseFlag::Plan);
                self.update_health(loaded).await;
            },
            PlanEvent::ContextChanged(context) => {
                tracing::trace!(?event, ?context, "Flink Planning context changed.");
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_flink_planning_event(&self, event: Arc<FlinkPlanningEvent>) {
        match event.as_ref() {
            FlinkPlanningEvent::ObservationAdded { observation, next_forecast } => {
                tracing::debug!(?observation, "observation added to planning");
                PLAN_OBSERVATION_COUNT.inc();

                if let Some((tx, (forecast_ts, forecast_val))) =
                    self.tx_feedback.as_ref().zip(*next_forecast)
                {
                    let feedback = PlanningFeedback {
                        forecasted_timestamp: forecast_ts,
                        forecasted_records_in_per_sec: forecast_val,
                    };

                    tracing::debug!(%forecast_ts, %forecast_val, "planning feedback of next forecast back into springline system");
                    if let Err(err) = ActorSourceCmd::push(tx, feedback.into()).await {
                        tracing::error!(
                            error=?err, %forecast_ts, %forecast_val,
                            "failed to feed forecasted workflow back into system."
                        )
                    }
                }
            },
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_governance_event(
        &self, event: Arc<GovernanceEvent<PlanningOutcomeT>>, loaded: &mut BitFlags<PhaseFlag>,
    ) {
        match event.as_ref() {
            GovernanceEvent::ItemPassed(_item, query_result) => {
                tracing::info!(?event, ?query_result, "governance outcome: data item passed governance");
                GOVERNANCE_PLAN_ACCEPTED.set(true as i64)
            },
            GovernanceEvent::ItemBlocked(_item, query_result) => {
                tracing::info!(?event, ?query_result, "governance outcome: data item blocked in governance");
                GOVERNANCE_PLAN_ACCEPTED.set(false as i64)
            },
            GovernanceEvent::ContextChanged(Some(_ctx))
                if !loaded.contains(PhaseFlag::Governance) =>
            {
                tracing::info!(?event, "Governance Phase initial context loaded!");
                loaded.toggle(PhaseFlag::Governance);
                self.update_health(loaded).await;
            },
            GovernanceEvent::ContextChanged(context) => {
                tracing::info!(?context, "Governance context changed.");
            },
        }
    }

    #[tracing::instrument(level = "debug", skip(self, _loaded))]
    async fn handle_action_event(
        &self, event: Arc<ActEvent<PlanningOutcome>>, _loaded: &mut BitFlags<PhaseFlag>,
    ) {
        let now = Timestamp::now();

        let action_feedback = match event.as_ref() {
            ActEvent::PlanActionStarted(plan) => Self::do_handle_rescale_started(plan),
            ActEvent::PlanExecuted { plan, outcomes } => {
                self.do_handle_rescale_executed(plan, outcomes, now).await
            },
            ActEvent::PlanFailed { plan, error_metric_label } => {
                Self::do_handle_rescale_failed(plan, error_metric_label, now)
            },
        };

        ACT_IS_RESCALING.set(if action_feedback.is_rescaling { 1 } else { 0 });

        if let Some(ref tx) = self.tx_feedback {
            tracing::debug!(?action_feedback, "feedback springline per scale action");
            let action_telemetry: Telemetry = action_feedback.into();
            let push_cmd = ActorSourceCmd::push(tx, action_telemetry.clone())
                .instrument(tracing::debug_span!(
                    "push_action_feedback_to_springline",
                    ?action_telemetry
                ))
                .await;
            if let Err(err) = push_cmd {
                tracing::error!(error=?err, "failed to send scale deployment notification from monitor -- may impact future eligibility determination.");
            }
        }
    }

    fn do_handle_rescale_started(plan: &PlanningOutcome) -> ActionFeedback {
        tracing::info!(?plan, correlation=%plan.correlation(), "rescale action started");
        ActionFeedback { is_rescaling: true, ..ActionFeedback::default() }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn do_handle_rescale_executed(
        &self, plan: &PlanningOutcome, outcomes: &[ActionOutcome], now: Timestamp,
    ) -> ActionFeedback {
        tracing::info!(%now, ?plan, correlation=%plan.correlation(), ?outcomes, "act outcome: rescale executed");
        ACT_RESCALE_ACTION_COUNT
            .with_label_values(&[
                plan.current_nr_taskmanagers.to_string().as_str(),
                plan.target_nr_taskmanagers.to_string().as_str(),
            ])
            .inc();

        if let Err(err) = ClearinghouseCmd::clear(&self.tx_clearinghouse_api).await {
            tracing::warn!(error=?err, correlation=%plan.correlation(), "failed to clear clearinghouse on rescaling.");
        }

        if let Err(err) = WindowCmd::clear(&self.tx_collect_window_api).await {
            tracing::warn!(error=?err, correlation=%plan.correlation(), "failed to clear window on rescaling.");
        }

        let rescale_total_duration = outcomes.iter().find_map(|o| {
            if o.label == ACTION_TOTAL_DURATION {
                Some(o.duration)
            } else {
                None
            }
        });

        let rescale_task_sum = outcomes
            .iter()
            .filter_map(|o| if o.is_leaf { Some(o.duration) } else { None })
            .sum();

        let rescale_restart = maplit::hashmap! {
            plan.direction() => rescale_total_duration.unwrap_or(rescale_task_sum),
        };

        let last_deployment = now.as_utc();
        tracing::info!(
            %last_deployment, ?rescale_restart, ?rescale_total_duration, ?rescale_task_sum,
            "Feeding act summary back into springline"
        );

        ActionFeedback {
            is_rescaling: false,
            last_deployment: Some(last_deployment),
            rescale_restart,
        }
    }

    fn do_handle_rescale_failed(
        plan: &PlanningOutcome, error_metric_label: &str, _now: Timestamp,
    ) -> ActionFeedback {
        tracing::warn!(%error_metric_label, ?plan, correlation=%plan.correlation(), "act outcome: rescale action during act phase failed.");
        PHASE_ACT_ERRORS
            .with_label_values(&[
                "monitor::act",
                error_metric_label,
                ActErrorDisposition::Failed.to_string().as_str(),
            ])
            .inc();

        ActionFeedback { is_rescaling: false, ..ActionFeedback::default() }
    }
}

pub static ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "eligibility_is_eligible_for_scaling",
            "Is the cluster in a state deemed eligible for scaling",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating eligibility_is_eligible_for_scaling metric")
});

pub static DECISION_RESCALE_DECISION: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "decision_rescale_decision",
            "Decision on rescaling the cluster: -1 = rescale down, 0 = no action, 1 = rescale up",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating decision_rescale_decision metric")
});

pub static PLAN_OBSERVATION_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "plan_observation_count",
            "Number of observations made for planning.",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating plan_observation_count metric")
});

pub static PLAN_TARGET_NR_TASK_MANAGERS: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "plan_target_nr_task_managers",
            "Number of task managers targeted by Planning",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating plan_target_nr_task_managers metric")
});

pub static GOVERNANCE_PLAN_ACCEPTED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(
        Opts::new(
            "governance_plan_accepted",
            "Has Springline governance accepted the last scaling plan presented",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating governance_plan_accepted metric")
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phases::plan::PlanningContext;
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::TelemetryValue;
    use std::time::Duration;

    #[test]
    fn test_action_feedback_telemetry_serde() {
        let now = Utc::now();
        let now_rep = format!("{}", now.format(FORMAT));

        let feedback = ActionFeedback {
            is_rescaling: false,
            last_deployment: Some(now),
            rescale_restart: maplit::hashmap! { ScaleDirection::Down => Duration::from_secs_f32(799.5) },
        };

        let mut actual: Telemetry = feedback.clone().into();
        assert_eq!(
            actual,
            maplit::hashmap! {
                crate::phases::eligibility::CLUSTER__IS_RESCALING.to_string() => false.into(),
                crate::phases::eligibility::CLUSTER__LAST_DEPLOYMENT.to_string() => now_rep.into(),
                plan::PLANNING__RESCALE_RESTART.to_string() => maplit::hashmap! {
                    ScaleDirection::Down.to_string() => TelemetryValue::Float(799.5),
                }.into(),
            }
            .into()
        );

        // extend to plan context
        actual.insert("cluster.total_task_slots".to_string(), 16.into());
        actual.insert("cluster.free_task_slots".to_string(), 0.into());

        let actual_context: PlanningContext = assert_ok!(actual.try_into());

        assert_eq!(
            actual_context,
            PlanningContext {
                min_scaling_step: None,
                total_task_slots: 16,
                free_task_slots: 0,
                rescale_restart: maplit::hashmap! { ScaleDirection::Down => Duration::from_secs_f32(799.5), },
                max_catch_up: None,
                recovery_valid: None,
            },
        )
    }
}
