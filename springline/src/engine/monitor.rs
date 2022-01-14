use chrono::Utc;
use std::fmt::{self, Display};
use std::sync::Arc;

use enumflags2::{bitflags, BitFlags};
use once_cell::sync::Lazy;
use proctor::elements::{Telemetry, Timestamp};
use proctor::graph::stage::{ActorSourceApi, ActorSourceCmd};
use proctor::phases::plan::PlanMonitor;
use proctor::serde::FORMAT;
use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts};
use prometheus_static_metric::make_static_metric;

use crate::phases::decision::{DecisionContext, DecisionEvent, DecisionMonitor, DecisionResult};
use crate::phases::eligibility::{EligibilityContext, EligibilityEvent, EligibilityMonitor};
use crate::phases::execution::{
    ExecutionEvent, ExecutionMonitor, EXECUTION_ERRORS, EXECUTION_SCALE_ACTION_COUNT, PIPELINE_CYCLE_TIME,
};
use crate::phases::governance::{GovernanceContext, GovernanceEvent, GovernanceMonitor, GovernanceOutcome};
use crate::phases::plan::{PlanEvent, PlanningStrategy};

#[bitflags(default = Collection | Plan | Execution)]
#[repr(u8)]
#[derive(Debug, Display, Copy, Clone, PartialEq)]
enum PhaseLoaded {
    Collection = 1 << 0,
    Eligibility = 1 << 1,
    Decision = 1 << 2,
    Plan = 1 << 3,
    Governance = 1 << 4,
    Execution = 1 << 5,
}

pub struct Monitor {
    rx_eligibility_monitor: EligibilityMonitor,
    rx_decision_monitor: DecisionMonitor,
    rx_plan_monitor: PlanMonitor<PlanningStrategy>,
    rx_governance_monitor: GovernanceMonitor,
    rx_execution_monitor: Option<ExecutionMonitor<GovernanceOutcome>>,
    tx_feedback: Option<ActorSourceApi<Telemetry>>,
}

impl std::fmt::Debug for Monitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Monitor")
            .field("rx_eligibility", &self.rx_eligibility_monitor)
            .field("rx_decision", &self.rx_decision_monitor)
            .field("rx_plan", &self.rx_plan_monitor)
            .field("rx_governance", &self.rx_governance_monitor)
            .field("rx_execution", &self.rx_execution_monitor)
            .field("tx_feedback", &self.tx_feedback)
            .finish()
    }
}

impl Monitor {
    pub fn new(
        rx_eligibility_monitor: EligibilityMonitor, rx_decision_monitor: DecisionMonitor,
        rx_plan_monitor: PlanMonitor<PlanningStrategy>, rx_governance_monitor: GovernanceMonitor,
        rx_execution_monitor: Option<ExecutionMonitor<GovernanceOutcome>>,
        tx_feedback: Option<ActorSourceApi<Telemetry>>,
    ) -> Self {
        Self {
            rx_eligibility_monitor,
            rx_decision_monitor,
            rx_plan_monitor,
            rx_governance_monitor,
            rx_execution_monitor,
            tx_feedback,
        }
    }

    #[tracing::instrument(level = "info", name = "monitor phase events")]
    pub async fn run(mut self) {
        let mut loaded = BitFlags::<PhaseLoaded>::default();
        Self::mark_ready_phases(&mut loaded);

        let mut rx_execution = self.rx_execution_monitor.unwrap_or_else(|| {
            let (_, rx_dummy) = tokio::sync::broadcast::channel(0);
            rx_dummy
        });

        let tx_feedback = self.tx_feedback.as_ref();

        loop {
            let span = tracing::info_span!("monitor event cycle");
            let _span_guard = span.enter();

            tokio::select! {
                Ok(e) = self.rx_eligibility_monitor.recv() => Self::handle_eligibility_event(e, &mut loaded),
                Ok(e) = self.rx_decision_monitor.recv() => Self::handle_decision_event(e, &mut loaded),
                Ok(e) = self.rx_plan_monitor.recv() => Self::handle_plan_event(e),
                Ok(e) = self.rx_governance_monitor.recv() => Self::handle_governance_event(e, &mut loaded),
                Ok(e) = rx_execution.recv() => Self::handle_execution_event(e, tx_feedback, &mut loaded).await,
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

    #[tracing::instrument(level = "info")]
    fn mark_ready_phases(loaded: &mut BitFlags<PhaseLoaded>) {
        use proctor::phases::collection::SubscriptionRequirements;

        vec![
            (
                PhaseLoaded::Eligibility,
                EligibilityContext::required_fields().is_empty(),
            ),
            (PhaseLoaded::Decision, DecisionContext::required_fields().is_empty()),
            (PhaseLoaded::Governance, GovernanceContext::required_fields().is_empty()),
        ]
        .into_iter()
        .for_each(|(phase, is_ready)| {
            if is_ready {
                tracing::info!(?phase, "springline {} phase starts as ready for data.", phase);
                loaded.toggle(phase);
            }
        })
    }

    fn handle_eligibility_event(event: Arc<EligibilityEvent>, loaded: &mut BitFlags<PhaseLoaded>) {
        match &*event {
            EligibilityEvent::ItemPassed(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item passed eligibility");
                ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(true as i64)
            },
            EligibilityEvent::ItemBlocked(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item blocked in eligibility");
                ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(false as i64)
            },
            EligibilityEvent::ContextChanged(Some(_ctx)) if !loaded.contains(PhaseLoaded::Eligibility) => {
                tracing::info!(?event, "Eligibility Phase initial context loaded!");
                loaded.toggle(PhaseLoaded::Eligibility);
            },
            ignored_event => tracing::warn!(?ignored_event, "ignoring eligibility event"),
        }
    }

    fn handle_decision_event(event: Arc<DecisionEvent>, loaded: &mut BitFlags<PhaseLoaded>) {
        match &*event {
            DecisionEvent::ItemPassed(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item passed scaling decision");
                DECISION_SHOULD_PLAN_FOR_SCALING.set(true as i64)
            },
            DecisionEvent::ItemBlocked(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item blocked by scaling decision");
                DECISION_SHOULD_PLAN_FOR_SCALING.set(false as i64)
            },
            DecisionEvent::ContextChanged(Some(_ctx)) if !loaded.contains(PhaseLoaded::Decision) => {
                tracing::info!(?event, "Decision Phase initial context loaded!");
                loaded.toggle(PhaseLoaded::Decision);
            },
            ignored_event => tracing::warn!(?ignored_event, "ignoring decision event"),
        }
    }

    fn handle_plan_event(event: Arc<PlanEvent>) {
        match &*event {
            PlanEvent::ObservationAdded(observation) => {
                tracing::info!(?observation, correlation=?observation.correlation_id, "observation added to planning");
                PLAN_OBSERVATION_COUNT.inc()
            },
            PlanEvent::DecisionPlanned(decision, plan) => {
                match decision {
                    DecisionResult::ScaleUp(_) => DECISION_SCALING_DECISION_COUNT.up.inc(),
                    DecisionResult::ScaleDown(_) => DECISION_SCALING_DECISION_COUNT.down.inc(),
                    DecisionResult::NoAction(_) => DECISION_SCALING_DECISION_COUNT.no_action.inc(),
                }

                match decision {
                    DecisionResult::ScaleUp(_) | DecisionResult::ScaleDown(_) => {
                        tracing::info!(?event, correlation=?decision.item().correlation_id, "planning for scaling decision");
                        DECISION_PLAN_CURRENT_NR_TASK_MANAGERS.set(plan.current_nr_task_managers as i64);
                        PLAN_TARGET_NR_TASK_MANAGERS.set(plan.target_nr_task_managers as i64);
                    },
                    _no_action => {
                        tracing::info!(?event, correlation=?decision.item().correlation_id, "no planning action by decision");
                    },
                }
            },
            PlanEvent::DecisionIgnored(decision) => {
                tracing::info!(?event, correlation=?decision.item().correlation_id, "planning is ignoring decision result.");

                match decision {
                    DecisionResult::ScaleUp(_) => DECISION_SCALING_DECISION_COUNT.up.inc(),
                    DecisionResult::ScaleDown(_) => DECISION_SCALING_DECISION_COUNT.down.inc(),
                    DecisionResult::NoAction(_) => DECISION_SCALING_DECISION_COUNT.no_action.inc(),
                }
            },
        }
    }

    fn handle_governance_event(event: Arc<GovernanceEvent>, loaded: &mut BitFlags<PhaseLoaded>) {
        match &*event {
            GovernanceEvent::ItemPassed(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item passed governance");
                GOVERNANCE_PLAN_ACCEPTED.set(true as i64)
            },
            GovernanceEvent::ItemBlocked(_item, query_result) => {
                tracing::info!(?event, ?query_result, "data item blocked in governance");
                GOVERNANCE_PLAN_ACCEPTED.set(false as i64)
            },
            GovernanceEvent::ContextChanged(Some(_ctx)) if !loaded.contains(PhaseLoaded::Governance) => {
                tracing::info!(?event, "Governance Phase initial context loaded!");
                loaded.toggle(PhaseLoaded::Governance);
            },
            ignored_event => tracing::warn!(?ignored_event, "ignoring governance event"),
        }
    }

    async fn handle_execution_event(
        event: Arc<ExecutionEvent<GovernanceOutcome>>, tx_feedback: Option<&ActorSourceApi<Telemetry>>,
        _loaded: &mut BitFlags<PhaseLoaded>,
    ) {
        match &*event {
            ExecutionEvent::PlanExecuted(plan) => {
                tracing::info!(?plan, correlation=?plan.correlation_id, "plan executed");
                EXECUTION_SCALE_ACTION_COUNT
                    .with_label_values(&[
                        plan.current_nr_task_managers.to_string().as_str(),
                        plan.target_nr_task_managers.to_string().as_str(),
                    ])
                    .inc();

                let now = Timestamp::now();
                let start = plan.recv_timestamp;
                let seconds = now.as_f64() - start.as_f64();
                PIPELINE_CYCLE_TIME.observe(seconds);

                if let Some(tx) = tx_feedback {
                    let now: chrono::DateTime<Utc> = now.into();
                    let now_rep = format!("{}", now.format(FORMAT));
                    let telemetry = maplit::hashmap! { crate::phases::eligibility::CLUSTER__LAST_DEPLOYMENT.to_string() => now_rep.into(), };
                    let (cmd, rx) = ActorSourceCmd::push(telemetry.into());

                    match tx.send(cmd) {
                        Ok(_) => {
                            tracing::info!(scale_deployment_timestamp=?now, "notify springline of scale deployment");
                        },
                        Err(err) => {
                            tracing::error!(error=?err, "failed to send scale deployment notification from monitor.");
                        },
                    }

                    if let Err(error) = rx.await {
                        tracing::error!(?error, "scale deployment notification failed at clearinghouse")
                    }
                }
            },
            ExecutionEvent::PlanFailed { plan, error_metric_label } => {
                tracing::error!(%error_metric_label, ?plan, "plan execution failed.");
                EXECUTION_ERRORS
                    .with_label_values(&[
                        plan.current_nr_task_managers.to_string().as_str(),
                        plan.target_nr_task_managers.to_string().as_str(),
                        error_metric_label,
                    ])
                    .inc()
            },
        }
    }
}

make_static_metric! {
    pub label_enum ScalingDecision {
        up,
        down,
        no_action,
    }

    pub struct ScalingDecisionsCount: IntCounter {
        "decision" => ScalingDecision,
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

pub(crate) static DECISION_SCALING_DECISION_COUNT_METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "decision_scaling_decision_count",
            "Count of decisions for scaling planning made.",
        ),
        &["decision"],
    )
    .expect("failed creating decision_scaling_decision_count metric")
});

pub(crate) static DECISION_SCALING_DECISION_COUNT: Lazy<ScalingDecisionsCount> =
    Lazy::new(|| ScalingDecisionsCount::from(&DECISION_SCALING_DECISION_COUNT_METRIC));

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
