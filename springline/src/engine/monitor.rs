use std::fmt;
use std::sync::Arc;

use enumflags2::{bitflags, BitFlags};
use lazy_static::lazy_static;
use proctor::phases::plan::PlanMonitor;
use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts};
use prometheus_static_metric::make_static_metric;

use crate::phases::decision::result::DecisionResult;
use crate::phases::decision::{DecisionEvent, DecisionMonitor};
use crate::phases::eligibility::{EligibilityEvent, EligibilityMonitor};
use crate::phases::governance::{GovernanceEvent, GovernanceMonitor};
use crate::phases::plan::{PlanEvent, PlanningStrategy};

#[bitflags(default = Collection | Plan | Execution)]
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
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
}

impl std::fmt::Debug for Monitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Monitor")
            .field("rx_eligibility", &self.rx_eligibility_monitor)
            .field("rx_decision", &self.rx_decision_monitor)
            .field("rx_plan", &self.rx_plan_monitor)
            .field("rx_governance", &self.rx_governance_monitor)
            .finish()
    }
}

impl Monitor {
    pub fn new(
        rx_eligibility_monitor: EligibilityMonitor, rx_decision_monitor: DecisionMonitor,
        rx_plan_monitor: PlanMonitor<PlanningStrategy>, rx_governance_monitor: GovernanceMonitor,
    ) -> Self {
        Self {
            rx_eligibility_monitor,
            rx_decision_monitor,
            rx_plan_monitor,
            rx_governance_monitor,
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn run(mut self) {
        let mut loaded = BitFlags::<PhaseLoaded>::default();

        loop {
            tokio::select! {
                Ok(e) = self.rx_eligibility_monitor.recv() => Self::handle_eligibility_event(e, &mut loaded),
                Ok(e) = self.rx_decision_monitor.recv() => Self::handle_decision_event(e, &mut loaded),
                Ok(e) = self.rx_plan_monitor.recv() => Self::handle_plan_event(e),
                Ok(e) = self.rx_governance_monitor.recv() => Self::handle_governance_event(e, &mut loaded),
                else => {
                    tracing::info!("springline monitor stopping...");
                    break;
                }
            }

            if loaded.is_all() {
                tracing::info!("All Phases have loaded initial set of context data.");
            } else {
                tracing::info!(status=?loaded, "Phase first-loading not complete.")
            }
        }
    }

    #[tracing::instrument(level = "info")]
    fn handle_eligibility_event(event: Arc<EligibilityEvent>, loaded: &mut BitFlags<PhaseLoaded>) {
        match &*event {
            EligibilityEvent::ItemPassed(_) => ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(true as i64),
            EligibilityEvent::ItemBlocked(_) => ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.set(false as i64),
            EligibilityEvent::ContextChanged(Some(_)) if !loaded.contains(PhaseLoaded::Eligibility) => {
                tracing::info!("Eligibility Phase initial context loaded!");
                loaded.toggle(PhaseLoaded::Eligibility);
            },
            _ => (),
        }
    }

    #[tracing::instrument(level = "info")]
    fn handle_decision_event(event: Arc<DecisionEvent>, loaded: &mut BitFlags<PhaseLoaded>) {
        match &*event {
            DecisionEvent::ItemPassed(_) => DECISION_SHOULD_PLAN_FOR_SCALING.set(true as i64),
            DecisionEvent::ItemBlocked(_) => DECISION_SHOULD_PLAN_FOR_SCALING.set(false as i64),
            DecisionEvent::ContextChanged(Some(_)) if !loaded.contains(PhaseLoaded::Decision) => {
                tracing::info!("Decision Phase initial context loaded!");
                loaded.toggle(PhaseLoaded::Decision);
            },
            _ => (),
        }
    }

    #[tracing::instrument(level = "info")]
    fn handle_plan_event(event: Arc<PlanEvent>) {
        match &*event {
            PlanEvent::ObservationAdded(_) => PLAN_OBSERVATION_COUNT.inc(),
            PlanEvent::DecisionPlanned(decision, plan) => {
                match decision {
                    DecisionResult::ScaleUp(_) => DECISION_SCALING_DECISION_COUNT.up.inc(),
                    DecisionResult::ScaleDown(_) => DECISION_SCALING_DECISION_COUNT.down.inc(),
                    DecisionResult::NoAction(_) => DECISION_SCALING_DECISION_COUNT.no_action.inc(),
                }

                match decision {
                    DecisionResult::ScaleUp(_) | DecisionResult::ScaleDown(_) => {
                        DECISION_PLAN_CURRENT_NR_TASK_MANAGERS.set(plan.current_nr_task_managers as i64);
                        PLAN_TARGET_NR_TASK_MANAGERS.set(plan.target_nr_task_managers as i64);
                    },
                    _ => {},
                }
            },
            PlanEvent::DecisionIgnored(decision) => match decision {
                DecisionResult::ScaleUp(_) => DECISION_SCALING_DECISION_COUNT.up.inc(),
                DecisionResult::ScaleDown(_) => DECISION_SCALING_DECISION_COUNT.down.inc(),
                DecisionResult::NoAction(_) => DECISION_SCALING_DECISION_COUNT.no_action.inc(),
            },
        }
    }

    #[tracing::instrument(level = "info")]
    fn handle_governance_event(event: Arc<GovernanceEvent>, loaded: &mut BitFlags<PhaseLoaded>) {
        match &*event {
            GovernanceEvent::ItemPassed(_) => GOVERNANCE_PLAN_ACCEPTED.set(true as i64),
            GovernanceEvent::ItemBlocked(_) => GOVERNANCE_PLAN_ACCEPTED.set(false as i64),
            GovernanceEvent::ContextChanged(Some(_)) if !loaded.contains(PhaseLoaded::Governance) => {
                tracing::info!("Governance Phase initial context loaded!");
                loaded.toggle(PhaseLoaded::Governance);
            },
            _ => (),
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

lazy_static! {
    pub(crate) static ref ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING: IntGauge = IntGauge::new(
        "eligibility_is_eligible_for_scaling",
        "Is the cluster in a state deemed eligible for scaling"
    )
    .expect("failed creating eligibility_is_eligible_for_scaling metric");
    pub(crate) static ref DECISION_SHOULD_PLAN_FOR_SCALING: IntGauge = IntGauge::new(
        "decision_should_plan_for_scaling",
        "Should plan for scaling the cluster"
    )
    .expect("failed creating decision_should_plan_for_scaling metric");
    pub(crate) static ref DECISION_SCALING_DECISION_COUNT_METRIC: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "decision_scaling_decision_count",
            "Count of decisions for scaling planning made.",
        ),
        &["decision"],
    )
    .expect("failed creating decision_scaling_decision_count metric");
    pub(crate) static ref DECISION_SCALING_DECISION_COUNT: ScalingDecisionsCount =
        ScalingDecisionsCount::from(&DECISION_SCALING_DECISION_COUNT_METRIC);
    pub(crate) static ref PLAN_OBSERVATION_COUNT: IntCounter =
        IntCounter::new("plan_observation_count", "Number of observations made for planning.")
            .expect("failed creating plan_observation_count metric");
    pub(crate) static ref DECISION_PLAN_CURRENT_NR_TASK_MANAGERS: IntGauge = IntGauge::new(
        "decision_plan_current_nr_task_managers",
        "Number of task managers currently known to Decision and Planning"
    )
    .expect("failed creating decision_plan_current_nr_task_managers metric");
    pub(crate) static ref PLAN_TARGET_NR_TASK_MANAGERS: IntGauge = IntGauge::new(
        "plan_target_nr_task_managers",
        "Number of task managers targeted by Planning"
    )
    .expect("failed creating plan_target_nr_task_managers metric");
    pub(crate) static ref GOVERNANCE_PLAN_ACCEPTED: IntGauge = IntGauge::new(
        "governance_plan_accepted",
        "Has Springline governance accepted the last scaling plan presented"
    )
    .expect("failed creating governance_plan_accepted metric");
}
