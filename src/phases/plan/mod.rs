pub use benchmark::Benchmark;
pub use forecast::*;
use oso::PolarClass;
pub use performance_history::PerformanceHistory;
pub use performance_repository::{
    make_performance_repository, PerformanceMemoryRepository, PerformanceRepository,
    PerformanceRepositorySettings, PerformanceRepositoryType,
};
pub use planning::FlinkPlanning;
use serde::{Deserialize, Serialize};

use crate::phases::decision::result::DecisionResult;
use crate::phases::decision::DecisionOutcome;
use crate::phases::MetricCatalog;
use crate::settings::Settings;
use proctor::elements::Timestamp;
use proctor::graph::stage::ThroughStage;

mod benchmark;
pub mod forecast;
mod performance_history;
mod performance_repository;
mod planning;

pub type PlanningOutcome = FlinkScalePlan;

#[tracing::instrument(level = "info", skip(_settings))]
pub async fn make_plan_phase(
    _settings: &Settings,
) -> anyhow::Result<Box<dyn ThroughStage<DecisionOutcome, PlanningOutcome>>> {
    todo!()
}

const MINIMAL_CLUSTER_SIZE: u16 = 1;

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlinkScalePlan {
    #[polar(attribute)]
    pub timestamp: Timestamp,

    #[polar(attribute)]
    pub target_nr_task_managers: u16,

    #[polar(attribute)]
    pub current_nr_task_managers: u16,
}

impl FlinkScalePlan {
    pub fn new(
        decision: DecisionResult<MetricCatalog>,
        calculated_nr_task_managers: Option<u16>,
        min_scaling_step: u16,
    ) -> Option<Self> {
        use DecisionResult as DR;

        let current_nr_task_managers = decision.item().cluster.nr_task_managers;
        let timestamp = decision.item().timestamp;
        let scale_plan_for = |target_nr_task_managers: u16| {
            Some(FlinkScalePlan {
                timestamp,
                target_nr_task_managers,
                current_nr_task_managers,
            })
        };

        match (decision, calculated_nr_task_managers) {
            (DR::ScaleUp(_), Some(calculated)) if current_nr_task_managers < calculated => {
                scale_plan_for(calculated)
            }

            (DR::ScaleUp(_), _) => {
                let corrected_nr_task_managers = current_nr_task_managers + min_scaling_step;

                tracing::warn!(
                    ?calculated_nr_task_managers,
                    %current_nr_task_managers,
                    %corrected_nr_task_managers,
                    %min_scaling_step,
                    "scale up calculation was not sufficient - applying minimal scaling step."
                );

                scale_plan_for(corrected_nr_task_managers)
            }

            (DR::ScaleDown(_), Some(calculated)) if calculated < current_nr_task_managers => {
                scale_plan_for(calculated)
            }

            (DR::ScaleDown(_), _) => {
                let corrected_nr_task_managers = if min_scaling_step < current_nr_task_managers {
                    current_nr_task_managers - min_scaling_step
                } else {
                    MINIMAL_CLUSTER_SIZE
                };

                tracing::warn!(
                    ?calculated_nr_task_managers,
                    %current_nr_task_managers,
                    %min_scaling_step,
                    %corrected_nr_task_managers,
                    "scale down calculation was not sufficient - applying minimal scaling step."
                );

                scale_plan_for(corrected_nr_task_managers)
            }

            (DR::NoAction(_), _) => None,
        }
    }
}
