use crate::phases::decision::result::DecisionResult;
use crate::phases::plan::MINIMAL_CLUSTER_SIZE;
use crate::phases::MetricCatalog;
use oso::PolarClass;
use proctor::elements::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(PolarClass, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalePlan {
    #[polar(attribute)]
    pub timestamp: Timestamp,

    #[polar(attribute)]
    pub target_nr_task_managers: u16,

    #[polar(attribute)]
    pub current_nr_task_managers: u16,
}

impl ScalePlan {
    pub fn new(
        decision: DecisionResult<MetricCatalog>, calculated_nr_task_managers: Option<u16>, min_scaling_step: u16,
    ) -> Option<Self> {
        use DecisionResult as DR;

        let current_nr_task_managers = decision.item().cluster.nr_task_managers;
        let timestamp = decision.item().timestamp;
        let scale_plan_for = |target_nr_task_managers: u16| {
            Some(ScalePlan {
                timestamp,
                target_nr_task_managers,
                current_nr_task_managers,
            })
        };

        match (decision, calculated_nr_task_managers) {
            (DR::ScaleUp(_), Some(calculated)) if current_nr_task_managers < calculated => scale_plan_for(calculated),

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

            (DR::ScaleDown(_), Some(calculated)) if calculated < current_nr_task_managers => scale_plan_for(calculated),

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
