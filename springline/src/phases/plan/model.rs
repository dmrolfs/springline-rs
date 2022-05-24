use std::fmt::{self, Display};

use oso::PolarClass;
use pretty_snowflake::Id;
use proctor::elements::Timestamp;
use proctor::Correlation;
use serde::{Deserialize, Serialize};

use crate::flink::MetricCatalog;
use crate::phases::decision::DecisionResult;
use crate::phases::plan::MINIMAL_CLUSTER_SIZE;
use crate::CorrelationId;

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ScaleDirection {
    Up,
    Down,
    None,
}

#[derive(PolarClass, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalePlan {
    pub correlation_id: CorrelationId,

    #[polar(attribute)]
    pub recv_timestamp: Timestamp,

    #[polar(attribute)]
    pub target_nr_task_managers: u32,

    #[polar(attribute)]
    pub current_nr_task_managers: u32,
}

impl fmt::Debug for ScalePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalePlan")
            .field("correlation_id", &self.correlation_id)
            .field("recv_timestamp", &format!("{}", self.recv_timestamp))
            .field("target_nr_task_managers", &self.target_nr_task_managers)
            .field("current_nr_task_managers", &self.current_nr_task_managers)
            .finish()
    }
}

impl Correlation for ScalePlan {
    type Correlated = MetricCatalog;

    fn correlation(&self) -> &Id<Self::Correlated> {
        &self.correlation_id
    }
}

impl ScalePlan {
    pub fn new(
        decision: DecisionResult<MetricCatalog>, calculated_nr_task_managers: Option<usize>, min_scaling_step: usize,
    ) -> Option<Self> {
        use DecisionResult as DR;

        let current_nr_task_managers = decision.item().cluster.nr_task_managers as usize;
        let timestamp = decision.item().recv_timestamp;
        let correlation_id = decision.item().correlation_id.clone();
        let scale_plan_for = |target_nr_task_managers: usize| {
            Some(Self {
                correlation_id,
                recv_timestamp: timestamp,
                target_nr_task_managers: target_nr_task_managers as u32,
                current_nr_task_managers: current_nr_task_managers as u32,
            })
        };

        match (decision, calculated_nr_task_managers) {
            (DR::ScaleUp(_), Some(calculated)) if current_nr_task_managers < calculated => scale_plan_for(calculated),

            (DR::ScaleUp(_), _) => {
                let corrected_nr_task_managers = current_nr_task_managers + min_scaling_step;

                tracing::info!(
                    ?calculated_nr_task_managers,
                    %current_nr_task_managers,
                    %corrected_nr_task_managers,
                    %min_scaling_step,
                    "scale up calculation was not sufficient - applying minimal scaling step."
                );

                scale_plan_for(corrected_nr_task_managers)
            },

            (DR::ScaleDown(_), Some(calculated)) if calculated < current_nr_task_managers => scale_plan_for(calculated),

            (DR::ScaleDown(_), _) => {
                let corrected_nr_task_managers = if min_scaling_step < current_nr_task_managers {
                    current_nr_task_managers - min_scaling_step
                } else {
                    MINIMAL_CLUSTER_SIZE
                };

                tracing::info!(
                    ?calculated_nr_task_managers,
                    %current_nr_task_managers,
                    %min_scaling_step,
                    %corrected_nr_task_managers,
                    "scale down calculation was not sufficient - applying minimal scaling step."
                );

                scale_plan_for(corrected_nr_task_managers)
            },

            (DR::NoAction(_), _) => None,
        }
    }

    pub const fn direction(&self) -> ScaleDirection {
        match (self.current_nr_task_managers, self.target_nr_task_managers) {
            (current, target) if current < target => ScaleDirection::Up,
            (current, target) if target < current => ScaleDirection::Down,
            (..) => ScaleDirection::None,
        }
    }
}
