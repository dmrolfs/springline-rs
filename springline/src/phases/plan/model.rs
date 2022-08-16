use std::fmt;

use oso::PolarClass;
use pretty_snowflake::Id;
use proctor::elements::Timestamp;
use proctor::Correlation;
use serde::{Deserialize, Serialize};

use crate::flink::MetricCatalog;
use crate::phases::decision::DecisionResult;
use crate::phases::plan::{ScaleDirection, MINIMAL_JOB_PARALLELISM};
use crate::CorrelationId;

#[derive(PolarClass, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalePlan {
    pub correlation_id: CorrelationId,

    #[polar(attribute)]
    pub recv_timestamp: Timestamp,

    #[polar(attribute)]
    pub current_job_parallelism: u32,

    #[polar(attribute)]
    pub target_job_parallelism: u32,

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
            .field(
                "job_parallelism_plan",
                &format!(
                    "{}->{}",
                    self.current_job_parallelism, self.target_job_parallelism
                ),
            )
            .field(
                "nr_task_managers_plan",
                &format!(
                    "{}->{}",
                    self.current_nr_task_managers, self.target_nr_task_managers
                ),
            )
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
        decision: DecisionResult<MetricCatalog>,
        calculated_job_parallelism: Option<u32>,
        min_cluster_size: u32,
        min_scaling_step: u32,
    ) -> Option<Self> {
        use DecisionResult as DR;

        let current_job_parallelism = decision.item().health.job_nonsource_max_parallelism;
        let current_nr_task_managers = decision.item().cluster.nr_task_managers;

        let recv_timestamp = decision.item().recv_timestamp;
        let correlation_id = decision.item().correlation_id.clone();

        let scale_plan_for = |target_job_parallelism: u32| {
            let target_nr_task_managers = u32::max(
                min_cluster_size,
                u32::max(current_nr_task_managers, target_job_parallelism)
            );

            Some(Self {
                correlation_id,
                recv_timestamp,
                current_job_parallelism,
                target_job_parallelism,
                target_nr_task_managers,
                current_nr_task_managers,
            })
        };

        match (decision, calculated_job_parallelism) {
            (DR::ScaleUp(_), Some(calculated)) if current_job_parallelism < calculated => {
                scale_plan_for(calculated)
            },

            (DR::ScaleUp(_), _) => {
                let adjusted_target_job_parallelism = current_job_parallelism + min_scaling_step;

                tracing::info!(
                    ?calculated_job_parallelism,
                    %current_job_parallelism,
                    %current_nr_task_managers,
                    %adjusted_target_job_parallelism,
                    %min_scaling_step,
                    "scale up calculation was not sufficient - applying minimal scaling step."
                );

                scale_plan_for(adjusted_target_job_parallelism)
            },

            (DR::ScaleDown(_), Some(calculated)) if calculated < current_job_parallelism => {
                scale_plan_for(calculated)
            },

            (DR::ScaleDown(_), _) => {
                let adjusted_target_job_parallelism = if min_scaling_step < current_job_parallelism {
                    current_job_parallelism - min_scaling_step
                } else {
                    MINIMAL_JOB_PARALLELISM
                };

                tracing::info!(
                    ?calculated_job_parallelism,
                    %current_job_parallelism,
                    %current_nr_task_managers,
                    %min_scaling_step,
                    %adjusted_target_job_parallelism,
                    "scale down calculation was not sufficient - applying minimal scaling step."
                );

                scale_plan_for(adjusted_target_job_parallelism)
            },

            (DR::NoAction(_), _) => None,
        }
    }

    pub const fn direction(&self) -> ScaleDirection {
        match (self.current_job_parallelism, self.target_job_parallelism) {
            (current, target) if current < target => ScaleDirection::Up,
            (current, target) if target < current => ScaleDirection::Down,
            (..) => ScaleDirection::None,
        }
    }
}
