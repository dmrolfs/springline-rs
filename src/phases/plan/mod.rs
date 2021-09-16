pub use benchmark::Benchmark;
pub use forecast::*;
use oso::PolarClass;
pub use performance_history::PerformanceHistory;
pub use performance_repository::{
    make_performance_repository, PerformanceFileRepository, PerformanceMemoryRepository,
    PerformanceRepository, PerformanceRepositorySettings, PerformanceRepositoryType,
};
pub use planning::FlinkPlanning;
use serde::{Deserialize, Serialize};

use crate::phases::decision::result::DecisionResult;
use crate::phases::MetricCatalog;
use crate::settings::PlanSettings;
use crate::Result;
use proctor::elements::Timestamp;
use proctor::graph::{Connect, SinkShape, SourceShape};
use proctor::phases::collection::{
    ClearinghouseSubscriptionMagnet, SubscriptionChannel, TelemetrySubscription,
};
use proctor::phases::plan::{Plan, Planning};

mod benchmark;
pub mod forecast;
mod performance_history;
mod performance_repository;
mod planning;

const MINIMAL_CLUSTER_SIZE: u16 = 1;

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
        decision: DecisionResult<MetricCatalog>,
        calculated_nr_task_managers: Option<u16>,
        min_scaling_step: u16,
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

pub type PlanningStrategy = FlinkPlanning<LeastSquaresWorkloadForecastBuilder>;
pub type PlanningOutcome = <PlanningStrategy as Planning>::Out;
pub type PlanningPhase = Box<Plan<PlanningStrategy>>;

#[tracing::instrument(level = "info", skip(settings, clearinghouse_magnet))]
pub async fn make_plan_phase(
    settings: &PlanSettings,
    clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<PlanningPhase> {
    let name = "autoscale_planning";
    let data_channel = do_connect_plan_data(name, clearinghouse_magnet).await?;
    let flink_planning = do_make_planning_strategy(name, settings).await?;
    let plan: PlanningPhase = Box::new(Plan::new(name, flink_planning));

    (data_channel.outlet(), plan.inlet()).connect().await;
    Ok(plan)
}

#[tracing::instrument(level = "info")]
async fn do_connect_plan_data(
    name: &str,
    mut magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<SubscriptionChannel<MetricCatalog>> {
    let channel = SubscriptionChannel::new(name).await?;
    let subscription = TelemetrySubscription::new(name).with_requirements::<MetricCatalog>();
    magnet
        .subscribe(subscription, channel.subscription_receiver.clone())
        .await?;
    Ok(channel)

    // WORK HERE
    // let ch = magnet.subscribe()
    // Ok(MetricCatalog::connect_channel(name, magnet).await?)
}

#[tracing::instrument(level = "info")]
async fn do_make_planning_strategy(
    name: &str,
    plan_settings: &PlanSettings,
) -> Result<PlanningStrategy> {
    let planning = PlanningStrategy::new(
        name,
        plan_settings.min_scaling_step,
        plan_settings.restart,
        plan_settings.max_catch_up,
        plan_settings.recovery_valid,
        LeastSquaresWorkloadForecastBuilder::new(plan_settings.window, plan_settings.spike),
        make_performance_repository(&plan_settings.performance_repository)?,
    )
    .await?;
    Ok(planning)
}
