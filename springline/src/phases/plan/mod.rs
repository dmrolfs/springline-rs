use crate::phases::decision::DecisionResult;
use crate::phases::MetricCatalog;
use crate::settings::PlanSettings;
use crate::Result;
use once_cell::sync::Lazy;
use proctor::graph::{Connect, SinkShape, SourceShape};
use proctor::phases::collection::{ClearinghouseSubscriptionMagnet, SubscriptionChannel, TelemetrySubscription};
use proctor::phases::plan::{Plan, Planning};
use proctor::SharedString;
use prometheus::Gauge;

mod benchmark;
mod forecast;
mod model;
mod performance_history;
mod performance_repository;
mod planning;

pub use forecast::{WorkloadMeasurement, SpikeSettings, WorkloadForecastBuilder, LeastSquaresWorkloadForecastBuilder};
pub use model::ScalePlan;
pub use performance_repository::{PerformanceRepositorySettings, PerformanceRepositoryType};
pub use planning::FlinkPlanning;

pub use performance_repository::make_performance_repository;

const MINIMAL_CLUSTER_SIZE: usize = 1;

pub type PlanningStrategy = planning::FlinkPlanning<forecast::LeastSquaresWorkloadForecastBuilder>;
pub type PlanningOutcome = <PlanningStrategy as Planning>::Out;
pub type PlanningPhase = (Box<Plan<PlanningStrategy>>, SubscriptionChannel<MetricCatalog>);
pub type PlanEvent = proctor::phases::plan::PlanEvent<MetricCatalog, DecisionResult<MetricCatalog>, ScalePlan>;

#[tracing::instrument(level = "info", skip(settings, clearinghouse_magnet))]
pub async fn make_plan_phase(
    settings: &PlanSettings, clearinghouse_magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<PlanningPhase> {
    let name: SharedString = "planning".into();
    let data_channel = do_connect_plan_data(name.clone(), clearinghouse_magnet).await?;
    let flink_planning = do_make_planning_strategy(name.as_ref(), settings).await?;
    let plan = Box::new(Plan::new(name.into_owned(), flink_planning));

    (data_channel.outlet(), plan.inlet()).connect().await;
    Ok((plan, data_channel))
}

pub(crate) static PLANNING_FORECASTED_WORKLOAD: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "planning_forecasted_workload",
        "forecasted workload (records per second), which the maximum of recovery and at valid point",
    )
    .expect("failed creating planning_forecasted_workload metric")
});

pub(crate) static PLANNING_RECOVERY_WORKLOAD_RATE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "planning_recovery_workload_rate",
        "workload rate (records per second) required to recover from restart, included processing buffered records",
    )
    .expect("failed creating planning_recovery_workload_rate metric")
});

pub(crate) static PLANNING_VALID_WORKLOAD_RATE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "planning_valid_workload_rate",
        "workload rate (records per second) required to reach target valid point after autoscale",
    )
    .expect("failed creating planning_valid_workload_rate metric")
});

#[tracing::instrument(level = "info")]
async fn do_connect_plan_data(
    name: SharedString, mut magnet: ClearinghouseSubscriptionMagnet<'_>,
) -> Result<SubscriptionChannel<MetricCatalog>> {
    let subscription = TelemetrySubscription::new(name.as_ref()).for_requirements::<MetricCatalog>();
    let channel = SubscriptionChannel::new(name).await?;
    magnet
        .subscribe(subscription, channel.subscription_receiver.clone())
        .await?;
    Ok(channel)
}

#[tracing::instrument(level = "info")]
async fn do_make_planning_strategy(name: &str, plan_settings: &PlanSettings) -> Result<PlanningStrategy> {
    let planning = PlanningStrategy::new(
        name,
        plan_settings.min_scaling_step as usize,
        plan_settings.restart,
        plan_settings.max_catch_up,
        plan_settings.recovery_valid,
        forecast::LeastSquaresWorkloadForecastBuilder::new(plan_settings.window, plan_settings.spike),
        performance_repository::make_performance_repository(&plan_settings.performance_repository)?,
    )
    .await?;
    Ok(planning)
}
