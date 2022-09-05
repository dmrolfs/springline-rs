use std::collections::HashSet;

use once_cell::sync::Lazy;
use pretty_snowflake::{Id, Label};
use proctor::elements::{RecordsPerSecond, Timestamp};
use proctor::graph::{Connect, SinkShape, SourceShape};
use proctor::phases::plan::{Plan, Planning};
use proctor::phases::sense::{
    ClearinghouseSubscriptionAgent, SubscriptionChannel, SubscriptionRequirements,
    TelemetrySubscription,
};
use proctor::AppData;
use prometheus::{Gauge, Opts};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::settings::PlanSettings;
use crate::Result;

mod benchmark;
mod context;
mod forecast;
mod model;
mod performance_history;
mod performance_repository;
mod planning;

pub use crate::phases::decision::ScaleDirection;
pub use context::{PlanningContext, PLANNING__TASK_SLOTS_PER_TASKMANAGER, PLANNING__RESCALE_RESTART};
pub use context::{
    PLANNING_CTX_FORECASTING_MAX_CATCH_UP_SECS, PLANNING_CTX_FORECASTING_RECOVERY_VALID_SECS,
    PLANNING_CTX_FORECASTING_RESTART_SECS, PLANNING_CTX_MIN_SCALING_STEP,
};
pub use forecast::{
    ForecastInputs, Forecaster, LeastSquaresWorkloadForecaster, SpikeSettings, WorkloadMeasurement,
};
pub use model::ScalePlan;
pub use performance_history::PerformanceHistory;
pub use performance_repository::make_performance_repository;
pub use performance_repository::{PerformanceRepositorySettings, PerformanceRepositoryType};
pub use planning::{FlinkPlanning, FlinkPlanningEvent, FlinkPlanningMonitor};

use crate::flink::{MetricCatalog, MC_FLOW__RECORDS_IN_PER_SEC};

const MINIMAL_JOB_PARALLELISM: u32 = 1;

pub type PlanningStrategy = planning::FlinkPlanning<forecast::LeastSquaresWorkloadForecaster>;
pub type PlanningOutcome = <PlanningStrategy as Planning>::Out;

pub struct PlanningPhase {
    pub phase: Box<Plan<PlanningStrategy>>,
    pub data_channel: SubscriptionChannel<PlanningMeasurement>,
    pub context_channel: SubscriptionChannel<PlanningContext>,
    pub rx_flink_planning_monitor: FlinkPlanningMonitor,
}

#[derive(Debug, Label, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanningMeasurement {
    pub correlation_id: Id<Self>,
    pub recv_timestamp: Timestamp,
    #[serde(rename = "flow.records_in_per_sec")]
    pub records_in_per_sec: RecordsPerSecond,
}

impl From<MetricCatalog> for PlanningMeasurement {
    fn from(metrics: MetricCatalog) -> Self {
        Self {
            correlation_id: metrics.correlation_id.relabel(),
            recv_timestamp: metrics.recv_timestamp,
            records_in_per_sec: metrics.flow.records_in_per_sec.into(),
        }
    }
}

impl SubscriptionRequirements for PlanningMeasurement {
    fn required_fields() -> HashSet<String> {
        maplit::hashset! {
            MC_FLOW__RECORDS_IN_PER_SEC.into(),
        }
    }
}

#[tracing::instrument(level = "trace", skip(settings, agent))]
pub async fn make_plan_phase<A>(settings: &PlanSettings, agent: &mut A) -> Result<PlanningPhase>
where
    A: ClearinghouseSubscriptionAgent,
{
    let name = "planning";
    let data_channel = do_subscribe_channel(&format!("{name}_observations"), agent).await?;
    let context_channel = do_subscribe_channel(&format!("{name}_context"), agent).await?;
    let flink_planning = do_make_planning_strategy(name, settings).await?;
    let rx_flink_planning_monitor = flink_planning.rx_monitor();
    let phase = Box::new(Plan::new(name, flink_planning));

    (data_channel.outlet(), phase.inlet()).connect().await;
    (context_channel.outlet(), phase.context_inlet()).connect().await;

    Ok(PlanningPhase {
        phase,
        data_channel,
        context_channel,
        rx_flink_planning_monitor,
    })
}

pub(crate) static PLANNING_FORECASTED_WORKLOAD: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "planning_forecasted_workload",
            "forecasted workload (records per second), which the maximum of recovery and at valid point",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating planning_forecasted_workload metric")
});

pub(crate) static PLANNING_RECOVERY_WORKLOAD_RATE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(Opts::new(
        "planning_recovery_workload_rate",
        "workload rate (records per second) required to recover from restart, included processing buffered records",
    ).const_labels(proctor::metrics::CONST_LABELS.clone()))
    .expect("failed creating planning_recovery_workload_rate metric")
});

pub(crate) static PLANNING_VALID_WORKLOAD_RATE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "planning_valid_workload_rate",
            "workload rate (records per second) required to reach target valid point after autoscale",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating planning_valid_workload_rate metric")
});

#[tracing::instrument(level = "trace", skip(agent))]
async fn do_subscribe_channel<T, A>(name: &str, agent: &mut A) -> Result<SubscriptionChannel<T>>
where
    T: AppData + SubscriptionRequirements + DeserializeOwned,
    A: ClearinghouseSubscriptionAgent,
{
    let subscription = TelemetrySubscription::new(name).for_requirements::<T>();
    let channel = SubscriptionChannel::new(name).await?;
    agent
        .subscribe(subscription, channel.subscription_receiver.clone())
        .await?;
    Ok(channel)
}

#[tracing::instrument(level = "trace")]
async fn do_make_planning_strategy(
    name: &str, plan_settings: &PlanSettings,
) -> Result<PlanningStrategy> {
    let inputs = ForecastInputs::from_settings(plan_settings)?;
    let forecaster = LeastSquaresWorkloadForecaster::new(plan_settings.window, plan_settings.spike);
    let repository =
        performance_repository::make_performance_repository(&plan_settings.performance_repository)?;

    let planning = PlanningStrategy::new(
        name,
        plan_settings.min_scaling_step,
        inputs,
        forecaster,
        repository,
    )
    .await?;
    Ok(planning)
}
