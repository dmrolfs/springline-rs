use crate::flink::{AppDataWindow, MetricCatalog};
use crate::phases;
use crate::phases::eligibility::EligibilityOutcome;
use crate::settings::DecisionSettings;
use crate::Result;
use proctor::elements::{PolicySubscription, QueryResult};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::{ClearinghouseSubscriptionAgent, SubscriptionChannel};
use proctor::Correlation;
use std::fmt::Display;

#[cfg(test)]
#[allow(dead_code)]
mod policy_tests;

mod context;
mod policy;
mod result;

use crate::phases::UNSPECIFIED;
pub use context::DecisionContext;
pub(crate) use policy::DECISION_SCALING_DECISION_COUNT_METRIC;
pub use policy::{DecisionPolicy, DecisionTemplateData};
pub use proctor::error::PolicyError;
pub use result::DecisionResult;
pub use result::DECISION_DIRECTION;
pub use result::{get_direction_and_reason, make_decision_transform};

pub type DecisionData = AppDataWindow<MetricCatalog>;
pub type DecisionOutcome = DecisionResult<DecisionData>;
pub type DecisionApi = proctor::elements::PolicyFilterApi<DecisionContext, DecisionTemplateData>;
pub type DecisionMonitor = proctor::elements::PolicyFilterMonitor<DecisionData, DecisionContext>;
pub type DecisionEvent = proctor::elements::PolicyFilterEvent<DecisionData, DecisionContext>;

pub type DecisionPhase = (
    Box<PolicyPhase<EligibilityOutcome, DecisionOutcome, DecisionContext, DecisionTemplateData>>,
    SubscriptionChannel<DecisionContext>,
);

#[tracing::instrument(level = "trace", skip(agent))]
pub async fn make_decision_phase<A>(
    settings: &DecisionSettings, agent: &mut A,
) -> Result<DecisionPhase>
where
    A: ClearinghouseSubscriptionAgent,
{
    let name = "decision";
    let policy = DecisionPolicy::new(settings)?;
    let subscription = policy.subscription(name, settings);
    let decision = Box::new(
        PolicyPhase::with_transform(
            name,
            policy,
            make_decision_transform(
                name,
                settings
                    .template_data
                    .as_ref()
                    .and_then(|td| td.evaluate_duration_secs)
                    .unwrap_or(60),
            ),
        )
        .await?,
    );
    let channel = phases::subscribe_policy_phase(subscription, &decision, agent).await?;
    Ok((decision, channel))
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ScaleDirection {
    Up,
    Down,
    None,
}

pub fn log_outcome_with_common_criteria(
    message: &str, item: &DecisionData, query_result: Option<&QueryResult>, window: u32,
) {
    let source_records_lag_max = item.flow_source_records_lag_max_rolling_average(window);
    let source_assigned_partitions = item.flow_source_assigned_partitions_rolling_average(window);
    let total_lag = item.flow_source_total_lag_rolling_average(window);
    let records_consumed_rate = item.flow_source_records_consumed_rate_rolling_average(window);
    let relative_lag_velocity = item.flow_source_relative_lag_velocity(window);

    let nonsource_utilization = item.flow_task_utilization_rolling_average(window);
    let source_back_pressure =
        item.flow_source_back_pressured_time_millis_per_sec_rolling_average(window);

    let cluster_task_cpu = item.cluster_task_cpu_load_rolling_average(window);
    let cluster_task_heap_memory_load = item.cluster_task_heap_memory_load_rolling_average(window);
    let cluster_task_network_input_utilization =
        item.cluster_task_network_input_utilization_rolling_average(window);
    let cluster_task_network_output_utilization =
        item.cluster_task_network_output_utilization_rolling_average(window);

    let decision_passed = query_result.as_ref().map(|qr| qr.passed);
    let decision_bindings = query_result.as_ref().map(|qr| &qr.bindings);
    let decision_reason = query_result
        .map(phases::get_outcome_reason)
        .unwrap_or_else(|| UNSPECIFIED.to_string());

    tracing::info!(
        correlation=%item.correlation(),
        ?decision_passed, ?decision_bindings, %decision_reason,
        %source_records_lag_max, %source_assigned_partitions, %total_lag, %records_consumed_rate, %relative_lag_velocity,
        %nonsource_utilization, %source_back_pressure,
        %cluster_task_cpu, %cluster_task_heap_memory_load,
        %cluster_task_network_input_utilization, %cluster_task_network_output_utilization,
        "decision outcome: {message}"
    )
}
