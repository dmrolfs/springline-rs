use once_cell::sync::Lazy;
use pretty_snowflake::Id;
use springline::flink::CorrelationId;

mod fixtures;
mod test_decision;
mod test_eligibility;
mod test_flink_plan_calculator;
mod test_flink_planning_stage;
mod test_flink_sensor_combined_stage;
mod test_governance;

static CORRELATION_ID: Lazy<CorrelationId> = Lazy::new(|| Id::direct("MetricCatalog", 9, "CBA"));
