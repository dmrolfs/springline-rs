use once_cell::sync::Lazy;
use pretty_snowflake::Id;
use springline::phases::MetricCatalog;

mod fixtures;
mod test_decision;
mod test_eligibility;
mod test_flink_plan_calculator;
mod test_flink_planning_stage;
mod test_governance;
mod test_flink_collection_combined_stage;

static CORRELATION_ID: Lazy<Id<MetricCatalog>> = Lazy::new(|| Id::direct("MetricCatalog", 9, "CBA"));
