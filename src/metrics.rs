use crate::engine::monitor;
use crate::phases::{decision, eligibility, governance, metric_catalog};
use crate::Result;
use prometheus::Registry;

#[tracing::instrument(level = "info")]
pub fn register_metrics(registry: &Registry) -> Result<()> {
    proctor::metrics::register_proctor_metrics(registry)?;

    registry.register(Box::new(metric_catalog::METRIC_CATALOG_TIMESTAMP.clone()))?;
    registry.register(Box::new(metric_catalog::METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC.clone()))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC.clone(),
    ))?;
    registry.register(Box::new(metric_catalog::METRIC_CATALOG_FLOW_INPUT_CONSUMER_LAG.clone()))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS.clone(),
    ))?;
    registry.register(Box::new(metric_catalog::METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD.clone()))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_NETWORK_IO_UTILIZATION.clone(),
    ))?;

    registry.register(Box::new(
        eligibility::context::ELIGIBILITY_CTX_TASK_LAST_FAILURE.clone(),
    ))?;
    registry.register(Box::new(
        eligibility::context::ELIGIBILITY_CTX_CLUSTER_IS_DEPLOYING.clone(),
    ))?;
    registry.register(Box::new(
        eligibility::context::ELIGIBILITY_CTX_CLUSTER_LAST_DEPLOYMENT.clone(),
    ))?;

    registry.register(Box::new(decision::context::DECISION_CTX_ALL_SINKS_HEALTHY.clone()))?;
    registry.register(Box::new(decision::context::DECISION_CTX_NR_TASK_MANAGERS.clone()))?;

    registry.register(Box::new(governance::context::GOVERNANCE_CTX_MIN_CLUSTER_SIZE.clone()))?;
    registry.register(Box::new(governance::context::GOVERNANCE_CTX_MAX_CLUSTER_SIZE.clone()))?;
    registry.register(Box::new(governance::context::GOVERNANCE_CTX_MAX_SCALING_STEP.clone()))?;

    registry.register(Box::new(monitor::ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.clone()))?;
    registry.register(Box::new(monitor::DECISION_SHOULD_PLAN_FOR_SCALING.clone()))?;
    registry.register(Box::new(monitor::DECISION_SCALING_DECISION_COUNT_METRIC.clone()))?;
    registry.register(Box::new(monitor::PLAN_OBSERVATION_COUNT.clone()))?;
    registry.register(Box::new(monitor::DECISION_PLAN_CURRENT_NR_TASK_MANAGERS.clone()))?;
    registry.register(Box::new(monitor::PLAN_TARGET_NR_TASK_MANAGERS.clone()))?;
    registry.register(Box::new(monitor::GOVERNANCE_PLAN_ACCEPTED.clone()))?;

    Ok(())
}
