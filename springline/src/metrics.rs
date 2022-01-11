use prometheus::Registry;

use crate::engine::monitor;
use crate::phases::{collection, eligibility, execution, governance, metric_catalog, plan};
use crate::Result;

#[tracing::instrument(level = "info")]
pub fn register_metrics(registry: &Registry) -> Result<()> {
    proctor::metrics::register_proctor_metrics(registry)?;

    registry.register(Box::new(metric_catalog::METRIC_CATALOG_TIMESTAMP.clone()))?;

    registry.register(Box::new(metric_catalog::METRIC_CATALOG_JOB_HEALTH_UPTIME.clone()))?;
    registry.register(Box::new(metric_catalog::METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS.clone()))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS.clone(),
    ))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS.clone(),
    ))?;

    registry.register(Box::new(metric_catalog::METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC.clone()))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC.clone(),
    ))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_FLOW_INPUT_RECORDS_LAG_MAX.clone(),
    ))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_FLOW_INPUT_MILLIS_BEHIND_LATEST.clone(),
    ))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS.clone(),
    ))?;
    registry.register(Box::new(metric_catalog::METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD.clone()))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED.clone(),
    ))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED.clone(),
    ))?;
    registry.register(Box::new(metric_catalog::METRIC_CATALOG_CLUSTER_TASK_NR_THREADS.clone()))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN.clone(),
    ))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE.clone(),
    ))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN.clone(),
    ))?;
    registry.register(Box::new(
        metric_catalog::METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE.clone(),
    ))?;

    registry.register(Box::new(collection::flink::FLINK_COLLECTION_TIME.clone()))?;
    registry.register(Box::new(collection::flink::FLINK_COLLECTION_ERRORS.clone()))?;

    registry.register(Box::new(
        eligibility::context::ELIGIBILITY_CTX_ALL_SINKS_HEALTHY.clone(),
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

    registry.register(Box::new(plan::PLANNING_FORECASTED_WORKLOAD.clone()))?;
    registry.register(Box::new(plan::PLANNING_RECOVERY_WORKLOAD_RATE.clone()))?;
    registry.register(Box::new(plan::PLANNING_VALID_WORKLOAD_RATE.clone()))?;

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

    registry.register(Box::new(execution::EXECUTION_SCALE_ACTION_COUNT.clone()))?;
    registry.register(Box::new(execution::PIPELINE_CYCLE_TIME.clone()))?;
    registry.register(Box::new(execution::EXECUTION_ERRORS.clone()))?;

    Ok(())
}
