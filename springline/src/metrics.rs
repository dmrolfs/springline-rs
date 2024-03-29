use proctor::elements::telemetry::UpdateMetricsFn;
use prometheus::Registry;

use crate::phases::{act, decision, eligibility, plan, sense};
use crate::{engine, flink, Result};

pub trait UpdateMetrics {
    fn update_metrics_for(name: &str) -> UpdateMetricsFn;
}

#[tracing::instrument(level = "trace")]
pub fn register_metrics(registry: &Registry) -> Result<()> {
    proctor::metrics::register_proctor_metrics(registry)?;

    registry.register(Box::new(engine::ENGINE_PROCESS_MEMORY.clone()))?;
    registry.register(Box::new(engine::ENGINE_SYSTEM_TOTAL_MEMORY.clone()))?;
    registry.register(Box::new(engine::ENGINE_SYSTEM_FREE_MEMORY.clone()))?;
    registry.register(Box::new(engine::ENGINE_SYSTEM_AVAILABLE_MEMORY.clone()))?;
    registry.register(Box::new(engine::ENGINE_SYSTEM_USED_MEMORY.clone()))?;
    registry.register(Box::new(engine::ENGINE_SYSTEM_TOTAL_SWAP.clone()))?;
    registry.register(Box::new(engine::ENGINE_SYSTEM_FREE_SWAP.clone()))?;
    registry.register(Box::new(engine::ENGINE_SYSTEM_USED_SWAP.clone()))?;

    registry.register(Box::new(flink::METRIC_CATALOG_JOB_HEALTH_UPTIME.clone()))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS.clone(),
    ))?;

    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_IDLE_TIME_MILLIS_PER_SEC.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_TASK_UTILIZATION.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_RECORDS_LAG_MAX.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_ASSIGNED_PARTITIONS.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_TOTAL_LAG.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_RECORDS_CONSUMED_RATE.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_IS_CONSUMER_TELEMETRY_POPULATED.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_MILLIS_BEHIND_LATEST.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_TASK_UTILIZATION_ROLLING_AVG.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_TOTAL_LAG_ROLLING_AVG.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_RELATIVE_LAG_VELOCITY_ROLLING_AVG.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_FLOW_SOURCE_BACK_PRESSURE_TIME_ROLLING_AVG.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_NR_ACTIVE_JOBS.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_TASK_NR_THREADS.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN.clone(),
    ))?;
    registry.register(Box::new(
        flink::METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE.clone(),
    ))?;

    registry.register(Box::new(flink::FLINK_ERRORS.clone()))?;
    registry.register(Box::new(flink::FLINK_UPLOADED_JARS_TIME.clone()))?;
    registry.register(Box::new(flink::FLINK_ACTIVE_JOBS_TIME.clone()))?;
    registry.register(Box::new(flink::FLINK_QUERY_JOB_DETAIL_TIME.clone()))?;
    registry.register(Box::new(flink::FLINK_QUERY_TASKMANAGER_ADMIN_TIME.clone()))?;

    registry.register(Box::new(sense::flink::FLINK_SENSOR_TIME.clone()))?;
    registry.register(Box::new(sense::flink::FLINK_VERTEX_SENSOR_TIME.clone()))?;
    registry.register(Box::new(
        sense::flink::FLINK_VERTEX_SENSOR_METRIC_PICKLIST_TIME.clone(),
    ))?;
    registry.register(Box::new(
        sense::flink::FLINK_VERTEX_SENSOR_AVAIL_TELEMETRY_TIME.clone(),
    ))?;

    registry.register(Box::new(
        eligibility::ELIGIBILITY_CTX_ALL_SINKS_HEALTHY.clone(),
    ))?;
    registry.register(Box::new(
        eligibility::ELIGIBILITY_CTX_TASK_LAST_FAILURE.clone(),
    ))?;
    registry.register(Box::new(
        eligibility::ELIGIBILITY_CTX_CLUSTER_IS_DEPLOYING.clone(),
    ))?;
    registry.register(Box::new(
        eligibility::ELIGIBILITY_CTX_CLUSTER_LAST_DEPLOYMENT.clone(),
    ))?;
    registry.register(Box::new(
        eligibility::ELIGIBILITY_POLICY_INELIGIBLE_DECISIONS_COUNT.clone(),
    ))?;

    registry.register(Box::new(
        decision::DECISION_SCALING_DECISION_COUNT_METRIC.clone(),
    ))?;

    registry.register(Box::new(plan::PLANNING_CTX_MIN_SCALING_STEP.clone()))?;
    registry.register(Box::new(
        plan::PLANNING_CTX_FORECASTING_RESTART_SECS.clone(),
    ))?;
    registry.register(Box::new(
        plan::PLANNING_CTX_FORECASTING_MAX_CATCH_UP_SECS.clone(),
    ))?;
    registry.register(Box::new(
        plan::PLANNING_CTX_FORECASTING_RECOVERY_VALID_SECS.clone(),
    ))?;
    registry.register(Box::new(plan::PLANNING_FORECASTED_WORKLOAD.clone()))?;
    registry.register(Box::new(plan::PLANNING_RECOVERY_WORKLOAD_RATE.clone()))?;
    registry.register(Box::new(plan::PLANNING_VALID_WORKLOAD_RATE.clone()))?;
    registry.register(Box::new(
        plan::PLANNING_PERFORMANCE_HISTORY_ENTRY_COUNT.clone(),
    ))?;
    registry.register(Box::new(plan::PLANNING_PARALLELISM_CLIPPING_POINT.clone()))?;
    registry.register(Box::new(plan::PLANNING_PARALLELISM_CLIPPING_STATE.clone()))?;

    registry.register(Box::new(
        engine::ELIGIBILITY_IS_ELIGIBLE_FOR_SCALING.clone(),
    ))?;
    registry.register(Box::new(engine::DECISION_RESCALE_DECISION.clone()))?;
    registry.register(Box::new(engine::PLAN_OBSERVATION_COUNT.clone()))?;
    registry.register(Box::new(engine::PLAN_TARGET_NR_TASK_MANAGERS.clone()))?;
    registry.register(Box::new(engine::GOVERNANCE_PLAN_ACCEPTED.clone()))?;

    registry.register(Box::new(act::ACT_IS_RESCALING.clone()))?;
    registry.register(Box::new(act::ACT_RESCALE_ACTION_TIME.clone()))?;
    registry.register(Box::new(act::ACT_RESCALE_ACTION_COUNT.clone()))?;
    registry.register(Box::new(act::PHASE_ACT_ERRORS.clone()))?;
    registry.register(Box::new(act::FLINK_MISSED_JAR_RESTARTS.clone()))?;

    Ok(())
}
