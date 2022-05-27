mod context;
mod error;
mod model;

pub use context::FlinkContext;
pub use error::FlinkError;
pub use model::catalog::{ClusterMetrics, FlowMetrics, JobHealthMetrics, MetricCatalog};
pub use model::catalog::{
    MC_CLUSTER__NR_ACTIVE_JOBS, MC_CLUSTER__NR_TASK_MANAGERS, MC_FLOW__FORECASTED_RECORDS_IN_PER_SEC,
    MC_FLOW__FORECASTED_TIMESTAMP, MC_FLOW__RECORDS_IN_PER_SEC,
};
pub(crate) use model::catalog::{
    METRIC_CATALOG_CLUSTER_NR_ACTIVE_JOBS, METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS,
    METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD, METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED,
    METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED, METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE,
    METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN, METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE,
    METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN, METRIC_CATALOG_CLUSTER_TASK_NR_THREADS,
    METRIC_CATALOG_FLOW_INPUT_MILLIS_BEHIND_LATEST, METRIC_CATALOG_FLOW_INPUT_RECORDS_LAG_MAX,
    METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC, METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC,
    METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS, METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS,
    METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS, METRIC_CATALOG_JOB_HEALTH_UPTIME, METRIC_CATALOG_TIMESTAMP,
};
pub use model::portfolio::{MetricPortfolio, MetricPortfolioBuilder, Portfolio};
pub use model::{
    CorrelationGenerator, CorrelationId, FailureCause, JobSavepointReport, OperationStatus, SavepointLocation,
    SavepointStatus,
};
pub use model::{JarId, JobDetail, JobId, JobState, JobSummary, RestoreMode, TaskState, VertexDetail, VertexId};
pub use model::{JOB_STATES, TASK_STATES};
use once_cell::sync::Lazy;
use proctor::error::MetricLabel;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, IntCounterVec, Opts};

#[allow(clippy::cognitive_complexity)]
pub(crate) fn log_response(label: &str, response: &reqwest::Response) {
    const PREAMBLE: &str = "flink response received";
    let status = response.status();
    if status.is_success() || status.is_informational() {
        tracing::debug!(?response, "{PREAMBLE}: {label}");
    } else if status.is_client_error() {
        tracing::warn!(?response, "{PREAMBLE}: {label}");
    } else {
        tracing::warn!(?response, "{PREAMBLE}: {label}");
    }
}

pub static FLINK_UPLOADED_JARS_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_uploaded_jars_time",
            "Time spent querying uploaded jars from Flink in seconds",
        )
        .buckets(vec![0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0]),
        &["action"],
    )
    .expect("failed creating flink_uploaded_jars_time metric")
});

#[inline]
fn start_flink_uploaded_jars_timer(label: &str) -> HistogramTimer {
    FLINK_UPLOADED_JARS_TIME.with_label_values(&[label]).start_timer()
}

pub static FLINK_ACTIVE_JOBS_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_active_jobs_time",
            "Time spent collecting active jobs from Flink in seconds",
        )
        .buckets(vec![0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0]),
        &["action"],
    )
    .expect("failed creating flink_active_jobs_time metric")
});

#[inline]
fn start_flink_active_jobs_timer(label: &str) -> HistogramTimer {
    FLINK_ACTIVE_JOBS_TIME.with_label_values(&[label]).start_timer()
}

pub static FLINK_QUERY_JOB_DETAIL_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_query_job_detail_time",
            "Time spent collecting job detail from Flink in seconds",
        )
        .buckets(vec![0.2, 0.225, 0.25, 0.275, 0.3, 0.35, 0.4, 0.45, 0.5, 0.75, 1.0]),
        &["action"],
    )
    .expect("failed creating flink_query_job_detail_time metric")
});

#[inline]
fn start_flink_query_job_detail_timer(label: &str) -> HistogramTimer {
    FLINK_QUERY_JOB_DETAIL_TIME.with_label_values(&[label]).start_timer()
}

pub static FLINK_QUERY_TASKMANAGER_ADMIN_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_query_taskmanager_admin_time",
            "Time spent querying Flink taskmanager admin in seconds",
        )
        .buckets(vec![0.2, 0.225, 0.25, 0.275, 0.3, 0.35, 0.4, 0.45, 0.5, 0.75, 1.0]),
        &["action"],
    )
    .expect("failed creating flink_query_taskmanager_admin_time metric")
});

#[inline]
fn start_flink_query_taskmanager_admin_timer(label: &str) -> HistogramTimer {
    FLINK_QUERY_TASKMANAGER_ADMIN_TIME
        .with_label_values(&[label])
        .start_timer()
}

pub(crate) static FLINK_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("flink_errors", "Number of errors calling the Flink API"),
        &["action", "error_type"],
    )
    .expect("failed creating flink_errors metric")
});

#[inline]
pub(crate) fn track_result<T>(
    label: &str, result: Result<T, FlinkError>, error_message: &str, correlation: &CorrelationId,
) -> Result<T, FlinkError> {
    if let Err(ref err) = result {
        tracing::error!(error=?err, ?correlation, "{}", error_message);
        track_flink_errors(label, err);
    }

    result
}

#[inline]
pub(crate) fn track_flink_errors<E: MetricLabel>(action: &str, error: &E) {
    FLINK_ERRORS.with_label_values(&[action, error.label().as_ref()]).inc()
}
