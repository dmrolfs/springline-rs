mod context;
mod error;
mod model;

pub use context::FlinkContext;
pub use error::FlinkError;
pub use model::{JobDetail, JobId, JobState, JobSummary, TaskState, VertexDetail, VertexId};
pub use model::{JOB_STATES, TASK_STATES};

use once_cell::sync::Lazy;
use proctor::error::MetricLabel;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, IntCounterVec, Opts};

#[allow(clippy::cognitive_complexity)]
pub(crate) fn log_response(label: &str, response: &reqwest::Response) {
    const PREAMBLE: &str = "flink telemetry response received";
    let status = response.status();
    if status.is_success() || status.is_informational() {
        tracing::info!(?response, "{PREAMBLE}:{label}");
    } else if status.is_client_error() {
        tracing::error!(?response, "{PREAMBLE}:{label}");
    } else {
        tracing::warn!(?response, "{PREAMBLE}:{label}");
    }
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

const ACTIVE_JOBS: &str = "active_jobs";

#[inline]
fn start_flink_active_jobs_timer() -> HistogramTimer {
    FLINK_ACTIVE_JOBS_TIME.with_label_values(&[ACTIVE_JOBS]).start_timer()
}

pub(crate) static FLINK_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("flink_errors", "Number of errors calling the Flink API"),
        &["action", "error_type"],
    )
    .expect("failed creating flink_errors metric")
});

#[inline]
pub(crate) fn track_flink_errors<E: MetricLabel>(action: &str, error: &E) {
    FLINK_ERRORS.with_label_values(&[action, error.label().as_ref()]).inc()
}
