use once_cell::sync::Lazy;
use proctor::graph::stage::{self, SinkStage};
use prometheus::{Histogram, HistogramOpts, IntCounterVec, Opts};

use crate::phases::governance::GovernanceOutcome;

mod patch_replicas;

pub use patch_replicas::{ExecutionPhaseError, PatchReplicas};

mod protocol {
    use std::sync::Arc;
    use tokio::sync::broadcast;

    pub type ExecutionMonitor<T> = broadcast::Receiver<Arc<ExecutionEvent<T>>>;

    #[derive(Debug, Clone, PartialEq)]
    pub enum ExecutionEvent<T> {
        PlanExecuted(T),
        PlanFailed { plan: T, error_metric_label: String },
    }
}

pub use protocol::{ExecutionEvent, ExecutionMonitor};

pub(crate) static EXECUTION_SCALE_ACTION_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("execution_scale_action_count", "Count of action taken to target sizes"),
        &["current_nr_task_managers", "target_nr_task_managers"],
    )
    .expect("failed creating execution_scale_action_count metric")
});

pub(crate) static PIPELINE_CYCLE_TIME: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        HistogramOpts::new(
            "pipeline_cycle_time",
            "cycle time processing for execution actions taken on telemetry from receipt in seconds",
        )
        .buckets(vec![1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 7.5, 10.0]),
    )
    .expect("failed creating pipeline_cycle_time metric")
});

pub(crate) static EXECUTION_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("execution_errors", "Count of errors executing scale plans"),
        &["current_nr_task_managers", "target_nr_task_managers", "error_type"],
    )
    .expect("failed creating execution_errors metric")
});

#[tracing::instrument(level = "info")]
pub fn make_logger_execution_phase() -> Box<dyn SinkStage<GovernanceOutcome>> {
    // let execution: Box<dyn SinkStage<GovernanceOutcome>> =
    Box::new(stage::Foreach::new("execution", |plan: GovernanceOutcome| {
        EXECUTION_SCALE_ACTION_COUNT
            .with_label_values(&[
                plan.current_nr_task_managers.to_string().as_str(),
                plan.target_nr_task_managers.to_string().as_str(),
            ])
            .inc();
        tracing::warn!(scale_plan=?plan, "EXECUTE SCALE PLAN!");
    }))

    // execution
}
