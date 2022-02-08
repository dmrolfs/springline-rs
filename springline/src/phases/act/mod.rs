use either::{Either, Left, Right};
use once_cell::sync::Lazy;
use proctor::error::MetricLabel;
use proctor::graph::stage::{self, SinkStage};
use proctor::SharedString;
use prometheus::{Histogram, HistogramOpts, IntCounterVec, Opts};
use thiserror::Error;

use crate::phases::governance::GovernanceOutcome;

mod scale_actuator;

pub use scale_actuator::ScaleActuator;

#[derive(Debug, Error)]
pub enum ActError {
    #[error("failure in kubernetes client: {0}")]
    Kube(#[from] kube::Error),

    #[error("failure in kubernetes api:{0}")]
    KubeApi(#[from] kube::error::ErrorResponse),

    #[error("failure occurred in the PatchReplicas inlet port: {0}")]
    Port(#[from] proctor::error::PortError),

    #[error("failure occurred while processing data in the PatchReplicas stage: {0}")]
    Stage(#[from] anyhow::Error),
}

impl MetricLabel for ActError {
    fn slug(&self) -> SharedString {
        SharedString::Borrowed("actact")
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::Kube(_) => Left("kubernetes".into()),
            Self::KubeApi(e) => Left(format!("kubernetes::{}", e.reason).into()),
            Self::Port(e) => Right(Box::new(e)),
            Self::Stage(_) => Left("stage".into()),
        }
    }
}

mod protocol {
    use std::sync::Arc;
    use tokio::sync::broadcast;

    pub type ActMonitor<T> = broadcast::Receiver<Arc<ActEvent<T>>>;

    #[derive(Debug, Clone, PartialEq)]
    pub enum ActEvent<T> {
        PlanExecuted(T),
        PlanFailed { plan: T, error_metric_label: String },
    }
}

pub use protocol::{ActEvent, ActMonitor};

pub(crate) static ACT_SCALE_ACTION_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("act_scale_action_count", "Count of action taken to target sizes"),
        &["current_nr_task_managers", "target_nr_task_managers"],
    )
    .expect("failed creating act_scale_action_count metric")
});

pub(crate) static PIPELINE_CYCLE_TIME: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        HistogramOpts::new(
            "pipeline_cycle_time",
            "cycle time processing for act actions taken on telemetry from receipt in seconds",
        )
        .buckets(vec![1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 7.5, 10.0]),
    )
    .expect("failed creating pipeline_cycle_time metric")
});

pub(crate) static ACT_PHASE_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("act_phase_errors", "Count of errors executing scale plans"),
        &["current_nr_task_managers", "target_nr_task_managers", "error_type"],
    )
    .expect("failed creating act_phase_errors metric")
});

#[tracing::instrument(level = "info")]
pub fn make_logger_act_phase() -> Box<dyn SinkStage<GovernanceOutcome>> {
    // let act: Box<dyn SinkStage<GovernanceOutcome>> =
    Box::new(stage::Foreach::new("actlogging_act", |plan: GovernanceOutcome| {
        ACT_SCALE_ACTION_COUNT
            .with_label_values(&[
                plan.current_nr_task_managers.to_string().as_str(),
                plan.target_nr_task_managers.to_string().as_str(),
            ])
            .inc();
        tracing::warn!(scale_plan=?plan, "EXECUTE SCALE PLAN!");
    }))

    // act
}
