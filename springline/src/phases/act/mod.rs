use crate::phases::governance::GovernanceOutcome;
use either::{Either, Left, Right};
use once_cell::sync::Lazy;
use proctor::elements::Timestamp;
use proctor::error::MetricLabel;
use proctor::graph::stage::{self, SinkStage};
use proctor::SharedString;
use prometheus::{Histogram, HistogramOpts, HistogramTimer, HistogramVec, IntCounterVec, Opts};
pub use protocol::{ActEvent, ActMonitor};
use std::time::Duration;
use thiserror::Error;

mod action;
mod scale_actuator;
use crate::model::CorrelationId;
use crate::phases::plan::ScalePlan;

pub use action::ACTION_TOTAL_DURATION;
pub use action::FLINK_MISSED_JAR_RESTARTS;
// pub use action::{
//     FLINK_JOB_SAVEPOINT_WITH_CANCEL_TIME, FLINK_RESTART_JOB_TIME, FLINK_TASKMANAGER_PATCH_REPLICAS_TIME,
// };
pub use scale_actuator::ScaleActuator;

pub trait ScaleActionPlan {
    fn correlation(&self) -> &CorrelationId;
    fn recv_timestamp(&self) -> Timestamp;
    fn current_replicas(&self) -> usize;
    fn target_replicas(&self) -> usize;
}

impl ScaleActionPlan for ScalePlan {
    fn correlation(&self) -> &CorrelationId {
        &self.correlation_id
    }

    fn recv_timestamp(&self) -> Timestamp {
        self.recv_timestamp
    }

    fn current_replicas(&self) -> usize {
        self.current_nr_task_managers as usize
    }

    fn target_replicas(&self) -> usize {
        self.target_nr_task_managers as usize
    }
}

#[derive(Debug, Error)]
pub enum ActError {
    #[error("Action timed out after {0:?}: {1}")]
    Timeout(Duration, String),

    #[error("failure into kubernetes: {0}")]
    Kubernetes(#[from] crate::kubernetes::KubernetesError),

    // #[error("failure in kubernetes client: {0}")]
    // Kube(#[from] kube::Error),
    #[error("failure while calling Flink API: {0}")]
    Flink(#[from] crate::flink::FlinkError),

    #[error("failure taking Flink savepoints for jobs:{job_ids:?}: {source}")]
    Savepoint {
        #[source]
        source: crate::flink::FlinkError,
        job_ids: Vec<crate::flink::JobId>,
    },

    #[error("failure occurred in the PatchReplicas inlet port: {0}")]
    Port(#[from] proctor::error::PortError),

    #[error("failure occurred while processing data in the PatchReplicas stage: {0}")]
    Stage(#[from] anyhow::Error),
}

impl MetricLabel for ActError {
    fn slug(&self) -> SharedString {
        SharedString::Borrowed("act")
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::Timeout(_, _) => Left("timeout".into()),
            Self::Kubernetes(_) => Left("kubernetes".into()),
            // Self::Kube(_) => Left("kubernetes".into()),
            // Self::KubeApi(e) => Left(format!("kubernetes::{}", e.reason).into()),
            Self::Flink(e) => Right(Box::new(e)),
            Self::Savepoint { .. } => Left("savepoint".into()),
            Self::Port(e) => Right(Box::new(e)),
            Self::Stage(_) => Left("stage".into()),
        }
    }
}

mod protocol {
    use crate::model::CorrelationId;
    use crate::phases::act::ScaleActionPlan;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;

    pub type ActMonitor<P> = broadcast::Receiver<Arc<ActEvent<P>>>;

    #[derive(Debug, Clone, PartialEq)]
    pub enum ActEvent<P> {
        PlanActionStarted(P),
        PlanExecuted {
            plan: P,
            durations: HashMap<String, Duration>,
        },
        PlanFailed {
            plan: P,
            error_metric_label: String,
        },
    }

    impl<P: ScaleActionPlan> ActEvent<P> {
        pub fn plan(&self) -> &P {
            match self {
                Self::PlanActionStarted(p) => p,
                Self::PlanExecuted { plan, .. } => plan,
                Self::PlanFailed { plan, .. } => plan,
            }
        }

        pub fn correlation(&self) -> &CorrelationId {
            self.plan().correlation()
        }
    }
}

#[inline]
fn start_scale_action_timer(cluster_label: &str, action: &str) -> HistogramTimer {
    ACT_SCALE_ACTION_TIME.with_label_values(&[cluster_label, action]).start_timer()
}

pub(crate) static ACT_SCALE_ACTION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "act_scale_action_time_seconds",
            "Time spent during the entire scale action",
        )
        .buckets(vec![1., 1.5, 2., 3., 4., 5., 10.0, 25., 50., 75., 100.]),
        &["label", "action"],
    )
    .expect("failed creating act_scale_action_time_seconds histogram metric")
});

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
        .buckets(vec![0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.75, 1.0]),
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
