use std::fmt::Display;
use std::time::Duration;

use either::{Either, Left, Right};
use once_cell::sync::Lazy;
use proctor::elements::Timestamp;
use proctor::error::MetricLabel;
use proctor::graph::stage::{self, SinkStage};
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, IntCounterVec, Opts};
pub use protocol::{ActEvent, ActMonitor};
use thiserror::Error;

use crate::phases::governance::GovernanceOutcome;

mod action;
mod scale_actuator;
pub use action::ActionOutcome;
pub use action::ACTION_TOTAL_DURATION;
pub use action::FLINK_MISSED_JAR_RESTARTS;
pub use scale_actuator::ScaleActuator;

use crate::phases::plan::{ScaleDirection, ScalePlan};
use crate::CorrelationId;

pub trait ScaleActionPlan {
    fn correlation(&self) -> &CorrelationId;
    fn recv_timestamp(&self) -> Timestamp;
    fn direction(&self) -> ScaleDirection;
    fn current_job_parallelism(&self) -> usize;
    fn target_job_parallelism(&self) -> usize;
    fn current_replicas(&self) -> usize;
    fn target_replicas(&self) -> usize;
    fn parallelism_for_replicas(&self, nr_replicas: usize) -> Option<usize>;
    fn replicas_for_parallelism(&self, parallelism: usize) -> Option<usize>;
}

impl ScaleActionPlan for ScalePlan {
    fn correlation(&self) -> &CorrelationId {
        &self.correlation_id
    }

    fn recv_timestamp(&self) -> Timestamp {
        self.recv_timestamp
    }

    fn direction(&self) -> ScaleDirection {
        self.direction()
    }

    fn current_job_parallelism(&self) -> usize {
        self.current_job_parallelism as usize
    }

    fn target_job_parallelism(&self) -> usize {
        self.target_job_parallelism as usize
    }

    fn current_replicas(&self) -> usize {
        self.current_nr_taskmanagers as usize
    }

    fn target_replicas(&self) -> usize {
        self.target_nr_taskmanagers as usize
    }

    fn parallelism_for_replicas(&self, nr_replicas: usize) -> Option<usize> {
        Some(self.parallelism_for_taskmanagers(nr_replicas as u32) as usize)
    }

    fn replicas_for_parallelism(&self, parallelism: usize) -> Option<usize> {
        Some(self.taskmanagers_for_parallelism(parallelism as u32) as usize)
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

    #[error("Job failed during attempted restart.")]
    FailedJob(crate::flink::JobId, crate::flink::SavepointLocation),

    #[error("failure restarting Flink savepoints for jars: {jar_savepoints:?}: {sources:?}")]
    JobRestart {
        sources: Vec<anyhow::Error>,
        jar_savepoints: Vec<(crate::flink::JarId, crate::flink::SavepointLocation)>,
        possible_depleted_taskmanagers: bool,
    },

    #[error("failure occurred in the PatchReplicas inlet port: {0}")]
    Port(#[from] proctor::error::PortError),

    #[error("Failure in act phase precondition precheck in {action} action: {reason}")]
    ActionPrecondition { action: String, reason: String },

    #[error("failure occurred while processing data in the PatchReplicas stage: {0}")]
    Stage(#[from] anyhow::Error),
}

impl MetricLabel for ActError {
    fn slug(&self) -> String {
        "act".into()
    }

    fn next(&self) -> Either<String, Box<&dyn MetricLabel>> {
        match self {
            Self::Timeout(..) => Left("timeout".into()),
            Self::Kubernetes(_) => Left("kubernetes".into()),
            // Self::Kube(_) => Left("kubernetes".into()),
            // Self::KubeApi(e) => Left(format!("kubernetes::{}", e.reason).into()),
            Self::Flink(e) => Right(Box::new(e)),
            Self::Savepoint { .. } => Left("savepoint".into()),
            Self::FailedJob(_, _) => Left("restart::flink".into()),
            Self::JobRestart { .. } => Left("restart::jar".into()),
            Self::Port(e) => Right(Box::new(e)),
            Self::ActionPrecondition { action, .. } => Left(action.into()),
            Self::Stage(_) => Left("stage".into()),
        }
    }
}

mod protocol {
    use std::sync::Arc;

    use tokio::sync::broadcast;

    use crate::phases::act::action::ActionOutcome;
    use crate::phases::act::ScaleActionPlan;
    use crate::CorrelationId;

    pub type ActMonitor<P> = broadcast::Receiver<Arc<ActEvent<P>>>;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ActEvent<P> {
        PlanActionStarted(P),
        PlanExecuted {
            plan: P,
            outcomes: Vec<ActionOutcome>,
        },
        PlanFailed {
            plan: P,
            error_metric_label: String,
        },
    }

    impl<P: ScaleActionPlan> ActEvent<P> {
        #[allow(clippy::missing_const_for_fn)]
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
fn start_rescale_timer(action: &str) -> HistogramTimer {
    ACT_RESCALE_ACTION_TIME.with_label_values(&[action]).start_timer()
}

pub(crate) static ACT_RESCALE_ACTION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "act_rescale_action_time_seconds",
            "Time spent during the entire rescale action",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone())
        .buckets(vec![
            0.25, 1.0, 2., 10., 30., 60., 120.0, 180., 300., 450., 600.,
        ]),
        &["action"],
    )
    .expect("failed creating act_rescale_action_time_seconds histogram metric")
});

pub(crate) static ACT_RESCALE_ACTION_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "act_rescale_action_count",
            "Count of action taken to rescale to target size",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
        &["current_nr_task_managers", "target_nr_task_managers"],
    )
    .expect("failed creating act_rescale_action_count metric")
});

pub(crate) static PHASE_ACT_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("phase_act_errors", "Count of errors executing scale plans")
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
        &[
            "action",
            "error_type",
            "disposition",
            // "current_job_parallelism",
            // "target_job_parallelism",
            // "current_nr_task_managers",
            // "target_nr_task_managers",
        ],
    )
    .expect("failed creating phase_act_errors metric")
});

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ActErrorDisposition {
    Failed,
    Recovered,
    Ignored,
}

#[inline]
pub(crate) fn track_act_errors<'p, E, P>(
    action: &str, error: Option<&E>, disposition: ActErrorDisposition, _plan: &'p P,
) where
    E: MetricLabel,
    P: ScaleActionPlan,
{
    let error_type = error.map(|e| e.label()).unwrap_or_else(|| "other".to_string());

    PHASE_ACT_ERRORS
        .with_label_values(&[
            action,
            error_type.as_str(),
            disposition.to_string().as_str(),
            // plan.current_job_parallelism().to_string().as_str(),
            // plan.target_job_parallelism().to_string().as_str(),
            // plan.current_replicas().to_string().as_str(),
            // plan.target_replicas().to_string().as_str(),
        ])
        .inc()
}

#[tracing::instrument(level = "trace")]
pub fn make_logger_act_phase() -> Box<dyn SinkStage<GovernanceOutcome>> {
    Box::new(stage::Foreach::new(
        "logging_act",
        |plan: GovernanceOutcome| {
            ACT_RESCALE_ACTION_COUNT
                .with_label_values(&[
                    plan.current_nr_taskmanagers.to_string().as_str(),
                    plan.target_nr_taskmanagers.to_string().as_str(),
                ])
                .inc();
        },
    ))
}
