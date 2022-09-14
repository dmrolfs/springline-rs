use std::fmt::{self, Debug, Display};
use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use proctor::AppData;

use super::ScaleActionPlan;
use crate::flink::{FlinkContext, JarId, JobId, JobSavepointReport};
use crate::kubernetes::KubernetesContext;
use crate::phases::act::ActError;
use crate::CorrelationId;

mod composite;
mod cull;
mod flink_settlement;
mod kubernetes_settlement;
mod patch_replicas;
mod prepare_data;
mod restart_jobs;
mod savepoint;

pub use composite::CompositeAction;
pub use cull::CullTaskmanagers;
pub use flink_settlement::FlinkSettlement;
pub use kubernetes_settlement::KubernetesSettlement;
pub use patch_replicas::PatchReplicas;
pub use prepare_data::PrepareData;
pub use restart_jobs::RestartJobs;
pub use restart_jobs::FLINK_MISSED_JAR_RESTARTS;
pub use savepoint::CancelWithSavepoint;

#[async_trait]
pub trait ScaleAction: Debug + Send + Sync {
    type Plan: AppData + ScaleActionPlan;

    fn label(&self) -> &str;

    fn check_preconditions(&self, session: &ActionSession) -> Result<(), ActError>;

    async fn execute<'s>(
        &mut self, plan: &'s Self::Plan, session: &'s mut ActionSession,
    ) -> Result<(), ActError>;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct NoAction<P> {
    marker: PhantomData<P>,
}

impl<P> Default for NoAction<P> {
    fn default() -> Self {
        Self { marker: PhantomData }
    }
}

pub const NO_ACTION_LABEL: &str = "no_action";

#[async_trait]
impl<P> ScaleAction for NoAction<P>
where
    P: AppData + ScaleActionPlan,
{
    type Plan = P;

    fn label(&self) -> &str {
        NO_ACTION_LABEL
    }

    fn check_preconditions(&self, _session: &ActionSession) -> Result<(), ActError> {
        Ok(())
    }

    async fn execute<'s>(
        &mut self, _plan: &'s Self::Plan, _session: &'s mut ActionSession,
    ) -> Result<(), ActError> {
        Ok(())
    }
}

pub const ACTION_TOTAL_DURATION: &str = "total_duration";

fn action_step(action: &str, step: &str) -> String {
    format!("{}::{}", action, step)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionOutcome {
    pub label: String,
    pub status: ActionStatus,
    pub duration: Duration,
}

impl Display for ActionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{{ {} @ {:?} }}",
            self.label, self.status, self.duration
        )
    }
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ActionStatus {
    Success,
    Recovered,
    Failure,
}

#[derive(Clone)]
pub struct ActionSession {
    pub correlation: CorrelationId,
    pub history: Vec<ActionOutcome>,
    pub kube: KubernetesContext,
    pub flink: FlinkContext,
    pub nr_target_replicas: Option<usize>,
    pub nr_confirmed_rescaled_taskmanagers: Option<usize>,
    pub active_jobs: Option<Vec<JobId>>,
    pub uploaded_jars: Option<Vec<JarId>>,
    pub savepoints: Option<JobSavepointReport>,
}

impl ActionSession {
    pub fn new(correlation: CorrelationId, kube: KubernetesContext, flink: FlinkContext) -> Self {
        Self {
            correlation,
            history: Default::default(),
            kube,
            flink,
            nr_target_replicas: None,
            nr_confirmed_rescaled_taskmanagers: None,
            active_jobs: None,
            uploaded_jars: None,
            savepoints: None,
        }
    }

    pub fn correlation(&self) -> CorrelationId {
        self.correlation.clone()
    }

    pub fn mark_completion(
        &mut self, label: impl AsRef<str>, status: ActionStatus, duration: Duration,
    ) {
        self.history.push(ActionOutcome {
            label: label.as_ref().to_string(),
            status,
            duration,
        })
    }
}

impl Debug for ActionSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ActionSession");
        debug.field("correlation", &format!("{}", self.correlation)).field(
            "history",
            &self.history.iter().map(|o| o.to_string()).collect::<Vec<_>>(),
        );

        if let Some(active_jobs) = &self.active_jobs {
            debug.field("active_jobs", &active_jobs);
        }

        if let Some(uploaded_jars) = &self.uploaded_jars {
            debug.field("uploaded_jars", &uploaded_jars);
        }

        if let Some(savepoints) = &self.savepoints {
            debug.field("savepoints", &savepoints);
        }

        if let Some(nr_target_replicas) = &self.nr_target_replicas {
            debug.field("nr_target_replicas", &nr_target_replicas);
        }

        if let Some(nr_confirmed_rescaled_taskmanagers) = &self.nr_confirmed_rescaled_taskmanagers {
            debug.field(
                "nr_confirmed_rescaled_taskmanagers",
                &nr_confirmed_rescaled_taskmanagers,
            );
        }

        debug.finish()
    }
}
