use crate::phases::act::scale_actuator::ScaleActionPlan;
use crate::phases::act::ActError;

use crate::settings::ActionSettings;
use async_trait::async_trait;
use patch_replicas::PatchReplicas;

use crate::flink::{JarId, JobId, JobSavepointReport};
use crate::model::CorrelationId;
use proctor::elements::{Telemetry, TelemetryValue};
use proctor::AppData;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

mod patch_replicas;
mod prepare_data;
mod restart_jobs;
mod savepoint;

pub use patch_replicas::FLINK_TASKMANAGER_PATCH_REPLICAS_TIME;
pub use restart_jobs::{FLINK_MISSED_JAR_RESTARTS, FLINK_RESTART_JOB_TIME};
pub use savepoint::FLINK_JOB_SAVEPOINT_WITH_CANCEL_TIME;

#[derive(Debug, Clone, PartialEq)]
pub struct ActionSession<P> {
    pub plan: P,
    pub active_jobs: Option<Vec<JobId>>,
    pub uploaded_jars: Option<Vec<JarId>>,
    pub savepoints: Option<JobSavepointReport>,
    pub durations: HashMap<String, Duration>,
}

impl<P> ActionSession<P>
where
    P: AppData + ScaleActionPlan,
{
    pub fn new(plan: P) -> Self {
        Self {
            plan,
            active_jobs: None,
            uploaded_jars: None,
            savepoints: None,
            durations: HashMap::new(),
        }
    }

    pub fn with_duration(mut self, label: impl AsRef<str>, duration: Duration) -> Self {
        self.durations.insert(label.as_ref().to_string(), duration);
        self
    }

    pub fn correlation(&self) -> CorrelationId {
        self.plan.correlation()
    }
}

#[async_trait]
pub trait ScaleAction<P>: Debug + Send + Sync {
    async fn execute(&mut self, session: ActionSession<P>) -> Result<ActionSession<P>, (ActError, ActionSession<P>)>;
}

pub fn make_action<T: AppData + ScaleActionPlan>(
    kube: &kube::Client, settings: &ActionSettings,
) -> Box<dyn ScaleAction<T>> {
    let patch_replicas = PatchReplicas::new(patch_replicas::ACTION_LABEL, &settings.taskmanager, kube);
    Box::new(patch_replicas)
}
